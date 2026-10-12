/* Thread merging closed archived redo logs of several RAC threads
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

OpenLogReplicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

OpenLogReplicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with OpenLogReplicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <map>
#include <sys/stat.h>
#include <unistd.h>

#include "../builder/Builder.h"
#include "../common/exception/RuntimeException.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "../parser/Parser.h"
#include "../reader/ReaderFilesystem.h"
#include "ReplicatorRac.h"

namespace OpenLogReplicator {
    ReplicatorRac::ReplicatorRac(Ctx* newCtx, void (*newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
                                 TransactionBuffer* newTransactionBuffer, std::string newAlias, std::string newDatabase,
                                 std::vector<uint16_t> newThreads, uint32_t newDbid, bool newFollow):
            Replicator(newCtx, newArchGetLog, newBuilder, newMetadata, newTransactionBuffer, std::move(newAlias), std::move(newDatabase)),
            threads(std::move(newThreads)),
            dbid(newDbid),
            follow(newFollow) {}

    ReplicatorRac::~ReplicatorRac() {
        for (ThreadStream& stream: streams) {
            delete stream.parser;
            stream.parser = nullptr;
        }
    }

    std::string ReplicatorRac::getModeName() const {
        return {follow ? "rac-follow" : "rac-batch"};
    }

    bool ReplicatorRac::continueWithOnline() {
        if (keepRunningAtEnd) {
            ctx->info(0, "finished rac-batch processing, the stream writer keeps serving until the program is stopped");
            return false;
        }
        ctx->info(0, "finished rac-batch processing, exiting");
        ctx->stopSoft();
        return false;
    }

    void ReplicatorRac::positionReader() {
        metadata->setSeqFileOffset(Seq::zero(), FileOffset::zero());
    }

    void ReplicatorRac::createSchema() {
        throw RuntimeException(10052, "schema file missing");
    }

    void ReplicatorRac::updateOnlineRedoLogData() {}

    void ReplicatorRac::loadDatabaseMetadata() {}

    ReplicatorRac::RedoFile ReplicatorRac::readHeader(const std::string& path) const {
        RedoFile file;
        file.path = path;

        const int fd = open(path.c_str(), O_RDONLY);
        if (fd == -1)
            throw RuntimeException(10074, "file: " + path + " - open returned: " + strerror(errno));
        struct stat fileStat{};
        std::array<uint8_t, 8192> buffer{};
        ssize_t got = -1;
        if (fstat(fd, &fileStat) == 0)
            got = pread(fd, buffer.data(), buffer.size(), 0);
        close(fd);
        if (got < 1024)
            throw RuntimeException(10074, "file: " + path + " - can't read redo log header");
        file.fileSize = fileStat.st_size;

        const uint8_t* data = buffer.data();
        if (data[0] != 0 || data[28] != 0x7D || data[29] != 0x7C || data[30] != 0x7B || data[31] != 0x7A)
            throw RuntimeException(10074, "file: " + path + " - not a little-endian redo log");
        file.blockSize = ctx->read32(data + 20);
        if (!((file.blockSize == 512 && data[1] == 0x22) || (file.blockSize == 1024 && data[1] == 0x22) ||
              (file.blockSize == 4096 && data[1] == 0x82)) || got < static_cast<ssize_t>(file.blockSize * 2))
            throw RuntimeException(10074, "file: " + path + " - invalid block size: " + std::to_string(file.blockSize));

        const uint8_t* header = data + file.blockSize;
        if (ctx->read32(header + 4) != 1)
            throw RuntimeException(10074, "file: " + path + " - invalid header block number");
        if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::BLOCK_SUM)) {
            uint64_t sum = 0;
            for (uint i = 0; i < file.blockSize / 8; ++i)
                sum ^= *reinterpret_cast<const uint64_t*>(header + (i * 8));
            sum ^= (sum >> 32);
            sum ^= (sum >> 16);
            if ((sum & 0xFFFF) != 0)
                throw RuntimeException(10074, "file: " + path + " - header block checksum mismatch");
        }

        file.sequence = Seq(ctx->read32(header + 8));
        file.dbid = ctx->read32(header + 24);
        file.activation = ctx->read32(header + 52);
        file.numBlocks = ctx->read32(header + 156);
        file.resetlogs = ctx->read32(header + 160);
        file.thread = ctx->read16(header + 176);
        file.firstScn = ctx->readScn(header + 180);
        file.nextScn = ctx->readScn(header + 192);
        return file;
    }

    // Compares the bytes from offset "from" up to the header NAB
    bool ReplicatorRac::sameContent(const RedoFile& file1, const RedoFile& file2, uint64_t from) const {
        const uint64_t size = static_cast<uint64_t>(file1.numBlocks) * file1.blockSize;
        const int fd1 = open(file1.path.c_str(), O_RDONLY);
        const int fd2 = open(file2.path.c_str(), O_RDONLY);
        bool same = (fd1 != -1 && fd2 != -1);
        std::vector<uint8_t> buffer1(Ctx::MEMORY_CHUNK_SIZE);
        std::vector<uint8_t> buffer2(Ctx::MEMORY_CHUNK_SIZE);
        for (uint64_t pos = from; same && pos < size; pos += Ctx::MEMORY_CHUNK_SIZE) {
            const size_t toRead = std::min<uint64_t>(Ctx::MEMORY_CHUNK_SIZE, size - pos);
            same = pread(fd1, buffer1.data(), toRead, static_cast<off_t>(pos)) == static_cast<ssize_t>(toRead) &&
                    pread(fd2, buffer2.data(), toRead, static_cast<off_t>(pos)) == static_cast<ssize_t>(toRead) &&
                    memcmp(buffer1.data(), buffer2.data(), toRead) == 0;
        }
        if (fd1 != -1)
            close(fd1);
        if (fd2 != -1)
            close(fd2);
        return same;
    }

    // A standby redo log and the archived log made from it (or two standby members) are the same log when blocks 2 to NAB are equal and
    // the first two blocks differ only where the copy kind is recorded. Observed in a two-thread test (threads 1 and 2, sequence 36): block 0 bytes
    // 16-17 and 24-27 (file size in blocks), header block bytes 14-15 (checksum) and 236-239 (flags). Both headers already passed
    // the checksum and identity checks
    bool ReplicatorRac::sameStandbyCopy(const RedoFile& file1, const RedoFile& file2) const {
        const uint blockSize = file1.blockSize;
        std::vector<uint8_t> buffer1(static_cast<size_t>(blockSize) * 2);
        std::vector<uint8_t> buffer2(static_cast<size_t>(blockSize) * 2);
        const int fd1 = open(file1.path.c_str(), O_RDONLY);
        const int fd2 = open(file2.path.c_str(), O_RDONLY);
        bool same = fd1 != -1 && fd2 != -1 &&
                pread(fd1, buffer1.data(), buffer1.size(), 0) == static_cast<ssize_t>(buffer1.size()) &&
                pread(fd2, buffer2.data(), buffer2.size(), 0) == static_cast<ssize_t>(buffer2.size());
        if (fd1 != -1)
            close(fd1);
        if (fd2 != -1)
            close(fd2);
        for (uint i = 0; same && i < blockSize * 2; ++i) {
            const bool copyKind = (i >= 16 && i < 18) || (i >= 24 && i < 28) || (i >= blockSize + 14 && i < blockSize + 16) ||
                    (i >= blockSize + 236 && i < blockSize + 240);
            same = copyKind || buffer1[i] == buffer2[i];
        }
        return same && sameContent(file1, file2, static_cast<uint64_t>(blockSize) * 2);
    }

    std::vector<ReplicatorRac::RedoFile> ReplicatorRac::collectInventory() {
        std::vector<std::string> paths;
        for (const std::string& entry: redoLogsBatch) {
            struct stat fileStat{};
            if (stat(entry.c_str(), &fileStat) != 0)
                throw RuntimeException(10074, "inventory entry: " + entry + " - stat returned: " + strerror(errno));
            if (S_ISREG(fileStat.st_mode)) {
                paths.push_back(entry);
                continue;
            }
            if (!S_ISDIR(fileStat.st_mode))
                throw RuntimeException(10074, "inventory entry: " + entry + " - not a file or directory");

            DIR* dir = opendir(entry.c_str());
            if (dir == nullptr)
                throw RuntimeException(10012, "directory: " + entry + " - can't read");
            const dirent* ent;
            while ((ent = readdir(dir)) != nullptr) {
                const std::string name(ent->d_name);
                if (name == "." || name == "..")
                    continue;
                const std::string path(entry + "/" + name);
                if (stat(path.c_str(), &fileStat) != 0 || !S_ISREG(fileStat.st_mode)) {
                    closedir(dir);
                    throw RuntimeException(10074, "inventory entry: " + path + " - not a regular file");
                }
                paths.push_back(path);
            }
            closedir(dir);
        }

        const auto checkIdentity = [this](const RedoFile& file) {
            if (file.dbid != dbid || file.resetlogs != metadata->resetlogs || (metadata->activation != 0 && file.activation != metadata->activation))
                throw RuntimeException(10074, "file: " + file.path + " - database identity (dbid " + std::to_string(file.dbid) + ", resetlogs " +
                                       std::to_string(file.resetlogs) + ", activation " + std::to_string(file.activation) + ") differs from expected (" +
                                       std::to_string(dbid) + ", " + std::to_string(metadata->resetlogs) + ", " + std::to_string(metadata->activation) + ")");
            if (std::find(threads.begin(), threads.end(), file.thread) == threads.end())
                throw RuntimeException(10074, "file: " + file.path + " - thread " + std::to_string(file.thread) + " is not a participating thread");
        };
        const auto complete = [](const RedoFile& file) {
            return file.numBlocks != 0 && file.numBlocks != Ctx::ZERO_BLK && file.nextScn != Scn::none();
        };

        // (thread, sequence) -> file; enumeration order of the inputs must not matter
        std::map<std::pair<uint16_t, uint32_t>, RedoFile> files;
        const auto add = [this, &files](const RedoFile& file) {
            if (file.fileSize < static_cast<uint64_t>(file.numBlocks) * file.blockSize)
                throw RuntimeException(10074, "file: " + file.path + " - not a complete closed " + (file.standby ? "standby" : "archived") +
                                       " log (blocks: " + std::to_string(file.numBlocks) + ", next scn: " + file.nextScn.toString() + ", size: " +
                                       std::to_string(file.fileSize) + ")");
            const auto key = std::make_pair(file.thread, file.sequence.getData());
            auto it = files.find(key);
            if (it == files.end()) {
                files.emplace(key, file);
                return;
            }
            const RedoFile& other = it->second;
            const bool archives = !other.standby && !file.standby;
            if (other.firstScn != file.firstScn || other.nextScn != file.nextScn || other.numBlocks != file.numBlocks ||
                    other.blockSize != file.blockSize || !(archives ? sameContent(other, file, 0) : sameStandbyCopy(other, file)))
                throw RuntimeException(10074, "conflicting copies of thread " + std::to_string(file.thread) + " sequence " +
                                       file.sequence.toString() + ": " + other.path + ", " + file.path);
            if (archives) {
                ctx->info(0, "rac-batch: identical copy of thread " + std::to_string(file.thread) + " sequence " + file.sequence.toString() + ": " +
                          other.path + ", " + file.path);
                if (file.path < other.path)
                    it->second = file;
                return;
            }
            // The archived log is immutable, a standby log is reused later: prefer the archived copy, else the lower path
            const RedoFile& use = (other.standby != file.standby) ? (other.standby ? file : other) : (file.path < other.path ? file : other);
            const std::string copies = "thread " + std::to_string(file.thread) + " sequence " + file.sequence.toString() + ": " + other.path +
                    ", " + file.path + "; using " + use.path;
            if (standbyCopies.insert(copies).second)
                ctx->info(0, getModeName() + ": identical copy (standby log) of " + copies);
            it->second = use;
        };

        for (const std::string& path: paths) {
            const RedoFile file = readHeader(path);
            checkIdentity(file);
            if (!complete(file))
                throw RuntimeException(10074, "file: " + path + " - not a complete closed archived log (blocks: " + std::to_string(file.numBlocks) +
                                       ", next scn: " + file.nextScn.toString() + ", size: " + std::to_string(file.fileSize) + ")");
            add(file);
        }

        // A standby redo log member holds no sequence before its first use, a growing sequence while redo arrives, and a complete one
        // until it is reused; only the complete state is an input, and with rac-standby-live also the growing state
        std::map<std::pair<uint16_t, uint32_t>, RedoFile> growing;
        for (const std::string& path: standbyLogs) {
            RedoFile file = readHeader(path);
            file.standby = true;
            std::string state;
            if (file.sequence == Seq::zero())
                state = "unused (sequence 0)";
            else {
                checkIdentity(file);
                state = std::string(complete(file) ? "complete" : "not complete") + ": thread " + std::to_string(file.thread) + " sequence " +
                        file.sequence.toString() + " scn " + file.firstScn.toString() + " to " + file.nextScn.toString() + " blocks " +
                        std::to_string(file.numBlocks);
            }
            auto it = standbyStates.find(path);
            if (it == standbyStates.end() || it->second != state) {
                ctx->info(0, getModeName() + ": standby log " + path + " " + state);
                standbyStates[path] = state;
            }
            if (file.sequence != Seq::zero() && complete(file))
                add(file);
            else if (file.sequence != Seq::zero() && standbyLive) {
                file.growing = true;
                if (!growing.emplace(std::make_pair(file.thread, file.sequence.getData()), file).second)
                    throw RuntimeException(10074, "two growing standby logs of thread " + std::to_string(file.thread) + " sequence " +
                                           file.sequence.toString() + ": " + growing.at(std::make_pair(file.thread, file.sequence.getData())).path +
                                           ", " + path + "; list one member per standby redo log group");
            }
        }

        // A growing member is an input only where no complete copy of the same thread and sequence exists
        std::vector<RedoFile> inventory;
        inventory.reserve(files.size() + growing.size());
        for (const auto& [key, file]: files)
            inventory.push_back(file);
        for (const auto& [key, file]: growing)
            if (files.find(key) == files.end())
                inventory.push_back(file);
        return inventory;
    }

    // Appends the logs of threadFiles (one thread, by sequence) that continue the chain: the next sequence, starting at the previous next
    // scn. Returns the first later log if a sequence is missing, nullptr if the chain uses every later log
    const ReplicatorRac::RedoFile* ReplicatorRac::extendChain(std::vector<RedoFile>& chain, const std::vector<RedoFile>& threadFiles) {
        if (chain.back().growing)
            return nullptr;
        for (const RedoFile& file: threadFiles) {
            const RedoFile& last = chain.back();
            if (file.growing)
                continue;
            if (file.sequence.getData() <= last.sequence.getData())
                continue;
            if (file.sequence.getData() != last.sequence.getData() + 1)
                return &file;
            if (file.firstScn != last.nextScn)
                throw RuntimeException(10074, "gap in thread " + std::to_string(file.thread) + " after sequence " + last.sequence.toString() +
                                       " (next scn " + last.nextScn.toString() + "): sequence " + file.sequence.toString() + " starts at scn " +
                                       file.firstScn.toString());
            chain.push_back(file);
        }
        return nullptr;
    }

    // rac-standby-live: a chain of complete logs ends with the growing standby log of the next sequence if one starts at its next scn
    void ReplicatorRac::appendGrowing(std::vector<RedoFile>& chain, const std::vector<RedoFile>& threadFiles) const {
        const RedoFile& last = chain.back();
        if (!standbyLive || last.growing)
            return;
        for (const RedoFile& file: threadFiles) {
            if (!file.growing || file.sequence.getData() != last.sequence.getData() + 1)
                continue;
            if (file.firstScn != last.nextScn)
                throw RuntimeException(10074, "gap in thread " + std::to_string(file.thread) + " after sequence " + last.sequence.toString() +
                                       " (next scn " + last.nextScn.toString() + "): growing standby log of sequence " + file.sequence.toString() +
                                       " starts at scn " + file.firstScn.toString() + ": " + file.path);
            chain.push_back(file);
            return;
        }
    }

    void ReplicatorRac::logPlanFile(const RedoFile& file) const {
        ctx->info(0, getModeName() + " plan: thread " + std::to_string(file.thread) + " sequence " + file.sequence.toString() + " scn " +
                  file.firstScn.toString() + " to " + (file.growing ? std::string("(growing)") : file.nextScn.toString()) + " blocks " +
                  (file.growing ? std::string("(growing)") : std::to_string(file.numBlocks)) + ": " + file.path +
                  (file.growing ? " (standby log, growing)" : file.standby ? " (standby log)" : ""));
    }

    void ReplicatorRac::planStreams(const std::vector<RedoFile>& inventory, Scn replayScn) {
        std::vector<uint16_t> sortedThreads(threads);
        std::sort(sortedThreads.begin(), sortedThreads.end());

        for (const uint16_t thread: sortedThreads) {
            std::vector<RedoFile> threadFiles;
            std::vector<RedoFile> threadGrowing;
            for (const RedoFile& file: inventory)
                if (file.thread == thread)
                    (file.growing ? threadGrowing : threadFiles).push_back(file);
            if (threadFiles.empty())
                throw RuntimeException(10074, "no archived log of participating thread " + std::to_string(thread) + " in the inventory");

            // Replay starts at the log containing the replay scn (the earlier of data and schema cut), so every transaction committed
            // after the cuts is read from its first record unless it began even earlier; Parser::requireBegin catches that case
            size_t start = threadFiles.size();
            for (size_t i = 0; i < threadFiles.size(); ++i)
                if (threadFiles[i].firstScn <= replayScn)
                    start = i;
            if (start == threadFiles.size())
                throw RuntimeException(10074, "no archived log of thread " + std::to_string(thread) + " covers replay scn " + replayScn.toString() +
                                       ": the earliest, sequence " + threadFiles.front().sequence.toString() + ", starts at scn " +
                                       threadFiles.front().firstScn.toString());
            if (threadFiles[start].nextScn <= replayScn)
                throw RuntimeException(10074, "no archived log of thread " + std::to_string(thread) + " covers replay scn " + replayScn.toString() +
                                       ": sequence " + threadFiles[start].sequence.toString() + " ends at scn " + threadFiles[start].nextScn.toString() +
                                       (start + 1 < threadFiles.size() ? ", sequence " + threadFiles[start + 1].sequence.toString() + " starts at scn " +
                                        threadFiles[start + 1].firstScn.toString() : std::string()));

            ThreadStream stream;
            stream.thread = thread;
            stream.files.push_back(threadFiles[start]);
            // rac-follow waits for a missing sequence instead (extendStreams); a log after it is not used until the chain reaches it
            const RedoFile* gap = extendChain(stream.files, threadFiles);
            if (gap != nullptr) {
                const RedoFile& last = stream.files.back();
                if (!follow)
                    throw RuntimeException(10074, "gap in thread " + std::to_string(thread) + " after sequence " + last.sequence.toString() +
                                           " (next scn " + last.nextScn.toString() + "): sequence " + gap->sequence.toString() + " starts at scn " +
                                           gap->firstScn.toString());
                followGaps += " thread " + std::to_string(thread) + " sequence " + std::to_string(last.sequence.getData() + 1) + " missing, sequence " +
                        gap->sequence.toString() + " present;";
            } else
                appendGrowing(stream.files, threadGrowing);
            // A growing log has no next scn (none): the end scn is the lowest next scn of the chains that end with a complete log
            if (endScn == Scn::none() || stream.files.back().nextScn < endScn)
                endScn = stream.files.back().nextScn;
            stream.bound = stream.files.front().firstScn;
            streams.push_back(std::move(stream));
        }

        if (endScn <= metadata->firstDataScn)
            throw RuntimeException(10074, "common end scn " + endScn.toString() + " of the inventory is not after start scn " +
                                   metadata->firstDataScn.toString());

        for (const ThreadStream& stream: streams) {
            ctx->info(0, getModeName() + " plan: thread " + std::to_string(stream.thread) + " sequence " + stream.files.front().sequence.toString() +
                      " to " + stream.files.back().sequence.toString() + ", scn " + stream.files.front().firstScn.toString() + " to " +
                      (stream.files.back().growing ? std::string("(growing)") : stream.files.back().nextScn.toString()));
            for (const RedoFile& file: stream.files)
                logPlanFile(file);
        }
        ctx->info(0, getModeName() + " plan: replay scn " + replayScn.toString() + ", data cut (commit scn >) " + metadata->firstDataScn.toString() +
                  ", schema cut " + metadata->firstSchemaScn.toString() + ", records merged below end scn " + endScn.toString() +
                  " (exclusive); scalar checkpoint position seq " + metadata->sequence.toString() + " ignored");
        if (!followGaps.empty())
            ctx->info(0, "rac-follow: waiting for a missing sequence:" + followGaps);

        // Merge processing order follows the configured thread list; it must not change the result
        std::vector<ThreadStream> ordered;
        for (const uint16_t thread: threads)
            for (ThreadStream& stream: streams)
                if (stream.thread == thread)
                    ordered.push_back(std::move(stream));
        streams = std::move(ordered);
    }

    // rac-follow: extends every thread's chain with the logs that continue it in a new inventory scan and raises the end scn to the
    // lowest next scn of the chains. All checks pass before any chain changes. Returns true if the end scn moved
    bool ReplicatorRac::extendStreams(const std::vector<RedoFile>& inventory) {
        std::vector<std::vector<RedoFile>> chains;
        std::string gaps;
        std::string consumed;
        Scn newEndScn = Scn::none();
        for (const ThreadStream& stream: streams) {
            std::vector<RedoFile> threadFiles;
            for (const RedoFile& file: inventory)
                if (file.thread == stream.thread)
                    threadFiles.push_back(file);

            // A resume replays the planned logs again, and their records have been merged already: they must stay, unchanged
            chains.push_back(stream.files);
            for (size_t j = 0; j < stream.files.size(); ++j) {
                const RedoFile& planned = stream.files[j];
                auto it = std::find_if(threadFiles.begin(), threadFiles.end(),
                                       [&planned](const RedoFile& file) { return file.sequence == planned.sequence; });
                if (it == threadFiles.end()) {
                    // rac-standby-live: the member being read (or reused while read) holds a newer sequence now; the reader reports the reuse,
                    // and the thread waits with its bound until the archived copy is in the inventory
                    if (standbyLive && planned.standby && j == stream.fileIdx && (stream.parser != nullptr || stream.reuseBlock != 0))
                        continue;
                    // rac-standby-live: a standby log read to the NAB and next scn of its complete header (completeGrowing, or a log planned
                    // complete) is fully merged; its member may be reused before the archived copy is in the inventory. The entry stays as
                    // read, and an archived copy found later must pass the checks below
                    if (standbyLive && planned.standby && !planned.growing && j < stream.fileIdx) {
                        consumed += " thread " + std::to_string(stream.thread) + " sequence " + planned.sequence.toString() + ";";
                        continue;
                    }
                    throw RuntimeException(10074, "thread " + std::to_string(stream.thread) + " sequence " + planned.sequence.toString() +
                                           ", already planned, is no longer in the inventory: " + planned.path);
                }
                // A growing standby log stays the same growing member or becomes complete with the same identity; a complete log has no
                // way back to growing
                if (planned.growing) {
                    if (it->firstScn != planned.firstScn || it->blockSize != planned.blockSize || (it->growing && it->path != planned.path))
                        throw RuntimeException(10074, "conflicting copies of thread " + std::to_string(stream.thread) + " sequence " +
                                               planned.sequence.toString() + ": " + planned.path + " (growing), " + it->path);
                    if (it->growing)
                        continue;
                    ctx->info(0, "rac-follow live: thread " + std::to_string(stream.thread) + " sequence " + planned.sequence.toString() +
                              " is complete in the inventory: next scn " + it->nextScn.toString() + ", blocks " + std::to_string(it->numBlocks) +
                              ": " + it->path);
                    RedoFile& entry = chains.back()[j];
                    if (j > stream.fileIdx || (j == stream.fileIdx && stream.parser == nullptr))
                        entry = *it;
                    else {
                        // Still read from the member opened as growing; the reader must end at this NAB and next scn (completeGrowing)
                        entry.nextScn = it->nextScn;
                        entry.numBlocks = it->numBlocks;
                        entry.growing = false;
                    }
                    continue;
                }
                if (it->growing)
                    throw RuntimeException(10074, "thread " + std::to_string(stream.thread) + " sequence " + planned.sequence.toString() +
                                           ", already planned as a complete log, is growing in the inventory: " + it->path);
                if (it->firstScn != planned.firstScn || it->nextScn != planned.nextScn || it->numBlocks != planned.numBlocks ||
                        it->blockSize != planned.blockSize)
                    throw RuntimeException(10074, "conflicting copies of thread " + std::to_string(stream.thread) + " sequence " +
                                           planned.sequence.toString() + ": " + planned.path + ", " + it->path);
                // A standby log not opened yet is read from its archived copy once that exists
                if (planned.standby && !it->standby && (j > stream.fileIdx || (j == stream.fileIdx && stream.parser == nullptr))) {
                    ctx->info(0, "rac-follow plan: thread " + std::to_string(stream.thread) + " sequence " + planned.sequence.toString() +
                              " now read from the archived copy " + it->path + " instead of " + planned.path);
                    chains.back()[j] = *it;
                }
            }

            const RedoFile* gap = extendChain(chains.back(), threadFiles);
            if (gap != nullptr)
                gaps += " thread " + std::to_string(stream.thread) + " sequence " + std::to_string(chains.back().back().sequence.getData() + 1) +
                        " missing, sequence " + gap->sequence.toString() + " present;";
            else
                appendGrowing(chains.back(), threadFiles);
            if (newEndScn == Scn::none() || chains.back().back().nextScn < newEndScn)
                newEndScn = chains.back().back().nextScn;
        }

        bool added = false;
        for (size_t i = 0; i < streams.size(); ++i) {
            for (size_t j = streams[i].files.size(); j < chains[i].size(); ++j) {
                logPlanFile(chains[i][j]);
                added = true;
            }
            streams[i].files = std::move(chains[i]);
        }
        if (gaps != followGaps) {
            followGaps = gaps;
            ctx->info(0, "rac-follow: waiting for a missing sequence:" + (gaps.empty() ? std::string(" none") : gaps));
        }
        if (consumed != liveConsumed) {
            liveConsumed = consumed;
            ctx->info(0, "rac-follow live: processed standby logs reused before their archived copy is in the inventory:" +
                      (consumed.empty() ? std::string(" none") : consumed));
        }
        // With growing logs the end scn only covers the chains that end with a complete log (none if there is no such chain), so it can
        // also go down when a growing log completes; the bounds of processArchivedRedoLogs hold every record that this could affect
        if (standbyLive && newEndScn != endScn) {
            ctx->info(0, "rac-follow live: end scn of the chains ending with a complete log changed from " + endScn.toString() + " to " +
                      newEndScn.toString());
            endScn = newEndScn;
            return true;
        }
        if (newEndScn > endScn) {
            ctx->info(0, "rac-follow: end scn advanced from " + endScn.toString() + " to " + newEndScn.toString() +
                      ", records merged below end scn " + newEndScn.toString() + " (exclusive)");
            endScn = newEndScn;
            return true;
        }
        if (added)
            ctx->info(0, "rac-follow: end scn stays " + endScn.toString());
        return false;
    }

    void ReplicatorRac::waitForLogs() {
        std::string waiting;
        for (const ThreadStream& stream: streams)
            waiting += " thread " + std::to_string(stream.thread) + " after sequence " + stream.files.back().sequence.toString() + " (next scn " +
                    stream.files.back().nextScn.toString() + ");";
        ctx->info(0, "rac-follow: waiting for closed archived logs, inventory rescan every " + std::to_string(ctx->archReadSleepUs) + " us:" + waiting);

        while (!ctx->softShutdown) {
            contextSet(CONTEXT::SLEEP);
            usleep(ctx->archReadSleepUs);
            contextSet(CONTEXT::CPU);
            if (ctx->softShutdown || extendStreams(collectInventory()))
                return;
        }
    }

    // rac-standby-live: the lowest bound of the threads without a framed head record; no record at or above it may be merged yet
    Scn ReplicatorRac::liveLimit() const {
        Scn limit = Scn::none();
        for (const ThreadStream& stream: streams)
            if (!stream.done && !(stream.framed && stream.pos < stream.parser->lwnSorted.size()) && stream.bound < limit)
                limit = stream.bound;
        return limit;
    }

    // rac-standby-live: nothing can be merged now. Flush, rescan the inventory every "arch-read-sleep-us" (logging the bounds), and sleep
    // "redo-read-sleep-us" before the merge polls the readers again; a thread that stalls keeps its bound, so the merge waits for it
    void ReplicatorRac::waitLive(Scn limit) {
        builder->flush();
        const time_ut now = ctx->clock->getTimeUt();
        if (now - liveScanTime >= static_cast<time_ut>(ctx->archReadSleepUs)) {
            liveScanTime = now;
            std::string state;
            for (const ThreadStream& stream: streams) {
                state += " thread " + std::to_string(stream.thread) + " bound " + stream.bound.toString() + " (";
                if (stream.framed && stream.pos < stream.parser->lwnSorted.size())
                    state += "head scn " + stream.parser->lwnSorted[stream.pos]->scn.toString();
                else if (stream.parser != nullptr)
                    state += "sequence " + stream.files[stream.fileIdx].sequence.toString() + (stream.live ? " growing" : "") + ", " +
                            stream.parser->framePosition();
                else if (stream.reuseBlock != 0)
                    state += "sequence " + stream.files[stream.fileIdx].sequence.toString() + " reused, continues at block " +
                            std::to_string(stream.reuseBlock) + (stream.files[stream.fileIdx].standby ?
                                                                 std::string(" of its archived log, not in the inventory yet") :
                                                                 ": " + stream.files[stream.fileIdx].path);
                else
                    state += "after sequence " + stream.files.back().sequence.toString();
                state += ", lwns " + std::to_string(stream.lwns) + ", lwn scn " + stream.lastLwnScn.toString() + ");";
            }
            ctx->info(0, "rac-follow live: release limit " + limit.toString() + ", end scn " + endScn.toString() + ", merged " +
                      std::to_string(liveReleased) + " records up to scn " + liveReleasedScn.toString() + ";" + state);
            extendStreams(collectInventory());
        }
        contextSet(CONTEXT::SLEEP);
        usleep(ctx->redoReadSleepUs);
        contextSet(CONTEXT::CPU);
    }

    void ReplicatorRac::openFile(ThreadStream& stream) {
        const RedoFile& file = stream.files[stream.fileIdx];
        // A growing standby log is read with online redo log semantics: a block of an older sequence is not written yet, a block of a
        // newer one means the member was reused (OVERWRITTEN, which stops), and with "redo-verify-delay-us" a block failing its checksum
        // is read again later instead of failing; good blocks become visible only after a second, delayed read
        stream.reader->setGroup(file.growing ? 1 : 0);
        stream.reader->fileName = file.path;
        if (!stream.reader->checkRedoLog() || !stream.reader->updateRedoLog())
            throw RuntimeException(10009, "file: " + file.path + " - failed to open");

        // The header the reader itself opened and checked (header block checksum and number) must be the planned log: a file or member
        // replaced after the inventory scan with the same sequence and scn range but another database, thread or size is refused here.
        // A log planned as growing may have become complete since the scan, with the same identity
        const Reader* reader = stream.reader;
        const bool readerComplete = reader->getNextScn() != Scn::none() && reader->getNumBlocks() != 0 && reader->getNumBlocks() != Ctx::ZERO_BLK;
        const bool sameRange = file.growing ? (reader->getNextScn() == Scn::none() || readerComplete) :
                (reader->getNextScn() == file.nextScn && reader->getNumBlocks() == file.numBlocks);
        if (reader->getDbid() != file.dbid || reader->getThread() != file.thread || reader->getSequence() != file.sequence ||
                reader->getResetlogs() != file.resetlogs || reader->getActivation() != file.activation || reader->getBlockSize() != file.blockSize ||
                reader->getFirstScn() != file.firstScn || !sameRange)
            throw RuntimeException(10074, "file: " + file.path + " - header changed after inventory: opened dbid " + std::to_string(reader->getDbid()) +
                                   " thread " + std::to_string(reader->getThread()) + " sequence " + reader->getSequence().toString() + " resetlogs " +
                                   std::to_string(reader->getResetlogs()) + " activation " + std::to_string(reader->getActivation()) + " block size " +
                                   std::to_string(reader->getBlockSize()) + " scn " + reader->getFirstScn().toString() + " to " +
                                   reader->getNextScn().toString() + " blocks " + std::to_string(reader->getNumBlocks()) + ", planned dbid " +
                                   std::to_string(file.dbid) + " thread " + std::to_string(file.thread) + " sequence " + file.sequence.toString() +
                                   " resetlogs " + std::to_string(file.resetlogs) + " activation " + std::to_string(file.activation) + " block size " +
                                   std::to_string(file.blockSize) + " scn " + file.firstScn.toString() + " to " +
                                   (file.growing ? std::string("(growing)") : file.nextScn.toString() + " blocks " + std::to_string(file.numBlocks)));

        // rac-standby-live: the sequence of a member reused while read continues in its archived copy (identity checked above) at the first
        // block of the first LWN that was not returned; that block is at most the copy's NAB (equal: every LWN was read already, the copy
        // ends at once), and the thread's bound must not be above its next scn
        const typeBlk startBlock = stream.reuseBlock != 0 ? stream.reuseBlock : 2;
        if (stream.reuseBlock != 0) {
            if (stream.reuseBlock > file.numBlocks || file.nextScn < stream.bound)
                throw RuntimeException(10074, "file: " + file.path + " - archived log of thread " + std::to_string(stream.thread) + " sequence " +
                                       file.sequence.toString() + " ends at block " + std::to_string(file.numBlocks) + ", next scn " +
                                       file.nextScn.toString() + ": it does not contain the continuation of the reused standby log at block " +
                                       std::to_string(stream.reuseBlock) + ", bound scn " + stream.bound.toString());
            ctx->info(0, "rac-follow live: thread " + std::to_string(stream.thread) + " sequence " + file.sequence.toString() +
                      " continues from the archived log at block " + std::to_string(stream.reuseBlock) + " (next scn " + file.nextScn.toString() +
                      ", blocks " + std::to_string(file.numBlocks) + "): " + file.path);
            stream.reuseBlock = 0;
        }

        stream.live = file.growing;
        stream.parser = new Parser(ctx, builder, metadata, transactionBuffer, 0, file.path);
        stream.parser->reader = stream.reader;
        stream.parser->sequence = file.sequence;
        stream.parser->requireBegin = true;
        stream.parser->frameStart(startBlock);
    }

    // A log opened as growing was read to the NAB of its now complete header: the reader's end is the log's next scn and NAB, which must
    // equal a complete copy already seen in the inventory
    void ReplicatorRac::completeGrowing(ThreadStream& stream) {
        RedoFile& file = stream.files[stream.fileIdx];
        const Scn readerNextScn = stream.reader->getNextScn();
        const typeBlk readerBlocks = stream.reader->getNumBlocks();
        if (!file.growing && (file.nextScn != readerNextScn || file.numBlocks != readerBlocks))
            throw RuntimeException(10074, "conflicting copies of thread " + std::to_string(stream.thread) + " sequence " + file.sequence.toString() +
                                   ": " + file.path + " was read to next scn " + readerNextScn.toString() + ", blocks " + std::to_string(readerBlocks) +
                                   ", the inventory has next scn " + file.nextScn.toString() + ", blocks " + std::to_string(file.numBlocks));
        file.nextScn = readerNextScn;
        file.numBlocks = readerBlocks;
        file.growing = false;
        stream.live = false;
        ctx->info(0, "rac-follow live: thread " + std::to_string(stream.thread) + " sequence " + file.sequence.toString() +
                  " completed while read: next scn " + readerNextScn.toString() + ", blocks " + std::to_string(readerBlocks) + ": " + file.path);
    }

    void ReplicatorRac::closeFile(ThreadStream& stream) {
        if (stream.framed) {
            stream.parser->releaseFramedLwn();
            stream.framed = false;
        }
        delete stream.parser;
        stream.parser = nullptr;
        builder->flush();
    }

    void ReplicatorRac::checkLwn(ThreadStream& stream) const {
        const RedoFile& file = stream.files[stream.fileIdx];
        const std::vector<LwnMember*>& members = stream.parser->lwnSorted;
        for (const LwnMember* member: members)
            if (member->scn < file.firstScn || member->scn > file.nextScn)
                throw RuntimeException(10075, "thread " + std::to_string(stream.thread) + " sequence " + file.sequence.toString() + " block " +
                                       std::to_string(member->block) + " offset " + std::to_string(member->pageOffset) + ": record scn " +
                                       member->scn.toString() + " outside log range " + file.firstScn.toString() + " to " + file.nextScn.toString());
        // The merge takes LWNs of one thread in log order; that is a global scn order only if no record of a later LWN is below the
        // records of an earlier one
        if (!members.empty() && members.front()->scn < stream.lastScn)
            throw RuntimeException(10075, "thread " + std::to_string(stream.thread) + " sequence " + file.sequence.toString() + " block " +
                                   std::to_string(members.front()->block) + " offset " + std::to_string(members.front()->pageOffset) +
                                   ": record scn " + members.front()->scn.toString() + " below scn " + stream.lastScn.toString() +
                                   " of an earlier lwn of the thread");
        if (!standbyLive)
            return;
        // The live bound of a thread is the scn of its last complete LWN. That is a lower bound of the thread's later records only if LWN
        // scns never go down and no record is below the scn of its LWN. Both are checked here, after the fact: a violation stops
        // processing, but records released before it was seen stay released
        const Scn lwnScn = stream.parser->frameLwnScn;
        if (lwnScn < stream.lastLwnScn)
            throw RuntimeException(10075, "thread " + std::to_string(stream.thread) + " sequence " + file.sequence.toString() + ": lwn scn " +
                                   lwnScn.toString() + " below scn " + stream.lastLwnScn.toString() + " of the previous lwn of the thread");
        for (const LwnMember* member: members)
            if (member->scn < lwnScn)
                throw RuntimeException(10075, "thread " + std::to_string(stream.thread) + " sequence " + file.sequence.toString() + " block " +
                                       std::to_string(member->block) + " offset " + std::to_string(member->pageOffset) + ": record scn " +
                                       member->scn.toString() + " below scn " + lwnScn.toString() + " of its lwn");
    }

    // Returns true when the thread's next record is below the end scn. rac-follow keeps a framed LWN whose next record is at or above
    // the end scn, and the position after the last planned log, until extendStreams raises the end scn or adds logs
    bool ReplicatorRac::advance(ThreadStream& stream) {
        while (!stream.done && !ctx->softShutdown) {
            if (stream.framed && stream.pos < stream.parser->lwnSorted.size()) {
                if (stream.parser->lwnSorted[stream.pos]->scn < endScn)
                    return true;
                // Records of a thread are in scn order (checkLwn), so nothing else of this thread is below the end scn
                if (follow)
                    return false;
                stream.done = true;
                break;
            }

            if (stream.framed) {
                stream.parser->releaseFramedLwn();
                stream.framed = false;
            }

            if (stream.parser == nullptr) {
                if (stream.fileIdx == stream.files.size())
                    return false;
                // Only an archived copy (found by extendStreams) continues a reused member's sequence; no other log replaces it
                if (stream.reuseBlock != 0 && stream.files[stream.fileIdx].standby)
                    return false;
                openFile(stream);
            }

            // A growing log is framed without waiting: PENDING keeps a partly framed LWN (and record) inside the parser, unreleased and
            // unparsed, and the next call continues with the next block
            const Parser::FRAME frame = stream.parser->frameNextLwn(!stream.live);
            if (frame == Parser::FRAME::LWN) {
                checkLwn(stream);
                if (!stream.parser->lwnSorted.empty())
                    stream.lastScn = stream.parser->lwnSorted.back()->scn;
                if (standbyLive) {
                    stream.lastLwnScn = stream.parser->frameLwnScn;
                    stream.bound = stream.parser->frameLwnScn;
                }
                stream.records += stream.parser->lwnSorted.size();
                ++stream.lwns;
                stream.framed = true;
                stream.pos = 0;
                continue;
            }
            if (frame == Parser::FRAME::PENDING)
                return false;
            if (frame == Parser::FRAME::REUSED) {
                // The reader reports a block or header of a later sequence before its block number and checksum are checked, so a corrupt
                // sequence field looks the same; only the member's valid header (readHeader checks it) with a later sequence of the same
                // database and thread shows a reuse
                const RedoFile& file = stream.files[stream.fileIdx];
                const RedoFile member = readHeader(file.path);
                if (member.dbid != file.dbid || member.thread != file.thread || member.resetlogs != file.resetlogs ||
                        member.activation != file.activation || member.sequence <= file.sequence)
                    throw RuntimeException(10074, "file: " + file.path + " - reading stopped with OVERWRITTEN (" + stream.parser->framePosition() +
                                           "), but its header shows dbid " + std::to_string(member.dbid) + " thread " + std::to_string(member.thread) +
                                           " sequence " + member.sequence.toString() + ": not a reuse of the standby log of thread " +
                                           std::to_string(file.thread) + " sequence " + file.sequence.toString());
                // Every returned LWN is released and analyzed, the partly framed one was never returned: closing the parser drops only that,
                // and the archived copy is framed again from its first block. The bound, scn checks and merge state stay
                stream.reuseBlock = stream.parser->frameResumeBlock();
                ctx->info(0, "rac-follow live: thread " + std::to_string(stream.thread) + " sequence " + stream.files[stream.fileIdx].sequence.toString() +
                          ": standby log reused while read (" + stream.parser->framePosition() + "), continues at block " +
                          std::to_string(stream.reuseBlock) + " of its archived log; bound stays " + stream.bound.toString() + ": " +
                          stream.files[stream.fileIdx].path);
                closeFile(stream);
                continue;
            }
            if (ctx->softShutdown)
                break;

            if (stream.live)
                completeGrowing(stream);
            if (standbyLive)
                stream.bound = stream.files[stream.fileIdx].nextScn;
            closeFile(stream);
            if (++stream.fileIdx == stream.files.size() && !follow) {
                stream.done = true;
                break;
            }
        }

        if (stream.done && stream.parser != nullptr)
            closeFile(stream);
        return false;
    }

    bool ReplicatorRac::processArchivedRedoLogs() {
        if (processed)
            return false;
        processed = true;

        if (metadata->firstDataScn == Scn::none() || metadata->firstSchemaScn == Scn::none())
            throw RuntimeException(10074, "rac-batch mode requires \"start-scn\" and a schema checkpoint");

        // Resume is a full replay of the same plan from the original bootstrap; the writer then suppresses every message up to the
        // acknowledged cursor
        if (metadata->clientScn != Scn::none()) {
            if (metadata->firstSchemaScn != metadata->clientSchemaScn)
                throw RuntimeException(10077, "acknowledged rac-batch cursor was produced with schema scn " + metadata->clientSchemaScn.toString() +
                                       ", but the schema checkpoint at or below first data scn " + metadata->firstDataScn.toString() +
                                       " is now " + metadata->firstSchemaScn.toString() + "; restore that checkpoint or start a new replication");
            ctx->info(0, getModeName() + " resume: full replay with first data scn: " + metadata->firstDataScn.toString() + ", schema scn: " +
                      metadata->firstSchemaScn.toString() + ", acknowledged scn: " + metadata->clientScn.toString() + ", idx: " +
                      std::to_string(metadata->clientIdx));
        }

        const Scn replayScn = std::min(metadata->firstDataScn, metadata->firstSchemaScn);
        planStreams(collectInventory(), replayScn);

        // Every concurrently read thread gets an equal part of the read buffer pool, minus two chunks for a partially consumed first
        // chunk and a read that ends past the cap
        const uint64_t chunksPerReader = ctx->memoryChunksReadBufferMax / streams.size();
        if (chunksPerReader < 3)
            throw RuntimeException(10074, "read buffer of " + std::to_string(ctx->memoryChunksReadBufferMax) + " MB too small for " +
                                   std::to_string(streams.size()) + " threads");
        for (ThreadStream& stream: streams) {
            auto* reader = new ReaderFilesystem(ctx, alias + "-reader-t" + std::to_string(stream.thread), database, 0,
                                                metadata->dbBlockChecksum != "OFF" && metadata->dbBlockChecksum != "FALSE");
            readers.insert(reader);
            reader->initialize();
            reader->readAheadMax = (chunksPerReader - 2) * Ctx::MEMORY_CHUNK_SIZE;
            ctx->spawnThread(reader);
            stream.reader = reader;
        }
        for (ThreadStream& stream: streams)
            openFile(stream);

        // Merge: repeatedly analyze the smallest head record; heads are compared by record scn, ties between threads go to the lower
        // thread number. Within a thread the production LWN order is kept unchanged. Cursor groups (c_scn) are record scns.
        // rac-standby-live (experimental): a thread reading its growing standby log may have no complete LWN to offer. The smallest head
        // is then merged only if it is below the bound of every thread without a head (liveLimit, exclusive, so the tie rule is
        // unchanged): the scn of that thread's last complete LWN, its growing log's first scn before the first LWN, or the next scn of a
        // log it read to the NAB. A missing or stalled thread keeps its bound; nothing advances it by timeout, end of file or another
        // thread's progress. The bound is only an assumption: it needs LWN scns that never go down and records never below their LWN's
        // scn (checked in checkLwn), and, like the closed merge, that the records of a later LWN of a thread are not below those of an
        // earlier one (checked when they arrive). All checks happen after the fact; a violation stops processing but cannot take back
        // records already merged.
        uint16_t prevThread = 0;
        Scn groupScn = Scn::none();
        uint16_t groupThread = 0;
        bool groupShared = false;
        uint64_t groups = 0;
        uint64_t sharedGroups = 0;
        while (!ctx->softShutdown) {
            ThreadStream* best = nullptr;
            Scn bestScn = Scn::none();
            for (ThreadStream& stream: streams) {
                if (!advance(stream))
                    continue;
                const Scn scn = stream.parser->lwnSorted[stream.pos]->scn;
                if (best == nullptr || scn < bestScn || (scn == bestScn && stream.thread < best->thread)) {
                    best = &stream;
                    bestScn = scn;
                }
            }
            if (standbyLive) {
                const Scn limit = liveLimit();
                if (best == nullptr || !(bestScn < limit)) {
                    if (ctx->softShutdown)
                        break;
                    waitLive(limit);
                    continue;
                }
            }
            if (best == nullptr) {
                if (!follow || ctx->softShutdown)
                    break;
                // Every record below the end scn is merged. The framed records, read positions, open transactions and this merge's
                // group state stay as they are, so the stream continues exactly as one merge over the extended inventory would
                builder->flush();
                for (const ThreadStream& stream: streams)
                    ctx->info(0, "rac-follow merged thread " + std::to_string(stream.thread) + ": lwns " + std::to_string(stream.lwns) +
                              ", records framed " + std::to_string(stream.records) + ", analyzed " + std::to_string(stream.applied) +
                              ", last framed scn " + stream.lastScn.toString());
                ctx->info(0, "rac-follow: all records below end scn " + endScn.toString() + " merged: " + std::to_string(groups) +
                          " record scn groups, " + std::to_string(sharedGroups) + " with records of several threads; transactions open at the" +
                          " end scn stay open");
                waitForLogs();
                continue;
            }

            LwnMember* member = best->parser->lwnSorted[best->pos++];
            if (member->scn != groupScn) {
                groupScn = member->scn;
                groupThread = best->thread;
                groupShared = false;
                ++groups;
            } else if (!groupShared && best->thread != groupThread) {
                groupShared = true;
                ++sharedGroups;
            }

            best->parser->analyzeFramed(member, groupScn, best->thread != prevThread);
            prevThread = best->thread;
            ++best->applied;
            ++liveReleased;
            liveReleasedScn = member->scn;
        }

        for (ThreadStream& stream: streams)
            if (stream.parser != nullptr)
                closeFile(stream);
        builder->flush();

        if (ctx->softShutdown)
            return true;
        for (const ThreadStream& stream: streams)
            ctx->info(0, "rac-batch merged thread " + std::to_string(stream.thread) + ": lwns " + std::to_string(stream.lwns) + ", records framed " +
                      std::to_string(stream.records) + ", analyzed " + std::to_string(stream.applied) + ", last framed scn " + stream.lastScn.toString());
        ctx->info(0, "rac-batch merge finished below end scn " + endScn.toString() + ": " + std::to_string(groups) + " record scn groups, " +
                  std::to_string(sharedGroups) + " with records of several threads; transactions still open at the end scn were not emitted");
        return true;
    }
}
