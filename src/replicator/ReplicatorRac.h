/* Header for ReplicatorRac class
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

This program is free software: you can redistribute it and/or
modify it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public
License along with this program; see the file LICENSE;
If not, see <http://www.gnu.org/licenses/>. */

#ifndef REPLICATOR_RAC_H_
#define REPLICATOR_RAC_H_

#include <map>
#include <set>
#include <vector>

#include "Replicator.h"

namespace OpenLogReplicator {
    // Experimental: merges closed archived redo logs of several RAC threads record by record into one Parser/TransactionBuffer/Builder
    // pipeline; a resume replays everything again from the original bootstrap. "rac-batch" processes a finite inventory and ends.
    // "rac-follow" keeps the process running at the end scn and rescans the inventory for further closed archived logs; it never reads
    // online redo logs. Standby redo logs listed in "rac-standby-log" are used only once they are complete, like archived logs.
    // Experimental, opt-in "rac-standby-live" (rac-follow only): the growing standby redo log that continues each thread's contiguous
    // chain is read too, without blocking the other threads; see processArchivedRedoLogs for the release rule and its assumptions.
    class ReplicatorRac : public Replicator {
    protected:
        struct RedoFile {
            std::string path;
            uint64_t fileSize{0};
            uint blockSize{0};
            uint32_t dbid{0};
            uint32_t compatVsn{0};
            uint16_t thread{0};
            Seq sequence{Seq::zero()};
            typeResetlogs resetlogs{0};
            typeActivation activation{0};
            typeBlk numBlocks{0};
            Scn firstScn{Scn::none()};
            Scn nextScn{Scn::none()};
            // From "rac-standby-log": a mutable standby redo log member, used only while its header shows a complete log
            bool standby{false};
            // rac-standby-live: a standby redo log member still receiving redo (no NAB and next scn in the header yet)
            bool growing{false};
        };

        struct ThreadStream {
            uint16_t thread{0};
            std::vector<RedoFile> files;
            size_t fileIdx{0};
            Reader* reader{nullptr};
            Parser* parser{nullptr};
            size_t pos{0};
            bool framed{false};
            bool done{false};
            // The open log was growing when it was planned: framed without waiting, a block not written yet is PENDING
            bool live{false};
            // rac-standby-live: the growing member was reused while read; its sequence continues at this block of the archived copy
            typeBlk reuseBlock{0};
            Scn lastScn{Scn::zero()};
            // rac-standby-live: assumed lower bound of every record of the thread not framed yet (see processArchivedRedoLogs): the first
            // scn of the first log, then the scn of the last complete LWN, and the next scn of a log read to its NAB
            Scn bound{Scn::zero()};
            Scn lastLwnScn{Scn::zero()};
            uint64_t lwns{0};
            uint64_t records{0};
            uint64_t applied{0};
        };

        std::vector<uint16_t> threads;
        uint32_t dbid;
        bool follow;
        bool processed{false};
        Scn endScn{Scn::none()};
        std::vector<ThreadStream> streams;
        // rac-follow: last logged list of threads whose chain stops at a missing sequence
        std::string followGaps;
        // rac-standby-live: last logged list of processed standby logs whose sequence is in no member or archived log of the inventory
        std::string liveConsumed;
        // Last logged state of every standby redo log member and the identical copies already logged that involve one
        std::map<std::string, std::string> standbyStates;
        std::set<std::string> standbyCopies;
        // rac-standby-live: time of the last inventory scan, records merged and the scn of the last one
        time_ut liveScanTime{0};
        uint64_t liveReleased{0};
        Scn liveReleasedScn{Scn::zero()};

        std::string getModeName() const override;
        bool continueWithOnline() override;
        void positionReader() override;
        void createSchema() override;
        void updateOnlineRedoLogData() override;
        void loadDatabaseMetadata() override;

        RedoFile readHeader(const std::string& path) const;
        bool sameContent(const RedoFile& file1, const RedoFile& file2, uint64_t from) const;
        bool sameStandbyCopy(const RedoFile& file1, const RedoFile& file2) const;
        virtual std::vector<RedoFile> collectInventory();
        static const RedoFile* extendChain(std::vector<RedoFile>& chain, const std::vector<RedoFile>& threadFiles);
        void appendGrowing(std::vector<RedoFile>& chain, const std::vector<RedoFile>& threadFiles) const;
        void logPlanFile(const RedoFile& file) const;
        void planStreams(const std::vector<RedoFile>& inventory, Scn replayScn);
        bool extendStreams(const std::vector<RedoFile>& inventory);
        void waitForLogs();
        Scn liveLimit() const;
        void waitLive(Scn limit);
        void openFile(ThreadStream& stream);
        void completeGrowing(ThreadStream& stream);
        void closeFile(ThreadStream& stream);
        void checkLwn(ThreadStream& stream) const;
        bool advance(ThreadStream& stream);

    public:
        // A stream writer stops reading client requests and sending messages at a soft shutdown, so with one the program
        // stays up after the plan is replayed until it is stopped
        bool keepRunningAtEnd{false};
        // "rac-standby-log": standby redo log members; their content changes, so they are classified again at every inventory scan
        std::vector<std::string> standbyLogs;
        // "rac-standby-live" (rac-follow only, needs "redo-verify-delay-us" > 0)
        bool standbyLive{false};
        bool startupPlanned{false};

        ReplicatorRac(Ctx* newCtx, void (*newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
                      TransactionBuffer* newTransactionBuffer, std::string newAlias, std::string newDatabase, std::vector<uint16_t> newThreads,
                      uint32_t newDbid, bool newFollow);
        ~ReplicatorRac() override;

        bool processArchivedRedoLogs() override;
    };
}

#endif
