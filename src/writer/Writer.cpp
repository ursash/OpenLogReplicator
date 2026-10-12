/* Base class for thread to write output
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
<http:////www.gnu.org/licenses/>.  */

#include <algorithm>
#include <thread>
#include <utility>
#include <unistd.h>

#include "../builder/Builder.h"
#include "../common/Ctx.h"
#include "../common/exception/DataException.h"
#include "../common/exception/NetworkException.h"
#include "../common/exception/RuntimeException.h"
#include "../common/metrics/Metrics.h"
#include "../metadata/Metadata.h"
#include "Writer.h"

namespace OpenLogReplicator {
    Writer::Writer(Ctx* newCtx, std::string newAlias, std::string newDatabase, Builder* newBuilder, Metadata* newMetadata):
            Thread(newCtx, std::move(newAlias)),
            database(std::move(newDatabase)),
            builder(newBuilder),
            metadata(newMetadata),
            checkpointTime(time(nullptr)) {
        ctx->writerThread = this;
    }

    Writer::~Writer() {
        delete[] queue;
        queue = nullptr;
    }

    void Writer::initialize() {
        if (queue != nullptr)
            return;
        queue = new BuilderMsg*[ctx->queueSize];
    }

    void Writer::createMessage(BuilderMsg* msg) {
        ++sentMessages;

        // After a rewind a message confirmed before the client reconnected can be queued again; it must wait for a new confirm
        msg->unsetFlag(BuilderMsg::OUTPUT_BUFFER::CONFIRMED);
        queue[currentQueueSize++] = msg;
        hwmQueueSize = std::max(currentQueueSize, hwmQueueSize);
    }

    void Writer::sortQueue() {
        if (currentQueueSize == 0)
            return;

        BuilderMsg** oldQueue = queue;
        queue = new BuilderMsg*[ctx->queueSize];
        uint64_t oldQueueSize = currentQueueSize;

        for (uint64_t newId = 0; newId < currentQueueSize; ++newId) {
            queue[newId] = oldQueue[0];
            uint64_t i = 0;
            --oldQueueSize;
            while (i < oldQueueSize) {
                if (i * 2 + 2 < oldQueueSize && oldQueue[(i * 2) + 2]->id < oldQueue[oldQueueSize]->id) {
                    if (oldQueue[(i * 2) + 1]->id < oldQueue[(i * 2) + 2]->id) {
                        oldQueue[i] = oldQueue[(i * 2) + 1];
                        i = i * 2 + 1;
                    } else {
                        oldQueue[i] = oldQueue[(i * 2) + 2];
                        i = i * 2 + 2;
                    }
                } else if (i * 2 + 1 < oldQueueSize && oldQueue[(i * 2) + 1]->id < oldQueue[oldQueueSize]->id) {
                    oldQueue[i] = oldQueue[(i * 2) + 1];
                    i = i * 2 + 1;
                } else
                    break;
            }
            oldQueue[i] = oldQueue[oldQueueSize];
        }

        delete[] oldQueue;
    }

    // Checks, before any client state changes, that every message after the cursor (scn, idx) can still be resent; an unset
    // cursor (Scn::none()) asks for everything. On success replayQueue/replayStart are where resetMessageQueue() must restart.
    bool Writer::checkReplay(Scn scn, typeIdx idx, BuilderQueue*& replayQueue, uint64_t& replayStart, std::string& reason) {
        bool floorSet;
        Scn floorScn;
        typeIdx floorIdx;
        if (!builder->getReplayStart(this, replayQueue, replayStart, floorSet, floorScn, floorIdx)) {
            reason = "the replay start of the first retained buffer (id: " + std::to_string(replayQueue->id) + ") is not known yet";
            return false;
        }
        if (floorSet && (scn == Scn::none() || scn < floorScn || (scn == floorScn && idx < floorIdx))) {
            reason = "output up to scn: " + floorScn.toString() + ", idx: " + std::to_string(floorIdx) +
                    " can no longer be resent (first retained buffer id: " + std::to_string(replayQueue->id) + ")";
            return false;
        }
        return true;
    }

    void Writer::resetMessageQueue(BuilderQueue* replayQueue, uint64_t replayStart) {
        for (uint64_t i = 0; i < currentQueueSize; ++i) {
            BuilderMsg* msg = queue[i];
            if (msg->isFlagSet(BuilderMsg::OUTPUT_BUFFER::ALLOCATED))
                delete[] msg->data;
        }
        currentQueueSize = 0;

        // Sent but unconfirmed messages can lie in earlier buffers than the current one; only buffers that hold nothing
        // but confirmed messages have been released, so the first retained buffer is where resending has to start
        builderQueue = replayQueue;
        oldSize = replayStart;
    }

    void Writer::confirmMessage(BuilderMsg* msg) {
        if (ctx->metrics != nullptr && msg != nullptr) {
            ctx->metrics->emitBytesConfirmed(msg->size);
            ctx->metrics->emitMessagesConfirmed(1);
        }

        contextSet(CONTEXT::MUTEX, REASON::WRITER_CONFIRM);
        std::unique_lock const lck(mtx);

        if (msg == nullptr) {
            if (currentQueueSize == 0) {
                ctx->warning(70007, "trying to confirm an empty message");
                contextSet(CONTEXT::CPU);
                return;
            }
            msg = queue[0];
        }

        msg->setFlag(BuilderMsg::OUTPUT_BUFFER::CONFIRMED);
        if (msg->isFlagSet(BuilderMsg::OUTPUT_BUFFER::ALLOCATED)) {
            delete[] msg->data;
            msg->unsetFlag(BuilderMsg::OUTPUT_BUFFER::ALLOCATED);
        }

        uint64_t maxId = 0;
        {
            while (currentQueueSize > 0 && queue[0]->isFlagSet(BuilderMsg::OUTPUT_BUFFER::CONFIRMED)) {
                maxId = queue[0]->queueId;
                if (confirmedScn == Scn::none() || msg->lwnScn > confirmedScn) {
                    confirmedScn = msg->lwnScn;
                    confirmedIdx = msg->lwnIdx;
                } else if (msg->lwnScn == confirmedScn && msg->lwnIdx > confirmedIdx)
                    confirmedIdx = msg->lwnIdx;

                if (--currentQueueSize == 0)
                    break;

                uint64_t i = 0;
                while (i < currentQueueSize) {
                    if ((i * 2) + 2 < currentQueueSize && queue[(i * 2) + 2]->id < queue[currentQueueSize]->id) {
                        if (queue[(i * 2) + 1]->id < queue[(i * 2) + 2]->id) {
                            queue[i] = queue[(i * 2) + 1];
                            i = (i * 2) + 1;
                        } else {
                            queue[i] = queue[(i * 2) + 2];
                            i = (i * 2) + 2;
                        }
                    } else if ((i * 2) + 1 < currentQueueSize && queue[(i * 2) + 1]->id < queue[currentQueueSize]->id) {
                        queue[i] = queue[(i * 2) + 1];
                        i = (i * 2) + 1;
                    } else
                        break;
                }
                queue[i] = queue[currentQueueSize];
            }
        }

        builder->releaseBuffers(this, maxId);
        contextSet(CONTEXT::CPU);
    }

    void Writer::run() {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "writer (" + ss.str() + ") start");
        }

        ctx->info(0, "writer is starting with " + getName());

        try {
            // Before anything, read the latest checkpoint
            readCheckpoint();
            builderQueue = builder->firstBuilderQueue;
            oldSize = 0;
            currentQueueSize = 0;

            // External loop for client disconnection
            while (!ctx->hardShutdown) {
                try {
                    mainLoop();

                    // Client disconnected
                } catch (NetworkException& ex) {
                    ctx->warning(ex.code, ex.msg);
                    streaming = false;
                }

                if (ctx->softShutdown && ctx->replicatorFinished)
                    break;
            }
        } catch (DataException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        }

        ctx->info(0, "writer is stopping: " + getType() + ", hwm queue size: " + std::to_string(hwmQueueSize));
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "writer (" + ss.str() + ") stop");
        }
    }

    void Writer::mainLoop() {
        BuilderMsg* msg{nullptr};
        uint64_t newSize = 0;
        currentQueueSize = 0;

        // Start streaming
        while (!ctx->hardShutdown) {
            // Check if the writer has a receiver of data which defined starting point of replication
            while (!ctx->hardShutdown) {
                pollQueue();

                if (streaming && metadata->status == Metadata::STATUS::REPLICATE)
                    break;

                if (unlikely(ctx->isTraceSet(Ctx::TRACE::WRITER)))
                    ctx->logTrace(Ctx::TRACE::WRITER, "waiting for client");
                contextSet(CONTEXT::SLEEP);
                usleep(ctx->pollIntervalUs);
                contextSet(CONTEXT::CPU);
            }

            // Get a message to send
            while (!ctx->hardShutdown) {
                // Verify sent messages, check what client receives
                pollQueue();

                // Update checkpoint
                writeCheckpoint(redo);

                // Next buffer
                if (builderQueue->next != nullptr)
                    if (builderQueue->confirmedSize == oldSize) {
                        builderQueue = builderQueue->next;
                        oldSize = 0;
                    }

                // Found something
                msg = reinterpret_cast<BuilderMsg*>(builderQueue->data + oldSize);
                if (builderQueue->confirmedSize > oldSize + sizeof(BuilderMsg) && msg->size > 0) {
                    newSize = builderQueue->confirmedSize;
                    break;
                }

                if (ctx->softShutdown && ctx->replicatorFinished)
                    break;
                builder->sleepForWriterWork(this, currentQueueSize, ctx->pollIntervalUs);
            }

            __builtin_prefetch(reinterpret_cast<char*>(msg), 0, 0);
            __builtin_prefetch(reinterpret_cast<char*>(msg) + 64, 0, 0);
            __builtin_prefetch(reinterpret_cast<char*>(msg) + 128, 0, 0);
            __builtin_prefetch(reinterpret_cast<char*>(msg) + 192, 0, 0);
            // Send the message
            while (oldSize + sizeof(BuilderMsg) < newSize && !ctx->hardShutdown) {
                msg = reinterpret_cast<BuilderMsg*>(builderQueue->data + oldSize);
                if (msg->size == 0)
                    break;

                // The queue is full
                pollQueue();
                while (currentQueueSize >= ctx->queueSize && !ctx->hardShutdown) {
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::WRITER)))
                        ctx->logTrace(Ctx::TRACE::WRITER, "output queue is full (" + std::to_string(currentQueueSize) +
                                      " elements), sleeping " + std::to_string(ctx->pollIntervalUs) + "us");
                    contextSet(CONTEXT::SLEEP);
                    usleep(ctx->pollIntervalUs);
                    contextSet(CONTEXT::CPU);
                    pollQueue();
                }

                writeCheckpoint(redo);
                if (ctx->hardShutdown)
                    break;

                const uint64_t size8 = (msg->size + 7) & 0xFFFFFFFFFFFFFFF8;
                oldSize += sizeof(BuilderMsg);

                // Message in one part - sent directly from buffer
                if (oldSize + size8 <= Builder::OUTPUT_BUFFER_DATA_SIZE) {
                    createMessage(msg);
                    if (msg->isFlagSet(BuilderMsg::OUTPUT_BUFFER::REDO))
                        redo = true;
                    // Send the message to the client in one part
                    if ((msg->isFlagSet(BuilderMsg::OUTPUT_BUFFER::CHECKPOINT) && !ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_CHECKPOINT)) ||
                        !metadata->isNewData(msg->lwnScn, msg->lwnIdx))
                        confirmMessage(msg);
                    else {
                        const uint64_t msgSize = msg->size;
                        sendMessage(msg);
                        if (ctx->metrics != nullptr) {
                            ctx->metrics->emitBytesSent(msgSize);
                            ctx->metrics->emitMessagesSent(1);
                        }
                    }
                    oldSize += size8;
                } else {
                    // The message is split to many parts - merge and copy
                    msg->data = new uint8_t[msg->size];
                    if (unlikely(msg->data == nullptr))
                        throw RuntimeException(10016, "couldn't allocate " + std::to_string(msg->size) +
                                               " bytes memory for: temporary buffer for JSON message");
                    msg->setFlag(BuilderMsg::OUTPUT_BUFFER::ALLOCATED);

                    uint64_t copied = 0;
                    while (msg->size > copied) {
                        uint64_t toCopy = msg->size - copied;
                        if (toCopy > newSize - oldSize) {
                            toCopy = newSize - oldSize;
                            memcpy(msg->data + copied, builderQueue->data + oldSize, toCopy);
                            builderQueue = builderQueue->next;
                            newSize = Builder::OUTPUT_BUFFER_DATA_SIZE;
                            oldSize = 0;
                        } else {
                            memcpy(msg->data + copied, builderQueue->data + oldSize, toCopy);
                            oldSize += (toCopy + 7) & 0xFFFFFFFFFFFFFFF8;
                        }
                        copied += toCopy;
                    }

                    createMessage(msg);
                    // Send only new messages to the client
                    if ((msg->isFlagSet(BuilderMsg::OUTPUT_BUFFER::CHECKPOINT) && !ctx->isFlagSet(Ctx::REDO_FLAGS::SHOW_CHECKPOINT)) ||
                        !metadata->isNewData(msg->lwnScn, msg->lwnIdx))
                        confirmMessage(msg);
                    else {
                        const uint64_t msgSize = msg->size;
                        sendMessage(msg);
                        if (ctx->metrics != nullptr) {
                            ctx->metrics->emitBytesSent(msgSize);
                            ctx->metrics->emitMessagesSent(1);
                        }
                    }
                    break;
                }
            }

            // All work done?
            if (ctx->softShutdown && ctx->replicatorFinished) {
                flush();
                // Is there still some data to send?
                if (builderQueue->confirmedSize != oldSize || builderQueue->next != nullptr)
                    continue;
                break;
            }
        }

        writeCheckpoint(true);
    }

    void Writer::writeCheckpoint(bool force) {
        redo = false;
        // Nothing changed
        if ((checkpointScn == confirmedScn && checkpointIdx == confirmedIdx) || confirmedScn == Scn::none())
            return;

        // Force first checkpoint
        if (checkpointScn == Scn::none())
            force = true;

        // Not yet
        const time_t now = time(nullptr);
        const uint64_t timeSinceCheckpoint = (now - checkpointTime);
        if (timeSinceCheckpoint < ctx->checkpointIntervalS && !force)
            return;

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::CHECKPOINT))) {
            if (checkpointScn == Scn::none())
                ctx->logTrace(Ctx::TRACE::CHECKPOINT, "writer confirmed scn: " + confirmedScn.toString() + " idx: " +
                              std::to_string(confirmedIdx));
            else
                ctx->logTrace(Ctx::TRACE::CHECKPOINT, "writer confirmed scn: " + confirmedScn.toString() + " idx: " +
                              std::to_string(confirmedIdx) + " checkpoint scn: " + checkpointScn.toString() + " idx: " + std::to_string(checkpointIdx));
        }
        const std::string name(database + "-chkpt");
        const bool rac = !metadata->racThreads.empty();
        std::ostringstream ss;
        ss << R"({"database":")" << database
                << R"(","scn":)" << std::dec << confirmedScn.toString()
                << R"(,"idx":)" << std::dec << confirmedIdx
                << R"(,"first-data-scn":)" << std::dec << metadata->firstDataScn.toString()
                << R"(,"cursor-format":)" << std::dec << (rac ? CURSOR_FORMAT_RAC : CURSOR_FORMAT);
        if (rac) {
            ss << R"(,"schema-scn":)" << std::dec << metadata->firstSchemaScn.toString() << R"(,"rac-threads":[)";
            for (size_t i = 0; i < metadata->racThreads.size(); ++i)
                ss << (i > 0 ? "," : "") << std::dec << metadata->racThreads[i];
            ss << "]";
        }
        ss << R"(,"resetlogs":)" << std::dec << metadata->resetlogs
                << R"(,"activation":)" << std::dec << metadata->activation << "}";

        if (metadata->stateWrite(name, confirmedScn, ss)) {
            checkpointScn = confirmedScn;
            checkpointIdx = confirmedIdx;
            checkpointTime = now;
        }
    }

    void Writer::readCheckpoint() {
        const std::string name(database + "-chkpt");

        // Checkpoint is present - read it
        std::string checkpoint;
        rapidjson::Document document;
        if (!metadata->stateRead(name, CHECKPOINT_FILE_MAX_SIZE, checkpoint))
            return;

        if (unlikely(checkpoint.empty() || document.Parse(checkpoint.c_str()).HasParseError()))
            throw DataException(20001, "file: " + name + " offset: " + std::to_string(document.GetErrorOffset()) +
                                " - parse error: " + GetParseError_En(document.GetParseError()));

        if (!metadata->ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::JSON_TAGS)) {
            static const std::vector<std::string> documentNames{
                "database",
                "resetlogs",
                "activation",
                "scn",
                "idx",
                "first-data-scn",
                "cursor-format",
                "schema-scn",
                "rac-threads"
            };
            Ctx::checkJsonFields(name, document, documentNames);
        }

        const std::string databaseJson = Ctx::getJsonFieldS(name, Ctx::JSON_PARAMETER_LENGTH, document, "database");
        if (unlikely(database != databaseJson))
            throw DataException(20001, "file: " + name + " - invalid database name: " + databaseJson);

        metadata->setResetlogs(Ctx::getJsonFieldU32(name, document, "resetlogs"));
        metadata->setActivation(Ctx::getJsonFieldU32(name, document, "activation"));

        // Cursor format 2: idx is the emitted 1-based c_idx of data messages (checkpoint messages take no ordinal), and the
        // acknowledged cursor (scn, idx) only suppresses delivered output while first-data-scn keeps the bootstrap commit
        // filter. Older checkpoints used a different idx numbering and coupled the start SCN to the acknowledged SCN;
        // they can't be reinterpreted safely, so they are rejected (no automatic migration).
        // Cursor format 3 is the rac-batch form: scn is a record scn of the merged stream, not an LWN scn, and "schema-scn" and
        // "rac-threads" fix the schema cut and thread set of the replayed plan. Neither mode accepts the other mode's cursor.
        const bool rac = !metadata->racThreads.empty();
        const uint64_t cursorFormat = rac ? CURSOR_FORMAT_RAC : CURSOR_FORMAT;
        if (!document.HasMember("cursor-format") || Ctx::getJsonFieldU64(name, document, "cursor-format") != cursorFormat ||
                !document.HasMember("first-data-scn") || (rac && (!document.HasMember("schema-scn") || !document.HasMember("rac-threads"))))
            throw DataException(20001, "file: " + name + " - unsupported writer checkpoint format (expected \"cursor-format\": " +
                                std::to_string(cursorFormat) + (rac ? R"( with "first-data-scn", "schema-scn" and "rac-threads" for reader type )"
                                R"("rac-batch")" : R"( with "first-data-scn" for single-thread reader types)") + "); legacy cursors and cursors "
                                "of the other reader mode can't be resumed, start a new replication");

        if (rac) {
            const rapidjson::Value& threadsJson = Ctx::getJsonFieldA(name, document, "rac-threads");
            std::vector<uint16_t> threads;
            for (rapidjson::SizeType k = 0; k < threadsJson.Size(); ++k)
                threads.push_back(Ctx::getJsonFieldU16(name, threadsJson, "rac-threads", k));
            if (threads != metadata->racThreads) {
                auto list = [](const std::vector<uint16_t>& v) {
                    std::string s;
                    for (const uint16_t t: v)
                        s += (s.empty() ? "" : ",") + std::to_string(t);
                    return "[" + s + "]";
                };
                throw DataException(20001, "file: " + name + " - acknowledged cursor belongs to rac-threads " + list(threads) +
                                    ", but the configuration merges " + list(metadata->racThreads) + "; start a new replication");
            }
            metadata->clientSchemaScn = Ctx::getJsonFieldU64(name, document, "schema-scn");
        }

        // Started earlier - continue work and ignore default startup parameters
        checkpointScn = Ctx::getJsonFieldU64(name, document, "scn");
        metadata->clientScn = checkpointScn;
        if (document.HasMember("idx"))
            checkpointIdx = Ctx::getJsonFieldU64(name, document, "idx");
        else
            checkpointIdx = 0;
        metadata->clientIdx = checkpointIdx;
        metadata->startScn = Ctx::getJsonFieldU64(name, document, "first-data-scn");
        metadata->startSequence = Seq::none();
        metadata->startTime.clear();
        metadata->startTimeRel = 0;

        ctx->info(0, "checkpoint - all confirmed till scn: " + checkpointScn.toString() + ", idx: " +
                  std::to_string(checkpointIdx));
        metadata->setStatusReplicate(this);
    }

    void Writer::wakeUp() {
        Thread::wakeUp();
        builder->wakeUp();
    }
}
