/* Header for Parser class
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

#ifndef PARSER_H_
#define PARSER_H_

#include <cstddef>
#include <vector>

#include "../common/Ctx.h"
#include "../common/RedoLogRecord.h"
#include "../reader/Reader.h"
#include "../common/types/Time.h"
#include "../common/types/Types.h"
#include "../common/types/Xid.h"

namespace OpenLogReplicator {
    class Builder;
    class Metadata;
    class Transaction;
    class TransactionBuffer;
    class XmlCtx;

    struct LwnMember {
        uint16_t pageOffset;
        Scn scn;
        uint32_t size;
        typeBlk block;
        typeSubScn subScn;

        bool operator<(const LwnMember& other) const {
            if (scn < other.scn)
                return true;
            if (other.scn < scn)
                return false;
            if (subScn < other.subScn)
                return true;
            if (other.subScn < subScn)
                return false;
            if (block < other.block)
                return true;
            if (block > other.block)
                return false;
            return (pageOffset < other.pageOffset);
        }
    };

    class Parser final {
    protected:
        static constexpr uint64_t MAX_LWN_CHUNKS = static_cast<uint64_t>(512 * 2) / Ctx::MEMORY_CHUNK_SIZE_MB;
        static constexpr uint64_t MAX_RECORDS_IN_LWN = 1048576;

        Ctx* ctx;
        Builder* builder;
        Metadata* metadata;
        TransactionBuffer* transactionBuffer;
        RedoLogRecord zero;
        Transaction* lastTransaction{nullptr};

        uint8_t* lwnChunks[MAX_LWN_CHUNKS]{};
        LwnMember* lwnMembers[MAX_RECORDS_IN_LWN + 1]{};
        uint64_t lwnAllocated{0};
        uint64_t lwnAllocatedMax{0};
        Time lwnTimestamp{0};
        Scn lwnScn;
        typeBlk lwnCheckpointBlock{0};

        // Framing state of frameNextLwn() (closed-input RAC merge); parse() does not use it
        FileOffset frameBufferStart;
        typeBlk frameCurrentBlock{0};
        typeBlk frameLwnEndBlock{0};
        typeLwn frameLwnNumMax{0};
        typeLwn frameLwnNumCnt{0};
        uint32_t frameRecordLeftToCopy{0};
        uint64_t frameRecordPos{0};
        LwnMember* frameLwnMember{nullptr};
        Scn framePieceScn{Scn::none()};
        bool frameLwnReturned{false};

        void freeLwn();
        void analyzeLwn(LwnMember* lwnMember);
        void appendToTransactionDdl(RedoLogRecord* redoLogRecord1);
        void appendToTransactionBegin(RedoLogRecord* redoLogRecord1);
        void appendToTransactionCommit(RedoLogRecord* redoLogRecord1);
        void appendToTransactionLob(RedoLogRecord* redoLogRecord1);
        void appendToTransactionIndex(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2);
        void appendToTransaction(RedoLogRecord* redoLogRecord1);
        void appendToTransactionRollback(RedoLogRecord* redoLogRecord1);
        void appendToTransaction(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2);
        void appendToTransactionRollback(RedoLogRecord* redoLogRecord1, RedoLogRecord* redoLogRecord2);
        void dumpRedoVector(const uint8_t* data, typeSize recordSize) const;

    public:
        int group;
        std::string path;
        Seq sequence;
        Scn firstScn{Scn::none()};
        Scn nextScn{Scn::none()};
        Reader* reader{nullptr};
        // A transaction committed after the data/schema cut without its begin record is a coverage error, not a warning
        bool requireBegin{false};
        // Records of the last framed LWN in production order (same comparator as parse()), valid until releaseFramedLwn()
        std::vector<LwnMember*> lwnSorted;

        enum class FRAME : unsigned char {
            LWN,     // a complete LWN is in lwnSorted
            END,     // the reader reached the end of the log
            PENDING, // frameNextLwn(false) only: no further block is available yet; the partial LWN stays in the framing state
            REUSED   // frameNextLwn(false) only: the reader found a newer sequence (OVERWRITTEN); the partial LWN was never returned
        };

        // SCN of the last piece header of the last complete framed LWN (the value parse() uses for that LWN), none before the first
        Scn frameLwnScn{Scn::none()};

        Parser(Ctx* newCtx, Builder* newBuilder, Metadata* newMetadata, TransactionBuffer* newTransactionBuffer, int newGroup, std::string newPath);
        ~Parser();

        Reader::REDO_CODE parse();
        void frameStart(typeBlk startBlock = 2);
        FRAME frameNextLwn(bool wait = true);
        [[nodiscard]] typeBlk frameResumeBlock() const;
        [[nodiscard]] std::string framePosition() const;
        void analyzeFramed(LwnMember* lwnMember, Scn groupScn, bool newSession);
        void releaseFramedLwn();
        [[nodiscard]] Time getLwnTimestamp() const;
        [[nodiscard]] std::string toString() const;
    };
}

#endif
