/* Thread reading database redo Logs using online mode
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

#include <algorithm>
#include <regex>
#include <unistd.h>

#include "../builder/Builder.h"
#include "../common/DbColumn.h"
#include "../common/DbIncarnation.h"
#include "../common/DbTable.h"
#include "../common/XmlCtx.h"
#include "../common/exception/BootException.h"
#include "../common/exception/RuntimeException.h"
#include "../common/table/SysCCol.h"
#include "../common/table/SysCDef.h"
#include "../common/table/SysCol.h"
#include "../common/table/SysDeferredStg.h"
#include "../common/table/SysECol.h"
#include "../common/table/SysLob.h"
#include "../common/table/SysLobCompPart.h"
#include "../common/table/SysLobFrag.h"
#include "../common/table/SysObj.h"
#include "../common/table/SysTab.h"
#include "../common/table/SysTabComPart.h"
#include "../common/table/SysTabPart.h"
#include "../common/table/SysTabSubPart.h"
#include "../common/table/SysTs.h"
#include "../common/table/SysUser.h"
#include "../common/table/XdbTtSet.h"
#include "../common/table/XdbXNm.h"
#include "../common/table/XdbXQn.h"
#include "../common/table/XdbXPt.h"
#include "../metadata/Metadata.h"
#include "../metadata/RedoLog.h"
#include "../metadata/Schema.h"
#include "../parser/Parser.h"
#include "../parser/TransactionBuffer.h"
#include "../reader/Reader.h"
#include "DatabaseConnection.h"
#include "DatabaseEnvironment.h"
#include "DatabaseStatement.h"
#include "ReplicatorOnline.h"
#include "OnlineCatalog.h"

namespace OpenLogReplicator {
    ReplicatorOnline::ReplicatorOnline(Ctx * newCtx, void(*newArchGetLog)(Replicator * replicator), Builder * newBuilder, Metadata * newMetadata,
                                       TransactionBuffer * newTransactionBuffer, std::string newAlias, std::string newDatabase, std::string newUser,
                                       std::string newPassword, std::string newConnectString, bool newKeepConnection):
        Replicator(newCtx, newArchGetLog, newBuilder, newMetadata, newTransactionBuffer, std::move(newAlias), std::move(newDatabase)),
        keepConnection(newKeepConnection) {
        env = new DatabaseEnvironment(ctx);
        env->initialize();
        conn = new DatabaseConnection(env, std::move(newUser), std::move(newPassword), std::move(newConnectString), false);
        catalog = new OnlineCatalog(ctx, metadata, conn);
    }

    ReplicatorOnline::~ReplicatorOnline() {
        delete catalog;
        catalog = nullptr;
        if (conn != nullptr) {
            delete conn;
            conn = nullptr;
        }

        if (env != nullptr) {
            delete env;
            env = nullptr;
        }
    }

    void ReplicatorOnline::loadDatabaseMetadata() {
        if (!checkConnection())
            return;
        catalog->readDatabaseMetadata(!standby);
        archReader = readerCreate(0);
    }

    void ReplicatorOnline::positionReader() {
        // Position by time
        if (!metadata->startTime.empty()) {
            DatabaseStatement stmt(conn);
            if (standby)
                throw BootException(10024, "can't position by time for standby database");

            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(OnlineCatalog::SQL_GET_SCN_FROM_TIME));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + metadata->startTime);
            }
            stmt.createStatement(OnlineCatalog::SQL_GET_SCN_FROM_TIME);

            stmt.bindString(1, metadata->startTime);
            Scn firstDataScn;
            stmt.defineUInt(1, firstDataScn);

            if (stmt.executeQuery() == 0)
                throw BootException(10025, "can't find scn for: " + metadata->startTime);
            metadata->firstDataScn = firstDataScn;
        } else if (metadata->startTimeRel > 0) {
            DatabaseStatement stmt(conn);
            if (standby)
                throw BootException(10026, "can't position by relative time for standby database");

            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(OnlineCatalog::SQL_GET_SCN_FROM_TIME_RELATIVE));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + std::to_string(metadata->startTimeRel));
            }
            stmt.createStatement(OnlineCatalog::SQL_GET_SCN_FROM_TIME_RELATIVE);
            stmt.bindUInt(1, metadata->startTimeRel);
            Scn firstDataScn;
            stmt.defineUInt(1, firstDataScn);

            if (stmt.executeQuery() == 0)
                throw BootException(10025, "can't find scn for " + metadata->startTime);
            metadata->firstDataScn = firstDataScn;
        } else if (metadata->firstDataScn == Scn::none() || metadata->firstDataScn == Scn::zero()) {
            // NOW
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL)))
                ctx->logTrace(Ctx::TRACE::SQL, std::string(OnlineCatalog::SQL_GET_DATABASE_SCN));
            stmt.createStatement(OnlineCatalog::SQL_GET_DATABASE_SCN);
            Scn firstDataScn;
            stmt.defineUInt(1, firstDataScn);

            if (stmt.executeQuery() == 0)
                throw BootException(10029, "can't find database current scn");
            metadata->firstDataScn = firstDataScn;
        }

        // First sequence
        if (metadata->startSequence != Seq::none()) {
            metadata->setSeqFileOffset(metadata->startSequence, FileOffset::zero());
            if (metadata->firstDataScn == Scn::none())
                metadata->firstDataScn = Scn::zero();
        } else {
            DatabaseStatement stmt(conn);
            if (standby) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                    ctx->logTrace(Ctx::TRACE::SQL, std::string(OnlineCatalog::SQL_GET_SEQUENCE_FROM_SCN_STANDBY));
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + metadata->firstDataScn.toString());
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + metadata->firstDataScn.toString());
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(metadata->resetlogs));
                }
                stmt.createStatement(OnlineCatalog::SQL_GET_SEQUENCE_FROM_SCN_STANDBY);
                stmt.bindUInt(1, metadata->firstDataScn);
                stmt.bindUInt(2, metadata->firstDataScn);
                stmt.bindUInt(3, metadata->resetlogs);
            } else {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                    ctx->logTrace(Ctx::TRACE::SQL, std::string(OnlineCatalog::SQL_GET_SEQUENCE_FROM_SCN));
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + metadata->firstDataScn.toString());
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + metadata->firstDataScn.toString());
                    ctx->logTrace(Ctx::TRACE::SQL, "PARAM3: " + std::to_string(metadata->resetlogs));
                }
                stmt.createStatement(OnlineCatalog::SQL_GET_SEQUENCE_FROM_SCN);
                stmt.bindUInt(1, metadata->firstDataScn);
                stmt.bindUInt(2, metadata->firstDataScn);
                stmt.bindUInt(3, metadata->resetlogs);
            }
            Seq sequence;
            stmt.defineUInt(1, sequence);

            if (stmt.executeQuery() == 0)
                throw BootException(10030, "getting database sequence for scn: " + metadata->firstDataScn.toString());

            metadata->setSeqFileOffset(sequence, FileOffset::zero());
            ctx->info(0, "starting sequence not found - starting with new batch with seq: " + metadata->sequence.toString());
        }

        if (metadata->firstDataScn == Scn::none())
            throw BootException(10031, "getting database scn");
    }

    bool ReplicatorOnline::checkConnection() {
        if (!conn->connected) {
            ctx->info(0, "connecting to the database instance of " + database + " to " + conn->connectString);
        }

        while (!ctx->softShutdown) {
            if (!conn->connected) {
                try {
                    conn->connect();
                } catch (RuntimeException& ex) {
                    ctx->error(ex.code, ex.msg);
                }
            }

            if (conn->connected) {
                try {
                    DatabaseStatement stmt(conn);
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL)))
                        ctx->logTrace(Ctx::TRACE::SQL, std::string(OnlineCatalog::SQL_CHECK_CONNECTION));
                    stmt.createStatement(OnlineCatalog::SQL_CHECK_CONNECTION);
                    uint dummy;
                    stmt.defineUInt(1, dummy);

                    stmt.executeQuery();
                } catch (RuntimeException& ex) {
                    ctx->error(ex.code, ex.msg);
                    conn->disconnect();
                    contextSet(CONTEXT::SLEEP);
                    ctx->usleepInt(ctx->refreshIntervalUs);
                    contextSet(CONTEXT::CPU);
                    ctx->info(0, "reconnecting to the database instance of " + database + " to " + conn->connectString);
                    continue;
                }

                return true;
            }

            if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                ctx->logTrace(Ctx::TRACE::REDO, "cannot connect to database, retry in " + std::to_string(ctx->refreshIntervalUs / 1000000) + " sec.");
            contextSet(CONTEXT::SLEEP);
            ctx->usleepInt(ctx->refreshIntervalUs);
            contextSet(CONTEXT::CPU);
        }

        return false;
    }

    void ReplicatorOnline::goStandby() {
        if (!keepConnection)
            conn->disconnect();
    }

    void ReplicatorOnline::verifySchema(Scn currentScn) {
        if (!ctx->isFlagSet(Ctx::REDO_FLAGS::VERIFY_SCHEMA))
            return;
        if (!checkConnection())
            return;

        ctx->info(0, "verifying schema for SCN: " + currentScn.toString());

        Schema otherSchema(ctx, metadata->locales);
        try {
            catalog->readSystemDictionariesMetadata(&otherSchema, currentScn);
            for (const SchemaElement* element: metadata->schemaElements)
                catalog->readSystemDictionaries(&otherSchema, currentScn, element->owner, element->table, element->options);
            std::string errMsg;
            const bool result = metadata->schema->compare(&otherSchema, errMsg);
            if (result) {
                ctx->warning(70000, "schema incorrect: " + errMsg);
            }
        } catch (RuntimeException& e) {
            ctx->error(e.code, e.msg);
        } catch (std::bad_alloc& ex) {
            ctx->error(10018, "memory allocation failed: " + std::string(ex.what()));
            ctx->stopHard();
        }
    }

    void ReplicatorOnline::createSchema() {
        if (!checkConnection())
            return;

        ctx->info(0, "reading dictionaries for scn: " + metadata->firstDataScn.toString());

        std::unordered_map<typeObj, std::string> tablesUpdated;
        {
            contextSet(CONTEXT::MUTEX, REASON::REPLICATOR_SCHEMA);
            std::unique_lock const lck(metadata->mtxSchema);
            metadata->schema->purgeMetadata();
            metadata->schema->purgeDicts();
            metadata->schema->scn = metadata->firstDataScn;
            metadata->firstSchemaScn = metadata->firstDataScn;
            catalog->readSystemDictionariesMetadata(metadata->schema, metadata->firstDataScn);

            for (const SchemaElement* element: metadata->schemaElements)
                catalog->createSchemaForTable(metadata->firstDataScn, element->owner, element->table, element->keyList, element->key, element->tagType,
                                     element->tagList, element->tag, element->condition, element->options, tablesUpdated);
            metadata->schema->resetTouched();

            if (unlikely(metadata->ctx->isTraceSet(Ctx::TRACE::CHECKPOINT)))
                metadata->ctx->logTrace(Ctx::TRACE::CHECKPOINT, "schema creation completed, allowing checkpoints");
            metadata->allowCheckpoints();
        }
        contextSet(CONTEXT::CPU);

        for (const auto& [_, tableName]: tablesUpdated)
            ctx->info(0, "- found: " + tableName);
    }

    void ReplicatorOnline::updateOnlineRedoLogData() {
        if (!checkConnection())
            return;

        contextSet(CONTEXT::CHKPT, REASON::CHKPT);
        std::unique_lock const lck(metadata->mtxCheckpoint);

        // Reload incarnation ctx
        typeResetlogs const oldResetlogs = metadata->resetlogs;
        for (DbIncarnation* oi: metadata->dbIncarnations)
            delete oi;
        metadata->dbIncarnations.clear();
        metadata->dbIncarnationCurrent = nullptr;

        {
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL)))
                ctx->logTrace(Ctx::TRACE::SQL, std::string(OnlineCatalog::SQL_GET_DATABASE_ROLE));
            stmt.createStatement(OnlineCatalog::SQL_GET_DATABASE_ROLE);
            std::array < char, 129 > databaseRole{};
            stmt.defineString(1, databaseRole.data(), databaseRole.size());

            if (stmt.executeQuery() != 0) {
                const std::string roleStr(databaseRole.data());
                if (roleStr == "PRIMARY") {
                    if (standby) {
                        standby = false;
                        ctx->info(0, "changed database role to: " + roleStr);
                    }
                } else if (roleStr == "PHYSICAL STANDBY") {
                    if (!standby) {
                        standby = true;
                        ctx->info(0, "changed database role to: " + roleStr);
                    }
                } else {
                    throw RuntimeException(10038, "unknown database role: " + roleStr);
                }
            }
        }

        {
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL)))
                ctx->logTrace(Ctx::TRACE::SQL, std::string(OnlineCatalog::SQL_GET_DATABASE_INCARNATION));
            stmt.createStatement(OnlineCatalog::SQL_GET_DATABASE_INCARNATION);
            uint32_t incarnation;
            stmt.defineUInt(1, incarnation);
            Scn resetlogsScn;
            stmt.defineUInt(2, resetlogsScn);
            Scn priorResetlogsScn;
            stmt.defineUInt(3, priorResetlogsScn);
            std::array < char, 129 > status{};
            stmt.defineString(4, status.data(), status.size());
            typeResetlogs resetlogs;
            stmt.defineUInt(5, resetlogs);
            uint32_t priorIncarnation;
            stmt.defineUInt(6, priorIncarnation);

            int ret = stmt.executeQuery();
            while (ret != 0) {
                auto* oi = new DbIncarnation(incarnation, resetlogsScn, priorResetlogsScn, status.data(),
                                             resetlogs, priorIncarnation);
                metadata->dbIncarnations.insert(oi);

                // Search prev value
                if (oldResetlogs != 0 && oi->resetlogs == oldResetlogs) {
                    metadata->dbIncarnationCurrent = oi;
                } else if (oi->current && metadata->dbIncarnationCurrent == nullptr) {
                    metadata->dbIncarnationCurrent = oi;
                    metadata->setResetlogs(oi->resetlogs);
                }

                ret = stmt.next();
            }
        }

        // Reload online redo log ctx
        {
            DatabaseStatement stmt(conn);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SQL))) {
                ctx->logTrace(Ctx::TRACE::SQL, std::string(OnlineCatalog::SQL_GET_LOGFILE_LIST));
                ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " + std::to_string(standby ? 1 : 0));
            }
            stmt.createStatement(OnlineCatalog::SQL_GET_LOGFILE_LIST);
            std::string standbyStr{"STANDBY"};
            std::string onlineStr{"ONLINE"};
            if (standby)
                stmt.bindString(1, standbyStr);
            else
                stmt.bindString(1, onlineStr);

            int group = -1;
            stmt.defineInt(1, group);
            std::array < char, 513 > pathStr{};
            stmt.defineString(2, pathStr.data(), pathStr.size());
            Reader* onlineReader = nullptr;
            int lastGroup = -1;

            int ret = stmt.executeQuery();
            while (ret != 0) {
                if (group != lastGroup || onlineReader == nullptr) {
                    onlineReader = readerCreate(group);
                    onlineReader->paths.clear();
                    lastGroup = group;
                }
                const std::string path(pathStr.data());
                onlineReader->paths.push_back(path);
                auto* redoLog = new RedoLog(group, pathStr.data());
                metadata->redoLogs.insert(redoLog);

                ret = stmt.next();
            }

            if (readers.empty()) {
                if (standby)
                    throw RuntimeException(10036, "failed to find standby redo log files");
                throw RuntimeException(10037, "failed to find online redo log files");
            }
        }
        checkOnlineRedoLogs();
        contextSet(CONTEXT::CPU);
    }

    void ReplicatorOnline::archGetLogOnline(Replicator* replicator) {
        auto* replicatorOnline = dynamic_cast<ReplicatorOnline*>(replicator);
        if (replicatorOnline == nullptr)
            return;

        if (!replicatorOnline->checkConnection())
            return;

        {
            DatabaseStatement stmt(replicatorOnline->conn);
            if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::SQL))) {
                replicator->ctx->logTrace(Ctx::TRACE::SQL, std::string(OnlineCatalog::SQL_GET_ARCHIVE_LOG_LIST));
                replicator->ctx->logTrace(Ctx::TRACE::SQL, "PARAM1: " +
                                          replicatorOnline->metadata->sequence.toString());
                replicator->ctx->logTrace(Ctx::TRACE::SQL, "PARAM2: " + std::to_string(replicator->metadata->resetlogs));
            }

            stmt.createStatement(OnlineCatalog::SQL_GET_ARCHIVE_LOG_LIST);
            stmt.bindUInt(1, replicator->metadata->sequence);
            stmt.bindUInt(2, replicator->metadata->resetlogs);

            std::array < char, 513 > path{};
            stmt.defineString(1, path.data(), path.size());
            Seq sequence;
            stmt.defineUInt(2, sequence);
            Scn firstScn;
            stmt.defineUInt(3, firstScn);
            Scn nextScn;
            stmt.defineUInt(4, nextScn);

            int ret = stmt.executeQuery();
            while (ret != 0) {
                std::string mappedPath(path.data());
                replicator->applyMapping(mappedPath);

                auto* parser = new Parser(replicator->ctx, replicator->builder, replicator->metadata,
                                          replicator->transactionBuffer, 0, mappedPath);
                parser->firstScn = firstScn;
                parser->nextScn = nextScn;
                parser->sequence = sequence;
                replicator->archiveRedoQueue.push(parser);
                ret = stmt.next();
            }
        }
        replicator->goStandby();
    }

    std::string ReplicatorOnline::getModeName() const {
        return {"online"};
    }
}
