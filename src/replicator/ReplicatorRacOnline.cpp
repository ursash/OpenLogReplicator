/* Online-discovered RAC replication. SPDX-License-Identifier: AGPL-3.0-or-later */
#include <array>
#include <algorithm>
#include <chrono>
#include <regex>
#include <sstream>
#include <unistd.h>

#include "../common/Ctx.h"
#include "../common/exception/RuntimeException.h"
#include "../common/types/Data.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "../metadata/SchemaElement.h"
#include "DatabaseEnvironment.h"
#include "DatabaseConnection.h"
#include "DatabaseStatement.h"
#include "OnlineCatalog.h"
#include "RacBootstrap.h"
#include "ReplicatorRacOnline.h"

namespace OpenLogReplicator {
    ReplicatorRacOnline::ReplicatorRacOnline(Ctx* newCtx, Builder* newBuilder, Metadata* newMetadata, TransactionBuffer* newTransactionBuffer,
                                           const std::string& newAlias, const std::string& newDatabase, const std::string& user,
                                           const std::string& password, const std::string& server):
            ReplicatorRac(newCtx, Replicator::archGetLogList, newBuilder, newMetadata, newTransactionBuffer, newAlias, newDatabase, {}, 0, true),
            env(new DatabaseEnvironment(newCtx)) {
        env->initialize();
        conn.reset(new DatabaseConnection(env.get(), user, password, server, false));
        catalog.reset(new OnlineCatalog(newCtx, newMetadata, conn.get()));
        standbyLive = !ctx->isFlagSet(Ctx::REDO_FLAGS::ARCH_ONLY);
    }

    ReplicatorRacOnline::~ReplicatorRacOnline() = default;

    std::string ReplicatorRacOnline::getModeName() const {
        return "rac-online";
    }

    void ReplicatorRacOnline::admit(const std::string& statePath) {
        bootstrap.reset(new RacBootstrap(metadata, statePath));
        bootstrap->admit();
        threads = metadata->racThreads;
    }

    Scn ReplicatorRacOnline::readIdentity() {
        DatabaseStatement stmt(conn.get());
        stmt.createStatement("SELECT D.DBID,D.DATABASE_ROLE,D.OPEN_MODE,D.CDB,D.CURRENT_SCN,D.ACTIVATION#,I.RESETLOGS_ID,DBTIMEZONE,"
                             "D.SUPPLEMENTAL_LOG_DATA_MIN,D.SUPPLEMENTAL_LOG_DATA_PK,D.SUPPLEMENTAL_LOG_DATA_ALL,"
                             "DECODE(P.ENDIAN_FORMAT,'Big',1,0),NVL(D.CON_DBID,0) "
                             "FROM SYS.V_$DATABASE D CROSS JOIN SYS.V_$DATABASE_INCARNATION I "
                             "JOIN SYS.V_$TRANSPORTABLE_PLATFORM P ON P.PLATFORM_NAME=D.PLATFORM_NAME WHERE I.STATUS='CURRENT'");
        uint32_t actualDbid = 0, activation = 0, resetlogs = 0;
        uint32_t actualBigEndian = 0;
        uint64_t actualContainerDbid = 0;
        Scn applied;
        std::array<char, 81> role{}, mode{}, cdb{};
        std::array<char, 81> actualTimezone{}, suppMin{}, suppPk{}, suppAll{};
        stmt.defineUInt(1, actualDbid);
        stmt.defineString(2, role.data(), role.size());
        stmt.defineString(3, mode.data(), mode.size());
        stmt.defineString(4, cdb.data(), cdb.size());
        stmt.defineUInt(5, applied);
        stmt.defineUInt(6, activation);
        stmt.defineUInt(7, resetlogs);
        stmt.defineString(8, actualTimezone.data(), actualTimezone.size());
        stmt.defineString(9, suppMin.data(), suppMin.size());
        stmt.defineString(10, suppPk.data(), suppPk.size());
        stmt.defineString(11, suppAll.data(), suppAll.size());
        stmt.defineUInt(12, actualBigEndian);
        stmt.defineUInt(13, actualContainerDbid);
        if (stmt.executeQuery() == 0 || std::string(role.data()) != "PHYSICAL STANDBY" ||
                (std::string(mode.data()) != "READ ONLY" && std::string(mode.data()) != "READ ONLY WITH APPLY"))
            throw RuntimeException(10077, "rac-online requires an already readable PHYSICAL STANDBY; it will not open or alter the database");
        if (actualDbid == 0 || resetlogs == 0 || stmt.next() != 0)
            throw RuntimeException(10077, "standby current database incarnation is ambiguous");

        DatabaseStatement th(conn.get());
        // Configured redo groups do not identify enabled threads or instantaneous primary topology.
        th.createStatement("SELECT DISTINCT THREAD# FROM SYS.V_$LOG ORDER BY THREAD#");
        uint16_t thread = 0;
        th.defineUInt(1, thread);
        std::vector<uint16_t> discovered;
        for (int ret = th.executeQuery(); ret != 0; ret = th.next())
            discovered.push_back(thread);
        if (discovered != std::vector<uint16_t>{1, 2} || (!threads.empty() && discovered != threads))
            throw RuntimeException(10077, "rac-online requires the same configured redo threads 1 and 2");

        DatabaseStatement container(conn.get());
        container.createStatement("SELECT TO_NUMBER(SYS_CONTEXT('USERENV','CON_ID')),SYS_CONTEXT('USERENV','CON_NAME'),"
                                  "CASE WHEN SYS_CONTEXT('USERENV','IS_APPLICATION_ROOT')='NO' "
                                  "AND SYS_CONTEXT('USERENV','IS_APPLICATION_PDB')='NO' THEN 1 ELSE 0 END "
                                  "FROM DUAL WHERE SYS_CONTEXT('USERENV','ISDBA')='FALSE'");
        typeConId conId = 0;
        uint32_t nonApplicationContainer = 0;
        std::array<char, 81> conName{};
        container.defineInt(1, conId);
        container.defineString(2, conName.data(), conName.size());
        container.defineUInt(3, nonApplicationContainer);
        if (container.executeQuery() == 0 || conName[0] == '\0' || container.next() != 0)
            throw RuntimeException(10077, "cannot identify standby container in an ordinary database session");
        uint64_t containerDbid = actualDbid;
        if (std::string(cdb.data()) == "YES") {
            if (conId <= 2 || nonApplicationContainer != 1)
                throw RuntimeException(10077, "rac-online requires an ordinary PDB, not a CDB root, seed or application container");
            containerDbid = actualContainerDbid;
            if (containerDbid == 0)
                throw RuntimeException(10077, "cannot identify standby PDB DBID");
            // Oracle 19c Multitenant Guide tables 15-4/15-5 require a read/write root for a read/write PDB.
            // The CDB read-only guard and ordinary PDB session do not replace the DATABASE_PROPERTIES reads
            // below or the actual AS OF schema extraction required before fresh REPLICATE.
        } else if (std::string(cdb.data()) != "NO" || conId != 0)
            throw RuntimeException(10077, "standby container identity is ambiguous");
        const std::string charset(catalog->getPropertyValue("NLS_CHARACTERSET"));
        const std::string nchar(catalog->getPropertyValue("NLS_NCHAR_CHARACTERSET"));
        int64_t effectiveTimezone = ctx->dbTimezone;
        if (effectiveTimezone == Ctx::BAD_TIMEZONE && !Data::parseTimezone(actualTimezone.data(), effectiveTimezone))
            throw RuntimeException(10068, "invalid standby DBTIMEZONE value");
        std::ostringstream identity;
        const auto text = [&identity](const std::string& value) {
            identity << '"';
            Data::writeEscapeValue(identity, value);
            identity << '"';
        };
        identity << actualDbid << ':' << resetlogs << ':' << activation << ':' << conId << ':' << containerDbid << ':';
        text(conName.data());
        identity << ':';
        text(charset);
        identity << ':';
        text(nchar);
        identity << ':';
        text(actualTimezone.data());
        std::string checksum = catalog->getParameterValue("db_block_checksum");
        std::transform(checksum.begin(), checksum.end(), checksum.begin(), ::toupper);
        identity << ':';
        text(checksum);
        identity << ':';
        text(suppMin.data());
        identity << ':';
        text(suppPk.data());
        identity << ':';
        text(suppAll.data());
        identity << ':' << effectiveTimezone << ':' << actualBigEndian;
        liveIdentity = identity.str();
        // Observe locally and compare first. A refresh must not expose transient or unaccepted identity to the ACK writer.
        bootstrap->checkIdentity(liveIdentity);
        if (actualBigEndian != 0)
            throw RuntimeException(10077, "rac-online currently requires little-endian redo");
        const typeDbId redoPdbDbid = std::string(cdb.data()) == "YES" && conId > 2 ? containerDbid : 0;
        if (!identityInitialized) {
            if (metadata->racResume) {
                if (metadata->resetlogs != resetlogs || metadata->activation != activation || metadata->conId != conId || metadata->dbId != redoPdbDbid ||
                        metadata->conName != conName.data() || metadata->nlsCharacterSet != charset || metadata->nlsNcharCharacterSet != nchar ||
                        metadata->dbTimezone != effectiveTimezone)
                    throw RuntimeException(10077, "native RAC original schema identity differs from the qualified standby");
            } else {
                metadata->racThreads = discovered;
                metadata->setResetlogs(resetlogs);
                metadata->setActivation(activation);
            }
            threads = discovered;
            dbid = actualDbid;
            // Initial metadata/grant checks occur before any successful START or ACK output. Established identity is never reset.
            metadata->dbId = redoPdbDbid;
            catalog->readDatabaseMetadata(false);
            identityInitialized = true;
        }
        return applied;
    }

    void ReplicatorRacOnline::loadDatabaseMetadata() {
        // The same thread owns OCI, schema extraction and stream planning. No stock single-thread reader is run alongside RAC.
        if (metadata->racDeadlineMs == 0)
            metadata->beginNativeStartup();
        conn->startupMetadata = metadata;
        conn->connect();
        readIdentity();
    }

    std::vector<ReplicatorRac::RedoFile> ReplicatorRacOnline::collectInventory() {
        readIdentity(); // Compare with immutable qualified identity before admitting any additional inventory.
        // Catalog copies can be deleted by archive retention. Accept an unreadable copy only if another validated copy of the
        // same thread/sequence is readable; never discard a corrupt readable header or invent a frontier for an idle thread.
        DatabaseStatement stmt(conn.get());
        stmt.createStatement("SELECT THREAD#,SEQUENCE#,NAME,FIRST_CHANGE#,NEXT_CHANGE# FROM SYS.V_$ARCHIVED_LOG "
                             "WHERE RESETLOGS_ID=:i AND NAME IS NOT NULL AND DELETED='NO' AND STATUS='A' AND NEXT_CHANGE#>:j "
                             "ORDER BY THREAD#,SEQUENCE#,DEST_ID,NAME");
        uint32_t rl = metadata->resetlogs;
        Scn cut = std::min(metadata->firstSchemaScn, metadata->firstDataScn);
        if (cut == Scn::none())
            cut = metadata->startScn;
        stmt.bindUInt(1, rl);
        stmt.bindUInt(2, cut);
        uint16_t thread = 0;
        uint32_t seq = 0;
        std::array<char, 2049> name{};
        Scn first, next;
        stmt.defineUInt(1, thread);
        stmt.defineUInt(2, seq);
        stmt.defineString(3, name.data(), name.size());
        stmt.defineUInt(4, first);
        stmt.defineUInt(5, next);
        std::map<std::pair<uint16_t, uint32_t>, bool> available;
        redoLogsBatch.clear();
        for (int ret = stmt.executeQuery(); ret != 0; ret = stmt.next()) {
            std::string path(name.data());
            applyMapping(path);
            const auto key = std::make_pair(thread, seq);
            available.emplace(key, false);
            if (access(path.c_str(), R_OK) != 0)
                continue;
            const RedoFile header = readHeader(path);
            if (header.dbid != dbid || header.resetlogs != rl || header.activation != metadata->activation || header.thread != thread ||
                    header.sequence != Seq(seq) || header.firstScn != first || header.nextScn != next || header.numBlocks == 0 ||
                    header.numBlocks == Ctx::ZERO_BLK)
                throw RuntimeException(10074, "catalog/header mismatch for local archive: " + path);
            redoLogsBatch.push_back(path);
            available[key] = true;
        }
        for (const auto& [key, readable]: available)
            if (!readable)
                throw RuntimeException(10074, "no readable local archive copy for thread " + std::to_string(key.first) +
                                              " sequence " + std::to_string(key.second));

        standbyLogs.clear();
        if (standbyLive) {
            DatabaseStatement srl(conn.get());
            srl.createStatement("SELECT S.GROUP#,F.MEMBER FROM SYS.V_$STANDBY_LOG S JOIN SYS.V_$LOGFILE F ON F.GROUP#=S.GROUP# "
                                "WHERE F.TYPE='STANDBY' ORDER BY S.GROUP#,F.MEMBER");
            uint32_t group = 0;
            std::array<char, 2049> member{};
            srl.defineUInt(1, group);
            srl.defineString(2, member.data(), member.size());
            std::map<uint32_t, std::string> members;
            for (int ret = srl.executeQuery(); ret != 0; ret = srl.next()) {
                std::string path(member.data());
                applyMapping(path);
                members.emplace(group, "");
                if (access(path.c_str(), R_OK) == 0) {
                    readHeader(path); // A readable but corrupt member is a qualification failure.
                    if (members[group].empty())
                        members[group] = path;
                }
            }
            if (members.empty())
                throw RuntimeException(10074, "no standby redo members discovered for live RAC following");
            for (const auto& [groupId, path]: members) {
                if (path.empty())
                    throw RuntimeException(10074, "no readable standby redo member in group " + std::to_string(groupId));
                standbyLogs.push_back(path);
            }
        }
        return ReplicatorRac::collectInventory();
    }

    void ReplicatorRacOnline::qualifyClosedStart() {
        const Scn cut(metadata->firstDataScn);
        while (true) {
            metadata->checkNativeDeadline();
            const Scn applied = readIdentity();
            bool covered = applied >= cut;
            const auto inventory = collectInventory();
            for (const uint16_t thread: threads) {
                bool found = false;
                for (const RedoFile& file: inventory)
                    if (file.thread == thread && !file.standby && !file.growing && file.firstScn <= cut && cut < file.nextScn)
                        found = true;
                covered = covered && found;
            }
            if (covered)
                return;
            contextSet(CONTEXT::SLEEP);
            usleep(100000);
            contextSet(CONTEXT::CPU);
        }
    }

    void ReplicatorRacOnline::positionReader() {
        if (!metadata->racResume) {
            if (metadata->startScn == Scn::none() || metadata->startScn == Scn::zero())
                throw RuntimeException(10077, "rac-online requires a concrete client START SCN");
            metadata->firstDataScn = metadata->startScn;
            metadata->firstSchemaScn = metadata->startScn;
        }
        qualifyClosedStart();
        metadata->setSeqFileOffset(Seq::zero(), FileOffset::zero());
    }

    void ReplicatorRacOnline::createSchema() {
        metadata->checkNativeDeadline();
        std::unordered_map<typeObj, std::string> updated;
        {
            std::unique_lock lock(metadata->mtxSchema);
            metadata->schema->purgeMetadata();
            metadata->schema->purgeDicts();
            metadata->schema->scn = metadata->firstDataScn;
            metadata->firstSchemaScn = metadata->firstDataScn;
            catalog->readSystemDictionariesMetadata(metadata->schema, metadata->firstDataScn);
            for (const SchemaElement* element: metadata->schemaElements) {
                catalog->createSchemaForTable(metadata->firstDataScn, element->owner, element->table, element->keyList, element->key,
                                              element->tagType, element->tagList, element->tag, element->condition, element->options, updated);
                if (element->options == DbTable::OPTIONS::DEFAULT) {
                    const std::regex owner(element->owner), table(element->table);
                    bool found = false;
                    for (const auto& [object, selected]: metadata->schema->tableMap)
                        if (std::regex_match(selected->owner, owner) && std::regex_match(selected->name, table))
                            found = true;
                    if (!found)
                        throw RuntimeException(10077, "captured table filter found no table in the standby snapshot: " + element->owner + "." + element->table);
                }
            }
            metadata->schema->resetTouched();
        } // Release schema lock before snapshot publication takes checkpoint -> schema.
        if (metadata->schema->tableMap.empty())
            throw RuntimeException(10077, "no captured tables found in the consistent standby schema at START");
        metadata->checkNativeDeadline();
    }

    void ReplicatorRacOnline::prepareReplicate() {
        // Resume must qualify without another START; fresh commissioning must qualify before any successful network response.
        if (metadata->racResume)
            qualifyClosedStart();
        metadata->checkNativeDeadline();
        planStreams(collectInventory(), metadata->firstSchemaScn);
        startupPlanned = true;
        bootstrap->publish(liveIdentity);
        metadata->checkNativeDeadline();
        conn->startupMetadata = nullptr;
        ub4 callTimeout = 10000;
        env->checkErr(conn->errhp, OCIAttrSet(conn->svchp, OCI_HTYPE_SVCCTX, &callTimeout, 0, OCI_ATTR_CALL_TIMEOUT, conn->errhp));
        metadata->racDeadlineMs = 0;
        ctx->info(0, "first data SCN: " + metadata->firstDataScn.toString());
        ctx->info(0, "first schema SCN: " + metadata->firstSchemaScn.toString());
        ctx->info(0, "native RAC qualification complete: original data/schema cut " + metadata->firstDataScn.toString());
    }
}
