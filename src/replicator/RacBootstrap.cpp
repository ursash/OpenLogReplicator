/* Native RAC initialization state. SPDX-License-Identifier: AGPL-3.0-or-later */
#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <regex>
#include <set>
#include <sstream>
#include <sys/file.h>
#include <unistd.h>

#include "../common/Ctx.h"
#include "../common/Sha256.h"
#include "../common/exception/RuntimeException.h"
#include "../common/types/Data.h"
#include "../metadata/Metadata.h"
#include "../metadata/Schema.h"
#include "../state/StateDisk.h"
#include "RacBootstrap.h"

namespace OpenLogReplicator {
    namespace {
        int lockState(const std::string& path, const std::string& database) {
            if (!std::regex_match(database, std::regex("[A-Za-z0-9_.-]+")))
                throw RuntimeException(10077, "rac-online requires a filename-safe logical database name");
            const int fd = open((path + "/" + database + "-rac.lock").c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
            if (fd == -1)
                throw RuntimeException(10077, "cannot open native RAC state lock: " + std::string(strerror(errno)));
            if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
                close(fd);
                throw RuntimeException(10077, "native RAC state is already in use");
            }
            return fd;
        }

        void parse(const std::string& bytes, const std::string& name, rapidjson::Document& doc) {
            if (doc.Parse(bytes.data(), bytes.size()).HasParseError() || !doc.IsObject())
                throw RuntimeException(10077, "invalid native RAC state: " + name);
        }

        std::string quoted(const std::string& value) {
            std::ostringstream out;
            out << '"';
            Data::writeEscapeValue(out, value);
            out << '"';
            return out.str();
        }

        void checkIntent(State* state, const std::string& database) {
            const std::string name(database + "-rac-intent");
            std::string bytes;
            rapidjson::Document doc;
            if (!state->read(name, 4096, bytes))
                throw RuntimeException(10077, "empty native RAC state requires the one-shot --rac-init command");
            parse(bytes, name, doc);
            Ctx::checkJsonFields(name, doc, {"intent-format", "database"});
            if (Ctx::getJsonFieldU64(name, doc, "intent-format") != 1 ||
                    Ctx::getJsonFieldS(name, Ctx::JSON_PARAMETER_LENGTH, doc, "database") != database)
                throw RuntimeException(10077, "invalid native RAC initialization intent");
        }
    }

    RacBootstrap::RacBootstrap(Metadata* newMetadata, const std::string& path): metadata(newMetadata),
            lease(lockState(path, newMetadata->database)) {}

    RacBootstrap::~RacBootstrap() {
        if (lease != -1)
            close(lease);
    }

    void RacBootstrap::createIntent(Ctx* ctx, const std::string& path, const std::string& database) {
        const int fd = lockState(path, database);
        try {
            StateDisk state(ctx, path);
            std::set<std::string> names;
            state.list(names);
            for (const std::string& name: names)
                if (name.compare(0, database.size() + 1, database + "-") == 0)
                    throw RuntimeException(10077, "--rac-init requires an empty source state; found " + name);
            state.writeAtomic(database + "-rac-intent", "{\"intent-format\":1,\"database\":" + quoted(database) + "}", false);
        } catch (...) {
            close(fd);
            throw;
        }
        close(fd);
        ctx->info(0, "native RAC initialization intent created; start OLR normally after resetting this connector's offsets");
    }

    void RacBootstrap::admit() {
        const std::string database(metadata->database);
        const std::string descriptor(database + "-rac-bootstrap");
        std::set<std::string> names;
        metadata->state->list(names);
        if (names.find(descriptor) == names.end()) {
            for (const std::string& name: names)
                if (name.compare(0, database.size() + 1, database + "-") == 0 && name != database + "-rac-intent")
                    throw RuntimeException(10077, "orphan or legacy RAC state without a native descriptor: " + name);
            checkIntent(metadata->state, database);
            return;
        }

        std::string bytes;
        rapidjson::Document doc;
        if (!metadata->state->read(descriptor, 65536, bytes))
            throw RuntimeException(10077, "native RAC descriptor cannot be read");
        parse(bytes, descriptor, doc);
        Ctx::checkJsonFields(descriptor, doc, {"descriptor-format", "profile", "database", "epoch", "fingerprint", "identity", "start-scn",
                                             "schema-file", "schema-digest", "rac-threads"});
        if (Ctx::getJsonFieldU64(descriptor, doc, "descriptor-format") != 2 ||
                Ctx::getJsonFieldS(descriptor, 128, doc, "profile") != "rac-online-closed-v1" ||
                Ctx::getJsonFieldS(descriptor, Ctx::JSON_PARAMETER_LENGTH, doc, "database") != database ||
                Ctx::getJsonFieldS(descriptor, 64, doc, "fingerprint") != metadata->racFingerprint)
            throw RuntimeException(10077, "native RAC descriptor does not match this reader/configuration");
        metadata->racEpoch = Ctx::getJsonFieldS(descriptor, 64, doc, "epoch");
        metadata->racSchemaDigest = Ctx::getJsonFieldS(descriptor, 64, doc, "schema-digest");
        if (!std::regex_match(metadata->racEpoch, std::regex("[0-9a-f]{32}")) ||
                !std::regex_match(metadata->racSchemaDigest, std::regex("[0-9a-f]{64}")))
            throw RuntimeException(10077, "invalid native RAC lineage");
        identity = Ctx::getJsonFieldS(descriptor, 16384, doc, "identity");
        const Scn cut(Ctx::getJsonFieldU64(descriptor, doc, "start-scn"));
        if (cut == Scn::none() || cut == Scn::zero())
            throw RuntimeException(10077, "native RAC descriptor has no concrete start SCN");
        const std::string schemaFile(database + "-chkpt-" + cut.toString());
        if (Ctx::getJsonFieldS(descriptor, 512, doc, "schema-file") != schemaFile)
            throw RuntimeException(10077, "native RAC descriptor points to a different schema file");
        const auto& threadJson = Ctx::getJsonFieldA(descriptor, doc, "rac-threads");
        for (rapidjson::SizeType i = 0; i < threadJson.Size(); ++i)
            metadata->racThreads.push_back(Ctx::getJsonFieldU16(descriptor, threadJson, "rac-threads", i));
        if (metadata->racThreads != std::vector<uint16_t>{1, 2})
            throw RuntimeException(10077, "rac-online first version requires participating threads 1 and 2");
        for (const std::string& name: names)
            if (name.compare(0, database.size() + 1, database + "-") == 0 && name != descriptor && name != schemaFile &&
                    name != database + "-chkpt" && name != database + "-rac-intent")
                throw RuntimeException(10077, "unexpected source state beside native RAC descriptor: " + name);
        std::string schemaBytes;
        if (!metadata->state->read(schemaFile, 2147483648ULL, schemaBytes) ||
                Sha256::hex(schemaBytes) != metadata->racSchemaDigest)
            throw RuntimeException(10077, "native RAC original schema is missing or its digest changed");
        rapidjson::Document schema;
        parse(schemaBytes, schemaFile, schema);
        if (!schema.HasMember("schema-scn") || schema.HasMember("schema-ref-scn") || schema.HasMember("min-tran") ||
                Ctx::getJsonFieldU64(schemaFile, schema, "schema-scn") != cut.getData() ||
                Ctx::getJsonFieldU64(schemaFile, schema, "scn") != cut.getData())
            throw RuntimeException(10077, "native RAC requires the exact original full schema at its start SCN");
        metadata->readCheckpoint(cut);
        if (metadata->schema->scn != cut || metadata->firstSchemaScn != cut || metadata->racSchemaInputs !=
                std::vector<std::string>{schemaFile + " " + metadata->racSchemaDigest})
            throw RuntimeException(10077, "native RAC schema load failed");
        metadata->startScn = cut;
        metadata->firstDataScn = cut;
        metadata->racResume = true;
    }

    void RacBootstrap::checkIdentity(const std::string& liveIdentity) {
        if (identity.empty() && !metadata->racResume)
            identity = liveIdentity;
        if (identity != liveIdentity)
            throw RuntimeException(10077, "live standby identity or interpretation differs from native RAC bootstrap");
    }

    void RacBootstrap::consumeIntent() {
        metadata->state->dropAtomic(metadata->database + "-rac-intent");
    }

    void RacBootstrap::publish(const std::string& liveIdentity) {
        checkIdentity(liveIdentity);
        if (metadata->racResume) {
            consumeIntent();
            return;
        }
        // No replacement is permitted, even after a publication error whose filename may already exist.
        const std::string schemaBytes(metadata->serializeBootstrapSchema());
        const std::string schemaFile(metadata->database + "-chkpt-" + metadata->firstSchemaScn.toString());
        metadata->racSchemaDigest = Sha256::hex(schemaBytes);
        std::array<uint8_t, 16> random{};
        const int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
        if (fd == -1)
            throw RuntimeException(10077, "cannot generate native RAC epoch");
        const ssize_t got = read(fd, random.data(), random.size());
        close(fd);
        if (got != static_cast<ssize_t>(random.size()))
            throw RuntimeException(10077, "cannot generate native RAC epoch");
        static const char hex[] = "0123456789abcdef";
        for (const uint8_t byte: random) {
            metadata->racEpoch += hex[byte >> 4];
            metadata->racEpoch += hex[byte & 15];
        }
        metadata->state->writeAtomic(schemaFile, schemaBytes, false);
        std::ostringstream out;
        out << R"({"descriptor-format":2,"profile":"rac-online-closed-v1","database":)" << quoted(metadata->database)
            << R"(,"epoch":)" << quoted(metadata->racEpoch) << R"(,"fingerprint":)" << quoted(metadata->racFingerprint)
            << R"(,"identity":)" << quoted(liveIdentity) << R"(,"start-scn":)" << metadata->firstDataScn.toString()
            << R"(,"schema-file":)" << quoted(schemaFile) << R"(,"schema-digest":)" << quoted(metadata->racSchemaDigest)
            << R"(,"rac-threads":[1,2]})";
        metadata->state->writeAtomic(metadata->database + "-rac-bootstrap", out.str(), false);
        consumeIntent();
        metadata->ctx->info(0, "native RAC schema and bootstrap lineage published");
    }
}
