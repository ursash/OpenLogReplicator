/* Online-discovered RAC replication. SPDX-License-Identifier: AGPL-3.0-or-later */
#ifndef REPLICATOR_RAC_ONLINE_H_
#define REPLICATOR_RAC_ONLINE_H_

#include <memory>
#include "ReplicatorRac.h"

namespace OpenLogReplicator {
    class DatabaseEnvironment;
    class DatabaseConnection;
    class OnlineCatalog;
    class RacBootstrap;

    class ReplicatorRacOnline final : public ReplicatorRac {
        std::unique_ptr<DatabaseEnvironment> env;
        std::unique_ptr<DatabaseConnection> conn;
        std::unique_ptr<OnlineCatalog> catalog;
        std::unique_ptr<RacBootstrap> bootstrap;
        std::string liveIdentity;
        bool identityInitialized{false};

        void loadDatabaseMetadata() override;
        void positionReader() override;
        void createSchema() override;
        void prepareReplicate() override;
        std::string getModeName() const override;
        std::vector<RedoFile> collectInventory() override;
        Scn readIdentity();
        void qualifyClosedStart();

    public:
        ReplicatorRacOnline(Ctx* newCtx, Builder* newBuilder, Metadata* newMetadata, TransactionBuffer* newTransactionBuffer,
                            const std::string& newAlias, const std::string& newDatabase, const std::string& user,
                            const std::string& password, const std::string& server);
        ~ReplicatorRacOnline() override;
        void admit(const std::string& statePath);
    };
}
#endif
