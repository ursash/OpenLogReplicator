/* Header for ReplicatorOnline class
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

#ifndef REPLICATOR_ONLINE_H_
#define REPLICATOR_ONLINE_H_

#include "Replicator.h"
#include "../common/DbTable.h"
#include "../metadata/SchemaElement.h"

namespace OpenLogReplicator {
    class DatabaseConnection;
    class DatabaseEnvironment;
    class Schema;
    class OnlineCatalog;

    class ReplicatorOnline final : public Replicator {
    protected:
        bool standby{false};
        OnlineCatalog* catalog{nullptr};

        void positionReader() override;
        void loadDatabaseMetadata() override;
        bool checkConnection() override;




        std::string getModeName() const override;
        void verifySchema(Scn currentScn) override;
        void createSchema() override;




        void updateOnlineRedoLogData() override;

    public:
        DatabaseEnvironment* env;
        DatabaseConnection* conn;
        bool keepConnection;

        ReplicatorOnline(Ctx* newCtx, void (*newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
                         TransactionBuffer* newTransactionBuffer, std::string newAlias, std::string newDatabase, std::string newUser,
                         std::string newPassword, std::string newConnectString, bool newKeepConnection);
        ~ReplicatorOnline() override;

        void goStandby() override;

        static void archGetLogOnline(Replicator* replicator);
    };
}

#endif
