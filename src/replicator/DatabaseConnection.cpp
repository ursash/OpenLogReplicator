/* Class to handle database connection
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

#include "DatabaseConnection.h"
#include "DatabaseEnvironment.h"
#include "../metadata/Metadata.h"
#include "../common/exception/RuntimeException.h"
#include <algorithm>
#include <chrono>
#include <regex>

namespace OpenLogReplicator {
    DatabaseConnection::DatabaseConnection(DatabaseEnvironment* newEnv, std::string newUser, std::string newPassword, std::string newConnectString,
                                           bool newSysAsm):
            user(std::move(newUser)),
            password(std::move(newPassword)),
            connectString(std::move(newConnectString)),
            sysAsm(newSysAsm),
            env(newEnv) {}

    void DatabaseConnection::connect() {
        disconnect();

        OCIHandleAlloc(env->envhp, reinterpret_cast<dvoid**>(&errhp), OCI_HTYPE_ERROR, 0, nullptr);
        OCIHandleAlloc(env->envhp, reinterpret_cast<dvoid**>(&srvhp), OCI_HTYPE_SERVER, 0, nullptr);
        OCIHandleAlloc(env->envhp, reinterpret_cast<dvoid**>(&svchp), OCI_HTYPE_SVCCTX, 0, nullptr);
        OCIHandleAlloc(env->envhp, reinterpret_cast<dvoid**>(&authp), OCI_HTYPE_SESSION, 0, nullptr);

        std::string endpoint(connectString);
        if (startupMetadata != nullptr) {
            startupMetadata->checkNativeDeadline();
            // Bound attachment as well as subsequent OCI round trips, without changing the customer's saved endpoint.
            if (!std::regex_match(endpoint, std::regex("//[A-Za-z0-9_.-]+:[0-9]+/[A-Za-z0-9_.-]+")))
                throw RuntimeException(10077, "rac-online currently requires the stock //host:port/service connection form");
            const int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
            const int64_t remaining = startupMetadata->racDeadlineMs - now;
            if (remaining <= 0)
                throw RuntimeException(10077, "native RAC connection attachment deadline exceeded");
            const std::string timeout = std::to_string(std::min<int64_t>(10000, remaining)) + "ms";
            endpoint += "?connect_timeout=" + timeout + "&transport_connect_timeout=" + timeout + "&retry_count=0";
        }
        env->checkErr(errhp, OCIServerAttach(srvhp, errhp, reinterpret_cast<const OraText*>(endpoint.c_str()), endpoint.length(), OCI_DEFAULT));
        env->checkErr(errhp, OCIAttrSet(svchp, OCI_HTYPE_SVCCTX, srvhp, 0,
                                        OCI_ATTR_SERVER, errhp));
        checkStartupDeadline();
        env->checkErr(errhp, OCIAttrSet(authp, OCI_HTYPE_SESSION,
                                        const_cast<dvoid*>(reinterpret_cast<const dvoid*>(user.c_str())), user.length(),
                                        OCI_ATTR_USERNAME, errhp));
        env->checkErr(errhp, OCIAttrSet(authp, OCI_HTYPE_SESSION,
                                        const_cast<dvoid*>(reinterpret_cast<const dvoid*>(password.c_str())), password.length(),
                                        OCI_ATTR_PASSWORD, errhp));

        if (sysAsm)
            env->checkErr(errhp, OCISessionBegin(svchp, errhp, authp, OCI_CRED_RDBMS, OCI_SYSASM));
        else
            env->checkErr(errhp, OCISessionBegin(svchp, errhp, authp, OCI_CRED_RDBMS, OCI_DEFAULT));

        env->checkErr(errhp, OCIAttrSet(svchp, OCI_HTYPE_SVCCTX,
                                        authp, 0, OCI_ATTR_SESSION, errhp));

        connected = true;
    }

    void DatabaseConnection::checkStartupDeadline() {
        if (startupMetadata == nullptr)
            return;
        startupMetadata->checkNativeDeadline();
        const int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
        const int64_t remaining = startupMetadata->racDeadlineMs - now;
        ub4 timeout = static_cast<ub4>(std::max<int64_t>(1, std::min<int64_t>(10000, remaining)));
        env->checkErr(errhp, OCIAttrSet(svchp, OCI_HTYPE_SVCCTX, &timeout, 0, OCI_ATTR_CALL_TIMEOUT, errhp));
    }

    void DatabaseConnection::disconnect() {
        if (svchp != nullptr && errhp != nullptr)
            OCISessionEnd(svchp, errhp, nullptr, OCI_DEFAULT);

        if (svchp != nullptr && errhp != nullptr)
            OCIServerDetach(srvhp, errhp, OCI_DEFAULT);

        if (authp != nullptr) {
            OCIHandleFree(authp, OCI_HTYPE_SESSION);
            authp = nullptr;
        }

        if (errhp != nullptr) {
            OCIServerDetach(srvhp, errhp, OCI_DEFAULT);
            errhp = nullptr;
        }

        if (svchp != nullptr) {
            OCIHandleFree(svchp, OCI_HTYPE_SVCCTX);
            svchp = nullptr;
        }

        if (authp != nullptr) {
            OCIHandleFree(authp, OCI_HTYPE_SERVER);
            authp = nullptr;
        }

        if (errhp != nullptr) {
            OCIHandleFree(errhp, OCI_HTYPE_ERROR);
            errhp = nullptr;
        }

        connected = false;
    }

    DatabaseConnection::~DatabaseConnection() {
        if (connected)
            disconnect();
    }
}
