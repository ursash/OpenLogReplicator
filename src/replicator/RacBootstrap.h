/* Native RAC initialization state. SPDX-License-Identifier: AGPL-3.0-or-later */
#ifndef RAC_BOOTSTRAP_H_
#define RAC_BOOTSTRAP_H_

#include <string>

namespace OpenLogReplicator {
    class Ctx;
    class Metadata;
    class State;

    class RacBootstrap final {
        Metadata* metadata;
        int lease{-1};
        std::string identity;

    public:
        RacBootstrap(Metadata* newMetadata, const std::string& path);
        ~RacBootstrap();
        RacBootstrap(const RacBootstrap&) = delete;
        RacBootstrap& operator=(const RacBootstrap&) = delete;

        static void createIntent(Ctx* ctx, const std::string& path, const std::string& database);
        void admit();
        void checkIdentity(const std::string& liveIdentity);
        void publish(const std::string& liveIdentity);
        void consumeIntent();
    };
}
#endif
