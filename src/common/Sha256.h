/* Header for Sha256 class
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

#ifndef SHA256_H_
#define SHA256_H_

#include <string>

namespace OpenLogReplicator {
    // SHA-256 (FIPS 180-4) of a byte string as 64 lowercase hex digits; used for the identity of the rac reader types' bootstrap
    class Sha256 final {
    public:
        static std::string hex(const std::string& data);
    };
}

#endif
