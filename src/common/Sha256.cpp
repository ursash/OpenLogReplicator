/* SHA-256 digest
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

#include <array>
#include <cstdint>

#include "Sha256.h"

namespace OpenLogReplicator {
    namespace {
        constexpr std::array<uint32_t, 64> K{
            0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
            0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
            0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
            0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
            0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
            0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
            0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
            0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2
        };

        constexpr uint32_t rotr(uint32_t x, int n) {
            return (x >> n) | (x << (32 - n));
        }

        void compress(std::array<uint32_t, 8>& h, const uint8_t* block) {
            std::array<uint32_t, 64> w{};
            for (int i = 0; i < 16; ++i)
                w[i] = (static_cast<uint32_t>(block[i * 4]) << 24) | (static_cast<uint32_t>(block[(i * 4) + 1]) << 16) |
                        (static_cast<uint32_t>(block[(i * 4) + 2]) << 8) | static_cast<uint32_t>(block[(i * 4) + 3]);
            for (int i = 16; i < 64; ++i) {
                const uint32_t s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
                const uint32_t s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
                w[i] = w[i - 16] + s0 + w[i - 7] + s1;
            }

            uint32_t a = h[0];
            uint32_t b = h[1];
            uint32_t c = h[2];
            uint32_t d = h[3];
            uint32_t e = h[4];
            uint32_t f = h[5];
            uint32_t g = h[6];
            uint32_t hh = h[7];
            for (int i = 0; i < 64; ++i) {
                const uint32_t t1 = hh + (rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25)) + ((e & f) ^ (~e & g)) + K[i] + w[i];
                const uint32_t t2 = (rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22)) + ((a & b) ^ (a & c) ^ (b & c));
                hh = g;
                g = f;
                f = e;
                e = d + t1;
                d = c;
                c = b;
                b = a;
                a = t1 + t2;
            }
            h[0] += a;
            h[1] += b;
            h[2] += c;
            h[3] += d;
            h[4] += e;
            h[5] += f;
            h[6] += g;
            h[7] += hh;
        }
    }

    std::string Sha256::hex(const std::string& data) {
        std::array<uint32_t, 8> h{0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
        const auto* bytes = reinterpret_cast<const uint8_t*>(data.data());
        const uint64_t size = data.size();

        uint64_t pos = 0;
        for (; pos + 64 <= size; pos += 64)
            compress(h, bytes + pos);

        // Padding: 0x80, zeros, then the bit length as a 64-bit big-endian number
        std::array<uint8_t, 128> tail{};
        const uint64_t rest = size - pos;
        for (uint64_t i = 0; i < rest; ++i)
            tail[i] = bytes[pos + i];
        tail[rest] = 0x80;
        const uint64_t tailSize = (rest < 56) ? 64 : 128;
        const uint64_t bits = size * 8;
        for (int i = 0; i < 8; ++i)
            tail[tailSize - 1 - i] = static_cast<uint8_t>(bits >> (i * 8));
        compress(h, tail.data());
        if (tailSize == 128)
            compress(h, tail.data() + 64);

        static constexpr char digits[] = "0123456789abcdef";
        std::string out;
        out.reserve(64);
        for (const uint32_t word: h)
            for (int shift = 28; shift >= 0; shift -= 4)
                out += digits[(word >> shift) & 0xF];
        return out;
    }
}
