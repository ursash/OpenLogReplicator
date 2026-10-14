/* Base class for state in files on disk
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

#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <sys/stat.h>
#include <unistd.h>

#include "../common/Ctx.h"
#include "../common/exception/RuntimeException.h"
#include "StateDisk.h"

namespace OpenLogReplicator {
    StateDisk::StateDisk(Ctx* newCtx, std::string newPath):
            State(newCtx),
            path(std::move(newPath)) {}

    void StateDisk::list(std::set<std::string>& namesList) const {
        DIR* dir = opendir(path.c_str());
        if (dir == nullptr)
            throw RuntimeException(10012, "directory: " + path + " - can't read");

        const dirent* ent;
        while ((ent = readdir(dir)) != nullptr) {
            const std::string dName(ent->d_name);
            if (dName == "." || dName == "..")
                continue;

            struct stat fileStat{};
            const std::string fileName(ent->d_name);

            const std::string fullName(path + "/" + ent->d_name);
            if (stat(fullName.c_str(), &fileStat) != 0) {
                ctx->warning(10003, "file: " + fileName + " - get metadata returned: " + strerror(errno));
                continue;
            }

            if (S_ISDIR(fileStat.st_mode))
                continue;

            const std::string suffix(".json");
            if (fileName.length() < suffix.length() || fileName.substr(fileName.length() - suffix.length(), fileName.length()) != suffix)
                continue;

            const std::string fileBase(fileName.substr(0, fileName.length() - suffix.length()));
            namesList.insert(fileBase);
        }
        closedir(dir);
    }

    bool StateDisk::read(const std::string& name, uint64_t maxSize, std::string& in) {
        const std::string fileName(path + "/" + name + ".json");
        struct stat fileStat{};
        if (stat(fileName.c_str(), &fileStat) != 0) {
            ctx->warning(10003, "file: " + fileName + " - get metadata returned: " + strerror(errno));
            return false;
        }
        if (static_cast<uint64_t>(fileStat.st_size) > maxSize || fileStat.st_size == 0)
            throw RuntimeException(10004, "file: " + fileName + " - wrong size: " + std::to_string(fileStat.st_size));

        in.resize(fileStat.st_size);
        std::ifstream inputStream;
        inputStream.open(fileName.c_str(), std::ios::in);

        if (!inputStream.is_open())
            throw RuntimeException(10001, "file: " + fileName + " - open for read returned: " + strerror(errno));

        inputStream.read(in.data(), fileStat.st_size);
        if (!inputStream)
            throw RuntimeException(10001, "file: " + fileName + " - read returned: " + strerror(errno));

        inputStream.close();
        return true;
    }

    void StateDisk::write(const std::string& name, Scn scn __attribute__((unused)), const std::ostringstream& out) {
        const std::string fileName(path + "/" + name + ".json");
        std::ofstream outputStream;

        outputStream.open(fileName.c_str(), std::ios::out | std::ios::trunc);
        if (!outputStream.is_open())
            throw RuntimeException(10006, "file: " + fileName + " - open for writing returned: " + strerror(errno));

        outputStream << out.str();
        if (outputStream.bad() || outputStream.fail())
            throw RuntimeException(10007, "file: " + fileName + " - 0 bytes written instead of " +
                                          std::to_string(out.str().length()) + ", code returned: " + strerror(errno));

        outputStream.close();
    }

    // Writes "<name>.json.tmp", syncs it and publishes it as "<name>.json" (rename, or with replace false a hard link that fails when
    // the name exists), then syncs the directory. A crash leaves the old content or the new one, never a torn file; list() only
    // reports ".json" names, so a left temporary file is never taken for state
    void StateDisk::writeAtomic(const std::string& name, const std::string& out, bool replace) {
        const std::string fileName(path + "/" + name + ".json");
        const std::string tmpName(fileName + ".tmp");

        // A temporary file left by a crash may still be a hard link of the published file: never write through it
        if (unlink(tmpName.c_str()) != 0 && errno != ENOENT)
            throw RuntimeException(10078, "file: " + tmpName + " - delete returned: " + strerror(errno));
        const int fd = open(tmpName.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
        if (fd == -1)
            throw RuntimeException(10006, "file: " + tmpName + " - open for writing returned: " + strerror(errno));

        uint64_t written = 0;
        while (written < out.size()) {
            const ssize_t bytes = ::write(fd, out.data() + written, out.size() - written);
            if (bytes < 0 && errno == EINTR)
                continue;
            if (bytes <= 0) {
                const std::string error(strerror(errno));
                close(fd);
                throw RuntimeException(10007, "file: " + tmpName + " - " + std::to_string(written) + " bytes written instead of " +
                                       std::to_string(out.size()) + ", code returned: " + error);
            }
            written += bytes;
        }
        if (fsync(fd) != 0) {
            const std::string error(strerror(errno));
            close(fd);
            throw RuntimeException(10078, "file: " + tmpName + " - fsync returned: " + error);
        }
        if (close(fd) != 0)
            throw RuntimeException(10078, "file: " + tmpName + " - close returned: " + strerror(errno));

        if (replace) {
            if (rename(tmpName.c_str(), fileName.c_str()) != 0)
                throw RuntimeException(10078, "file: " + tmpName + " - rename to " + fileName + " returned: " + strerror(errno));
        } else {
            if (link(tmpName.c_str(), fileName.c_str()) != 0) {
                const std::string error(strerror(errno));
                unlink(tmpName.c_str());
                throw RuntimeException(10078, "file: " + fileName + " - create from " + tmpName + " returned: " + error);
            }
            unlink(tmpName.c_str());
        }

        const int dirFd = open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dirFd == -1)
            throw RuntimeException(10078, "directory: " + path + " - open returned: " + strerror(errno));
        if (fsync(dirFd) != 0) {
            const std::string error(strerror(errno));
            close(dirFd);
            throw RuntimeException(10078, "directory: " + path + " - fsync returned: " + error);
        }
        close(dirFd);
    }

    void StateDisk::drop(const std::string& name) {
        const std::string fileName(path + "/" + name + ".json");
        if (unlink(fileName.c_str()) != 0)
            throw RuntimeException(10010, "file: " + fileName + " - delete returned: " + strerror(errno));
    }

    void StateDisk::dropAtomic(const std::string& name) {
        const std::string fileName(path + "/" + name + ".json");
        if (unlink(fileName.c_str()) != 0 && errno != ENOENT)
            throw RuntimeException(10010, "file: " + fileName + " - delete returned: " + strerror(errno));
        const int fd = open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (fd == -1)
            throw RuntimeException(10078, "directory: " + path + " - open for sync returned: " + strerror(errno));
        const int result = fsync(fd);
        const int savedErrno = errno;
        close(fd);
        if (result != 0)
            throw RuntimeException(10078, "directory: " + path + " - sync returned: " + strerror(savedErrno));
    }
}
