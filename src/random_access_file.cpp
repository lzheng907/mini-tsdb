#include "random_access_file.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace minitsdb {

namespace {

#ifdef _WIN32
std::string LastErrorMessage(const std::string& prefix) {
    return prefix + ": error " + std::to_string(GetLastError());
}
#else
std::string ErrnoMessage(const std::string& prefix) {
    return prefix + ": " + std::strerror(errno);
}
#endif

}  // namespace

RandomAccessFile::RandomAccessFile(const std::string& fname) : fname_(fname) {}

RandomAccessFile::~RandomAccessFile() {
#ifdef _WIN32
    if (handle_ != INVALID_HANDLE_VALUE) {
        CloseHandle(handle_);
        handle_ = INVALID_HANDLE_VALUE;
    }
#else
    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
#endif
}

Status RandomAccessFile::Open() {
#ifdef _WIN32
    handle_ = CreateFileA(fname_.c_str(), GENERIC_READ,
                          FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                          nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (handle_ == INVALID_HANDLE_VALUE) {
        return Status::IOError(LastErrorMessage("Cannot open " + fname_));
    }
#else
    fd_ = open(fname_.c_str(), O_RDONLY);
    if (fd_ < 0) {
        return Status::IOError(ErrnoMessage("Cannot open " + fname_));
    }
#endif
    return Status::OK();
}

Status RandomAccessFile::ReadAt(uint64_t offset, size_t n, std::string* out) const {
    out->assign(n, '\0');
    size_t done = 0;

    while (done < n) {
#ifdef _WIN32
        size_t remaining = n - done;
        DWORD chunk = static_cast<DWORD>(
            std::min<size_t>(remaining, (std::numeric_limits<DWORD>::max)()));
        OVERLAPPED overlapped = {};
        uint64_t chunk_offset = offset + done;
        overlapped.Offset = static_cast<DWORD>(chunk_offset & 0xFFFFFFFFu);
        overlapped.OffsetHigh = static_cast<DWORD>(chunk_offset >> 32);

        DWORD bytes_read = 0;
        BOOL ok = ReadFile(handle_, &(*out)[done], chunk, &bytes_read, &overlapped);
        if (!ok) {
            out->clear();
            return Status::IOError(LastErrorMessage("Failed to read " + fname_));
        }
        if (bytes_read == 0) {
            out->clear();
            return Status::IOError("Short read: " + fname_);
        }
        done += bytes_read;
#else
        size_t remaining = n - done;
        size_t chunk = std::min<size_t>(
            remaining, static_cast<size_t>(std::numeric_limits<ssize_t>::max()));
        ssize_t bytes_read = pread(fd_, &(*out)[done], chunk,
                                   static_cast<off_t>(offset + done));
        if (bytes_read < 0) {
            if (errno == EINTR) continue;
            out->clear();
            return Status::IOError(ErrnoMessage("Failed to read " + fname_));
        }
        if (bytes_read == 0) {
            out->clear();
            return Status::IOError("Short read: " + fname_);
        }
        done += static_cast<size_t>(bytes_read);
#endif
    }

    return Status::OK();
}

Status RandomAccessFile::Size(uint64_t* size) const {
#ifdef _WIN32
    LARGE_INTEGER file_size;
    if (!GetFileSizeEx(handle_, &file_size)) {
        return Status::IOError(LastErrorMessage("Failed to stat " + fname_));
    }
    *size = static_cast<uint64_t>(file_size.QuadPart);
#else
    struct stat st;
    if (fstat(fd_, &st) != 0) {
        return Status::IOError(ErrnoMessage("Failed to stat " + fname_));
    }
    *size = static_cast<uint64_t>(st.st_size);
#endif
    return Status::OK();
}

}  // namespace minitsdb
