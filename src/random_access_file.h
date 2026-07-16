#ifndef MINITSDB_RANDOM_ACCESS_FILE_H
#define MINITSDB_RANDOM_ACCESS_FILE_H

#include "status.h"

#include <cstddef>
#include <cstdint>
#include <string>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#endif

namespace minitsdb {

class RandomAccessFile {
public:
    explicit RandomAccessFile(const std::string& fname);
    ~RandomAccessFile();

    RandomAccessFile(const RandomAccessFile&) = delete;
    RandomAccessFile& operator=(const RandomAccessFile&) = delete;

    Status Open();
    Status ReadAt(uint64_t offset, size_t n, std::string* out) const;
    Status Size(uint64_t* size) const;

private:
    std::string fname_;

#ifdef _WIN32
    HANDLE handle_ = INVALID_HANDLE_VALUE;
#else
    int fd_ = -1;
#endif
};

}  // namespace minitsdb

#endif  // MINITSDB_RANDOM_ACCESS_FILE_H
