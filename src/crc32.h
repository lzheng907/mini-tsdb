// minitsdb/src/crc32.h
//
// 自实现 CRC32（IEEE 802.3 多项式 0xEDB88320），无外部依赖。
// 用于 WAL 记录校验和 SSTable Block 校验。
#ifndef MINITSDB_CRC32_H
#define MINITSDB_CRC32_H

#include <cstdint>
#include <cstddef>
#include <string>

namespace minitsdb {

// 计算 data 的 CRC32 值。initial 可传入前一次的 CRC 实现增量计算。
uint32_t Crc32(const char* data, size_t len, uint32_t initial = 0);

// 便捷重载
inline uint32_t Crc32(const std::string& s, uint32_t initial = 0) {
    return Crc32(s.data(), s.size(), initial);
}

}  // namespace minitsdb

#endif  // MINITSDB_CRC32_H
