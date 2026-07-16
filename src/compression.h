// minitsdb/src/compression.h
//
// 时间戳 delta-of-delta 压缩 (Gorilla 论文方法)。
// 时序数据中连续样本的 timestamp 通常等间距（如每 10ms），
// delta-of-delta 编码能把大多数时间戳压缩到 1-2 字节。
#ifndef MINITSDB_COMPRESSION_H
#define MINITSDB_COMPRESSION_H

#include <cstdint>
#include <string>
#include <vector>

namespace minitsdb {

// 编码器：按顺序喂入 timestamp，压缩成二进制串。
class DeltaOfDeltaEncoder {
public:
    DeltaOfDeltaEncoder() = default;

    // 喂入一个 timestamp（必须按升序）。
    void Add(int64_t ts);

    // 输出压缩后的二进制数据。
    std::string Finish();

    // 重置编码器状态
    void Reset();

private:
    void WriteBit(uint64_t bit);
    void WriteBits(int num_bits, uint64_t value);

    std::string buf_;
    uint64_t last_ts_ = 0;
    uint64_t last_delta_ = 0;
    uint64_t block_ = 0;       // 当前正在拼的比特块
    int pending_bits_ = 0;     // block_ 中已积攒的比特数
    int count_ = 0;            // 已编码的 ts 数量
};

// 解码器：从压缩二进制串还原 timestamp 序列。
class DeltaOfDeltaDecoder {
public:
    DeltaOfDeltaDecoder() = default;

    // 初始化解码器，传入压缩数据。
    bool Init(const std::string& compressed);

    // 解码下一个 timestamp。返回 false 如果已耗尽。
    bool Next(int64_t* ts);

    int DecodedCount() const { return count_; }

private:
    uint64_t ReadBit();
    uint64_t ReadBits(int num_bits);

    std::string data_;
    size_t pos_ = 0;
    int pending_bits_ = 0;
    uint64_t block_ = 0;
    uint64_t last_ts_ = 0;
    uint64_t last_delta_ = 0;
    int count_ = 0;
    int total_count_ = 0;
};

// 通用小整数变长编码 (varint): 每字节 7 bit payload + 1 bit continuation。
// 用于 SSTable 前缀压缩中的长度字段。
void EncodeVarint(uint32_t value, std::string* out);
// 返回解码后的值，并前移 *pos。返回 0xFFFFFFFF 表示溢出。
uint32_t DecodeVarint(const std::string& s, size_t* pos);

}  // namespace minitsdb

#endif  // MINITSDB_COMPRESSION_H
