// minitsdb/src/compression.cpp
#include "compression.h"

namespace minitsdb {

// ─── varint 编解码 ───

void EncodeVarint(uint32_t value, std::string* out) {
    while (value >= 0x80) {
        out->push_back(static_cast<char>(value | 0x80));
        value >>= 7;
    }
    out->push_back(static_cast<char>(value));
}

uint32_t DecodeVarint(const std::string& s, size_t* pos) {
    uint32_t result = 0;
    int shift = 0;
    while (true) {
        if (*pos >= s.size()) return 0xFFFFFFFF;
        uint8_t byte = static_cast<uint8_t>(s[*pos]);
        ++(*pos);
        result |= static_cast<uint32_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) return result;
        shift += 7;
        if (shift >= 35) return 0xFFFFFFFF;
    }
}

// ─── 辅助: 64-bit varint (用于首帧 timestamp) ───
static void EncodeVarint64(uint64_t value, std::string* out) {
    while (value >= 0x80) {
        out->push_back(static_cast<char>(value | 0x80));
        value >>= 7;
    }
    out->push_back(static_cast<char>(value));
}

static uint64_t DecodeVarint64(const std::string& s, size_t* pos) {
    uint64_t result = 0;
    int shift = 0;
    while (true) {
        if (*pos >= s.size()) return 0;
        uint8_t byte = static_cast<uint8_t>(s[*pos]);
        ++(*pos);
        result |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) return result;
        shift += 7;
        if (shift >= 70) return 0;
    }
}

// ─── DeltaOfDeltaEncoder ───

void DeltaOfDeltaEncoder::Add(int64_t ts) {
    if (count_ == 0) {
        // 第一个 timestamp：64-bit varint
        EncodeVarint64(static_cast<uint64_t>(ts), &buf_);
        count_ = 1;
        last_ts_ = static_cast<uint64_t>(ts);
        return;
    }

    uint64_t delta = static_cast<uint64_t>(ts) - last_ts_;
    int64_t dod = static_cast<int64_t>(delta) - static_cast<int64_t>(last_delta_);

    if (dod == 0) {
        // delta 不变（等间距）：1 bit = 0
        WriteBit(0);
    } else if (dod >= -63 && dod <= 63) {
        // 前缀 "10" + sign + 6 bits value = 9 bits total
        WriteBit(1); WriteBit(0);
        if (dod < 0) {
            WriteBit(1);
            WriteBits(6, static_cast<uint64_t>(-dod));
        } else {
            WriteBit(0);
            WriteBits(6, static_cast<uint64_t>(dod));
        }
    } else if (dod >= -8191 && dod <= 8191) {
        // 前缀 "110" + sign + 13 bits value = 17 bits total
        WriteBit(1); WriteBit(1); WriteBit(0);
        if (dod < 0) {
            WriteBit(1);
            WriteBits(13, static_cast<uint64_t>(-dod));
        } else {
            WriteBit(0);
            WriteBits(13, static_cast<uint64_t>(dod));
        }
    } else {
        // 前缀 "111" + sign + 64 bit abs_dod
        WriteBit(1); WriteBit(1); WriteBit(1);
        uint64_t abs_dod;
        if (dod < 0) {
            WriteBit(1);
            abs_dod = static_cast<uint64_t>(-dod);
        } else {
            WriteBit(0);
            abs_dod = static_cast<uint64_t>(dod);
        }
        WriteBits(64, abs_dod);
    }

    last_ts_ = static_cast<uint64_t>(ts);
    last_delta_ = delta;
    ++count_;
}

std::string DeltaOfDeltaEncoder::Finish() {
    // Flush pending bits into the payload buffer.
    if (pending_bits_ > 0) {
        buf_.push_back(static_cast<char>(block_));
        block_ = 0;
        pending_bits_ = 0;
    }
    std::string result;
    EncodeVarint64(static_cast<uint64_t>(count_), &result);
    result.append(buf_);
    Reset();
    return result;
}

void DeltaOfDeltaEncoder::Reset() {
    buf_.clear();
    last_ts_ = 0;
    last_delta_ = 0;
    block_ = 0;
    pending_bits_ = 0;
    count_ = 0;
}

void DeltaOfDeltaEncoder::WriteBit(uint64_t bit) {
    block_ |= (bit << pending_bits_);
    ++pending_bits_;
    if (pending_bits_ == 8) {
        buf_.push_back(static_cast<char>(block_));
        block_ = 0;
        pending_bits_ = 0;
    }
}

void DeltaOfDeltaEncoder::WriteBits(int num_bits, uint64_t value) {
    for (int i = num_bits - 1; i >= 0; --i) {
        WriteBit((value >> i) & 1);
    }
}

// ─── DeltaOfDeltaDecoder ───

bool DeltaOfDeltaDecoder::Init(const std::string& compressed) {
    data_ = compressed;
    pos_ = 0;
    pending_bits_ = 0;
    block_ = 0;
    last_ts_ = 0;
    last_delta_ = 0;
    count_ = 0;
    total_count_ = static_cast<int>(DecodeVarint64(data_, &pos_));
    return true;
}

uint64_t DeltaOfDeltaDecoder::ReadBit() {
    if (pending_bits_ == 0) {
        if (pos_ >= data_.size()) return 0;
        block_ = static_cast<uint8_t>(data_[pos_++]);
        pending_bits_ = 8;
    }
    uint64_t bit = (block_ & 0x01);
    block_ >>= 1;
    --pending_bits_;
    return bit;
}

uint64_t DeltaOfDeltaDecoder::ReadBits(int num_bits) {
    uint64_t val = 0;
    for (int i = 0; i < num_bits; ++i) {
        val = (val << 1) | ReadBit();
    }
    return val;
}

bool DeltaOfDeltaDecoder::Next(int64_t* ts) {
    if (count_ >= total_count_) {
        return false;
    }
    if (count_ == 0) {
        // 第一个 timestamp：读取 64-bit varint
        last_ts_ = DecodeVarint64(data_, &pos_);
        *ts = static_cast<int64_t>(last_ts_);
        ++count_;
        // 跳过 varint 已消费的字节，初始化 bit 缓冲区
        pending_bits_ = 0;
        block_ = 0;
        return true;
    }

    // 读取 delta-of-delta
    uint64_t b1 = ReadBit();
    if (b1 == 0) {
        // dod == 0
    } else {
        uint64_t b2 = ReadBit();
        if (b2 == 0) {
            // "10": sign + 6 bits
            uint64_t sign = ReadBit();
            uint64_t val = ReadBits(6);
            int64_t dod = sign ? -static_cast<int64_t>(val) : static_cast<int64_t>(val);
            last_delta_ = static_cast<uint64_t>(static_cast<int64_t>(last_delta_) + dod);
        } else {
            uint64_t b3 = ReadBit();
            if (b3 == 0) {
                // "110": sign + 13 bits
                uint64_t sign = ReadBit();
                uint64_t val = ReadBits(13);
                int64_t dod = sign ? -static_cast<int64_t>(val) : static_cast<int64_t>(val);
                last_delta_ = static_cast<uint64_t>(static_cast<int64_t>(last_delta_) + dod);
            } else {
                // "111": sign + 64 bits
                uint64_t sign = ReadBit();
                uint64_t val = ReadBits(64);
                int64_t dod = sign ? -static_cast<int64_t>(val) : static_cast<int64_t>(val);
                last_delta_ = static_cast<uint64_t>(static_cast<int64_t>(last_delta_) + dod);
            }
        }
    }

    last_ts_ += last_delta_;
    *ts = static_cast<int64_t>(last_ts_);
    ++count_;
    return true;
}

}  // namespace minitsdb
