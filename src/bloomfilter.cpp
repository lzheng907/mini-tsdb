#include "bloomfilter.h"

#include <algorithm>
#include <cmath>
#include <cstring>

namespace minitsdb {

BloomFilter::BloomFilter(int bits_per_key) : bits_per_key_(bits_per_key) {
    bits_.resize(1024, 0);
    num_probes_ = static_cast<int>(std::ceil(std::log(2.0) * bits_per_key_));
    if (num_probes_ < 1) num_probes_ = 1;
    if (num_probes_ > 30) num_probes_ = 30;
}

void BloomFilter::Add(const std::string& key) {
    keys_.push_back(key);
    ++num_keys_;
    EnsureCapacity(num_keys_);
    AddHashOnly(key);
}

void BloomFilter::EnsureCapacity(size_t key_count) {
    size_t needed_bits = std::max<size_t>(1024 * 8, key_count * static_cast<size_t>(bits_per_key_));
    size_t needed_bytes = (needed_bits + 7) / 8;
    if (needed_bytes <= bits_.size()) return;

    size_t new_size = bits_.empty() ? 1024 : bits_.size();
    while (new_size < needed_bytes) {
        new_size *= 2;
    }
    bits_.assign(new_size, 0);
    for (const auto& key : keys_) {
        AddHashOnly(key);
    }
}

void BloomFilter::AddHashOnly(const std::string& key) {
    uint32_t h1 = Hash1(key);
    uint32_t h2 = Hash2(key);
    uint32_t bit_count = static_cast<uint32_t>(bits_.size() * 8);
    for (int i = 0; i < num_probes_; ++i) {
        uint32_t pos = (h1 + i * h2) % bit_count;
        bits_[pos / 8] |= static_cast<uint8_t>(1u << (pos % 8));
    }
}

bool BloomFilter::MayContain(const std::string& key) const {
    if (bits_.empty()) return false;
    uint32_t h1 = Hash1(key);
    uint32_t h2 = Hash2(key);
    uint32_t bit_count = static_cast<uint32_t>(bits_.size() * 8);
    for (int i = 0; i < num_probes_; ++i) {
        uint32_t pos = (h1 + i * h2) % bit_count;
        if ((bits_[pos / 8] & (1u << (pos % 8))) == 0) return false;
    }
    return true;
}

std::string BloomFilter::Encode() const {
    std::string result;
    result.resize(12 + bits_.size());
    std::memcpy(&result[0], &num_probes_, 4);
    uint32_t blen = static_cast<uint32_t>(bits_.size());
    std::memcpy(&result[4], &blen, 4);
    uint32_t keys = static_cast<uint32_t>(num_keys_);
    std::memcpy(&result[8], &keys, 4);
    std::memcpy(&result[12], bits_.data(), bits_.size());
    return result;
}

BloomFilter BloomFilter::Decode(const std::string& data) {
    if (data.size() < 8) return BloomFilter();
    int num_probes;
    uint32_t blen;
    std::memcpy(&num_probes, data.data(), 4);
    std::memcpy(&blen, data.data() + 4, 4);
    size_t header_size = data.size() >= 12 + blen ? 12 : 8;

    BloomFilter bf;
    bf.num_probes_ = num_probes;
    bf.bits_.resize(blen);
    if (header_size == 12) {
        uint32_t keys = 0;
        std::memcpy(&keys, data.data() + 8, 4);
        bf.num_keys_ = keys;
    }
    if (data.size() >= header_size + blen) {
        std::memcpy(bf.bits_.data(), data.data() + header_size, blen);
    }
    return bf;
}

uint32_t BloomFilter::Hash1(const std::string& key) {
    uint32_t h = 2166136261u;
    for (unsigned char c : key) {
        h ^= c;
        h *= 16777619u;
    }
    return h;
}

uint32_t BloomFilter::Hash2(const std::string& key) {
    uint32_t h = 0x12345678u;
    for (unsigned char c : key) {
        h ^= static_cast<uint32_t>(c) + 0x9e3779b9u + (h << 6) + (h >> 2);
    }
    h |= 1u;
    return h;
}

}  // namespace minitsdb
