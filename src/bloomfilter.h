#ifndef MINITSDB_BLOOMFILTER_H
#define MINITSDB_BLOOMFILTER_H

#include <cstdint>
#include <string>
#include <vector>

namespace minitsdb {

class BloomFilter {
public:
    explicit BloomFilter(int bits_per_key = 10);

    void Add(const std::string& key);
    bool MayContain(const std::string& key) const;

    std::string Encode() const;
    static BloomFilter Decode(const std::string& data);

    size_t NumKeys() const { return num_keys_; }

private:
    void AddHashOnly(const std::string& key);
    void EnsureCapacity(size_t key_count);

    static uint32_t Hash1(const std::string& key);
    static uint32_t Hash2(const std::string& key);

    std::vector<uint8_t> bits_;
    std::vector<std::string> keys_;
    int bits_per_key_;
    int num_probes_;
    size_t num_keys_ = 0;
};

}  // namespace minitsdb

#endif  // MINITSDB_BLOOMFILTER_H
