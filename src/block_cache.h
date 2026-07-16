#ifndef MINITSDB_BLOCK_CACHE_H
#define MINITSDB_BLOCK_CACHE_H

#include <cstddef>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>

namespace minitsdb {

class BlockCache {
public:
    struct Stats {
        size_t hits = 0;
        size_t misses = 0;
        size_t bytes = 0;
        size_t entries = 0;
    };

    explicit BlockCache(size_t capacity_bytes);

    bool Get(const std::string& key, std::string* value);
    void Put(const std::string& key, const std::string& value);
    void Clear();
    Stats GetStats() const;
    size_t Capacity() const { return capacity_bytes_; }

private:
    struct Entry {
        std::string key;
        std::string value;
        size_t charge = 0;
    };

    using ListIt = std::list<Entry>::iterator;

    void PruneUnlocked();

    size_t capacity_bytes_ = 0;
    size_t usage_bytes_ = 0;
    mutable std::mutex mu_;
    std::list<Entry> lru_;
    std::unordered_map<std::string, ListIt> map_;
    size_t hits_ = 0;
    size_t misses_ = 0;
};

}  // namespace minitsdb

#endif  // MINITSDB_BLOCK_CACHE_H
