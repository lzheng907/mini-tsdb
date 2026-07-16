#include "block_cache.h"

namespace minitsdb {

BlockCache::BlockCache(size_t capacity_bytes)
    : capacity_bytes_(capacity_bytes) {}

bool BlockCache::Get(const std::string& key, std::string* value) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = map_.find(key);
    if (it == map_.end()) {
        ++misses_;
        return false;
    }
    lru_.splice(lru_.begin(), lru_, it->second);
    *value = it->second->value;
    ++hits_;
    return true;
}

void BlockCache::Put(const std::string& key, const std::string& value) {
    if (capacity_bytes_ == 0 || value.size() > capacity_bytes_) return;

    std::lock_guard<std::mutex> lock(mu_);
    auto old = map_.find(key);
    if (old != map_.end()) {
        usage_bytes_ -= old->second->charge;
        lru_.erase(old->second);
        map_.erase(old);
    }

    Entry entry;
    entry.key = key;
    entry.value = value;
    entry.charge = value.size();
    lru_.push_front(std::move(entry));
    map_[lru_.front().key] = lru_.begin();
    usage_bytes_ += lru_.front().charge;
    PruneUnlocked();
}

void BlockCache::Clear() {
    std::lock_guard<std::mutex> lock(mu_);
    lru_.clear();
    map_.clear();
    usage_bytes_ = 0;
    hits_ = 0;
    misses_ = 0;
}

BlockCache::Stats BlockCache::GetStats() const {
    std::lock_guard<std::mutex> lock(mu_);
    return Stats{hits_, misses_, usage_bytes_, lru_.size()};
}

void BlockCache::PruneUnlocked() {
    while (usage_bytes_ > capacity_bytes_ && !lru_.empty()) {
        auto& entry = lru_.back();
        usage_bytes_ -= entry.charge;
        map_.erase(entry.key);
        lru_.pop_back();
    }
}

}  // namespace minitsdb
