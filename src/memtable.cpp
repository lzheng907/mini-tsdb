#include "memtable.h"

#include <mutex>

namespace minitsdb {

namespace {

Record ToRecord(const TsKey& key, const InternalEntry& entry) {
    return Record{key.measurement, key.timestamp, entry.value, entry.IsDeletion()};
}

}  // namespace

void MemTable::Put(const std::string& measurement, int64_t timestamp,
                   const std::string& value) {
    std::unique_lock<std::shared_mutex> lock(mu_);  // 写锁
    TsKey key{measurement, timestamp};
    table_.Insert(key, InternalEntry{value, EntryType::kValue});
    mem_usage_ += measurement.size() + value.size() + sizeof(int64_t) + 64;
}

void MemTable::Delete(const std::string& measurement, int64_t timestamp) {
    std::unique_lock<std::shared_mutex> lock(mu_);  // 写锁
    TsKey key{measurement, timestamp};
    table_.Insert(key, InternalEntry{"", EntryType::kDeletion});
    mem_usage_ += measurement.size() + sizeof(int64_t) + 64;
}

bool MemTable::Get(const std::string& measurement, int64_t timestamp,
                   std::string* value) const {
    InternalEntry entry;
    // Get 自身不加锁，复用 GetEntry 的锁；若这里再加会导致同一把非重入锁死锁。
    if (!GetEntry(measurement, timestamp, &entry) || entry.IsDeletion()) {
        return false;
    }
    *value = entry.value;
    return true;
}

bool MemTable::GetEntry(const std::string& measurement, int64_t timestamp,
                        InternalEntry* entry) const {
    std::shared_lock<std::shared_mutex> lock(mu_);  // 读锁：多个 Get 可并发
    TsKey key{measurement, timestamp};
    return table_.Get(key, entry);
}

void MemTable::RangeQuery(const std::string& measurement, int64_t t_start,
                          int64_t t_end, std::vector<Record>* out) const {
    std::vector<Record> entries;
    RangeQueryEntries(measurement, t_start, t_end, &entries);
    out->clear();
    for (const auto& record : entries) {
        if (!record.deleted) {
            out->push_back(record);
        }
    }
}

void MemTable::RangeQueryEntries(const std::string& measurement, int64_t t_start,
                                 int64_t t_end, std::vector<Record>* out) const {
    out->clear();
    if (t_end < t_start) return;

    std::shared_lock<std::shared_mutex> lock(mu_);  // 读锁

    TsKey begin{measurement, t_start};
    Table::Node* node = table_.FindGreaterOrEqual(begin);

    TsKeyComparator cmp;
    TsKey upper_bound{measurement, t_end};
    while (node != nullptr) {
        const TsKey& nk = table_.KeyOf(node);
        if (cmp(nk, upper_bound) > 0) break;
        out->push_back(ToRecord(nk, table_.ValueOf(node)));
        node = table_.NextOf(node);
    }
}

bool MemTable::Empty() const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    return table_.FindGreaterOrEqual(TsKey{}) == nullptr;
}

void MemTable::Iterate(
    const std::function<void(const std::string&, int64_t,
                             const std::string&)>& callback) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    auto* node = table_.FindGreaterOrEqual(TsKey{});
    while (node != nullptr) {
        const TsKey& k = table_.KeyOf(node);
        const InternalEntry& entry = table_.ValueOf(node);
        if (!entry.IsDeletion()) {
            callback(k.measurement, k.timestamp, entry.value);
        }
        node = table_.NextOf(node);
    }
}

void MemTable::IterateEntries(
    const std::function<void(const std::string&, int64_t,
                             const InternalEntry&)>& callback) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    auto* node = table_.FindGreaterOrEqual(TsKey{});
    while (node != nullptr) {
        const TsKey& k = table_.KeyOf(node);
        callback(k.measurement, k.timestamp, table_.ValueOf(node));
        node = table_.NextOf(node);
    }
}

size_t MemTable::Count() const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    return table_.Count();
}

}  // namespace minitsdb
