#ifndef MINITSDB_MEMTABLE_H
#define MINITSDB_MEMTABLE_H

#include "skiplist.h"
#include "utils.h"

#include <cstddef>
#include <functional>
#include <shared_mutex>
#include <string>
#include <vector>

namespace minitsdb {

// MemTable 的并发模型：读写锁（std::shared_mutex）。
//   - Put / Delete：独占锁（unique_lock），写写 / 读写 互斥
//   - Get / GetEntry / RangeQuery / Iterate：共享锁（shared_lock），读读 并发
// 相比最初版本的 std::mutex（读读也互斥），这里拿回了"读读并行"的收益。
class MemTable {
public:
    MemTable() = default;

    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    void Put(const std::string& measurement, int64_t timestamp,
             const std::string& value);
    void Delete(const std::string& measurement, int64_t timestamp);

    bool Get(const std::string& measurement, int64_t timestamp,
             std::string* value) const;
    bool GetEntry(const std::string& measurement, int64_t timestamp,
                  InternalEntry* entry) const;

    void RangeQuery(const std::string& measurement, int64_t t_start,
                    int64_t t_end, std::vector<Record>* out) const;
    void RangeQueryEntries(const std::string& measurement, int64_t t_start,
                           int64_t t_end, std::vector<Record>* out) const;

    bool Empty() const;

    size_t ApproximateMemoryUsage() const {
        std::shared_lock<std::shared_mutex> lock(mu_);
        return mem_usage_;
    }

    void Iterate(const std::function<void(const std::string& measurement,
                                          int64_t timestamp,
                                          const std::string& value)>& callback) const;
    void IterateEntries(
        const std::function<void(const std::string& measurement,
                                 int64_t timestamp,
                                 const InternalEntry& entry)>& callback) const;

    size_t Count() const;

private:
    using Table = SkipList<TsKey, InternalEntry, TsKeyComparator>;

    mutable Table table_;
    mutable std::shared_mutex mu_;
    size_t mem_usage_ = 0;
};

}  // namespace minitsdb

#endif  // MINITSDB_MEMTABLE_H
