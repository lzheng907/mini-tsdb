#ifndef MINITSDB_TSDB_ENGINE_H
#define MINITSDB_TSDB_ENGINE_H

#include "block_cache.h"
#include "compaction.h"
#include "memtable.h"
#include "sstable.h"
#include "status.h"
#include "version.h"
#include "wal.h"
#include "write_batch.h"

#include <atomic>
#include <array>
#include <condition_variable>  // std::condition_variable_any
#include <cstddef>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace minitsdb {

class TsdbEngine {
public:
    struct Options {
        int64_t memtable_limit = 4 * 1024 * 1024;
        bool sync_wal = true;
        size_t block_cache_bytes = 512 * 1024;
        uint64_t target_file_size = 2 * 1024 * 1024;
    };

    struct EngineStats {
        size_t version_files = 0;
        std::array<size_t, Version::kMaxLevel> level_files{};
        size_t disk_sst_files = 0;
        uint64_t version_file_bytes = 0;
        size_t sstable_candidate_count = 0;
        size_t sstable_probe_count = 0;
        size_t memtable_bytes = 0;
        size_t immutable_bytes = 0;
        bool flush_in_progress = false;
        size_t block_cache_hits = 0;
        size_t block_cache_misses = 0;
        size_t block_cache_bytes = 0;
        size_t block_cache_entries = 0;
        bool compaction_in_progress = false;
        bool compaction_pending = false;
        size_t compacting_files = 0;
        size_t pending_delete_files = 0;
    };

    explicit TsdbEngine(const std::string& db_dir,
                        int64_t memtable_limit = 4 * 1024 * 1024);
    explicit TsdbEngine(const std::string& db_dir, const Options& options);
    ~TsdbEngine();

    TsdbEngine(const TsdbEngine&) = delete;
    TsdbEngine& operator=(const TsdbEngine&) = delete;

    enum class Agg { kAvg, kMax, kMin, kLast };

    Status Put(const std::string& measurement, int64_t timestamp,
               const std::string& value);
    Status Delete(const std::string& measurement, int64_t timestamp);
    Status Write(const WriteBatch& batch);
    bool Get(const std::string& measurement, int64_t timestamp,
             std::string* value);
    void RangeQuery(const std::string& measurement, int64_t t_start,
                    int64_t t_end, std::vector<Record>* out);

    void SetTTL(const std::string& measurement, int64_t ttl_seconds);
    void Downsample(const std::string& measurement, int64_t t_start,
                    int64_t t_end, int64_t interval_ns, Agg agg,
                    std::vector<Record>* out);

    size_t MemUsage();
    void Flush();
    void Compact(bool cleanup_obsolete = true);
    void CleanupObsoleteFiles();
    void ResetReadStats();
    EngineStats GetStats() const;
    void Close();

    std::function<void(const std::string& fname, std::vector<Record>* records)>
    MakeSSTReader();

    const std::string& DbDir() const { return db_dir_; }

private:
    struct TtlRule {
        int64_t ttl_seconds = 0;
        int64_t latest_timestamp = INT64_MIN;
    };

    struct RankedRecord {
        Record record;
        int rank = 0;
    };

    struct ReaderRef {
        std::shared_ptr<FileMeta> meta;
        std::shared_ptr<SSTableReader> reader;
    };

    struct ReadView {
        std::shared_ptr<MemTable> mem;
        std::shared_ptr<MemTable> imm;
        std::array<std::vector<ReaderRef>, Version::kMaxLevel> levels;
        std::unordered_map<std::string, TtlRule> ttl_map;
    };

    struct CompactionWork {
        CompactionJob job;
        std::unordered_map<std::string, TtlRule> ttl_snapshot;
    };

    struct PendingDelete {
        std::string file_name;
        std::shared_ptr<SSTableReader> reader;
    };

    void FlushThread();
    void CompactionThread();
    void MaybeScheduleCompaction();
    bool RunOneCompaction(bool force, bool cleanup_obsolete);
    void RunCompactionWork(CompactionWork work, bool cleanup_obsolete);
    bool PickCompactionWorkUnlocked(bool force, CompactionWork* work) const;
    void MarkCompactionJobStartedUnlocked(const CompactionJob& job);
    void MarkCompactionJobFinishedUnlocked(const CompactionJob& job);
    bool IsCompactionInputValidUnlocked(const CompactionJob& job) const;
    size_t L0TotalFileCountUnlocked() const;
    size_t L0NonCompactingFileCountUnlocked() const;
    bool ShouldScheduleCompactionUnlocked() const;
    bool ShouldStopWritesUnlocked() const;
    bool GetFromSSTables(const ReadView& view, const TsKey& key, InternalEntry* entry);
    ReadView CaptureReadView() const;
    static bool IsExpiredInMap(const std::unordered_map<std::string, TtlRule>& ttl_map,
                               const std::string& measurement, int64_t timestamp);
    static bool IsExpiredByRule(const TtlRule& rule, int64_t timestamp);
    bool LoadExistingSSTables();
    void MakeRoomForWriteUnlocked(std::unique_lock<std::shared_mutex>& lock);
    void AddSourceRecords(const std::vector<Record>& records, int rank,
                          std::vector<RankedRecord>* out) const;
    size_t CountSSTFilesOnDisk() const;
    void GarbageCollectSSTablesOnStartupUnlocked();

    // 读路径辅助：所有 SSTable reader 必须在读路径执行前由 flush/compaction
    // 在写锁下预打开，使 Get/RangeQuery 成为纯读路径、可由读锁保护。
    std::shared_ptr<SSTableReader> FindReaderUnlocked(const std::string& fname) const;
    void EnsureReadersForVersionUnlocked();
    void CleanupObsoleteFilesUnlocked();
    void TryDeletePendingFilesUnlocked();
    void UpdateLatestTimestampUnlocked(const std::string& measurement,
                                       int64_t timestamp);
    static bool KeepRecordForCompaction(
        const Record& record,
        const std::unordered_map<std::string, TtlRule>& ttl_map);
    void OpenReadersForCurrentVersionUnlocked();
    void DeleteFilesOutsideLock(std::vector<std::string> files);

    std::string db_dir_;
    Options options_;
    int64_t memtable_limit_;

    WAL wal_;
    std::shared_ptr<MemTable> mem_;
    std::shared_ptr<MemTable> imm_;
    bool flush_in_progress_ = false;
    // 读写锁：写路径(Put/Delete/Flush 切换/Compaction 收尾)独占，
    // 读路径(Get/RangeQuery/MemUsage)共享，从而读读可并发。
    mutable std::shared_mutex mu_;
    // condition_variable 只支持 std::mutex；改用 condition_variable_any 以适配 shared_mutex。
    std::condition_variable_any flush_cv_;
    std::condition_variable_any flush_done_cv_;
    std::condition_variable_any compaction_cv_;
    std::condition_variable_any compaction_done_cv_;

    std::thread flush_thread_;
    std::thread compaction_thread_;
    std::atomic<bool> running_{true};
    // Close() 重入保护：与 running_ 解耦。running_ 必须保持 true 直到最终
    // Flush() 完成，否则后台线程可能先于 Flush() 退出而导致死锁。
    std::atomic<bool> closed_{false};

    VersionManager vmgr_;
    std::vector<std::shared_ptr<SSTableReader>> sst_readers_;
    std::unique_ptr<Compaction> compaction_;
    BlockCache block_cache_;
    std::unordered_map<std::string, TtlRule> ttl_map_;
    bool compaction_in_progress_ = false;
    bool compaction_pending_ = false;
    std::unordered_set<uint64_t> compacting_files_;
    std::vector<PendingDelete> pending_delete_;
    static constexpr size_t kL0CompactionTrigger = 4;
    static constexpr size_t kL0StopTrigger = 12;
    static constexpr int64_t kLevelSizeBase = 1 * 1024 * 1024;
    // 读路径统计：多个 Get 在读锁下并发递增，必须用原子变量避免 data race。
    mutable std::atomic<size_t> sstable_probe_count_{0};
    mutable std::atomic<size_t> sstable_candidate_count_{0};
};

}  // namespace minitsdb

#endif  // MINITSDB_TSDB_ENGINE_H
