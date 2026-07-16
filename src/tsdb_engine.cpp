#include "tsdb_engine.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <numeric>
#include <unordered_set>

#ifdef _WIN32
#include <direct.h>
#define MKDIR(path) _mkdir(path)
#else
#include <sys/stat.h>
#define MKDIR(path) mkdir(path, 0755)
#endif

namespace minitsdb {

namespace fs = std::filesystem;

TsdbEngine::TsdbEngine(const std::string& db_dir, int64_t memtable_limit)
    : TsdbEngine(db_dir, Options{memtable_limit, true}) {}

TsdbEngine::TsdbEngine(const std::string& db_dir, const Options& options)
    : db_dir_(db_dir),
      options_(options),
      memtable_limit_(options.memtable_limit),
      wal_(db_dir + "/wal.log", options.sync_wal),
      mem_(std::make_shared<MemTable>()),
      vmgr_(db_dir),
      compaction_(std::make_unique<Compaction>(&vmgr_)),
      block_cache_(options.block_cache_bytes) {
    MKDIR(db_dir.c_str());
    vmgr_.Init();
    compaction_->SetTargetFileSize(options.target_file_size);
    LoadExistingSSTables();

    wal_.ReplayEntries([this](EntryType type, const std::string& measurement,
                              int64_t timestamp, const std::string& value) {
        if (type == EntryType::kDeletion) {
            mem_->Delete(measurement, timestamp);
        } else {
            mem_->Put(measurement, timestamp, value);
        }
        UpdateLatestTimestampUnlocked(measurement, timestamp);
    });

    flush_thread_ = std::thread(&TsdbEngine::FlushThread, this);
    compaction_thread_ = std::thread(&TsdbEngine::CompactionThread, this);
}

TsdbEngine::~TsdbEngine() { Close(); }

void TsdbEngine::Close() {
    // 用独立标志做重入保护：running_ 必须保持 true 直到最终 Flush() 完成。
    // 若先置 running_=false，后台线程可能在 Flush() 设置 imm_ 之前就因
    // !running_ && imm_==nullptr 而退出，导致 Flush() 永远等不到 imm_ 被清空。
    bool expected = false;
    if (!closed_.compare_exchange_strong(expected, true)) return;

    Flush();  // running_ 仍为 true，后台线程存活，可处理 imm_
    running_.store(false);
    flush_cv_.notify_all();
    compaction_cv_.notify_all();
    compaction_done_cv_.notify_all();
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
    if (compaction_thread_.joinable()) {
        compaction_thread_.join();
    }
    {
        std::unique_lock<std::shared_mutex> lock(mu_);
        CleanupObsoleteFilesUnlocked();
        // Normal shutdown is a safe point to compact the append-only
        // manifest into a current-version snapshot.
        (void)vmgr_.WriteSnapshotManifest();
    }
    wal_.Close();
}

Status TsdbEngine::Put(const std::string& measurement, int64_t timestamp,
                       const std::string& value) {
    std::unique_lock<std::shared_mutex> lock(mu_);  // 写锁
    MakeRoomForWriteUnlocked(lock);

    Status s = wal_.AppendPut(measurement, timestamp, value);
    if (!s.ok()) return s;

    mem_->Put(measurement, timestamp, value);
    UpdateLatestTimestampUnlocked(measurement, timestamp);

    MakeRoomForWriteUnlocked(lock);
    return Status::OK();
}

Status TsdbEngine::Delete(const std::string& measurement, int64_t timestamp) {
    std::unique_lock<std::shared_mutex> lock(mu_);  // 写锁
    MakeRoomForWriteUnlocked(lock);

    Status s = wal_.AppendDelete(measurement, timestamp);
    if (!s.ok()) return s;

    mem_->Delete(measurement, timestamp);
    UpdateLatestTimestampUnlocked(measurement, timestamp);

    MakeRoomForWriteUnlocked(lock);
    return Status::OK();
}

Status TsdbEngine::Write(const WriteBatch& batch) {
    if (batch.Empty()) return Status::OK();

    std::unique_lock<std::shared_mutex> lock(mu_);
    MakeRoomForWriteUnlocked(lock);

    for (const auto& op : batch.Operations()) {
        Status s = op.type == WriteBatch::OpType::kDelete
                       ? wal_.AppendDeleteNoSync(op.measurement, op.timestamp)
                       : wal_.AppendPutNoSync(op.measurement, op.timestamp, op.value);
        if (!s.ok()) return s;
    }

    Status s = wal_.Sync();
    if (!s.ok()) return s;

    for (const auto& op : batch.Operations()) {
        if (op.type == WriteBatch::OpType::kDelete) {
            mem_->Delete(op.measurement, op.timestamp);
        } else {
            mem_->Put(op.measurement, op.timestamp, op.value);
        }
        UpdateLatestTimestampUnlocked(op.measurement, op.timestamp);
        MakeRoomForWriteUnlocked(lock);
    }
    return Status::OK();
}

bool TsdbEngine::Get(const std::string& measurement, int64_t timestamp,
                     std::string* value) {
    TsKey key{measurement, timestamp};
    ReadView view = CaptureReadView();
    if (IsExpiredInMap(view.ttl_map, measurement, timestamp)) return false;

    InternalEntry entry;
    if (view.mem && view.mem->GetEntry(measurement, timestamp, &entry)) {
        if (entry.IsDeletion()) return false;
        *value = entry.value;
        return true;
    }
    if (view.imm && view.imm->GetEntry(measurement, timestamp, &entry)) {
        if (entry.IsDeletion()) return false;
        *value = entry.value;
        return true;
    }
    if (GetFromSSTables(view, key, &entry)) {
        if (entry.IsDeletion()) return false;
        *value = entry.value;
        return true;
    }
    return false;
}

void TsdbEngine::RangeQuery(const std::string& measurement, int64_t t_start,
                            int64_t t_end, std::vector<Record>* out) {
    out->clear();
    if (t_end < t_start) return;

    std::vector<RankedRecord> all;
    bool has_ttl = false;
    TtlRule ttl_snapshot;  // 在读锁内快照 TTL 规则，避免后续逐条记录重新加锁
    ReadView view = CaptureReadView();

    std::vector<Record> mem_out;
    if (view.mem) {
        view.mem->RangeQueryEntries(measurement, t_start, t_end, &mem_out);
        AddSourceRecords(mem_out, 0, &all);
    }

    if (view.imm) {
        std::vector<Record> imm_out;
        view.imm->RangeQueryEntries(measurement, t_start, t_end, &imm_out);
        AddSourceRecords(imm_out, 1, &all);
    }

    for (int level = 0; level < Version::kMaxLevel; ++level) {
        const auto& files = view.levels[static_cast<size_t>(level)];
        for (int i = static_cast<int>(files.size()) - 1; i >= 0; --i) {
            const auto& file = files[static_cast<size_t>(i)];
            if (!file.reader) continue;

            std::vector<Record> sst_out;
            file.reader->RangeScanEntries(TsKey{measurement, t_start},
                                          TsKey{measurement, t_end}, &sst_out);
            AddSourceRecords(sst_out,
                             2 + level * 100000 + (static_cast<int>(files.size()) - 1 - i),
                             &all);
        }
    }

    auto it = view.ttl_map.find(measurement);
    if (it != view.ttl_map.end()) {
        ttl_snapshot = it->second;
        has_ttl = true;
    }

    std::sort(all.begin(), all.end(), [](const RankedRecord& a, const RankedRecord& b) {
        if (a.record.measurement != b.record.measurement) {
            return a.record.measurement < b.record.measurement;
        }
        if (a.record.timestamp != b.record.timestamp) {
            return a.record.timestamp < b.record.timestamp;
        }
        return a.rank < b.rank;
    });

    for (size_t i = 0; i < all.size();) {
        size_t j = i + 1;
        while (j < all.size() &&
               all[j].record.measurement == all[i].record.measurement &&
               all[j].record.timestamp == all[i].record.timestamp) {
            ++j;
        }

        const Record& latest = all[i].record;
        bool expired = has_ttl && IsExpiredByRule(ttl_snapshot, latest.timestamp);
        if (!latest.deleted && !expired) {
            out->push_back(latest);
        }
        i = j;
    }
}

void TsdbEngine::SetTTL(const std::string& measurement, int64_t ttl_seconds) {
    std::unique_lock<std::shared_mutex> lock(mu_);
    auto& ttl = ttl_map_[measurement];
    ttl.ttl_seconds = ttl_seconds;
}

void TsdbEngine::Downsample(const std::string& measurement, int64_t t_start,
                            int64_t t_end, int64_t interval_ns, Agg agg,
                            std::vector<Record>* out) {
    out->clear();
    if (interval_ns <= 0 || t_end < t_start) return;

    std::vector<Record> raw;
    RangeQuery(measurement, t_start, t_end, &raw);
    if (raw.empty()) return;

    size_t begin = 0;
    for (int64_t bucket_start = t_start; bucket_start <= t_end; bucket_start += interval_ns) {
        int64_t bucket_end = bucket_start + interval_ns;
        std::vector<double> values;
        std::string last_value;

        while (begin < raw.size() && raw[begin].timestamp < bucket_start) {
            ++begin;
        }

        size_t i = begin;
        while (i < raw.size() && raw[i].timestamp < bucket_end) {
            values.push_back(std::stod(raw[i].value));
            last_value = raw[i].value;
            ++i;
        }

        if (values.empty()) continue;

        if (agg == Agg::kLast) {
            out->push_back({measurement, bucket_start, last_value});
            continue;
        }

        double result = 0.0;
        if (agg == Agg::kAvg) {
            result = std::accumulate(values.begin(), values.end(), 0.0) / values.size();
        } else if (agg == Agg::kMax) {
            result = *std::max_element(values.begin(), values.end());
        } else {
            result = *std::min_element(values.begin(), values.end());
        }

        char buf[64];
        std::snprintf(buf, sizeof(buf), "%.6f", result);
        out->push_back({measurement, bucket_start, buf});
    }
}

size_t TsdbEngine::MemUsage() {
    std::shared_ptr<MemTable> mem;
    std::shared_ptr<MemTable> imm;
    {
        std::shared_lock<std::shared_mutex> lock(mu_);
        mem = mem_;
        imm = imm_;
    }
    size_t total = mem ? mem->ApproximateMemoryUsage() : 0;
    if (imm) total += imm->ApproximateMemoryUsage();
    return total;
}

void TsdbEngine::Flush() {
    std::unique_lock<std::shared_mutex> lock(mu_);  // 写锁：会修改 mem_/imm_
    // 循环直到 mem_ 与 imm_ 都为空：若进入时 imm_ 正被后台线程 flush，
    // 需先等它排空，再把 mem_ 轮转为 imm_ 继续 flush，避免遗漏 mem_ 中的数据。
    while (true) {
        if (!mem_->Empty() && !imm_ && !flush_in_progress_) {
            imm_ = std::move(mem_);
            mem_ = std::make_shared<MemTable>();
            flush_cv_.notify_one();
        }
        if (mem_->Empty() && !imm_ && !flush_in_progress_) break;
        flush_done_cv_.wait(lock, [this] {
            return !running_.load() || (!imm_ && !flush_in_progress_);
        });
        if (!running_.load() && !imm_ && !flush_in_progress_) break;
    }
}

void TsdbEngine::Compact(bool cleanup_obsolete) {
    RunOneCompaction(true, cleanup_obsolete);
}

void TsdbEngine::CleanupObsoleteFiles() {
    std::unique_lock<std::shared_mutex> lock(mu_);
    CleanupObsoleteFilesUnlocked();
}

void TsdbEngine::CleanupObsoleteFilesUnlocked() {
    auto files = vmgr_.ConsumeObsoleteFiles();
    for (const auto& fname : files) {
        auto it = std::find_if(sst_readers_.begin(), sst_readers_.end(),
                               [&fname](const std::shared_ptr<SSTableReader>& reader) {
                                   return reader && reader->FileName() == fname;
                               });
        if (it != sst_readers_.end()) {
            pending_delete_.push_back(PendingDelete{fname, *it});
            sst_readers_.erase(it);
        } else {
            pending_delete_.push_back(PendingDelete{fname, nullptr});
        }
    }
    TryDeletePendingFilesUnlocked();
}

void TsdbEngine::ResetReadStats() {
    sstable_probe_count_.store(0, std::memory_order_relaxed);
    sstable_candidate_count_.store(0, std::memory_order_relaxed);
}

TsdbEngine::EngineStats TsdbEngine::GetStats() const {
    EngineStats stats;
    {
        std::shared_lock<std::shared_mutex> lock(mu_);
        stats.memtable_bytes = mem_ ? mem_->ApproximateMemoryUsage() : 0;
        stats.immutable_bytes = imm_ ? imm_->ApproximateMemoryUsage() : 0;
        stats.flush_in_progress = flush_in_progress_;
        stats.compaction_in_progress = compaction_in_progress_;
        stats.compaction_pending = compaction_pending_;
        stats.compacting_files = compacting_files_.size();
        stats.pending_delete_files = pending_delete_.size();
    }
    stats.version_files = vmgr_.NumFiles();
    stats.version_file_bytes = vmgr_.TotalFileSize();
    stats.disk_sst_files = CountSSTFilesOnDisk();
    auto cache_stats = block_cache_.GetStats();
    stats.block_cache_hits = cache_stats.hits;
    stats.block_cache_misses = cache_stats.misses;
    stats.block_cache_bytes = cache_stats.bytes;
    stats.block_cache_entries = cache_stats.entries;
    for (int level = 0; level < Version::kMaxLevel; ++level) {
        stats.level_files[static_cast<size_t>(level)] = vmgr_.NumFiles(level);
    }
    // 统计已是原子变量，读路径无需再加引擎锁。
    stats.sstable_probe_count = sstable_probe_count_.load(std::memory_order_relaxed);
    stats.sstable_candidate_count = sstable_candidate_count_.load(std::memory_order_relaxed);
    return stats;
}

std::function<void(const std::string&, std::vector<Record>*)>
TsdbEngine::MakeSSTReader() {
    return [](const std::string& fname, std::vector<Record>* records) {
        records->clear();
        SSTableReader reader(fname);
        if (!reader.Open().ok()) return;
        reader.RangeScanEntries(TsKey{"", INT64_MIN}, TsKey{"\xFF", INT64_MAX}, records);
    };
}

void TsdbEngine::FlushThread() {
    while (true) {
        std::unique_lock<std::shared_mutex> lock(mu_);  // 写锁
        flush_cv_.wait_for(lock, std::chrono::milliseconds(100), [this] {
            return imm_ != nullptr || !running_.load();
        });

        if (!running_.load() && imm_ == nullptr) break;
        if (!imm_) continue;

        std::shared_ptr<MemTable> to_flush = std::move(imm_);
        flush_in_progress_ = true;
        lock.unlock();  // 写 SSTable 期间放开锁，前台读写可继续

        uint64_t file_num = vmgr_.NewFileNumber();
        std::string sst_name = vmgr_.NewFileName(file_num);
        SSTableBuilder builder(sst_name);
        to_flush->IterateEntries([&builder](const std::string& measurement,
                                            int64_t timestamp,
                                            const InternalEntry& entry) {
            builder.Add(TsKey{measurement, timestamp}, entry);
        });
        Status s = builder.Finish();

        if (s.ok()) {
            auto reader = std::make_shared<SSTableReader>(sst_name);
            if (reader->Open().ok()) {
                auto meta = std::make_shared<FileMeta>();
                meta->file_number = file_num;
                meta->file_name = sst_name;
                meta->file_size = static_cast<uint64_t>(fs::file_size(sst_name));
                meta->smallest = reader->MinKey();
                meta->largest = reader->MaxKey();

                VersionEdit edit;
                edit.added_files.push_back({0, meta});

                lock.lock();  // 重新加写锁，更新版本视图并注册 reader
                vmgr_.LogAndApply(edit);
                reader->SetBlockCache(&block_cache_);
                sst_readers_.push_back(reader);
                wal_.Rotate();
            } else {
                lock.lock();
            }
        } else {
            lock.lock();
        }

        flush_in_progress_ = false;
        flush_done_cv_.notify_all();
        lock.unlock();
        MaybeScheduleCompaction();
    }
}

void TsdbEngine::MaybeScheduleCompaction() {
    std::unique_lock<std::shared_mutex> lock(mu_);
    if (ShouldScheduleCompactionUnlocked() ||
        (compaction_in_progress_ &&
         L0NonCompactingFileCountUnlocked() >= kL0CompactionTrigger)) {
        compaction_pending_ = true;
        compaction_cv_.notify_one();
    }
}

void TsdbEngine::CompactionThread() {
    while (true) {
        CompactionWork work;
        {
            std::unique_lock<std::shared_mutex> lock(mu_);
            compaction_cv_.wait(lock, [this] {
                return compaction_pending_ || !running_.load();
            });
            if (!running_.load()) break;
            if (!PickCompactionWorkUnlocked(false, &work)) {
                compaction_pending_ = false;
                continue;
            }
            compaction_pending_ = false;
            MarkCompactionJobStartedUnlocked(work.job);
        }

        RunCompactionWork(std::move(work), true);
    }
}

bool TsdbEngine::RunOneCompaction(bool force, bool cleanup_obsolete) {
    CompactionWork work;
    {
        std::unique_lock<std::shared_mutex> lock(mu_);
        while (compaction_in_progress_ && running_.load()) {
            compaction_done_cv_.wait(lock);
        }
        if (compaction_in_progress_) return false;
        if (!PickCompactionWorkUnlocked(force, &work)) {
            TryDeletePendingFilesUnlocked();
            return false;
        }
        compaction_pending_ = false;
        MarkCompactionJobStartedUnlocked(work.job);
    }

    RunCompactionWork(std::move(work), cleanup_obsolete);
    return true;
}

void TsdbEngine::RunCompactionWork(CompactionWork work, bool cleanup_obsolete) {
    auto reader = MakeSSTReader();
    auto ttl_snapshot = work.ttl_snapshot;
    auto filter = [ttl_snapshot](const Record& record) {
        return KeepRecordForCompaction(record, ttl_snapshot);
    };

    CompactionResult result;
    Status status = compaction_->Run(work.job, reader, filter, &result);
    bool committed = false;
    std::vector<std::string> files_to_delete;
    std::vector<std::shared_ptr<SSTableReader>> readers_to_drop;

    {
        std::unique_lock<std::shared_mutex> lock(mu_);
        if (status.ok() && IsCompactionInputValidUnlocked(work.job)) {
            for (const auto& new_reader : result.new_readers) {
                if (new_reader) new_reader->SetBlockCache(&block_cache_);
            }
            status = vmgr_.LogAndApply(result.edit);
            if (status.ok()) {
                for (auto& new_reader : result.new_readers) {
                    if (new_reader) sst_readers_.push_back(std::move(new_reader));
                }
                committed = true;
                if (cleanup_obsolete) {
                    CleanupObsoleteFilesUnlocked();
                }
            }
        }

        if (!committed) {
            files_to_delete = result.new_files;
            readers_to_drop = std::move(result.new_readers);
        }

        MarkCompactionJobFinishedUnlocked(work.job);
        if (running_.load() &&
            (compaction_pending_ || ShouldScheduleCompactionUnlocked())) {
            compaction_pending_ = true;
            compaction_cv_.notify_one();
        }
        compaction_done_cv_.notify_all();
    }

    readers_to_drop.clear();
    if (!committed) {
        DeleteFilesOutsideLock(std::move(files_to_delete));
    }
}

bool TsdbEngine::PickCompactionWorkUnlocked(bool force, CompactionWork* work) const {
    if (!work || compaction_in_progress_) return false;
    *work = CompactionWork();
    work->ttl_snapshot = ttl_map_;

    const auto& version = vmgr_.CurrentVersion();
    std::vector<std::shared_ptr<FileMeta>> l0_candidates;
    for (const auto& file : version.level_files[0]) {
        if (!file) continue;
        if (compacting_files_.find(file->file_number) != compacting_files_.end()) {
            continue;
        }
        l0_candidates.push_back(file);
    }

    if (!l0_candidates.empty() &&
        (force || l0_candidates.size() >= kL0CompactionTrigger)) {
        TsKeyComparator cmp;
        TsKey smallest = l0_candidates.front()->smallest;
        TsKey largest = l0_candidates.front()->largest;
        for (const auto& file : l0_candidates) {
            if (cmp(file->smallest, smallest) < 0) smallest = file->smallest;
            if (cmp(file->largest, largest) > 0) largest = file->largest;
        }

        for (const auto& file : version.level_files[1]) {
            if (!file) continue;
            if (!(cmp(file->largest, smallest) < 0 ||
                  cmp(file->smallest, largest) > 0)) {
                work->job.inputs.push_back(CompactionInput{1, file});
            }
        }
        for (const auto& file : l0_candidates) {
            work->job.inputs.push_back(CompactionInput{0, file});
        }
        work->job.output_level = 1;
        return !work->job.inputs.empty();
    }

    for (int level = 1; level < Version::kMaxLevel - 1; ++level) {
        int64_t total_size = 0;
        for (const auto& file : version.level_files[level]) {
            if (!file) continue;
            total_size += static_cast<int64_t>(file->file_size);
        }
        int64_t limit = kLevelSizeBase * (1LL << level);
        if (total_size <= limit) continue;

        for (const auto& file : version.level_files[level]) {
            if (file) work->job.inputs.push_back(CompactionInput{level, file});
        }
        work->job.output_level = level + 1;
        return !work->job.inputs.empty();
    }

    return false;
}

void TsdbEngine::MarkCompactionJobStartedUnlocked(const CompactionJob& job) {
    compaction_in_progress_ = true;
    for (const auto& input : job.inputs) {
        if (input.meta) compacting_files_.insert(input.meta->file_number);
    }
}

void TsdbEngine::MarkCompactionJobFinishedUnlocked(const CompactionJob& job) {
    for (const auto& input : job.inputs) {
        if (input.meta) compacting_files_.erase(input.meta->file_number);
    }
    compaction_in_progress_ = false;
}

bool TsdbEngine::IsCompactionInputValidUnlocked(const CompactionJob& job) const {
    const auto& version = vmgr_.CurrentVersion();
    for (const auto& input : job.inputs) {
        if (!input.meta || input.level < 0 || input.level >= Version::kMaxLevel) {
            return false;
        }
        const auto& files = version.level_files[input.level];
        auto it = std::find_if(files.begin(), files.end(),
                               [&input](const std::shared_ptr<FileMeta>& file) {
                                   return file &&
                                          file->file_number == input.meta->file_number;
                               });
        if (it == files.end()) return false;
    }
    return true;
}

size_t TsdbEngine::L0TotalFileCountUnlocked() const {
    const auto& files = vmgr_.CurrentVersion().level_files[0];
    size_t count = 0;
    for (const auto& file : files) {
        if (file) ++count;
    }
    return count;
}

size_t TsdbEngine::L0NonCompactingFileCountUnlocked() const {
    const auto& files = vmgr_.CurrentVersion().level_files[0];
    size_t count = 0;
    for (const auto& file : files) {
        if (!file) continue;
        if (compacting_files_.find(file->file_number) == compacting_files_.end()) {
            ++count;
        }
    }
    return count;
}

bool TsdbEngine::ShouldScheduleCompactionUnlocked() const {
    if (compaction_in_progress_) return false;
    if (L0NonCompactingFileCountUnlocked() >= kL0CompactionTrigger) return true;

    const auto& version = vmgr_.CurrentVersion();
    for (int level = 1; level < Version::kMaxLevel - 1; ++level) {
        int64_t total_size = 0;
        for (const auto& file : version.level_files[level]) {
            if (file) total_size += static_cast<int64_t>(file->file_size);
        }
        int64_t limit = kLevelSizeBase * (1LL << level);
        if (total_size > limit) return true;
    }
    return false;
}

bool TsdbEngine::ShouldStopWritesUnlocked() const {
    return compaction_in_progress_ && L0TotalFileCountUnlocked() >= kL0StopTrigger;
}

TsdbEngine::ReadView TsdbEngine::CaptureReadView() const {
    ReadView view;
    std::shared_lock<std::shared_mutex> lock(mu_);
    view.mem = mem_;
    view.imm = imm_;
    view.ttl_map = ttl_map_;

    const auto& version = vmgr_.CurrentVersion();
    for (int level = 0; level < Version::kMaxLevel; ++level) {
        for (const auto& file : version.level_files[level]) {
            if (!file) continue;
            auto reader = FindReaderUnlocked(file->file_name);
            if (reader) {
                view.levels[static_cast<size_t>(level)].push_back(ReaderRef{file, reader});
            }
        }
    }
    return view;
}

bool TsdbEngine::GetFromSSTables(const ReadView& view, const TsKey& key,
                                 InternalEntry* entry) {
    TsKeyComparator cmp;
    for (int level = 0; level < Version::kMaxLevel; ++level) {
        const auto& files = view.levels[static_cast<size_t>(level)];
        for (int i = static_cast<int>(files.size()) - 1; i >= 0; --i) {
            const auto& file = files[static_cast<size_t>(i)];
            if (!file.meta || !file.reader) continue;
            if (cmp(key, file.meta->smallest) < 0 || cmp(key, file.meta->largest) > 0) {
                continue;
            }
            ++sstable_candidate_count_;  // 原子递增，多读并发安全

            bool data_block_read = false;
            if (file.reader->GetEntry(key, entry, &data_block_read)) {
                if (data_block_read) ++sstable_probe_count_;
                return true;
            }
            if (data_block_read) ++sstable_probe_count_;
        }
    }
    return false;
}

bool TsdbEngine::IsExpiredInMap(
    const std::unordered_map<std::string, TtlRule>& ttl_map,
    const std::string& measurement, int64_t timestamp) {
    auto it = ttl_map.find(measurement);
    if (it == ttl_map.end()) return false;
    return IsExpiredByRule(it->second, timestamp);
}

bool TsdbEngine::IsExpiredByRule(const TtlRule& rule, int64_t timestamp) {
    if (rule.ttl_seconds <= 0 || rule.latest_timestamp == INT64_MIN) {
        return false;
    }
    int64_t threshold = rule.latest_timestamp - rule.ttl_seconds * 1000000000LL;
    return timestamp < threshold;
}

bool TsdbEngine::LoadExistingSSTables() {
    if (!fs::exists(db_dir_)) return true;

    if (vmgr_.NumFiles() > 0) {
        std::unique_lock<std::shared_mutex> lock(mu_);
        OpenReadersForCurrentVersionUnlocked();
        GarbageCollectSSTablesOnStartupUnlocked();
        return true;
    }

    std::vector<fs::path> paths;
    for (const auto& entry : fs::directory_iterator(db_dir_)) {
        if (entry.is_regular_file() && entry.path().extension() == ".sst") {
            paths.push_back(entry.path());
        }
    }
    std::sort(paths.begin(), paths.end());

    for (const auto& path : paths) {
        auto reader = std::make_shared<SSTableReader>(path.string());
        if (!reader->Open().ok()) continue;

        auto meta = std::make_shared<FileMeta>();
        std::string stem = path.stem().string();
        try {
            meta->file_number = static_cast<uint64_t>(std::stoull(stem));
        } catch (...) {
            meta->file_number = vmgr_.NewFileNumber();
        }
        meta->file_name = path.string();
        meta->file_size = static_cast<uint64_t>(fs::file_size(path));
        meta->smallest = reader->MinKey();
        meta->largest = reader->MaxKey();

        VersionEdit edit;
        edit.added_files.push_back({0, meta});
        vmgr_.Apply(edit);
        reader->SetBlockCache(&block_cache_);
        sst_readers_.push_back(reader);
    }
    if (vmgr_.NumFiles() > 0) {
        vmgr_.WriteSnapshotManifest();
    }
    return true;
}

void TsdbEngine::MakeRoomForWriteUnlocked(
    std::unique_lock<std::shared_mutex>& lock) {
    while (true) {
        while (ShouldStopWritesUnlocked()) {
            compaction_done_cv_.wait(lock);
            if (!running_.load()) return;
        }

        if (mem_->ApproximateMemoryUsage() < static_cast<size_t>(memtable_limit_)) {
            return;
        }

        if (!imm_ && !flush_in_progress_) {
            imm_ = std::move(mem_);
            mem_ = std::make_shared<MemTable>();
            flush_cv_.notify_one();
            return;
        }
        flush_done_cv_.wait(lock, [this] {
            return !running_.load() || (!imm_ && !flush_in_progress_);
        });
        if (!running_.load()) return;
    }
}

void TsdbEngine::AddSourceRecords(const std::vector<Record>& records, int rank,
                                  std::vector<RankedRecord>* out) const {
    for (const auto& record : records) {
        out->push_back(RankedRecord{record, rank});
    }
}

size_t TsdbEngine::CountSSTFilesOnDisk() const {
    if (!fs::exists(db_dir_)) return 0;
    size_t count = 0;
    for (const auto& entry : fs::directory_iterator(db_dir_)) {
        if (entry.is_regular_file() && entry.path().extension() == ".sst") {
            ++count;
        }
    }
    return count;
}

void TsdbEngine::GarbageCollectSSTablesOnStartupUnlocked() {
    if (!fs::exists(db_dir_)) return;

    std::unordered_set<std::string> live_files;
    const auto& version = vmgr_.CurrentVersion();
    for (int level = 0; level < Version::kMaxLevel; ++level) {
        for (const auto& file : version.level_files[level]) {
            if (!file) continue;
            live_files.insert(fs::path(file->file_name).filename().string());
        }
    }

    for (const auto& entry : fs::directory_iterator(db_dir_)) {
        if (!entry.is_regular_file() || entry.path().extension() != ".sst") continue;
        if (live_files.find(entry.path().filename().string()) == live_files.end()) {
            std::error_code ignored;
            fs::remove(entry.path(), ignored);
        }
    }
}

std::shared_ptr<SSTableReader> TsdbEngine::FindReaderUnlocked(
    const std::string& fname) const {
    for (const auto& reader : sst_readers_) {
        if (reader && reader->FileName() == fname) {
            return reader;
        }
    }
    return {};
}

void TsdbEngine::EnsureReadersForVersionUnlocked() {
    // compaction 产出的新 SSTable 进入版本视图后，必须在此（写锁下）补开 reader，
    // 否则读路径会因找不到 reader 而漏读。
    const auto& version = vmgr_.CurrentVersion();
    for (int level = 0; level < Version::kMaxLevel; ++level) {
        for (const auto& file : version.level_files[level]) {
            if (!file) continue;
            if (FindReaderUnlocked(file->file_name)) continue;
            auto reader = std::make_shared<SSTableReader>(file->file_name);
            if (reader->Open().ok()) {
                reader->SetBlockCache(&block_cache_);
                sst_readers_.push_back(reader);
            }
        }
    }
}

void TsdbEngine::OpenReadersForCurrentVersionUnlocked() {
    const auto& version = vmgr_.CurrentVersion();
    for (int level = 0; level < Version::kMaxLevel; ++level) {
        for (const auto& file : version.level_files[level]) {
            if (!file || FindReaderUnlocked(file->file_name)) continue;
            auto reader = std::make_shared<SSTableReader>(file->file_name);
            if (reader->Open().ok()) {
                reader->SetBlockCache(&block_cache_);
                sst_readers_.push_back(reader);
                vmgr_.MarkFileNumberUsed(file->file_number);
            }
        }
    }
}

void TsdbEngine::UpdateLatestTimestampUnlocked(const std::string& measurement,
                                               int64_t timestamp) {
    auto& ttl = ttl_map_[measurement];
    if (timestamp > ttl.latest_timestamp) {
        ttl.latest_timestamp = timestamp;
    }
}

bool TsdbEngine::KeepRecordForCompaction(
    const Record& record,
    const std::unordered_map<std::string, TtlRule>& ttl_map) {
    auto it = ttl_map.find(record.measurement);
    if (it == ttl_map.end()) return true;
    return !IsExpiredByRule(it->second, record.timestamp);
}

void TsdbEngine::TryDeletePendingFilesUnlocked() {
    pending_delete_.erase(
        std::remove_if(pending_delete_.begin(), pending_delete_.end(),
                       [](PendingDelete& item) {
                           if (item.reader && item.reader.use_count() > 1) {
                               return false;
                           }
                           item.reader.reset();
                           bool removed = std::remove(item.file_name.c_str()) == 0;
                           return removed || !fs::exists(item.file_name);
                       }),
        pending_delete_.end());
}

void TsdbEngine::DeleteFilesOutsideLock(std::vector<std::string> files) {
    for (const auto& fname : files) {
        std::remove(fname.c_str());
    }
}

}  // namespace minitsdb
