#include "../src/tsdb_engine.h"
#include "../src/bloomfilter.h"
#include "../src/wal.h"
#include "../src/write_batch.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using minitsdb::TsdbEngine;

TsdbEngine::Options FastOptions(int64_t memtable_limit = 4 * 1024 * 1024) {
    TsdbEngine::Options options;
    options.memtable_limit = memtable_limit;
    options.sync_wal = false;
    return options;
}

namespace {

using Clock = std::chrono::steady_clock;

const char* kBenchRoot = "bench_data";

void ResetDir(const std::string& dir) {
    fs::remove_all(dir);
    fs::create_directories(dir);
}

int64_t Micros(Clock::time_point start, Clock::time_point end) {
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
}

void PrintThroughput(const char* name, int ops, int64_t micros) {
    double seconds = micros / 1000000.0;
    double qps = seconds > 0 ? ops / seconds : 0;
    std::printf("%-42s ops=%d time=%.3fs throughput=%.0f ops/s\n",
                name, ops, seconds, qps);
}

void PrintLatency(const char* name, int ops, int64_t micros) {
    double avg = ops > 0 ? static_cast<double>(micros) / ops : 0;
    std::printf("%-42s ops=%d total=%.3fs avg=%.2f us/op\n",
                name, ops, micros / 1000000.0, avg);
}

void BenchSequentialWrite() {
    const std::string db = std::string(kBenchRoot) + "/seq_write";
    ResetDir(db);
    const int n = 50000;
    TsdbEngine eng(db, FastOptions(4 * 1024 * 1024));

    auto begin = Clock::now();
    for (int i = 0; i < n; ++i) {
        eng.Put("bench.seq", i, std::to_string(i));
    }
    eng.Flush();
    auto end = Clock::now();
    PrintThroughput("sequential write buffered WAL", n, Micros(begin, end));
}

void BenchSequentialWriteSyncWal() {
    const std::string db = std::string(kBenchRoot) + "/seq_write_sync";
    ResetDir(db);
    const int n = 1000;
    TsdbEngine eng(db, 4 * 1024 * 1024);

    auto begin = Clock::now();
    for (int i = 0; i < n; ++i) {
        eng.Put("bench.seq.sync", i, std::to_string(i));
    }
    eng.Flush();
    auto end = Clock::now();
    PrintThroughput("sequential write sync WAL", n, Micros(begin, end));
}

void BenchWriteBatchSyncWal() {
    for (int batch_size : {10, 100}) {
        const std::string db = std::string(kBenchRoot) + "/write_batch_sync_" +
                               std::to_string(batch_size);
        ResetDir(db);
        const int n = 1000;
        TsdbEngine eng(db, 4 * 1024 * 1024);

        auto begin = Clock::now();
        for (int base = 0; base < n; base += batch_size) {
            minitsdb::WriteBatch batch;
            for (int i = 0; i < batch_size && base + i < n; ++i) {
                batch.Put("bench.batch.sync", base + i, std::to_string(base + i));
            }
            eng.Write(batch);
        }
        eng.Flush();
        auto end = Clock::now();
        std::string name = "write batch sync WAL size=" + std::to_string(batch_size);
        PrintThroughput(name.c_str(), n, Micros(begin, end));
    }
}

void BenchRandomWrite() {
    const std::string db = std::string(kBenchRoot) + "/rand_write";
    ResetDir(db);
    const int n = 50000;
    std::vector<int> keys(n);
    for (int i = 0; i < n; ++i) keys[i] = i;
    std::mt19937 rng(42);
    std::shuffle(keys.begin(), keys.end(), rng);

    TsdbEngine eng(db, FastOptions(4 * 1024 * 1024));
    auto begin = Clock::now();
    for (int key : keys) {
        eng.Put("bench.rand", key, std::to_string(key));
    }
    eng.Flush();
    auto end = Clock::now();
    PrintThroughput("random write", n, Micros(begin, end));
}

void BenchPointGetAndRange() {
    const std::string db = std::string(kBenchRoot) + "/read_latency";
    ResetDir(db);
    const int n = 20000;
    TsdbEngine eng(db, FastOptions(2 * 1024 * 1024));
    for (int i = 0; i < n; ++i) {
        eng.Put("bench.read", i, std::to_string(i));
        if (i % 5000 == 4999) eng.Flush();
    }
    eng.Flush();

    std::string value;
    auto begin = Clock::now();
    for (int i = 0; i < 300; ++i) {
        eng.Get("bench.read", (i * 7919) % n, &value);
    }
    auto end = Clock::now();
    PrintLatency("point get", 300, Micros(begin, end));

    std::vector<minitsdb::Record> out;
    begin = Clock::now();
    for (int i = 0; i < 60; ++i) {
        int start = (i * 23) % (n - 100);
        eng.RangeQuery("bench.read", start, start + 99, &out);
    }
    end = Clock::now();
    PrintLatency("range query 100 rows", 60, Micros(begin, end));
}

void BenchBloomNegativeLookup() {
    const std::string db = std::string(kBenchRoot) + "/bloom_negative";
    ResetDir(db);
    const int n = 20000;
    TsdbEngine eng(db, FastOptions(2 * 1024 * 1024));
    for (int i = 0; i < n; ++i) {
        eng.Put("bench.bloom", i * 2, std::to_string(i));
        if (i % 2000 == 1999) eng.Flush();
    }
    eng.Flush();

    std::string value;
    eng.ResetReadStats();
    auto begin = Clock::now();
    for (int i = 0; i < 300; ++i) {
        eng.Get("bench.bloom", i * 2 + 1, &value);
    }
    auto end = Clock::now();
    auto stats = eng.GetStats();
    PrintLatency("negative get with bloom", 300, Micros(begin, end));
    std::printf("%-42s candidates=%zu data_block_probes=%zu avg_probes=%.3f\n",
                "negative get bloom filtering", stats.sstable_candidate_count,
                stats.sstable_probe_count,
                stats.sstable_probe_count / 300.0);

    minitsdb::BloomFilter bf;
    for (int i = 0; i < n; ++i) bf.Add(std::to_string(i * 2));
    int may_contain = 0;
    begin = Clock::now();
    for (int i = 0; i < 300; ++i) {
        if (bf.MayContain(std::to_string(i * 2 + 1))) ++may_contain;
    }
    end = Clock::now();
    PrintLatency("standalone bloom negative check", 300, Micros(begin, end));
    std::printf("%-42s false_positive=%d\n", "standalone bloom false positives", may_contain);
}

void BenchBloomParameterExperiment() {
    const int n = 10000;
    const int probes = 5000;
    std::printf("--- bloom bits_per_key experiment ---\n");
    for (int bits_per_key : {4, 8, 10, 14}) {
        minitsdb::BloomFilter bf(bits_per_key);
        for (int i = 0; i < n; ++i) {
            bf.Add(std::to_string(i * 2));
        }
        int false_positive = 0;
        auto begin = Clock::now();
        for (int i = 0; i < probes; ++i) {
            if (bf.MayContain(std::to_string(i * 2 + 1))) ++false_positive;
        }
        auto end = Clock::now();
        std::string encoded = bf.Encode();
        std::printf("  bits_per_key=%d bloom_bytes=%zu false_positive=%d/%d avg=%.3f us/op\n",
                    bits_per_key, encoded.size(), false_positive, probes,
                    static_cast<double>(Micros(begin, end)) / probes);
    }
}

void BenchCompactionImpact() {
    const std::string db = std::string(kBenchRoot) + "/compaction";
    ResetDir(db);
    TsdbEngine eng(db, FastOptions(256 * 1024));
    const int rounds = 6;
    const int per_round = 2000;
    for (int r = 0; r < rounds; ++r) {
        for (int i = 0; i < per_round; ++i) {
            eng.Put("bench.compact", i, std::to_string(r * per_round + i));
        }
        eng.Flush();
    }

    std::string value;
    eng.ResetReadStats();
    for (int i = 0; i < 300; ++i) {
        eng.Get("bench.compact", i % per_round, &value);
    }
    auto before = eng.GetStats();

    auto begin = Clock::now();
    eng.Compact();
    auto end = Clock::now();

    eng.ResetReadStats();
    for (int i = 0; i < 300; ++i) {
        eng.Get("bench.compact", i % per_round, &value);
    }
    auto after = eng.GetStats();

    std::printf("%-42s before_version=%zu before_disk=%zu before_bytes=%llu after_version=%zu after_disk=%zu after_bytes=%llu compact_time=%.3fs\n",
                "compaction files", before.version_files, before.disk_sst_files,
                static_cast<unsigned long long>(before.version_file_bytes),
                after.version_files, after.disk_sst_files,
                static_cast<unsigned long long>(after.version_file_bytes),
                Micros(begin, end) / 1000000.0);
    std::printf("%-42s before_avg=%.3f after_avg=%.3f\n",
                "read amplification probes/get",
                before.sstable_probe_count / 300.0,
                after.sstable_probe_count / 300.0);
}

void BenchHotRangeWithBlockCache() {
    const std::string db = std::string(kBenchRoot) + "/hot_range_cache";
    ResetDir(db);
    TsdbEngine::Options options = FastOptions(512 * 1024);
    options.block_cache_bytes = 256 * 1024;
    TsdbEngine eng(db, options);
    const int n = 10000;
    for (int i = 0; i < n; ++i) {
        eng.Put("bench.hot", i, std::to_string(i));
    }
    eng.Flush();

    std::vector<minitsdb::Record> out;
    auto begin = Clock::now();
    for (int i = 0; i < 1000; ++i) {
        eng.RangeQuery("bench.hot", 1000, 1199, &out);
    }
    auto end = Clock::now();
    auto stats = eng.GetStats();
    PrintLatency("hot range query with block cache", 1000, Micros(begin, end));
    std::printf("%-42s hits=%zu misses=%zu entries=%zu bytes=%zu\n",
                "block cache stats", stats.block_cache_hits,
                stats.block_cache_misses, stats.block_cache_entries,
                stats.block_cache_bytes);
}

void BenchWalRecovery() {
    const std::string dir = std::string(kBenchRoot) + "/wal_recovery";
    ResetDir(dir);
    const std::string wal_path = dir + "/wal.log";
    const int n = 20000;
    {
        minitsdb::WAL wal(wal_path, false);
        for (int i = 0; i < n; ++i) {
            wal.AppendPut("bench.recover", i, std::to_string(i));
        }
        wal.Close();
    }

    int replayed = 0;
    int64_t last_ts = -1;
    auto begin = Clock::now();
    minitsdb::WAL wal(wal_path, false);
    wal.ReplayEntries([&](minitsdb::EntryType type, const std::string&,
                          int64_t timestamp, const std::string&) {
        if (type == minitsdb::EntryType::kValue) {
            ++replayed;
            last_ts = timestamp;
        }
    });
    auto end = Clock::now();
    std::printf("%-42s wal_records=%d replayed=%d time=%.3fs last_ts=%lld\n",
                "wal recovery", n, replayed, Micros(begin, end) / 1000000.0,
                static_cast<long long>(last_ts));
}

// 并发读扩展性：验证读写锁改造后多个读线程能否并行。
// 改造前(std::mutex)：多线程读会被串行化，吞吐几乎不随线程数增长。
// 改造后(shared_mutex 读锁共享)：吞吐应随线程数近似线性增长（受物理核数限制）。
void BenchConcurrentReadScaling() {
    const std::string db = std::string(kBenchRoot) + "/concurrent_read";
    ResetDir(db);
    const int n = 20000;
    TsdbEngine eng(db, FastOptions(2 * 1024 * 1024));
    for (int i = 0; i < n; ++i) {
        eng.Put("bench.cr", i, std::to_string(i));
        if (i % 5000 == 4999) eng.Flush();
    }
    eng.Flush();

    const int per_thread = 60000;
    auto run = [&](int threads) {
        std::vector<std::thread> ts;
        std::atomic<long long> hits{0};
        auto begin = Clock::now();
        for (int t = 0; t < threads; ++t) {
            ts.emplace_back([&, t]() {
                std::string v;
                for (int i = 0; i < per_thread; ++i) {
                    int key = (i * 7919 + t * 1000003) % n;
                    if (eng.Get("bench.cr", key, &v)) ++hits;
                }
            });
        }
        for (auto& th : ts) th.join();
        auto end = Clock::now();
        double secs = Micros(begin, end) / 1000000.0;
        long long total = static_cast<long long>(threads) * per_thread;
        std::printf("  concurrent get  threads=%d  ops=%lld  time=%.3fs  throughput=%.0f ops/s  hits=%lld\n",
                    threads, total, secs, total / secs, hits.load());
    };

    std::printf("--- concurrent read scaling (read-write lock) ---\n");
    run(1);
    run(2);
    run(4);
}

}  // namespace

int main() {
    std::printf("mini-tsdb benchmark\n");
    BenchSequentialWrite();
    BenchSequentialWriteSyncWal();
    BenchWriteBatchSyncWal();
    BenchRandomWrite();
    BenchPointGetAndRange();
    BenchBloomNegativeLookup();
    BenchBloomParameterExperiment();
    BenchCompactionImpact();
    BenchHotRangeWithBlockCache();
    BenchWalRecovery();
    BenchConcurrentReadScaling();
    return 0;
}
