#include "../src/bloomfilter.h"
#include "../src/compression.h"
#include "../src/crc32.h"
#include "../src/memtable.h"
#include "../src/skiplist.h"
#include "../src/sstable.h"
#include "../src/status.h"
#include "../src/tsdb_engine.h"
#include "../src/utils.h"
#include "../src/version.h"
#include "../src/wal.h"
#include "../src/write_batch.h"

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

using namespace minitsdb;
namespace fs = std::filesystem;

namespace {

int g_pass = 0;
int g_fail = 0;

#define CHECK(cond)                                                          \
    do {                                                                     \
        if (!(cond)) {                                                       \
            std::printf("  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);    \
            ++g_fail;                                                        \
        } else {                                                             \
            ++g_pass;                                                        \
        }                                                                    \
    } while (0)

#define CHECK_ST(status)                                                     \
    do {                                                                     \
        if (!(status).ok()) {                                                \
            std::printf("  FAIL %s:%d: %s => %s\n", __FILE__, __LINE__,     \
                        #status, (status).ToString().c_str());               \
            ++g_fail;                                                        \
        } else {                                                             \
            ++g_pass;                                                        \
        }                                                                    \
    } while (0)

#define TEST(name) static void name()

const char* kTestRoot = "test_data";

void EnsureTestRoot() {
    fs::create_directories(kTestRoot);
}

void ResetDir(const std::string& dir) {
    fs::remove_all(dir);
    fs::create_directories(dir);
}

void TruncateTail(const std::string& file, size_t bytes_to_remove) {
    std::ifstream in(file, std::ios::binary);
    std::string data((std::istreambuf_iterator<char>(in)),
                     std::istreambuf_iterator<char>());
    in.close();
    if (bytes_to_remove >= data.size()) {
        data.clear();
    } else {
        data.resize(data.size() - bytes_to_remove);
    }
    std::ofstream out(file, std::ios::binary | std::ios::trunc);
    out.write(data.data(), static_cast<std::streamsize>(data.size()));
}


void FlipByte(const std::string& file, size_t offset) {
    std::fstream io(file, std::ios::binary | std::ios::in | std::ios::out);
    io.seekg(0, std::ios::end);
    size_t size = static_cast<size_t>(io.tellg());
    if (offset >= size) return;
    io.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    char c = 0;
    io.read(&c, 1);
    c = static_cast<char>(c ^ 0x7F);
    io.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
    io.write(&c, 1);
}

size_t CountSSTFiles(const std::string& dir) {
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".sst") {
            ++count;
        }
    }
    return count;
}

template <typename Predicate>
bool WaitUntil(Predicate pred, int timeout_ms = 5000) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

TEST(ComparatorBasic) {
    TsKeyComparator cmp;
    CHECK(cmp({"a", 1}, {"b", 1}) < 0);
    CHECK(cmp({"b", 1}, {"a", 1}) > 0);
    CHECK(cmp({"a", 1}, {"a", 1}) == 0);
    CHECK(cmp({"a", 1}, {"a", 2}) < 0);
}

TEST(SkipListInsertGet) {
    SkipList<TsKey, std::string, TsKeyComparator> sl;
    sl.Insert({"cpu.temp", 100}, "55.0");
    sl.Insert({"cpu.temp", 200}, "56.0");
    sl.Insert({"cpu.temp", 50}, "54.0");
    std::string v;
    CHECK(sl.Get({"cpu.temp", 50}, &v) && v == "54.0");
    CHECK(sl.Get({"cpu.temp", 100}, &v) && v == "55.0");
    CHECK(sl.Get({"cpu.temp", 200}, &v) && v == "56.0");
}

TEST(SkipListOverwrite) {
    SkipList<TsKey, std::string, TsKeyComparator> sl;
    sl.Insert({"m", 1}, "v1");
    sl.Insert({"m", 1}, "v2");
    std::string v;
    CHECK(sl.Get({"m", 1}, &v) && v == "v2");
}

TEST(SkipListRangeOrder) {
    SkipList<TsKey, std::string, TsKeyComparator> sl;
    sl.Insert({"b", 10}, "b10");
    sl.Insert({"a", 30}, "a30");
    sl.Insert({"a", 10}, "a10");
    sl.Insert({"a", 20}, "a20");
    auto* n = sl.FindGreaterOrEqual({"a", 0});
    std::vector<std::string> seq;
    while (n != nullptr) {
        seq.push_back(sl.ValueOf(n));
        n = sl.NextOf(n);
    }
    CHECK(seq.size() == 4);
    CHECK(seq[0] == "a10" && seq[1] == "a20" && seq[2] == "a30" && seq[3] == "b10");
}

TEST(MemTableBasic) {
    MemTable mt;
    mt.Put("imu.accel.x", 1000, "0.1");
    mt.Put("imu.accel.x", 2000, "0.2");
    mt.Put("imu.accel.x", 1500, "0.15");
    std::string v;
    CHECK(mt.Get("imu.accel.x", 1500, &v) && v == "0.15");
    std::vector<Record> out;
    mt.RangeQuery("imu.accel.x", 1000, 2000, &out);
    CHECK(out.size() == 3);
}

TEST(MemTableIterate) {
    MemTable mt;
    mt.Put("a", 1, "v1");
    mt.Put("a", 2, "v2");
    mt.Put("b", 1, "v3");
    std::vector<std::tuple<std::string, int64_t, std::string>> items;
    mt.Iterate([&](const std::string& m, int64_t ts, const std::string& v) {
        items.emplace_back(m, ts, v);
    });
    CHECK(items.size() == 3);
}


TEST(MemTableDelete) {
    MemTable mt;
    mt.Put("a", 1, "v1");
    mt.Put("a", 2, "v2");
    mt.Delete("a", 1);

    std::string v;
    CHECK(!mt.Get("a", 1, &v));
    CHECK(mt.Get("a", 2, &v) && v == "v2");

    InternalEntry entry;
    CHECK(mt.GetEntry("a", 1, &entry) && entry.IsDeletion());

    std::vector<Record> out;
    mt.RangeQuery("a", 1, 2, &out);
    CHECK(out.size() == 1);
    CHECK(out[0].timestamp == 2);
}

TEST(StressOrdering) {
    SkipList<TsKey, std::string, TsKeyComparator> sl;
    std::mt19937 rng(42);
    for (int i = 0; i < 5000; ++i) {
        int64_t ts = static_cast<int64_t>(rng() % 100000);
        sl.Insert({"m", ts}, std::to_string(ts));
    }
    auto* n = sl.FindGreaterOrEqual({"m", 0});
    int64_t prev = -1;
    while (n != nullptr) {
        CHECK(sl.KeyOf(n).timestamp >= prev);
        prev = sl.KeyOf(n).timestamp;
        n = sl.NextOf(n);
    }
}

TEST(StatusBasic) {
    CHECK(Status::OK().ok());
    CHECK(Status::NotFound().IsNotFound());
    CHECK(Status::IOError("err").IsIOError());
    CHECK(Status::Corruption("bad").IsCorruption());
}

TEST(CRC32Basic) {
    CHECK(Crc32("hello", 5, 0) == Crc32("hello", 5, 0));
    CHECK(Crc32("hello", 5, 0) != Crc32("world", 5, 0));
}

TEST(VarintBasic) {
    for (uint32_t v : {0u, 1u, 127u, 128u, 16383u, 1000000u}) {
        std::string s;
        EncodeVarint(v, &s);
        size_t pos = 0;
        CHECK(DecodeVarint(s, &pos) == v);
        CHECK(pos == s.size());
    }
}

TEST(SSTableWriteRead) {
    EnsureTestRoot();
    const std::string file = std::string(kTestRoot) + "/test_sst.sst";
    std::remove(file.c_str());

    SSTableBuilder builder(file, 128);
    builder.Add({"sensor.a", 100}, "v100");
    builder.Add({"sensor.a", 200}, "v200");
    builder.Add({"sensor.b", 150}, "v150");
    CHECK_ST(builder.Finish());

    SSTableReader reader(file);
    CHECK_ST(reader.Open());
    std::string v;
    CHECK(reader.Get({"sensor.a", 100}, &v) && v == "v100");
    CHECK(reader.Get({"sensor.a", 200}, &v) && v == "v200");
    CHECK(reader.Get({"sensor.b", 150}, &v) && v == "v150");
    std::remove(file.c_str());
}

TEST(SSTableRangeScan) {
    EnsureTestRoot();
    const std::string file = std::string(kTestRoot) + "/range_sst.sst";
    std::remove(file.c_str());

    SSTableBuilder builder(file, 64);
    for (int i = 0; i < 100; ++i) {
        builder.Add({"s", i}, std::to_string(i));
    }
    CHECK_ST(builder.Finish());

    SSTableReader reader(file);
    CHECK_ST(reader.Open());
    std::vector<Record> out;
    reader.RangeScan({"s", 20}, {"s", 30}, &out);
    CHECK(out.size() == 11);
    CHECK(out.front().timestamp == 20);
    CHECK(out.back().timestamp == 30);
    std::remove(file.c_str());
}

TEST(SSTableConcurrentReaderUsesRandomAccessFile) {
    EnsureTestRoot();
    const std::string file = std::string(kTestRoot) + "/concurrent_sst.sst";
    std::remove(file.c_str());

    constexpr int kRecords = 1000;
    SSTableBuilder builder(file, 128);
    for (int i = 0; i < kRecords; ++i) {
        builder.Add({"s", i}, std::to_string(i));
    }
    CHECK_ST(builder.Finish());

    SSTableReader reader(file);
    CHECK_ST(reader.Open());

    std::atomic<bool> ok{true};
    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&reader, &ok, t] {
            for (int i = 0; i < 2000; ++i) {
                int ts = (i * 17 + t * 31) % kRecords;
                std::string v;
                if (!reader.Get({"s", ts}, &v) || v != std::to_string(ts)) {
                    ok.store(false);
                    return;
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    CHECK(ok.load());
    std::remove(file.c_str());
}


TEST(SSTableTombstone) {
    EnsureTestRoot();
    const std::string file = std::string(kTestRoot) + "/delete_sst.sst";
    std::remove(file.c_str());

    SSTableBuilder builder(file, 64);
    builder.Add({"s", 1}, "v1");
    builder.Add({"s", 2}, InternalEntry{"", EntryType::kDeletion});
    builder.Add({"s", 3}, "v3");
    CHECK_ST(builder.Finish());

    SSTableReader reader(file);
    CHECK_ST(reader.Open());
    std::string v;
    CHECK(reader.Get({"s", 1}, &v) && v == "v1");
    CHECK(!reader.Get({"s", 2}, &v));

    InternalEntry entry;
    CHECK(reader.GetEntry({"s", 2}, &entry) && entry.IsDeletion());

    std::vector<Record> out;
    reader.RangeScan({"s", 1}, {"s", 3}, &out);
    CHECK(out.size() == 2);

    std::vector<Record> raw;
    reader.RangeScanEntries({"s", 1}, {"s", 3}, &raw);
    CHECK(raw.size() == 3);
    CHECK(raw[1].deleted);
    std::remove(file.c_str());
}


TEST(SSTableDataBlockCrcCorruption) {
    EnsureTestRoot();
    const std::string file = std::string(kTestRoot) + "/crc_bad.sst";
    std::remove(file.c_str());

    SSTableBuilder builder(file, 4096);
    builder.Add({"s", 1}, "v1");
    builder.Add({"s", 2}, "v2");
    CHECK_ST(builder.Finish());

    FlipByte(file, 8);

    SSTableReader reader(file);
    CHECK_ST(reader.Open());
    std::string v;
    CHECK(!reader.Get({"s", 1}, &v));
    std::remove(file.c_str());
}

TEST(BloomFilterBasic) {
    BloomFilter bf;
    bf.Add("key1");
    bf.Add("key2");
    CHECK(bf.MayContain("key1"));
    CHECK(bf.MayContain("key2"));
}

TEST(BloomFilterEncodeDecode) {
    BloomFilter bf;
    bf.Add("hello");
    bf.Add("world");
    BloomFilter decoded = BloomFilter::Decode(bf.Encode());
    CHECK(decoded.MayContain("hello"));
    CHECK(decoded.MayContain("world"));
}

TEST(DeltaOfDeltaBasic) {
    DeltaOfDeltaEncoder enc;
    enc.Add(1000);
    enc.Add(1010);
    enc.Add(1020);
    std::string compressed = enc.Finish();

    DeltaOfDeltaDecoder dec;
    CHECK(dec.Init(compressed));
    int64_t ts = 0;
    CHECK(dec.Next(&ts) && ts == 1000);
    CHECK(dec.Next(&ts) && ts == 1010);
    CHECK(dec.Next(&ts) && ts == 1020);
}

TEST(DeltaOfDeltaVariable) {
    DeltaOfDeltaEncoder enc;
    enc.Add(0);
    enc.Add(100);
    enc.Add(250);
    enc.Add(401);
    std::string compressed = enc.Finish();

    DeltaOfDeltaDecoder dec;
    CHECK(dec.Init(compressed));
    int64_t ts = 0;
    CHECK(dec.Next(&ts) && ts == 0);
    CHECK(dec.Next(&ts) && ts == 100);
    CHECK(dec.Next(&ts) && ts == 250);
    CHECK(dec.Next(&ts) && ts == 401);
}

TEST(WALReplayAfterRestart) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/wal_restart";
    ResetDir(db);

    {
        TsdbEngine eng(db);
        CHECK_ST(eng.Put("m", 1, "v1"));
        CHECK_ST(eng.Put("m", 2, "v2"));
    }
    {
        TsdbEngine eng(db);
        std::string v;
        CHECK(eng.Get("m", 1, &v) && v == "v1");
        CHECK(eng.Get("m", 2, &v) && v == "v2");
    }
}

TEST(WALTruncateRecovery) {
    EnsureTestRoot();
    const std::string dir = std::string(kTestRoot) + "/wal_only";
    ResetDir(dir);
    fs::create_directories(dir);

    const std::string wal_path = dir + "/wal.log";
    {
        WAL wal(wal_path);
        CHECK_ST(wal.AppendPut("m", 1, "v1"));
        CHECK_ST(wal.AppendPut("m", 2, "v2"));
        const size_t truncate_bytes = static_cast<size_t>(wal.FileSize() / 2);
        wal.Close();
        TruncateTail(wal_path, truncate_bytes > 0 ? truncate_bytes : 1);
    }

    std::vector<Record> replayed;
    {
        WAL wal(wal_path);
        CHECK_ST(wal.Replay([&replayed](const std::string& measurement, int64_t timestamp,
                                        const std::string& value) {
            replayed.push_back({measurement, timestamp, value});
        }));
    }
    CHECK(replayed.size() == 1);
    CHECK(replayed[0].measurement == "m");
    CHECK(replayed[0].timestamp == 1);
    CHECK(replayed[0].value == "v1");
}


TEST(WALReplayDelete) {
    EnsureTestRoot();
    const std::string dir = std::string(kTestRoot) + "/wal_delete";
    ResetDir(dir);
    fs::create_directories(dir);

    const std::string wal_path = dir + "/wal.log";
    {
        WAL wal(wal_path);
        CHECK_ST(wal.AppendPut("m", 1, "v1"));
        CHECK_ST(wal.AppendDelete("m", 1));
    }

    std::vector<Record> replayed;
    {
        WAL wal(wal_path);
        CHECK_ST(wal.ReplayEntries([&replayed](EntryType type,
                                               const std::string& measurement,
                                               int64_t timestamp,
                                               const std::string& value) {
            replayed.push_back({measurement, timestamp, value,
                                type == EntryType::kDeletion});
        }));
    }
    CHECK(replayed.size() == 2);
    CHECK(!replayed[0].deleted);
    CHECK(replayed[1].deleted);
}

TEST(WALFragmentedReplay) {
    EnsureTestRoot();
    const std::string dir = std::string(kTestRoot) + "/wal_fragment";
    ResetDir(dir);
    fs::create_directories(dir);

    const std::string wal_path = dir + "/wal.log";
    const std::string big_value(WAL::kMaxFragmentPayload * 2 + 123, 'x');
    {
        WAL wal(wal_path, false);
        CHECK_ST(wal.AppendPut("big", 1, big_value));
        CHECK_ST(wal.AppendPut("small", 2, "ok"));
    }

    std::vector<Record> replayed;
    {
        WAL wal(wal_path, false);
        CHECK_ST(wal.ReplayEntries([&replayed](EntryType type,
                                               const std::string& measurement,
                                               int64_t timestamp,
                                               const std::string& value) {
            replayed.push_back({measurement, timestamp, value,
                                type == EntryType::kDeletion});
        }));
    }

    CHECK(replayed.size() == 2);
    CHECK(replayed[0].measurement == "big");
    CHECK(replayed[0].timestamp == 1);
    CHECK(replayed[0].value == big_value);
    CHECK(replayed[1].measurement == "small");
    CHECK(replayed[1].value == "ok");
}

TEST(WALFragmentedTailTruncateRecovery) {
    EnsureTestRoot();
    const std::string dir = std::string(kTestRoot) + "/wal_fragment_truncate";
    ResetDir(dir);
    fs::create_directories(dir);

    const std::string wal_path = dir + "/wal.log";
    const std::string big_value(WAL::kMaxFragmentPayload * 2 + 123, 'y');
    {
        WAL wal(wal_path, false);
        CHECK_ST(wal.AppendPut("keep", 1, "ok"));
        CHECK_ST(wal.AppendPut("big", 2, big_value));
        wal.Close();
        TruncateTail(wal_path, 16);
    }

    std::vector<Record> replayed;
    {
        WAL wal(wal_path, false);
        CHECK_ST(wal.ReplayEntries([&replayed](EntryType type,
                                               const std::string& measurement,
                                               int64_t timestamp,
                                               const std::string& value) {
            replayed.push_back({measurement, timestamp, value,
                                type == EntryType::kDeletion});
        }));
    }

    CHECK(replayed.size() == 1);
    CHECK(replayed[0].measurement == "keep");
    CHECK(replayed[0].timestamp == 1);
    CHECK(replayed[0].value == "ok");
}

TEST(EngineBasicPutGet) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_basic";
    ResetDir(db);
    TsdbEngine eng(db);
    CHECK_ST(eng.Put("s1", 100, "v100"));
    CHECK_ST(eng.Put("s1", 200, "v200"));
    std::string v;
    CHECK(eng.Get("s1", 100, &v) && v == "v100");
    CHECK(eng.Get("s1", 200, &v) && v == "v200");
}


TEST(EngineDeleteInMem) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_delete_mem";
    ResetDir(db);
    TsdbEngine eng(db);
    CHECK_ST(eng.Put("s", 1, "v1"));
    CHECK_ST(eng.Put("s", 2, "v2"));
    CHECK_ST(eng.Delete("s", 1));

    std::string v;
    CHECK(!eng.Get("s", 1, &v));
    CHECK(eng.Get("s", 2, &v) && v == "v2");

    std::vector<Record> out;
    eng.RangeQuery("s", 1, 2, &out);
    CHECK(out.size() == 1);
    CHECK(out[0].timestamp == 2);
}

TEST(EngineFlushAndReloadSST) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_flush_reload";
    ResetDir(db);

    {
        TsdbEngine eng(db, 128);
        for (int i = 0; i < 50; ++i) {
            CHECK_ST(eng.Put("s", i, std::to_string(i)));
        }
        eng.Flush();
    }

    {
        TsdbEngine eng(db);
        std::string v;
        CHECK(eng.Get("s", 0, &v) && v == "0");
        CHECK(eng.Get("s", 49, &v) && v == "49");
    }
}


TEST(EngineDeleteFlushAndReload) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_delete_reload";
    ResetDir(db);

    {
        TsdbEngine eng(db, 128);
        CHECK_ST(eng.Put("s", 1, "v1"));
        CHECK_ST(eng.Put("s", 2, "v2"));
        eng.Flush();
        CHECK_ST(eng.Delete("s", 1));
        eng.Flush();
    }

    {
        TsdbEngine eng(db);
        std::string v;
        CHECK(!eng.Get("s", 1, &v));
        CHECK(eng.Get("s", 2, &v) && v == "v2");

        std::vector<Record> out;
        eng.RangeQuery("s", 1, 2, &out);
        CHECK(out.size() == 1);
        CHECK(out[0].timestamp == 2);
    }
}


TEST(EngineFlushRestartRecovery) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_flush_restart";
    ResetDir(db);

    {
        TsdbEngine eng(db, 128);
        for (int i = 0; i < 100; ++i) {
            CHECK_ST(eng.Put("s", i, std::to_string(i)));
        }
        eng.Flush();
    }

    {
        TsdbEngine eng(db);
        std::string v;
        CHECK(eng.Get("s", 0, &v) && v == "0");
        CHECK(eng.Get("s", 99, &v) && v == "99");
    }
}

TEST(EngineWriteBackpressureLimitsMutableMemTable) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_backpressure";
    ResetDir(db);

    const int64_t limit = 512;
    const std::string value(512, 'v');
    TsdbEngine::Options options;
    options.memtable_limit = limit;
    options.sync_wal = false;
    TsdbEngine eng(db, options);

    for (int i = 0; i < 40; ++i) {
        CHECK_ST(eng.Put("bp", i, value));
        auto stats = eng.GetStats();
        CHECK(stats.memtable_bytes < static_cast<size_t>(limit * 3));
    }

    eng.Flush();
    auto stats = eng.GetStats();
    CHECK(stats.memtable_bytes == 0);
    CHECK(stats.immutable_bytes == 0);
    CHECK(!stats.flush_in_progress);
}

TEST(EngineCompaction) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_compact";
    ResetDir(db);

    TsdbEngine eng(db, 128);
    for (int batch = 0; batch < 5; ++batch) {
        for (int i = 0; i < 20; ++i) {
            CHECK_ST(eng.Put("cpu", i, std::to_string(batch * 100 + i)));
        }
        eng.Flush();
    }
    eng.Compact();
    auto stats = eng.GetStats();
    CHECK(stats.version_file_bytes > 0);

    std::string v;
    CHECK(eng.Get("cpu", 0, &v));
    CHECK(eng.Get("cpu", 19, &v));
}

TEST(EngineBackgroundCompactionPublishesReadableReaders) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_bg_compact";
    ResetDir(db);

    TsdbEngine::Options options;
    options.memtable_limit = 512;
    options.sync_wal = false;
    TsdbEngine eng(db, options);

    for (int batch = 0; batch < 4; ++batch) {
        for (int i = 0; i < 40; ++i) {
            CHECK_ST(eng.Put("bg.cpu", i, std::to_string(batch * 100 + i)));
        }
        eng.Flush();
    }

    CHECK(WaitUntil([&eng] {
        auto stats = eng.GetStats();
        return stats.level_files[1] >= 1 && !stats.compaction_in_progress;
    }));

    std::string v;
    CHECK(eng.Get("bg.cpu", 0, &v) && v == "300");
    CHECK(eng.Get("bg.cpu", 39, &v) && v == "339");
}

TEST(EngineConcurrentReadsDuringBackgroundCompaction) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_concurrent_compact";
    ResetDir(db);

    TsdbEngine::Options options;
    options.memtable_limit = 1024;
    options.sync_wal = false;
    options.target_file_size = 2048;
    TsdbEngine eng(db, options);
    std::string payload(64, 'x');

    for (int batch = 0; batch < 3; ++batch) {
        for (int i = 0; i < 200; ++i) {
            CHECK_ST(eng.Put("concurrent.cpu", i, std::to_string(batch) + payload));
        }
        eng.Flush();
    }

    std::atomic<bool> stop{false};
    std::atomic<int> misses{0};
    std::thread reader([&] {
        while (!stop.load()) {
            for (int i = 0; i < 200; ++i) {
                std::string v;
                if (!eng.Get("concurrent.cpu", i, &v)) {
                    ++misses;
                }
            }
        }
    });

    for (int i = 0; i < 200; ++i) {
        CHECK_ST(eng.Put("concurrent.cpu", i, "3" + payload));
    }
    eng.Flush();

    CHECK(WaitUntil([&eng] {
        auto stats = eng.GetStats();
        return stats.level_files[1] >= 1 && !stats.compaction_in_progress;
    }));

    stop.store(true);
    reader.join();
    CHECK(misses.load() == 0);

    std::string v;
    CHECK(eng.Get("concurrent.cpu", 42, &v) && v == "3" + payload);
}

TEST(EngineWriteBatchRecovery) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_batch_recovery";
    ResetDir(db);

    {
        TsdbEngine eng(db);
        WriteBatch batch;
        batch.Put("obs.qps", 1, "10");
        batch.Put("obs.qps", 2, "11");
        batch.Delete("obs.qps", 1);
        CHECK(batch.Count() == 3);
        CHECK_ST(eng.Write(batch));
    }

    TsdbEngine reopened(db);
    std::string v;
    CHECK(!reopened.Get("obs.qps", 1, &v));
    CHECK(reopened.Get("obs.qps", 2, &v) && v == "11");
}


TEST(EngineDeleteCompaction) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_delete_compact";
    ResetDir(db);

    TsdbEngine eng(db, 128);
    CHECK_ST(eng.Put("cpu", 1, "old"));
    CHECK_ST(eng.Put("cpu", 2, "keep"));
    eng.Flush();
    CHECK_ST(eng.Delete("cpu", 1));
    eng.Flush();
    eng.Compact();

    std::string v;
    CHECK(!eng.Get("cpu", 1, &v));
    CHECK(eng.Get("cpu", 2, &v) && v == "keep");

    std::vector<Record> out;
    eng.RangeQuery("cpu", 1, 2, &out);
    CHECK(out.size() == 1);
    CHECK(out[0].timestamp == 2);
}


TEST(EngineCompactionVersionViewWithOldFiles) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_compact_view";
    ResetDir(db);

    TsdbEngine eng(db, 128);
    CHECK_ST(eng.Put("cpu", 1, "old"));
    eng.Flush();
    CHECK_ST(eng.Put("cpu", 1, "new"));
    eng.Flush();

    auto before = eng.GetStats();
    CHECK(before.version_files >= 2);
    eng.Compact(false);
    auto after = eng.GetStats();
    CHECK(after.version_files == 1);
    CHECK(after.disk_sst_files >= before.disk_sst_files);

    std::string v;
    CHECK(eng.Get("cpu", 1, &v) && v == "new");
}

TEST(EngineManifestKeepsCompactionVersionAfterRestart) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_manifest_restart";
    ResetDir(db);

    {
        TsdbEngine eng(db, 128);
        CHECK_ST(eng.Put("cpu", 1, "old"));
        eng.Flush();
        CHECK_ST(eng.Put("cpu", 1, "new"));
        eng.Flush();
        eng.Compact(false);
        auto stats = eng.GetStats();
        CHECK(stats.version_files == 1);
        CHECK(stats.version_file_bytes > 0);
    }

    TsdbEngine reopened(db, 128);
    auto stats = reopened.GetStats();
    CHECK(stats.version_files == 1);
    CHECK(stats.level_files[1] == 1);
    std::string v;
    CHECK(reopened.Get("cpu", 1, &v) && v == "new");
}

TEST(EngineCloseRewritesManifestSnapshot) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_manifest_snapshot";
    ResetDir(db);
    const std::string manifest = db + "/MANIFEST-000001";

    {
        TsdbEngine eng(db, 128);
        CHECK_ST(eng.Put("cpu", 1, "old"));
        eng.Flush();
        CHECK_ST(eng.Put("cpu", 1, "new"));
        eng.Flush();
        eng.Compact(false);
        auto stats = eng.GetStats();
        CHECK(stats.version_files == 1);
        CHECK(stats.level_files[1] == 1);
    }

    std::ifstream in(manifest);
    CHECK(in.good());
    int add_lines = 0;
    int del_lines = 0;
    int next_file_lines = 0;
    std::string line;
    while (std::getline(in, line)) {
        if (line.rfind("add ", 0) == 0) ++add_lines;
        if (line.rfind("del ", 0) == 0) ++del_lines;
        if (line.rfind("next_file ", 0) == 0) ++next_file_lines;
    }
    CHECK(add_lines == 1);
    CHECK(del_lines == 0);
    CHECK(next_file_lines == 1);

    TsdbEngine reopened(db, 128);
    std::string v;
    CHECK(reopened.Get("cpu", 1, &v) && v == "new");
}

TEST(EngineStartupGarbageCollectsOrphanSSTables) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_startup_gc";
    ResetDir(db);

    {
        TsdbEngine eng(db, 128);
        CHECK_ST(eng.Put("cpu", 1, "live"));
        eng.Flush();
    }

    std::filesystem::path live_sst;
    for (const auto& entry : std::filesystem::directory_iterator(db)) {
        if (entry.is_regular_file() && entry.path().extension() == ".sst") {
            live_sst = entry.path();
            break;
        }
    }
    CHECK(!live_sst.empty());

    std::filesystem::path orphan = std::filesystem::path(db) / "999999.sst";
    std::error_code ec;
    std::filesystem::copy_file(live_sst, orphan,
                               std::filesystem::copy_options::overwrite_existing, ec);
    CHECK(!ec);
    CHECK(std::filesystem::exists(orphan));

    TsdbEngine reopened(db, 128);
    CHECK(!std::filesystem::exists(orphan));
    auto stats = reopened.GetStats();
    CHECK(stats.disk_sst_files == stats.version_files);
    std::string v;
    CHECK(reopened.Get("cpu", 1, &v) && v == "live");
}

TEST(EngineBlockCacheHitsRepeatedRange) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_block_cache";
    ResetDir(db);

    TsdbEngine::Options options;
    options.memtable_limit = 512;
    options.sync_wal = false;
    options.block_cache_bytes = 64 * 1024;
    TsdbEngine eng(db, options);
    for (int i = 0; i < 200; ++i) {
        CHECK_ST(eng.Put("hot.metric", i, std::to_string(i)));
    }
    eng.Flush();

    std::vector<Record> out;
    eng.RangeQuery("hot.metric", 0, 199, &out);
    eng.RangeQuery("hot.metric", 0, 199, &out);
    auto stats = eng.GetStats();
    CHECK(stats.block_cache_hits > 0);
    CHECK(stats.block_cache_entries > 0);
}

TEST(EngineCompactionObsoleteFileCleanup) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_compact_cleanup";
    ResetDir(db);

    TsdbEngine eng(db, 128);
    CHECK_ST(eng.Put("cpu", 1, "old"));
    eng.Flush();
    CHECK_ST(eng.Put("cpu", 1, "new"));
    eng.Flush();

    eng.Compact(false);
    size_t disk_before_cleanup = CountSSTFiles(db);
    eng.CleanupObsoleteFiles();
    size_t disk_after_cleanup = CountSSTFiles(db);
    CHECK(disk_after_cleanup < disk_before_cleanup);

    std::string v;
    CHECK(eng.Get("cpu", 1, &v) && v == "new");
}

TEST(EngineDownsample) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_down";
    ResetDir(db);

    TsdbEngine eng(db);
    for (int i = 0; i < 100; ++i) {
        CHECK_ST(eng.Put("temp", i * 10, std::to_string(i * 0.1)));
    }
    std::vector<Record> out;
    eng.Downsample("temp", 0, 999, 100, TsdbEngine::Agg::kAvg, &out);
    CHECK(!out.empty());
    CHECK(std::fabs(std::stod(out.front().value) - 0.45) < 0.01);
}

TEST(EngineTTL) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_ttl";
    ResetDir(db);

    TsdbEngine eng(db);
    CHECK_ST(eng.Put("temp", 0, "1"));
    CHECK_ST(eng.Put("temp", 1000000000LL, "2"));
    CHECK_ST(eng.Put("temp", 2000000000LL, "3"));
    eng.SetTTL("temp", 1);

    std::string v;
    CHECK(!eng.Get("temp", 0, &v));
    CHECK(eng.Get("temp", 1000000000LL, &v));
    CHECK(eng.Get("temp", 2000000000LL, &v));

    std::vector<Record> out;
    eng.RangeQuery("temp", 0, 2000000000LL, &out);
    CHECK(out.size() == 2);
}

TEST(EngineTTLCompactionDropsExpiredRecords) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_ttl_compact";
    ResetDir(db);

    TsdbEngine eng(db, 128);
    CHECK_ST(eng.Put("temp", 0, "old"));
    CHECK_ST(eng.Put("temp", 1000000000LL, "mid"));
    CHECK_ST(eng.Put("temp", 2000000000LL, "new"));
    eng.Flush();
    CHECK_ST(eng.Put("temp", 3000000000LL, "latest"));
    eng.SetTTL("temp", 1);
    eng.Flush();
    eng.Compact(false);

    std::vector<Record> out;
    eng.RangeQuery("temp", 0, 3000000000LL, &out);
    CHECK(out.size() == 2);
    CHECK(out.front().timestamp >= 2000000000LL);
}

TEST(EngineCompactionSplitsOutputFiles) {
    EnsureTestRoot();
    const std::string db = std::string(kTestRoot) + "/eng_compact_split";
    ResetDir(db);

    TsdbEngine::Options options;
    options.memtable_limit = 256;
    options.sync_wal = false;
    options.target_file_size = 512;
    TsdbEngine eng(db, options);
    std::string value(80, 'x');
    for (int batch = 0; batch < 5; ++batch) {
        for (int i = 0; i < 25; ++i) {
            CHECK_ST(eng.Put("split.metric", batch * 100 + i, value));
        }
        eng.Flush();
    }
    eng.Compact(false);
    auto stats = eng.GetStats();
    CHECK(stats.level_files[1] > 1);
    CHECK(stats.version_file_bytes > 0);
}

struct Test {
    const char* name;
    void (*fn)();
};

}  // namespace

int main() {
    EnsureTestRoot();

    Test tests[] = {
        {"ComparatorBasic", ComparatorBasic},
        {"SkipListInsertGet", SkipListInsertGet},
        {"SkipListOverwrite", SkipListOverwrite},
        {"SkipListRangeOrder", SkipListRangeOrder},
        {"MemTableBasic", MemTableBasic},
        {"MemTableIterate", MemTableIterate},
        {"MemTableDelete", MemTableDelete},
        {"StressOrdering", StressOrdering},
        {"StatusBasic", StatusBasic},
        {"CRC32Basic", CRC32Basic},
        {"VarintBasic", VarintBasic},
        {"SSTableWriteRead", SSTableWriteRead},
        {"SSTableRangeScan", SSTableRangeScan},
        {"SSTableConcurrentReaderUsesRandomAccessFile", SSTableConcurrentReaderUsesRandomAccessFile},
        {"SSTableTombstone", SSTableTombstone},
        {"SSTableDataBlockCrcCorruption", SSTableDataBlockCrcCorruption},
        {"BloomFilterBasic", BloomFilterBasic},
        {"BloomFilterEncodeDecode", BloomFilterEncodeDecode},
        {"DeltaOfDeltaBasic", DeltaOfDeltaBasic},
        {"DeltaOfDeltaVariable", DeltaOfDeltaVariable},
        {"WALReplayAfterRestart", WALReplayAfterRestart},
        {"WALTruncateRecovery", WALTruncateRecovery},
        {"WALReplayDelete", WALReplayDelete},
        {"WALFragmentedReplay", WALFragmentedReplay},
        {"WALFragmentedTailTruncateRecovery", WALFragmentedTailTruncateRecovery},
        {"EngineBasicPutGet", EngineBasicPutGet},
        {"EngineDeleteInMem", EngineDeleteInMem},
        {"EngineFlushAndReloadSST", EngineFlushAndReloadSST},
        {"EngineDeleteFlushAndReload", EngineDeleteFlushAndReload},
        {"EngineFlushRestartRecovery", EngineFlushRestartRecovery},
        {"EngineWriteBackpressureLimitsMutableMemTable", EngineWriteBackpressureLimitsMutableMemTable},
        {"EngineCompaction", EngineCompaction},
        {"EngineBackgroundCompactionPublishesReadableReaders", EngineBackgroundCompactionPublishesReadableReaders},
        {"EngineConcurrentReadsDuringBackgroundCompaction", EngineConcurrentReadsDuringBackgroundCompaction},
        {"EngineWriteBatchRecovery", EngineWriteBatchRecovery},
        {"EngineDeleteCompaction", EngineDeleteCompaction},
        {"EngineCompactionVersionViewWithOldFiles", EngineCompactionVersionViewWithOldFiles},
        {"EngineManifestKeepsCompactionVersionAfterRestart", EngineManifestKeepsCompactionVersionAfterRestart},
        {"EngineCloseRewritesManifestSnapshot", EngineCloseRewritesManifestSnapshot},
        {"EngineStartupGarbageCollectsOrphanSSTables", EngineStartupGarbageCollectsOrphanSSTables},
        {"EngineBlockCacheHitsRepeatedRange", EngineBlockCacheHitsRepeatedRange},
        {"EngineCompactionObsoleteFileCleanup", EngineCompactionObsoleteFileCleanup},
        {"EngineDownsample", EngineDownsample},
        {"EngineTTL", EngineTTL},
        {"EngineTTLCompactionDropsExpiredRecords", EngineTTLCompactionDropsExpiredRecords},
        {"EngineCompactionSplitsOutputFiles", EngineCompactionSplitsOutputFiles},
    };

    for (const auto& t : tests) {
        std::printf("[RUN ] %s\n", t.name);
        int before = g_fail;
        t.fn();
        std::printf("[ %s ] %s\n", g_fail == before ? "OK  " : "FAIL", t.name);
    }

    std::printf("\n==== %d passed, %d failed ====\n", g_pass, g_fail);
    fs::remove_all(kTestRoot);
    return g_fail == 0 ? 0 : 1;
}
