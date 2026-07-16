// minitsdb/tests/smoke_test.cpp
// 最小烟雾测试：逐个启用，定位崩溃点
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

#include <cstdio>
#include <filesystem>
#include <string>
#include <vector>

using namespace minitsdb;
namespace fs = std::filesystem;

namespace {
const char* kTestRoot = "test_data";

void ResetDir(const std::string& dir) {
    fs::remove_all(dir);
    fs::create_directories(dir);
}
}

int main() {
    fs::create_directories(kTestRoot);
    std::printf("STEP 1: status\n");
    { Status s = Status::OK(); std::printf("  ok=%d\n", s.ok()); }

    std::printf("STEP 2: crc32\n");
    { uint32_t c = Crc32("hello", 5); std::printf("  crc=%u\n", c); }

    std::printf("STEP 3: varint\n");
    {
        std::string s; EncodeVarint(128, &s);
        size_t p = 0; uint32_t v = DecodeVarint(s, &p);
        std::printf("  varint=%u\n", v);
    }

    std::printf("STEP 4: bloomfilter\n");
    {
        BloomFilter bf; bf.Add("k1");
        std::printf("  contains k1=%d\n", bf.MayContain("k1") ? 1 : 0);
    }

    std::printf("STEP 5: compression\n");
    {
        DeltaOfDeltaEncoder enc;
        enc.Add(1000); enc.Add(1010); enc.Add(1020);
        std::string comp = enc.Finish();
        DeltaOfDeltaDecoder dec; dec.Init(comp);
        int64_t ts;
        while (dec.Next(&ts)) std::printf("  ts=%lld\n", (long long)ts);
    }

    std::printf("STEP 6: skiplist\n");
    {
        SkipList<TsKey, std::string, TsKeyComparator> sl;
        sl.Insert({"a", 1}, "v1");
        std::string v;
        sl.Get({"a", 1}, &v);
        std::printf("  sl=%s\n", v.c_str());
    }

    std::printf("STEP 7: memtable\n");
    {
        MemTable mt; mt.Put("a", 1, "v1");
        std::string v; mt.Get("a", 1, &v);
        std::printf("  mt=%s\n", v.c_str());
    }

    std::printf("STEP 8: sstable\n");
    {
        std::remove("test_data/smoke.sst");
        SSTableBuilder b("test_data/smoke.sst", 256);
        b.Add({"a", 1}, "v1");
        b.Add({"a", 2}, "v2");
        b.Add({"b", 1}, "v3");
        Status s = b.Finish();
        std::printf("  build ok=%d\n", s.ok());

        SSTableReader r("test_data/smoke.sst");
        Status s2 = r.Open();
        std::printf("  open ok=%d\n", s2.ok());
        std::string v;
        bool found = r.Get({"a", 1}, &v);
        std::printf("  get a,1 found=%d v=%s\n", found ? 1 : 0, v.c_str());
    }

    std::printf("STEP 9: engine\n");
    {
        ResetDir("test_data/smoke_eng");
        TsdbEngine eng("test_data/smoke_eng");
        eng.Put("a", 1, "v1");
        std::string v;
        bool found = eng.Get("a", 1, &v);
        std::printf("  eng get found=%d v=%s\n", found ? 1 : 0, v.c_str());
        eng.Close();
    }

    std::printf("ALL STEPS DONE\n");
    return 0;
}
