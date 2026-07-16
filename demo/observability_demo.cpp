#include "../src/tsdb_engine.h"
#include "../src/write_batch.h"

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

const char* kDbDir = "demo_data/observability";

void ResetDir(const std::string& dir) {
    fs::remove_all(dir);
    fs::create_directories(dir);
}

void PrintRange(const char* title, const std::vector<minitsdb::Record>& rows,
                size_t limit = 6) {
    std::printf("\n%s count=%zu\n", title, rows.size());
    for (size_t i = 0; i < rows.size() && i < limit; ++i) {
        std::printf("  ts=%lld value=%s\n",
                    static_cast<long long>(rows[i].timestamp),
                    rows[i].value.c_str());
    }
}

}  // namespace

int main() {
    ResetDir(kDbDir);

    minitsdb::TsdbEngine::Options options;
    options.sync_wal = true;
    options.memtable_limit = 128 * 1024;
    options.block_cache_bytes = 256 * 1024;

    const int64_t second = 1000000000LL;
    const int64_t start = 1700000000LL * second;
    const int minutes = 10;

    {
        minitsdb::TsdbEngine engine(kDbDir, options);
        for (int t = 0; t < minutes * 60; ++t) {
            minitsdb::WriteBatch batch;
            int64_t ts = start + static_cast<int64_t>(t) * second;
            for (int worker = 1; worker <= 3; ++worker) {
                std::string node = "node.worker0" + std::to_string(worker);
                batch.Put(node + ".qps", ts, std::to_string(35 + worker * 3 + t % 11));
                batch.Put(node + ".queue_depth", ts,
                          std::to_string((worker * 7 + t) % 32));
                batch.Put(node + ".rpc_latency_ms", ts,
                          std::to_string(15 + worker + (t % 9)));
                batch.Put(node + ".error_count", ts,
                          std::to_string((t % 97 == 0) ? worker : 0));
            }
            batch.Put("node.bus01.inflight_tasks", ts, std::to_string(20 + t % 40));
            batch.Put("model.qwen.ttft_ms", ts, std::to_string(80 + t % 25));
            batch.Put("model.qwen.tpot_ms", ts, std::to_string(12 + t % 7));
            batch.Put("gpu.3090.memory_used_mb", ts, std::to_string(9200 + (t % 80) * 8));
            engine.Write(batch);
        }
        engine.Flush();

        std::vector<minitsdb::Record> rows;
        engine.RangeQuery("node.worker01.qps", start + 8 * 60 * second,
                          start + 10 * 60 * second, &rows);
        PrintRange("worker01 qps recent raw", rows);

        engine.Downsample("model.qwen.ttft_ms", start, start + minutes * 60 * second,
                          60 * second, minitsdb::TsdbEngine::Agg::kAvg, &rows);
        PrintRange("model.qwen.ttft_ms downsample avg per minute", rows);

        auto stats = engine.GetStats();
        std::printf("\nversion_files=%zu version_file_bytes=%llu cache_entries=%zu\n",
                    stats.version_files,
                    static_cast<unsigned long long>(stats.version_file_bytes),
                    stats.block_cache_entries);
    }

    {
        minitsdb::TsdbEngine engine(kDbDir, options);
        std::vector<minitsdb::Record> rows;
        engine.RangeQuery("node.worker02.queue_depth", start + 9 * 60 * second,
                          start + 10 * 60 * second, &rows);
        PrintRange("after restart worker02 queue_depth", rows);
        std::printf("\nobservability demo ok\n");
    }

    return 0;
}
