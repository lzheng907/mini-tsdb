// minitsdb/demo/sensor_demo.cpp
//
// 端到端 demo：模拟车端传感器样本写入并回放查询。
// 使用完整 TsdbEngine (WAL + MemTable + SSTable)。
#include "../src/tsdb_engine.h"
#include "../src/utils.h"

#include <cstdio>
#include <filesystem>
#include <string>
#include <vector>

int main() {
    namespace fs = std::filesystem;
    const std::string db_dir = "demo_data";
    // 清理旧数据
    fs::remove_all(db_dir);
    fs::create_directories(db_dir);

    minitsdb::TsdbEngine db(db_dir);

    const int64_t T0 = 1'700'000'000'000'000'000LL;

    // 1) 模拟 100 个 IMU 加速度样本（100Hz -> 每 10ms 一个）
    for (int i = 0; i < 100; ++i) {
        int64_t ts = T0 + i * 10'000'000LL;
        db.Put("imu.accel.x", ts, std::to_string(0.01 * i));
        db.Put("imu.accel.y", ts, std::to_string(9.8));
    }

    // 2) 模拟 10 个 GPS 样本（10Hz -> 每 100ms 一个）
    for (int i = 0; i < 10; ++i) {
        int64_t ts = T0 + i * 100'000'000LL;
        db.Put("gps.lat", ts, std::to_string(34.0 + 0.0001 * i));
        db.Put("gps.lon", ts, std::to_string(108.0 + 0.0001 * i));
    }

    std::printf("写入完成，内存占用约 %.1f KB\n", db.MemUsage() / 1024.0);

    // 3) 点查
    std::string val;
    if (db.Get("imu.accel.x", T0 + 500'000'000LL, &val)) {
        std::printf("imu.accel.x @ +500ms = %s\n", val.c_str());
    }

    // 4) 范围回放
    std::vector<minitsdb::Record> out;
    db.RangeQuery("imu.accel.x", T0 + 200'000'000LL, T0 + 250'000'000LL, &out);
    std::printf("imu.accel.x 回放 [+200ms, +250ms]: %zu 条\n", out.size());
    for (const auto& r : out) {
        int64_t off_ms = (r.timestamp - T0) / 1'000'000LL;
        std::printf("  t=%lldms  x=%s\n", static_cast<long long>(off_ms), r.value.c_str());
    }

    // 5) 降采样演示: 100Hz -> 1Hz
    std::vector<minitsdb::Record> down;
    db.Downsample("imu.accel.x", T0, T0 + 990'000'000LL,
                   100'000'000LL, minitsdb::TsdbEngine::Agg::kAvg, &down);
    std::printf("降采样 (100Hz->1Hz): %zu 条\n", down.size());
    for (const auto& r : down) {
        int64_t off_ms = (r.timestamp - T0) / 1'000'000LL;
        std::printf("  t=%lldms  avg_x=%s\n", static_cast<long long>(off_ms), r.value.c_str());
    }

    db.Close();
    std::printf("\n演示完成。\n");
    return 0;
}
