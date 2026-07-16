# mini-tsdb

基于 C++17 实现的轻量级时序存储引擎，采用 LSM-tree 写入路径，面向 `(measurement, timestamp)` 时序键。

## 核心能力

- 支持 Put、Get、范围查询、删除墓碑和 WriteBatch。
- WAL 支持 CRC 校验、分片记录、截断恢复与同步/异步两种落盘模式。
- SkipList MemTable、后台 Flush、SSTable、Bloom Filter、Block Cache 与后台 Compaction。
- 支持 Manifest 恢复、TTL 清理、降采样及 Compaction 输出切分。

## 构建与测试

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
cd build && ctest --output-on-failure
```

运行示例与基准：

```bash
./build/sensor_demo
./build/observability_demo
./build/minitsdb_bench
```

## 性能验证

Linux Release 环境下，完整单元测试 **6,789 passed, 0 failed**。连续 3 轮基准平均结果：

| 场景 | 结果 |
|---|---:|
| 异步 WAL 顺序写 | 38.0w ops/s |
| 随机写 | 31.8w ops/s |
| 同步 WAL 批写（batch=100） | 14.9w ops/s |
| 点查延迟 | 26.45 us/op |
| 100 行范围查询 | 62.68 us/op |
| WAL 回放 2 万条记录 | 0.020 s |

性能会受硬件、磁盘与系统负载影响，以上数据用于版本验证与参考。

## 目录

```text
src/      核心存储引擎
tests/    单元测试与 smoke test
bench/    性能基准
demo/     传感器与可观测性示例
```
