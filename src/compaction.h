// minitsdb/src/compaction.h
//
// Compaction: 后台合并 SSTable，消除重复 key、回收空间、降低读放大。
//
// 策略: Leveled Compaction (参考 LevelDB)
//   - L0 文件间可重叠，超过阈值 (kL0CompactionTrigger=4) 触发 L0→L1
//   - Li 总大小超过 10^(i+1) MB 触发 Li→L(i+1)
//   - Compaction: 多路归并 → 输出新 SSTable → 返回 VersionEdit
#ifndef MINITSDB_COMPACTION_H
#define MINITSDB_COMPACTION_H

#include "sstable.h"
#include "status.h"
#include "version.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace minitsdb {

struct CompactionInput {
    int level = 0;
    std::shared_ptr<FileMeta> meta;
};

struct CompactionJob {
    std::vector<CompactionInput> inputs;
    int output_level = 1;
};

struct CompactionResult {
    VersionEdit edit;
    std::vector<std::shared_ptr<SSTableReader>> new_readers;
    std::vector<std::string> new_files;
};

class Compaction {
public:
    using SSTableRecordReader =
        std::function<void(const std::string& fname, std::vector<Record>* records)>;
    using RecordFilter = std::function<bool(const Record& record)>;

    explicit Compaction(VersionManager* version_mgr);

    void SetTargetFileSize(uint64_t bytes) { target_file_size_ = bytes; }

    // 根据调用方传入的输入快照执行重活，生成新文件和 VersionEdit。
    // Version 切换、reader 发布、旧文件清理由 TsdbEngine 在写锁下完成。
    Status Run(const CompactionJob& job,
               const SSTableRecordReader& reader,
               const RecordFilter& filter,
               CompactionResult* result);

    VersionManager* vmgr_;

    uint64_t target_file_size_ = 2 * 1024 * 1024;
};

}  // namespace minitsdb

#endif  // MINITSDB_COMPACTION_H
