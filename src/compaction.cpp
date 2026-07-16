#include "compaction.h"

#include <algorithm>
#include <filesystem>

namespace minitsdb {

namespace fs = std::filesystem;

Compaction::Compaction(VersionManager* version_mgr) : vmgr_(version_mgr) {}

Status Compaction::Run(const CompactionJob& job,
                       const SSTableRecordReader& reader,
                       const RecordFilter& filter,
                       CompactionResult* result) {
    if (!result) {
        return Status::InvalidArgument("CompactionResult is null");
    }
    *result = CompactionResult();
    if (job.inputs.empty()) return Status::OK();
    if (job.output_level >= Version::kMaxLevel) {
        return Status::InvalidArgument("Too many levels");
    }

    struct RankedRecord {
        Record record;
        size_t rank;
    };

    std::vector<RankedRecord> all_records;
    for (size_t file_index = 0; file_index < job.inputs.size(); ++file_index) {
        const auto& input = job.inputs[file_index];
        if (!input.meta) continue;
        std::vector<Record> records;
        reader(input.meta->file_name, &records);
        for (const auto& record : records) {
            if (filter && !filter(record)) continue;
            all_records.push_back(RankedRecord{record, file_index});
        }
    }

    std::sort(all_records.begin(), all_records.end(),
              [](const RankedRecord& a, const RankedRecord& b) {
                  TsKeyComparator cmp;
                  int c = cmp(TsKey{a.record.measurement, a.record.timestamp},
                              TsKey{b.record.measurement, b.record.timestamp});
                  if (c != 0) return c < 0;
                  return a.rank > b.rank;
              });

    std::vector<Record> merged;
    for (size_t i = 0; i < all_records.size();) {
        size_t j = i + 1;
        while (j < all_records.size() &&
               all_records[j].record.measurement == all_records[i].record.measurement &&
               all_records[j].record.timestamp == all_records[i].record.timestamp) {
            ++j;
        }
        merged.push_back(all_records[i].record);
        i = j;
    }

    for (const auto& input : job.inputs) {
        if (!input.meta) continue;
        if (input.level >= 0 && input.level < Version::kMaxLevel) {
            result->edit.deleted_files.push_back({input.level, input.meta->file_number});
        }
    }

    const uint64_t target = target_file_size_ == 0 ? UINT64_MAX : target_file_size_;
    size_t begin = 0;
    while (begin < merged.size()) {
        uint64_t approx_bytes = 0;
        size_t end = begin;
        while (end < merged.size()) {
            approx_bytes += merged[end].measurement.size() + merged[end].value.size() + 32;
            ++end;
            if (approx_bytes >= target) break;
        }

        uint64_t new_num = vmgr_->NewFileNumber();
        std::string new_name = vmgr_->NewFileName(new_num);
        SSTableBuilder builder(new_name);
        for (size_t i = begin; i < end; ++i) {
            const auto& record = merged[i];
            builder.Add(TsKey{record.measurement, record.timestamp},
                        InternalEntry{record.value,
                                      record.deleted ? EntryType::kDeletion
                                                     : EntryType::kValue});
        }
        Status s = builder.Finish();
        if (!s.ok()) return s;

        auto meta = std::make_shared<FileMeta>();
        meta->file_number = new_num;
        meta->file_name = new_name;
        try {
            meta->file_size = static_cast<uint64_t>(fs::file_size(new_name));
        } catch (...) {
            // 保守 fallback：正常路径应写入真实文件大小；异常时不影响版本更新。
            meta->file_size = 0;
        }
        meta->smallest = TsKey{merged[begin].measurement, merged[begin].timestamp};
        meta->largest = TsKey{merged[end - 1].measurement, merged[end - 1].timestamp};
        result->new_files.push_back(new_name);
        auto new_reader = std::make_shared<SSTableReader>(new_name);
        Status open_status = new_reader->Open();
        if (!open_status.ok()) return open_status;

        result->edit.added_files.push_back({job.output_level, meta});
        result->new_readers.push_back(std::move(new_reader));
        begin = end;
    }

    return Status::OK();
}

}  // namespace minitsdb
