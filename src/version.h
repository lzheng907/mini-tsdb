#ifndef MINITSDB_VERSION_H
#define MINITSDB_VERSION_H

#include "status.h"
#include "utils.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace minitsdb {

struct FileMeta {
    uint64_t file_number = 0;
    std::string file_name;
    uint64_t file_size = 0;
    TsKey smallest;
    TsKey largest;
    bool allowed_seeks = true;
};

struct Version {
    static constexpr int kMaxLevel = 7;
    std::vector<std::shared_ptr<FileMeta>> level_files[kMaxLevel];
};

struct VersionEdit {
    std::vector<std::pair<int, std::shared_ptr<FileMeta>>> added_files;
    std::vector<std::pair<int, uint64_t>> deleted_files;

    void Clear() {
        added_files.clear();
        deleted_files.clear();
    }
};

class VersionManager {
public:
    explicit VersionManager(const std::string& db_dir);

    Status Init();
    const Version& CurrentVersion() const;
    void Apply(VersionEdit& edit);
    Status LogAndApply(VersionEdit& edit);
    Status RecoverFromManifest();
    Status WriteSnapshotManifest();

    uint64_t NewFileNumber();
    void MarkFileNumberUsed(uint64_t number);
    std::string NewFileName(uint64_t number);

    const std::string& DbDir() const { return db_dir_; }
    bool Overlaps(int level, const TsKey& smallest, const TsKey& largest);
    std::vector<std::shared_ptr<FileMeta>> GetLevelFiles(int level);
    int L0Size() const;
    size_t NumFiles() const;
    size_t NumFiles(int level) const;
    uint64_t TotalFileSize() const;
    std::vector<std::string> ConsumeObsoleteFiles();

private:
    std::string db_dir_;
    std::string manifest_name_;
    Version current_;
    uint64_t next_file_number_ = 1;
    std::vector<std::string> obsolete_files_;
    mutable std::mutex mu_;

    void ApplyUnlocked(VersionEdit& edit);
};

}  // namespace minitsdb

#endif  // MINITSDB_VERSION_H
