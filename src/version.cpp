#include "version.h"

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace minitsdb {

VersionManager::VersionManager(const std::string& db_dir) : db_dir_(db_dir) {}

namespace {

std::string BaseName(const std::string& path) {
    return std::filesystem::path(path).filename().string();
}

}  // namespace

Status VersionManager::Init() {
    std::filesystem::create_directories(db_dir_);
    manifest_name_ = db_dir_ + "/MANIFEST-000001";
    return RecoverFromManifest();
}

const Version& VersionManager::CurrentVersion() const { return current_; }

void VersionManager::Apply(VersionEdit& edit) {
    std::lock_guard<std::mutex> lock(mu_);
    ApplyUnlocked(edit);
}

void VersionManager::ApplyUnlocked(VersionEdit& edit) {
    for (const auto& item : edit.deleted_files) {
        int level = item.first;
        uint64_t file_number = item.second;
        if (level < 0 || level >= Version::kMaxLevel) continue;
        auto& files = current_.level_files[level];
        for (const auto& meta : files) {
            if (meta && meta->file_number == file_number) {
                obsolete_files_.push_back(meta->file_name);
                break;
            }
        }
        files.erase(std::remove_if(files.begin(), files.end(),
                                   [file_number](const std::shared_ptr<FileMeta>& meta) {
                                       return meta->file_number == file_number;
                                   }),
                    files.end());
    }

    for (const auto& item : edit.added_files) {
        int level = item.first;
        const auto& meta = item.second;
        if (level < 0 || level >= Version::kMaxLevel) continue;
        current_.level_files[level].push_back(meta);
    }

    for (int level = 0; level < Version::kMaxLevel; ++level) {
        auto& files = current_.level_files[level];
        std::sort(files.begin(), files.end(),
                  [](const std::shared_ptr<FileMeta>& a,
                     const std::shared_ptr<FileMeta>& b) {
                      return a->file_number < b->file_number;
                  });
    }

    if (!edit.added_files.empty()) {
        const auto& last = edit.added_files.back().second;
        if (last && last->file_number >= next_file_number_) {
            next_file_number_ = last->file_number + 1;
        }
    }

    edit.Clear();
}

Status VersionManager::LogAndApply(VersionEdit& edit) {
    std::lock_guard<std::mutex> lock(mu_);
    if (manifest_name_.empty()) manifest_name_ = db_dir_ + "/MANIFEST-000001";
    std::ofstream out(manifest_name_, std::ios::app);
    if (!out) return Status::IOError("Cannot open manifest: " + manifest_name_);

    for (const auto& item : edit.deleted_files) {
        out << "del " << item.first << " " << item.second << "\n";
    }
    for (const auto& item : edit.added_files) {
        const auto& meta = item.second;
        if (!meta) continue;
        out << "add " << item.first << " "
            << meta->file_number << " "
            << BaseName(meta->file_name) << " "
            << meta->file_size << " "
            << meta->smallest.measurement << " "
            << meta->smallest.timestamp << " "
            << meta->largest.measurement << " "
            << meta->largest.timestamp << "\n";
    }
    out << "next_file " << next_file_number_ << "\n";
    out.flush();
    if (!out) return Status::IOError("Failed to write manifest");

    std::ofstream current(db_dir_ + "/CURRENT", std::ios::trunc);
    if (!current) return Status::IOError("Cannot write CURRENT");
    current << BaseName(manifest_name_) << "\n";
    current.flush();
    if (!current) return Status::IOError("Failed to write CURRENT");

    ApplyUnlocked(edit);
    return Status::OK();
}

Status VersionManager::RecoverFromManifest() {
    std::ifstream current(db_dir_ + "/CURRENT");
    if (current) {
        std::string name;
        std::getline(current, name);
        if (!name.empty()) {
            manifest_name_ = db_dir_ + "/" + name;
        }
    }
    if (manifest_name_.empty()) manifest_name_ = db_dir_ + "/MANIFEST-000001";

    std::ifstream in(manifest_name_);
    if (!in) return Status::NotFound("manifest not found");

    current_ = Version();
    obsolete_files_.clear();
    next_file_number_ = 1;

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        std::istringstream iss(line);
        std::string tag;
        iss >> tag;
        if (tag == "next_file") {
            uint64_t n = 0;
            if (iss >> n && n > next_file_number_) next_file_number_ = n;
            continue;
        }

        VersionEdit edit;
        if (tag == "add") {
            int level = 0;
            auto meta = std::make_shared<FileMeta>();
            std::string fname;
            if (!(iss >> level >> meta->file_number >> fname >> meta->file_size >>
                  meta->smallest.measurement >> meta->smallest.timestamp >>
                  meta->largest.measurement >> meta->largest.timestamp)) {
                return Status::Corruption("Bad manifest add record");
            }
            meta->file_name = db_dir_ + "/" + fname;
            edit.added_files.push_back({level, meta});
            Apply(edit);
        } else if (tag == "del") {
            int level = 0;
            uint64_t file_number = 0;
            if (!(iss >> level >> file_number)) {
                return Status::Corruption("Bad manifest del record");
            }
            edit.deleted_files.push_back({level, file_number});
            Apply(edit);
        }
    }
    obsolete_files_.clear();
    return Status::OK();
}

Status VersionManager::WriteSnapshotManifest() {
    VersionEdit snapshot;
    uint64_t next_file_number = 0;
    {
        std::lock_guard<std::mutex> lock(mu_);
        for (int level = 0; level < Version::kMaxLevel; ++level) {
            for (const auto& file : current_.level_files[level]) {
                snapshot.added_files.push_back({level, file});
            }
        }
        next_file_number = next_file_number_;
    }

    if (manifest_name_.empty()) manifest_name_ = db_dir_ + "/MANIFEST-000001";
    std::ofstream out(manifest_name_, std::ios::trunc);
    if (!out) return Status::IOError("Cannot rewrite manifest");
    for (const auto& item : snapshot.added_files) {
        const auto& meta = item.second;
        out << "add " << item.first << " "
            << meta->file_number << " "
            << BaseName(meta->file_name) << " "
            << meta->file_size << " "
            << meta->smallest.measurement << " "
            << meta->smallest.timestamp << " "
            << meta->largest.measurement << " "
            << meta->largest.timestamp << "\n";
    }
    out << "next_file " << next_file_number << "\n";
    out.flush();
    return out ? Status::OK() : Status::IOError("Failed to rewrite manifest");
}

uint64_t VersionManager::NewFileNumber() {
    std::lock_guard<std::mutex> lock(mu_);
    return next_file_number_++;
}

void VersionManager::MarkFileNumberUsed(uint64_t number) {
    std::lock_guard<std::mutex> lock(mu_);
    if (number >= next_file_number_) {
        next_file_number_ = number + 1;
    }
}

std::string VersionManager::NewFileName(uint64_t number) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%06llu.sst",
                  static_cast<unsigned long long>(number));
    return db_dir_ + "/" + buf;
}

bool VersionManager::Overlaps(int level, const TsKey& smallest, const TsKey& largest) {
    std::lock_guard<std::mutex> lock(mu_);
    if (level < 0 || level >= Version::kMaxLevel) return false;

    TsKeyComparator cmp;
    for (const auto& file : current_.level_files[level]) {
        if (!(cmp(file->largest, smallest) < 0 || cmp(file->smallest, largest) > 0)) {
            return true;
        }
    }
    return false;
}

std::vector<std::shared_ptr<FileMeta>> VersionManager::GetLevelFiles(int level) {
    std::lock_guard<std::mutex> lock(mu_);
    if (level < 0 || level >= Version::kMaxLevel) return {};
    return current_.level_files[level];
}

int VersionManager::L0Size() const {
    std::lock_guard<std::mutex> lock(mu_);
    return static_cast<int>(current_.level_files[0].size());
}

size_t VersionManager::NumFiles() const {
    std::lock_guard<std::mutex> lock(mu_);
    size_t total = 0;
    for (int level = 0; level < Version::kMaxLevel; ++level) {
        total += current_.level_files[level].size();
    }
    return total;
}

size_t VersionManager::NumFiles(int level) const {
    std::lock_guard<std::mutex> lock(mu_);
    if (level < 0 || level >= Version::kMaxLevel) return 0;
    return current_.level_files[level].size();
}

uint64_t VersionManager::TotalFileSize() const {
    std::lock_guard<std::mutex> lock(mu_);
    uint64_t total = 0;
    for (int level = 0; level < Version::kMaxLevel; ++level) {
        for (const auto& file : current_.level_files[level]) {
            if (file) total += file->file_size;
        }
    }
    return total;
}

std::vector<std::string> VersionManager::ConsumeObsoleteFiles() {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<std::string> files;
    files.swap(obsolete_files_);
    return files;
}

}  // namespace minitsdb
