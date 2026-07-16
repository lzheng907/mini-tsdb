#ifndef MINITSDB_SSTABLE_H
#define MINITSDB_SSTABLE_H

#include "bloomfilter.h"
#include "block_cache.h"
#include "random_access_file.h"
#include "status.h"
#include "utils.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace minitsdb {

static constexpr uint64_t kSSTableMagic = 0x3230494E494D5F53ULL;  // "S_MINI02"

class SSTableBuilder {
public:
    explicit SSTableBuilder(const std::string& fname, int block_size = 4096);

    void Add(const TsKey& key, const std::string& value);
    void Add(const TsKey& key, const InternalEntry& entry);
    Status Finish();

    size_t NumEntries() const { return num_entries_; }

private:
    void FlushBlock();

    std::string fname_;
    FILE* file_ = nullptr;
    int block_size_;

    std::string block_buf_;
    TsKey last_block_key_;
    TsKey prev_key_;
    std::vector<uint64_t> block_offsets_;
    std::vector<uint32_t> block_sizes_;
    std::vector<TsKey> block_last_keys_;
    int64_t file_offset_ = 0;
    BloomFilter bloom_;
    size_t num_entries_ = 0;
};

class SSTableReader {
public:
    explicit SSTableReader(const std::string& fname);
    ~SSTableReader();

    Status Open();
    void SetBlockCache(BlockCache* cache) { block_cache_ = cache; }

    bool Get(const TsKey& key, std::string* value);
    bool GetEntry(const TsKey& key, InternalEntry* entry);
    bool GetEntry(const TsKey& key, InternalEntry* entry, bool* data_block_read);

    void RangeScan(const TsKey& begin, const TsKey& end,
                   std::vector<Record>* out);
    void RangeScanEntries(const TsKey& begin, const TsKey& end,
                          std::vector<Record>* out);

    const TsKey& MaxKey() const { return max_key_; }
    const TsKey& MinKey() const { return min_key_; }

    const std::string& FileName() const { return fname_; }

private:
    std::string ReadBlock(uint64_t offset, uint32_t size);
    bool GetFromBlock(const std::string& block_data, const TsKey& key,
                      InternalEntry* entry);

    std::string fname_;
    std::unique_ptr<RandomAccessFile> file_;

    struct IndexEntry {
        TsKey last_key;
        uint64_t offset;
        uint32_t size;
    };
    std::vector<IndexEntry> index_;

    BloomFilter bloom_;
    BlockCache* block_cache_ = nullptr;

    TsKey min_key_;
    TsKey max_key_;
};

}  // namespace minitsdb

#endif  // MINITSDB_SSTABLE_H
