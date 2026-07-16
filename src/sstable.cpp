#include "sstable.h"
#include "compression.h"
#include "crc32.h"

#include <algorithm>
#include <cstdio>
#include <sstream>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace minitsdb {

namespace {

void EncodeFixed32Str(std::string* s, uint32_t v) {
    char buf[4];
    buf[0] = static_cast<char>(v & 0xFF);
    buf[1] = static_cast<char>((v >> 8) & 0xFF);
    buf[2] = static_cast<char>((v >> 16) & 0xFF);
    buf[3] = static_cast<char>((v >> 24) & 0xFF);
    s->append(buf, 4);
}

void EncodeFixed64Str(std::string* s, uint64_t v) {
    char buf[8];
    for (int i = 0; i < 8; ++i) {
        buf[i] = static_cast<char>((v >> (i * 8)) & 0xFF);
    }
    s->append(buf, 8);
}

uint32_t DecodeFixed32(const char* p) {
    return static_cast<uint32_t>(static_cast<unsigned char>(p[0])) |
           (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 8) |
           (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 16) |
           (static_cast<uint32_t>(static_cast<unsigned char>(p[3])) << 24);
}

uint64_t DecodeFixed64(const char* p) {
    uint64_t r = 0;
    for (int i = 0; i < 8; ++i) {
        r |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (i * 8);
    }
    return r;
}

void EncodeKeyTo(std::string* out, const TsKey& key) {
    EncodeVarint(static_cast<uint32_t>(key.measurement.size()), out);
    out->append(key.measurement);
    EncodeFixed64Str(out, static_cast<uint64_t>(key.timestamp));
}

std::string EncodeKey(const TsKey& key) {
    std::string out;
    EncodeKeyTo(&out, key);
    return out;
}

bool DecodeKey(const std::string& input, TsKey* key) {
    size_t pos = 0;
    uint32_t m_len = DecodeVarint(input, &pos);
    if (m_len == 0xFFFFFFFF || pos + m_len + 8 > input.size()) return false;
    key->measurement.assign(input.data() + pos, m_len);
    pos += m_len;
    key->timestamp = static_cast<int64_t>(DecodeFixed64(input.data() + pos));
    pos += 8;
    return pos == input.size();
}

}  // namespace

SSTableBuilder::SSTableBuilder(const std::string& fname, int block_size)
    : fname_(fname), file_(fopen(fname.c_str(), "wb")), block_size_(block_size) {}

void SSTableBuilder::Add(const TsKey& key, const std::string& value) {
    Add(key, InternalEntry{value, EntryType::kValue});
}

void SSTableBuilder::Add(const TsKey& key, const InternalEntry& entry) {
    if (!file_) return;

    std::string key_buf = EncodeKey(key);
    EncodeVarint(static_cast<uint32_t>(key_buf.size()), &block_buf_);
    block_buf_.append(key_buf);
    block_buf_.push_back(static_cast<char>(entry.type));
    EncodeVarint(static_cast<uint32_t>(entry.value.size()), &block_buf_);
    block_buf_.append(entry.value);

    bloom_.Add(key_buf);
    last_block_key_ = key;
    prev_key_ = key;
    ++num_entries_;

    if (static_cast<int>(block_buf_.size()) >= block_size_) {
        FlushBlock();
    }
}

void SSTableBuilder::FlushBlock() {
    if (!file_ || block_buf_.empty()) return;

    block_offsets_.push_back(static_cast<uint64_t>(file_offset_));
    std::string record;
    EncodeFixed32Str(&record, Crc32(block_buf_));
    record.append(block_buf_);
    fwrite(record.data(), 1, record.size(), file_);
    file_offset_ += static_cast<int64_t>(record.size());
    block_sizes_.push_back(static_cast<uint32_t>(record.size()));
    block_last_keys_.push_back(last_block_key_);
    block_buf_.clear();
}

Status SSTableBuilder::Finish() {
    if (!file_) return Status::IOError("File not open: " + fname_);

    FlushBlock();

    uint64_t index_offset = static_cast<uint64_t>(file_offset_);
    std::string index_buf;
    EncodeVarint(static_cast<uint32_t>(block_offsets_.size()), &index_buf);
    for (size_t i = 0; i < block_offsets_.size(); ++i) {
        std::string key_buf = EncodeKey(block_last_keys_[i]);
        EncodeVarint(static_cast<uint32_t>(key_buf.size()), &index_buf);
        index_buf.append(key_buf);
        EncodeFixed64Str(&index_buf, block_offsets_[i]);
        EncodeFixed64Str(&index_buf, static_cast<uint64_t>(block_sizes_[i]));
    }
    fwrite(index_buf.data(), 1, index_buf.size(), file_);
    file_offset_ += static_cast<int64_t>(index_buf.size());

    uint64_t bloom_offset = static_cast<uint64_t>(file_offset_);
    std::string bloom_data = bloom_.Encode();
    fwrite(bloom_data.data(), 1, bloom_data.size(), file_);
    file_offset_ += static_cast<int64_t>(bloom_data.size());

    char footer[24];
    for (int i = 0; i < 8; ++i) {
        footer[i] = static_cast<char>((index_offset >> (i * 8)) & 0xFF);
        footer[8 + i] = static_cast<char>((bloom_offset >> (i * 8)) & 0xFF);
        footer[16 + i] = static_cast<char>((kSSTableMagic >> (i * 8)) & 0xFF);
    }
    fwrite(footer, 1, sizeof(footer), file_);
    fflush(file_);
#ifdef _WIN32
    int fd = _fileno(file_);
    if (fd >= 0) _commit(fd);
#else
    fsync(fileno(file_));
#endif
    fclose(file_);
    file_ = nullptr;
    return Status::OK();
}

SSTableReader::SSTableReader(const std::string& fname) : fname_(fname) {}

SSTableReader::~SSTableReader() = default;

Status SSTableReader::Open() {
    file_ = std::make_unique<RandomAccessFile>(fname_);
    Status status = file_->Open();
    if (!status.ok()) return status;

    uint64_t file_size = 0;
    status = file_->Size(&file_size);
    if (!status.ok()) return status;
    if (file_size < 24) {
        return Status::Corruption("File too small for footer");
    }

    std::string footer;
    status = file_->ReadAt(file_size - 24, 24, &footer);
    if (!status.ok()) return Status::IOError("Failed to read footer");

    uint64_t index_offset = DecodeFixed64(footer.data());
    uint64_t bloom_offset = DecodeFixed64(footer.data() + 8);
    uint64_t magic = DecodeFixed64(footer.data() + 16);
    if (magic != kSSTableMagic || index_offset > bloom_offset ||
        bloom_offset > file_size - 24) {
        return Status::Corruption("Bad footer");
    }

    size_t index_size = static_cast<size_t>(bloom_offset - index_offset);
    std::string index_data;
    if (index_size > 0) {
        status = file_->ReadAt(index_offset, index_size, &index_data);
        if (!status.ok()) return Status::IOError("Failed to read index block");
    }

    size_t bloom_size = static_cast<size_t>(file_size - 24 - bloom_offset);
    std::string bloom_data;
    if (bloom_size > 0) {
        status = file_->ReadAt(bloom_offset, bloom_size, &bloom_data);
        if (!status.ok()) return Status::IOError("Failed to read bloom block");
    }
    bloom_ = BloomFilter::Decode(bloom_data);

    index_.clear();
    size_t pos = 0;
    uint32_t num_blocks = DecodeVarint(index_data, &pos);
    if (num_blocks == 0xFFFFFFFF) num_blocks = 0;
    for (uint32_t i = 0; i < num_blocks; ++i) {
        uint32_t key_len = DecodeVarint(index_data, &pos);
        if (key_len == 0xFFFFFFFF || pos + key_len + 16 > index_data.size()) {
            return Status::Corruption("Bad index block");
        }
        std::string key_buf(index_data.data() + pos, key_len);
        pos += key_len;

        IndexEntry entry;
        if (!DecodeKey(key_buf, &entry.last_key)) {
            return Status::Corruption("Bad index key");
        }
        entry.offset = DecodeFixed64(index_data.data() + pos);
        pos += 8;
        entry.size = static_cast<uint32_t>(DecodeFixed64(index_data.data() + pos));
        pos += 8;
        index_.push_back(entry);
    }

    if (!index_.empty()) {
        max_key_ = index_.back().last_key;
        auto first_block = ReadBlock(index_.front().offset, index_.front().size);
        size_t block_pos = 0;
        uint32_t key_len = DecodeVarint(first_block, &block_pos);
        if (key_len != 0xFFFFFFFF && block_pos + key_len <= first_block.size()) {
            std::string key_buf(first_block.data() + block_pos, key_len);
            DecodeKey(key_buf, &min_key_);
        }
    }

    return Status::OK();
}

bool SSTableReader::Get(const TsKey& key, std::string* value) {
    InternalEntry entry;
    if (!GetEntry(key, &entry) || entry.IsDeletion()) return false;
    *value = entry.value;
    return true;
}

bool SSTableReader::GetEntry(const TsKey& key, InternalEntry* entry) {
    bool data_block_read = false;
    return GetEntry(key, entry, &data_block_read);
}

bool SSTableReader::GetEntry(const TsKey& key, InternalEntry* entry,
                             bool* data_block_read) {
    if (data_block_read) *data_block_read = false;
    if (index_.empty()) return false;

    std::string key_buf = EncodeKey(key);
    if (!bloom_.MayContain(key_buf)) return false;

    TsKeyComparator cmp;
    auto it = std::lower_bound(index_.begin(), index_.end(), key,
                               [&cmp](const IndexEntry& index_entry, const TsKey& target) {
                                   return cmp(index_entry.last_key, target) < 0;
                               });
    if (it == index_.end()) return false;
    if (data_block_read) *data_block_read = true;
    return GetFromBlock(ReadBlock(it->offset, it->size), key, entry);
}

void SSTableReader::RangeScan(const TsKey& begin, const TsKey& end,
                              std::vector<Record>* out) {
    std::vector<Record> entries;
    RangeScanEntries(begin, end, &entries);
    out->clear();
    for (const auto& record : entries) {
        if (!record.deleted) {
            out->push_back(record);
        }
    }
}

void SSTableReader::RangeScanEntries(const TsKey& begin, const TsKey& end,
                                     std::vector<Record>* out) {
    out->clear();
    TsKeyComparator cmp;
    for (const auto& block : index_) {
        if (cmp(block.last_key, begin) < 0) continue;

        std::string block_data = ReadBlock(block.offset, block.size);
        size_t pos = 0;
        while (pos < block_data.size()) {
            uint32_t key_len = DecodeVarint(block_data, &pos);
            if (key_len == 0xFFFFFFFF || pos + key_len > block_data.size()) break;
            std::string key_buf(block_data.data() + pos, key_len);
            pos += key_len;

            TsKey key;
            if (!DecodeKey(key_buf, &key)) break;
            if (pos >= block_data.size()) break;

            EntryType type = static_cast<EntryType>(static_cast<uint8_t>(block_data[pos++]));
            if (type != EntryType::kValue && type != EntryType::kDeletion) break;

            uint32_t value_len = DecodeVarint(block_data, &pos);
            if (value_len == 0xFFFFFFFF || pos + value_len > block_data.size()) break;
            std::string value(block_data.data() + pos, value_len);
            pos += value_len;

            if (cmp(key, begin) < 0) continue;
            if (cmp(key, end) > 0) return;
            out->push_back(Record{key.measurement, key.timestamp, value,
                                  type == EntryType::kDeletion});
        }
    }
}

std::string SSTableReader::ReadBlock(uint64_t offset, uint32_t size) {
    if (!file_ || size < 4) return "";

    std::string cache_key;
    if (block_cache_) {
        std::ostringstream oss;
        oss << fname_ << "#" << offset;
        cache_key = oss.str();
        std::string cached;
        if (block_cache_->Get(cache_key, &cached)) {
            return cached;
        }
    }

    std::string raw;
    if (!file_->ReadAt(offset, size, &raw).ok()) return "";

    uint32_t stored_crc = DecodeFixed32(raw.data());
    std::string block = raw.substr(4);
    if (Crc32(block) != stored_crc) return "";
    if (block_cache_) {
        block_cache_->Put(cache_key, block);
    }
    return block;
}

bool SSTableReader::GetFromBlock(const std::string& block_data, const TsKey& key,
                                 InternalEntry* entry) {
    TsKeyComparator cmp;
    size_t pos = 0;
    while (pos < block_data.size()) {
        uint32_t key_len = DecodeVarint(block_data, &pos);
        if (key_len == 0xFFFFFFFF || pos + key_len > block_data.size()) break;
        std::string key_buf(block_data.data() + pos, key_len);
        pos += key_len;

        TsKey current;
        if (!DecodeKey(key_buf, &current)) break;
        if (pos >= block_data.size()) break;

        EntryType type = static_cast<EntryType>(static_cast<uint8_t>(block_data[pos++]));
        if (type != EntryType::kValue && type != EntryType::kDeletion) break;

        uint32_t value_len = DecodeVarint(block_data, &pos);
        if (value_len == 0xFFFFFFFF || pos + value_len > block_data.size()) break;
        std::string current_value(block_data.data() + pos, value_len);
        pos += value_len;

        int c = cmp(current, key);
        if (c == 0) {
            *entry = InternalEntry{current_value, type};
            return true;
        }
        if (c > 0) return false;
    }
    return false;
}

}  // namespace minitsdb
