#include "wal.h"
#include "crc32.h"

#include <cstdio>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#else
#include <unistd.h>
#endif

namespace minitsdb {

namespace {

constexpr uint8_t kPhysicalFull = 1;
constexpr uint8_t kPhysicalFirst = 2;
constexpr uint8_t kPhysicalMiddle = 3;
constexpr uint8_t kPhysicalLast = 4;
constexpr size_t kPhysicalHeaderSize = 9;  // crc32(4) + len(4) + physical_type(1)

}  // namespace

WAL::WAL(const std::string& path, bool sync_on_write)
    : path_(path), sync_on_write_(sync_on_write) {
    file_ = fopen(path.c_str(), "ab");
    if (!file_) {
        file_ = fopen(path.c_str(), "wb");
        if (!file_) return;
        fclose(file_);
        file_ = fopen(path.c_str(), "ab");
    }

    if (file_) {
        fseek(file_, 0, SEEK_END);
        bytes_written_ = static_cast<int64_t>(ftell(file_));
    }
}

WAL::~WAL() { Close(); }

void WAL::Close() {
    if (!file_) return;
    fflush(file_);
    fclose(file_);
    file_ = nullptr;
}

Status WAL::AppendPut(const std::string& measurement, int64_t timestamp,
                      const std::string& value) {
    return AppendRecord(kTypePut, measurement, timestamp, value, sync_on_write_);
}

Status WAL::AppendDelete(const std::string& measurement, int64_t timestamp) {
    return AppendRecord(kTypeDelete, measurement, timestamp, "", sync_on_write_);
}

Status WAL::AppendPutNoSync(const std::string& measurement, int64_t timestamp,
                            const std::string& value) {
    return AppendRecord(kTypePut, measurement, timestamp, value, false);
}

Status WAL::AppendDeleteNoSync(const std::string& measurement, int64_t timestamp) {
    return AppendRecord(kTypeDelete, measurement, timestamp, "", false);
}

Status WAL::AppendRecord(uint8_t type, const std::string& measurement,
                         int64_t timestamp, const std::string& value,
                         bool sync) {
    if (!file_) return Status::IOError("WAL file not open: " + path_);

    std::string payload;
    payload.push_back(static_cast<char>(type));
    EncodeFixed32(&payload, static_cast<uint32_t>(measurement.size()));
    EncodeFixed64(&payload, static_cast<uint64_t>(timestamp));
    payload.append(measurement);
    payload.append(value);

    size_t offset = 0;
    const bool fragmented = payload.size() > kMaxFragmentPayload;
    while (offset < payload.size() || (payload.empty() && offset == 0)) {
        size_t remaining = payload.size() - offset;
        size_t fragment_len = remaining > kMaxFragmentPayload
                                  ? kMaxFragmentPayload
                                  : remaining;
        uint8_t physical_type = kPhysicalFull;
        if (fragmented) {
            if (offset == 0) {
                physical_type = kPhysicalFirst;
            } else if (offset + fragment_len == payload.size()) {
                physical_type = kPhysicalLast;
            } else {
                physical_type = kPhysicalMiddle;
            }
        }

        Status s = AppendPhysicalRecord(physical_type, payload.data() + offset,
                                        fragment_len);
        if (!s.ok()) return s;
        offset += fragment_len;
        if (payload.empty()) break;
    }

    if (sync) {
        return Sync();
    }
    return Status::OK();
}

Status WAL::Sync() {
    if (!file_) return Status::IOError("WAL file not open: " + path_);
    if (fflush(file_) != 0) return Status::IOError("WAL fflush failed");
#ifdef _WIN32
    int fd = _fileno(file_);
    if (fd >= 0 && _commit(fd) != 0) return Status::IOError("WAL commit failed");
#else
    if (fsync(fileno(file_)) != 0) return Status::IOError("WAL fsync failed");
#endif
    return Status::OK();
}

Status WAL::AppendPhysicalRecord(uint8_t physical_type, const char* data,
                                 size_t len) {
    if (len > kMaxFragmentPayload) {
        return Status::InvalidArgument("WAL fragment too large");
    }

    std::string fragment;
    fragment.push_back(static_cast<char>(physical_type));
    fragment.append(data, len);

    std::string record;
    uint32_t crc = Crc32(fragment);
    EncodeFixed32(&record, crc);
    EncodeFixed32(&record, static_cast<uint32_t>(fragment.size()));
    record.append(fragment);

    if (fwrite(record.data(), 1, record.size(), file_) != record.size()) {
        return Status::IOError("WAL write failed");
    }
    bytes_written_ += static_cast<int64_t>(record.size());
    return Status::OK();
}

Status WAL::Replay(const std::function<void(const std::string&, int64_t,
                                            const std::string&)>& on_record) {
    return ReplayEntries([&on_record](EntryType type, const std::string& measurement,
                                      int64_t timestamp, const std::string& value) {
        if (type == EntryType::kValue) {
            on_record(measurement, timestamp, value);
        }
    });
}

Status WAL::ReplayEntries(const ReplayHandler& on_record) {
    Close();

    FILE* read_file = fopen(path_.c_str(), "rb");
    if (!read_file) {
        file_ = fopen(path_.c_str(), "ab");
        bytes_written_ = 0;
        return Status::OK();
    }

    fseek(read_file, 0, SEEK_END);
    long file_size = ftell(read_file);
    fseek(read_file, 0, SEEK_SET);

    std::string data;
    data.resize(static_cast<size_t>(file_size));
    if (!data.empty()) {
        size_t n = fread(&data[0], 1, data.size(), read_file);
        data.resize(n);
    }
    fclose(read_file);

    size_t offset = 0;
    size_t valid_offset = 0;
    bool has_tail_corruption = false;
    std::string logical_payload;
    bool assembling_fragmented_record = false;

    auto emit_logical = [&](const std::string& payload) -> bool {
        EntryType entry_type;
        std::string measurement;
        int64_t ts = 0;
        std::string value;
        if (!DecodeLogicalRecord(payload, &entry_type, &measurement, &ts, &value)) {
            return false;
        }
        on_record(entry_type, measurement, ts, value);
        return true;
    };

    while (offset + kPhysicalHeaderSize <= data.size()) {
        uint32_t stored_crc = DecodeFixed32(&data[offset]);
        uint32_t fragment_len = DecodeFixed32(&data[offset + 4]);
        size_t record_end = offset + 8 + fragment_len;
        if (record_end > data.size()) {
            has_tail_corruption = true;
            break;
        }

        const char* fragment = data.data() + offset + 8;
        if (Crc32(fragment, static_cast<size_t>(fragment_len), 0) != stored_crc) {
            has_tail_corruption = true;
            break;
        }
        if (fragment_len < 1) {
            has_tail_corruption = true;
            break;
        }

        uint8_t physical_type = static_cast<uint8_t>(fragment[0]);
        std::string fragment_payload(fragment + 1, fragment + fragment_len);
        EntryType legacy_type = EntryType::kValue;
        std::string legacy_measurement;
        int64_t legacy_ts = 0;
        std::string legacy_value;
        const bool legacy_record =
            !assembling_fragmented_record &&
            DecodeLogicalRecord(std::string(fragment, fragment + fragment_len),
                                &legacy_type, &legacy_measurement, &legacy_ts,
                                &legacy_value);
        switch (physical_type) {
            case kPhysicalFull:
                if (assembling_fragmented_record) {
                    has_tail_corruption = true;
                    break;
                }
                if (!emit_logical(fragment_payload)) {
                    if (legacy_record && physical_type == kTypePut) {
                        on_record(legacy_type, legacy_measurement, legacy_ts,
                                  legacy_value);
                    } else {
                        has_tail_corruption = true;
                        break;
                    }
                }
                valid_offset = record_end;
                break;
            case kPhysicalFirst:
                if (assembling_fragmented_record) {
                    has_tail_corruption = true;
                    break;
                }
                logical_payload = fragment_payload;
                assembling_fragmented_record = true;
                break;
            case kPhysicalMiddle:
                if (!assembling_fragmented_record) {
                    has_tail_corruption = true;
                    break;
                }
                logical_payload.append(fragment_payload);
                break;
            case kPhysicalLast:
                if (!assembling_fragmented_record) {
                    has_tail_corruption = true;
                    break;
                }
                logical_payload.append(fragment_payload);
                if (!emit_logical(logical_payload)) {
                    has_tail_corruption = true;
                    break;
                }
                logical_payload.clear();
                assembling_fragmented_record = false;
                valid_offset = record_end;
                break;
            default:
                has_tail_corruption = true;
                break;
        }
        if (has_tail_corruption) break;
        offset = record_end;
    }
    if (assembling_fragmented_record) {
        has_tail_corruption = true;
    }

    file_ = fopen(path_.c_str(), "ab");
    if (!file_) return Status::IOError("Cannot reopen WAL: " + path_);

    if (has_tail_corruption) {
#ifdef _WIN32
        int fd = _fileno(file_);
        if (fd >= 0 && _chsize_s(fd, static_cast<long long>(valid_offset)) != 0) {
            return Status::IOError("WAL truncate failed: " + path_);
        }
#else
        int fd = fileno(file_);
        if (fd >= 0 && ftruncate(fd, static_cast<off_t>(valid_offset)) != 0) {
            return Status::IOError("WAL truncate failed: " + path_);
        }
#endif
    }

    fseek(file_, 0, SEEK_END);
    bytes_written_ = static_cast<int64_t>(ftell(file_));
    return Status::OK();
}

int64_t WAL::FileSize() const { return bytes_written_; }

bool WAL::DecodeLogicalRecord(const std::string& payload, EntryType* type,
                              std::string* measurement, int64_t* timestamp,
                              std::string* value) {
    size_t p = 0;
    if (payload.size() < 1 + 4 + 8) return false;

    uint8_t logical_type = static_cast<uint8_t>(payload[p++]);
    uint32_t m_len = DecodeFixed32(payload.data() + p);
    p += 4;
    *timestamp = static_cast<int64_t>(DecodeFixed64(payload.data() + p));
    p += 8;
    if (p + m_len > payload.size()) return false;

    measurement->assign(payload.data() + p, m_len);
    p += m_len;
    value->assign(payload.data() + p, payload.size() - p);

    if (logical_type == kTypePut) {
        *type = EntryType::kValue;
        return true;
    }
    if (logical_type == kTypeDelete) {
        *type = EntryType::kDeletion;
        value->clear();
        return true;
    }
    return false;
}

Status WAL::Rotate() {
    Close();
    std::string old_path = path_ + ".old";
    std::remove(old_path.c_str());
    std::rename(path_.c_str(), old_path.c_str());

    file_ = fopen(path_.c_str(), "wb");
    if (!file_) return Status::IOError("Cannot rotate WAL: " + path_);
    fclose(file_);
    file_ = fopen(path_.c_str(), "ab");
    if (!file_) return Status::IOError("Cannot reopen WAL after rotate: " + path_);
    bytes_written_ = 0;
    return Status::OK();
}

void WAL::EncodeFixed32(std::string* s, uint32_t v) {
    char buf[4];
    buf[0] = static_cast<char>(v & 0xFF);
    buf[1] = static_cast<char>((v >> 8) & 0xFF);
    buf[2] = static_cast<char>((v >> 16) & 0xFF);
    buf[3] = static_cast<char>((v >> 24) & 0xFF);
    s->append(buf, 4);
}

void WAL::EncodeFixed64(std::string* s, uint64_t v) {
    char buf[8];
    for (int i = 0; i < 8; ++i) {
        buf[i] = static_cast<char>((v >> (i * 8)) & 0xFF);
    }
    s->append(buf, 8);
}

uint32_t WAL::DecodeFixed32(const char* p) {
    return static_cast<uint32_t>(static_cast<unsigned char>(p[0])) |
           (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 8) |
           (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 16) |
           (static_cast<uint32_t>(static_cast<unsigned char>(p[3])) << 24);
}

uint64_t WAL::DecodeFixed64(const char* p) {
    uint64_t r = 0;
    for (int i = 0; i < 8; ++i) {
        r |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (i * 8);
    }
    return r;
}

}  // namespace minitsdb
