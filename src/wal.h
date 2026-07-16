#ifndef MINITSDB_WAL_H
#define MINITSDB_WAL_H

#include "status.h"
#include "utils.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>

namespace minitsdb {

class WAL {
public:
    static constexpr uint8_t kTypePut = 1;
    static constexpr uint8_t kTypeDelete = 2;
    static constexpr uint32_t kMaxFragmentPayload = 32 * 1024;

    using ReplayHandler = std::function<void(EntryType type,
                                             const std::string& measurement,
                                             int64_t timestamp,
                                             const std::string& value)>;

    explicit WAL(const std::string& path, bool sync_on_write = true);
    ~WAL();

    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;

    Status AppendPut(const std::string& measurement, int64_t timestamp,
                     const std::string& value);
    Status AppendDelete(const std::string& measurement, int64_t timestamp);
    Status AppendPutNoSync(const std::string& measurement, int64_t timestamp,
                           const std::string& value);
    Status AppendDeleteNoSync(const std::string& measurement, int64_t timestamp);
    Status Sync();
    Status Replay(const std::function<void(const std::string& measurement,
                                           int64_t timestamp,
                                           const std::string& value)>& on_record);
    Status ReplayEntries(const ReplayHandler& on_record);
    Status Rotate();

    int64_t FileSize() const;
    void Close();

private:
    Status AppendRecord(uint8_t type, const std::string& measurement,
                        int64_t timestamp, const std::string& value,
                        bool sync);
    Status AppendPhysicalRecord(uint8_t physical_type, const char* data, size_t len);

    static bool DecodeLogicalRecord(const std::string& payload, EntryType* type,
                                    std::string* measurement, int64_t* timestamp,
                                    std::string* value);

    static void EncodeFixed32(std::string* s, uint32_t v);
    static void EncodeFixed64(std::string* s, uint64_t v);
    static uint32_t DecodeFixed32(const char* p);
    static uint64_t DecodeFixed64(const char* p);

    std::string path_;
    FILE* file_ = nullptr;
    int64_t bytes_written_ = 0;
    bool sync_on_write_ = true;
};

}  // namespace minitsdb

#endif  // MINITSDB_WAL_H
