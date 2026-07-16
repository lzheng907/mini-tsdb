#ifndef MINITSDB_UTILS_H
#define MINITSDB_UTILS_H

#include <cstdint>
#include <string>

namespace minitsdb {

enum class EntryType : uint8_t {
    kValue = 1,
    kDeletion = 2,
};

struct InternalEntry {
    std::string value;
    EntryType type = EntryType::kValue;

    bool IsDeletion() const { return type == EntryType::kDeletion; }
};

struct TsKey {
    std::string measurement;
    int64_t timestamp = 0;
};

struct TsKeyComparator {
    int operator()(const TsKey& a, const TsKey& b) const {
        int c = a.measurement.compare(b.measurement);
        if (c != 0) return c;
        if (a.timestamp < b.timestamp) return -1;
        if (a.timestamp > b.timestamp) return 1;
        return 0;
    }
};

struct Record {
    std::string measurement;
    int64_t timestamp = 0;
    std::string value;
    bool deleted = false;
};

}  // namespace minitsdb

#endif  // MINITSDB_UTILS_H
