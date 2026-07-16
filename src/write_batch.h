#ifndef MINITSDB_WRITE_BATCH_H
#define MINITSDB_WRITE_BATCH_H

#include <cstdint>
#include <string>
#include <vector>

namespace minitsdb {

class WriteBatch {
public:
    enum class OpType {
        kPut,
        kDelete,
    };

    struct Operation {
        OpType type = OpType::kPut;
        std::string measurement;
        int64_t timestamp = 0;
        std::string value;
    };

    void Put(const std::string& measurement, int64_t timestamp,
             const std::string& value) {
        ops_.push_back(Operation{OpType::kPut, measurement, timestamp, value});
    }

    void Delete(const std::string& measurement, int64_t timestamp) {
        ops_.push_back(Operation{OpType::kDelete, measurement, timestamp, ""});
    }

    size_t Count() const { return ops_.size(); }
    bool Empty() const { return ops_.empty(); }
    void Clear() { ops_.clear(); }

    const std::vector<Operation>& Operations() const { return ops_; }

private:
    std::vector<Operation> ops_;
};

}  // namespace minitsdb

#endif  // MINITSDB_WRITE_BATCH_H
