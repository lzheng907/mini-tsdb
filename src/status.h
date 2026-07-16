// minitsdb/src/status.h
//
// 通用状态码，替代异常，用于跨模块错误传播。
// LevelDB/RocksDB 风格，所有 IO 操作返回 Status 而非抛异常。
#ifndef MINITSDB_STATUS_H
#define MINITSDB_STATUS_H

#include <string>

namespace minitsdb {

class Status {
public:
    Status() : code_(kOk) {}

    static Status OK() { return Status(kOk, ""); }
    static Status NotFound(const std::string& msg = "") {
        return Status(kNotFound, msg);
    }
    static Status Corruption(const std::string& msg = "") {
        return Status(kCorruption, msg);
    }
    static Status IOError(const std::string& msg = "") {
        return Status(kIOError, msg);
    }
    static Status InvalidArgument(const std::string& msg = "") {
        return Status(kInvalidArgument, msg);
    }

    bool ok() const { return code_ == kOk; }
    bool IsNotFound() const { return code_ == kNotFound; }
    bool IsCorruption() const { return code_ == kCorruption; }
    bool IsIOError() const { return code_ == kIOError; }
    bool IsInvalidArgument() const { return code_ == kInvalidArgument; }
    const std::string& message() const { return msg_; }

    // ToString 返回人类可读的 "[OK]" / "[IOError: xxx]" 格式
    std::string ToString() const;

private:
    enum Code { kOk, kNotFound, kCorruption, kIOError, kInvalidArgument };
    Code code_;
    std::string msg_;

    Status(Code c, const std::string& m) : code_(c), msg_(m) {}
};

}  // namespace minitsdb

#endif  // MINITSDB_STATUS_H
