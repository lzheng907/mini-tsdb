// minitsdb/src/status.cpp
#include "status.h"

namespace minitsdb {

std::string Status::ToString() const {
    if (ok()) return "[OK]";
    if (msg_.empty()) {
        switch (code_) {
            case kNotFound: return "[NotFound]";
            case kCorruption: return "[Corruption]";
            case kIOError: return "[IOError]";
            case kInvalidArgument: return "[InvalidArgument]";
            default: return "[Unknown]";
        }
    }
    const char* tag = "";
    switch (code_) {
        case kNotFound: tag = "NotFound"; break;
        case kCorruption: tag = "Corruption"; break;
        case kIOError: tag = "IOError"; break;
        case kInvalidArgument: tag = "InvalidArgument"; break;
        default: tag = "Unknown"; break;
    }
    return std::string("[") + tag + ": " + msg_ + "]";
}

}  // namespace minitsdb
