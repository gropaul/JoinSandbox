//
// Created by Paul on 09/02/2025.
//

#ifndef UTILS_HPP
#define UTILS_HPP


inline std::string BytesToString(uint64_t bytes) {
    if (bytes < 1024) {
        return std::to_string(bytes) + "B";
    }
    bytes /= 1024;
    if (bytes < 1024) {
        return std::to_string(bytes) + "KB";
    }
    bytes /= 1024;
    if (bytes < 1024) {
        return std::to_string(bytes) + "MB";
    }
    bytes /= 1024;
    return std::to_string(bytes) + "GB";
}

#endif //UTILS_HPP
