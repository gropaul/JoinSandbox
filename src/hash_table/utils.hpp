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



inline uint64_t GetFullValuesFromGroupSlow(const uint8_t* __restrict ht_1, const uint64_t start_offset, uint8_t max_offset) {

    uint64_t n = 0;

    for (int i = 0; i < max_offset; i++) {
        n += ht_1[start_offset + i] != 0;
    }

    return n;
}


uint64_t Reverse(uint64_t n) {
    n = (n >> 32) | (n << 32);
    n = ((n & 0xFFFF0000FFFF0000ULL) >> 16) | ((n & 0x0000FFFF0000FFFFULL) << 16);
    n = ((n & 0xFF00FF00FF00FF00ULL) >> 8)  | ((n & 0x00FF00FF00FF00FFULL) << 8);
    n = ((n & 0xF0F0F0F0F0F0F0F0ULL) >> 4)  | ((n & 0x0F0F0F0F0F0F0F0FULL) << 4);
    n = ((n & 0xCCCCCCCCCCCCCCCCULL) >> 2)  | ((n & 0x3333333333333333ULL) << 2);
    n = ((n & 0xAAAAAAAAAAAAAAAAULL) >> 1)  | ((n & 0x5555555555555555ULL) << 1);
    return n;
}


#endif //UTILS_HPP
