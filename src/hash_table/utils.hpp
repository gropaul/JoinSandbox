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


inline uint64_t Trailing(uint64_t value_in) {
    if (!value_in) {
        return 64;
    }
    uint64_t value = value_in;

    constexpr uint64_t index64lsb[] = {63, 0,  58, 1,  59, 47, 53, 2,  60, 39, 48, 27, 54, 33, 42, 3,
                                        61, 51, 37, 40, 49, 18, 28, 20, 55, 30, 34, 11, 43, 14, 22, 4,
                                        62, 57, 46, 52, 38, 26, 32, 41, 50, 36, 17, 19, 29, 10, 13, 21,
                                        56, 45, 25, 31, 35, 16, 9,  12, 44, 24, 15, 8,  23, 7,  6,  5};
    constexpr uint64_t debruijn64lsb = 0x07EDD5E59A4E28C2ULL;
    auto result = index64lsb[((value & -value) * debruijn64lsb) >> 58];

    return result;
}

inline uint64_t GetFullValuesFromGroupSlow(const uint8_t* __restrict ht_1, const uint64_t start_offset, uint8_t max_offset) {

    uint64_t n = 0;

    #pragma clang loop unroll(disable)
    for (int i = 0; i < max_offset; i++) {
        n += ht_1[start_offset + i] != 0;
    }

    return n;
}

inline uint64_t ProbeGroup(const uint8_t salt, const uint8_t* __restrict ht_1, const uint64_t start_offset, uint8_t* __restrict buffer, const uint64_t GROUP_SIZE) {

    #pragma clang loop unroll(disable)
    for (int i = 0; i < GROUP_SIZE;i++) {
        buffer[i] = ht_1[start_offset + i] == salt | ht_1[start_offset + i] == 0;
    }

    const auto lower = reinterpret_cast<uint64_t*>(&buffer[0]);
    const auto upper = reinterpret_cast<uint64_t*>(&buffer[8]);

    if (lower){
        return Trailing(*lower) / 8;
    }
    if (upper){
        return Trailing(*upper) / 8;
    }

    return -1;
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


uint64_t Leading(const uint64_t value_in) {
    uint64_t reversed = Reverse(value_in);
    uint64_t result = Trailing(reversed);
    return result;
}

#endif //UTILS_HPP
