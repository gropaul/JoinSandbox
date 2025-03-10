
#include <stdint.h>
#include <iostream>
#include <chrono>
#include <random>
#include <vector>


void ASSERT(bool condition) {
    if (!condition) {
        throw "Assertion failed";
    }
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

inline static uint64_t LeadingDefault(const uint64_t value_in) {
    if (!value_in) {
        return 64;
    }

    uint64_t value = value_in;

    constexpr uint64_t index64msb[] = {0,  47, 1,  56, 48, 27, 2,  60, 57, 49, 41, 37, 28, 16, 3,  61,
                                       54, 58, 35, 52, 50, 42, 21, 44, 38, 32, 29, 23, 17, 11, 4,  62,
                                       46, 55, 26, 59, 40, 36, 15, 53, 34, 51, 20, 43, 31, 22, 10, 45,
                                       25, 39, 14, 33, 19, 30, 9,  24, 13, 18, 8,  12, 7,  6,  5,  63};

    constexpr uint64_t debruijn64msb = 0X03F79D71B4CB0A89;

    value |= value >> 1;
    value |= value >> 2;
    value |= value >> 4;
    value |= value >> 8;
    value |= value >> 16;
    value |= value >> 32;
    auto result = 63 - index64msb[(value * debruijn64msb) >> 58];
    ASSERT(result == static_cast<uint64_t>(__builtin_clzl(value_in)));
    return result;
}

inline static uint64_t Trailing(uint64_t value_in) {
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
    ASSERT(result == static_cast<uint64_t>(__builtin_ctzl(value_in)));
    return result;
}

inline static uint64_t LeadingNew(const uint64_t value_in) {
    uint64_t reversed = Reverse(value_in);
    uint64_t result = Trailing(reversed);
    ASSERT(result == static_cast<uint64_t>(__builtin_clzl(value_in)));
    return result;
}



// Benchmarking function
void benchmark(uint64_t (*func)(uint64_t), const uint64_t repeats, const std::vector<uint64_t>& values, const std::string& name) {
    using namespace std::chrono;
    uint64_t sum = 0; // Prevent compiler optimizations
    // access all elements in the vector and sum them up to heat up the cache

    for (const auto& value : values) {
        sum += func(value);
    }

    auto start = high_resolution_clock::now();

    for (uint64_t i = 0; i < repeats; i++) {
        for (const auto& value : values) {
            sum += func(value);
        }
    }

    auto end = high_resolution_clock::now();

    // Measure execution time in nanoseconds
    double duration_ns = static_cast<double>(duration_cast<nanoseconds>(end - start).count());

    // Processor frequency in Hz (e.g., 3.5 GHz = 3.5 * 10^9 Hz)
    constexpr double processor_frequency_hz = 3.5e9;

    // Convert duration from nanoseconds to seconds
    double duration_s = duration_ns * 1e-9;

    // Total number of instructions (assuming one instruction per function call)
    auto n_instructions = static_cast<double>(values.size()) * repeats;

    // Calculate IPC (Instructions Per Cycle)
    double cycles_taken = processor_frequency_hz * duration_s;
    double ipc = round(n_instructions / cycles_taken * 1000) / 1000;

    std::cout << name << " took " << duration_ns << " ns,\t" << ipc << " IPC (instructions per cycle). Check Sum: " << sum << '\n';
}
int main() {
    constexpr size_t TEST_SIZE = 1000000;
    constexpr size_t N_REPEATS = 20000;

    // Random number generation
    std::random_device rd;
    std::mt19937_64 rng(rd());
    std::uniform_int_distribution<uint64_t> dist(1, UINT64_MAX);

    std::vector<uint64_t> test_values(TEST_SIZE);
    for (auto& v : test_values) {
        v = dist(rng);
    }

    std::cout << "Benchmarking LeadingDefault vs LeadingNew on " << TEST_SIZE << " random 64-bit values.\n";
    benchmark(LeadingNew, N_REPEATS, test_values, "LeadingNew    ");
    benchmark(LeadingDefault, N_REPEATS, test_values, "LeadingDefault");

    return 0;
}


