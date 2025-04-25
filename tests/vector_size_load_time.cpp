#include <iostream>
#include <chrono>
#include <vector>
#include <cstring>

std::string NSToString(uint64_t ns) {
    if (ns < 1000) return std::to_string(ns) + " ns";
    else if (ns < 1000000) return std::to_string(ns / 1000) + " us";
    else return std::to_string(ns / 1000000) + " ms";
}

struct BenchmarkResult {
    uint64_t sum = 0;
    uint64_t init_time = 0;
    uint64_t vector_size = 0;
    uint64_t lookup_duration_us = 0;
    uint64_t sum_duration_us = 0;
    uint64_t free_duration_us = 0;

    void Print(size_t run_index = -1) const {
        if (run_index != static_cast<size_t>(-1)) {
            std::cout << "--- Run #" << run_index + 1 << " ---\n";
        } else {
            std::cout << "--- Average ---\n";
        }
        std::cout << "  Vector size: " << vector_size << "\n";
        std::cout << "  Sum: " << sum << "\n";
        std::cout << "  Init: " << NSToString(init_time) << "\n";
        std::cout << "  Lookup duration: " << NSToString(lookup_duration_us) << "\n";
        std::cout << "  Sum duration: " << NSToString(sum_duration_us) << "\n";
        std::cout << "  Free duration: " << NSToString(free_duration_us) << "\n";
        uint64_t total = init_time + lookup_duration_us + sum_duration_us + free_duration_us;
        std::cout << "  Total time: " << NSToString(total) << "\n\n";
    }

    void PrintCSV(bool header = false) const {
        if (header) {
            std::cout << "vector_size,sum,init_time,lookup_duration_us,sum_duration_us,free_duration_us\n";
        }
        std::cout << vector_size << "," << sum << "," << init_time << "," << lookup_duration_us << "," << sum_duration_us
                  << "," << free_duration_us << "\n";
    }

    BenchmarkResult &operator+=(const BenchmarkResult &other) {
        this->sum += other.sum;
        this->init_time += other.init_time;
        this->lookup_duration_us += other.lookup_duration_us;
        this->sum_duration_us += other.sum_duration_us;
        this->free_duration_us += other.free_duration_us;
        this->vector_size += other.vector_size;
        return *this;
    }

    BenchmarkResult operator/(uint64_t n) const {
        BenchmarkResult avg = *this;
        avg.sum /= n;
        avg.init_time /= n;
        avg.lookup_duration_us /= n;
        avg.sum_duration_us /= n;
        avg.free_duration_us /= n;
        avg.vector_size /= n;
        return avg;
    }
};

uint64_t GetNextPowerOfTwo(uint64_t value) {
    if (value == 0) return 1;
    if ((value & (value - 1)) == 0) return value;
    uint64_t power = 1;
    while (power < value) power <<= 1;
    return power;
}

uint64_t GetRandomValue(uint64_t value_range) {
    return rand() % value_range;
}

uint64_t GetHashTable(const uint64_t num_elements, const uint64_t value_range, uint64_t *&hash_table) {
    const uint64_t capacity = GetNextPowerOfTwo(num_elements);
    void *hash_table_ptr = malloc(capacity * sizeof(uint64_t));
    memset(hash_table_ptr, 0, capacity * sizeof(uint64_t));
    hash_table = static_cast<uint64_t *>(hash_table_ptr);
    uint64_t mask = capacity - 1;
    for (uint64_t i = 0; i < num_elements; i++) {
        uint64_t value = GetRandomValue(value_range);
        uint64_t offset = value & mask;
        hash_table[offset] = value;
    }
    return mask;
}

template<uint64_t vector_size>
uint64_t GetRandomVector(uint64_t *&vector, uint64_t value_range) {
    void *ptr = malloc(vector_size * sizeof(uint64_t));
    vector = static_cast<uint64_t *>(ptr);
    for (uint64_t i = 0; i < vector_size; i++) {
        vector[i] = GetRandomValue(value_range);
    }
    return 0;
}

template<uint64_t vector_size>
uint64_t SetRandomValues(uint64_t *vector, uint64_t value_range) {
    for (uint64_t i = 0; i < vector_size; i++) {
        vector[i] = GetRandomValue(value_range);
    }
    return 0;
}

template<uint64_t vector_size>
BenchmarkResult TestHashTable(const uint64_t num_elements_probe,const uint64_t num_elements_build,  const uint64_t value_range) {
    auto init_start = std::chrono::high_resolution_clock::now();

    if (num_elements_probe % vector_size != 0) {
        throw std::runtime_error("Number of elements is not divisible by vector size");
    }

    auto n_vectors = num_elements_probe / vector_size;

    uint64_t *hash_table;
    uint64_t mask = GetHashTable(num_elements_build, value_range, hash_table);

    uint64_t *gather_vector;
    uint64_t *probe_vector;
    uint64_t *sel_vector;
    GetRandomVector<vector_size>(gather_vector, 0);
    GetRandomVector<vector_size>(probe_vector, value_range);
    GetRandomVector<vector_size>(sel_vector, 0);


    auto init_end = std::chrono::high_resolution_clock::now();
    uint64_t init_time = std::chrono::duration_cast<std::chrono::nanoseconds>(init_end - init_start).count();

    uint64_t lookup_duration_us = 0;
    uint64_t sum_duration_us = 0;
    uint64_t sum = 0;

    for (uint64_t i = 0; i < n_vectors; i++) {

        SetRandomValues<vector_size>(probe_vector, value_range);

        auto lookup_start = std::chrono::high_resolution_clock::now();

        uint64_t found_count = 0;
        for (uint64_t idx = 0; idx < vector_size; idx++) {
            uint64_t ht_offset = probe_vector[idx] & mask;

            while (true) {
                uint64_t ht_value = hash_table[ht_offset];
                if (ht_value == 0) {
                    break;
                }

                if (ht_value == probe_vector[idx]) {
                    sel_vector[found_count] = idx;
                    found_count++;
                    break;
                }
                // linearly probing
                ht_offset = (ht_offset + 1) & mask;
            }

        }
        auto lookup_end = std::chrono::high_resolution_clock::now();
        lookup_duration_us += std::chrono::duration_cast<std::chrono::nanoseconds>(lookup_end - lookup_start).count();

        auto sum_start = std::chrono::high_resolution_clock::now();
        for (uint64_t idx = 0; idx < found_count; idx++) {
            uint64_t sel_idx = sel_vector[idx];
            uint64_t value = probe_vector[sel_idx];
            sum += value;
        }
        auto sum_end = std::chrono::high_resolution_clock::now();
        sum_duration_us += std::chrono::duration_cast<std::chrono::nanoseconds>(sum_end - sum_start).count();
    }

    auto free_start = std::chrono::high_resolution_clock::now();
    free(hash_table);
    free(probe_vector);
    free(gather_vector);
    free(sel_vector);
    auto free_end = std::chrono::high_resolution_clock::now();

    uint64_t free_duration_us = std::chrono::duration_cast<std::chrono::nanoseconds>(free_end - free_start).count();

    BenchmarkResult result;
    result.sum = sum;
    result.vector_size = vector_size;
    result.init_time = init_time;
    result.lookup_duration_us = lookup_duration_us;
    result.sum_duration_us = sum_duration_us;
    result.free_duration_us = free_duration_us;
    return result;

}

template<uint64_t vector_size>
void BenchmarkAverage(uint64_t num_elements_probe, uint64_t num_elements_build, uint64_t value_range, size_t repeat, uint64_t first_vector_size = 0) {
    BenchmarkResult total;
    for (size_t i = 0; i < repeat; i++) {
        BenchmarkResult res = TestHashTable<vector_size>(num_elements_probe, num_elements_build, value_range);
        total += res;
    }
    BenchmarkResult avg = total / repeat;

    const bool header = vector_size == first_vector_size;
    avg.PrintCSV(header);
}

int main() {
    const uint64_t num_elements_build = 65536 * 256; // (32.77 million)
    const uint64_t num_elements_probe = num_elements_build * 16;
    const uint64_t value_range = num_elements_build;
    const size_t repeat = 3;

    BenchmarkAverage<8>(num_elements_probe, num_elements_build, value_range, repeat, 8);
    BenchmarkAverage<16>(num_elements_probe, num_elements_build, value_range, repeat);
    BenchmarkAverage<32>(num_elements_probe, num_elements_build, value_range, repeat);
    BenchmarkAverage<64>(num_elements_probe, num_elements_build, value_range, repeat);
    BenchmarkAverage<128>(num_elements_probe, num_elements_build, value_range, repeat);
    BenchmarkAverage<256>(num_elements_probe, num_elements_build, value_range, repeat);
    BenchmarkAverage<512>(num_elements_probe, num_elements_build, value_range, repeat);
    BenchmarkAverage<1024>(num_elements_probe, num_elements_build, value_range, repeat);
    BenchmarkAverage<2048>(num_elements_probe, num_elements_build, value_range, repeat);
    BenchmarkAverage<4096>(num_elements_probe, num_elements_build, value_range, repeat);
    BenchmarkAverage<8192>(num_elements_probe, num_elements_build, value_range, repeat);
    BenchmarkAverage<16384>(num_elements_probe, num_elements_build, value_range, repeat);
    BenchmarkAverage<32768>(num_elements_probe, num_elements_build, value_range, repeat);
    BenchmarkAverage<65536>(num_elements_probe, num_elements_build, value_range, repeat);

}
