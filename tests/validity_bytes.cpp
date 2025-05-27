#include "duckdb.hpp"
#include <iostream>
#include <chrono>  // For benchmarking
#include "duckdb/common/sort/comparators.hpp"

// === Debug Flag ===
#define DEBUG_MODE false

const uint64_t ROW_COUNT = 1000000;
const uint64_t ROW_KEYS = 8;
const uint64_t VECTOR_SIZE = 2048;
const uint64_t N_VECTORS = 1000;
const uint64_t NUM_RUNS = 5;
const uint64_t SEED = 42;

namespace duckdb {
    struct TestRows {
        uint64_t n_keys;
        uint8_t *allocation;
        uint64_t row_width;
        uint64_t row_count;
        vector<column_t> columns;

        TestRows(uint64_t n_keys, uint64_t row_count)
            : n_keys(n_keys), row_count(row_count), allocation(nullptr) {
            row_width = sizeof(uint8_t) + n_keys * sizeof(uint64_t);
            for (uint64_t i = 0; i < n_keys; ++i) {
                columns.push_back(i);
            }
        }

        uint64_t GetAllocationSize() const {
            return row_width * row_count;
        }

        void Allocate() {
            allocation = new uint8_t[row_width * row_count];
        }

        void PopulateRandom() const {
            if (!allocation) return;
            for (uint64_t i = 0; i < row_count; ++i) {
                uint8_t *row = allocation + i * row_width;
                row[0] = static_cast<uint8_t>(std::rand() % 256);
                for (uint64_t j = 0; j < n_keys; ++j) {
                    auto *key = reinterpret_cast<uint64_t *>(row + sizeof(uint8_t) + j * sizeof(uint64_t));
                    *key = std::rand();
                }
            }
        }

        void GetRandomRowLocations(uint8_t **ptrs, uint64_t row_count) const {
            if (!allocation) throw std::runtime_error("Allocation not done. Call Allocate() first.");
            for (uint64_t i = 0; i < row_count; ++i) {
                uint64_t random_row_index = std::rand() % this->row_count;
                uint8_t *row_ptr = allocation + random_row_index * row_width;
                ptrs[i] = row_ptr;
                if (DEBUG_MODE) {
                    std::cout << "Row " << i << ": " << static_cast<void *>(row_ptr)
                              << " Byte: " << std::bitset<8>(row_ptr[0]) << "\n";
                }
            }
        }

        void Free() {
            if (allocation) {
                delete[] allocation;
                allocation = nullptr;
            }
        }

        ~TestRows() {
            Free();
        }
    };

    TestRows InitializeRowLayout() {
        TestRows layout(ROW_KEYS, ROW_COUNT);
        layout.Allocate();
        layout.PopulateRandom();
        return layout;
    }

    void GatherValidityMask(uint8_t **ptrs, const uint64_t row_count, uint8_t *buffer,
                            vector<ValidityMask> &masks, const vector<column_t> &columns) {
        for (auto col_idx: columns) {
            ValidityMask &mask = masks[col_idx];
            idx_t entry_idx, idx_in_entry;
            ValidityBytes::GetEntryIndex(col_idx, entry_idx, idx_in_entry);
            for (uint64_t target_idx = 0; target_idx < row_count; ++target_idx) {
                uint8_t *source_row = ptrs[target_idx];
                ValidityBytes row_mask(source_row, 1);
                if (!ValidityBytes::RowIsValid(row_mask.GetValidityEntryUnsafe(entry_idx), idx_in_entry)) {
                    mask.SetInvalid(target_idx);
                }
            }
        }
    }

    void GatherValidityMaskOptimized(uint8_t **ptrs, const uint64_t row_count, uint8_t *buffer,
                                     vector<ValidityMask> &masks, const vector<column_t> &columns) {
        constexpr uint64_t BLOCK_SIZE = 8;
        constexpr uint64_t BITS_PER_BYTE = 8;
        constexpr uint64_t BLOCK_WINDOW_SIZE = BLOCK_SIZE * BITS_PER_BYTE;

        uint8_t collector[BLOCK_SIZE];

        for (const auto col_idx: columns) {
            ValidityMask &mask = masks[col_idx];
            auto __restrict *validity_mask_ptr = reinterpret_cast<uint8_t *>(mask.GetData());

            uint64_t flat_idx = 0;
            for (uint64_t window_start_idx = 0; window_start_idx < row_count; window_start_idx += BLOCK_WINDOW_SIZE) {
                for (uint64_t idx_in_block = 0; idx_in_block < BLOCK_SIZE; ++idx_in_block) {
                    for (uint64_t row_idx = idx_in_block; row_idx < BLOCK_WINDOW_SIZE; row_idx += BLOCK_SIZE) {
                        buffer[flat_idx] = *ptrs[window_start_idx + row_idx];
                        flat_idx += 1;
                    }
                }
            }

            const uint8_t column_bitmask = 0x1 << col_idx;
            uint64_t validity_idx = 0;

            for (uint64_t window_start_idx = 0; window_start_idx < row_count; window_start_idx += BLOCK_WINDOW_SIZE) {
                uint64_t block_bit_target_position = 0;
                std::fill(collector, collector + BLOCK_SIZE, 0);

                for (uint64_t outer_loop_idx = 0; outer_loop_idx < BLOCK_WINDOW_SIZE; outer_loop_idx += BITS_PER_BYTE) {
                    for (uint64_t idx_in_block = 0; idx_in_block < BLOCK_SIZE; ++idx_in_block) {
                        const uint64_t buffer_idx = window_start_idx + outer_loop_idx + idx_in_block;
                        const uint8_t buffer_byte = buffer[buffer_idx];
                        const uint8_t column_bit = buffer_byte & column_bitmask;
                        const uint8_t column_bit_shifted = column_bit >> col_idx;
                        const uint8_t combined_bit = column_bit_shifted << block_bit_target_position;
                        collector[idx_in_block] |= combined_bit;

                        if (DEBUG_MODE) {
                            std::cout << "buffer_byte: " << std::bitset<8>(buffer_byte)
                                      << ", combined_bit: " << std::bitset<8>(combined_bit)
                                      << ", collector[" << idx_in_block << "]: "
                                      << std::bitset<8>(collector[idx_in_block]) << "\n";
                        }
                    }
                    block_bit_target_position += 1;
                }

                for (uint64_t idx_in_block = 0; idx_in_block < BLOCK_SIZE; ++idx_in_block) {
                    validity_mask_ptr[validity_idx + idx_in_block] = collector[idx_in_block];
                }
                validity_idx += BLOCK_SIZE;
            }
        }
    }

    struct StrategyResult {
        double avg_time;
        vector<uint64_t> non_null_bytes_per_column;

        void Print(const std::string &strategy_name) {
            std::cout << strategy_name << " - Average Time: " << avg_time << " seconds\n";
            for (size_t i = 0; i < non_null_bytes_per_column.size(); ++i) {
                std::cout << "\tColumn " << i << ": " << non_null_bytes_per_column[i] << " valid bytes\n";
            }
        }
    };

    StrategyResult TestStrategy(
        void (*gather_function)(uint8_t **, const uint64_t, uint8_t *, vector<ValidityMask> &, const vector<column_t> &),
        const std::string &strategy_name) {
        std::srand(SEED);
        double total_time = 0.0;
        duckdb::vector<uint64_t> total_valid_values_avg(ROW_KEYS, 0);

        for (uint64_t run = 0; run < NUM_RUNS; ++run) {
            duckdb::TestRows layout = duckdb::InitializeRowLayout();
            auto *buffer = new uint8_t[VECTOR_SIZE];
            duckdb::vector<uint64_t> total_valid_values(layout.columns.size(), 0);
            duckdb::vector<duckdb::ValidityMask> masks(layout.columns.size());
            for (size_t i = 0; i < layout.columns.size(); ++i) {
                masks[i].Initialize(VECTOR_SIZE);
            }

            auto start = std::chrono::high_resolution_clock::now();

            for (uint64_t i = 0; i < N_VECTORS; ++i) {
                uint8_t *row_ptrs[VECTOR_SIZE];
                layout.GetRandomRowLocations(row_ptrs, VECTOR_SIZE);
                gather_function(row_ptrs, VECTOR_SIZE, buffer, masks, layout.columns);

                for (size_t col_idx = 0; col_idx < layout.columns.size(); ++col_idx) {
                    auto &mask = masks[col_idx];
                    uint64_t valid_count = mask.CountValid(VECTOR_SIZE);
                    total_valid_values[col_idx] += valid_count;

                    if (DEBUG_MODE) {
                        const auto validity_mask_ptr = reinterpret_cast<uint8_t *>(mask.GetData());
                        for (uint64_t d_idx = 0; d_idx < VECTOR_SIZE / 8; ++d_idx) {
                            std::cout << std::bitset<8>(validity_mask_ptr[d_idx]) << " ";
                        }
                        std::cout << '\n';
                    }

                    mask.Reset(VECTOR_SIZE);
                    mask.Initialize(VECTOR_SIZE);
                }
            }

            auto end = std::chrono::high_resolution_clock::now();
            total_time += std::chrono::duration<double>(end - start).count();
            for (size_t i = 0; i < total_valid_values_avg.size(); ++i) {
                total_valid_values_avg[i] += total_valid_values[i];
            }

            layout.Free();
            delete[] buffer;
        }

        for (auto &val : total_valid_values_avg) {
            val /= NUM_RUNS;
        }

        StrategyResult result;
        result.avg_time = total_time / NUM_RUNS;
        result.non_null_bytes_per_column = total_valid_values_avg;
        result.Print(strategy_name);
        return result;
    }
}

int main() {
    duckdb::TestStrategy(duckdb::GatherValidityMask, "Strategy 1 (Original)");
    duckdb::TestStrategy(duckdb::GatherValidityMaskOptimized, "Strategy 2 (Optimized)");
    return 0;
}
