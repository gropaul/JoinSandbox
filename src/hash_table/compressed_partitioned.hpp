//
// Created by Paul on 08/02/2025.
//

#ifndef HASH_TABLE_COMPRESSED_PARTITIONED
#define HASH_TABLE_COMPRESSED_PARTITIONED
#include "utils.hpp"
#include "base.hpp"
#include "partitioned.hpp"

#include <iostream>
#include <cmath>

#include "base.hpp"
#include "materialization/row_layout.hpp"


// typedef uint32_t key_t;
typedef uint64_t ht_slot_t;


namespace duckdb {
    struct Functions {
    public:
        static inline constexpr uint64_t GetSlotMask(const uint64_t bytes_per_value) {
            return (1ULL << (bytes_per_value * 8)) - 1;
        }
    };

    template<idx_t bytes_per_value>
    struct CompressedConstants {
    public:
        //! Has 1s where the slot is, 0s elsewhere
        static constexpr uint64_t SLOT_MASK = Functions::GetSlotMask(bytes_per_value);

    public:
        static inline constexpr bool IsPowerOfTwo(const uint64_t value) {
            return (value & (value - 1)) == 0;
        }

        static inline constexpr uint64_t GetPowerOfTwo(const uint64_t value) {
            // we already know that the value is a power of two, we can't use a loop
            return static_cast<uint64_t>(std::log2(value));
        }

        static inline uint64_t GetCompressedOffset(const uint64_t offset) {
            if (IsPowerOfTwo(bytes_per_value)) {
                return offset << GetPowerOfTwo(bytes_per_value);
            } else {
                return offset * bytes_per_value;
            }
        }

        static inline constexpr uint64_t ReadRawValueAtPointer(data_ptr_t ptr) {
            return *reinterpret_cast<uint64_t *>(ptr);
        }

        static inline void WriteRawValueAtPointer(data_ptr_t ptr, const uint64_t value) {
            auto *ptr_uint64 = reinterpret_cast<uint64_t *>(ptr);
            *ptr_uint64 = value;
        }

        static inline uint64_t ReadValueAtOffset(data_ptr_t start_pointer, const uint64_t offset) {
            const uint64_t byte_offset = GetCompressedOffset(offset);
            data_ptr_t ptr = start_pointer + byte_offset;
            const auto raw_value = ReadRawValueAtPointer(ptr);
            const uint64_t value = (raw_value & SLOT_MASK);
            return value;
        }

        static inline void WriteValueAtOffset(data_ptr_t start_pointer, const uint64_t offset, const uint64_t value) {
            uint64_t byte_offset = GetCompressedOffset(offset);
            data_ptr_t ptr = start_pointer + byte_offset;
            const auto current_value = ReadRawValueAtPointer(ptr);

            // zero out the slot, keep the other values
            const uint64_t mask = ~SLOT_MASK;
            const auto new_value = (current_value & mask) | value;
            WriteRawValueAtPointer(ptr, new_value);
        }

        static inline bool WriteIfZero(data_ptr_t start_pointer, const uint64_t offset, const uint64_t value) {
            uint64_t byte_offset = GetCompressedOffset(offset);
            data_ptr_t ptr = start_pointer + byte_offset;
            const auto current_raw_value = ReadRawValueAtPointer(ptr);
            const uint64_t current_value = (current_raw_value & SLOT_MASK);
            if (current_value != 0) {
                return false;
            }
            const uint64_t mask = ~SLOT_MASK;
            const auto new_value = (current_raw_value & mask) | value;
            WriteRawValueAtPointer(ptr, new_value);
            return true;
        }

        static inline bool ProbeAndWrite(data_ptr_t start_pointer, uint64_t &offset, uint64_t &collisions,
                                         const uint64_t value) {
            const uint64_t byte_offset = GetCompressedOffset(offset);
            data_ptr_t ptr = start_pointer + byte_offset;

            const auto raw_value = ReadRawValueAtPointer(ptr);
            const uint64_t value_slot_0 = (raw_value & SLOT_MASK);
            if (value_slot_0 == 0) {
                // we found the slot, write the value
                const uint64_t mask = ~SLOT_MASK;
                const auto new_value = (raw_value & mask) | value;
                WriteRawValueAtPointer(ptr, new_value);
                return true;
            }

            // todo, what if we are at the end of the ht? + How do we handle multithreading?
            idx_t remaining_bytes = sizeof(uint64_t) - bytes_per_value;
            if (remaining_bytes < bytes_per_value) {
                // todo: in theory we could check if the remaining bits are 0, if not we have a collision
                // and could increment the offset right away
                return false;
            }

            offset++;
            collisions++;

            const uint64_t mask_slot_1 = (SLOT_MASK << (bytes_per_value * 8));

            const uint64_t unaligned_value_slot_1 = (raw_value & mask_slot_1);
            if (unaligned_value_slot_1 == 0) {
                const uint64_t value_shifted = value << (bytes_per_value * 8);
                const auto new_value = raw_value | value_shifted;
                WriteRawValueAtPointer(ptr, new_value);
                return true;
            } else {
                remaining_bytes -= bytes_per_value;
                if (remaining_bytes < 1) {
                    return false;
                }

                const uint64_t mask_slot_2 = mask_slot_1 << (bytes_per_value * 8);
                const uint64_t unaligned_value_slot_2 = (raw_value & mask_slot_2);

                const bool is_filled = unaligned_value_slot_2 != 0;
                offset += is_filled;
                collisions += is_filled;

                return false;
            }
        }
    };

    class CompressedPartitioned : PartitionedHashTable {
    public:
        unique_ptr<RowLayoutPartition> continuous_partition;
        uint64_t bytes_per_value;

        data_ptr_t ht_allocation_compressed;

        CompressedPartitioned(uint64_t number_of_records, MemoryManager &memory_manager) : PartitionedHashTable(
            number_of_records, memory_manager) {
        }

        void PostProcessBuild(RowLayout &layout, uint8_t partition_bits) override {
            const double bits_float = std::log2(static_cast<double>(number_of_records));
            const auto bits_per_value = static_cast<uint64_t>(floor(bits_float));
            bytes_per_value = (bits_per_value + 7) / 8;

            switch (bytes_per_value) {
                case 1:
                    PostProcessBuildIternal<1>(layout, partition_bits);
                    break;
                case 2:
                    PostProcessBuildIternal<2>(layout, partition_bits);
                    break;
                case 3:
                    PostProcessBuildIternal<3>(layout, partition_bits);
                    break;
                case 4:
                    PostProcessBuildIternal<4>(layout, partition_bits);
                    break;
                case 5:
                    PostProcessBuildIternal<5>(layout, partition_bits);
                    break;
                case 6:
                    PostProcessBuildIternal<6>(layout, partition_bits);
                    break;
                case 7:
                    PostProcessBuildIternal<7>(layout, partition_bits);
                    break;
                default:
                    throw std::runtime_error("Unsupported bytes per value: " + std::to_string(bytes_per_value));
            }
        }

        template<idx_t BYTES_PER_VALUE>
        void PostProcessBuildIternal(RowLayout &layout, uint8_t partition_bits) {
            data_ptr_t prefix_sum_ptr = memory_manager.allocate(sizeof(uint32_t) * capacity);
            auto *prefix_sum = reinterpret_cast<uint32_t *>(prefix_sum_ptr);

            uint64_t row_width = layout.format.size;
            uint64_t continuous_size = row_width * layout.row_count;
            continuous_partition = make_uniq<RowLayoutPartition>(0, layout.scatter_functions, layout.gather_functions, layout.equality_functions,
                                                                 layout.format,
                                                                 memory_manager, continuous_size);
            data_ptr_t continuous_start = continuous_partition->data;

            constexpr size_t BLOCK_SIZE = 2048;
            const idx_t block_count = capacity / BLOCK_SIZE;
            const auto mask = new bool[BLOCK_SIZE];
            uint32_t current_prefix_sum = 0;

            float entries_per_slot = static_cast<float>(layout.row_count) / static_cast<float>(capacity);
            uint32_t max_error = 0;

            for (size_t block_idx = 0; block_idx < block_count; block_idx++) {
                for (idx_t i = 0; i < BLOCK_SIZE; i++) {
                    const idx_t ht_idx = block_idx * BLOCK_SIZE + i;
                    const bool is_occupied = ht[ht_idx] != 0;
                    mask[i] = is_occupied;
                }
                for (idx_t i = 0; i < BLOCK_SIZE; i++) {
                    const idx_t ht_idx = block_idx * BLOCK_SIZE + i;
                    prefix_sum[ht_idx] = current_prefix_sum;
                    current_prefix_sum += mask[i];

                    // ht_idx * entries_per_slot is the expected value
                    const float expected_value = static_cast<float>(ht_idx) * entries_per_slot;
                    const auto actual_value = static_cast<float>(current_prefix_sum);
                    const auto abs_error = static_cast<uint32_t>(std::abs(expected_value - actual_value));
                    max_error = std::max(max_error, abs_error);
                }
            }

            // std::cout << "MaxError=" << max_error << ' ';
            delete[] mask;


            const uint64_t ht_compressed_size = capacity * BYTES_PER_VALUE + sizeof(uint64_t);
            ht_allocation_compressed = memory_manager.allocate(ht_compressed_size);
            memset(ht_allocation_compressed, 0, ht_compressed_size);

            using Constants = CompressedConstants<BYTES_PER_VALUE>;


            for (size_t ht_idx = 0; ht_idx < capacity; ht_idx++) {
                const bool is_occupied = ht[ht_idx] != 0;

                if (!is_occupied) {
                    continue;
                }

                const uint64_t row_offset = prefix_sum[ht_idx];
                Constants::WriteValueAtOffset(ht_allocation_compressed, ht_idx, row_offset);

                data_ptr_t __restrict row_source_ptr = cast_uint64_to_pointer(ht[ht_idx]);
                data_ptr_t __restrict row_target_ptr = continuous_start + row_offset * row_width;

                if (row_width >= 32) {
                    // copy the data in blocks of 32 bytes
                    constexpr idx_t COPY_BLOCK_SIZE = 32;

                    data_ptr_t __restrict row_source_end = row_source_ptr + row_width - COPY_BLOCK_SIZE;
                    data_ptr_t __restrict row_target_end = row_target_ptr + row_width - COPY_BLOCK_SIZE;

                    for (idx_t j = 0; j < row_width; j += COPY_BLOCK_SIZE) {
                        data_ptr_t __restrict source_ptr = std::min(row_source_ptr + j, row_source_end);
                        data_ptr_t __restrict target_ptr = std::min(row_target_ptr + j, row_target_end);

                        std::memcpy(target_ptr, source_ptr, COPY_BLOCK_SIZE);
                    }
                } else if (row_width == 16) {
                    std::memcpy(row_target_ptr, row_source_ptr, 16);
                } else {
                    std::memcpy(row_target_ptr, row_source_ptr, row_width);
                }
            }

            memory_manager.deallocate(ht_allocation);
            memory_manager.deallocate(prefix_sum_ptr);
        }


        void Free() override {
            continuous_partition->Free();
            memory_manager.deallocate(ht_allocation_compressed);
        }
    };
}


#endif //HASH_TABLE_H
