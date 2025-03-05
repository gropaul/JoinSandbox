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
        uint64_t row_width;

        data_ptr_t ht_allocation_compressed;

        CompressedPartitioned(uint64_t number_of_records, MemoryManager &memory_manager,
                              const vector<column_t> &keys) : PartitionedHashTable(
            number_of_records, memory_manager, keys) {
        }

        void PostProcessBuild(RowLayout &layout, uint8_t partition_bits) override {
            const double bits_float = std::log2(static_cast<double>(number_of_records));
            const auto bits_per_value = static_cast<uint64_t>(ceil(bits_float));
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

        static data_ptr_t CopyRow(uint8_t * __restrict row_source_ptr, uint8_t * __restrict row_target_ptr,
                                  uint64_t row_width, uint64_t next_pointer_offset) {
            if (row_width >= 32) {
                constexpr idx_t COPY_BLOCK_SIZE = 32;

                uint8_t * __restrict row_source_end = row_source_ptr + row_width - COPY_BLOCK_SIZE;
                uint8_t * __restrict row_target_end = row_target_ptr + row_width - COPY_BLOCK_SIZE;

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

            return nullptr;
            // return cast_uint64_to_pointer(Load<uint64_t>(row_source_ptr + next_pointer_offset));
        }

        uint64_t GetHTSize(const uint64_t n_partitions) const override {
            return (capacity * bytes_per_value) / n_partitions;
        }

        template<idx_t BYTES_PER_VALUE>
        idx_t GetKeysToCompareInternal(const idx_t remaining_count, const SelectionVector &remaining_sel,
                                       const Vector &offsets_v, ProbeState &state) const {
            const auto offsets = FlatVector::GetData<uint64_t>(state.offsets_v);
            const auto rhs_ptrs = FlatVector::GetData<data_ptr_t>(state.rhs_row_pointers_v);

            auto &key_comp_sel = state.key_comp_sel;
            idx_t key_comp_count = 0;

            using Constants = CompressedConstants<BYTES_PER_VALUE>;
            data_ptr_t start_ptr = ht_allocation_compressed;
            data_ptr_t continuous_start = continuous_partition->data;
            uint64_t row_width = continuous_partition->format.size - sizeof(uint64_t);

            // find empty or filled slots, add filled slots to the key_comp_sel
            for (idx_t idx = 0; idx < remaining_count; idx++) {
                const auto remaining_idx = state.remaining_sel.get_index(idx);
                const auto ht_offset = offsets[remaining_idx];
                while (true) {
                    const uint64_t ht_slot = Constants::ReadValueAtOffset(start_ptr, ht_offset);
                    if (ht_slot == 0) {
                        break;
                    }
                    // we need to compare the keys
                    rhs_ptrs[remaining_idx] = continuous_start + ht_slot * row_width;
                    key_comp_sel.set_index(key_comp_count, remaining_idx);
                    key_comp_count++;
                    break;
                }
            }

            return key_comp_count;
        }

        idx_t GetKeysToCompare(const idx_t remaining_count, const SelectionVector &remaining_sel,
                               const Vector &offsets_v, ProbeState &state) const override {
            switch (bytes_per_value) {
                case 1:
                    return GetKeysToCompareInternal<1>(remaining_count, remaining_sel, offsets_v, state);
                case 2:
                    return GetKeysToCompareInternal<2>(remaining_count, remaining_sel, offsets_v, state);
                case 3:
                    return GetKeysToCompareInternal<3>(remaining_count, remaining_sel, offsets_v, state);
                case 4:
                    return GetKeysToCompareInternal<4>(remaining_count, remaining_sel, offsets_v, state);
                case 5:
                    return GetKeysToCompareInternal<5>(remaining_count, remaining_sel, offsets_v, state);
                case 6:
                    return GetKeysToCompareInternal<6>(remaining_count, remaining_sel, offsets_v, state);
                case 7:
                    return GetKeysToCompareInternal<7>(remaining_count, remaining_sel, offsets_v, state);
                default:
                    throw std::runtime_error("Unsupported bytes per value: " + std::to_string(bytes_per_value));
            }
        }

        data_ptr_t CreatePrefixSum(const RowLayout &layout, const idx_t capacity,
                                   const idx_t next_pointer_offset) const {
            data_ptr_t prefix_sum_ptr = memory_manager.allocate(sizeof(uint32_t) * capacity);

            float entries_per_slot = static_cast<float>(layout.row_count) / static_cast<float>(capacity);


            auto *prefix_sum = reinterpret_cast<uint32_t *>(prefix_sum_ptr);

            constexpr uint32_t BLOCK_SIZE = 4096;

            uint16_t mask[BLOCK_SIZE];

            uint32_t block_prefix_sum = 0;

            float max_error = 0;
            float min_error = 0;

            for (idx_t block_idx = 0; block_idx < capacity; block_idx += BLOCK_SIZE) {
                idx_t n_filled = 0;


                for (idx_t element_idx = 0; element_idx < BLOCK_SIZE; element_idx++) {
                    const idx_t ht_idx = block_idx + element_idx;
                    const bool is_full = ht[ht_idx] != 0;
                    mask[element_idx] = is_full;
                    n_filled += is_full;
                }

                // get the running sum of the mask
                for (idx_t element_idx = 1; element_idx < BLOCK_SIZE; element_idx++) {
                    mask[element_idx] += mask[element_idx - 1];
                }

                float local_max_error = 0;
                float local_min_error = 0;

                for (idx_t element_idx = 0; element_idx < BLOCK_SIZE; element_idx++) {
                    const idx_t ht_idx = block_idx + element_idx;
                    const uint32_t local_prefix_sum = mask[element_idx];
                    const uint32_t global_prefix_sum = block_prefix_sum + local_prefix_sum;
                    prefix_sum[ht_idx] = global_prefix_sum;

                    // ht_idx * entries_per_slot is the expected value
                    // const float expected_value = static_cast<float>(ht_idx) * entries_per_slot;
                    // const auto actual_value = static_cast<float>(global_prefix_sum);
                    // const auto error = expected_value - actual_value;
                    // local_max_error = std::max(max_error, error);
                    // local_min_error = std::min(min_error, error);
                }

                max_error = std::max(max_error, local_max_error);
                min_error = std::min(min_error, local_min_error);

                block_prefix_sum += mask[BLOCK_SIZE - 1];
            }

            std::cout << "MaxError=" << max_error << " MinError=" << min_error << ' ';
            return prefix_sum_ptr;
        }


        template<idx_t BYTES_PER_VALUE>
         __attribute__((noinline)) void FillContinuousLayout(const uint32_t *prefix_sum, data_ptr_t continuous_start,
                                    const idx_t capacity, const idx_t next_pointer_offset) {

            using Constants = CompressedConstants<BYTES_PER_VALUE>;

            for (size_t ht_offset = 0; ht_offset < capacity; ht_offset++) {
                const bool is_occupied = ht[ht_offset] != 0;
                const uint32_t row_offset = prefix_sum[ht_offset];

                if (!is_occupied) {
                    continue;
                }
                // todo: this must go before the !is_occupied check to encode the chain length
                Constants::WriteValueAtOffset(ht_allocation_compressed, ht_offset, row_offset);

                data_ptr_t __restrict row_source_ptr = cast_uint64_to_pointer(ht[ht_offset]);
                data_ptr_t __restrict row_target_ptr = continuous_start + row_offset * row_width;

                // uint64_t read_offset = Constants::ReadValueAtOffset(ht_allocation_compressed, ht_offset);
                // D_ASSERT(read_offset == row_offset);

                do {
                    data_ptr_t next_ptr = CopyRow(row_source_ptr, row_target_ptr, row_width, next_pointer_offset);

                    // check if the first uint64_t at row_source_ptr is the same as the row_target_ptr
                    // const auto new_value = Load<uint64_t>(row_target_ptr);
                    // const auto og_value = Load<uint64_t>(row_source_ptr);
                    // D_ASSERT(new_value == og_value);

                    row_source_ptr = next_ptr;
                    row_target_ptr += row_width;
                } while (row_source_ptr != nullptr);
            }
        }

        template<idx_t BYTES_PER_VALUE>
        void PostProcessBuildIternal(RowLayout &layout, uint8_t partition_bits) {
            idx_t next_pointer_offset = layout.format.offsets[layout.format.types.size() - 1];
            data_ptr_t prefix_sum_ptr = CreatePrefixSum(layout, capacity, next_pointer_offset);
            auto *prefix_sum = reinterpret_cast<uint32_t *>(prefix_sum_ptr);

            // we don't need the hash anymore
            row_width = layout.format.size - sizeof(uint64_t);
            uint64_t continuous_size = row_width * layout.row_count;
            continuous_partition = make_uniq<RowLayoutPartition>(0, layout.scatter_functions, layout.gather_functions,
                                                                 layout.equality_functions,
                                                                 layout.format,
                                                                 memory_manager, continuous_size);
            data_ptr_t continuous_start = continuous_partition->data;

            const uint64_t ht_compressed_size = capacity * BYTES_PER_VALUE + sizeof(uint64_t);
            ht_allocation_compressed = memory_manager.allocate(ht_compressed_size);
            memset(ht_allocation_compressed, 0, ht_compressed_size);

            FillContinuousLayout<BYTES_PER_VALUE>(prefix_sum, continuous_start, capacity, next_pointer_offset);

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
