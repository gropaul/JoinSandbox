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
#include "utils.hpp"


// typedef uint32_t key_t;
typedef uint64_t ht_slot_t;


namespace duckdb {
    struct Functions {
    public:
        static inline constexpr uint64_t GetSlotMask(const uint64_t bytes_per_slot) {
            return (1ULL << (bytes_per_slot * 8)) - 1;
        }
    };

    template<idx_t bytes_per_slot>
    struct CompressedConstants {
    public:
        //! Has 1s where the slot is, 0s elsewhere
        static constexpr uint64_t SLOT_MASK = Functions::GetSlotMask(bytes_per_slot);

    public:
        static inline constexpr bool IsPowerOfTwo(const uint64_t value) {
            return (value & (value - 1)) == 0;
        }

        static inline constexpr uint64_t GetPowerOfTwo(const uint64_t value) {
            // we already know that the value is a power of two, we can't use a loop
            return static_cast<uint64_t>(std::log2(value));
        }

        static inline uint64_t GetCompressedOffset(const uint64_t offset) {
            if (IsPowerOfTwo(bytes_per_slot)) {
                return offset << GetPowerOfTwo(bytes_per_slot);
            } else {
                return offset * bytes_per_slot;
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
            idx_t remaining_bytes = sizeof(uint64_t) - bytes_per_slot;
            if (remaining_bytes < bytes_per_slot) {
                // todo: in theory we could check if the remaining bits are 0, if not we have a collision
                // and could increment the offset right away
                return false;
            }

            offset++;
            collisions++;

            const uint64_t mask_slot_1 = (SLOT_MASK << (bytes_per_slot * 8));

            const uint64_t unaligned_value_slot_1 = (raw_value & mask_slot_1);
            if (unaligned_value_slot_1 == 0) {
                const uint64_t value_shifted = value << (bytes_per_slot * 8);
                const auto new_value = raw_value | value_shifted;
                WriteRawValueAtPointer(ptr, new_value);
                return true;
            } else {
                remaining_bytes -= bytes_per_slot;
                if (remaining_bytes < 1) {
                    return false;
                }

                const uint64_t mask_slot_2 = mask_slot_1 << (bytes_per_slot * 8);
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
        uint64_t bytes_per_slot;

        data_ptr_t ht_allocation_compressed;
        uint8_t *ht_1;
        uint8_t *ht_2;

        vector<compressed_vector_equality_function_t> compressed_vector_eq_functions;
        vector<uint8_t> compressed_value_widths;
        uint64_t continuous_row_width;

        static constexpr uint8_t GROUP_SIZE = 16;

        CompressedPartitioned(uint64_t number_of_records, MemoryManager &memory_manager,
                              const vector<column_t> &keys) : PartitionedHashTable(
            number_of_records, memory_manager, keys) {
        }

        void PostProcessBuild(RowLayout &layout, uint8_t partition_bits) override {
            const double bits_float = std::log2(static_cast<double>(number_of_records));
            const auto bits_per_slot = static_cast<uint64_t>(ceil(bits_float));
            bytes_per_slot = (bits_per_slot + 7) / 8;

            switch (bytes_per_slot) {
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
                    throw std::runtime_error("Unsupported bytes per value: " + std::to_string(bytes_per_slot));
            }
        }

        uint64_t GetHTSize(const uint64_t n_partitions) const override {
            return ((capacity * bytes_per_slot / GROUP_SIZE) + capacity) / n_partitions;
        }

        void GetInitialHTOffsetAndSalt(DataChunk &left, ProbeState &state, const vector<column_t> &key_columns,
                                       const size_t capacity_bit_shift) override {
            idx_t count = left.size();
            auto &key = left.data[key_columns[0]];
            VectorOperations::Hash(key, state.offsets_v, count);

            for (idx_t i = 1; i < key_columns.size(); i++) {
                auto &combined_key = left.data[key_columns[i]];
                VectorOperations::CombineHash(state.offsets_v, combined_key, count);
            }

            const auto hashes = FlatVector::GetData<uint64_t>(state.offsets_v);
            const auto salts_small = FlatVector::GetData<uint64_t>(state.salts_small_v);
            for (idx_t i = 0; i < count; i++) {
                uint64_t salt = (hashes[i] << SALT_SHIFT >> 64 - 8);
                if (salt == 0) {
                    salt = 1;
                }
                uint64_t salt_u = 0x0101010101010101ULL * salt;
                salts_small[i] = salt_u;
                hashes[i] = hashes[i] >> capacity_bit_shift;
            }
        }

        uint64_t Trailing(uint64_t value_in) {
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

        inline uint64_t zero_byte_mask(uint64_t x) {
            return (x - 0x0101010101010101ULL) & ~x & 0x8080808080808080ULL;
        }

        uint64_t ProbeGroupUint64(const uint64_t salt, uint8_t* __restrict ht_1, const uint64_t group_offset, uint64_t &full){

            const auto ht_ptr = reinterpret_cast<uint64_t*>(&ht_1[group_offset]);

            const uint64_t ht_u = *ht_ptr;
            const uint64_t salt_match = salt ^ ht_u;

            const uint64_t match = ht_u & salt_match;
            const uint64_t zero_mask = zero_byte_mask(match);

            const uint64_t idx_bits = Trailing(zero_mask >> 7) ;

            uint64_t offset = idx_bits / 8;
            full = ht_1[group_offset + offset] != 0;

            return offset;
        }

        uint8_t found_buffer[STANDARD_VECTOR_SIZE];

        idx_t __attribute__((noinline)) GetKeysToCompareInternal(const idx_t remaining_count, const SelectionVector &remaining_sel,
                                       ProbeState &state, uint8_t * __restrict found_idx_flat_ptr, uint8_t * __restrict found_bool_flat_ptr) {

            const auto offsets = FlatVector::GetData<uint64_t>(state.offsets_v);
            const auto salts = FlatVector::GetData<uint64_t>(state.salts_small_v);
            const auto salts_dense = FlatVector::GetData<uint64_t>(state.salts_small_dense_v);
            const auto intermediates = FlatVector::GetData<uint64_t>(state.ht_intermediates_v);

            auto &key_comp_sel = state.key_comp_sel;
            idx_t found_count = 0;

            // find empty or filled slots, add filled slots to the key_comp_sel
            for (idx_t idx = 0; idx < remaining_count; idx++) {
                const idx_t sel_idx = remaining_sel.get_index(idx); // getting rid of the sel_idx was promising
                const uint64_t ht_offset = offsets[sel_idx];

                const auto ht_ptr = reinterpret_cast<uint64_t*>(&ht_1[ht_offset]);
                intermediates[idx] = *ht_ptr;;
                //
                const uint64_t salt = salts[sel_idx];
                salts_dense[idx] = salt;
            }

            for (idx_t idx = 0; idx < remaining_count; idx++) {
                const uint64_t salt = salts_dense[idx];
                const uint64_t ht_u = intermediates[idx];
                const uint64_t salt_match = salt ^ ht_u;

                const uint64_t match = ht_u & salt_match;
                const uint64_t zero_mask = zero_byte_mask(match);

                const uint64_t idx_bits = Trailing(zero_mask >> 7) ;
                const uint64_t full_mask = 0xFFULL << idx_bits;
                const uint64_t ht_mask = ht_u & full_mask;

                const uint8_t full = (ht_mask) != 0;
                const uint8_t found_idx = idx_bits / 8;
                found_idx_flat_ptr[idx] = found_idx;
                found_bool_flat_ptr[idx] = full;
            }

            for (idx_t idx = 0; idx < remaining_count; idx++) {
                const uint64_t found_idx = found_idx_flat_ptr[idx];
                const uint64_t found_full = found_bool_flat_ptr[idx];
                const idx_t sel_idx = remaining_sel.get_index(idx); // getting rid of the sel_idx was promising
                found_buffer[found_count] = found_idx;
                key_comp_sel.set_index(found_count, sel_idx);
                found_count += found_full;
            }

            GetPointersForMatches(remaining_count, remaining_sel, state, found_count, found_buffer);
            return found_count;
        }

        void __attribute__((noinline)) GetPointersForMatches(const idx_t remaining_count, const SelectionVector &remaining_sel,
                                   ProbeState &state, const idx_t found_count, const uint8_t *found_buffer) {
            switch (bytes_per_slot) {
                case 1:
                    GetPointersForMatchesInternal<1>(remaining_count, remaining_sel, state, found_count, found_buffer);
                    break;
                case 2:
                    GetPointersForMatchesInternal<2>(remaining_count, remaining_sel, state, found_count, found_buffer);
                    break;
                case 3:
                    GetPointersForMatchesInternal<3>(remaining_count, remaining_sel, state, found_count, found_buffer);
                    break;
                case 4:
                    GetPointersForMatchesInternal<4>(remaining_count, remaining_sel, state, found_count, found_buffer);
                    break;
                case 5:
                    GetPointersForMatchesInternal<5>(remaining_count, remaining_sel, state, found_count, found_buffer);
                    break;
                case 6:
                    GetPointersForMatchesInternal<6>(remaining_count, remaining_sel, state, found_count, found_buffer);
                    break;
                case 7:
                    GetPointersForMatchesInternal<7>(remaining_count, remaining_sel, state, found_count, found_buffer);
                    break;
                default:
                    throw std::runtime_error("Unsupported bytes per value: " + std::to_string(bytes_per_slot));
            }
        }

        template<idx_t BYTES_PER_SLOT>
        void GetPointersForMatchesInternal(const idx_t remaining_count, const SelectionVector &remaining_sel,
                                           ProbeState &state, const idx_t found_count, const uint8_t *found_buffer) {
            const auto rhs_ptrs = FlatVector::GetData<data_ptr_t>(state.rhs_row_pointers_v);
            const auto offsets = FlatVector::GetData<uint64_t>(state.offsets_v);

            data_ptr_t continuous_start = continuous_partition->data;

            using Constants = CompressedConstants<BYTES_PER_SLOT>;

            // std::cout << "GetPointersForMatchesInternal: FoundCount=" << found_count << '\n';

            // find empty or filled slots, add filled slots to the key_comp_sel
            for (idx_t idx = 0; idx < found_count; idx++) {
                const auto sel_idx = state.key_comp_sel.get_index(idx);

                const uint64_t found_idx = static_cast<uint64_t>(found_buffer[idx]);
                const auto ht_offset = offsets[sel_idx] + found_idx;

                const auto group_idx = ht_offset / GROUP_SIZE;
                const auto group_offset = group_idx * GROUP_SIZE;

                const auto max_offset_within_group = ht_offset - group_offset;
                const auto chains_in_group = GetFullValuesFromGroupSlow(ht_1, group_offset, max_offset_within_group);

                const auto key_base_offset = Constants::ReadValueAtOffset(ht_2, group_idx);
                const auto key_actual_offset = key_base_offset + chains_in_group;

                rhs_ptrs[sel_idx] = continuous_start + key_actual_offset * continuous_row_width;

                // std::cout << "Key=" << sel_idx << " BaseOffset=" << key_base_offset << " ActualOffset=" << key_actual_offset << '\n';
            }
        }
        uint8_t compute_buffer[GROUP_SIZE];

        uint8_t found_idx_flat[STANDARD_VECTOR_SIZE];
        uint8_t found_bool_flat[STANDARD_VECTOR_SIZE];

        idx_t GetKeysToCompare(const idx_t remaining_count, const SelectionVector &remaining_sel,
                               ProbeState &state) override {
            return GetKeysToCompareInternal(remaining_count, remaining_sel, state, found_idx_flat, found_bool_flat);
        }

        idx_t CompareKeys(const Vector &keys_v, ProbeState &state, const idx_t key_comp_count) const override {
            auto &key_comp_sel = state.key_comp_sel;
            const auto offset = key_row_offsets[0];
            idx_t equality_count = compressed_vector_eq_functions[0](keys_v, state.rhs_row_pointers_v, key_comp_sel,
                                                                     key_comp_count, offset, state.key_equal_sel,
                                                                     state.remaining_sel);
            return equality_count;
        }



        vector<uint64_t> col_min_values;
        vector<uint8_t> GetCompressedValueWidths(RowLayout &layout) {
            vector<uint8_t> widths;
            for (const auto &range: layout.min_max_ranges) {
                auto value_width = range.max - range.min;
                auto value_offset = range.min;
                col_min_values.push_back(value_offset);
                auto bytes_for_width = static_cast<uint8_t>(std::ceil(std::log2(value_width + 1) / 8));
                widths.push_back(bytes_for_width);
            }

            return widths;
        }

        struct CopyTask {
            data_ptr_t from;
            data_ptr_t to;
        };

        template<idx_t BYTES_PER_SLOT>
        data_ptr_t CreateTasksAndFillHTs(const RowLayout &layout, const idx_t capacity,
                                         const idx_t next_pointer_offset) const {
            // in the last iteration, there could be that we have already the whole ht values through but there are
            // trailing empty slots which would cause a buffer overflow
            data_ptr_t copy_tasks_ptr = memory_manager.allocate((layout.row_count + 1) * sizeof(CopyTask));


            auto *copy_tasks = reinterpret_cast<CopyTask *>(copy_tasks_ptr);

            using Constants = CompressedConstants<BYTES_PER_SLOT>;

            data_ptr_t continuous_start = continuous_partition->data;

            uint64_t stored_values_count = 0;

            for (idx_t ht_offset = 0; ht_offset < capacity; ht_offset += 1) {
                const data_ptr_t from = cast_uint64_to_pointer(ht[ht_offset] & 0x0000FFFFFFFFFFFF);
                const data_ptr_t to = continuous_start + stored_values_count * continuous_row_width;

                D_ASSERT(stored_values_count <= layout.row_count);
                copy_tasks[stored_values_count] = {from, to};

                // get the salt and store it in the ht_1
                uint8_t salt = ht[ht_offset] >> (8 - sizeof(uint8_t)) * 8;

                const bool is_full = ht[ht_offset] != 0;
                if (salt == 0 && is_full) {
                    salt = 1;
                }

                ht_1[ht_offset] = salt; // never allow 0, this is the empty slot, if empty store 0!

                // write to ht_2 if element_idx is divisible by GROUP_SIZE
                if (ht_offset % GROUP_SIZE == 0) {
                    Constants::WriteValueAtOffset(ht_2, ht_offset / GROUP_SIZE, stored_values_count);
                    // std::cout << "Group=" << ht_offset / GROUP_SIZE << "->" << stored_values_count << '\n';
                }


                if (is_full) {
                    const uint64_t key = Load<uint64_t>(from);
                    // std::cout << "Continuous=" << stored_values_count << "->" << key << " (HTOffset=" << ht_offset << ")\n";
                }


                stored_values_count += is_full;
            }

            D_ASSERT(stored_values_count == layout.row_count);
            // std::cout << "MaxError=" << max_error << " MinError=" << min_error << ' ';
            return copy_tasks_ptr;
        }


        __attribute__((noinline)) void Copy(const size_t element_idx,
                                            const CopyTask *copy_tasks,
                                            const vector<uint64_t> &value_offsets,
                                            const vector<uint64_t> &value_offsets_compressed) {
            const CopyTask task = copy_tasks[element_idx];

            column_t n_cols = value_offsets_compressed.size();
            for (column_t col_idx = 0; col_idx < n_cols; col_idx++) {
                const auto value_offset = value_offsets[col_idx];
                const auto value_offset_compressed = value_offsets_compressed[col_idx];
                const auto compressed_width = compressed_value_widths[col_idx];
                // todo: target aligned 64bit writes because 8 bit write will trigger 64 bit read + mask + or + write
                // todo: Do this vectorized: first fill vector of these sizes, then write them to the target ->
                // todo: overhead of masking and shifting reduced, loop at frame of reference encoding
                const auto value_source_ptr = task.from + value_offset;
                const auto value_target_ptr = task.to + value_offset_compressed;

                // write the compressed value to the target
                std::memcpy(value_target_ptr, value_source_ptr, compressed_width);
            }

            // return cast_uint64_to_pointer(Load<uint64_t>(row_source_ptr + next_pointer_offset));
            // do {
            //     data_ptr_t next_ptr = CopyCompressedRow(row_source_ptr, row_target_ptr, value_offsets,
            //                                             value_offsets_compressed, next_pointer_offset,
            //                                             compressed_value_widths);
            //
            //     row_source_ptr = next_ptr;
            //     row_target_ptr += continuous_row_width;
            // } while (row_source_ptr != nullptr);
        }

        __attribute__((noinline)) void FillContinuousLayout(const CopyTask *copy_tasks, const uint64_t elements_count,
                                                            const vector<uint64_t> &value_offsets,
                                                            const vector<uint64_t> &value_offsets_compressed
        ) {
            for (size_t element_idx = 0; element_idx < elements_count; element_idx++) {
                Copy(element_idx, copy_tasks, value_offsets, value_offsets_compressed);
            }
        }


        template<idx_t BYTES_PER_SLOT>
        void PostProcessBuildIternal(RowLayout &layout, uint8_t partition_bits) {
            // *** ALLOCATE THE NEW COMPRESSED HT ***

            // one additional group at the end that is the same as the first group
            const uint64_t salt_region_size = capacity * sizeof(uint8_t) + GROUP_SIZE;
            const uint64_t offset_region_size = (capacity / GROUP_SIZE) * BYTES_PER_SLOT + sizeof(uint8_t);

            // offset if accessing the last elements
            const uint64_t ht_compressed_size = salt_region_size + offset_region_size;
            ht_allocation_compressed = memory_manager.allocate(ht_compressed_size);
            memset(ht_allocation_compressed, 0, salt_region_size);

            ht_1 = ht_allocation_compressed;
            ht_2 = ht_allocation_compressed + salt_region_size;

            // *** INITIALIZE THE CONTINUOUS LAYOUT ***

            // we don't need the hash anymore
            continuous_row_width = 0;
            compressed_value_widths = GetCompressedValueWidths(layout);
            vector<uint64_t> value_offsets_compressed;
            for (const auto &compressed_width: compressed_value_widths) {
                value_offsets_compressed.push_back(continuous_row_width);
                continuous_row_width += compressed_width;
            }

            for (const auto key_col_idx: key_columns) {
                LogicalType type = layout.format.types[key_col_idx];
                uint8_t compressed_width = compressed_value_widths[key_col_idx];
                compressed_vector_eq_functions.push_back(GetCompressedEqualityFunction(type, compressed_width));
            }

            uint64_t continuous_size = continuous_row_width * layout.row_count;
            continuous_partition = make_uniq<RowLayoutPartition>(0, layout.scatter_functions, layout.gather_functions,
                                                                 layout.equality_functions,
                                                                 layout.format,
                                                                 memory_manager, continuous_size);

            // *** CREATE COPY TASKS AND FILL THE HTS ***

            const idx_t next_pointer_offset = layout.format.offsets[layout.format.types.size() - 1];
            data_ptr_t copy_tasks_ptr = CreateTasksAndFillHTs<BYTES_PER_SLOT>(layout, capacity, next_pointer_offset);
            const auto *copy_tasks = reinterpret_cast<CopyTask *>(copy_tasks_ptr);

            // *** FILL THE CONTINUOUS LAYOUT ***

            FillContinuousLayout(copy_tasks, layout.row_count, layout.format.offsets,
                                 value_offsets_compressed);

            memory_manager.deallocate(ht_allocation);
            memory_manager.deallocate(copy_tasks_ptr);

            // fill the last group with the first group
            for (idx_t i = 0; i < GROUP_SIZE; i++) {
                ht_1[capacity + i] = ht_1[i];
            }
        }


        void Free() override {
            continuous_partition->Free();
            memory_manager.deallocate(ht_allocation_compressed);
        }
    };
}


#endif //HASH_TABLE_H
