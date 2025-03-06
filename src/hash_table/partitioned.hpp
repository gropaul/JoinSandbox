//
// Created by Paul on 08/02/2025.
//

#ifndef HASH_TABLE_PARTIONED
#define HASH_TABLE_PARTIONED
#include "utils.hpp"
#include "base.hpp"
#include <iostream>
#include <cmath>

#include "base.hpp"
#include "materialization/row_layout.hpp"


typedef uint64_t ht_slot_t;

namespace duckdb {
    struct ProbeState {
        Vector offsets_v;
        Vector found_row_pointers_v;

        // used for bulk key comparison
        Vector rhs_row_pointers_v;

        idx_t found_count;
        SelectionVector found_sel;


        SelectionVector remaining_sel;

        SelectionVector key_equal_sel;
        SelectionVector key_comp_sel;

        explicit ProbeState(): offsets_v(LogicalType::HASH), found_row_pointers_v(LogicalType::POINTER),
                               rhs_row_pointers_v(LogicalType::POINTER), found_count(0),
                               found_sel(STANDARD_VECTOR_SIZE), remaining_sel(STANDARD_VECTOR_SIZE),
                               key_equal_sel(STANDARD_VECTOR_SIZE),
                               key_comp_sel(STANDARD_VECTOR_SIZE) {
        }
    };

    class PartitionedHashTable : HashTableBase {
    public:
        data_ptr_t ht_allocation;
        ht_slot_t *ht;

        uint64_t number_of_records;
        uint64_t capacity;
        uint64_t capacity_mask;
        uint64_t capacity_bit_shift;

        uint64_t elements_build = 0;
        uint64_t collisions_build = 0;

        uint64_t elements_probe = 0;
        uint64_t collisions_probe = 0;

        MemoryManager &memory_manager;
        const vector<column_t> &key_columns;

        bool needs_new_row_pointers = true;
        ProbeState probe_state;


        vector<gather_function_t> gather_functions;
        RowLayoutFormat *format;

        vector<row_equality_function_t> row_eq_functions;
        vector<vector_equality_function_t> vector_eq_functions;
        vector<size_t> key_row_offsets;

        PartitionedHashTable(uint64_t number_of_records_p, MemoryManager &memory_manager,
                             const vector<column_t> &keys) : memory_manager(memory_manager), key_columns(keys) {
            number_of_records = number_of_records_p;
            capacity = NextPowerOfTwo(2 * number_of_records);
            capacity_mask = capacity - 1;
            auto n_bits = std::log2(capacity);
            capacity_bit_shift = (64 - static_cast<uint64_t>(n_bits));
        }

        void InitializeHT() override {
            const uint64_t ht_size = capacity * sizeof(ht_slot_t);
            ht_allocation = memory_manager.allocate(ht_size);
            ht = reinterpret_cast<ht_slot_t *>(ht_allocation);
            std::memset(ht_allocation, 0, ht_size);
        }

        void InsertAll(RowLayout &layout, uint8_t partition_bits, uint64_t hash_idx) override {
            const uint64_t partition_count = 1 << partition_bits;

            Vector hashes_v(LogicalType::HASH);
            column_t hash_col_idx = layout.format.types.size() - 1;
            auto hash_col_offset = layout.format.offsets[hash_col_idx];

            gather_functions = layout.gather_functions;
            format = &layout.format;

            for (const auto key_col_idx: key_columns) {
                LogicalType type = layout.format.types[key_col_idx];
                row_eq_functions.push_back(GetRowEqualityFunction(type));
                vector_eq_functions.push_back(GetEqualityFunction(type));
                size_t offset = layout.format.offsets[key_col_idx];
                key_row_offsets.push_back(offset);
            }

            for (uint64_t partition_idx = 0; partition_idx < partition_count; partition_idx++) {
                RowLayoutIterator layout_iterator(layout, partition_idx);
                IteratorStep state;
                while (layout_iterator.Next(state)) {
                    layout.Gather(state.partition_step.row_pointer, hash_col_idx, state.partition_step.count, hashes_v);
                    Insert(hashes_v, state, partition_idx, partition_bits, row_eq_functions, key_row_offsets,
                           hash_col_offset);
                }
            }
        }

        virtual void Insert(Vector &hashes_v, IteratorStep &state, uint64_t partition_idx, uint8_t partition_bits,
                            const vector<row_equality_function_t> &key_equal,
                            const vector<size_t> &key_offsets, const size_t hash_col_offset) {
            // perform linear probing
            idx_t count = state.partition_step.count;
            auto hashes_data = FlatVector::GetData<uint64_t>(hashes_v);
            auto row_pointer_data = FlatVector::GetData<data_ptr_t>(state.partition_step.row_pointer);

            elements_build += count;

            for (uint64_t i = 0; i < count; i++) {
                auto lhs_row_pointer = row_pointer_data[i];
                const auto hash = hashes_data[i];
                auto ht_offset = hash >> capacity_bit_shift;
                while (true) {
                    if (ht[ht_offset] == 0) {
                        ht[ht_offset] = cast_pointer_to_uint64(lhs_row_pointer);
                        // store zero to mark the end of the chain
                        data_ptr_t next_pointer_location = lhs_row_pointer + hash_col_offset;
                        Store<uint64_t>(0, next_pointer_location);
                        break;
                    } else {
                        bool equal = true;
                        for (auto key_col_idx: key_columns) {
                            const auto offset = key_offsets[key_col_idx];
                            const auto left = cast_uint64_to_pointer(ht[ht_offset]);
                            const auto right = lhs_row_pointer;
                            if (!key_equal[key_col_idx](left, right, offset)) {
                                equal = false;
                                break;
                            }
                        }
                        if (equal) {
                            // chain the row: for this row to insert, set the current row pointer to the next row pointer
                            data_ptr_t next_element_pointer = cast_uint64_to_pointer(ht[ht_offset]);
                            data_ptr_t next_pointer_location = lhs_row_pointer + hash_col_offset;
                            // write the next pointer
                            Store<uint64_t>(cast_pointer_to_uint64(next_element_pointer), next_pointer_location);
                            // put the current pointer in the hash table
                            ht[ht_offset] = cast_pointer_to_uint64(lhs_row_pointer);
                            break;
                        } else {
                            collisions_build++;
                            ht_offset = (ht_offset + 1) & capacity_mask;
                        }
                    }
                }
            }
            // std::cout << "Partition=" << partition_idx << " Min=" << min_idx << " Max=" << max_idx << '\n';
        }

        static void GetInitialHTOffset(DataChunk &left, Vector &hashes_v, const vector<column_t> &key_columns,
                                       const size_t capacity_bit_shift) {
            idx_t count = left.size();
            auto &key = left.data[key_columns[0]];
            VectorOperations::Hash(key, hashes_v, count);

            for (idx_t i = 1; i < key_columns.size(); i++) {
                auto &combined_key = left.data[key_columns[i]];
                VectorOperations::CombineHash(hashes_v, combined_key, count);
            }
            auto hashes = FlatVector::GetData<uint64_t>(hashes_v);
            for (idx_t i = 0; i < count; i++) {
                hashes[i] = hashes[i] >> capacity_bit_shift;
            }
        }

        //! Probes the HT and if the key hits and full slot, add it to the list of keys to compare
        virtual idx_t GetKeysToCompare(const idx_t remaining_count, const SelectionVector &remaining_sel,
                                      const Vector &offsets_v, ProbeState &state) const {
            const auto offsets = FlatVector::GetData<uint64_t>(state.offsets_v);
            const auto rhs_ptrs = FlatVector::GetData<data_ptr_t>(state.rhs_row_pointers_v);

            auto &key_comp_sel = state.key_comp_sel;
            idx_t key_comp_count = 0;

            // find empty or filled slots, add filled slots to the key_comp_sel
            for (idx_t idx = 0; idx < remaining_count; idx++) {
                auto remaining_idx = state.remaining_sel.get_index(idx);
                auto ht_offset = offsets[remaining_idx];
                while (true) {
                    auto ht_slot = ht[ht_offset];
                    if (ht_slot == 0) {
                        break;
                    }

                    // we need to compare the keys
                    rhs_ptrs[remaining_idx] = cast_uint64_to_pointer(ht_slot);
                    key_comp_sel.set_index(key_comp_count, remaining_idx);
                    key_comp_count++;
                    break;
                }
            }

            return key_comp_count;
        }

        virtual idx_t CompareKeys(const Vector &keys_v, ProbeState &state, const idx_t key_comp_count) const {
            auto &key_comp_sel = state.key_comp_sel;
            const auto offset = key_row_offsets[0];
            idx_t equality_count = vector_eq_functions[0](keys_v, state.rhs_row_pointers_v, key_comp_sel,
                                                    key_comp_count, offset, state.key_equal_sel,
                                                    state.remaining_sel);
            return equality_count;
        }

        idx_t GetRowPointers(DataChunk &left, ProbeState &state) {
            GetInitialHTOffset(left, state.offsets_v, key_columns, capacity_bit_shift);

            const auto keys_v = left.data[key_columns[0]];
            D_ASSERT(keys_v.GetType() == LogicalType::UBIGINT);
            const auto found_ptrs = FlatVector::GetData<data_ptr_t>(state.found_row_pointers_v);
            const auto rhs_ptrs = FlatVector::GetData<data_ptr_t>(state.rhs_row_pointers_v);
            const auto offsets = FlatVector::GetData<uint64_t>(state.offsets_v);

            // initialize the remaining selection vector
            idx_t remaining_count = left.size();
            for (idx_t i = 0; i < remaining_count; i++) {
                state.remaining_sel.set_index(i, i);
            }

            idx_t &found_count = state.found_count;
            found_count = 0;

            auto &key_comp_sel = state.key_comp_sel;

            while (remaining_count != 0) {

                const idx_t key_comp_count = GetKeysToCompare(remaining_count, state.remaining_sel, state.offsets_v, state);

                // compare the keys in the key_comp_sel with the keys in the hash table
                const auto equality_count = CompareKeys(keys_v, state, key_comp_count);

                // add the equal keys to the found pointers
                for (idx_t i = 0; i < equality_count; i++) {
                    const auto sel_idx = state.key_equal_sel.get_index(i);
                    found_ptrs[sel_idx] = rhs_ptrs[sel_idx];

                    state.found_sel.set_index(found_count, sel_idx);
                    found_count++;
                }

                // add the unequal keys to the key_comp_sel, increment their ht_offset
                const idx_t unequal_count = key_comp_count - equality_count;
                for (idx_t i = 0; i < unequal_count; i++) {
                    const auto sel_idx = state.remaining_sel.get_index(i);
                    auto &ht_offset = offsets[sel_idx];
                    ht_offset = (ht_offset + 1) & capacity_mask;
                    collisions_probe ++;
                }

                remaining_count = unequal_count;
            }


            return found_count;
        }

        OperatorResultType Probe(DataChunk &left, DataChunk &result) override {
            column_t hash_col_idx = format->types.size() - 1;
            auto hash_col_offset = format->offsets[hash_col_idx];

            if (needs_new_row_pointers) {
                elements_probe += left.size();
                GetRowPointers(left, probe_state);
            }

            // load the current row pointers, then load their next pointer
            auto &found_sel = probe_state.found_sel;
            auto &found_count = probe_state.found_count;
            auto found_ptrs = FlatVector::GetData<data_ptr_t>(probe_state.found_row_pointers_v);

            result.SetCardinality(probe_state.found_count);

            // slice the lhs data
            left.Slice(found_sel, found_count);
            for (idx_t i = 0; i < left.ColumnCount(); i++) {
                result.data[i].Reference(left.data[i]);
            }

            // load the data for the found row
            idx_t payload_offset = left.ColumnCount();
            idx_t payload_col_count = result.ColumnCount() - payload_offset;
            for (idx_t i = 0; i < payload_col_count; i++) {
                auto &target = result.data[payload_offset + i];
                auto &gather_function = gather_functions[i];
                auto offset = format->offsets[i];
                gather_function(probe_state.found_row_pointers_v, found_sel, found_count, offset, target);
            }

            uint64_t advanced_count = 0;
            // advance the pointers
            for (idx_t i = 0; i < found_count; i++) {
                // auto found_idx = found_sel.get_index(i);
                // auto next_ptr_location = found_ptrs[found_idx] + hash_col_offset;
                // auto next_ptr = Load<data_ptr_t>(next_ptr_location);
                //
                // found_ptrs[found_idx] = next_ptr;
                // found_sel.set_index(advanced_count, found_idx);
                // advanced_count += next_ptr != nullptr;;
            }

            if (advanced_count == 0) {
                needs_new_row_pointers = true;
                return OperatorResultType::NEED_MORE_INPUT;
            } else {
                probe_state.found_count = advanced_count;
                needs_new_row_pointers = false;
                return OperatorResultType::HAVE_MORE_OUTPUT;
            }
        }

        uint64_t GetCapacity() const override {
            return capacity;
        }

        uint64_t GetHTSize(const uint64_t n_partitions) const override {
            return (capacity * sizeof(ht_slot_t)) / n_partitions;
        }

        double GetCollisionRateBuild() const override {
            return static_cast<double>(collisions_build) / static_cast<double>(elements_build);
        }

        double GetCollisionRateProbe() const override {
            return static_cast<double>(collisions_probe) / static_cast<double>(elements_probe);
        }

        void Free() override {
            memory_manager.deallocate(ht_allocation);
        }

        void PostProcessBuild(RowLayout &layout, uint8_t partition_bits) override {
            // nothing to do
        }

        void Print() const override {
            std::cout << "\nCapacity=" << capacity << " Elements=" << elements_build << " BCollisions=" << collisions_build << '\n';
            for (uint64_t i = 0; i < capacity; i++) {
                std::cout << "ht[" << i << "]=" << ht[i] << '\n';
            }
        }
    };
}


#endif //HASH_TABLE_H
