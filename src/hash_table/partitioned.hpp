//
// Created by Paul on 08/02/2025.
//

#ifndef HASH_TABLE_PARTIONED
#define HASH_TABLE_PARTIONED
#include "utils.hpp"
#include "base.hpp"
#include <iostream>

#include "base.hpp"
#include "materialization/row_layout.hpp"


// typedef uint32_t key_t;
typedef uint64_t ht_slot_t;

namespace duckdb {
    class PartitionedHashTable : HashTableBase {
    public:

        data_ptr_t ht_allocation;
        ht_slot_t *ht;
        uint64_t number_of_records;
        uint64_t capacity;
        uint64_t capacity_mask;

        uint64_t elements = 0;
        uint64_t collisions = 0;

        MemoryManager &memory_manager;

        PartitionedHashTable(uint64_t number_of_records_p, MemoryManager &memory_manager) : memory_manager(memory_manager) {
            number_of_records = number_of_records_p;
            capacity = NextPowerOfTwo(2 * number_of_records);
            capacity_mask = capacity - 1;
        }

        void InitializeHT() override {
            const uint64_t ht_size = capacity * sizeof(ht_slot_t);
            ht_allocation = memory_manager.allocate(ht_size);
            ht = reinterpret_cast<ht_slot_t*>(ht_allocation);
            std::memset(ht_allocation, 0, ht_size);
        }

        void InsertAll(RowLayout &layout, uint8_t partition_bits, uint64_t hash_idx) override {
            const uint64_t partition_count = 1 << partition_bits;

            Vector hashes_v(LogicalType::HASH);
            column_t hash_col_idx = layout.format.types.size() - 1;

            for (uint64_t partition_idx = 0; partition_idx < partition_count; partition_idx++) {
                RowLayoutIterator layout_iterator(layout, partition_idx);
                IteratorStep state;
                while (layout_iterator.Next(state)) {
                    layout.Gather(state.partition_step.row_pointer, hash_col_idx, state.partition_step.count, hashes_v);
                    Insert(hashes_v, state, partition_idx, partition_bits);
                }
            }
        }

        virtual void Insert(Vector &hashes_v, IteratorStep &state, uint64_t partition_idx, uint8_t partition_bits) {

            const auto partition_size = capacity >> partition_bits;
            const auto partition_mask = partition_size - 1;
            const auto partition_offset = partition_idx * partition_size;

            // perform linear probing
            idx_t count = state.partition_step.count;
            auto hashes_data = FlatVector::GetData<uint64_t>(hashes_v);
            auto row_pointer_data = FlatVector::GetData<data_ptr_t>(state.partition_step.row_pointer);

            elements += count;

            for (uint64_t i = 0; i < count; i++) {
                auto row_pointer = row_pointer_data[i];
                auto hash = hashes_data[i];
                auto idx = (hash & partition_mask) + partition_offset;

                while (true) {
                    if (ht[idx] == 0) {
                        ht[idx] = cast_pointer_to_uint64(row_pointer);
                        break;
                    }
                    collisions++;
                    idx = (idx + 1) & capacity_mask;
                }
            }

            // std::cout << "Partition=" << partition_idx << " Min=" << min_idx << " Max=" << max_idx << '\n';
        }

        uint64_t GetCapacity() const override {
            return capacity;
        }

        uint64_t GetHTSize(const uint64_t n_partitions) const override {
            return (capacity * sizeof(ht_slot_t)) / n_partitions;
        }

        double GetCollisionRate() const override {
            return static_cast<double>(collisions) / static_cast<double>(elements);
        }

        void Free() override {
            memory_manager.deallocate(ht_allocation);
        }

        void PostProcessBuild(RowLayout &layout, uint8_t partition_bits) override {
            // nothing to do
        }

        void Print() const override {
            std::cout << "\nCapacity=" << capacity << " Elements=" << elements << " Collisions=" << collisions << '\n';
            for (uint64_t i = 0; i < capacity; i++) {
                std::cout << "ht[" << i << "]=" << ht[i] << '\n';
            }
        }
    };
}



#endif //HASH_TABLE_H
