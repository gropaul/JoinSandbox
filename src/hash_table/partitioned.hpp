//
// Created by Paul on 08/02/2025.
//

#ifndef HASH_TABLE_PARTIONED
#define HASH_TABLE_PARTIONED
#include "utils.hpp"
#include "base.hpp"
#include <iostream>

#include "base.hpp"


namespace duckdb {
    class PartitionedHashTable : HashTableBase {
    public:

        data_ptr_t ht_array;
        uint64_t capacity;
        uint64_t capacity_mask;

        uint64_t elements = 0;
        uint64_t collisions = 0;

        MemoryManager &memory_manager;

        PartitionedHashTable(uint64_t number_of_records, MemoryManager &memory_manager) : memory_manager(memory_manager) {
            capacity = next_power_of_two(2 * number_of_records);
            uint64_t ht_size = capacity * sizeof(uint64_t);
            ht_array = memory_manager.allocate(ht_size);
            std::memset(ht_array, 0, ht_size);

            capacity_mask = capacity - 1;
        }

        void InsertAll(RowLayout &layout, uint8_t partition_bits, uint64_t hash_idx) override {
            const uint64_t partition_count = 1 << partition_bits;
            for (uint64_t partition_idx = 0; partition_idx < partition_count; partition_idx++) {
                RowLayoutIterator layout_iterator(layout, partition_idx);
                while (layout_iterator.HasNext()) {
                    DataChunk &chunk = layout_iterator.Next();
                    Vector &hash_v = chunk.data[hash_idx];
                    Insert(chunk, hash_v, partition_idx, partition_bits);
                }
            }
        }

        void Insert(DataChunk &keys, Vector &hashes_v, uint64_t partition_idx, uint8_t partition_bits) {

            auto partition_size = capacity >> partition_bits;
            auto partition_mask = partition_size - 1;
            auto partition_offset = partition_idx * partition_size;

            // perform linear probing
            auto keys_data = FlatVector::GetData<uint64_t>(keys.data[0]);
            auto hashes_data = FlatVector::GetData<uint64_t>(hashes_v);

            elements += keys.size();

            for (uint64_t i = 0; i < keys.size(); i++) {
                auto key = keys_data[i];
                auto hash = hashes_data[i];
                auto idx = (hash & partition_mask) + partition_offset;

                while (true) {
                    if (ht_array[idx] == 0) {
                        ht_array[idx] = key;
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

        double GetCollisionRate() const override {
            return static_cast<double>(collisions) / static_cast<double>(elements);
        }

        void Free() override {
            memory_manager.deallocate(ht_array);
        }
    };
}



#endif //HASH_TABLE_H
