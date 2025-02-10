//
// Created by Paul on 08/02/2025.
//

#ifndef HASH_TABLE_LINEAR_PROBING_HPP
#define HASH_TABLE_LINEAR_PROBING_HPP
#include "utils.hpp"
#include "base.hpp"


namespace duckdb {
    class LPHashTable final : HashTableBase {
    public:
        data_ptr_t ht_array;
        uint64_t capacity;
        uint64_t capacity_mask;

        uint64_t elements = 0;
        uint64_t collisions = 0;

        uint64_t GetCapacity() const override {
            return capacity;
        }

        double GetCollisionRate() const override {
            return static_cast<double>(collisions) / static_cast<double>(elements);
        }

        MemoryManager &memory_manager;

        LPHashTable(uint64_t number_of_records, MemoryManager &memory_manager) : memory_manager(memory_manager) {
            capacity = next_power_of_two(2 * number_of_records);
            uint64_t ht_size = capacity * sizeof(uint64_t);
            ht_array = memory_manager.allocate(ht_size);
            std::memset(ht_array, 0, ht_size);

            capacity_mask = capacity - 1;
        }

        void InsertAll(RowLayout &layout, uint8_t partition_bits, uint64_t hash_idx) override {
            RowLayoutIterator layout_iterator(layout);
            while (layout_iterator.HasNext()) {
                DataChunk &chunk = layout_iterator.Next();
                Vector &hash_v = chunk.data[hash_idx];
                Insert(chunk, hash_v);
            }
        }

        void Insert(DataChunk &keys, Vector &hashes_v) {
            // perform linear probing
            auto keys_data = FlatVector::GetData<uint64_t>(keys.data[0]);
            auto hashes_data = FlatVector::GetData<uint64_t>(hashes_v);

            auto ht_array_atomic = reinterpret_cast<std::atomic<uint64_t> *>(ht_array);

            elements += keys.size();

            for (uint64_t i = 0; i < keys.size(); i++) {
                uint64_t key = keys_data[i];
                uint64_t hash = hashes_data[i];
                uint64_t idx = hash & capacity_mask;

                while (true) {
                    if (ht_array_atomic[idx] == 0) {
                        ht_array_atomic[idx] = key;
                        break;
                    }
                    collisions++;
                    idx = (idx + 1) & capacity_mask;
                }
            }
        }


        void Free() override {
            memory_manager.deallocate(ht_array);
        }
    };
}


#endif //HASH_TABLE_LINEAR_PROBING_HPP
