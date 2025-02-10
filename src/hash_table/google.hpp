//
// Created by Paul on 08/02/2025.
//

#ifndef HASH_TABLE_GOOGLE
#define HASH_TABLE_GOOGLE

#include <iostream>
#include "utils.hpp"


namespace duckdb {
    class GoogleHashTable final: HashTableBase {
    public:


        data_ptr_t ht1_array;
        data_ptr_t ht2_array;
        uint64_t capacity;

        uint64_t elements = 0;
        uint64_t collisions = 0;

        uint64_t GetCapacity() const override {
            return capacity;
        }

        double GetCollisionRate() const override {
            return static_cast<double>(collisions) / static_cast<double>(elements);
        }


        MemoryManager &memory_manager;

        GoogleHashTable(uint64_t number_of_records, MemoryManager &memory_manager) : memory_manager(memory_manager) {
            capacity = next_power_of_two(2 * number_of_records);

            uint64_t ht1_size = capacity * sizeof(uint8_t);
            uint64_t ht2_size = capacity * sizeof(uint64_t);
            ht1_array = memory_manager.allocate(ht1_size + ht2_size);
            ht2_array = ht1_array + ht1_size;
        }

        void Insert(DataChunk &chunk) {
        }

        void InsertAll(RowLayout &layout, uint8_t partition_bits, uint64_t hash_idx) override {

        }

        void Free() override {
            memory_manager.deallocate(ht1_array);
        }
    };
}



#endif //HASH_TABLE_GOOGLE
