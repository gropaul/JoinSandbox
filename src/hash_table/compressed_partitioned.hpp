//
// Created by Paul on 08/02/2025.
//

#ifndef HASH_TABLE_COMPRESSED_PARTITIONED
#define HASH_TABLE_COMPRESSED_PARTITIONED
#include "utils.hpp"
#include "base.hpp"
#include "partitioned.hpp"

#include <iostream>

#include "base.hpp"
#include "materialization/row_layout.hpp"


// typedef uint32_t key_t;
typedef uint64_t ht_slot_t;

namespace duckdb {
    class CompressedPartitioned : PartitionedHashTable {
    public:

        uint64_t ht_size_bytes = 0;
        uint64_t bits_per_value = 0;

        uint64_t GetCompressedOffset(const uint64_t offset) const {
            uint64_t bits_offset = offset * bits_per_value;
        }

        CompressedPartitioned(uint64_t number_of_records, MemoryManager &memory_manager) : PartitionedHashTable(number_of_records, memory_manager) {
        }

        void InitializeHT() override {

            double bits_float = log2(static_cast<double>(number_of_records));
            bits_per_value = static_cast<uint64_t>(floor(bits_float));

            uint64_t ht_size_bits = capacity * bits_per_value;
            ht_size_bytes = (ht_size_bits + 7) / 8;

            allocation = memory_manager.allocate(ht_size_bytes);
            ht = reinterpret_cast<ht_slot_t*>(allocation);
            std::memset(allocation, 0, ht_size_bytes);
        }

        void Insert(Vector &hashes_v, IteratorStep &state, uint64_t partition_idx, uint8_t partition_bits) override {

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

        uint64_t GetHTSize(uint64_t n_partitions) const override {
            return ht_size_bytes / n_partitions;
        }
    };
}



#endif //HASH_TABLE_H
