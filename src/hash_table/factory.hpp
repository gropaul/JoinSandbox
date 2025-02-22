//
// Created by Paul on 10/02/2025.
//

#ifndef FACTORY_H
#define FACTORY_H
#include "base.hpp"
#include "partitioned.hpp"
#include "compressed_partitioned.hpp"
#include "google.hpp"

namespace duckdb {
    enum HashTableType {
        LINEAR_PROBING_PARTITIONED,
        LINEAR_PROBING_PARTITIONED_COMPRESSED,
        GOOGLE
    };

    static HashTableBase *HashTableFactory(HashTableType type, uint64_t number_of_records,
                                           MemoryManager &memory_manager) {
        switch (type) {
            case LINEAR_PROBING_PARTITIONED:
                return reinterpret_cast<HashTableBase *>(new PartitionedHashTable(number_of_records, memory_manager));
            case LINEAR_PROBING_PARTITIONED_COMPRESSED:
                return reinterpret_cast<HashTableBase *>(new CompressedPartitioned(number_of_records, memory_manager));
            case GOOGLE:
                return reinterpret_cast<HashTableBase *>(new GoogleHashTable(number_of_records, memory_manager));
            default:
                throw std::runtime_error("Unknown hash table type");
        }
    }
}

#endif //FACTORY_H
