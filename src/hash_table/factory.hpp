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

    static string HTTypeToString(HashTableType type) {
        switch (type) {
            case LINEAR_PROBING_PARTITIONED:
                return "PARTITIONED";
            case LINEAR_PROBING_PARTITIONED_COMPRESSED:
                return "PARTITIONED_COMPRESSED";
            case GOOGLE:
                return "GOOGLE";
            default:
                throw std::runtime_error("Unknown hash table type");
        }
    }

    static HashTableBase *HashTableFactory(HashTableType type, uint64_t number_of_records,
                                           MemoryManager &memory_manager, const vector<column_t> &keys) {
        switch (type) {
            case LINEAR_PROBING_PARTITIONED:
                return reinterpret_cast<HashTableBase *>(new PartitionedHashTable(number_of_records, memory_manager, keys));
            case LINEAR_PROBING_PARTITIONED_COMPRESSED:
                return reinterpret_cast<HashTableBase *>(new CompressedPartitioned(number_of_records, memory_manager, keys));
            case GOOGLE:
                return reinterpret_cast<HashTableBase *>(new GoogleHashTable(number_of_records, memory_manager));
            default:
                throw std::runtime_error("Unknown hash table type");
        }
    }
}

#endif //FACTORY_H
