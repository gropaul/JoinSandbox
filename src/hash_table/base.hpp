//
// Created by Paul on 08/02/2025.
//

#ifndef HASH_TABLE_BASE
#define HASH_TABLE_BASE

namespace duckdb {

    class HashTableBase  {
    public:
        virtual ~HashTableBase() = default;

        virtual void InsertAll(RowLayout &layout, uint8_t partition_bits, uint64_t hash_idx) {
            throw std::runtime_error("Not implemented");
        }

        virtual double GetCollisionRate() const {
            throw std::runtime_error("Not implemented");
        }

        virtual uint64_t GetCapacity() const {
            throw std::runtime_error("Not implemented");

        }

        virtual void Free() {
            throw std::runtime_error("Not implemented");
        }
    };
}


#endif //HASH_TABLE_BASE
