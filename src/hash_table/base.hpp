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

        virtual void InitializeHT() {
            throw std::runtime_error("Not implemented");
        }

        //! Size of the hash table in bytes
        virtual uint64_t GetHTSize(uint64_t n_partitions) const {
            throw std::runtime_error("Not implemented");
        }

        virtual void Free() {
            throw std::runtime_error("Not implemented");
        }
    };
}


#endif //HASH_TABLE_BASE
