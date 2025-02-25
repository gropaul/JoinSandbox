//
// Created by Paul on 08/02/2025.
//

#ifndef HASH_TABLE_COMPRESSED_PARTITIONED
#define HASH_TABLE_COMPRESSED_PARTITIONED
#include "utils.hpp"
#include "base.hpp"
#include "partitioned.hpp"

#include <iostream>
#include <cmath>

#include "base.hpp"
#include "materialization/row_layout.hpp"


// typedef uint32_t key_t;
typedef uint64_t ht_slot_t;


namespace duckdb {


    struct Functions {
    public:

        static inline constexpr uint64_t GetSlotMask(const uint64_t bytes_per_value) {
            constexpr uint64_t one = 1;
            const uint64_t mask = (one << (bytes_per_value * 8)) - 1;
            return mask;
        }
    };

    template<idx_t bytes_per_value>
    struct CompressedConstants {
    public:

        //! Has 1s where the slot is, 0s elsewhere
        static constexpr uint64_t SLOT_MASK = Functions::GetSlotMask(bytes_per_value);

    public:

        static inline constexpr bool IsPowerOfTwo(const uint64_t value) {
            return (value & (value - 1)) == 0;
        }

        static inline constexpr uint64_t GetPowerOfTwo(const uint64_t value) {
            // we already know that the value is a power of two, we can't use a loop
            double power = log2(value);
            return static_cast<uint64_t>(power);
        }

        static inline constexpr uint64_t GetCompressedOffset(const uint64_t offset) {
            if (IsPowerOfTwo(bytes_per_value)) {
                return offset << GetPowerOfTwo(bytes_per_value);
            } else {
                const uint64_t byte_offset = offset * bytes_per_value;
                return byte_offset;
            }
        }

        static inline constexpr uint64_t ReadRawValueAtPointer(const data_ptr_t ptr) {
            const auto *ptr_uint64 = reinterpret_cast<uint64_t *>(ptr);
            return *ptr_uint64;
        }

        static inline void WriteRawValueAtPointer(const data_ptr_t ptr, const uint64_t value) {
            auto *ptr_uint64 = reinterpret_cast<uint64_t *>(ptr);
            *ptr_uint64 = value;
        }

        static inline constexpr uint64_t ReadValueAtOffset(data_ptr_t start_pointer, const uint64_t offset) {
            const uint64_t byte_offset = GetCompressedOffset(offset);
            data_ptr_t ptr = start_pointer + byte_offset;
            const auto raw_value = ReadRawValueAtPointer(ptr);
            // if (raw_value != 0) {
            //     std::cout << "ReadValueAtOffset: " << offset << " " << raw_value << '\n';
            // }
            const uint64_t value = (raw_value & SLOT_MASK);

            // if (value != 0) {
            //     std::cout << "ReadValueAtOffset: " << offset << " " << value << '\n';
            // }
            return value;
        }

        static inline void WriteValueAtOffset(data_ptr_t start_pointer, const uint64_t offset, const uint64_t value) {
            uint64_t byte_offset = GetCompressedOffset(offset);
            data_ptr_t ptr = start_pointer + byte_offset;
            const auto current_value = ReadRawValueAtPointer(ptr);

            // zero out the slot, keep the other values
            const uint64_t mask = ~SLOT_MASK;
            const auto new_value = (current_value & mask) | value;
            WriteRawValueAtPointer(ptr, new_value);
        }

        static inline bool WriteIfZero(data_ptr_t start_pointer, const uint64_t offset, const uint64_t value) {
            uint64_t byte_offset = GetCompressedOffset(offset);
            data_ptr_t ptr = start_pointer + byte_offset;
            const auto current_raw_value = ReadRawValueAtPointer(ptr);
            const uint64_t current_value = (current_raw_value & SLOT_MASK);
            if (current_value != 0) {
                return false;
            }
            const uint64_t mask = ~SLOT_MASK;
            const auto new_value = (current_raw_value & mask) | value;
            WriteRawValueAtPointer(ptr, new_value);
            return true;
        }

        static inline bool ProbeAndWrite(data_ptr_t start_pointer, uint64_t &offset, uint64_t &collisions, const uint64_t value) {
            const uint64_t byte_offset = GetCompressedOffset(offset);
            data_ptr_t ptr = start_pointer + byte_offset;

            const auto raw_value = ReadRawValueAtPointer(ptr);
            const uint64_t value_slot_0 = (raw_value & SLOT_MASK);
            if (value_slot_0 == 0) {
                // we found the slot, write the value
                const uint64_t mask = ~SLOT_MASK;
                const auto new_value = (raw_value & mask) | value;
                WriteRawValueAtPointer(ptr, new_value);
                return true;
            }

            // todo, what if we are at the end of the ht? + How do we handle multithreading?
            idx_t remaining_bytes = sizeof(uint64_t) - bytes_per_value;
            if (remaining_bytes < bytes_per_value) {
                // todo: in theory we could check if the remaining bits are 0, if not we have a collision
                // and could increment the offset right away
                return false;
            }

            offset++;
            collisions++;

            const uint64_t mask_slot_1 = (SLOT_MASK << (bytes_per_value * 8));

            const uint64_t unaligned_value_slot_1 = (raw_value & mask_slot_1);
            if (unaligned_value_slot_1 == 0) {
                const uint64_t value_shifted = value << (bytes_per_value * 8);
                const auto new_value = raw_value | value_shifted;
                WriteRawValueAtPointer(ptr, new_value);
                return true;
            } else {

                remaining_bytes -= bytes_per_value;
                if (remaining_bytes < 1) {
                    return false;
                }

                const uint64_t mask_slot_2 = mask_slot_1 << (bytes_per_value * 8);
                const uint64_t unaligned_value_slot_2 = (raw_value & mask_slot_2);

                const bool is_filled = unaligned_value_slot_2 != 0;
                offset += is_filled;
                collisions += is_filled;

                return false;

            }

        }
    };

    class CompressedPartitioned : PartitionedHashTable {
    public:
        uint64_t ht_size_bytes = 0;
        uint64_t bytes_per_value = 0;
        uint64_t value_shift;


        CompressedPartitioned(uint64_t number_of_records, MemoryManager &memory_manager) : PartitionedHashTable(
            number_of_records, memory_manager) {
        }

        void InitializeHT() override {
            double bits_float = log2(static_cast<double>(number_of_records));
            const auto bits_per_value = static_cast<uint64_t>(floor(bits_float));
            bytes_per_value = (bits_per_value + 7) / 8;

            // when we read a value from the ht, we will read it as uint64_t
            // if we e.g. encode one slot as only 3 bytes, when reading we will read 8 bytes
            // and then shift the value to the right
            // when writing we will write the value to the right position and then shift it to the left
            // then we will read the current value, mask where it needs to be written and then write the new value
            // using an OR operation
            const uint64_t value_shift_bytes = sizeof(uint64_t) - bytes_per_value;
            value_shift = value_shift_bytes * 8;

            // used to zero out the existing value to then or the new value, consits
            // of 0s in the left value bytes and 1s in the right value bytes

            // add padding so that we don't overflow when accessing the last slot
            ht_size_bytes = capacity * bytes_per_value + sizeof(uint64_t);

            allocation = memory_manager.allocate(ht_size_bytes);
            ht = reinterpret_cast<ht_slot_t *>(allocation);
            std::memset(allocation, 0, ht_size_bytes);
        }


        void Insert(Vector &hashes_v, IteratorStep &state, uint64_t partition_idx, uint8_t partition_bits) override {
            switch (bytes_per_value) {
                case 1:
                    InsertInternal<1>(hashes_v, state, partition_idx, partition_bits);
                    break;
                case 2:
                    InsertInternal<2>(hashes_v, state, partition_idx, partition_bits);
                    break;
                case 3:
                    InsertInternal<3>(hashes_v, state, partition_idx, partition_bits);
                    break;
                case 4:
                    InsertInternal<4>(hashes_v, state, partition_idx, partition_bits);
                    break;
                case 5:
                    InsertInternal<5>(hashes_v, state, partition_idx, partition_bits);
                    break;
                case 6:
                    InsertInternal<6>(hashes_v, state, partition_idx, partition_bits);
                    break;
                case 7:
                    InsertInternal<7>(hashes_v, state, partition_idx, partition_bits);
                    break;
                default:
                    throw std::runtime_error("Unsupported bytes per value");
            }
        }

        template<idx_t bytes_per_value>
        void InsertInternal(Vector &hashes_v, IteratorStep &state, uint64_t partition_idx, uint8_t partition_bits) {
            const auto partition_size = capacity >> partition_bits;
            const auto partition_mask = partition_size - 1;
            const auto partition_offset = partition_idx * partition_size;

            // perform linear probing
            idx_t count = state.partition_step.count;
            auto hashes_data = FlatVector::GetData<uint64_t>(hashes_v);
            auto row_pointer_data = FlatVector::GetData<data_ptr_t>(state.partition_step.row_pointer);

            data_ptr_t start_pointer = allocation;
            using Constants = CompressedConstants<bytes_per_value>;

            for (uint64_t i = 0; i < count; i++) {
                auto mask = Constants::SLOT_MASK;
                auto row_offset = cast_pointer_to_uint64(row_pointer_data[i]) & mask;
                // auto row_offset = elements + i + 1; // don't start at 0, this is used to check if the slot is empty

                const auto hash = hashes_data[i];
                auto offset = (hash & partition_mask) + partition_offset;

                while (true) {
                    // if (Constants::ProbeAndWrite(start_pointer, offset, collisions, row_offset)) {
                    if (DUCKDB_LIKELY(Constants::ProbeAndWrite(start_pointer, offset, collisions, row_offset))) {
                        break;
                    }
                    collisions++;
                    offset = (offset + 1) & capacity_mask;
                }
            }

            elements += count;

            // std::cout << "Partition=" << partition_idx << " Min=" << min_idx << " Max=" << max_idx << '\n';
        }

        uint64_t GetHTSize(uint64_t n_partitions) const override {
            return ht_size_bytes / n_partitions;
        }

        void Print() const override {
            switch (bytes_per_value) {
                case 1:
                    PrintInternal<1>();
                    break;
                case 2:
                    PrintInternal<2>();
                    break;
                case 3:
                    PrintInternal<3>();
                    break;
                case 4:
                    PrintInternal<4>();
                    break;
                case 5:
                    PrintInternal<5>();
                    break;
                case 6:
                    PrintInternal<6>();
                    break;
                case 7:
                    PrintInternal<7>();
                    break;
                default:
                    throw std::runtime_error("Unsupported bytes per value");
            }
        }

        template<idx_t bytes_per_value>
        void PrintInternal() const {
            data_ptr_t start_pointer = allocation;
            using Constants = CompressedConstants<bytes_per_value>;

            std::cout << "\nCapacity=" << capacity << " Elements=" << elements << " Collisions=" << collisions << '\n';

            for (uint64_t i = 0; i < capacity; i++) {
                const auto value = Constants::ReadValueAtOffset(start_pointer, i);
                std::cout << "ht[" << i << "]=" << value << '\n';
            }
        }
    };
}


#endif //HASH_TABLE_H
