//
// Created by Paul on 08/02/2025.
//

#ifndef ROW_LAYOUT_H
#define ROW_LAYOUT_H
#include <iostream>
#include <utility>

#include "duckdb.hpp"
#include "memory_manager.hpp"
#include "scatter.hpp"


namespace duckdb {
    // 4mb partition size
    constexpr idx_t PARTITION_SIZE = 4 * 1024 * 1024;

    //! Returns the size of the data type in bytes
    static idx_t GetSize(const duckdb::LogicalType &type) {
        const auto size = GetTypeIdSize(type.InternalType());
        return size;
    }

    struct RowLayoutFormat {
        explicit RowLayoutFormat(const vector<LogicalType> &types) : types(types) {
            size = 0;
            for (const auto &type: types) {
                offsets.push_back(size);
                size += GetSize(type);
            }
        }

        vector<idx_t> offsets;
        const vector<LogicalType> &types;
        idx_t size;
    };

    struct PartitionIteratorStep {
        Vector row_pointer = Vector(LogicalType::POINTER);
        idx_t start_offset;
        idx_t count;
    };

    struct SegmentPosition {
        //! The current partition index
        idx_t partition_idx;
        //! The current idx of the segment within the partition
        idx_t partition_segment_idx;
    };

    struct IteratorStep {
        SegmentPosition segment_position;
        //! The state within the partition
        PartitionIteratorStep partition_step;
    };


    class RowLayoutPartition {
    public:
        idx_t radix;
        idx_t row_count;

        const RowLayoutFormat &format;

        //! In bytes
        idx_t current_write_offset;
        data_ptr_t data;

        const vector<scatter_function_t> &scatter_functions;
        const vector<gather_function_t> &gather_functions;
        const vector<vector_equality_function_t> &equality_functions_v;
        MemoryManager &memory_manager;

        explicit RowLayoutPartition(const idx_t radix,
                                    const vector<scatter_function_t> &scatter_functions,
                                    const vector<gather_function_t> &gather_functions,
                                    const vector<vector_equality_function_t> &equality_functions,
                                    const RowLayoutFormat &format, MemoryManager &memory_manager)
            : RowLayoutPartition(radix, scatter_functions, gather_functions, equality_functions, format, memory_manager, PARTITION_SIZE) {
        }

        explicit RowLayoutPartition(const idx_t radix,
                                    const vector<scatter_function_t> &scatter_functions,
                                    const vector<gather_function_t> &gather_functions,
                                    const vector<vector_equality_function_t> &equality_functions,
                                    const RowLayoutFormat &format, MemoryManager &memory_manager,
                                    const idx_t allocation_size)
            : radix(radix), row_count(0), current_write_offset(0), format(format),
              scatter_functions(scatter_functions), gather_functions(gather_functions), equality_functions_v(equality_functions), memory_manager(memory_manager) {
            data = memory_manager.allocate(allocation_size);
        }

        bool CanFit(const DataChunk &chunk) const {
            const auto chunk_count = chunk.size();
            const auto chunk_size = chunk_count * format.size;
            return current_write_offset + chunk_size <= PARTITION_SIZE;
        }

        bool CanSink(const DataChunk &chunk) const {
            return CanFit(chunk);
        }

        void CopyTo(const data_ptr_t start, const idx_t copy_count, const data_ptr_t target) const {
            const auto copy_size = copy_count * format.size;
            std::memcpy(target, start, copy_size);
        }

        void Sink(const DataChunk &chunk, const Vector &hashes_v, const SelectionVector &sel, const idx_t count) {
            const idx_t row_width = format.size;

            for (idx_t i = 0; i < chunk.ColumnCount() + 1; i++) {
                auto &vector = i == chunk.ColumnCount() ? hashes_v : chunk.data[i];
                data_ptr_t target = data + current_write_offset + format.offsets[i];
                auto &scatter_function = scatter_functions[i];
                scatter_function(vector, sel, count, row_width, target);
            }

            const auto written_bytes = row_width * count;
            current_write_offset += written_bytes;
            row_count += count;
        }

        void Free() const {
            memory_manager.deallocate(data);
        }

        void Print() const {
            std::cerr << "Partition " << radix << " has " << row_count << " rows" << '\n';
            column_t col_count = format.types.size();
            for (column_t col_idx = 0; col_idx < col_count; col_idx++) {
                std::cerr << "Column " << col_idx << ": ";
                auto &gather_function = gather_functions[col_idx];
                Vector target(format.types[col_idx], row_count);
                Vector row_pointers_v(LogicalType::POINTER, row_count);
                auto row_pointers = FlatVector::GetData<data_ptr_t>(row_pointers_v);
                for (idx_t i = 0; i < row_count; i++) {
                    row_pointers[i] = data + i * format.size;
                }

                gather_function(row_pointers_v, *FlatVector::IncrementalSelectionVector(), row_count,
                                format.offsets[col_idx], target);
                target.Print(row_count);
            }
        }
    };

    class RowLayoutPartitionIterator {
    private:
        // Store a reference to an existing partition instead of a unique pointer
        RowLayoutPartition &layout;
        idx_t current_row_offset;

    public:
        // Take the partition by reference
        explicit RowLayoutPartitionIterator(RowLayoutPartition &layout_p)
            : layout(layout_p), current_row_offset(0) {
            const vector<LogicalType> &types = layout.format.types;
        }

        void Reset() {
            current_row_offset = 0;
        }

        bool HasNext() const {
            return current_row_offset < layout.row_count;
        }

        bool Next(PartitionIteratorStep &state) {
            if (!HasNext()) {
                return false;
            }

            const idx_t row_width = layout.format.size;
            data_ptr_t start_ptr = layout.data + current_row_offset * row_width;
            idx_t next_count = layout.row_count - current_row_offset;

            if (next_count > STANDARD_VECTOR_SIZE) {
                next_count = STANDARD_VECTOR_SIZE;
            }

            state.count = next_count;
            state.start_offset = current_row_offset;

            const auto row_pointers = FlatVector::GetData<data_ptr_t>(state.row_pointer);
            for (idx_t i = 0; i < next_count; i++) {
                row_pointers[i] = start_ptr + i * row_width;
            }
            current_row_offset += next_count;
            return true;
        }
    };

    class RowLayout {
    public:
        explicit RowLayout(const vector<LogicalType> &types, vector<column_t> key_columns, uint8_t partition_bits,
                           MemoryManager &memory_manager)
            : format(types), memory_manager(memory_manager), hash_v(LogicalType::HASH),
              key_columns(std::move(key_columns)), partition_bits(partition_bits) {
            // the last type is the hash
            D_ASSERT(types[types.size() - 1] == LogicalType::HASH);

            for (auto &type: types) {
                scatter_functions.push_back(GetScatterFunction(type));
                gather_functions.push_back(GetGatherFunction(type));
                equality_functions.push_back(GetEqualityFunction(type));
            }

            for (idx_t radix = 0; radix < (1 << partition_bits); radix++) {
                partition_copy_count.emplace_back(0);
                partitions_copy_sel.emplace_back(STANDARD_VECTOR_SIZE);
                vector<RowLayoutPartition> partition_chain;
                partition_chain.emplace_back(radix, scatter_functions, gather_functions, equality_functions, format, memory_manager);
                partition_chains.emplace_back(partition_chain);
            }
        }

        idx_t partition_bits;
        idx_t row_count = 0;

        vector<column_t> key_columns;
        RowLayoutFormat format;
        vector<vector<RowLayoutPartition> > partition_chains;
        vector<scatter_function_t> scatter_functions;
        vector<gather_function_t> gather_functions;
        vector<vector_equality_function_t> equality_functions;

        Vector hash_v;
        vector<uint64_t> partition_copy_count;
        vector<SelectionVector> partitions_copy_sel;

        MemoryManager &memory_manager;

    public:
        void Append(DataChunk &chunk) {
            const idx_t count = chunk.size();
            row_count += count;

            // create the hash based on the key columns
            const idx_t keys_count = key_columns.size();
            auto &key = chunk.data[key_columns[0]];
            D_ASSERT(key.GetType() == LogicalType::UBIGINT);
            VectorOperations::Hash(key, hash_v, count);

            for (idx_t i = 1; i < keys_count; i++) {
                auto &combined_key = chunk.data[key_columns[i]];
                VectorOperations::CombineHash(hash_v, combined_key, count);
            }

            const auto hashes = FlatVector::GetData<hash_t>(hash_v);
            for (idx_t i = 0; i < count; i++) {
                auto hash = hashes[i];
                // use the top partition_bits to determine the partition
                uint64_t partition_idx;
                if (partition_bits == 0) {
                    partition_idx = 0;
                } else {
                    // take the upper n bits of the hash
                    partition_idx = hash >> (64 - partition_bits);
                }
                auto &copy_sel = partitions_copy_sel[partition_idx];
                auto &copy_count = partition_copy_count[partition_idx];
                copy_sel.set_index(copy_count, i);
                copy_count++;
            }

            // sink the data into the partitions and reset the copy count
            for (idx_t i = 0; i < (1 << partition_bits); i++) {
                // get the last partition
                auto &chain = partition_chains[i];
                auto &last_partition = chain[chain.size() - 1];
                if (last_partition.CanSink(chunk)) {
                    last_partition.Sink(chunk, hash_v, partitions_copy_sel[i], partition_copy_count[i]);
                } else {
                    chain.emplace_back(i, scatter_functions, gather_functions, equality_functions, format, memory_manager);
                    auto &new_partition = chain[chain.size() - 1];
                    new_partition.Sink(chunk, hash_v, partitions_copy_sel[i], partition_copy_count[i]);
                }
                partition_copy_count[i] = 0;
            }
        }

        void Gather(Vector &row_pointers, const column_t col_idx, const idx_t count, Vector &result) {
            D_ASSERT(row_pointers.GetType() == LogicalType::POINTER);
            const auto col_type = format.types[col_idx];
            D_ASSERT(result.GetType() == col_type);

            const auto gather_function = gather_functions[col_idx];
            gather_function(row_pointers, *FlatVector::IncrementalSelectionVector(), count, format.offsets[col_idx],
                            result);
        }

        void Free() const {
            for (auto &partition_chain: partition_chains) {
                for (auto &partition: partition_chain) {
                    partition.Free();
                }
            }
        }

        void Print() const {
            idx_t partition_idx = 0;
            for (const auto &partition_chain: partition_chains) {
                std::cerr << "Partition Chain " << partition_idx << '\n';
                for (const auto &partition: partition_chain) {
                    partition.Print();
                }
                partition_idx++;
            }
        }
    };

    class RowLayoutIterator {
    private:
        // Use a non-const reference here so we can pass partitions as non-const
        RowLayout &layout;
        bool iterate_one_chain = false;

        idx_t partition_idx;
        idx_t partition_segment_idx;

        unique_ptr<RowLayoutPartitionIterator> partition_iterator;

    public:
        explicit RowLayoutIterator(RowLayout &layout_p)
            : layout(layout_p), partition_idx(0), partition_segment_idx(0) {
            const vector<LogicalType> &types = layout.format.types;

            iterate_one_chain = false;
            // Create an iterator for the first partition in chain 0
            partition_iterator = make_uniq<RowLayoutPartitionIterator>(
                layout.partition_chains[partition_idx][partition_segment_idx]
            );
        }

        RowLayoutIterator(RowLayout &layout_p, idx_t chain_idx_p)
            : layout(layout_p), partition_idx(chain_idx_p), partition_segment_idx(0) {
            const vector<LogicalType> &types = layout.format.types;

            iterate_one_chain = true;
            // Create an iterator for the first partition in the specified chain
            partition_iterator = make_uniq<RowLayoutPartitionIterator>(
                layout.partition_chains[partition_idx][partition_segment_idx]
            );
        }

        bool HasNext() const {
            if (iterate_one_chain) {
                const auto current_chain_size = layout.partition_chains[partition_idx].size();
                // If at last partition of the chain, just check if there's more data
                if (partition_segment_idx == current_chain_size - 1) {
                    return partition_iterator->HasNext();
                }
                // if we overflow the chain, there's no more data
                return partition_idx < layout.partition_chains.size();
            } else {
                // If at the last chain and last partition, check if there's more data
                if (partition_idx == layout.partition_chains.size() - 1 &&
                    partition_segment_idx == layout.partition_chains[partition_idx].size() - 1) {
                    return partition_iterator->HasNext();
                }
            }
            // Otherwise, yes, there's more
            return true;
        }

        bool Next(IteratorStep &step) {
            if (!HasNext()) {
                return false;
            }

            if (partition_iterator->HasNext()) {
                // Still data left in current partition
                partition_iterator->Next(step.partition_step);
                return true;
            }
            // Move to next partition
            partition_segment_idx++;
            if (iterate_one_chain) {
                // Single chain mode
                if (partition_segment_idx < layout.partition_chains[partition_idx].size()) {
                    partition_iterator = make_uniq<RowLayoutPartitionIterator>(
                        layout.partition_chains[partition_idx][partition_segment_idx]
                    );
                    partition_iterator->Next(step.partition_step);
                }
            } else {
                // Multi-chain mode
                if (partition_segment_idx >= layout.partition_chains[partition_idx].size()) {
                    partition_idx++;
                    partition_segment_idx = 0;
                }
                if (partition_idx < layout.partition_chains.size()) {
                    partition_iterator = make_uniq<RowLayoutPartitionIterator>(
                        layout.partition_chains[partition_idx][partition_segment_idx]
                    );
                    partition_iterator->Next(step.partition_step);
                }
            }
            step.segment_position.partition_idx = partition_idx;
            step.segment_position.partition_segment_idx = partition_segment_idx;
            return true;
        }
    };

};


#endif //ROW_LAYOUT_H
