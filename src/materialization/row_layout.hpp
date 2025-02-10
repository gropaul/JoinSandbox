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
        MemoryManager &memory_manager;

        explicit RowLayoutPartition(idx_t radix,
                                    const vector<scatter_function_t> &scatter_functions,
                                    const vector<gather_function_t> &gather_functions,
                                    const RowLayoutFormat &format, MemoryManager &memory_manager)
            : radix(radix), row_count(0), current_write_offset(0), format(format),
              scatter_functions(scatter_functions), gather_functions(gather_functions), memory_manager(memory_manager) {
            data = memory_manager.allocate(PARTITION_SIZE);
        }

        bool CanFit(const DataChunk &chunk) const {
            auto chunk_count = chunk.size();
            auto chunk_size = chunk_count * format.size;
            return current_write_offset + chunk_size <= PARTITION_SIZE;
        }

        bool CanSink(const DataChunk &chunk) const {
            return CanFit(chunk);
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
                data_ptr_t start = data + format.offsets[col_idx];
                gather_function(target, *FlatVector::IncrementalSelectionVector(), row_count, format.size, start);
                target.Print(row_count);
            }
        }
    };

    class RowLayoutPartitionIterator {
    private:
        // Store a reference to an existing partition instead of a unique pointer
        RowLayoutPartition &layout;
        idx_t partition_idx;
        DataChunk chunk;

    public:
        // Take the partition by reference
        explicit RowLayoutPartitionIterator(RowLayoutPartition &layout_p)
            : layout(layout_p), partition_idx(0) {
            const vector<LogicalType> &types = layout.format.types;
            chunk.Initialize(Allocator::DefaultAllocator(), types);
        }

        void Reset() {
            partition_idx = 0;
        }

        bool HasNext() const {
            return partition_idx < layout.row_count;
        }

        DataChunk &Next() {
            idx_t next_count = layout.row_count - partition_idx;
            if (next_count > STANDARD_VECTOR_SIZE) {
                next_count = STANDARD_VECTOR_SIZE;
            }

            const idx_t row_width = layout.format.size;
            for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
                auto &vector = chunk.data[i];
                auto &gather_function = layout.gather_functions[i];
                data_ptr_t start = layout.data + layout.format.offsets[i] + partition_idx * row_width;

                // Gather into the vector
                gather_function(vector, *FlatVector::IncrementalSelectionVector(), next_count, row_width, start);
            }

            partition_idx += next_count;
            chunk.SetCardinality(next_count);

            return chunk;
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
            }

            for (idx_t radix = 0; radix < (1 << partition_bits); radix++) {
                partition_copy_count.emplace_back(0);
                partitions_copy_sel.emplace_back(STANDARD_VECTOR_SIZE);
                vector<RowLayoutPartition> partition_chain;
                partition_chain.emplace_back(radix, scatter_functions, gather_functions, format, memory_manager);
                partion_chains.emplace_back(partition_chain);
            }
        }

        idx_t partition_bits;
        idx_t row_count = 0;

        vector<column_t> key_columns;
        RowLayoutFormat format;
        vector<vector<RowLayoutPartition> > partion_chains;
        vector<scatter_function_t> scatter_functions;
        vector<gather_function_t> gather_functions;

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
                    partition_idx = (hash >> (sizeof(uint64_t) * 8 - partition_bits));
                }
                auto &copy_sel = partitions_copy_sel[partition_idx];
                auto &copy_count = partition_copy_count[partition_idx];
                copy_sel.set_index(copy_count, i);
                copy_count++;
            }

            // sink the data into the partitions and reset the copy count
            for (idx_t i = 0; i < (1 << partition_bits); i++) {
                // get the last partition
                auto &chain = partion_chains[i];
                auto &last_partition = chain[chain.size() - 1];
                if (last_partition.CanSink(chunk)) {
                    last_partition.Sink(chunk, hash_v, partitions_copy_sel[i], partition_copy_count[i]);
                } else {
                    chain.emplace_back(i, scatter_functions, gather_functions, format, memory_manager);
                    auto &new_partition = chain[chain.size() - 1];
                    new_partition.Sink(chunk, hash_v, partitions_copy_sel[i], partition_copy_count[i]);
                }
                partition_copy_count[i] = 0;
            }
        }

        void Free() const {
            for (auto &partition_chain: partion_chains) {
                for (auto &partition: partition_chain) {
                    partition.Free();
                }
            }
        }

        void Print() const {
            idx_t partition_idx = 0;
            for (const auto &partition_chain: partion_chains) {
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
        DataChunk chunk;
        bool iterate_one_chain = false;

        idx_t chain_idx;
        idx_t chain_partition_idx;

        unique_ptr<RowLayoutPartitionIterator> partition_iterator;

    public:
        explicit RowLayoutIterator(RowLayout &layout_p)
            : layout(layout_p), chain_idx(0), chain_partition_idx(0) {
            const vector<LogicalType> &types = layout.format.types;
            chunk.Initialize(Allocator::DefaultAllocator(), types);

            iterate_one_chain = false;
            // Create an iterator for the first partition in chain 0
            partition_iterator = make_uniq<RowLayoutPartitionIterator>(
                layout.partion_chains[chain_idx][chain_partition_idx]
            );
        }

        RowLayoutIterator(RowLayout &layout_p, idx_t chain_idx_p)
            : layout(layout_p), chain_idx(chain_idx_p), chain_partition_idx(0) {
            const vector<LogicalType> &types = layout.format.types;
            chunk.Initialize(Allocator::DefaultAllocator(), types);

            iterate_one_chain = true;
            // Create an iterator for the first partition in the specified chain
            partition_iterator = make_uniq<RowLayoutPartitionIterator>(
                layout.partion_chains[chain_idx][chain_partition_idx]
            );
        }

        bool HasNext() const {
            if (iterate_one_chain) {
                auto current_chain_size = layout.partion_chains[chain_idx].size();
                // If at last partition of the chain, just check if there's more data
                if (chain_partition_idx == current_chain_size - 1) {
                    return partition_iterator->HasNext();
                }
            } else {
                // If at the last chain and last partition, check if there's more data
                if (chain_idx == layout.partion_chains.size() - 1 &&
                    chain_partition_idx == layout.partion_chains[chain_idx].size() - 1) {
                    return partition_iterator->HasNext();
                }
            }
            // Otherwise, yes, there's more
            return true;
        }

        DataChunk &Next() {
            if (partition_iterator->HasNext()) {
                // Still data left in current partition
                return partition_iterator->Next();
            }
            // Move to next partition
            chain_partition_idx++;
            if (iterate_one_chain) {
                // Single chain mode
                if (chain_partition_idx < layout.partion_chains[chain_idx].size()) {
                    partition_iterator = make_uniq<RowLayoutPartitionIterator>(
                        layout.partion_chains[chain_idx][chain_partition_idx]
                    );
                    return partition_iterator->Next();
                }
            } else {
                // Multi-chain mode
                if (chain_partition_idx >= layout.partion_chains[chain_idx].size()) {
                    chain_idx++;
                    chain_partition_idx = 0;
                }
                if (chain_idx < layout.partion_chains.size()) {
                    partition_iterator = make_uniq<RowLayoutPartitionIterator>(
                        layout.partion_chains[chain_idx][chain_partition_idx]
                    );
                    return partition_iterator->Next();
                }
            }
            // If no more partitions/chains
            throw std::out_of_range("No more elements in RowLayoutIterator");
        }
    };
};


#endif //ROW_LAYOUT_H
