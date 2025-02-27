//
// Created by Paul on 09/02/2025.
//

#ifndef SCATTER_H
#define SCATTER_H

#endif //SCATTER_H

#include "duckdb.hpp"

// generic function to scatter a chunk of data into a partition
namespace duckdb {
    typedef void (*scatter_function_t)(const Vector &source, const SelectionVector &sel, const idx_t count,
                                       const idx_t jump_offset, data_ptr_t target);

    template<typename DATA_TYPE>
    void Scatter(const Vector &source, const SelectionVector &sel, const idx_t count,
                 const idx_t jump_offset, data_ptr_t partition_data) {
        auto source_data = FlatVector::GetData<DATA_TYPE>(source);
        for (idx_t i = 0; i < count; i++) {
            auto source_idx = sel.get_index(i);
            auto source_value = source_data[source_idx];
            Store<DATA_TYPE>(source_value, partition_data + jump_offset * i);
        }
    }

    typedef void (*gather_function_t)(const Vector &row_pointers, const SelectionVector &sel, const idx_t count,
                                  const idx_t column_offset, Vector &target);

    template<typename DATA_TYPE>
    void Gather(const Vector &row_pointers_v, const SelectionVector &sel, const idx_t count,
                const idx_t column_offset, Vector &target) {
        auto row_pointers = FlatVector::GetData<data_ptr_t>(row_pointers_v);
        auto target_data = FlatVector::GetData<DATA_TYPE>(target);

        for (idx_t i = 0; i < count; i++) {
            idx_t sel_idx = sel.get_index(i);
            auto row_ptr = row_pointers[sel_idx];
            auto value_ptr = row_ptr + column_offset;
            auto value = Load<DATA_TYPE>(value_ptr);
            target_data[i] = value;
        }
    }

    typedef idx_t (*equality_function_t)(const Vector &left, const Vector &row_pointers, const SelectionVector &sel,
                                    const idx_t count, const idx_t column_offset,
                                    SelectionVector &result);
    template <typename DATA_TYPE>
    idx_t Equal(const Vector &left, const Vector &row_pointers, const SelectionVector &sel,
                const idx_t count, const idx_t column_offset, SelectionVector &result_sel) {
        // Obtain pointers to the actual data in 'left' and the row pointers
        auto left_data = FlatVector::GetData<DATA_TYPE>(left);
        auto row_ptrs  = FlatVector::GetData<data_ptr_t>(row_pointers);

        idx_t match_count = 0;
        for (idx_t i = 0; i < count; i++) {
            // Index in the original data
            auto source_idx = sel.get_index(i);

            // Address of the row data in the partition
            auto row_ptr   = row_ptrs[source_idx];
            auto value_ptr = row_ptr + column_offset;

            // Load the value from that row
            auto stored_val = Load<DATA_TYPE>(value_ptr);

            // Compare against the element in 'left' at source_idx
            if (left_data[source_idx] == stored_val) {
                // Write the matching index to the result selection vector
                result_sel.set_index(match_count, source_idx);
                match_count++;
            }
        }
        // Return how many matches were found
        return match_count;
    }

    static scatter_function_t GetScatterFunction(const LogicalType &type) {
        switch (type.id()) {
            case LogicalTypeId::BIGINT:
                return Scatter<int64_t>;
            case LogicalTypeId::UBIGINT:
                return Scatter<uint64_t>;
            case LogicalTypeId::INTEGER:
                return Scatter<int32_t>;
            case LogicalTypeId::UINTEGER:
                return Scatter<uint32_t>;
            case LogicalTypeId::SMALLINT:
                return Scatter<int16_t>;
            case LogicalTypeId::USMALLINT:
                return Scatter<uint16_t>;
            case LogicalTypeId::TINYINT:
                return Scatter<int8_t>;
            case LogicalTypeId::UTINYINT:
                return Scatter<uint8_t>;
            case LogicalTypeId::FLOAT:
                return Scatter<float>;
            case LogicalTypeId::DOUBLE:
                return Scatter<double>;
            default:
                throw NotImplementedException("Scatter function not implemented for type: " + type.ToString());
        }
    }

    static gather_function_t GetGatherFunction(const LogicalType &type) {
        switch (type.id()) {
            case LogicalTypeId::BIGINT:
                return Gather<int64_t>;
            case LogicalTypeId::UBIGINT:
                return Gather<uint64_t>;
            case LogicalTypeId::INTEGER:
                return Gather<int32_t>;
            case LogicalTypeId::UINTEGER:
                return Gather<uint32_t>;
            case LogicalTypeId::SMALLINT:
                return Gather<int16_t>;
            case LogicalTypeId::USMALLINT:
                return Gather<uint16_t>;
            case LogicalTypeId::TINYINT:
                return Gather<int8_t>;
            case LogicalTypeId::UTINYINT:
                return Gather<uint8_t>;
            case LogicalTypeId::FLOAT:
                return Gather<float>;
            case LogicalTypeId::DOUBLE:
                return Gather<double>;
            default:
                throw NotImplementedException("Gather function not implemented for type");
        }
    }

    static equality_function_t GetEqualityFunction(const LogicalType &type) {
        switch (type.id()) {
            case LogicalTypeId::BIGINT:
                return Equal<int64_t>;
            case LogicalTypeId::UBIGINT:
                return Equal<uint64_t>;
            case LogicalTypeId::INTEGER:
                return Equal<int32_t>;
            case LogicalTypeId::UINTEGER:
                return Equal<uint32_t>;
            case LogicalTypeId::SMALLINT:
                return Equal<int16_t>;
            case LogicalTypeId::USMALLINT:
                return Equal<uint16_t>;
            case LogicalTypeId::TINYINT:
                return Equal<int8_t>;
            case LogicalTypeId::UTINYINT:
                return Equal<uint8_t>;
            case LogicalTypeId::FLOAT:
                return Equal<float>;
            case LogicalTypeId::DOUBLE:
                return Equal<double>;
            default:
                throw NotImplementedException("Equality function not implemented for type");
        }
    }
};
