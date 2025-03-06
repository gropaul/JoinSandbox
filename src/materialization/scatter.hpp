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

    typedef idx_t (*vector_equality_function_t)(const Vector &left, const Vector &row_pointers, const SelectionVector &sel,
                                    const idx_t count, const idx_t column_offset,
                                    SelectionVector &equal, SelectionVector &un_equal);

    template <typename DATA_TYPE>
    idx_t VectorRowEqual(const Vector &left, const Vector &row_pointers, const SelectionVector &sel,
                const idx_t count, const idx_t column_offset, SelectionVector &equal, SelectionVector &un_equal) {
        // Obtain pointers to the actual data in 'left' and the row pointers
        auto __restrict left_data = FlatVector::GetData<DATA_TYPE>(left);
        auto __restrict row_ptrs  = FlatVector::GetData<data_ptr_t>(row_pointers);

        idx_t match_count = 0;
        for (idx_t i = 0; i < count; i++) {
            // Index in the original data
            auto source_idx = sel.get_index(i);

            // Address of the row data in the partition
            auto row_ptr   = row_ptrs[source_idx];
            auto value_ptr = row_ptr + column_offset;

            // Load the value from that row
            const auto stored_val = Load<DATA_TYPE>(value_ptr);
            const auto lhs_val    = left_data[source_idx];
            // Compare against the element in 'left' at source_idx
            if (lhs_val == stored_val) {
                // Write the matching index to the result selection vector
                equal.set_index(match_count, source_idx);
                match_count++;
            } else {
                // Write the non-matching index to the result selection vector
                un_equal.set_index(i - match_count, source_idx);
            }
        }
        // Return how many matches were found
        return match_count;
    }

    typedef idx_t (*compressed_vector_equality_function_t)(const Vector &left, const Vector &row_pointers, const SelectionVector &sel,
                                   const idx_t count, const idx_t column_offset,
                                   SelectionVector &equal, SelectionVector &un_equal);

    template <typename DATA_TYPE, int COMPRESSED_WIDTH>
    idx_t CompressedVectorRowEqual(const Vector &left, const Vector &row_pointers, const SelectionVector &sel,
             const idx_t count, const idx_t column_offset, SelectionVector &equal, SelectionVector &un_equal) {

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

            const auto lhs_ptr    = &left_data[source_idx];
            // Compare against the element in 'left' at source_idx, but only the compressed width of the value
            if (memcmp(lhs_ptr, value_ptr, COMPRESSED_WIDTH) == 0) {
                // Write the matching index to the result selection vector
                equal.set_index(match_count, source_idx);
                match_count++;
            } else {
                // Write the non-matching index to the result selection vector
                un_equal.set_index(i - match_count, source_idx);
            }
        }
        // Return how many matches were found
        return match_count;
    }


    typedef bool (*row_equality_function_t)(data_ptr_t left, data_ptr_t right, const idx_t column_offset);

    template <typename DATA_TYPE>
    bool RowRowEqual(data_ptr_t left, data_ptr_t right, const idx_t column_offset) {
        auto left_val  = Load<DATA_TYPE>(left + column_offset);
        auto right_val = Load<DATA_TYPE>(right + column_offset);
        return left_val == right_val;
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

    template <int COMPRESSED_WIDTH>
    static compressed_vector_equality_function_t GetCompressedEqualityFunctionA(const LogicalType &type) {
        switch (type.id()) {
            case LogicalTypeId::BIGINT:
                return CompressedVectorRowEqual<int64_t, COMPRESSED_WIDTH>;
            case LogicalTypeId::UBIGINT:
                return CompressedVectorRowEqual<uint64_t, COMPRESSED_WIDTH>;
            case LogicalTypeId::INTEGER:
                return CompressedVectorRowEqual<int32_t, COMPRESSED_WIDTH>;
            case LogicalTypeId::UINTEGER:
                return CompressedVectorRowEqual<uint32_t, COMPRESSED_WIDTH>;
            case LogicalTypeId::SMALLINT:
                return CompressedVectorRowEqual<int16_t, COMPRESSED_WIDTH>;
            case LogicalTypeId::USMALLINT:
                return CompressedVectorRowEqual<uint16_t, COMPRESSED_WIDTH>;
            case LogicalTypeId::TINYINT:
                return CompressedVectorRowEqual<int8_t, COMPRESSED_WIDTH>;
            case LogicalTypeId::UTINYINT:
                return CompressedVectorRowEqual<uint8_t, COMPRESSED_WIDTH>;
            case LogicalTypeId::FLOAT:
                return CompressedVectorRowEqual<float, COMPRESSED_WIDTH>;
            case LogicalTypeId::DOUBLE:
                return CompressedVectorRowEqual<double, COMPRESSED_WIDTH>;
            default:
                throw NotImplementedException("Equality function not implemented for type");
        }
    }

    static compressed_vector_equality_function_t GetCompressedEqualityFunction(const LogicalType &type, const int compressed_width) {
        switch (compressed_width){
            case 1:
                return GetCompressedEqualityFunctionA<1>(type);
            case 2:
                return GetCompressedEqualityFunctionA<2>(type);
            case 3:
                return GetCompressedEqualityFunctionA<3>(type);
            case 4:
                return GetCompressedEqualityFunctionA<4>(type);
            case 5:
                return GetCompressedEqualityFunctionA<5>(type);
            case 6:
                return GetCompressedEqualityFunctionA<6>(type);
            case 7:
                return GetCompressedEqualityFunctionA<7>(type);
            case 8:
                return GetCompressedEqualityFunctionA<8>(type);
            default:
                throw NotImplementedException("Equality function not implemented for type");
        }
    }



    static vector_equality_function_t GetEqualityFunction(const LogicalType &type) {
        switch (type.id()) {
            case LogicalTypeId::BIGINT:
                return VectorRowEqual<int64_t>;
            case LogicalTypeId::UBIGINT:
                return VectorRowEqual<uint64_t>;
            case LogicalTypeId::INTEGER:
                return VectorRowEqual<int32_t>;
            case LogicalTypeId::UINTEGER:
                return VectorRowEqual<uint32_t>;
            case LogicalTypeId::SMALLINT:
                return VectorRowEqual<int16_t>;
            case LogicalTypeId::USMALLINT:
                return VectorRowEqual<uint16_t>;
            case LogicalTypeId::TINYINT:
                return VectorRowEqual<int8_t>;
            case LogicalTypeId::UTINYINT:
                return VectorRowEqual<uint8_t>;
            case LogicalTypeId::FLOAT:
                return VectorRowEqual<float>;
            case LogicalTypeId::DOUBLE:
                return VectorRowEqual<double>;
            default:
                throw NotImplementedException("Equality function not implemented for type");
        }
    }

    static row_equality_function_t GetRowEqualityFunction(const LogicalType &type) {
        switch (type.id()) {
            case LogicalTypeId::BIGINT:
                return RowRowEqual<int64_t>;
            case LogicalTypeId::UBIGINT:
                return RowRowEqual<uint64_t>;
            case LogicalTypeId::INTEGER:
                return RowRowEqual<int32_t>;
            case LogicalTypeId::UINTEGER:
                return RowRowEqual<uint32_t>;
            case LogicalTypeId::SMALLINT:
                return RowRowEqual<int16_t>;
            case LogicalTypeId::USMALLINT:
                return RowRowEqual<uint16_t>;
            case LogicalTypeId::TINYINT:
                return RowRowEqual<int8_t>;
            case LogicalTypeId::UTINYINT:
                return RowRowEqual<uint8_t>;
            case LogicalTypeId::FLOAT:
                return RowRowEqual<float>;
            case LogicalTypeId::DOUBLE:
                return RowRowEqual<double>;
            default:
                throw NotImplementedException("Row equality function not implemented for type");
        }
    }
};
