#include "duckdb.hpp"
#include "materialization/row_layout.hpp"
#include "hash_table/factory.hpp"
#include "utils.hpp"
#include <random>

#include "duckdb/common/types/row/tuple_data_states.hpp"

namespace duckdb {
    struct ExperimentConfig {
        uint64_t colum_count;
        uint64_t cardinality;
    };

    struct AnalysisResult {
        uint64_t append_time;
        uint64_t copy_time;
        uint64_t gather_time;

        void Print() const {
            std::cout << "AppendTime=" << append_time << " CopyTime=" << copy_time << " GatherTime=" << gather_time
                      << std::endl;
        }
    };

    std::string GetBuildQuery(const ExperimentConfig &config) {
        std::string select_statement = "SELECT ";
        for (uint64_t i = 0; i < config.colum_count; ++i) {
            select_statement += "CAST(range as UINT64) as col" + std::to_string(i) + ", ";
        }
        select_statement += "FROM range(" + std::to_string(config.cardinality) + ")";
        return select_statement;
    }

    void ShuffleVectors(vector<data_ptr_t> &row_pointers) {
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(row_pointers.begin(), row_pointers.end(), g);
    }

    void RowMajorGather(Vector pointers_v, RowLayoutFormat &format, const idx_t count, const column_t column_offset, const idx_t column_count, DataChunk &result) {
        vector<uint8_t> byte_widths;
        vector<data_ptr_t> vector_head_pointers;
        for (column_t idx = 0; idx < column_count; idx++) {
            const column_t column_idx = column_offset + idx;
            const auto &type = format.types[column_idx];
            byte_widths.push_back(GetByteSize(type));

            Vector &vector = result.data[idx];
            auto vector_data = FlatVector::GetData<data_t>(vector);
            vector_head_pointers.push_back(vector_data);
        }

        auto row_ptrs = FlatVector::GetData<data_ptr_t>(pointers_v);


        for (idx_t row_idx = 0; row_idx < count; row_idx++) {
            auto row_ptr = row_ptrs[row_idx];
            for (column_t col_idx = 0; col_idx < column_count; col_idx++) {
                const auto byte_width = byte_widths[col_idx];
                auto vector_head_ptr = vector_head_pointers[col_idx];
                auto destination_ptr = vector_head_ptr + row_idx * byte_width;
                auto source_ptr = row_ptr + format.offsets[col_idx];

                memcpy(destination_ptr, source_ptr, byte_width);
            }
        }




    }

    AnalysisResult test_materialization(ExperimentConfig config) {
        DuckDB db(nullptr);
        Connection con(db);

        const auto build_query = GetBuildQuery(config);
        const auto build_result = con.Query(build_query);
        const auto &build_result_collection = build_result->Collection();

        ColumnDataScanState build_state;
        DataChunk next_chunk_build;
        build_result_collection.InitializeScan(build_state);
        build_result_collection.InitializeScanChunk(next_chunk_build);

        auto build_types = build_result_collection.Types();

        auto materialization_types = build_types;
        const auto hash_col_idx = materialization_types.size();
        materialization_types.push_back(LogicalType::HASH);

        // *** BUILDING THE HASH TABLE ***

        MemoryManager mm;
        const vector<column_t> keys = {0};
        const uint8_t partition_bits = 4; // results in 16 partitions

        RowLayout layout(materialization_types, keys, partition_bits, false, mm);

        // time the appending
        const auto append_start = high_resolution_clock::now();

        while (build_result_collection.Scan(build_state, next_chunk_build)) {
            layout.Append(next_chunk_build);
        }

        const uint64_t append_time = time(append_start);

        // collect all pointers
        RowLayoutIterator layout_iterator(layout);
        IteratorStep state;

        vector<data_ptr_t> row_pointers;

        const auto copy_start = high_resolution_clock::now();

        while (layout_iterator.Next(state)) {
            // add a new vector to the row_pointer_vectors
            // copy the row pointers to the new vector
            const auto row_pointer_vector = FlatVector::GetData<data_ptr_t>(state.partition_step.row_pointer);
            for (idx_t i = 0; i < state.partition_step.count; i++) {
                row_pointers.push_back(row_pointer_vector[i]);
            }
        }

        const auto copy_time = time(copy_start);

        ShuffleVectors(row_pointers);
        Vector pointer_v(LogicalType::POINTER);
        const auto pointer_data = FlatVector::GetData<data_ptr_t>(pointer_v);

        const uint64_t INNER_LOOP_SIZE = 2048;

        const vector<column_t> columns = layout.columns;
        DataChunk chunk;
        chunk.Initialize(Allocator::DefaultAllocator(), layout.format.types, STANDARD_VECTOR_SIZE);

        const auto gather_start = high_resolution_clock::now();

        for (idx_t idx = 0; idx < row_pointers.size() - STANDARD_VECTOR_SIZE; idx += STANDARD_VECTOR_SIZE) {
            for (idx_t vector_idx = 0; vector_idx < STANDARD_VECTOR_SIZE; vector_idx++) {
                pointer_data[vector_idx] = row_pointers[idx + vector_idx];
            }

            RowMajorGather(pointer_v, layout.format, STANDARD_VECTOR_SIZE, 0, columns.size(), chunk);
        }

        auto gather_time = time(gather_start);
        layout.Free();

        return {
            .append_time = append_time,
            .copy_time = copy_time,
            .gather_time = gather_time
    };
}

}


int main() {

    for (uint64_t i = 0; i < 5; ++i) {
        constexpr duckdb::ExperimentConfig config = {5, 100000000};
        const auto result = duckdb::test_materialization(config);
        result.Print();
    }
    return 0;
}
