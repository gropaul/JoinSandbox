#include "duckdb.hpp"
#include "materialization/row_layout.hpp"
#include "hash_table/factory.hpp"

using namespace duckdb;

uint64_t time(time_point<high_resolution_clock> start, bool print = false) {
    const auto end = high_resolution_clock::now();
    const auto duration = duration_cast<milliseconds>(end - start);
    if (print) {
        std::cout << "Time=" << duration.count() << "ms" << '\n';
    }
    return duration.count();
}

void test_materialization(uint8_t partition_bits, HashTableType ht_type) {
    DuckDB db("/Users/paul/micro.duckdb");

    Connection con(db);

    const vector<column_t> keys = {0};
    // const auto result = con.Query("SELECT key, key, key FROM build_10_percent;");
    const auto result = con.Query("SELECT range as key, key, key FROM range(100_000_000);");

    if (result->HasError()) {
        throw std::runtime_error(result->GetError());
    }
    auto next_chunk = result->Fetch();
    if (!next_chunk) {
        throw std::runtime_error("No data");
    }
    auto types = next_chunk->GetTypes();
    const auto hash_col_idx = types.size();
    types.push_back(LogicalType::HASH);

    // time the start of the build
    const auto start = std::chrono::high_resolution_clock::now();

    MemoryManager mm;
    RowLayout layout(types, keys, partition_bits, mm);

    while (next_chunk) {
        layout.Append(*next_chunk);
        next_chunk = result->Fetch();
    }

    const auto partition_duration = time(start);
    std::cout << "Bits=" << static_cast<int>(partition_bits) << ' ';
    std::cout << "Partition=" << partition_duration << "ms ";


    const auto copy_start = high_resolution_clock::now();
    RowLayout continious_layout = layout.CopyIntoContinuous(mm);
    const auto copy_duration = time(copy_start);
    std::cout << "Copy=" << copy_duration << "ms ";

    const auto ht_start = high_resolution_clock::now();

    HashTableBase *hash_table = HashTableFactory(ht_type, layout.row_count, mm);
    hash_table->InitializeHT();
    if (ht_type == LINEAR_PROBING_PARTITIONED_COMPRESSED) {
        hash_table->InsertAll(continious_layout, partition_bits, hash_col_idx);
    } else {
        hash_table->InsertAll(layout, partition_bits, hash_col_idx);
    }

    // hash_table->Print();

    const auto build_duration = time(ht_start);
    std::cout << "Build=" << build_duration << "ms ";
    auto total_duration = time(start);
    if (ht_type != LINEAR_PROBING_PARTITIONED_COMPRESSED) {
        total_duration -= copy_duration;
    }
    std::cout << "Total=" << total_duration << "ms ";

    std::cout << "Collisions=" << hash_table->GetCollisionRate() << " HTSize=" << BytesToString(hash_table->GetHTSize(1)) << " HTPartitionSize=" << BytesToString(hash_table->GetHTSize(1 << partition_bits)) << '\n';

    // layout.Print();
    layout.Free();
    continious_layout.Free();
    hash_table->Free();

}

int main() {
    // three runs
    const uint64_t N_RUNS = 30;
    const uint64_t MAX_PARTITION_BITS = 1;
    const uint64_t PARTITION_STEP_SIZE = 2;
    for (uint64_t run = 0; run < N_RUNS; run++) {
        std::cout << "*********** Run " << run << " ***********" << '\n';

        std::cout << "LINEAR_PROBING_PARTITIONED_COMPRESSED: ";
        for (uint8_t i = 0; i < MAX_PARTITION_BITS; i += PARTITION_STEP_SIZE) {
            test_materialization(i, LINEAR_PROBING_PARTITIONED_COMPRESSED);
        }
        std::cout << "LINEAR_PROBING_PARTITIONED:            ";
        for (uint8_t i = 0; i < MAX_PARTITION_BITS; i += PARTITION_STEP_SIZE) {
            test_materialization(i, LINEAR_PROBING_PARTITIONED);
        }

    }
}
