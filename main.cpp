#include "duckdb.hpp"
#include "materialization/row_layout.hpp"
#include "hash_table/factory.hpp"

using namespace duckdb;

uint64_t time(time_point<high_resolution_clock> start, const string &name = "") {
    const auto end = high_resolution_clock::now();
    const auto duration = duration_cast<milliseconds>(end - start);
    if (!name.empty()) {
        std::cout << name << "=" << duration.count() << "ms ";
    }
    return duration.count();
}

const string BUILD_QUERY = "SELECT CAST((range) AS uint64) as key FROM range(100_000_000);";
const string PROBE_QUERY = "SELECT CAST((range) AS uint64) as key FROM range(100_000_000);";

void test_materialization(uint8_t partition_bits, HashTableType ht_type, Connection &con) {
    const vector<column_t> keys = {0};

    // *** GETTING THE BUILD DATA ***

    const auto build_result = con.Query(BUILD_QUERY);
    if (build_result->HasError()) throw std::runtime_error(build_result->GetError());

    auto next_chunk = build_result->Fetch();
    auto build_types = next_chunk->GetTypes();

    auto materialization_types = build_types;
    const auto hash_col_idx = materialization_types.size();
    materialization_types.push_back(LogicalType::HASH);

    // *** BUILDING THE HASH TABLE ***

    const auto start = std::chrono::high_resolution_clock::now();
    MemoryManager mm;
    RowLayout layout(materialization_types, keys, partition_bits, mm);

    while (next_chunk) {
        layout.Append(*next_chunk);
        next_chunk = build_result->Fetch();
    }
    std::cout << "Bits=" << static_cast<int>(partition_bits) << ' ';
    time(start, "Partition");

    const auto ht_start = high_resolution_clock::now();

    HashTableBase *hash_table = HashTableFactory(ht_type, layout.row_count, mm, keys);
    hash_table->InitializeHT();
    hash_table->InsertAll(layout, partition_bits, hash_col_idx);
    time(ht_start, "Build");

    const auto ht_post_process_start = high_resolution_clock::now();
    hash_table->PostProcessBuild(layout, partition_bits);
    time(ht_post_process_start, "PostProcess");

    time(start, "TotalBuild");
    std::cout << "Collisions=" << hash_table->GetCollisionRate() << " HTSize=" <<
            BytesToString(hash_table->GetHTSize(1)) << " HTPartitionSize=" << BytesToString(
                hash_table->GetHTSize(1 << partition_bits)) << ' ';

    // *** GETTING THE PROBE DATA ***

    const auto probe_result = con.Query(PROBE_QUERY);
    if (probe_result->HasError()) throw std::runtime_error(probe_result->GetError());

    next_chunk = probe_result->Fetch();
    auto probe_types = next_chunk->GetTypes();

    auto result_types = probe_types;
    result_types.insert(result_types.end(), build_types.begin(), build_types.end());

    // *** PROBING THE HASH TABLE ***

    auto probe_start = high_resolution_clock::now();
    idx_t count = 0;
    DataChunk result;
    result.Initialize(Allocator::DefaultAllocator(), result_types);

    while (next_chunk) {
        OperatorResultType op_result = hash_table->Probe(*next_chunk, result);
        count += result.size();
        if (op_result == OperatorResultType::NEED_MORE_INPUT) {
            next_chunk = probe_result->Fetch();
        }
    }
    time(probe_start, "Probe");
    std::cout << "Count=" << count << ' ';

    std::cout << '\n';

    // layout.Print();
    layout.Free();
    hash_table->Free();
}

int main() {
    DuckDB db(nullptr);
    Connection con(db);

    // three runs
    const uint64_t N_RUNS = 1;
    const uint64_t START_PARTITION_BITS = 3;
    const uint64_t MAX_PARTITION_BITS = 9;
    const uint64_t PARTITION_STEP_SIZE = 1;
    for (uint64_t run = 0; run < N_RUNS; run++) {
        std::cout << "*********** Run " << run << " ***********" << '\n';
        //
        // for (uint8_t i = START_PARTITION_BITS; i < MAX_PARTITION_BITS; i += PARTITION_STEP_SIZE) {
        //     std::cout << "PARTITIONED_COMPRESSED: ";
        //     test_materialization(i, LINEAR_PROBING_PARTITIONED_COMPRESSED, con);
        // }
        for (uint8_t i = START_PARTITION_BITS; i < MAX_PARTITION_BITS; i += PARTITION_STEP_SIZE) {
            std::cout << "PARTITIONED:            ";
            test_materialization(i, LINEAR_PROBING_PARTITIONED, con);
        }
    }
}
