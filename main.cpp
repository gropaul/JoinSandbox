#include <fstream>

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

struct JoinAnalysisResult {
    HashTableType ht_type;
    uint64_t n_partition_bits;

    uint64_t cardinality_build;
    uint64_t cardinality_probe;
    uint64_t cardinality_result;

    uint64_t capacity;

    double build_collision_rate;
    double probe_collision_rate_key;
    double probe_collision_rate_salt;

    uint64_t time_partitioning;
    uint64_t time_inserting;
    uint64_t time_post_processing;
    uint64_t time_probing;
    uint64_t time_total;

    void Print() const {
        std::cout << "HTType=" << HTTypeToString(ht_type) << " PartitionBits=" << n_partition_bits << " BuildCardinality="
                  << cardinality_build << " ProbeCardinality=" << cardinality_probe << " ResultCardinality="
                  << cardinality_result << " Capacity=" << capacity << " BuildCollisionRate=" << build_collision_rate
                  << " ProbeCollisionRateKey=" << probe_collision_rate_key << " ProbeCollisionRateSalt="
                  << probe_collision_rate_salt << " Partitioning=" << time_partitioning << " Inserting=" << time_inserting
                  << " PostProcessing=" << time_post_processing << " Probing=" << time_probing << " Total=" << time_total
                  << '\n';
    }

    string GetCSVRow() const {
        return HTTypeToString(ht_type) + "," +
               std::to_string(n_partition_bits) + "," +
               std::to_string(cardinality_build) + "," +
               std::to_string(cardinality_probe) + "," +
               std::to_string(cardinality_result) + "," +
               std::to_string(capacity) + "," +
               std::to_string(build_collision_rate) + "," +
               std::to_string(probe_collision_rate_key) + "," +
               std::to_string(probe_collision_rate_salt) + "," +
               std::to_string(time_partitioning) + "," +
               std::to_string(time_inserting) + "," +
               std::to_string(time_post_processing) + "," +
               std::to_string(time_probing) + "," +
               std::to_string(time_total) + "\n";
    }

    static string GetCSVHeader() {
        return
                "HTType,PartitionBits,CardinalityBuild,CardinalityProbe,CardinalityResult,Capacity,BuildCollisionRate,ProbeCollisionRateKey,ProbeCollisionRateSalt,Partitioning,Inserting,PostProcessing,Probing,Total\n";
    }

    static void WriteCSV(const vector<JoinAnalysisResult> &results, const string &file_name) {
        std::ofstream file(file_name);
        file << GetCSVHeader();
        for (const auto &result: results) {
            file << result.GetCSVRow();
        }
        file.close();
    }
};

string GetBuildSideQuery(uint64_t cardinality) {
    return "SELECT key, FROM build_100m LIMIT " + std::to_string(cardinality) + ";";
}

string GetProbeSideQuery(uint64_t cardinality) {
    return "PRAGMA disabled_optimizers='top_n';WITH values AS (SELECT key FROM probe_100m LIMIT " + std::to_string(cardinality) + ") SELECT * FROM values ORDER BY hash(key*13441);";
}

JoinAnalysisResult AnalyzeHT(const uint8_t partition_bits, const HashTableType ht_type, Connection &con,
                             const uint64_t cardinality_build, const uint64_t cardinality_probe) {

    const vector<column_t> keys = {0};

    // *** GETTING THE BUILD DATA ***
    const auto build_query = GetBuildSideQuery(cardinality_build);
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

    const auto partition_start = high_resolution_clock::now();
    MemoryManager mm;
    RowLayout layout(materialization_types, keys, partition_bits, true, mm);

    while (build_result_collection.Scan(build_state, next_chunk_build)) {
        layout.Append(next_chunk_build);
    }

    const uint64_t partition_time = time(partition_start);

    const auto insert_start = high_resolution_clock::now();

    HashTableBase *hash_table = HashTableFactory(ht_type, layout.row_count, mm, keys);
    hash_table->InitializeHT();
    hash_table->InsertAll(layout, partition_bits, hash_col_idx);
    const uint64_t insert_time = time(insert_start);

    const auto post_process_start = high_resolution_clock::now();
    hash_table->PostProcessBuild(layout, partition_bits);
    uint64_t post_process_time = time(post_process_start);

    // *** GETTING THE PROBE DATA ***
    const auto probe_query = GetProbeSideQuery(cardinality_probe);
    const auto probe_result = con.Query(probe_query);
    const auto &probe_result_collection = probe_result->Collection();


    ColumnDataScanState probe_state;
    DataChunk next_chunk_probe, result;
    probe_result_collection.InitializeScan(probe_state);
    probe_result_collection.InitializeScanChunk(next_chunk_probe);


    auto result_types = probe_result_collection.Types();
    result_types.insert(result_types.end(), build_types.begin(), build_types.end());

    // *** PROBING THE HASH TABLE ***

    idx_t cardinality_result = 0;
    result.Initialize(Allocator::DefaultAllocator(), result_types);

    auto probe_start = high_resolution_clock::now();

    while (probe_result_collection.Scan(probe_state, next_chunk_probe)) {
        OperatorResultType op_result;
        do {
            op_result = hash_table->Probe(next_chunk_probe, result);
            cardinality_result += result.size();
        } while (op_result == OperatorResultType::HAVE_MORE_OUTPUT);
    }
    const uint64_t probe_time = time(probe_start);

    // layout.Print();
    layout.Free();
    hash_table->Free();

    return {
        .ht_type = ht_type,
        .n_partition_bits = partition_bits,
        .cardinality_build = cardinality_build,
        .cardinality_probe = cardinality_probe,
        .cardinality_result = cardinality_result,
        .capacity = hash_table->GetCapacity(),
        .build_collision_rate = hash_table->GetCollisionRateBuild(),
        .probe_collision_rate_key = hash_table->GetCollisionRateProbeKey(),
        .probe_collision_rate_salt = hash_table->GetCollisionRateProbeSalt(),
        .time_partitioning = partition_time,
        .time_inserting = insert_time,
        .time_post_processing = post_process_time,
        .time_probing = probe_time,
        .time_total = partition_time + insert_time + post_process_time + probe_time
    };
}

constexpr uint64_t N_RUNS = 1;
constexpr uint64_t START_PARTITION_BITS = 8;
constexpr uint64_t MAX_PARTITION_BITS = 8;
constexpr uint64_t PARTITION_STEP_SIZE = 1;

// const vector<uint64_t> BUILD_CARDINALITIES = {10000000, 20000000, 30000000, 40000000, 50000000, 60000000, 70000000,
//                                               80000000, 90000000, 100000000};
// const vector<uint64_t> PROBE_CARDINALITIES = {10000000, 20000000, 30000000, 40000000, 50000000, 60000000, 70000000,
//                                               80000000, 90000000, 100000000};
const vector<uint64_t> BUILD_CARDINALITIES = {100000, 300000, 1000000, 3000000, 1000000, 3000000, 10000000, 30000000, 100000000};
const vector<uint64_t> PROBE_CARDINALITIES = {100000, 300000, 1000000, 3000000, 1000000, 3000000, 10000000, 30000000, 100000000};
const vector<HashTableType> HT_TYPES = {LINEAR_PROBING_PARTITIONED_COMPRESSED, LINEAR_PROBING_PARTITIONED};

int main() {
    DuckDB db("./micro.duckdb");
    Connection con(db);

    // three runs

    vector<JoinAnalysisResult> results;

    uint64_t total_experiments = 2 *  N_RUNS * BUILD_CARDINALITIES.size() * PROBE_CARDINALITIES.size() * (
                                     (MAX_PARTITION_BITS - START_PARTITION_BITS) / PARTITION_STEP_SIZE) * HT_TYPES.
                                 size();
    uint64_t current_experiment = 0;
    for (uint64_t run = 0; run < N_RUNS; run++) {
        for (const auto build_cardinality: BUILD_CARDINALITIES) {
            for (const auto probe_cardinality: PROBE_CARDINALITIES) {

                // skip configurations if the build cardinality is greater than the probe cardinality
                if (probe_cardinality < build_cardinality) {
                    continue;
                }

                for (uint64_t partition_bits = START_PARTITION_BITS; partition_bits <= MAX_PARTITION_BITS;
                     partition_bits += PARTITION_STEP_SIZE) {
                    for (const auto ht_type: HT_TYPES) {
                        const auto result = AnalyzeHT(partition_bits, ht_type, con, build_cardinality,
                                                      probe_cardinality);
                        results.push_back(result);
                        std::cout << "Experiment " << current_experiment << " out of " << total_experiments <<
                                " completed\n";
                        current_experiment++;

                        result.Print();


                    }
                }
            }
        }
    }

    auto now = std::chrono::system_clock::now();
    auto now_time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&now_time_t), "%Y-%m-%d_%H-%M-%S");
    std::string date_string = ss.str();

    JoinAnalysisResult::WriteCSV(results, "out/results_" + date_string + ".csv");
}
