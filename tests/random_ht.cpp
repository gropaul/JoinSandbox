// #include <iostream>
// #include <thread>
// #include <vector>
// #include <chrono>
// #include <cstdlib>
// #include <random>
// #include <cstdint>
// #include <sys/mman.h>
// #include <unistd.h>
// #include <mutex>
// #include <condition_variable>
// #include <barrier>  // C++20 feature
//
//
// const uint64_t CARDINALITY = 200000000;
// // const uint64_t CARDINALITY = 200000;
//
//
// uint64_t inline next_power_of_two(uint64_t n) {
//     n--;
//     n |= n >> 1;
//     n |= n >> 2;
//     n |= n >> 4;
//     n |= n >> 8;
//     n |= n >> 16;
//     n |= n >> 32;
//     n++;
//     return n;
// }
//
//
// enum STRATEGY {
//     PARTITION,
//     ATOMIC
// };
//
//
// uint64_t populate_ht_atomics(uint64_t *ht_array, const uint64_t ht_size, uint64_t * &keys,
//                              const uint64_t tuples_start, const uint64_t tuples_end) {
//     uint64_t collisions = 0;
//
//     // cast the array to an atomic array using a cpp cast
//     std::atomic<uint64_t> *ht_atomic_array = reinterpret_cast<std::atomic<uint64_t> *>(ht_array);
//
//     for (uint64_t i = tuples_start; i < tuples_end; i++) {
//         const uint64_t key = keys[i];
//         uint64_t idx = 0 + (key % ht_size);
//         while (true) {
//             // use a compare and swap operation to insert the key
//             uint64_t expected = 0;
//             if (ht_atomic_array[idx].compare_exchange_strong(expected, key, std::memory_order_acquire,
//                                                              std::memory_order_relaxed)) {
//                 break;
//             }
//             collisions++;
//             idx = (idx + 1) % ht_size;
//         }
//     }
//
//     return collisions;
// }
//
// uint64_t populate_ht_partition(uint64_t *ht_array, const uint64_t ht_size, uint64_t * &keys,
//                                const uint64_t tuples_start, const uint64_t tuples_end, const uint64_t ht_start,
//                                const uint64_t partition_size) {
//     uint64_t collisions = 0;
//
//     for (uint64_t i = tuples_start; i < tuples_end; i++) {
//         const uint64_t key = keys[i];
//         uint64_t idx = ht_start + (key % partition_size);
//         while (true) {
//             if (ht_array[idx] == 0) {
//                 ht_array[idx] = key;
//                 break;
//             }
//             collisions++;
//             idx = (idx + 1) % ht_size;
//         }
//     }
//
//     return collisions;
// }
//
//
// // Function to allocate aligned memory on macOS
// void *allocate_large_aligned_memory(size_t element_count, size_t element_size) {
//     // Get the system page size on macOS
//     size_t page_size = 2 * 1024 * 1024;
//
//     // Calculate the total memory size
//     size_t mem_size = element_count * element_size;
//
//     // Align memory size to a multiple of the system page size
//     if (mem_size % page_size != 0) {
//         mem_size = ((mem_size / page_size) + 1) * page_size;
//     }
//
//     // Allocate memory with alignment
//     void *mem_ptr = mmap(nullptr, mem_size, PROT_READ | PROT_WRITE,
//                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
//
//     if (mem_ptr == MAP_FAILED) {
//         std::cerr << "mmap failed: " << strerror(errno) << std::endl;
//         return nullptr;
//     }
//
//     // Initialize memory to zero
//     std::memset(mem_ptr, 0, mem_size);
//
//     return mem_ptr;
// }
//
// // Function to deallocate memory on macOS
// void deallocate_memory(void *mem_ptr, size_t element_count, size_t element_size) {
//     size_t page_size = 2 * 1024 * 1024;
//
//     // Calculate the total memory size (aligned to page size)
//     size_t mem_size = element_count * element_size;
//     if (mem_size % page_size != 0) {
//         mem_size = ((mem_size / page_size) + 1) * page_size;
//     }
//
//     if (munmap(mem_ptr, mem_size) != 0) {
//         std::cerr << "munmap failed: " << strerror(errno) << std::endl;
//     }
// }
//
// uint64_t *get_random_uint64_array(const size_t size, const size_t value_range = 1000000000) {
//     // Initialize random number generator and distribution
//     static std::random_device rd;
//     static std::mt19937_64 gen(rd());
//     static std::uniform_int_distribution<uint64_t> dist(
//         0, value_range - 1
//     );
//
//     void *keys = allocate_large_aligned_memory(size, sizeof(uint64_t));
//
//
//     // Fill the vector with random values
//     for (size_t i = 0; i < size; ++i) {
//         static_cast<uint64_t *>(keys)[i] = dist(gen);
//     }
//
//     return static_cast<uint64_t *>(keys);
// }
//
// void populate(const uint64_t number_of_threads, const STRATEGY strategy, const uint64_t key_range = 1000000000) {
//     const uint64_t ht_size = next_power_of_two(2 * CARDINALITY);
//     auto ht_array = static_cast<uint64_t *>(allocate_large_aligned_memory(ht_size, sizeof(uint64_t)));
//
//     const uint64_t ht_partition_size = ht_size / number_of_threads;
//     const uint64_t tuples_per_thread = CARDINALITY / number_of_threads;
//     // std::cout << "HT Size=" << ht_size << " Chunk Size=" << ht_chunk_size << " Tuples per Thread=" << tuples_per_thread << '\n';
//
//     // set random seed
//     srand(42);
//     uint64_t *keys = get_random_uint64_array(CARDINALITY, key_range);
//
//     std::atomic<uint64_t> thread_duration_sum(0);
//     std::atomic<uint64_t> thread_collisions(0);
//
//     const auto overall_start = std::chrono::high_resolution_clock::now();
//
//     for (int64_t i = 0; i < 1; i++) {
//         std::vector<std::thread> threads;
//
//         // create barrier for ht_array initialization
//         std::barrier sync_barrier(number_of_threads, []() noexcept {});
//
//         for (uint64_t thread_index = 0; thread_index < number_of_threads; thread_index++) {
//             threads.push_back(std::thread([&, thread_index]() {
//                 const uint64_t ht_thread_offset = thread_index * ht_partition_size;
//                 // std::cout << "Thread=" << thread_index << " Start=" << ht_start << " End=" << ht_end << '\n';
//
//                 uint64_t* start_of_ht = ht_array + ht_thread_offset;
//                 std::memset(start_of_ht, 0, ht_partition_size * sizeof(uint64_t));
//
//                 // Wait for all to finish memset
//                 sync_barrier.arrive_and_wait();
//
//                 const uint64_t tuples_start = thread_index * tuples_per_thread;
//                 const uint64_t tuples_end = (thread_index + 1) * tuples_per_thread;
//
//                 const auto thread_start = std::chrono::high_resolution_clock::now();
//                 uint64_t collisions;
//                 switch (strategy) {
//                     case ATOMIC:
//                         collisions = populate_ht_atomics(ht_array, ht_size, keys, tuples_start, tuples_end);
//                         break;
//                     case PARTITION:
//                         collisions = populate_ht_partition(ht_array, ht_size, keys, tuples_start, tuples_end, ht_thread_offset,
//                                                            ht_partition_size);
//                         break;
//                 }
//
//                 const auto thread_end = std::chrono::high_resolution_clock::now();
//                 const auto thread_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
//                     thread_end - thread_start).count();
//
//                 thread_duration_sum += thread_duration;
//                 thread_collisions += collisions;
//             }));
//         }
//
//         for (auto &thread: threads) {
//             thread.join();
//         }
//     }
//
//     // measure overall time
//     const auto overall_end = std::chrono::high_resolution_clock::now();
//     const auto overall_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
//         overall_end - overall_start).count();
//
//     std::cout << "Overall Time=" << overall_duration << "ms ";
//
//
//     // count the number of elements in the hash table
//     uint64_t count = 0;
//     for (uint64_t i = 0; i < ht_size; i++) {
//         if (ht_array[i] != 0) {
//             count++;
//         }
//     }
//
//     uint64_t runtime = thread_duration_sum / number_of_threads / 1000;
//
//     std::cout << "Threads=" << number_of_threads << " CPU-Time=" << thread_duration_sum / 1000 << "ms " << "Runtime=" <<
//             runtime << "ms ";
//     std::cout << "Collisions Rate=" << (double) thread_collisions / (double) count << '\n';
//
//     deallocate_memory(ht_array, ht_size, sizeof(uint64_t));
//     deallocate_memory(keys, CARDINALITY, sizeof(uint64_t));
// }
//
// void run_with_key_range(const uint64_t key_range) {
//     std::cout << "===== PARTITION =====" << '\n';
//     for (uint8_t i = 1; i <= 8; i++) {
//         populate(i, PARTITION, key_range);
//     }
//     std::cout << "===== ATOMIC =====" << '\n';
//     for (uint8_t i = 1; i <= 8; i++) {
//         populate(i, ATOMIC, key_range);
//     }
// }
//
// int main() {
//     std::cout << "========== PRIMARY KEY =========" << '\n';
//     uint64_t key_range = CARDINALITY * 1000;
//     run_with_key_range(key_range);
//
//     std::cout << "========== FOREIGN KEY (~8 duplicates) =========" << '\n';
//     key_range = CARDINALITY / 8;
//     run_with_key_range(key_range);
//
//
//     std::cout << "========== FOREIGN KEY (~64 duplicates) =========" << '\n';
//     key_range = CARDINALITY / 64;
//     run_with_key_range(key_range);
// }
