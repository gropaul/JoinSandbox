#pragma once
#include <cstddef>
#include <unordered_map>

// Adjust the definition of data_ptr_t as you currently use it.
// For example:
typedef unsigned char *data_ptr_t;

namespace duckdb {

    class MemoryManager {
    public:
        MemoryManager() = default;
        ~MemoryManager();

        data_ptr_t allocate(std::size_t size);
        void deallocate(data_ptr_t ptr);

        std::size_t getActiveAllocations() const;

    private:
        // To store metadata about each allocation
        struct AllocationInfo {
            std::size_t size;
            bool used_hugepages;
        };

        std::unordered_map<void*, AllocationInfo> allocations;
    };

}
