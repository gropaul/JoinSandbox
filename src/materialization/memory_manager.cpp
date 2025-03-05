//
// memory_manager.cpp
//

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "memory_manager.hpp"
#include <iostream>
#include <cstdlib>      // for malloc/free
#include <stdexcept>    // for std::runtime_error, std::bad_alloc
#include <sys/mman.h>   // for mmap, munmap, etc. (Linux-specific)
#include <unistd.h>     // for sysconf, _SC_PAGESIZE (optional)

// If MAP_HUGETLB is not defined on this system, define it as 0 so
// that references to it compile but effectively do nothing.
#ifndef MAP_HUGETLB
#define MAP_HUGETLB 1
#endif

namespace duckdb {

static constexpr std::size_t HUGEPAGE_THRESHOLD = 2UL * 1024UL * 1024UL; // Example threshold: 2MB

data_ptr_t MemoryManager::allocate(std::size_t size) {
    void *ptr = nullptr;
    bool used_hugepages = false;

    // If the requested size is above our threshold, try to allocate using huge pages.
    if (size >= HUGEPAGE_THRESHOLD) {
        // Prepare flags for mmap
        int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS;
        // Add MAP_HUGETLB if it's not the dummy 0
        if (MAP_HUGETLB != 0) {
            mmap_flags |= MAP_HUGETLB;
        }

        ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, mmap_flags, -1, 0);
        if (ptr == MAP_FAILED) {
            ptr = nullptr; // Indicate failure
        } else {
            used_hugepages = true;
        }
    }

    // If we did not allocate via mmap, fall back to malloc
    if (!ptr) {
        ptr = std::malloc(size);
        if (!ptr) {
            throw std::bad_alloc();
        }
    }

    // Record the allocation info (size + whether or not it used huge pages)
    AllocationInfo info{size, used_hugepages};
    allocations[ptr] = info;

    return static_cast<data_ptr_t>(ptr);
}

void MemoryManager::deallocate(data_ptr_t ptr) {
    if (!ptr) {
        // Deallocating a null pointer is normally a no-op
        return;
    }

    auto it = allocations.find(static_cast<void*>(ptr));
    if (it == allocations.end()) {
        throw std::runtime_error("Attempt to deallocate an unknown or already freed pointer.");
    }

    const auto &info = it->second;
    if (info.used_hugepages && (MAP_HUGETLB != 0)) {
        // This allocation came from mmap with MAP_HUGETLB
        if (munmap(static_cast<void*>(ptr), info.size) != 0) {
            // Handle error or throw an exception if desired
            std::cerr << "munmap (huge pages) failed.\n";
        }
    } else {
        // Normal allocation from malloc
        std::free(ptr);
    }

    allocations.erase(it);
}

std::size_t MemoryManager::getActiveAllocations() const {
    return allocations.size();
}

MemoryManager::~MemoryManager() {
    for (const auto &pair : allocations) {
        const auto &info = pair.second;
        std::cerr << "Warning: Memory leak detected. Address: " << pair.first
                  << ", Size: " << info.size << " bytes\n";

        // Free them all
        if (info.used_hugepages && (MAP_HUGETLB != 0)) {
            munmap(pair.first, info.size);
        } else {
            std::free(pair.first);
        }
    }
    allocations.clear();
}

} // namespace duckdb
