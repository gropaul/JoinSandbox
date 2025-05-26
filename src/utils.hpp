#pragma once

#include <chrono>
#include <string>
#include <iostream>

namespace duckdb {
    inline uint64_t time(std::chrono::time_point<std::chrono::high_resolution_clock> start,
                         const std::string &name = "") {
        const auto end = std::chrono::high_resolution_clock::now();
        const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        if (!name.empty()) {
            std::cout << name << "=" << duration.count() << "ms ";
        }
        return duration.count();
    }
}
