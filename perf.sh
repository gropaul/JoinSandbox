#!/bin/bash

# Check if a command is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <command>"
    exit 1
fi

# Capture the command to benchmark
BENCHMARK_CMD="$@"

# Define perf events
EVENTS="cycles,instructions,cache-references,cache-misses,\
L1-dcache-loads,L1-dcache-load-misses,\
L1-icache-loads,L1-icache-load-misses,\
dTLB-loads,dTLB-load-misses,\
LLC-loads,LLC-load-misses,\
branch-instructions,branch-misses"

# Run perf with the given benchmark command
echo "Running benchmark with perf..."
perf stat -e $EVENTS $BENCHMARK_CMD