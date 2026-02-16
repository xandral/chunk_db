#!/bin/bash

# ChunkDB Column Scaling Benchmark Runner
# Tests how performance varies with number of columns selected (1, 3, 7, 15, 31, 63, 127)
# Usage: ./run_scaling.sh [full|quick]

set -e  # Exit on error

MODE=${1:-quick}
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"
cd "$PROJECT_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}ChunkDB Column Scaling Benchmark${NC}"
echo -e "${BLUE}================================================${NC}\n"

# Create output directory with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_DIR="${SCRIPT_DIR}/benchmark_scaling_${TIMESTAMP}"
mkdir -p "$OUTPUT_DIR"

echo -e "${GREEN}Output directory: ${OUTPUT_DIR}${NC}\n"

# Check mode
if [ "$MODE" == "quick" ]; then
    echo -e "${YELLOW}QUICK MODE: 27 configurations x 7 column counts (~20 minutes)${NC}\n"
    CONFIGS=27
    ESTIMATED_TIME="20 minutes"

    # Backup original file
    cp examples/column_scaling_benchmark.rs examples/column_scaling_benchmark.rs.backup

    # Modify for quick run
    sed -i 's/let chunk_rows_range: Vec<u64> = (1..=10).map(|i| i \* 5000).collect();/let chunk_rows_range: Vec<u64> = vec![5000, 25000, 50000];/' examples/column_scaling_benchmark.rs
    sed -i 's/let hash_buckets_range: Vec<u64> = (1..=10).map(|i| i \* 10).collect();/let hash_buckets_range: Vec<u64> = vec![10, 50, 100];/' examples/column_scaling_benchmark.rs
    sed -i 's/let range_dim_range: Vec<u64> = (1..=10).map(|i| i \* 5000).collect();/let range_dim_range: Vec<u64> = vec![5000, 25000, 50000];/' examples/column_scaling_benchmark.rs

    trap cleanup EXIT
    cleanup() {
        echo -e "\n${YELLOW}Restoring original file...${NC}"
        mv examples/column_scaling_benchmark.rs.backup examples/column_scaling_benchmark.rs
    }
elif [ "$MODE" == "full" ]; then
    echo -e "${RED}FULL MODE: 1000 configurations x 7 column counts (~8-16 hours)${NC}\n"
    CONFIGS=1000
    ESTIMATED_TIME="8-16 hours"
else
    echo -e "${RED}Usage: $0 [full|quick]${NC}"
    echo -e "  quick - Run 27 configurations for testing (default)"
    echo -e "  full  - Run all 1000 configurations"
    exit 1
fi

echo -e "Configuration:"
echo -e "  Configs:       ${GREEN}${CONFIGS}${NC}"
echo -e "  Column counts: ${GREEN}1, 3, 7, 15, 31, 63, 127${NC}"
echo -e "  Queries:       ${GREEN}3 types (single_sensor, time_range, combined)${NC}"
echo -e "  Total tests:   ${GREEN}$((CONFIGS * 7 * 3))${NC}"
echo -e "  Time:          ${YELLOW}${ESTIMATED_TIME}${NC}"
echo -e "  Data:          1M rows, 100 sensors, 128 columns\n"

# Confirm
if [ "$MODE" == "full" ]; then
    read -p "This will take ${ESTIMATED_TIME}. Continue? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${RED}Cancelled${NC}"
        rm -rf "$OUTPUT_DIR"
        exit 0
    fi
fi

echo

# Step 1: Create Python venv
echo -e "${BLUE}[1/5]${NC} Creating Python virtual environment..."
python3 -m venv "${OUTPUT_DIR}/venv"

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to create venv${NC}"
    exit 1
fi

# Activate venv
source "${OUTPUT_DIR}/venv/bin/activate"
echo -e "${GREEN}Virtual environment created and activated${NC}\n"

# Step 2: Install Python dependencies
echo -e "${BLUE}[2/5]${NC} Installing Python dependencies in venv..."
pip install --quiet --upgrade pip
pip install --quiet pandas matplotlib seaborn numpy

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to install dependencies${NC}"
    exit 1
fi
echo -e "${GREEN}Python dependencies installed${NC}\n"

# Step 3: Compile benchmark
echo -e "${BLUE}[3/5]${NC} Compiling column scaling benchmark..."
cargo build --example column_scaling_benchmark --release 2>&1 | grep -E "(Compiling|Finished|error)" || true

if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo -e "${RED}Compilation failed${NC}"
    exit 1
fi
echo -e "${GREEN}Compiled successfully${NC}\n"

# Step 4: Run benchmark
echo -e "${BLUE}[4/5]${NC} Running benchmark (${ESTIMATED_TIME})..."
echo -e "${YELLOW}Note: Testing column scaling from 1 to 127 columns${NC}"
echo -e "${YELLOW}Output will be saved to: ${OUTPUT_DIR}/benchmark_run.log${NC}"
echo -e "${YELLOW}Monitor progress: tail -f ${OUTPUT_DIR}/benchmark_run.log${NC}\n"

START_TIME=$(date +%s)

# Run benchmark from project root
cargo run --example column_scaling_benchmark --release 2>&1 | tee "${OUTPUT_DIR}/benchmark_run.log"

if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo -e "\n${RED}Benchmark failed${NC}"
    deactivate
    exit 1
fi

# Move generated files to OUTPUT_DIR
mv benchmark_results.csv "${OUTPUT_DIR}/" 2>/dev/null || echo -e "${YELLOW}benchmark_results.csv not found${NC}"
mv benchmark_summary.txt "${OUTPUT_DIR}/" 2>/dev/null || echo -e "${YELLOW}benchmark_summary.txt not found${NC}"

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
HOURS=$((DURATION / 3600))
MINUTES=$(((DURATION % 3600) / 60))
SECONDS=$((DURATION % 60))

echo -e "\n${GREEN}Benchmark completed in ${HOURS}h ${MINUTES}m ${SECONDS}s${NC}\n"

# Check if CSV was generated
if [ ! -f "${OUTPUT_DIR}/benchmark_results.csv" ]; then
    echo -e "${RED}benchmark_results.csv not found in ${OUTPUT_DIR}${NC}"
    deactivate
    exit 1
fi

LINES=$(wc -l < "${OUTPUT_DIR}/benchmark_results.csv")
echo -e "${GREEN}Generated benchmark_results.csv (${LINES} lines)${NC}\n"

# Step 5: Generate plots
echo -e "${BLUE}[5/5]${NC} Generating scaling analysis plots..."
echo -e "${YELLOW}Note: Plots will show marginal cost per column${NC}\n"

# Copy plot script to output dir
cp "${SCRIPT_DIR}/plot_scaling.py" "${OUTPUT_DIR}/plot_scaling_local.py"

# Run plot script from output directory
cd "${OUTPUT_DIR}"
python plot_scaling_local.py

if [ $? -ne 0 ]; then
    echo -e "\n${RED}Plot generation failed${NC}"
    deactivate
    exit 1
fi

cd "$PROJECT_DIR"

echo -e "\n${GREEN}All plots generated${NC}\n"

# Deactivate venv
deactivate

# Create summary file
cat > "${OUTPUT_DIR}/README.txt" << EOF
ChunkDB Column Scaling Benchmark Results
=========================================

Run Date: $(date)
Mode: ${MODE}
Configurations: ${CONFIGS}
Duration: ${HOURS}h ${MINUTES}m ${SECONDS}s

Test Configuration:
- 128 data columns (col_0 to col_127)
- 4 column groups (~32 columns each)
- Column counts tested: 1, 3, 7, 15, 31, 63, 127
- 3 query types:
  * single_sensor: Filter by sensor_id, select N columns
  * time_range: Filter by timestamp range, select N columns
  * combined: Filter by both sensor_id and timestamp, select N columns

Columns are distributed across all 4 groups to test vertical join overhead.

Files in this directory:
========================

Data Files:
  benchmark_results.csv      - Raw benchmark data
  benchmark_summary.txt       - Quick summary
  scaling_analysis.txt        - Detailed scaling analysis

Plot Files:
  scaling_curves.png          - Performance vs column count
  overhead_per_column.png     - Marginal cost per column
  config_comparison.png       - Best vs worst configs
  query_comparison.png        - All query types compared

Other Files:
  benchmark_run.log           - Complete execution log
  plot_scaling_local.py       - Plot generation script
  venv/                       - Python virtual environment
  README.txt                  - This file

Quick Start:
============

1. View summary:
   cat benchmark_summary.txt

2. View detailed analysis:
   cat scaling_analysis.txt

3. View plots:
   xdg-open scaling_curves.png
   xdg-open overhead_per_column.png
EOF

# Summary
echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}BENCHMARK COMPLETE!${NC}"
echo -e "${BLUE}================================================${NC}\n"

echo -e "${GREEN}All results saved to:${NC}"
echo -e "   ${YELLOW}${OUTPUT_DIR}${NC}\n"

echo -e "Generated files:"
echo -e "  ${GREEN}+${NC} benchmark_results.csv"
echo -e "  ${GREEN}+${NC} benchmark_summary.txt"
echo -e "  ${GREEN}+${NC} scaling_analysis.txt"
echo -e "  ${GREEN}+${NC} scaling_curves.png"
echo -e "  ${GREEN}+${NC} overhead_per_column.png"
echo -e "  ${GREEN}+${NC} config_comparison.png"
echo -e "  ${GREEN}+${NC} query_comparison.png"
echo -e "  ${GREEN}+${NC} benchmark_run.log"
echo -e "  ${GREEN}+${NC} venv/"
echo -e "  ${GREEN}+${NC} README.txt\n"

# Show best configs
echo -e "${BLUE}Quick Summary:${NC}\n"
if [ -f "${OUTPUT_DIR}/benchmark_summary.txt" ]; then
    head -40 "${OUTPUT_DIR}/benchmark_summary.txt"
fi

echo -e "\n${YELLOW}View detailed results:${NC}"
echo -e "  cd ${OUTPUT_DIR}"
echo -e "  cat scaling_analysis.txt"
echo -e "  xdg-open scaling_curves.png"
echo

echo -e "${GREEN}Done!${NC}\n"
echo -e "Results directory: ${YELLOW}${OUTPUT_DIR}${NC}\n"
