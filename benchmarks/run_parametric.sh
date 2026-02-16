#!/bin/bash

# ChunkDB Parametric Benchmark Runner
# Usage: ./run_parametric.sh [full|quick]

set -e  # Exit on error

MODE=${1:-full}
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"
cd "$PROJECT_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}ChunkDB Parametric Benchmark Runner${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Create output directory with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_DIR="${SCRIPT_DIR}/benchmark_results_${TIMESTAMP}"
mkdir -p "$OUTPUT_DIR"

echo -e "${GREEN}Output directory: ${OUTPUT_DIR}${NC}\n"

# Check mode
if [ "$MODE" == "quick" ]; then
    echo -e "${YELLOW}QUICK MODE: 27 configurations (~10 minutes)${NC}\n"
    CONFIGS=27
    ESTIMATED_TIME="10 minutes"

    # Backup original file
    cp examples/parametric_benchmark.rs examples/parametric_benchmark.rs.backup

    # Modify for quick run
    sed -i 's/let chunk_rows_range: Vec<u64> = (1..=10).map(|i| i \* 5000).collect();/let chunk_rows_range: Vec<u64> = vec![5000, 25000, 50000];/' examples/parametric_benchmark.rs
    sed -i 's/let hash_buckets_range: Vec<u64> = (1..=10).map(|i| i \* 10).collect();/let hash_buckets_range: Vec<u64> = vec![10, 50, 100];/' examples/parametric_benchmark.rs
    sed -i 's/let range_dim_range: Vec<u64> = (1..=10).map(|i| i \* 5000).collect();/let range_dim_range: Vec<u64> = vec![5000, 25000, 50000];/' examples/parametric_benchmark.rs

    trap cleanup EXIT
    cleanup() {
        echo -e "\n${YELLOW}Restoring original file...${NC}"
        mv examples/parametric_benchmark.rs.backup examples/parametric_benchmark.rs
    }
elif [ "$MODE" == "full" ]; then
    echo -e "${RED}FULL MODE: 1000 configurations (~4-8 hours)${NC}\n"
    CONFIGS=1000
    ESTIMATED_TIME="4-8 hours"
else
    echo -e "${RED}Usage: $0 [full|quick]${NC}"
    echo -e "  full  - Run all 1000 configurations (default)"
    echo -e "  quick - Run 27 configurations for testing"
    exit 1
fi

echo -e "Configuration:"
echo -e "  Configs: ${GREEN}${CONFIGS}${NC}"
echo -e "  Time:    ${YELLOW}${ESTIMATED_TIME}${NC}"
echo -e "  Data:    1M rows, 100 sensors\n"

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
echo -e "${BLUE}[3/5]${NC} Compiling benchmark..."
cargo build --example parametric_benchmark --release 2>&1 | grep -E "(Compiling|Finished|error)" || true

if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo -e "${RED}Compilation failed${NC}"
    exit 1
fi
echo -e "${GREEN}Compiled successfully${NC}\n"

# Step 4: Run benchmark
echo -e "${BLUE}[4/5]${NC} Running benchmark (${ESTIMATED_TIME})..."
echo -e "${YELLOW}Output will be saved to: ${OUTPUT_DIR}/benchmark_run.log${NC}"
echo -e "${YELLOW}Monitor progress: tail -f ${OUTPUT_DIR}/benchmark_run.log${NC}\n"

START_TIME=$(date +%s)

# Run benchmark from project root
cargo run --example parametric_benchmark --release 2>&1 | tee "${OUTPUT_DIR}/benchmark_run.log"

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
echo -e "${BLUE}[5/5]${NC} Generating plots and analysis..."

# Copy plot script to output dir
cp "${SCRIPT_DIR}/plot_benchmark.py" "${OUTPUT_DIR}/plot_benchmark_local.py"

# Run plot script from output directory
cd "${OUTPUT_DIR}"
python plot_benchmark_local.py

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
ChunkDB Parametric Benchmark Results
====================================

Run Date: $(date)
Mode: ${MODE}
Configurations: ${CONFIGS}
Duration: ${HOURS}h ${MINUTES}m ${SECONDS}s

Files in this directory:
========================

Data Files:
  benchmark_results.csv   - Raw benchmark data (all measurements)
  benchmark_summary.txt    - Quick summary with best/worst configs
  benchmark_analysis.txt   - Detailed statistical analysis

Plot Files:
  benchmark_heatmaps.png            - 2D heatmaps for each query
  benchmark_3d_surfaces.png         - 3D surface visualizations
  benchmark_parameter_effects.png   - Individual parameter impact
  benchmark_best_configs.png        - Best configuration comparison
  benchmark_distribution.png        - Performance distributions

Other Files:
  benchmark_run.log        - Complete execution log
  plot_benchmark_local.py  - Plot generation script
  venv/                    - Python virtual environment
  README.txt               - This file

Quick Start:
============

1. View summary:
   cat benchmark_summary.txt

2. View detailed analysis:
   cat benchmark_analysis.txt

3. View plots:
   xdg-open benchmark_heatmaps.png
   xdg-open benchmark_best_configs.png

4. Re-generate plots (if needed):
   source venv/bin/activate
   python plot_benchmark_local.py
   deactivate
EOF

# Summary
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}BENCHMARK COMPLETE!${NC}"
echo -e "${BLUE}========================================${NC}\n"

echo -e "${GREEN}All results saved to:${NC}"
echo -e "   ${YELLOW}${OUTPUT_DIR}${NC}\n"

echo -e "Generated files:"
echo -e "  ${GREEN}+${NC} benchmark_results.csv (raw data)"
echo -e "  ${GREEN}+${NC} benchmark_summary.txt (summary)"
echo -e "  ${GREEN}+${NC} benchmark_analysis.txt (analysis)"
echo -e "  ${GREEN}+${NC} benchmark_heatmaps.png"
echo -e "  ${GREEN}+${NC} benchmark_3d_surfaces.png"
echo -e "  ${GREEN}+${NC} benchmark_parameter_effects.png"
echo -e "  ${GREEN}+${NC} benchmark_best_configs.png"
echo -e "  ${GREEN}+${NC} benchmark_distribution.png"
echo -e "  ${GREEN}+${NC} benchmark_run.log"
echo -e "  ${GREEN}+${NC} venv/ (Python virtual environment)"
echo -e "  ${GREEN}+${NC} README.txt\n"

# Show best configs
echo -e "${BLUE}Quick Summary:${NC}\n"
if [ -f "${OUTPUT_DIR}/benchmark_summary.txt" ]; then
    tail -20 "${OUTPUT_DIR}/benchmark_summary.txt" | head -15
fi

echo -e "\n${YELLOW}View detailed results:${NC}"
echo -e "  cd ${OUTPUT_DIR}"
echo -e "  cat benchmark_summary.txt"
echo -e "  cat benchmark_analysis.txt"
echo -e "  xdg-open benchmark_heatmaps.png"
echo

# Open images if display available
if command -v xdg-open &> /dev/null && [ -n "$DISPLAY" ]; then
    read -p "Open images now? [y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        xdg-open "${OUTPUT_DIR}/benchmark_heatmaps.png" 2>/dev/null &
        xdg-open "${OUTPUT_DIR}/benchmark_best_configs.png" 2>/dev/null &
    fi
fi

echo -e "${GREEN}Done!${NC}\n"
echo -e "Results directory: ${YELLOW}${OUTPUT_DIR}${NC}\n"
