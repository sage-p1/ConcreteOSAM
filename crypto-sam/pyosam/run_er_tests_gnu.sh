#!/bin/bash

# As a reminder to myself, make sure we cite GNU Parallel in the paper, because every time I run the program
# they request us to.

NUM_JOBS=${1:-$(nproc)}
MEMFREE_GB=254

export LOG_DIR="logs"
mkdir -p "$LOG_DIR"

block_sizes=(64 4096)
degrees=(5 100)
degree_big=(2000)
powers=$(seq 2 23)
powers_big=$(seq 14 19)
pointer_names=("stock" "og" "mw")

run_single_job() {
    local bs="$1"
    local power="$2"
    local d="$3"
    local pt_name="$4"
    local use_naive="$5"
    
    local n=$((2**power))
    local log_file="$LOG_DIR/ER_n${n}_d${d}_${pt_name}_naive${use_naive}_bs${bs}.log"
    local naive_arg=""
    [ "$use_naive" == "true" ] && naive_arg="--use_naive"

    python3 run_single_test.py \
        --n "$n" \
        --d "$d" \
        --bs "$bs" \
        --pt_name "$pt_name" \
        $naive_arg > "$log_file" 2>&1
}

export -f run_single_job

(
    for bs in "${block_sizes[@]}"; do for power in $powers; do for d in "${degrees[@]}"; do for pt_name in "${pointer_names[@]}"; do
        case "$pt_name" in "stock") use_naives=(false) ;; "og") use_naives=(true false) ;; "mw") use_naives=(false) ;; esac
        for use_naive in "${use_naives[@]}"; do echo "$bs $power $d $pt_name $use_naive"; done
    done; done; done; done

    for bs in "${block_sizes[@]}"; do for power in $powers_big; do for d in "${degree_big[@]}"; do for pt_name in "${pointer_names[@]}"; do
        case "$pt_name" in "stock") use_naives=(false) ;; "og") use_naives=(true false) ;; "mw") use_naives=(false) ;; esac
        for use_naive in "${use_naives[@]}"; do echo "$bs $power $d $pt_name $use_naive"; done
    done; done; done; done
) | parallel \
    --bar \
    --colsep ' ' \
    -j "$NUM_JOBS" \
    --memfree "${MEMFREE_GB}G" \
    run_single_job {1} {2} {3} {4} {5}

echo -e "\n--- All test suites complete ---"

