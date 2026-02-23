#!/bin/bash

LOG_DIR="logs"
mkdir -p "$LOG_DIR"

block_sizes=(64 4096)
degrees=(20 100)
# degrees_big=(2000)
powers=$(seq 5 23)
# powers_big=$(seq 14 19)
functions=("pr" "cd" "rw")
# functions=("pr" "cd" "rw" "dtc")
pointer_names=("recursive" "original" "multiwrite")
primes=("True")
make_copies=("False")


for p in "${primes[@]}"; do
  for mc in "${make_copies[@]}"; do
    for bs in "${block_sizes[@]}"; do
      for power in $powers; do
        n=$((2**power))
        for d in "${degrees[@]}"; do
          if [ "$d" -le "$n" ]; then
            for pt_name in "${pointer_names[@]}"; do
              case "$pt_name" in
                "recursive") move_semantics_plural=("True") ;;
                "original")    move_semantics_plural=("True" "False") ;; 
                "multiwrite")    move_semantics_plural=("True") ;;
                *)       continue ;;
              esac

              for move_semantics in "${move_semantics_plural[@]}"; do
                for fn in "${functions[@]}"; do
                  log_file="$LOG_DIR/ER_n${n}_d${d}_${pt_name}_semantics${move_semantics}_bs${bs}_${fn}_prime${p}_copies${mc}.log"
                  echo "Launching test: n=$n, d=$d, bs=$bs, pt=$pt_name, semantics=$move_semantics, fn=$fn"
                  python3 run_single_test.py \
                    --n "$n" \
                    --d "$d" \
                    --bs "$bs" \
                    --pt_name "$pt_name" \
                    --move_semantics "$move_semantics" \
                    --fn "$fn" \
                    --prime "$p" \
                    --make_copies "$mc" \
                    > "$log_file" 2>&1 &
                
                done
              done
            done
          fi
        done
      done
    done
  done

  # for power in $powers_big; do
  #   n=$((2**power))
  #   for d in "${degrees_big[@]}"; do
  #     for pt_name in "${pointer_names[@]}"; do
  #       case "$pt_name" in
  #         "recursive") move_semantics_plural=("True") ;;
  #         "original")    move_semantics_plural=("True" "False") ;;
  #         "multiwrite")    move_semantics_plural=("True") ;;
  #         *)       continue ;;
  #       esac

  #       for move_semantics in "${move_semantics_plural[@]}"; do
  #         log_file="$LOG_DIR/ER_n${n}_d${d}_${pt_name}_semantics${move_semantics}_bs${bs}.log"
  #         echo "Launching big test: n=$n, d=$d, bs=$bs, pt=$pt_name, semantics=$move_semantics"
  #         python3 run_single_test.py \
  #           --n "$n" \
  #           --d "$d" \
  #           --bs "$bs" \
  #           --pt_name "$pt_name" \
  #           --move_semantics "$move_semantics" \
  #           > "$log_file" 2>&1 &
  #       done
  #     done
  #   done
  # done
done

echo "All jobs launched in background. Use 'jobs' or 'ps' to monitor."
