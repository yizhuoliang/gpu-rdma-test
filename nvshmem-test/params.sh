#!/usr/bin/bash

num_blocks=(8)

num_warps=(8 10 12 16)

for num_block in ${num_blocks[@]}; do
    for num_warp in ${num_warps[@]}; do
        echo "num_block: $num_block, num_warp: $num_warp"
        nvshmrun -np 2 ./build/put_nvshmem $num_block $num_warp
    done
done


# export NVSHMEM_REMOTE_TRANSPORT=none

# nvshmrun -np 2 ./build/put_nvshmem