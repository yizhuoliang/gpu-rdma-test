## NCCL RDMA GPU↔GPU Test

This test verifies direct GPU-to-GPU transfers over IB/RoCE between two hosts using NCCL point-to-point (ncclSend/ncclRecv). A simple TCP rendezvous shares the NCCL unique ID; data is exchanged directly over the IB fabric using the NCCL IB transport.

### Network and host assumptions
- IB netdev `ibs3` up between the two nodes
- Server IP baked in code as `10.10.2.1` (see macros in `src/main.cpp`)
- Client connects to that server IP (also baked)
- One GPU per host
- Port and message size are baked as macros (`TEST_PORT`, `TEST_NUM_FLOATS`)

### Conda-based setup on BOTH hosts
First, set your repo path:
```bash
export REPO_ROOT_PATH=/path/to/your/clone
```
```bash
conda create -n nccltest -y -c conda-forge -c nvidia nccl=2.18.5.1 cuda-cudart=12.0.107 cuda-cudart-dev=12.0.107
conda activate nccltest

cmake -S $REPO_ROOT_PATH/nccl-test \
  -B $REPO_ROOT_PATH/nccl-test/build-conda \
  -DNCCL_INCLUDE_DIR=$CONDA_PREFIX/include \
  -DNCCL_LIBRARY=$CONDA_PREFIX/lib/libnccl.so
cmake --build $REPO_ROOT_PATH/nccl-test/build-conda -j
```

### Run
- On server host (10.10.2.1):
```bash
conda activate nccltest
export LD_LIBRARY_PATH=$CONDA_PREFIX/lib
NCCL_DEBUG=INFO NCCL_SOCKET_IFNAME=ibs3 NCCL_IB_HCA=mlx5 NCCL_NET_GDR_LEVEL=PHB \
$REPO_ROOT_PATH/nccl-test/build-conda/nccl_rdma_test server
```

- On client host (10.10.2.2):
```bash
conda activate nccltest
export LD_LIBRARY_PATH=$CONDA_PREFIX/lib
NCCL_DEBUG=INFO NCCL_SOCKET_IFNAME=ibs3 NCCL_IB_HCA=mlx5 NCCL_NET_GDR_LEVEL=PHB \
$REPO_ROOT_PATH/nccl-test/build-conda/nccl_rdma_test client
```

Notes:
- Defaults: `TEST_PORT` and `TEST_NUM_FLOATS` (256 MiB). Adjust macros in `src/main.cpp` if needed.
- To force a specific RoCE GID, add `NCCL_IB_GID_INDEX=3` to both commands.

