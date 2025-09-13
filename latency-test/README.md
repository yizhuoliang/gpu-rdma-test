## Latency Test: CPU↔CPU (ZeroMQ) vs CPU→GPU→IB→GPU→CPU (NCCL)

Measures round-trip latency with two patterns and sizes:
- 1) CPU→CPU via ZeroMQ (1 KB)
- 2) CPU→GPU→IB→GPU→CPU via NCCL (1 KB)
- 3) CPU→CPU via ZeroMQ (1 GB)
- 4) CPU→GPU→IB→GPU→CPU via NCCL (1 GB)

### Assumptions
- Server IP baked as `10.10.2.1` (see `src/main.cpp` macros)
- IB interface `ibs3` up; two hosts connected
- One GPU per host
- Server/client run once each to measure round-trip (REQ/REP style)

### Setup (conda) on BOTH hosts
```bash
export REPO_ROOT_PATH=/path/to/your/clone
conda create -n lattest -y -c conda-forge -c nvidia nccl=2.18.5.1 cuda-cudart=12.0.107 cuda-cudart-dev=12.0.107 zeromq cppzmq
conda activate lattest

cmake -S $REPO_ROOT_PATH/latency-test \
  -B $REPO_ROOT_PATH/latency-test/build-conda \
  -DNCCL_INCLUDE_DIR=$CONDA_PREFIX/include \
  -DNCCL_LIBRARY=$CONDA_PREFIX/lib/libnccl.so
cmake --build $REPO_ROOT_PATH/latency-test/build-conda -j
```

### Run
- On server host (10.10.2.1):
```bash
conda activate lattest
export LD_LIBRARY_PATH=$CONDA_PREFIX/lib
NCCL_SOCKET_IFNAME=ibs3 NCCL_IB_HCA=mlx5 NCCL_DEBUG=INFO \
$REPO_ROOT_PATH/latency-test/build-conda/latency_test server
```

- On client host (10.10.2.2):
```bash
conda activate lattest
export LD_LIBRARY_PATH=$CONDA_PREFIX/lib
NCCL_SOCKET_IFNAME=ibs3 NCCL_IB_HCA=mlx5 NCCL_DEBUG=INFO \
$REPO_ROOT_PATH/latency-test/build-conda/latency_test client
```

The server will print three rounds per size and pattern. It also appends results to `results.csv` in the current working directory (created on first run) with columns: `size_bytes,pattern,round,latency_usec`.

Notes:
- If your RoCE setup needs a GID index, add `NCCL_IB_GID_INDEX=3` to both commands.
- For ZeroMQ over IB, specifying `tcp://SERVER_IP:PORT` binds on the IB IP; ZMQ will use that route. For lowest overhead you could also evaluate other transports, but TCP over the IB interface is sufficient for this comparison.


