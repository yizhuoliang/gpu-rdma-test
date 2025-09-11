## NCCL RDMA GPU↔GPU Test (ConnectX-7, RTX 2000 Ada)

This sets up a minimal C++/CUDA/NCCL program to verify direct GPU↔GPU data transfer over IB/RoCE between two hosts (peer-to-peer). It uses a TCP rendezvous to exchange the NCCL unique ID and then does `ncclSend`/`ncclRecv` between the GPUs.

### Host assumptions
- Ubuntu 24.04 (noble)
- CUDA 12.x already installed (nvcc present)
- One NVIDIA GPU per node (RTX 2000 Ada)
- Mellanox/NVIDIA ConnectX-7 NICs with an IB/RoCE link between nodes
- IB netdev `ibs3` is up with IPs: server 10.10.2.1, client 10.10.2.2
- This project directory on both machines: `/home/yliang/gpu-test`

### 1) Fix APT sources (disable broken LLVM entry)
If the LLVM apt entry is present, disable it and refresh package lists:
```bash
sudo sed -i 's%^deb http://apt\.llvm\.org/noble/ llvm-toolchain-noble-14 main%# deb http://apt.llvm.org/noble/ llvm-toolchain-noble-14 main%' \
  /etc/apt/sources.list.d/archive_uri-http_apt_llvm_org_noble_-noble.list || true
sudo apt-get update -y
```

### 2) Install NCCL runtime and headers
```bash
sudo apt-get install -y libnccl2 libnccl-dev
```

### 3) Enable GPUDirect RDMA (kernel module)
```bash
sudo modprobe nvidia_peermem
lsmod | grep nvidia_peermem
```

### 4) Verify environment (optional)
```bash
nvcc --version
nvidia-smi -L
ibdev2netdev
ip -br addr show ibs3
```

### 5) Get the source onto both machines
On the already-configured machine (10.10.2.1), sync this directory to the peer (10.10.2.2):
```bash
rsync -a /home/yliang/gpu-test/ yliang@10.10.2.2:/home/yliang/gpu-test/
```

### 6) Build on each machine
```bash
cmake -S /home/yliang/gpu-test -B /home/yliang/gpu-test/build
cmake --build /home/yliang/gpu-test/build -j
```

### 7) Run the test
- On server (10.10.2.1):
```bash
NCCL_DEBUG=INFO \
NCCL_IB_HCA=mlx5 \
NCCL_NET_GDR_LEVEL=PHB \
NCCL_SOCKET_IFNAME=ibs3,ens1np0 \
/home/yliang/gpu-test/build/nccl_rdma_test server 10.10.2.1 50051 67108864
```

- On client (10.10.2.2):
```bash
NCCL_DEBUG=INFO \
NCCL_IB_HCA=mlx5 \
NCCL_NET_GDR_LEVEL=PHB \
NCCL_SOCKET_IFNAME=ibs3,ens1np0 \
/home/yliang/gpu-test/build/nccl_rdma_test client 10.10.2.1 50051 67108864
```

Notes:
- `67108864` floats ≈ 256 MiB per direction. Adjust to taste.
- Keep server and client ports/size identical.
- If your fabric requires a GID index, add for both sides: `NCCL_IB_GID_INDEX=3` (or the correct index for your setup).

### 8) Expected output
Both sides print NCCL INFO logs and a bandwidth line, e.g.:
```
Server: exchange completed, size=0.256 GB, time=XX ms, bw ~ YY GB/s
Client: exchange completed, size=0.256 GB, time=XX ms, bw ~ YY GB/s
Validation OK
```
Look for `NET/IB` in NCCL logs to confirm the IB transport; presence of `nvidia_peermem` indicates GPUDirect RDMA is active.

### Troubleshooting (quick)
- Ensure `ibs3` is UP and IPs are reachable: `ping -c1 10.10.2.1` from client.
- Force IB transport and NIC selection via env shown above.
- If logs mention TCP or sockets only, verify `libnccl2` is installed and `nvidia_peermem` is loaded.

