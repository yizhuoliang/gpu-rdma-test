## UCX Fan-In Queue (ucx-queue)

A minimal many-to-one queue built on UCX UCP tag API. Multiple sender endpoints (possibly across hosts) feed messages into a single receiver queue on the server. The server offers a blocking `dequeue` API to pop messages.

### Design

- **Control plane (TCP OOB):**
  - Each endpoint connection exchanges UCX worker addresses over a small TCP socket, then creates a UCP endpoint (`ucp_ep_create`).
  - Server runs an `acceptThread` to accept any number of connections and create endpoints on-the-fly.

- **Data plane (UCX UCP TAG):**
  - Server runs a dedicated `progressThread` which repeatedly calls `ucp_tag_recv_nbx` with a wildcard source and fixed tag, then `ucp_worker_progress` until completion.
  - Completed receives are pushed as `Message` objects into an in-process `std::queue` protected by a mutex. The application consumes via `dequeue`.
  - Senders use `ucp_tag_send_nbx` on their endpoint; completion is waited with a helper.

- **Local vs remote senders:**
  - The example test simulates 16 local and 16 remote senders. Local senders enqueue directly (via `send_local`) to isolate network effects; remote senders use UCX endpoints over IB (or your configured transport).
  - If you prefer to exercise UCX for local senders too, set `UCX_TLS=sm,self,...` and direct local threads to send using an endpoint targeting a local listener.

- **Why a progress thread?**
  - UCX requires explicit progress calls unless integrated with an event loop. The `progressThread` centralizes progress and receive completion to feed the queue.

### API (header: `include/ucx_queue.hpp`)

- `FanInQueue(role, ip, tcp_port)` — role is `"server"` or `"client"`.
- `start(num_endpoints)` — server: begin accepting; client: establish `num_endpoints` UCX endpoints.
- `send(ep_index, buf, len, tag)` — client sender API.
- `send_local(buf, len)` — server-only convenience to enqueue without UCX (used by the test to simulate local producers).
- `bool dequeue(Message& out)` — blocking pop; returns false on shutdown.
- `stop()` — shutdown threads and UCX resources.

### Build

```bash
cmake -S ucx-queue -B ucx-queue/build \
  -D CMAKE_BUILD_TYPE=Release
cmake --build ucx-queue/build -j
```

Requirements: UCX headers/libs and ZeroMQ (`ucx`, `zeromq`, `cppzmq`), e.g., via conda.

### Run the test

Environment (optional but recommended for IB + shm):

```bash
export UCX_TLS=rc_x,sm,self
export UCX_SOCKADDR_TLS_PRIORITY=rdmacm
export UCX_NET_DEVICES=mlx5_0:1
```

On server (IP baked as 10.10.2.1 in test):

```bash
./ucx-queue/build/ucx_queue_test server ucx
./ucx-queue/build/ucx_queue_test server zmq
```

On client:

```bash
./ucx-queue/build/ucx_queue_test client ucx
./ucx-queue/build/ucx_queue_test client zmq
```

The test runs fan-in with 16 local senders (on server) and 16 remote senders (on client) for message sizes: 1KB, 8KB, 64KB, 128KB, 1MB. It reports total time to receive all messages on the server.



