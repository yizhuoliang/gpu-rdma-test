UCX Push-Pull Wrapper (DisagMoE)

A UCX-backed push/pull socket abstraction that mirrors ZeroMQ semantics while letting senders bind to a well-known endpoint and receivers connect to it for address exchange.

Push/Pull API

- socket_t(socket_type::{push,pull})
- socket.bind("tcp://IP:PORT") (push)
- socket.connect("tcp://IP:PORT") (pull)
- send(buffer, send_flags::{none,sndmore}) (multipart: use sndmore for all but last)
- recv(message_t&) single-frame
- recv_multipart(std::vector<message_t>&) multi-frame (peer_id + metadata)
- message_t::to_string(), .data(), .size()
- buffer(ptr, size), str_buffer(cstr)

Build

```bash
cmake -S ucx-push-pull -B ucx-push-pull/build -D CMAKE_BUILD_TYPE=Release
cmake --build ucx-push-pull/build -j
```

This produces the `ucxpp_lib` library and the `ucxq_example` benchmark binary. Requires UCX (libucp + libucs) and ZeroMQ (libzmq/cppzmq headers).

Example

```bash
# Fixed network topology:
# - Server IP: 10.10.2.1
# - Client IP: 10.10.2.2 (advertised to server)
# - Port: 62000 (control uses 62001)
# - Threads: 16 local on server, 16 remote on client

# On the server node (10.10.2.1):
./ucx-push-pull/build/ucxq_example server

# On the client node (10.10.2.2):
./ucx-push-pull/build/ucxq_example client
```

Notes

- The benchmark now runs both semantics automatically and records CSV labels:
  - UCXQ_local_sb_rc, UCXQ_local_sc_rb, UCXQ_remote_sb_rc, UCXQ_remote_sc_rb
- Sender-side bind swaps the out-of-band handshake roles vs. the original ucx-zmq variant.
- Multipart framing is unchanged and remains compatible with the fan-in queue implementation.
- Single-frame send/recv still maps directly to the queue for the low-latency tensor path.
- The sample benchmark is fixed to 16 local and 16 remote senders and the IPs above.
