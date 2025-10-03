UCX-ZMQ Compatibility Wrapper (DisagMoE)

A minimal ZeroMQ-like API backed by UCX fan-in queue, preserving the ZMQ usage patterns in DisagMoE while reusing the existing `ucx-queue` core logic.

ZMQ-style API

- socket_t(socket_type::{push,pull})
- socket.bind("tcp://IP:PORT") (pull)
- socket.connect("tcp://IP:PORT") (push)
- send(buffer, send_flags::{none,sndmore}) (multipart: use sndmore for all but last)
- recv(message_t&) single-frame
- recv_multipart(std::vector<message_t>&) multi-frame (peer_id + metadata)
- message_t::to_string(), .data(), .size()
- buffer(ptr, size), str_buffer(cstr)

Build

```bash
cmake -S ucx-zmq -B ucx-zmq/build -D CMAKE_BUILD_TYPE=Release
cmake --build ucx-zmq/build -j
```

Requires UCX (libucp + libucs) and ZeroMQ (libzmq/cppzmq headers).

Example

```bash
# Terminal 1 (server on 10.10.2.1)
./ucx-zmq/build/ucxq_example server

# Terminal 2 (client using same IP/port)
./ucx-zmq/build/ucxq_example client
```

Notes

- Multipart framing is implemented on top of the fan-in queue with a compact header.
- Single-frame send/recv maps directly to the underlying queue for tensor fast path.
- Core UCX implementation remains unchanged and is reused as-is.
