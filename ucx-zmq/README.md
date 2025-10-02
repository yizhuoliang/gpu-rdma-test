UCX-ZMQ Compatibility Wrapper (DisagMoE)

A minimal ZeroMQ-like API backed by UCX fan-in queue, preserving the ZMQ usage patterns in DisagMoE while reusing the existing `ucx-queue` core logic.

ZMQ-style API

- context_t(size_t threads)
- socket_t(context_t&, socket_type::{push,pull})
- socket.bind("tcp://IP:PORT") (pull)
- socket.connect("tcp://IP:PORT") (push)
- send(buffer, send_flags::{none,sndmore}) (multipart: use sndmore for all but last)
- recv(message_t&, recv_flags::none) single-frame
- recv_multipart(std::vector<message_t>&) multi-frame (peer_id + metadata)
- message_t::to_string(), .data(), .size()
- buffer(ptr, size), str_buffer(cstr)

Build

```bash
cmake -S ucx-zmq -B ucx-zmq/build -D CMAKE_BUILD_TYPE=Release
cmake --build ucx-zmq/build -j
```

Requires UCX and builds against sibling ucx-queue.

Example

```bash
./ucx-zmq/build/ucx_zmq_example
```

Notes

- Multipart framing is implemented on top of the fan-in queue with a compact header.
- Single-frame send/recv maps directly to the underlying queue for tensor fast path.
- Core UCX implementation remains unchanged and is reused as-is.


