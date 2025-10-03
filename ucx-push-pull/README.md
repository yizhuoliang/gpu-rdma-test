UCX Push-Pull Wrapper

A UCX-backed socket abstraction that mirrors ZeroMQ's PUSH/PULL roles with sender-side bind semantics. The sender exposes a well-known TCP endpoint for out-of-band address exchange, while receivers actively connect and initiate the UCX endpoint creation.

API Overview

- `socket_t(socket_type::{push,pull})`
- `socket.bind("tcp://IP:PORT")` (push)
- `socket.connect("tcp://IP:PORT")` (pull)
- `send(buffer, send_flags::{none,sndmore})` (multipart: use `sndmore` for all but last frame)
- `recv(message_t&)` single-frame
- `recv_multipart(std::vector<message_t>&)` multi-frame (peer_id + metadata)
- `message_t::to_string()`, `.data()`, `.size()`
- `buffer(ptr, size)`, `str_buffer(cstr)`

Build

```bash
cmake -S ucx-push-pull -B ucx-push-pull/build -D CMAKE_BUILD_TYPE=Release
cmake --build ucx-push-pull/build -j
```

Requires UCX (libucp + libucs).

Notes

- Multipart framing matches the original fan-in queue implementation.
- Single-frame send/recv still maps directly onto the underlying queue for minimal overhead.
- The core UCX data path remains unchanged; only the out-of-band setup swaps bind/connect roles.
