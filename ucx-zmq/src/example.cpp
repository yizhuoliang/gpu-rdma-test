#include "ucx_zmq.hpp"
#include <iostream>
#include <thread>

int main() {
    using namespace ucxq;
    context_t ctx(1);

    socket_t pull(ctx, socket_type::pull);
    socket_t push(ctx, socket_type::push);

    pull.bind("tcp://127.0.0.1:24000");
    push.connect("tcp://127.0.0.1:24000");

    std::thread t([&](){
        push.send(str_buffer("peer_0"), send_flags::sndmore);
        const char payload[] = "hello";
        push.send(buffer(payload, sizeof(payload)));
    });

    std::vector<message_t> frames;
    auto r = pull.recv_multipart(frames);
    if (r && *r == 2) {
        std::cout << "peer: " << frames[0].to_string() << " size: " << frames[1].size() << std::endl;
    } else {
        std::cout << "recv failed" << std::endl;
    }
    t.join();
    return 0;
}



