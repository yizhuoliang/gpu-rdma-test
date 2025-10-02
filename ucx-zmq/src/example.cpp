#include "ucxq.hpp"
#include <iostream>
#include <string>
#include <vector>

int main(int argc, char** argv) {
    using namespace ucxq;

    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <server|client> tcp://IP:PORT" << std::endl;
        return 1;
    }

    std::string role = argv[1];
    std::string endpoint = argv[2];

    context_t ctx(1);

    if (role == "server") {
        socket_t pull(ctx, socket_type::pull);
        pull.bind(endpoint);

        std::vector<message_t> frames;
        auto r = pull.recv_multipart(frames);
        if (r && *r >= 1) {
            std::cout << "received " << *r << " frame(s)" << std::endl;
            for (size_t i = 0; i < frames.size(); ++i) {
                std::cout << "  frame[" << i << "]: size=" << frames[i].size() << std::endl;
            }
            if (frames.size() >= 2) {
                std::cout << "peer: " << frames[0].to_string() << " payload_size: " << frames[1].size() << std::endl;
            }
            return 0;
        } else {
            std::cerr << "recv failed" << std::endl;
            return 2;
        }
    } else if (role == "client") {
        socket_t push(ctx, socket_type::push);
        push.connect(endpoint);

        const char* peer = "peer_0";
        const char payload[] = "hello_from_client";
        push.send(str_buffer(peer), send_flags::sndmore);
        push.send(buffer(payload, sizeof(payload)));
        std::cout << "sent 2-frame multipart" << std::endl;
        return 0;
    } else {
        std::cerr << "Unknown role: " << role << std::endl;
        return 3;
    }
}

