#include "ucxq.hpp"

#include <arpa/inet.h>
#include <cstring>
#include <stdexcept>

namespace ucxq {

namespace {
// Wire framing for multipart messages:
// [uint32_t num_frames]
//   repeat num_frames times:
//     [uint32_t frame_len][frame_bytes]
// For single-frame send/recv() path we just send a single frame directly
// to avoid extra copy, aligning with tensor fast path.

static void append_u32(std::vector<uint8_t>& v, uint32_t x) {
    uint32_t be = htonl(x);
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&be);
    v.insert(v.end(), p, p + sizeof(be));
}

static uint32_t read_u32(const uint8_t* p) {
    uint32_t be;
    std::memcpy(&be, p, sizeof(be));
    return ntohl(be);
}
}

socket_t::socket_t(context_t& /*ctx*/, socket_type type) : type_(type) {}

socket_t::~socket_t() {
    if (sender_) sender_->stop();
    if (receiver_) receiver_->stop();
}

void socket_t::parse_tcp_endpoint(const std::string& endpoint, std::string& ip, int& port) {
    // Expect format tcp://IP:PORT
    const std::string prefix = "tcp://";
    if (endpoint.rfind(prefix, 0) != 0) {
        throw std::invalid_argument("Only tcp:// endpoints are supported");
    }
    auto rest = endpoint.substr(prefix.size());
    auto colon = rest.find(":");
    if (colon == std::string::npos) {
        throw std::invalid_argument("Endpoint must be tcp://IP:PORT");
    }
    ip = rest.substr(0, colon);
    port = std::stoi(rest.substr(colon + 1));
}

void socket_t::bind(const std::string& endpoint) {
    if (type_ != socket_type::pull) {
        throw std::logic_error("bind() only valid for pull sockets");
    }
    std::string ip; int port = 0;
    parse_tcp_endpoint(endpoint, ip, port);
    receiver_ = std::make_unique<ucxq::FanInQueueReceiver>(ip, port);
    receiver_->start();
}

void socket_t::connect(const std::string& endpoint) {
    if (type_ != socket_type::push) {
        throw std::logic_error("connect() only valid for push sockets");
    }
    std::string ip; int port = 0;
    parse_tcp_endpoint(endpoint, ip, port);
    sender_ = std::make_unique<ucxq::FanInQueueSender>(ip, port);
    sender_->start();
}

bool socket_t::send(buffer_view buf, send_flags::type flags) {
    if (type_ != socket_type::push || !sender_) return false;
    if (flags == send_flags::sndmore) {
        // Buffer up as part of multipart message
        if (!multipart_open_) {
            multipart_staging_.clear();
            append_u32(multipart_staging_, 0); // placeholder for num_frames
            multipart_open_ = true;
        }
        // Increment num_frames in place
        uint32_t num = read_u32(multipart_staging_.data());
        ++num;
        uint32_t be = htonl(num);
        std::memcpy(multipart_staging_.data(), &be, sizeof(be));
        append_u32(multipart_staging_, static_cast<uint32_t>(buf.size));
        const uint8_t* p = reinterpret_cast<const uint8_t*>(buf.data);
        multipart_staging_.insert(multipart_staging_.end(), p, p + buf.size);
        return true;
    }

    if (multipart_open_) {
        // This is the last frame: finalize and send all
        uint32_t num = read_u32(multipart_staging_.data());
        (void)num;
        sender_->send(multipart_staging_.data(), multipart_staging_.size());
        multipart_staging_.clear();
        multipart_open_ = false;
        return true;
    }

    // Single-frame path: send directly
    sender_->send(buf.data, buf.size);
    return true;
}

recv_result_t socket_t::recv(message_t& msg, recv_flags::type /*flags*/) {
    if (type_ != socket_type::pull || !receiver_) return std::nullopt;
    ucxq::Message m;
    if (!receiver_->dequeue(m)) return std::nullopt;

    // Detect if this is a framed multipart packet. If so, do not unpack here.
    // Single-frame path: just deliver payload.
    msg.resize(m.data.size());
    if (!m.data.empty()) {
        std::memcpy(msg.data(), m.data.data(), m.data.size());
    }
    return recv_result_t{m.data.size()};
}

recv_result_t socket_t::recv_multipart(std::vector<message_t>& out_frames) {
    if (type_ != socket_type::pull || !receiver_) return std::nullopt;
    ucxq::Message m;
    if (!receiver_->dequeue(m)) return std::nullopt;

    if (m.data.size() < 4) {
        // Treat as a single frame if not enough bytes for header
        out_frames.clear();
        out_frames.emplace_back(m.data.size());
        if (!m.data.empty()) {
            std::memcpy(out_frames.back().data(), m.data.data(), m.data.size());
        }
        return recv_result_t{1};
    }

    const uint8_t* p = m.data.data();
    const uint8_t* end = p + m.data.size();
    uint32_t num_frames = read_u32(p);
    p += 4;

    out_frames.clear();
    out_frames.reserve(num_frames);
    for (uint32_t i = 0; i < num_frames; ++i) {
        if (p + 4 > end) break;
        uint32_t len = read_u32(p);
        p += 4;
        if (p + len > end) break;
        out_frames.emplace_back(len);
        if (len) std::memcpy(out_frames.back().data(), p, len);
        p += len;
    }
    return recv_result_t{out_frames.size()};
}

} // namespace ucxq



