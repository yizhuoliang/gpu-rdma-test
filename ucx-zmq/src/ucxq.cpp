#include "ucxq.hpp"

#include <arpa/inet.h>
#include <cstring>
#include <stdexcept>

namespace ucxq {

namespace {
// Wire framing for multipart messages:
// [uint32_t magic = kMultipartMagic]
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

static void store_u32(std::vector<uint8_t>& v, size_t offset, uint32_t value) {
    uint32_t be = htonl(value);
    std::memcpy(v.data() + offset, &be, sizeof(be));
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

    auto append_frame = [&](buffer_view frame) {
        append_u32(multipart_staging_, static_cast<uint32_t>(frame.size));
        if (frame.size && frame.data != nullptr) {
            const uint8_t* p = reinterpret_cast<const uint8_t*>(frame.data);
            multipart_staging_.insert(multipart_staging_.end(), p, p + frame.size);
        }
        ++multipart_frame_count_;
        store_u32(multipart_staging_, sizeof(uint32_t), multipart_frame_count_);
    };

    if (flags == send_flags::sndmore) {
        if (!multipart_open_) {
            multipart_staging_.clear();
            multipart_staging_.reserve(sizeof(uint32_t) * 2 + buf.size);
            append_u32(multipart_staging_, kMultipartMagic);
            append_u32(multipart_staging_, 0); // placeholder for num_frames
            multipart_frame_count_ = 0;
            multipart_open_ = true;
        }
        append_frame(buf);
        return true;
    }

    if (multipart_open_) {
        append_frame(buf);
        sender_->send(multipart_staging_.data(), multipart_staging_.size());
        multipart_staging_.clear();
        multipart_open_ = false;
        multipart_frame_count_ = 0;
        return true;
    }

    sender_->send(buf.data, buf.size);
    return true;
}

recv_result_t socket_t::recv(message_t& msg, recv_flags::type /*flags*/) {
    if (type_ != socket_type::pull || !receiver_) return std::nullopt;
    ucxq::Message m;
    if (!receiver_->dequeue(m)) return std::nullopt;

    // Deliver payload as-is. Callers needing multipart semantics should use recv_multipart().
    const size_t bytes = m.data.size();
    msg.assign(std::move(m.data));
    return recv_result_t{bytes};
}

recv_result_t socket_t::recv_multipart(std::vector<message_t>& out_frames) {
    if (type_ != socket_type::pull || !receiver_) return std::nullopt;
    ucxq::Message m;
    if (!receiver_->dequeue(m)) return std::nullopt;

    std::vector<uint8_t> payload = std::move(m.data);
    if (payload.size() < sizeof(uint32_t) * 2) {
        out_frames.clear();
        out_frames.emplace_back();
        out_frames.back().assign(std::move(payload));
        return recv_result_t{1};
    }

    const uint8_t* p = payload.data();
    const uint8_t* end = p + payload.size();
    uint32_t magic = read_u32(p);
    p += sizeof(uint32_t);
    if (magic != kMultipartMagic) {
        out_frames.clear();
        out_frames.emplace_back();
        out_frames.back().assign(std::move(payload));
        return recv_result_t{1};
    }

    uint32_t num_frames = read_u32(p);
    p += sizeof(uint32_t);

    out_frames.clear();
    out_frames.reserve(num_frames);
    for (uint32_t i = 0; i < num_frames; ++i) {
        if (p + sizeof(uint32_t) > end) break;
        uint32_t len = read_u32(p);
        p += sizeof(uint32_t);
        if (p + len > end) break;
        out_frames.emplace_back();
        out_frames.back().assign(p, len);
        p += len;
    }
    return recv_result_t{out_frames.size()};
}

} // namespace ucxq
