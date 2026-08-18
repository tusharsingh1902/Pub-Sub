#pragma once

// client_common.h
//
// Thin wrapper over a TCP connection to the broker. Used by both the
// publisher and the consumer.
//
// The connection stays open across many commands. That matters: opening a
// fresh TCP connection per message would make the benchmark measure connect
// cost rather than broker throughput.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

class BrokerClient {
public:
    BrokerClient(const std::string& host, int port) {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd_ < 0) throw std::runtime_error("socket() failed");

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));
        if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
            ::close(fd_);
            throw std::runtime_error("bad host address: " + host);
        }
        if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            ::close(fd_);
            throw std::runtime_error("connect failed -- is the broker running?");
        }

        int one = 1;
        ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    }

    ~BrokerClient() { if (fd_ >= 0) ::close(fd_); }

    BrokerClient(const BrokerClient&) = delete;
    BrokerClient& operator=(const BrokerClient&) = delete;

    void sendLine(const std::string& line) {
        std::string out = line + "\n";
        size_t sent = 0;
        while (sent < out.size()) {
            ssize_t n = ::send(fd_, out.data() + sent, out.size() - sent, 0);
            if (n <= 0) throw std::runtime_error("send failed");
            sent += static_cast<size_t>(n);
        }
    }

    // Reads one newline-terminated response line.
    std::string recvLine() {
        std::string line;
        while (true) {
            if (buf_pos_ < buf_.size()) {
                char c = buf_[buf_pos_++];
                if (c == '\n') return line;
                if (c != '\r') line.push_back(c);
                continue;
            }
            buf_.resize(4096);
            ssize_t n = ::recv(fd_, buf_.data(), buf_.size(), 0);
            if (n <= 0) throw std::runtime_error("connection closed by broker");
            buf_.resize(static_cast<size_t>(n));
            buf_pos_ = 0;
        }
    }

private:
    int fd_ = -1;
    std::string buf_;
    size_t buf_pos_ = 0;
};