// broker_server_unbuffered.cpp -- BASELINE for comparison, do not ship
//
// TCP server. One thread per connection.
//
// Protocol -- one command per line, newline terminated:
//     CREATE <topic> <num_partitions>
//     PUBLISH <topic> <key> <message with spaces is fine>
//     FETCH <topic> <partition> <offset>
//     TOPICS
//     JOIN <group> <member> <topic>
//     POLL <group> <member> <topic>
//     COMMIT <group> <topic> <partition> <offset>
//     GROUPS
//
// Responses:
//     OK <partition> <offset>        after PUBLISH
//     OK created                     after CREATE
//     MSGS <count>                   then <count> lines, each: <offset> <message>
//     TOPICS <count>                 then <count> lines, each: <name> <partitions>
//     ERR <reason>
//
// Build:  clang++ -std=c++17 -O2 -o broker_server broker_server.cpp
// Run:    mkdir -p data && ./broker_server 9092

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "broker.h"
#include "consumer_group.h"

static std::atomic<bool> g_running{true};

// UNBUFFERED baseline -- one recv() syscall per byte.
//
// This is the original implementation, kept so the buffered version can be
// measured against it under identical conditions. A ~124-byte command costs
// ~124 syscalls here.
//
// Build this file as broker_server_unbuffered and run the same benchmark
// against both binaries to get a matched comparison.
static bool readLineUnbuffered(int fd, std::string& line) {
    line.clear();
    char c;
    while (true) {
        ssize_t n = ::recv(fd, &c, 1, 0);
        if (n <= 0) return false;
        if (c == '\n') return true;
        if (c != '\r') line.push_back(c);
        if (line.size() > 1u << 20) return false;
    }
}


static bool sendAll(int fd, const std::string& s) {
    size_t sent = 0;
    while (sent < s.size()) {
        ssize_t n = ::send(fd, s.data() + sent, s.size() - sent, 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

static void handleClient(int client_fd, Broker* broker, GroupRegistry* groups) {
    // Disable Nagle so small replies go out immediately instead of being
    // buffered waiting for more data. Matters a lot for request/response.
    int one = 1;
    ::setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    std::string line;
    while (g_running && readLineUnbuffered(client_fd, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;

        try {
            if (cmd == "CREATE") {
                std::string name;
                int parts = 0;
                iss >> name >> parts;
                if (name.empty() || parts < 1) {
                    sendAll(client_fd, "ERR usage: CREATE <topic> <partitions>\n");
                    continue;
                }
                broker->createTopic(name, parts);
                sendAll(client_fd, "OK created\n");

            } else if (cmd == "PUBLISH") {
                std::string name, key;
                iss >> name >> key;
                std::string msg;
                std::getline(iss, msg);
                if (!msg.empty() && msg[0] == ' ') msg.erase(0, 1);

                Topic* t = broker->getTopic(name);
                if (!t) { sendAll(client_fd, "ERR no such topic\n"); continue; }

                Record r = t->publish(key, msg);
                sendAll(client_fd, "OK " + std::to_string(r.partition) + " " +
                                       std::to_string(r.offset) + "\n");

            } else if (cmd == "FETCH") {
                std::string name;
                int part = 0;
                long long off = 0;
                iss >> name >> part >> off;

                Topic* t = broker->getTopic(name);
                if (!t) { sendAll(client_fd, "ERR no such topic\n"); continue; }
                if (part < 0 || part >= t->numPartitions()) {
                    sendAll(client_fd, "ERR bad partition\n"); continue;
                }

                auto msgs = t->readFrom(part, off);
                std::string out = "MSGS " + std::to_string(msgs.size()) + "\n";
                long long o = off;
                for (const auto& m : msgs)
                    out += std::to_string(o++) + " " + m + "\n";
                sendAll(client_fd, out);

            } else if (cmd == "TOPICS") {
                auto list = broker->listTopics();
                std::string out = "TOPICS " + std::to_string(list.size()) + "\n";
                for (auto& kv : list)
                    out += kv.first + " " + std::to_string(kv.second) + "\n";
                sendAll(client_fd, out);

            } else if (cmd == "JOIN") {
                std::string group, member, name;
                iss >> group >> member >> name;
                Topic* t = broker->getTopic(name);
                if (!t) { sendAll(client_fd, "ERR no such topic\n"); continue; }

                auto parts = groups->join(group, member, name, t->numPartitions());
                std::string out = "ASSIGNED " + std::to_string(parts.size()) + "\n";
                for (int p : parts)
                    out += std::to_string(p) + " " +
                           std::to_string(groups->committed(group, name, p)) + "\n";
                sendAll(client_fd, out);

            } else if (cmd == "POLL") {
                std::string group, member, name;
                iss >> group >> member >> name;
                Topic* t = broker->getTopic(name);
                if (!t) { sendAll(client_fd, "ERR no such topic\n"); continue; }

                auto parts = groups->assignment(group, member);
                std::string out;
                int total = 0;
                std::string body;
                for (int p : parts) {
                    int64_t from = groups->committed(group, name, p);
                    auto msgs = t->readFrom(p, from);
                    int64_t o = from;
                    for (const auto& m : msgs) {
                        body += std::to_string(p) + " " + std::to_string(o++) +
                                " " + m + "\n";
                        total++;
                    }
                }
                out = "RECORDS " + std::to_string(total) + "\n" + body;
                sendAll(client_fd, out);

            } else if (cmd == "COMMIT") {
                std::string group, name;
                int part = 0;
                long long off = 0;
                iss >> group >> name >> part >> off;
                groups->commit(group, name, part, off);
                sendAll(client_fd, "OK committed\n");

            } else if (cmd == "GROUPS") {
                auto gl = groups->listGroups();
                std::string out = "GROUPS " + std::to_string(gl.size()) + "\n";
                for (auto& g : gl) {
                    out += g;
                    for (auto& m : groups->members(g)) out += " " + m;
                    out += "\n";
                }
                sendAll(client_fd, out);

            } else if (cmd == "QUIT") {
                sendAll(client_fd, "OK bye\n");
                break;

            } else {
                sendAll(client_fd, "ERR unknown command\n");
            }
        } catch (const std::exception& e) {
            sendAll(client_fd, std::string("ERR ") + e.what() + "\n");
        }
    }
    ::close(client_fd);
}

int main(int argc, char** argv) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 9092;
    std::string data_dir = (argc > 2) ? argv[2] : "data";

    // Sync policy as an argument, not a compile-time constant. Baking it into
    // the binary makes it far too easy to benchmark one policy while believing
    // you are measuring another.
    std::string sync_arg = (argc > 3) ? argv[3] : "everyn";
    int sync_n = (argc > 4) ? std::atoi(argv[4]) : 100;

    SyncPolicy policy;
    std::string policy_desc;
    if (sync_arg == "none") {
        policy = SyncPolicy::None;
        policy_desc = "none (no fsync; a crash loses whatever the OS had buffered)";
    } else if (sync_arg == "always") {
        policy = SyncPolicy::Always;
        policy_desc = "always (fsync every message; slowest, loses nothing)";
    } else if (sync_arg == "everyn") {
        policy = SyncPolicy::EveryN;
        policy_desc = "everyn:" + std::to_string(sync_n) +
                      " (fsync every " + std::to_string(sync_n) +
                      " appends; bounded loss)";
    } else {
        std::cerr << "usage: " << argv[0]
                  << " [port] [data_dir] [none|everyn|always] [n]\n";
        return 1;
    }

    // Without this, writing to a socket the peer already closed kills the
    // whole process instead of just failing the send().
    ::signal(SIGPIPE, SIG_IGN);

    Broker broker(data_dir, policy, sync_n);
    GroupRegistry groups(data_dir);

    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }

    int opt = 1;
    ::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (::bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind"); ::close(server_fd); return 1;
    }
    if (::listen(server_fd, 128) < 0) {
        perror("listen"); ::close(server_fd); return 1;
    }

    std::cout << "broker listening on port " << port
              << ", data dir '" << data_dir << "'\n"
              << "sync policy: " << policy_desc << "\n";

    while (g_running) {
        sockaddr_in caddr{};
        socklen_t clen = sizeof(caddr);
        int client_fd = ::accept(server_fd,
                                 reinterpret_cast<sockaddr*>(&caddr), &clen);
        if (client_fd < 0) continue;

        std::thread(handleClient, client_fd, &broker, &groups).detach();
    }

    ::close(server_fd);
    return 0;
}