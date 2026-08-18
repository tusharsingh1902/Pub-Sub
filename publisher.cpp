// publisher.cpp
//
// Sends messages to a topic on the broker.
//
// Build:  clang++ -std=c++17 -O2 -o publisher publisher.cpp
//
// Usage:
//   ./publisher <topic> <key> <message>          send one message
//   ./publisher <topic> <key> <message> <count>  send it <count> times
//   ./publisher <topic> --interactive            type messages as "key message"
//
// Env: BROKER_HOST (default 127.0.0.1), BROKER_PORT (default 9092)

#include <cstdlib>
#include <iostream>
#include <string>

#include "client_common.h"

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: " << argv[0]
                  << " <topic> <key> <message> [count]\n"
                  << "       " << argv[0] << " <topic> --interactive\n";
        return 1;
    }

    const char* host_env = std::getenv("BROKER_HOST");
    const char* port_env = std::getenv("BROKER_PORT");
    std::string host = host_env ? host_env : "127.0.0.1";
    int port = port_env ? std::atoi(port_env) : 9092;

    std::string topic = argv[1];

    try {
        BrokerClient client(host, port);

        if (std::string(argv[2]) == "--interactive") {
            std::cout << "connected to " << host << ":" << port
                      << ". type: <key> <message>   (ctrl-D to quit)\n";
            std::string line;
            while (std::getline(std::cin, line)) {
                if (line.empty()) continue;
                size_t sp = line.find(' ');
                if (sp == std::string::npos) {
                    std::cout << "  need: <key> <message>\n";
                    continue;
                }
                std::string key = line.substr(0, sp);
                std::string msg = line.substr(sp + 1);
                client.sendLine("PUBLISH " + topic + " " + key + " " + msg);
                std::cout << "  " << client.recvLine() << "\n";
            }
            return 0;
        }

        if (argc < 4) {
            std::cerr << "need a message\n";
            return 1;
        }

        std::string key = argv[2];
        std::string message = argv[3];
        int count = (argc > 4) ? std::atoi(argv[4]) : 1;

        for (int i = 0; i < count; i++) {
            std::string body = message;
            if (count > 1) body += " #" + std::to_string(i);
            client.sendLine("PUBLISH " + topic + " " + key + " " + body);
            std::string resp = client.recvLine();
            if (count <= 10 || i % 100 == 0)
                std::cout << resp << "\n";
        }
        if (count > 1)
            std::cout << "published " << count << " messages\n";

    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}