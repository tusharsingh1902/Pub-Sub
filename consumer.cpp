// consumer.cpp
//
// Reads a partition, remembering where it got to.
//
// The consumer -- not the broker -- tracks its position. It asks for
// everything from offset N, prints it, then sets N to the offset after the
// last message it saw. This is what makes replay possible and what lets
// several independent consumers read the same partition at different speeds.
//
// Build:  clang++ -std=c++17 -O2 -o consumer consumer.cpp
//
// Usage:
//   ./consumer <topic> <partition> [start_offset]
//   ./consumer <topic> <partition> --from-start
//
// Env: BROKER_HOST (default 127.0.0.1), BROKER_PORT (default 9092)

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

#include "client_common.h"

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: " << argv[0]
                  << " <topic> <partition> [start_offset|--from-start]\n";
        return 1;
    }

    const char* host_env = std::getenv("BROKER_HOST");
    const char* port_env = std::getenv("BROKER_PORT");
    std::string host = host_env ? host_env : "127.0.0.1";
    int port = port_env ? std::atoi(port_env) : 9092;

    std::string topic = argv[1];
    int partition = std::atoi(argv[2]);

    long long offset = 0;
    if (argc > 3 && std::string(argv[3]) != "--from-start")
        offset = std::atoll(argv[3]);

    try {
        BrokerClient client(host, port);
        std::cout << "consuming " << topic << " partition " << partition
                  << " from offset " << offset << " (ctrl-C to stop)\n";

        while (true) {
            client.sendLine("FETCH " + topic + " " + std::to_string(partition) +
                            " " + std::to_string(offset));

            std::string header = client.recvLine();
            if (header.rfind("ERR", 0) == 0) {
                std::cout << header << "\n";
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }

            std::istringstream iss(header);
            std::string tag;
            int count = 0;
            iss >> tag >> count;

            for (int i = 0; i < count; i++) {
                std::string line = client.recvLine();
                size_t sp = line.find(' ');
                long long off = std::atoll(line.substr(0, sp).c_str());
                std::string msg = (sp == std::string::npos) ? "" : line.substr(sp + 1);

                std::cout << "[offset " << off << "] " << msg << "\n";

                // Advance past what we just consumed. If this process dies
                // before here, we re-read those messages on restart --
                // at-least-once delivery.
                offset = off + 1;
            }

            if (count == 0)
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }

    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}