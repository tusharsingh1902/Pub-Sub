// bench.cpp
//
// Load generator for the broker.
//
// Uses a fixed pool of threads on persistent connections. This matters: the
// naive approach of spawning one process per message measures fork/exec and
// TCP handshake cost, not the system under test. Here each thread connects
// once and then publishes in a loop.
//
// What is measured is end-to-end client-observed publish latency: the full
// round trip including TCP, broker processing, the log append, any fsync, and
// the response. Not broker CPU time in isolation.
//
// Note the client and broker share a machine here, so at high thread counts
// they compete for the same cores. Report that alongside the numbers.
//
// Every request is timed individually so we can report percentiles. Averages
// hide tail latency, and the tail is what users actually notice.
//
// Build:  clang++ -std=c++17 -O2 -o bench bench.cpp
//
// Usage:
//   ./bench <topic> <threads> <messages_per_thread> [payload_bytes] [warmup]
//
// The warmup argument (messages per thread, default 10% of the run) is
// published and discarded before timing starts. Without it a short run
// measures cold page cache, thread startup, TCP window growth, and CPU
// frequency ramp rather than steady-state throughput.
//
// Aim for runs of at least 10 seconds. Sub-second runs are dominated by
// startup transients and will not reproduce.
//
// Env: BROKER_HOST (default 127.0.0.1), BROKER_PORT (default 9092)

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "client_common.h"

using Clock = std::chrono::steady_clock;

static double percentile(std::vector<double>& sorted, double p) {
    if (sorted.empty()) return 0.0;
    size_t idx = static_cast<size_t>(p / 100.0 * (sorted.size() - 1));
    return sorted[idx];
}

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "usage: " << argv[0]
                  << " <topic> <threads> <messages_per_thread> [payload_bytes]\n";
        return 1;
    }

    const char* host_env = std::getenv("BROKER_HOST");
    const char* port_env = std::getenv("BROKER_PORT");
    std::string host = host_env ? host_env : "127.0.0.1";
    int port = port_env ? std::atoi(port_env) : 9092;

    std::string topic = argv[1];
    int num_threads = std::atoi(argv[2]);
    int per_thread = std::atoi(argv[3]);
    int payload = (argc > 4) ? std::atoi(argv[4]) : 100;
    int warmup = (argc > 5) ? std::atoi(argv[5]) : std::max(100, per_thread / 10);

    std::string body(static_cast<size_t>(payload), 'x');

    std::vector<std::vector<double>> latencies(num_threads);
    std::atomic<long long> errors{0};

    // All threads finish warmup before any thread starts the timed phase.
    // Otherwise fast threads begin measuring while slow ones are still warming,
    // and the measured window includes another thread's startup.
    std::atomic<int> warmed{0};
    std::atomic<bool> go{false};

    // Warm up: one connection to make sure the broker is reachable before we
    // start timing, so connection setup does not land inside the measurement.
    try {
        BrokerClient probe(host, port);
        probe.sendLine("TOPICS");
        probe.recvLine();
    } catch (const std::exception& e) {
        std::cerr << "cannot reach broker: " << e.what() << "\n";
        return 1;
    }

    std::cout << "warmup " << warmup << " msgs/thread, then "
              << (long long)num_threads * per_thread
              << " timed messages: " << num_threads << " threads x " << per_thread
              << ", payload " << payload << " bytes\n";

    auto worker = [&](int tid) {
        try {
            BrokerClient client(host, port);
            auto& lat = latencies[tid];
            lat.reserve(per_thread);

            // --- warmup: not timed, results discarded ---
            for (int i = 0; i < warmup; i++) {
                std::string key = "w" + std::to_string(tid * 1000000 + i);
                client.sendLine("PUBLISH " + topic + " " + key + " " + body);
                client.recvLine();
            }

            // Wait until every thread has finished warming up.
            warmed++;
            while (!go.load(std::memory_order_acquire))
                std::this_thread::yield();

            // --- timed phase ---
            for (int i = 0; i < per_thread; i++) {
                // Vary the key so messages spread across partitions the same
                // way real traffic would.
                std::string key = "k" + std::to_string(tid * 1000000 + i);

                auto t0 = Clock::now();
                client.sendLine("PUBLISH " + topic + " " + key + " " + body);
                std::string resp = client.recvLine();
                auto t1 = Clock::now();

                // Only successful publishes count. A failed request has a
                // meaningless latency (often faster, since no disk write
                // happened) and would drag the percentiles down while
                // inflating the message total.
                if (resp.rfind("OK", 0) == 0) {
                    lat.push_back(
                        std::chrono::duration<double, std::milli>(t1 - t0).count());
                } else {
                    errors++;
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "thread " << tid << " failed: " << e.what() << "\n";
            errors += per_thread;
            warmed++;  // do not deadlock the barrier
        }
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) threads.emplace_back(worker, t);

    // Release the barrier once everyone is warm, and only start the clock then.
    while (warmed.load() < num_threads) std::this_thread::yield();
    auto start = Clock::now();
    go.store(true, std::memory_order_release);

    for (auto& t : threads) t.join();
    auto end = Clock::now();
    double secs = std::chrono::duration<double>(end - start).count();

    std::vector<double> all;
    for (auto& v : latencies) all.insert(all.end(), v.begin(), v.end());
    std::sort(all.begin(), all.end());

    long long total = static_cast<long long>(all.size());
    double throughput = total / secs;

    std::cout << std::fixed << std::setprecision(3);
    std::cout << "\n--- results ---\n";
    std::cout << "successful   " << total << "\n";
    std::cout << "errors       " << errors.load() << "\n";
    std::cout << "elapsed      " << secs << " s\n";
    std::cout << "throughput   " << std::setprecision(0) << throughput
              << " msgs/sec\n";
    std::cout << std::setprecision(3);
    std::cout << "latency p50  " << percentile(all, 50)   << " ms\n";
    std::cout << "latency p95  " << percentile(all, 95)   << " ms\n";
    std::cout << "latency p99  " << percentile(all, 99)   << " ms\n";
    std::cout << "latency p999 " << percentile(all, 99.9) << " ms\n";
    std::cout << "latency max  " << (all.empty() ? 0.0 : all.back()) << " ms\n";

    if (secs < 5.0) {
        std::cout << "\nWARNING: timed window was only " << std::setprecision(2)
                  << secs << " s. Short runs are dominated by startup effects "
                  << "and vary between runs. Increase messages per thread until "
                  << "the window is at least 10 s.\n";
    }

    return 0;
}