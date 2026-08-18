#pragma once

// consumer_group.h
//
// Two jobs:
//
//   1. Assignment. A group has N members and a topic has P partitions. Each
//      partition goes to exactly one member, so within a group every message
//      is handled once. Members are sorted by id and dealt partitions
//      round-robin, so every member computes the same assignment from the
//      same membership list.
//
//   2. Committed offsets. The group records "we have processed up to offset
//      K on partition P". Persisted to disk, so restarting the whole group
//      resumes rather than replaying from zero.
//
// Note that different groups are completely independent -- each gets its own
// copy of every message. That is the pub-sub half. Within one group each
// message goes to one member. That is the queue half.

#include <algorithm>
#include <chrono>
#include <fstream>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

class GroupRegistry {
public:
    GroupRegistry(const std::string& data_dir) : path_(data_dir + "/offsets.txt") {
        load();
    }

    // Adds a member and returns the partitions it now owns.
    std::vector<int> join(const std::string& group, const std::string& member,
                          const std::string& topic, int num_partitions) {
        std::lock_guard<std::mutex> lock(mu_);
        auto& g = groups_[group];
        g.members.insert(member);
        g.topic = topic;
        g.num_partitions = num_partitions;
        return assignmentFor(g, member);
    }

    void leave(const std::string& group, const std::string& member) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = groups_.find(group);
        if (it != groups_.end()) it->second.members.erase(member);
    }

    std::vector<int> assignment(const std::string& group, const std::string& member) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = groups_.find(group);
        if (it == groups_.end()) return {};
        return assignmentFor(it->second, member);
    }

    // Where this group should resume reading a partition.
    int64_t committed(const std::string& group, const std::string& topic, int partition) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = offsets_.find(key(group, topic, partition));
        return it == offsets_.end() ? 0 : it->second;
    }

    // Records progress. Only moves forward -- a late commit from a stale
    // member must never rewind the group.
    void commit(const std::string& group, const std::string& topic, int partition,
                int64_t offset) {
        std::lock_guard<std::mutex> lock(mu_);
        std::string k = key(group, topic, partition);
        auto it = offsets_.find(k);
        if (it == offsets_.end() || offset > it->second) {
            offsets_[k] = offset;
            persist();
        }
    }

    std::vector<std::string> members(const std::string& group) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = groups_.find(group);
        if (it == groups_.end()) return {};
        return std::vector<std::string>(it->second.members.begin(),
                                        it->second.members.end());
    }

    std::vector<std::string> listGroups() {
        std::lock_guard<std::mutex> lock(mu_);
        std::vector<std::string> out;
        for (auto& kv : groups_) out.push_back(kv.first);
        return out;
    }

private:
    struct Group {
        std::set<std::string> members;  // sorted, so assignment is deterministic
        std::string topic;
        int num_partitions = 0;
    };

    // Round-robin: with members [a,b] and 5 partitions, a gets 0,2,4 and
    // b gets 1,3. Every member derives the same answer independently.
    std::vector<int> assignmentFor(const Group& g, const std::string& member) {
        std::vector<int> out;
        std::vector<std::string> sorted(g.members.begin(), g.members.end());
        auto it = std::find(sorted.begin(), sorted.end(), member);
        if (it == sorted.end()) return out;

        size_t idx = static_cast<size_t>(std::distance(sorted.begin(), it));
        size_t n = sorted.size();
        for (int p = 0; p < g.num_partitions; p++)
            if (static_cast<size_t>(p) % n == idx) out.push_back(p);
        return out;
    }

    static std::string key(const std::string& group, const std::string& topic,
                           int partition) {
        return group + "|" + topic + "|" + std::to_string(partition);
    }

    void load() {
        std::ifstream in(path_);
        if (!in) return;
        std::string k;
        int64_t v;
        while (in >> k >> v) offsets_[k] = v;
    }

    // Rewrites the whole file. Fine at this scale; a real broker would append
    // to a log and compact it, since rewriting gets expensive with many groups.
    void persist() {
        std::ofstream out(path_, std::ios::trunc);
        if (!out) return;
        for (const auto& kv : offsets_) out << kv.first << " " << kv.second << "\n";
    }

    std::string path_;
    std::map<std::string, Group> groups_;
    std::map<std::string, int64_t> offsets_;
    std::mutex mu_;
};