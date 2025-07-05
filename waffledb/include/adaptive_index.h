// waffledb/include/adaptive_index.h
#ifndef ADAPTIVE_INDEX_H
#define ADAPTIVE_INDEX_H

#include <vector>
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <mutex>

namespace waffledb
{

    // Adaptive indexing for time-series data
    class AdaptiveIndex
    {
    private:
        struct IndexEntry
        {
            uint64_t minTime;
            uint64_t maxTime;
            size_t chunkId;
            std::string metric;
            std::unordered_map<std::string, std::unordered_set<std::string>> tagIndex;
        };

        std::vector<IndexEntry> entries_;
        std::atomic<size_t> queryCount_{0};
        std::unordered_map<std::string, size_t> queryPatterns_;
        mutable std::mutex mutex_;

    public:
        void addChunk(size_t chunkId, const std::string &metric,
                      uint64_t minTime, uint64_t maxTime,
                      const std::unordered_map<std::string, std::unordered_set<std::string>> &tags);

        std::vector<size_t> findChunks(const std::string &metric,
                                       uint64_t startTime, uint64_t endTime,
                                       const std::unordered_map<std::string, std::string> &tags);

        void recordQuery(const std::string &pattern);
        void optimize();
        void clear();
    };

} // namespace waffledb

#endif // ADAPTIVE_INDEX_H