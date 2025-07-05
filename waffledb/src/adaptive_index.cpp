// waffledb/src/adaptive_index.cpp
#include "adaptive_index.h"
#include <algorithm>
#include <mutex>

namespace waffledb
{

    void AdaptiveIndex::addChunk(size_t chunkId, const std::string &metric,
                                 uint64_t minTime, uint64_t maxTime,
                                 const std::unordered_map<std::string, std::unordered_set<std::string>> &tags)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        IndexEntry entry;
        entry.chunkId = chunkId;
        entry.metric = metric;
        entry.minTime = minTime;
        entry.maxTime = maxTime;
        entry.tagIndex = tags;

        entries_.push_back(entry);
    }

    std::vector<size_t> AdaptiveIndex::findChunks(const std::string &metric,
                                                  uint64_t startTime, uint64_t endTime,
                                                  const std::unordered_map<std::string, std::string> &tags)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        std::vector<size_t> result;

        // Record query pattern
        std::string pattern = metric;
        for (const auto &[key, value] : tags)
        {
            pattern += ":" + key + "=" + value;
        }
        queryPatterns_[pattern]++;
        queryCount_++;

        // Find matching chunks
        for (const auto &entry : entries_)
        {
            // Check metric
            if (entry.metric != metric)
            {
                continue;
            }

            // Check time range overlap
            if (entry.maxTime < startTime || entry.minTime > endTime)
            {
                continue;
            }

            // Check tags
            bool tagsMatch = true;
            for (const auto &[key, value] : tags)
            {
                auto it = entry.tagIndex.find(key);
                if (it == entry.tagIndex.end() || it->second.find(value) == it->second.end())
                {
                    tagsMatch = false;
                    break;
                }
            }

            if (tagsMatch)
            {
                result.push_back(entry.chunkId);
            }
        }

        // Trigger optimization if query count is high
        if (queryCount_ % 1000 == 0)
        {
            optimize();
        }

        return result;
    }

    void AdaptiveIndex::recordQuery(const std::string &pattern)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queryPatterns_[pattern]++;
    }

    void AdaptiveIndex::optimize()
    {
        // Analyze query patterns and create specialized indexes
        // for frequently accessed data

        // Find most common query patterns
        std::vector<std::pair<std::string, size_t>> patterns;
        for (const auto &[pattern, count] : queryPatterns_)
        {
            patterns.push_back({pattern, count});
        }

        // Sort by frequency
        std::sort(patterns.begin(), patterns.end(),
                  [](const auto &a, const auto &b)
                  {
                      return a.second > b.second;
                  });

        // In a real implementation, we would:
        // 1. Create specialized indexes for top patterns
        // 2. Reorganize chunks for better locality
        // 3. Build bloom filters for tag combinations
        // 4. Create time-based partitions

        // For now, just sort entries by metric and time for binary search
        std::sort(entries_.begin(), entries_.end(),
                  [](const IndexEntry &a, const IndexEntry &b)
                  {
                      if (a.metric != b.metric)
                      {
                          return a.metric < b.metric;
                      }
                      return a.minTime < b.minTime;
                  });
    }

    void AdaptiveIndex::clear()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        entries_.clear();
        queryPatterns_.clear();
        queryCount_ = 0;
    }

} // namespace waffledb