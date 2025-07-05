// waffledb/include/wal.h
#pragma once

#include "waffledb.h"
#include <string>
#include <fstream>
#include <mutex>
#include <atomic>
#include <vector>

namespace waffledb
{

    struct LogEntry
    {
        uint64_t sequence;
        uint64_t timestamp;
        double value;
        std::string metric;
        std::unordered_map<std::string, std::string> tags;
    };

    class WriteAheadLog
    {
    private:
        std::string logPath_;
        std::ofstream logFile_;
        std::mutex writeMutex_;
        std::atomic<uint64_t> sequenceNumber_{0};

        // Helper methods for proper serialization
        void writeEntryToFile(const TimePoint &point);
        TimePoint parseEntry(const uint8_t *data, uint32_t entrySize, uint64_t &maxSequence);

    public:
        explicit WriteAheadLog(const std::string &basePath);
        ~WriteAheadLog();

        void append(const TimePoint &point);
        void appendBatch(const std::vector<TimePoint> &points);

        std::vector<TimePoint> recover();
        void checkpoint();
        void clear();
    };

} // namespace waffledb