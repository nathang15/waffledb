// waffledb/src/wal.cpp
#include "wal.h"
#include <filesystem>
#include <iostream>
#include <sstream>
#include <cstring>

namespace fs = std::filesystem;

namespace waffledb
{

    WriteAheadLog::WriteAheadLog(const std::string &basePath)
        : logPath_(basePath + "/wal.log")
    {
        // Create directory if it doesn't exist
        fs::create_directories(basePath);

        // Open log file in append mode
        logFile_.open(logPath_, std::ios::out | std::ios::app | std::ios::binary);
        if (!logFile_)
        {
            throw std::runtime_error("Failed to open WAL file: " + logPath_);
        }
    }

    WriteAheadLog::~WriteAheadLog()
    {
        if (logFile_.is_open())
        {
            logFile_.flush();
            logFile_.close();
        }
    }

    void WriteAheadLog::append(const TimePoint &point)
    {
        std::lock_guard<std::mutex> lock(writeMutex_);
        writeEntryToFile(point);
    }

    void WriteAheadLog::appendBatch(const std::vector<TimePoint> &points)
    {
        std::lock_guard<std::mutex> lock(writeMutex_);

        for (const auto &point : points)
        {
            writeEntryToFile(point);
        }
    }

    void WriteAheadLog::writeEntryToFile(const TimePoint &point)
    {
        // Create log entry with proper structure
        LogEntry entry;
        entry.sequence = sequenceNumber_.fetch_add(1);
        entry.timestamp = point.timestamp;
        entry.value = point.value;
        entry.metric = point.metric;
        entry.tags = point.tags;

        // Calculate total size needed
        size_t totalSize = sizeof(uint32_t) +      // entry size
                           sizeof(uint64_t) +      // sequence
                           sizeof(uint64_t) +      // timestamp
                           sizeof(double) +        // value
                           sizeof(uint32_t) +      // metric length
                           entry.metric.length() + // metric data
                           sizeof(uint32_t);       // tag count

        for (const auto &[key, value] : entry.tags)
        {
            totalSize += sizeof(uint32_t) + key.length() +  // key length + key data
                         sizeof(uint32_t) + value.length(); // value length + value data
        }

        // Create buffer with exact size
        std::vector<uint8_t> buffer;
        buffer.reserve(totalSize);

        // Write entry size first (for validation during recovery)
        uint32_t entrySize = static_cast<uint32_t>(totalSize - sizeof(uint32_t));
        buffer.insert(buffer.end(),
                      reinterpret_cast<const uint8_t *>(&entrySize),
                      reinterpret_cast<const uint8_t *>(&entrySize) + sizeof(uint32_t));

        // Write sequence number
        buffer.insert(buffer.end(),
                      reinterpret_cast<const uint8_t *>(&entry.sequence),
                      reinterpret_cast<const uint8_t *>(&entry.sequence) + sizeof(uint64_t));

        // Write timestamp
        buffer.insert(buffer.end(),
                      reinterpret_cast<const uint8_t *>(&entry.timestamp),
                      reinterpret_cast<const uint8_t *>(&entry.timestamp) + sizeof(uint64_t));

        // Write value
        buffer.insert(buffer.end(),
                      reinterpret_cast<const uint8_t *>(&entry.value),
                      reinterpret_cast<const uint8_t *>(&entry.value) + sizeof(double));

        // Write metric
        uint32_t metricLen = static_cast<uint32_t>(entry.metric.length());
        buffer.insert(buffer.end(),
                      reinterpret_cast<const uint8_t *>(&metricLen),
                      reinterpret_cast<const uint8_t *>(&metricLen) + sizeof(uint32_t));
        buffer.insert(buffer.end(),
                      reinterpret_cast<const uint8_t *>(entry.metric.data()),
                      reinterpret_cast<const uint8_t *>(entry.metric.data()) + entry.metric.length());

        // Write tags
        uint32_t tagCount = static_cast<uint32_t>(entry.tags.size());
        buffer.insert(buffer.end(),
                      reinterpret_cast<const uint8_t *>(&tagCount),
                      reinterpret_cast<const uint8_t *>(&tagCount) + sizeof(uint32_t));

        for (const auto &[key, value] : entry.tags)
        {
            // Write key
            uint32_t keyLen = static_cast<uint32_t>(key.length());
            buffer.insert(buffer.end(),
                          reinterpret_cast<const uint8_t *>(&keyLen),
                          reinterpret_cast<const uint8_t *>(&keyLen) + sizeof(uint32_t));
            buffer.insert(buffer.end(),
                          reinterpret_cast<const uint8_t *>(key.data()),
                          reinterpret_cast<const uint8_t *>(key.data()) + key.length());

            // Write value
            uint32_t valueLen = static_cast<uint32_t>(value.length());
            buffer.insert(buffer.end(),
                          reinterpret_cast<const uint8_t *>(&valueLen),
                          reinterpret_cast<const uint8_t *>(&valueLen) + sizeof(uint32_t));
            buffer.insert(buffer.end(),
                          reinterpret_cast<const uint8_t *>(value.data()),
                          reinterpret_cast<const uint8_t *>(value.data()) + value.length());
        }

        // Write to file
        logFile_.write(reinterpret_cast<const char *>(buffer.data()), buffer.size());
        logFile_.flush();
    }

    std::vector<TimePoint> WriteAheadLog::recover()
    {
        std::vector<TimePoint> points;

        std::ifstream readFile(logPath_, std::ios::in | std::ios::binary);
        if (!readFile)
        {
            return points; // No WAL file, nothing to recover
        }

        // Get file size
        readFile.seekg(0, std::ios::end);
        size_t fileSize = readFile.tellg();
        readFile.seekg(0, std::ios::beg);

        if (fileSize == 0)
        {
            return points;
        }

        // Read entire file into buffer for safer parsing
        std::vector<uint8_t> fileBuffer(fileSize);
        readFile.read(reinterpret_cast<char *>(fileBuffer.data()), fileSize);
        readFile.close();

        size_t offset = 0;
        uint64_t maxSequence = 0;

        while (offset < fileSize)
        {
            // Safety check for remaining bytes
            if (offset + sizeof(uint32_t) > fileSize)
            {
                std::cerr << "WAL: Incomplete entry size at offset " << offset << std::endl;
                break;
            }

            // Read entry size
            uint32_t entrySize;
            std::memcpy(&entrySize, fileBuffer.data() + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);

            // Validate entry size
            if (entrySize == 0 || offset + entrySize > fileSize)
            {
                std::cerr << "WAL: Invalid entry size " << entrySize << " at offset " << offset << std::endl;
                break;
            }

            // Read entry data
            try
            {
                TimePoint point = parseEntry(fileBuffer.data() + offset, entrySize, maxSequence);
                points.push_back(point);
                offset += entrySize;
            }
            catch (const std::exception &e)
            {
                std::cerr << "WAL: Failed to parse entry at offset " << offset << ": " << e.what() << std::endl;
                break;
            }
        }

        // Update sequence number
        sequenceNumber_.store(maxSequence + 1);

        return points;
    }

    TimePoint WriteAheadLog::parseEntry(const uint8_t *data, uint32_t entrySize, uint64_t &maxSequence)
    {
        size_t offset = 0;
        TimePoint point;

        // Read sequence
        if (offset + sizeof(uint64_t) > entrySize)
        {
            throw std::runtime_error("Entry too small for sequence");
        }
        uint64_t sequence;
        std::memcpy(&sequence, data + offset, sizeof(uint64_t));
        offset += sizeof(uint64_t);
        maxSequence = std::max(maxSequence, sequence);

        // Read timestamp
        if (offset + sizeof(uint64_t) > entrySize)
        {
            throw std::runtime_error("Entry too small for timestamp");
        }
        std::memcpy(&point.timestamp, data + offset, sizeof(uint64_t));
        offset += sizeof(uint64_t);

        // Read value
        if (offset + sizeof(double) > entrySize)
        {
            throw std::runtime_error("Entry too small for value");
        }
        std::memcpy(&point.value, data + offset, sizeof(double));
        offset += sizeof(double);

        // Read metric
        if (offset + sizeof(uint32_t) > entrySize)
        {
            throw std::runtime_error("Entry too small for metric length");
        }
        uint32_t metricLen;
        std::memcpy(&metricLen, data + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        if (metricLen > 1024 || offset + metricLen > entrySize)
        {
            throw std::runtime_error("Invalid metric length");
        }
        point.metric = std::string(reinterpret_cast<const char *>(data + offset), metricLen);
        offset += metricLen;

        // Read tags
        if (offset + sizeof(uint32_t) > entrySize)
        {
            throw std::runtime_error("Entry too small for tag count");
        }
        uint32_t tagCount;
        std::memcpy(&tagCount, data + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        if (tagCount > 100) // Sanity check
        {
            throw std::runtime_error("Too many tags");
        }

        for (uint32_t i = 0; i < tagCount; ++i)
        {
            // Read key length
            if (offset + sizeof(uint32_t) > entrySize)
            {
                throw std::runtime_error("Entry too small for tag key length");
            }
            uint32_t keyLen;
            std::memcpy(&keyLen, data + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);

            if (keyLen > 256 || offset + keyLen > entrySize)
            {
                throw std::runtime_error("Invalid tag key length");
            }

            // Read key
            std::string key(reinterpret_cast<const char *>(data + offset), keyLen);
            offset += keyLen;

            // Read value length
            if (offset + sizeof(uint32_t) > entrySize)
            {
                throw std::runtime_error("Entry too small for tag value length");
            }
            uint32_t valueLen;
            std::memcpy(&valueLen, data + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);

            if (valueLen > 256 || offset + valueLen > entrySize)
            {
                throw std::runtime_error("Invalid tag value length");
            }

            // Read value
            std::string value(reinterpret_cast<const char *>(data + offset), valueLen);
            offset += valueLen;

            point.tags[key] = value;
        }

        return point;
    }

    void WriteAheadLog::checkpoint()
    {
        std::lock_guard<std::mutex> lock(writeMutex_);
        logFile_.flush();
    }

    void WriteAheadLog::clear()
    {
        std::lock_guard<std::mutex> lock(writeMutex_);

        // Close current file
        if (logFile_.is_open())
        {
            logFile_.close();
        }

        // Delete the file
        fs::remove(logPath_);

        // Reopen empty file
        logFile_.open(logPath_, std::ios::out | std::ios::binary);
        sequenceNumber_.store(0);
    }

} // namespace waffledb