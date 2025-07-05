// waffledb/include/columnar_storage.h
#pragma once

#include "waffledb.h"
#include "compression.h"
#include <vector>
#include <unordered_map>
#include <memory>
#include <cstdint>

namespace waffledb
{

    constexpr size_t VALUES_PER_CHUNK = 1000;

    class ColumnarChunk
    {
    private:
        std::vector<uint64_t> timestamps_;
        std::vector<double> values_;
        std::vector<std::unordered_map<std::string, std::string>> tags_;

        uint64_t minTimestamp_ = UINT64_MAX;
        uint64_t maxTimestamp_ = 0;
        size_t count_ = 0;
        bool compressed_ = false;

        std::unique_ptr<CompressionEngine> compressor_;

        // SIMD optimization methods
        double sumSIMD(size_t start, size_t end) const;
        double minSIMD(size_t start, size_t end) const;
        double maxSIMD(size_t start, size_t end) const;

    public:
        ColumnarChunk();
        ~ColumnarChunk();

        void append(uint64_t timestamp, double value,
                    const std::unordered_map<std::string, std::string> &tags);

        bool canAppend() const { return count_ < VALUES_PER_CHUNK; }
        size_t size() const { return count_; }

        uint64_t getMinTimestamp() const { return minTimestamp_; }
        uint64_t getMaxTimestamp() const { return maxTimestamp_; }

        // Data access methods
        const double *getValuesPtr() const { return values_.data(); }
        const uint64_t *getTimestampsPtr() const { return timestamps_.data(); }
        const std::vector<std::unordered_map<std::string, std::string>> &getTagsRef() const { return tags_; }

        // Query methods
        std::vector<size_t> queryTimeRange(uint64_t startTime, uint64_t endTime) const;
        std::vector<size_t> queryWithTags(
            const std::unordered_map<std::string, std::string> &queryTags) const;

        // Aggregation methods
        double sum(uint64_t startTime, uint64_t endTime) const;
        double avg(uint64_t startTime, uint64_t endTime) const;
        double min(uint64_t startTime, uint64_t endTime) const;
        double max(uint64_t startTime, uint64_t endTime) const;

        // Compression
        void compress();
        void decompress();

        // Serialization
        std::vector<uint8_t> serialize() const;
        void deserialize(const std::vector<uint8_t> &data);
    };

    class ColumnarStorageManager
    {
    private:
        std::string basePath_;

    public:
        explicit ColumnarStorageManager(const std::string &basePath);

        void saveChunk(const std::string &metric, size_t chunkId,
                       const ColumnarChunk &chunk);
        std::unique_ptr<ColumnarChunk> loadChunk(
            const std::string &metric, size_t chunkId);

        void deleteChunks(const std::string &metric);
        std::vector<size_t> listChunks(const std::string &metric);
    };

} // namespace waffledb