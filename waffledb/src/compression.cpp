// waffledb/src/compression.cpp
#include "compression.h"
#include <algorithm>
#include <cstring>
#include <cmath>

namespace waffledb
{

    // DeltaEncoding implementation
    std::vector<uint8_t> DeltaEncoding::compress(const uint8_t *data, size_t size)
    {
        // Generic byte compression - not optimal for timestamps
        std::vector<uint8_t> result;
        result.reserve(size);

        if (size == 0)
            return result;

        result.push_back(data[0]);
        for (size_t i = 1; i < size; ++i)
        {
            result.push_back(data[i] - data[i - 1]);
        }

        return result;
    }

    std::vector<uint8_t> DeltaEncoding::decompress(const uint8_t *data, size_t size)
    {
        std::vector<uint8_t> result;
        result.reserve(size);

        if (size == 0)
            return result;

        result.push_back(data[0]);
        for (size_t i = 1; i < size; ++i)
        {
            result.push_back(result.back() + data[i]);
        }

        return result;
    }

    std::vector<uint8_t> DeltaEncoding::compressTimestamps(const uint64_t *timestamps, size_t count)
    {
        if (count == 0)
            return {};

        std::vector<uint8_t> result;

        // Store first timestamp as-is (8 bytes)
        result.resize(sizeof(uint64_t));
        memcpy(result.data(), &timestamps[0], sizeof(uint64_t));

        // Store count
        result.resize(result.size() + sizeof(size_t));
        memcpy(result.data() + sizeof(uint64_t), &count, sizeof(size_t));

        // Calculate deltas and determine optimal encoding
        std::vector<int64_t> deltas;
        deltas.reserve(count - 1);

        int64_t maxDelta = 0;
        for (size_t i = 1; i < count; ++i)
        {
            int64_t delta = timestamps[i] - timestamps[i - 1];
            deltas.push_back(delta);
            maxDelta = std::max(maxDelta, std::abs(delta));
        }

        // Choose encoding based on max delta
        uint8_t bytesPerDelta = 1;
        if (maxDelta > INT8_MAX)
            bytesPerDelta = 2;
        if (maxDelta > INT16_MAX)
            bytesPerDelta = 4;
        if (maxDelta > INT32_MAX)
            bytesPerDelta = 8;

        result.push_back(bytesPerDelta);

        // Encode deltas
        for (int64_t delta : deltas)
        {
            size_t oldSize = result.size();
            result.resize(oldSize + bytesPerDelta);

            switch (bytesPerDelta)
            {
            case 1:
            {
                int8_t d = static_cast<int8_t>(delta);
                memcpy(result.data() + oldSize, &d, 1);
                break;
            }
            case 2:
            {
                int16_t d = static_cast<int16_t>(delta);
                memcpy(result.data() + oldSize, &d, 2);
                break;
            }
            case 4:
            {
                int32_t d = static_cast<int32_t>(delta);
                memcpy(result.data() + oldSize, &d, 4);
                break;
            }
            case 8:
            {
                memcpy(result.data() + oldSize, &delta, 8);
                break;
            }
            }
        }

        return result;
    }

    std::vector<uint64_t> DeltaEncoding::decompressTimestamps(const uint8_t *data, size_t size)
    {
        if (size < sizeof(uint64_t) + sizeof(size_t) + 1)
            return {};

        const uint8_t *ptr = data;

        // Read first timestamp
        uint64_t firstTimestamp;
        memcpy(&firstTimestamp, ptr, sizeof(uint64_t));
        ptr += sizeof(uint64_t);

        // Read count
        size_t count;
        memcpy(&count, ptr, sizeof(size_t));
        ptr += sizeof(size_t);

        // Read bytes per delta
        uint8_t bytesPerDelta = *ptr++;

        std::vector<uint64_t> result;
        result.reserve(count);
        result.push_back(firstTimestamp);

        // Decode deltas
        uint64_t current = firstTimestamp;
        for (size_t i = 1; i < count; ++i)
        {
            int64_t delta = 0;

            switch (bytesPerDelta)
            {
            case 1:
            {
                int8_t d;
                memcpy(&d, ptr, 1);
                delta = d;
                ptr += 1;
                break;
            }
            case 2:
            {
                int16_t d;
                memcpy(&d, ptr, 2);
                delta = d;
                ptr += 2;
                break;
            }
            case 4:
            {
                int32_t d;
                memcpy(&d, ptr, 4);
                delta = d;
                ptr += 4;
                break;
            }
            case 8:
            {
                memcpy(&delta, ptr, 8);
                ptr += 8;
                break;
            }
            }

            current += delta;
            result.push_back(current);
        }

        return result;
    }

    // RunLengthEncoding implementation
    std::vector<uint8_t> RunLengthEncoding::compress(const uint8_t *data, size_t size)
    {
        std::vector<uint8_t> result;
        if (size == 0)
            return result;

        size_t i = 0;
        while (i < size)
        {
            uint8_t value = data[i];
            size_t runLength = 1;

            while (i + runLength < size && data[i + runLength] == value && runLength < 255)
            {
                runLength++;
            }

            result.push_back(static_cast<uint8_t>(runLength));
            result.push_back(value);
            i += runLength;
        }

        return result;
    }

    std::vector<uint8_t> RunLengthEncoding::decompress(const uint8_t *data, size_t size)
    {
        std::vector<uint8_t> result;

        for (size_t i = 0; i < size; i += 2)
        {
            uint8_t runLength = data[i];
            uint8_t value = data[i + 1];

            for (size_t j = 0; j < runLength; ++j)
            {
                result.push_back(value);
            }
        }

        return result;
    }

    std::vector<uint8_t> RunLengthEncoding::compressDoubles(const double *values, size_t count)
    {
        std::vector<uint8_t> result;
        if (count == 0)
            return result;

        // Store count
        result.resize(sizeof(size_t));
        memcpy(result.data(), &count, sizeof(size_t));

        size_t i = 0;
        while (i < count)
        {
            double value = values[i];
            size_t runLength = 1;

            // Check for runs of identical values
            while (i + runLength < count && values[i + runLength] == value && runLength < 65535)
            {
                runLength++;
            }

            // Store run length (2 bytes) and value (8 bytes)
            uint16_t runLen16 = static_cast<uint16_t>(runLength);
            size_t oldSize = result.size();
            result.resize(oldSize + sizeof(uint16_t) + sizeof(double));
            memcpy(result.data() + oldSize, &runLen16, sizeof(uint16_t));
            memcpy(result.data() + oldSize + sizeof(uint16_t), &value, sizeof(double));

            i += runLength;
        }

        return result;
    }

    std::vector<double> RunLengthEncoding::decompressDoubles(const uint8_t *data, size_t size)
    {
        if (size < sizeof(size_t))
            return {};

        const uint8_t *ptr = data;

        // Read count
        size_t count;
        memcpy(&count, ptr, sizeof(size_t));
        ptr += sizeof(size_t);

        std::vector<double> result;
        result.reserve(count);

        while (result.size() < count && ptr < data + size)
        {
            uint16_t runLength;
            double value;

            memcpy(&runLength, ptr, sizeof(uint16_t));
            ptr += sizeof(uint16_t);
            memcpy(&value, ptr, sizeof(double));
            ptr += sizeof(double);

            for (size_t i = 0; i < runLength; ++i)
            {
                result.push_back(value);
            }
        }

        return result;
    }

    // BitPackingCompression implementation
    BitPackingCompression::BitPackingCompression(uint8_t bitsPerValue)
        : bitsPerValue_(bitsPerValue) {}

    void BitPackingCompression::detectBitWidth(const uint64_t *values, size_t count)
    {
        uint64_t maxValue = 0;
        for (size_t i = 0; i < count; ++i)
        {
            maxValue = std::max(maxValue, values[i]);
        }

        bitsPerValue_ = 1;
        while ((1ULL << bitsPerValue_) <= maxValue && bitsPerValue_ < 64)
        {
            bitsPerValue_++;
        }
    }

    std::vector<uint8_t> BitPackingCompression::compress(const uint8_t *data, size_t size)
    {
        // Simple bit packing for bytes
        std::vector<uint8_t> result;

        // For now, just copy - real implementation would pack bits
        result.assign(data, data + size);

        return result;
    }

    std::vector<uint8_t> BitPackingCompression::decompress(const uint8_t *data, size_t size)
    {
        // For now, just copy - real implementation would unpack bits
        return std::vector<uint8_t>(data, data + size);
    }

    // CompressionEngine implementation
    CompressionEngine::CompressionEngine()
        : deltaEncoder_(std::make_unique<DeltaEncoding>()),
          rleEncoder_(std::make_unique<RunLengthEncoding>()),
          bitPacker_(std::make_unique<BitPackingCompression>()) {}

    CompressionEngine::~CompressionEngine() = default;

    CompressionEngine::CompressedData CompressionEngine::compressColumns(
        const uint64_t *timestamps, const double *values, size_t count)
    {

        CompressedData result;

        // Choose and apply timestamp compression
        result.timestampCodec = selectTimestampCodec(timestamps, count);
        if (result.timestampCodec == "delta")
        {
            result.timestamps = deltaEncoder_->compressTimestamps(timestamps, count);
        }

        // Choose and apply value compression
        result.valueCodec = selectValueCodec(values, count);
        if (result.valueCodec == "rle")
        {
            result.values = rleEncoder_->compressDoubles(values, count);
        }
        else
        {
            // No compression - just copy
            result.values.resize(count * sizeof(double));
            memcpy(result.values.data(), values, count * sizeof(double));
        }

        // Calculate compression stats
        size_t originalSize = count * (sizeof(uint64_t) + sizeof(double));
        size_t compressedSize = result.timestamps.size() + result.values.size();

        lastStats_ = {
            originalSize,
            compressedSize,
            static_cast<double>(originalSize) / compressedSize,
            result.timestampCodec + "+" + result.valueCodec};

        return result;
    }

    CompressionEngine::DecompressedData CompressionEngine::decompressColumns(
        const CompressedData &compressed)
    {

        DecompressedData result;

        // Decompress timestamps
        if (compressed.timestampCodec == "delta")
        {
            result.timestamps = deltaEncoder_->decompressTimestamps(
                compressed.timestamps.data(), compressed.timestamps.size());
        }

        // Decompress values
        if (compressed.valueCodec == "rle")
        {
            result.values = rleEncoder_->decompressDoubles(
                compressed.values.data(), compressed.values.size());
        }
        else
        {
            // No compression - just copy
            size_t count = compressed.values.size() / sizeof(double);
            result.values.resize(count);
            memcpy(result.values.data(), compressed.values.data(), compressed.values.size());
        }

        return result;
    }

    std::string CompressionEngine::selectTimestampCodec(const uint64_t *timestamps, size_t count)
    {
        if (count < 2)
            return "none";

        // Check if timestamps are regular (fixed interval)
        bool regular = true;
        int64_t firstDelta = timestamps[1] - timestamps[0];

        for (size_t i = 2; i < count; ++i)
        {
            if (timestamps[i] - timestamps[i - 1] != firstDelta)
            {
                regular = false;
                break;
            }
        }

        // Use delta encoding for all timestamp data
        return "delta";
    }

    std::string CompressionEngine::selectValueCodec(const double *values, size_t count)
    {
        if (count < 10)
            return "none";

        // Count unique values
        std::unordered_map<double, size_t> valueCounts;
        for (size_t i = 0; i < count; ++i)
        {
            valueCounts[values[i]]++;
        }

        // If few unique values relative to total, use RLE
        if (valueCounts.size() < count / 10)
        {
            return "rle";
        }

        // Check for runs
        size_t totalRuns = 0;
        size_t i = 0;
        while (i < count)
        {
            size_t runLength = 1;
            while (i + runLength < count && values[i + runLength] == values[i])
            {
                runLength++;
            }
            if (runLength > 1)
            {
                totalRuns += runLength;
            }
            i += runLength;
        }

        // If significant runs exist, use RLE
        if (totalRuns > count / 2)
        {
            return "rle";
        }

        return "none";
    }

    // CompressionBlockHeader implementation
    std::vector<uint8_t> CompressionBlockHeader::serialize() const
    {
        std::vector<uint8_t> result(sizeof(CompressionBlockHeader));
        memcpy(result.data(), this, sizeof(CompressionBlockHeader));
        return result;
    }

    CompressionBlockHeader CompressionBlockHeader::deserialize(const uint8_t *data)
    {
        CompressionBlockHeader header;
        memcpy(&header, data, sizeof(CompressionBlockHeader));
        return header;
    }

} // namespace waffledb