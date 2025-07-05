// waffledb/include/compression.h
#ifndef COMPRESSION_H
#define COMPRESSION_H

#include <vector>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

namespace waffledb
{

    // Base compression interface
    class CompressionAlgorithm
    {
    public:
        virtual ~CompressionAlgorithm() = default;

        virtual std::vector<uint8_t> compress(const uint8_t *data, size_t size) = 0;
        virtual std::vector<uint8_t> decompress(const uint8_t *data, size_t size) = 0;
        virtual std::string name() const = 0;
    };

    // Delta encoding for timestamps
    class DeltaEncoding : public CompressionAlgorithm
    {
    public:
        std::vector<uint8_t> compress(const uint8_t *data, size_t size) override;
        std::vector<uint8_t> decompress(const uint8_t *data, size_t size) override;
        std::string name() const override { return "delta"; }

        // Specialized methods for timestamps
        std::vector<uint8_t> compressTimestamps(const uint64_t *timestamps, size_t count);
        std::vector<uint64_t> decompressTimestamps(const uint8_t *data, size_t size);
    };

    // Run-length encoding for repeated values
    class RunLengthEncoding : public CompressionAlgorithm
    {
    public:
        std::vector<uint8_t> compress(const uint8_t *data, size_t size) override;
        std::vector<uint8_t> decompress(const uint8_t *data, size_t size) override;
        std::string name() const override { return "rle"; }

        // Specialized for double values
        std::vector<uint8_t> compressDoubles(const double *values, size_t count);
        std::vector<double> decompressDoubles(const uint8_t *data, size_t size);
    };

    // Bit-packing for small integer values
    class BitPackingCompression : public CompressionAlgorithm
    {
    private:
        uint8_t bitsPerValue_;

    public:
        explicit BitPackingCompression(uint8_t bitsPerValue = 0);

        std::vector<uint8_t> compress(const uint8_t *data, size_t size) override;
        std::vector<uint8_t> decompress(const uint8_t *data, size_t size) override;
        std::string name() const override { return "bitpacking"; }

        // Auto-detect optimal bit width
        void detectBitWidth(const uint64_t *values, size_t count);
    };

    // Compression engine that manages multiple algorithms
    class CompressionEngine
    {
    private:
        std::unique_ptr<DeltaEncoding> deltaEncoder_;
        std::unique_ptr<RunLengthEncoding> rleEncoder_;
        std::unique_ptr<BitPackingCompression> bitPacker_;

    public:
        CompressionEngine();
        ~CompressionEngine();

        // Compress columnar data
        struct CompressedData
        {
            std::vector<uint8_t> timestamps;
            std::vector<uint8_t> values;
            std::vector<uint8_t> metadata;
            std::string timestampCodec;
            std::string valueCodec;
        };

        CompressedData compressColumns(
            const uint64_t *timestamps,
            const double *values,
            size_t count);

        struct DecompressedData
        {
            std::vector<uint64_t> timestamps;
            std::vector<double> values;
        };

        DecompressedData decompressColumns(const CompressedData &compressed);

        // Compression statistics
        struct CompressionStats
        {
            size_t originalSize;
            size_t compressedSize;
            double compressionRatio;
            std::string algorithm;
        };

        CompressionStats getLastStats() const { return lastStats_; }

    private:
        CompressionStats lastStats_;

        // Choose best algorithm based on data characteristics
        std::string selectTimestampCodec(const uint64_t *timestamps, size_t count);
        std::string selectValueCodec(const double *values, size_t count);
    };

    // Compression block header for random access
    struct CompressionBlockHeader
    {
        uint32_t blockSize;
        uint32_t uncompressedSize;
        uint16_t compressionType;
        uint16_t blockNumber;
        uint64_t minTimestamp;
        uint64_t maxTimestamp;
        uint32_t checksum;

        std::vector<uint8_t> serialize() const;
        static CompressionBlockHeader deserialize(const uint8_t *data);
    };

} // namespace waffledb

#endif // COMPRESSION_H