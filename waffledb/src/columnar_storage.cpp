// waffledb/src/columnar_storage.cpp
#include "columnar_storage.h"
#include "compression.h"
#include <fstream>
#include <filesystem>
#include <algorithm>
#include <cstring>
#include <sstream>
#include <iostream>

// Add SIMD headers for AVX2 support
#ifdef __AVX2__
#include <immintrin.h>
#elif defined(_MSC_VER)
#include <intrin.h>
#endif

namespace fs = std::filesystem;

namespace waffledb
{

    ColumnarChunk::ColumnarChunk()
        : compressor_(std::make_unique<CompressionEngine>())
    {
        timestamps_.reserve(VALUES_PER_CHUNK);
        values_.reserve(VALUES_PER_CHUNK);
        tags_.reserve(VALUES_PER_CHUNK);
    }

    ColumnarChunk::~ColumnarChunk() = default;

    void ColumnarChunk::append(uint64_t timestamp, double value,
                               const std::unordered_map<std::string, std::string> &tags)
    {
        if (count_ >= VALUES_PER_CHUNK)
        {
            throw std::runtime_error("Chunk is full");
        }

        timestamps_.push_back(timestamp);
        values_.push_back(value);
        tags_.push_back(tags);

        if (count_ == 0)
        {
            minTimestamp_ = timestamp;
            maxTimestamp_ = timestamp;
        }
        else
        {
            minTimestamp_ = std::min(minTimestamp_, timestamp);
            maxTimestamp_ = std::max(maxTimestamp_, timestamp);
        }
        count_++;
    }

    std::vector<size_t> ColumnarChunk::queryTimeRange(uint64_t startTime, uint64_t endTime) const
    {
        std::vector<size_t> indices;
        indices.reserve(count_);

        // Binary search for start position
        auto startIt = std::lower_bound(timestamps_.begin(), timestamps_.begin() + count_, startTime);
        auto endIt = std::upper_bound(timestamps_.begin(), timestamps_.begin() + count_, endTime);

        for (auto it = startIt; it != endIt; ++it)
        {
            indices.push_back(it - timestamps_.begin());
        }

        return indices;
    }

    std::vector<size_t> ColumnarChunk::queryWithTags(
        const std::unordered_map<std::string, std::string> &queryTags) const
    {
        std::vector<size_t> indices;

        for (size_t i = 0; i < count_; ++i)
        {
            bool match = true;
            for (const auto &[key, value] : queryTags)
            {
                auto it = tags_[i].find(key);
                if (it == tags_[i].end() || it->second != value)
                {
                    match = false;
                    break;
                }
            }
            if (match)
            {
                indices.push_back(i);
            }
        }

        return indices;
    }

    // SIMD-optimized sum using AVX2
    double ColumnarChunk::sumSIMD(size_t start, size_t end) const
    {
#if defined(__AVX2__) || (defined(_MSC_VER) && defined(__AVX2__))
        const double *data = values_.data() + start;
        size_t n = end - start;

        __m256d sum_vec = _mm256_setzero_pd();
        size_t simd_end = n - (n % 4);

        // Process 4 doubles at a time
        for (size_t i = 0; i < simd_end; i += 4)
        {
            __m256d values = _mm256_loadu_pd(data + i);
            sum_vec = _mm256_add_pd(sum_vec, values);
        }

        // Horizontal sum
        double sum_array[4];
        _mm256_storeu_pd(sum_array, sum_vec);
        double sum = sum_array[0] + sum_array[1] + sum_array[2] + sum_array[3];

        // Handle remaining elements
        for (size_t i = simd_end; i < n; ++i)
        {
            sum += data[i];
        }

        return sum;
#else
        // Fallback to scalar implementation
        double sum = 0.0;
        for (size_t i = start; i < end; ++i)
        {
            sum += values_[i];
        }
        return sum;
#endif
    }

    double ColumnarChunk::sum(uint64_t startTime, uint64_t endTime) const
    {
        auto indices = queryTimeRange(startTime, endTime);
        if (indices.empty())
            return 0.0;

        // If indices are contiguous, use SIMD
        bool contiguous = true;
        for (size_t i = 1; i < indices.size(); ++i)
        {
            if (indices[i] != indices[i - 1] + 1)
            {
                contiguous = false;
                break;
            }
        }

        if (contiguous && indices.size() >= 4)
        {
            return sumSIMD(indices.front(), indices.back() + 1);
        }

        // Otherwise, sum individual values
        double total = 0.0;
        for (size_t idx : indices)
        {
            total += values_[idx];
        }
        return total;
    }

    double ColumnarChunk::avg(uint64_t startTime, uint64_t endTime) const
    {
        auto indices = queryTimeRange(startTime, endTime);
        if (indices.empty())
            return 0.0;

        double total = sum(startTime, endTime);
        return total / indices.size();
    }

    // SIMD-optimized min using AVX2
    double ColumnarChunk::minSIMD(size_t start, size_t end) const
    {
#if defined(__AVX2__) || (defined(_MSC_VER) && defined(__AVX2__))
        const double *data = values_.data() + start;
        size_t n = end - start;

        __m256d min_vec = _mm256_set1_pd(std::numeric_limits<double>::max());
        size_t simd_end = n - (n % 4);

        // Process 4 doubles at a time
        for (size_t i = 0; i < simd_end; i += 4)
        {
            __m256d values = _mm256_loadu_pd(data + i);
            min_vec = _mm256_min_pd(min_vec, values);
        }

        // Horizontal min
        double min_array[4];
        _mm256_storeu_pd(min_array, min_vec);
        double min_val = std::min({min_array[0], min_array[1], min_array[2], min_array[3]});

        // Handle remaining elements
        for (size_t i = simd_end; i < n; ++i)
        {
            min_val = std::min(min_val, data[i]);
        }

        return min_val;
#else
        // Fallback to scalar implementation
        double min_val = std::numeric_limits<double>::max();
        for (size_t i = start; i < end; ++i)
        {
            min_val = std::min(min_val, values_[i]);
        }
        return min_val;
#endif
    }

    double ColumnarChunk::min(uint64_t startTime, uint64_t endTime) const
    {
        auto indices = queryTimeRange(startTime, endTime);
        if (indices.empty())
            return 0.0;

        // Check if indices are contiguous for SIMD optimization
        bool contiguous = true;
        for (size_t i = 1; i < indices.size(); ++i)
        {
            if (indices[i] != indices[i - 1] + 1)
            {
                contiguous = false;
                break;
            }
        }

        if (contiguous && indices.size() >= 4)
        {
            return minSIMD(indices.front(), indices.back() + 1);
        }

        // Otherwise, find min of individual values
        double min_val = std::numeric_limits<double>::max();
        for (size_t idx : indices)
        {
            min_val = std::min(min_val, values_[idx]);
        }
        return min_val;
    }

    // SIMD-optimized max using AVX2
    double ColumnarChunk::maxSIMD(size_t start, size_t end) const
    {
#if defined(__AVX2__) || (defined(_MSC_VER) && defined(__AVX2__))
        const double *data = values_.data() + start;
        size_t n = end - start;

        __m256d max_vec = _mm256_set1_pd(std::numeric_limits<double>::lowest());
        size_t simd_end = n - (n % 4);

        // Process 4 doubles at a time
        for (size_t i = 0; i < simd_end; i += 4)
        {
            __m256d values = _mm256_loadu_pd(data + i);
            max_vec = _mm256_max_pd(max_vec, values);
        }

        // Horizontal max
        double max_array[4];
        _mm256_storeu_pd(max_array, max_vec);
        double max_val = std::max({max_array[0], max_array[1], max_array[2], max_array[3]});

        // Handle remaining elements
        for (size_t i = simd_end; i < n; ++i)
        {
            max_val = std::max(max_val, data[i]);
        }

        return max_val;
#else
        // Fallback to scalar implementation
        double max_val = std::numeric_limits<double>::lowest();
        for (size_t i = start; i < end; ++i)
        {
            max_val = std::max(max_val, values_[i]);
        }
        return max_val;
#endif
    }

    double ColumnarChunk::max(uint64_t startTime, uint64_t endTime) const
    {
        auto indices = queryTimeRange(startTime, endTime);
        if (indices.empty())
            return 0.0;

        // Check if indices are contiguous for SIMD optimization
        bool contiguous = true;
        for (size_t i = 1; i < indices.size(); ++i)
        {
            if (indices[i] != indices[i - 1] + 1)
            {
                contiguous = false;
                break;
            }
        }

        if (contiguous && indices.size() >= 4)
        {
            return maxSIMD(indices.front(), indices.back() + 1);
        }

        // Otherwise, find max of individual values
        double max_val = std::numeric_limits<double>::lowest();
        for (size_t idx : indices)
        {
            max_val = std::max(max_val, values_[idx]);
        }
        return max_val;
    }

    void ColumnarChunk::compress()
    {
        if (compressed_)
            return;

        auto compressed = compressor_->compressColumns(
            timestamps_.data(), values_.data(), count_);

        // Replace data with compressed versions
        // In a real implementation, we'd store the compressed data
        // and clear the uncompressed vectors
        compressed_ = true;
    }

    void ColumnarChunk::decompress()
    {
        if (!compressed_)
            return;

        // In a real implementation, decompress the data
        compressed_ = false;
    }

    std::vector<uint8_t> ColumnarChunk::serialize() const
    {
        std::vector<uint8_t> buffer;

        // Calculate total size needed for better memory allocation
        size_t totalSize = sizeof(uint64_t) * 2 + sizeof(size_t) + // header
                           count_ * sizeof(uint64_t) +             // timestamps
                           count_ * sizeof(double);                // values

        // Estimate tag size
        for (const auto &tag_map : tags_)
        {
            totalSize += sizeof(uint32_t); // tag count
            for (const auto &[key, value] : tag_map)
            {
                totalSize += sizeof(uint32_t) + key.length() +
                             sizeof(uint32_t) + value.length();
            }
        }

        buffer.reserve(totalSize);

        // Write header
        buffer.resize(sizeof(uint64_t) * 2 + sizeof(size_t));
        uint8_t *ptr = buffer.data();

        std::memcpy(ptr, &minTimestamp_, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        std::memcpy(ptr, &maxTimestamp_, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        std::memcpy(ptr, &count_, sizeof(size_t));

        // Write timestamps
        size_t timestamps_size = count_ * sizeof(uint64_t);
        size_t old_size = buffer.size();
        buffer.resize(old_size + timestamps_size);
        std::memcpy(buffer.data() + old_size, timestamps_.data(), timestamps_size);

        // Write values
        size_t values_size = count_ * sizeof(double);
        old_size = buffer.size();
        buffer.resize(old_size + values_size);
        std::memcpy(buffer.data() + old_size, values_.data(), values_size);

        // Write tags with better format
        for (const auto &tag_map : tags_)
        {
            // Write tag count for this entry
            uint32_t tag_count = static_cast<uint32_t>(tag_map.size());
            old_size = buffer.size();
            buffer.resize(old_size + sizeof(uint32_t));
            std::memcpy(buffer.data() + old_size, &tag_count, sizeof(uint32_t));

            // Write each tag key-value pair
            for (const auto &[key, value] : tag_map)
            {
                // Write key
                uint32_t key_len = static_cast<uint32_t>(key.length());
                old_size = buffer.size();
                buffer.resize(old_size + sizeof(uint32_t) + key_len);
                std::memcpy(buffer.data() + old_size, &key_len, sizeof(uint32_t));
                std::memcpy(buffer.data() + old_size + sizeof(uint32_t), key.data(), key_len);

                // Write value
                uint32_t value_len = static_cast<uint32_t>(value.length());
                old_size = buffer.size();
                buffer.resize(old_size + sizeof(uint32_t) + value_len);
                std::memcpy(buffer.data() + old_size, &value_len, sizeof(uint32_t));
                std::memcpy(buffer.data() + old_size + sizeof(uint32_t), value.data(), value_len);
            }
        }

        return buffer;
    }

    void ColumnarChunk::deserialize(const std::vector<uint8_t> &data)
    {
        if (data.size() < sizeof(uint64_t) * 2 + sizeof(size_t))
        {
            throw std::runtime_error("Invalid chunk data: too small for header");
        }

        const uint8_t *ptr = data.data();
        size_t remaining = data.size();

        // Read header
        std::memcpy(&minTimestamp_, ptr, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        remaining -= sizeof(uint64_t);

        std::memcpy(&maxTimestamp_, ptr, sizeof(uint64_t));
        ptr += sizeof(uint64_t);
        remaining -= sizeof(uint64_t);

        std::memcpy(&count_, ptr, sizeof(size_t));
        ptr += sizeof(size_t);
        remaining -= sizeof(size_t);

        // Validate count
        if (count_ > VALUES_PER_CHUNK)
        {
            throw std::runtime_error("Invalid chunk data: count too large");
        }

        // Read timestamps
        size_t timestamps_size = count_ * sizeof(uint64_t);
        if (remaining < timestamps_size)
        {
            throw std::runtime_error("Invalid chunk data: insufficient data for timestamps");
        }

        timestamps_.resize(count_);
        std::memcpy(timestamps_.data(), ptr, timestamps_size);
        ptr += timestamps_size;
        remaining -= timestamps_size;

        // Read values
        size_t values_size = count_ * sizeof(double);
        if (remaining < values_size)
        {
            throw std::runtime_error("Invalid chunk data: insufficient data for values");
        }

        values_.resize(count_);
        std::memcpy(values_.data(), ptr, values_size);
        ptr += values_size;
        remaining -= values_size;

        // Read tags
        tags_.resize(count_);
        for (size_t i = 0; i < count_; ++i)
        {
            if (remaining < sizeof(uint32_t))
            {
                throw std::runtime_error("Invalid chunk data: insufficient data for tag count");
            }

            uint32_t tag_count;
            std::memcpy(&tag_count, ptr, sizeof(uint32_t));
            ptr += sizeof(uint32_t);
            remaining -= sizeof(uint32_t);

            if (tag_count > 100) // Sanity check
            {
                throw std::runtime_error("Invalid chunk data: too many tags");
            }

            for (uint32_t j = 0; j < tag_count; ++j)
            {
                // Read key
                if (remaining < sizeof(uint32_t))
                {
                    throw std::runtime_error("Invalid chunk data: insufficient data for key length");
                }

                uint32_t key_len;
                std::memcpy(&key_len, ptr, sizeof(uint32_t));
                ptr += sizeof(uint32_t);
                remaining -= sizeof(uint32_t);

                if (key_len > 256 || remaining < key_len)
                {
                    throw std::runtime_error("Invalid chunk data: invalid key length");
                }

                std::string key(reinterpret_cast<const char *>(ptr), key_len);
                ptr += key_len;
                remaining -= key_len;

                // Read value
                if (remaining < sizeof(uint32_t))
                {
                    throw std::runtime_error("Invalid chunk data: insufficient data for value length");
                }

                uint32_t value_len;
                std::memcpy(&value_len, ptr, sizeof(uint32_t));
                ptr += sizeof(uint32_t);
                remaining -= sizeof(uint32_t);

                if (value_len > 256 || remaining < value_len)
                {
                    throw std::runtime_error("Invalid chunk data: invalid value length");
                }

                std::string value(reinterpret_cast<const char *>(ptr), value_len);
                ptr += value_len;
                remaining -= value_len;

                tags_[i][key] = value;
            }
        }
    }

    // ColumnarStorageManager implementation
    ColumnarStorageManager::ColumnarStorageManager(const std::string &basePath)
        : basePath_(basePath)
    {
        fs::create_directories(basePath_);
    }

    void ColumnarStorageManager::saveChunk(const std::string &metric, size_t chunkId,
                                           const ColumnarChunk &chunk)
    {
        std::string filename = basePath_ + "/" + metric + "_" + std::to_string(chunkId) + ".chunk";

        auto data = chunk.serialize();

        // Use RAII to ensure file is closed
        {
            std::ofstream file(filename, std::ios::binary);
            if (!file)
            {
                throw std::runtime_error("Failed to save chunk: " + filename);
            }
            file.write(reinterpret_cast<const char *>(data.data()), data.size());
            file.flush(); // Ensure data is written
        } // File automatically closed here
    }

    std::unique_ptr<ColumnarChunk> ColumnarStorageManager::loadChunk(
        const std::string &metric, size_t chunkId)
    {
        std::string filename = basePath_ + "/" + metric + "_" + std::to_string(chunkId) + ".chunk";

        // Check if file exists
        if (!fs::exists(filename))
        {
            return nullptr;
        }

        std::vector<uint8_t> buffer;

        // Use RAII to ensure file is closed
        {
            std::ifstream file(filename, std::ios::binary);
            if (!file)
            {
                return nullptr;
            }

            file.seekg(0, std::ios::end);
            size_t size = file.tellg();
            file.seekg(0, std::ios::beg);

            if (size == 0)
            {
                return nullptr;
            }

            buffer.resize(size);
            file.read(reinterpret_cast<char *>(buffer.data()), size);
        } // File automatically closed here

        auto chunk = std::make_unique<ColumnarChunk>();
        try
        {
            chunk->deserialize(buffer);
        }
        catch (const std::exception &e)
        {
            std::cerr << "Failed to deserialize chunk " << filename << ": " << e.what() << std::endl;
            return nullptr;
        }

        return chunk;
    }

    void ColumnarStorageManager::deleteChunks(const std::string &metric)
    {
        for (const auto &entry : fs::directory_iterator(basePath_))
        {
            if (entry.path().filename().string().find(metric + "_") == 0)
            {
                fs::remove(entry.path());
            }
        }
    }

    std::vector<size_t> ColumnarStorageManager::listChunks(const std::string &metric)
    {
        std::vector<size_t> chunkIds;

        for (const auto &entry : fs::directory_iterator(basePath_))
        {
            std::string filename = entry.path().filename().string();
            if (filename.find(metric + "_") == 0 && filename.find(".chunk") != std::string::npos)
            {
                size_t start = metric.length() + 1;
                size_t end = filename.find(".chunk");
                if (end > start)
                {
                    try
                    {
                        size_t chunkId = std::stoull(filename.substr(start, end - start));
                        chunkIds.push_back(chunkId);
                    }
                    catch (const std::exception &)
                    {
                        // Skip files with invalid chunk IDs
                    }
                }
            }
        }

        std::sort(chunkIds.begin(), chunkIds.end());
        return chunkIds;
    }

} // namespace waffledb