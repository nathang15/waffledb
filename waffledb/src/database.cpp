#include "database.h"
#include "extensions/extdatabase.h"

#include <iostream>
#include <fstream>
#include <unordered_map>
#include <filesystem>
#include <sstream>
#include <algorithm>
#include <limits>
#include <cmath>
#include <unordered_set>

namespace fs = std::filesystem;
using namespace waffledb;
using namespace waffledbext;

namespace
{
    // Serialize string to tags
    std::string serializeTags(const std::unordered_map<std::string, std::string> &tags)
    {
        std::stringstream ss;
        for (const auto &tag : tags)
        {
            ss << tag.first << "=" << tag.second << ";";
        }
        return ss.str();
    }

    // Deserialize string to tags
    std::unordered_map<std::string, std::string> deserializeTags(const std::string &serialized)
    {
        std::unordered_map<std::string, std::string> tags;
        std::stringstream ss(serialized);
        std::string tagPair;

        while (std::getline(ss, tagPair, ';'))
        {
            if (tagPair.empty())
                continue;

            size_t pos = tagPair.find('=');
            if (pos != std::string::npos)
            {
                std::string key = tagPair.substr(0, pos);
                std::string value = tagPair.substr(pos + 1);
                tags[key] = value;
            }
        }

        return tags;
    }

    // Serialize time series to strings
    std::string serializeTimeSeries(const TimeSeries &series)
    {
        std::stringstream ss;

        // Write tag count and tags
        ss << serializeTags(series.tags) << "|";

        // Write timestamps and values
        size_t count = series.timestamps.size();
        ss << count << "|";

        for (size_t i = 0; i < count; ++i)
        {
            ss << series.timestamps[i] << ":" << series.values[i];
            if (i < count - 1)
            {
                ss << ",";
            }
        }

        return ss.str();
    }

    // Deserialize string to time series
    TimeSeries deserializeTimeSeries(const std::string &serialized, const std::string &metric)
    {
        TimeSeries series;
        series.metric = metric;

        // Split the serialized string into parts
        size_t tagEnd = serialized.find('|');
        if (tagEnd == std::string::npos)
            return series;

        std::string tagStr = serialized.substr(0, tagEnd);
        series.tags = deserializeTags(tagStr);

        size_t countEnd = serialized.find('|', tagEnd + 1);
        if (countEnd == std::string::npos)
            return series;

        std::string countStr = serialized.substr(tagEnd + 1, countEnd - tagEnd - 1);
        size_t count = std::stoull(countStr);

        std::string dataStr = serialized.substr(countEnd + 1);
        std::stringstream ss(dataStr);
        std::string pointStr;

        series.timestamps.reserve(count);
        series.values.reserve(count);

        while (std::getline(ss, pointStr, ','))
        {
            size_t pos = pointStr.find(':');
            if (pos != std::string::npos)
            {
                uint64_t timestamp = std::stoull(pointStr.substr(0, pos));
                double value = std::stod(pointStr.substr(pos + 1));

                series.timestamps.push_back(timestamp);
                series.values.push_back(value);
            }
        }

        return series;
    }

    // Create a unique key for a metric with tags
    std::string createSeriesKey(const std::string &metric, const std::unordered_map<std::string, std::string> &tags)
    {
        if (tags.empty())
        {
            return "ts:" + metric;
        }

        std::stringstream ss;
        ss << "ts:" << metric << ":";

        // Sort tags for consistent key generation
        std::vector<std::pair<std::string, std::string>> sortedTags(tags.begin(), tags.end());
        std::sort(sortedTags.begin(), sortedTags.end());

        for (size_t i = 0; i < sortedTags.size(); ++i)
        {
            ss << sortedTags[i].first << "=" << sortedTags[i].second;
            if (i < sortedTags.size() - 1)
            {
                ss << ":";
            }
        }

        return ss.str();
    }

    // Check if tags match
    bool tagsMatch(const std::unordered_map<std::string, std::string> &queryTags,
                   const std::unordered_map<std::string, std::string> &seriesTags)
    {
        if (queryTags.empty())
            return true;

        for (const auto &tag : queryTags)
        {
            auto it = seriesTags.find(tag.first);
            if (it == seriesTags.end() || it->second != tag.second)
            {
                return false;
            }
        }

        return true;
    }

    std::string sanitizeKeyForFilename(const std::string &key)
    {
        std::string result = key;
        // Replace invalid characters with underscores
        for (char &c : result)
        {
            if (c == ':' || c == '<' || c == '>' || c == '"' || c == '/' ||
                c == '\\' || c == '|' || c == '?' || c == '*')
            {
                c = '_';
            }
        }
        return result;
    }

}

class EmbeddedDatabase::Impl : public IDatabase
{
public:
    Impl(std::string dbname, std::string fullpath);

    ~Impl();

    // Key-value operations
    std::string getDirectory(void);
    void setKeyValue(std::string key, std::string value);
    std::string getKeyValue(std::string key);

    // Timeseries operations
    void write(const TimePoint &point);
    void writeBatch(const std::vector<TimePoint> &points);
    std::vector<TimePoint> query(
        const std::string &metric,
        uint64_t start_time,
        uint64_t end_time,
        const std::unordered_map<std::string, std::string> &tags);
    double avg(
        const std::string &metric,
        uint64_t start_time,
        uint64_t end_time,
        const std::unordered_map<std::string, std::string> &tags);
    double sum(
        const std::string &metric,
        uint64_t start_time,
        uint64_t end_time,
        const std::unordered_map<std::string, std::string> &tags);
    double min(
        const std::string &metric,
        uint64_t start_time,
        uint64_t end_time,
        const std::unordered_map<std::string, std::string> &tags);
    double max(
        const std::string &metric,
        uint64_t start_time,
        uint64_t end_time,
        const std::unordered_map<std::string, std::string> &tags);
    std::vector<std::string> getMetrics();
    void deleteMetric(const std::string &metric);

    // management functions
    static const std::unique_ptr<IDatabase> createEmpty(std::string dbname);
    static const std::unique_ptr<IDatabase> load(std::string dbname);
    void destroy();

private:
    std::string m_name;
    std::string m_fullpath;
    std::unordered_map<std::string, std::string> m_keyValueStore;
    std::unordered_set<std::string> m_metrics;

    void saveMetrics();
};

EmbeddedDatabase::Impl::Impl(std::string dbname, std::string fullpath)
    : m_name(dbname), m_fullpath(fullpath)
{
    try
    {
        // Check if directory exists first
        if (!fs::exists(fullpath))
        {
            return;
        }

        std::unordered_map<std::string, std::string> fileToKeyMap;
        std::vector<std::string> knownKeys = {
            "ts:metrics"};

        for (const auto &key : knownKeys)
        {
            fileToKeyMap[sanitizeKeyForFilename(key) + "_string.kv"] = key;
        }

        for (const auto &entry : fs::directory_iterator(fullpath))
        {
            if (entry.is_regular_file() && entry.path().extension() == ".kv")
            {
                std::string filename = entry.path().filename().string();

                // Try to determine the original key
                std::string key;
                if (filename.length() > 10 && filename.substr(filename.length() - 10) == "_string.kv")
                {
                    std::string filePrefix = filename.substr(0, filename.length() - 10);
                    // Check if it's a known key
                    if (fileToKeyMap.find(filename) != fileToKeyMap.end())
                    {
                        key = fileToKeyMap[filename];
                    }
                    else
                    {
                        // For unknown keys, just use the file prefix
                        key = filePrefix;
                    }
                }
                else
                {
                    // Not a valid key file
                    continue;
                }

                // Read the file content
                std::ifstream file(entry.path());
                if (!file.is_open())
                {
                    continue;
                }

                // Read file contents
                std::string value;
                file.seekg(0, std::ios::end);
                size_t fileSize = file.tellg();
                value.reserve(fileSize);
                file.seekg(0, std::ios::beg);
                value.assign((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
                file.close();

                // Store in map
                m_keyValueStore[key] = value;

                // If this is a metrics list, load the metrics
                if (key == "ts:metrics" && !value.empty())
                {
                    std::stringstream ss(value);
                    std::string metric;
                    while (std::getline(ss, metric, ','))
                    {
                        if (!metric.empty())
                        {
                            m_metrics.insert(metric);
                        }
                    }
                }
            }
        }
    }
    catch (const std::exception &e)
    {
        std::cout << "Exception during database loading: " << e.what() << std::endl;
    }
}

EmbeddedDatabase::Impl::~Impl()
{
    saveMetrics();
}

// Management functions

const std::unique_ptr<IDatabase> EmbeddedDatabase::Impl::createEmpty(std::string dbname)
{
    std::string basedir(".waffledb");
    if (!fs::exists(basedir))
    {
        fs::create_directory(basedir);
    }

    std::string dbfolder(basedir + "/" + dbname);
    if (!fs::exists(dbfolder))
    {
        fs::create_directory(dbfolder);
    }

    return std::make_unique<EmbeddedDatabase::Impl>(dbname, dbfolder);
}

const std::unique_ptr<IDatabase> EmbeddedDatabase::Impl::load(std::string dbname)
{
    std::string basedir(".waffledb");
    std::string dbfolder(basedir + "/" + dbname);
    return (std::make_unique<EmbeddedDatabase::Impl>(dbname, dbfolder));
}

void EmbeddedDatabase::Impl::destroy()
{
    if (fs::exists(m_fullpath))
    {
        fs::remove_all(m_fullpath);
    }

    m_keyValueStore.clear();
    m_metrics.clear();
}

// Key value operations

std::string EmbeddedDatabase::Impl::getDirectory()
{
    return m_fullpath;
}

void EmbeddedDatabase::Impl::setKeyValue(std::string key, std::string value)
{
    try
    {
        // Sanitize key for filename
        std::string safeKey = sanitizeKeyForFilename(key);
        std::string filePath = m_fullpath + "/" + safeKey + "_string.kv";
        std::ofstream os(filePath, std::ios::out | std::ios::trunc);
        if (!os.is_open())
        {
            return;
        }

        os << value;
        os.close();

        // Update in-memory map
        m_keyValueStore[key] = value;
    }
    catch (const std::exception &e)
    {
        std::cout << "Exception during setKeyValue: " << e.what() << std::endl;
    }
}

std::string EmbeddedDatabase::Impl::getKeyValue(std::string key)
{
    const auto &mapEntry = m_keyValueStore.find(key);
    if (mapEntry != m_keyValueStore.end())
    {
        return mapEntry->second;
    }

    // If not found in memory, try to read from disk
    try
    {
        std::string safeKey = sanitizeKeyForFilename(key);
        std::string filePath = m_fullpath + "/" + safeKey + "_string.kv";

        std::ifstream file(filePath);
        if (!file.is_open())
        {
            return "";
        }

        std::string fileContent;
        file.seekg(0, std::ios::end);
        size_t fileSize = file.tellg();
        fileContent.reserve(fileSize);
        file.seekg(0, std::ios::beg);
        fileContent.assign((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        file.close();

        // Update the in-memory map
        m_keyValueStore[key] = fileContent;
        return fileContent;
    }
    catch (const std::exception &e)
    {
        return "";
    }
}

// Time series operations

void EmbeddedDatabase::Impl::saveMetrics()
{
    std::stringstream ss;
    bool first = true;

    for (const auto &metric : m_metrics)
    {
        if (!first)
        {
            ss << ",";
        }
        ss << metric;
        first = false;
    }

    setKeyValue("ts:metrics", ss.str());
}

void EmbeddedDatabase::Impl::write(const TimePoint &point)
{
    // Add metric to set if new
    if (m_metrics.find(point.metric) == m_metrics.end())
    {
        m_metrics.insert(point.metric);
        saveMetrics();
    }

    // Create series key
    std::string seriesKey = createSeriesKey(point.metric, point.tags);

    // Load existing series if any
    std::string seriesStr = getKeyValue(seriesKey);
    TimeSeries series;

    if (!seriesStr.empty())
    {
        series = deserializeTimeSeries(seriesStr, point.metric);
    }
    else
    {
        series.metric = point.metric;
        series.tags = point.tags;
    }

    // Find insert position (maintain sorted order by timestamp)
    auto it = std::lower_bound(series.timestamps.begin(), series.timestamps.end(), point.timestamp);
    size_t pos = it - series.timestamps.begin();

    // Insert the new point
    series.timestamps.insert(series.timestamps.begin() + pos, point.timestamp);
    series.values.insert(series.values.begin() + pos, point.value);

    // Serialize and save updated series
    std::string serialized = serializeTimeSeries(series);
    setKeyValue(seriesKey, serialized);
}

void EmbeddedDatabase::Impl::writeBatch(const std::vector<TimePoint> &points)
{
    // Group points by series
    std::unordered_map<std::string, std::vector<TimePoint>> seriesPoints;

    for (const auto &point : points)
    {
        std::string seriesKey = createSeriesKey(point.metric, point.tags);
        seriesPoints[seriesKey].push_back(point);

        // Add metric to set if new
        if (m_metrics.find(point.metric) == m_metrics.end())
        {
            m_metrics.insert(point.metric);
        }
    }

    // Save metrics
    saveMetrics();

    // Process each series
    for (const auto &entry : seriesPoints)
    {
        const std::string &seriesKey = entry.first;
        const std::vector<TimePoint> &seriesPointsVec = entry.second;

        if (seriesPointsVec.empty())
            continue;

        // Load existing series if any
        std::string seriesStr = getKeyValue(seriesKey);
        TimeSeries series;

        if (!seriesStr.empty())
        {
            series = deserializeTimeSeries(seriesStr, seriesPointsVec[0].metric);
        }
        else
        {
            series.metric = seriesPointsVec[0].metric;
            series.tags = seriesPointsVec[0].tags;
        }

        // Add all points
        for (const auto &point : seriesPointsVec)
        {
            // Find insert position (maintain sorted order by timestamp)
            auto it = std::lower_bound(series.timestamps.begin(), series.timestamps.end(), point.timestamp);
            size_t pos = it - series.timestamps.begin();

            // Insert the new point
            series.timestamps.insert(series.timestamps.begin() + pos, point.timestamp);
            series.values.insert(series.values.begin() + pos, point.value);
        }

        // Save updated series
        setKeyValue(seriesKey, serializeTimeSeries(series));
    }
}

std::vector<TimePoint> EmbeddedDatabase::Impl::query(
    const std::string &metric,
    uint64_t start_time,
    uint64_t end_time,
    const std::unordered_map<std::string, std::string> &tags)
{
    std::vector<TimePoint> results;

    // If metric doesn't exist, return empty results
    if (m_metrics.find(metric) == m_metrics.end())
    {
        return results;
    }

    // Match by prefix instead of exact keys
    std::string metricPrefix = "ts:" + metric;

    for (const auto &pair : m_keyValueStore)
    {
        // Try matching with two formats: with colon and with underscore
        if (pair.first.find(metricPrefix) == 0 ||
            pair.first.find("ts_" + metric) == 0)
        {
            // If tags are specified, check if they match
            if (!tags.empty())
            {
                // Deserialize this series to check its tags
                TimeSeries series = deserializeTimeSeries(pair.second, metric);
                bool tagsMatch = true;

                for (const auto &tagPair : tags)
                {
                    auto it = series.tags.find(tagPair.first);
                    if (it == series.tags.end() || it->second != tagPair.second)
                    {
                        tagsMatch = false;
                        break;
                    }
                }

                if (!tagsMatch)
                {
                    continue; // Skip this series if tags don't match
                }
            }

            std::string seriesStr = pair.second;
            if (!seriesStr.empty())
            {
                TimeSeries series = deserializeTimeSeries(seriesStr, metric);
                // Add points in time range
                for (size_t i = 0; i < series.timestamps.size(); ++i)
                {
                    if (series.timestamps[i] >= start_time && series.timestamps[i] <= end_time)
                    {
                        TimePoint point;
                        point.timestamp = series.timestamps[i];
                        point.value = series.values[i];
                        point.metric = metric;
                        point.tags = series.tags;
                        results.push_back(point);
                    }
                }
            }
        }
    }

    // Sort results by timestamp
    std::sort(results.begin(), results.end(),
              [](const TimePoint &a, const TimePoint &b)
              {
                  return a.timestamp < b.timestamp;
              });

    return results;
}

double EmbeddedDatabase::Impl::avg(
    const std::string &metric,
    uint64_t start_time,
    uint64_t end_time,
    const std::unordered_map<std::string, std::string> &tags)
{

    std::vector<TimePoint> points = query(metric, start_time, end_time, tags);

    if (points.empty())
    {
        return 0.0;
    }

    double sum = 0.0;
    for (const auto &point : points)
    {
        sum += point.value;
    }

    return sum / points.size();
}

double EmbeddedDatabase::Impl::sum(
    const std::string &metric,
    uint64_t start_time,
    uint64_t end_time,
    const std::unordered_map<std::string, std::string> &tags)
{

    std::vector<TimePoint> points = query(metric, start_time, end_time, tags);

    double sum = 0.0;
    for (const auto &point : points)
    {
        sum += point.value;
    }

    return sum;
}

double EmbeddedDatabase::Impl::min(
    const std::string &metric,
    uint64_t start_time,
    uint64_t end_time,
    const std::unordered_map<std::string, std::string> &tags)
{

    std::vector<TimePoint> points = query(metric, start_time, end_time, tags);

    if (points.empty())
    {
        return 0.0;
    }

    double minVal = std::numeric_limits<double>::max();
    for (const auto &point : points)
    {
        minVal = std::min(minVal, point.value);
    }

    return minVal;
}

double EmbeddedDatabase::Impl::max(
    const std::string &metric,
    uint64_t start_time,
    uint64_t end_time,
    const std::unordered_map<std::string, std::string> &tags)
{

    std::vector<TimePoint> points = query(metric, start_time, end_time, tags);

    if (points.empty())
    {
        return 0.0;
    }

    double maxVal = std::numeric_limits<double>::lowest();
    for (const auto &point : points)
    {
        maxVal = std::max(maxVal, point.value);
    }

    return maxVal;
}

std::vector<std::string> EmbeddedDatabase::Impl::getMetrics()
{
    return std::vector<std::string>(m_metrics.begin(), m_metrics.end());
}

void EmbeddedDatabase::Impl::deleteMetric(const std::string &metric)
{
    // Remove from metrics set
    m_metrics.erase(metric);
    saveMetrics();

    // In a real implementation, we would use a prefix scan to delete all series
    // For this implementation, we'll leave the data in the KV store
}

// High Level Database Client API

EmbeddedDatabase::EmbeddedDatabase(std::string dbname, std::string fullpath)
    : mImpl(std::make_unique<EmbeddedDatabase::Impl>(dbname, fullpath))
{
    ;
}

EmbeddedDatabase::~EmbeddedDatabase()
{
    ;
}

const std::unique_ptr<IDatabase> EmbeddedDatabase::createEmpty(std::string dbname)
{
    return EmbeddedDatabase::Impl::createEmpty(dbname);
}
const std::unique_ptr<IDatabase> EmbeddedDatabase::load(std::string dbname)
{
    return EmbeddedDatabase::Impl::load(dbname);
}
void EmbeddedDatabase::destroy()
{
    mImpl->destroy();
}

// Key-value operations
std::string EmbeddedDatabase::getDirectory()
{
    return mImpl->getDirectory();
}

void EmbeddedDatabase::setKeyValue(std::string key, std::string value)
{
    mImpl->setKeyValue(key, value);
}

std::string EmbeddedDatabase::getKeyValue(std::string key)
{
    return mImpl->getKeyValue(key);
}

// Time Series operations
void EmbeddedDatabase::write(const TimePoint &point)
{
    mImpl->write(point);
}

void EmbeddedDatabase::writeBatch(const std::vector<TimePoint> &points)
{
    mImpl->writeBatch(points);
}

std::vector<TimePoint> EmbeddedDatabase::query(
    const std::string &metric,
    uint64_t start_time,
    uint64_t end_time,
    const std::unordered_map<std::string, std::string> &tags)
{
    return mImpl->query(metric, start_time, end_time, tags);
}

double EmbeddedDatabase::avg(
    const std::string &metric,
    uint64_t start_time,
    uint64_t end_time,
    const std::unordered_map<std::string, std::string> &tags)
{
    return mImpl->avg(metric, start_time, end_time, tags);
}

double EmbeddedDatabase::sum(
    const std::string &metric,
    uint64_t start_time,
    uint64_t end_time,
    const std::unordered_map<std::string, std::string> &tags)
{
    return mImpl->sum(metric, start_time, end_time, tags);
}

double EmbeddedDatabase::min(
    const std::string &metric,
    uint64_t start_time,
    uint64_t end_time,
    const std::unordered_map<std::string, std::string> &tags)
{
    return mImpl->min(metric, start_time, end_time, tags);
}

double EmbeddedDatabase::max(
    const std::string &metric,
    uint64_t start_time,
    uint64_t end_time,
    const std::unordered_map<std::string, std::string> &tags)
{
    return mImpl->max(metric, start_time, end_time, tags);
}

std::vector<std::string> EmbeddedDatabase::getMetrics()
{
    return mImpl->getMetrics();
}

void EmbeddedDatabase::deleteMetric(const std::string &metric)
{
    mImpl->deleteMetric(metric);
}