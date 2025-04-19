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

    // Timeseries operations
    std::string getDirectory(void);
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
    std::unordered_map<std::string, std::string> m_timeSeriesStore;
    std::unordered_set<std::string> m_metrics;

    void saveMetrics();
    void saveTimeSeries(const std::string &seriesKey, const std::string &serializedData);
    std::string loadTimeSeries(const std::string &seriesKey);
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

        // Load metrics list
        for (const auto &entry : fs::directory_iterator(fullpath))
        {
            if (entry.is_regular_file() && entry.path().extension() == ".ts")
            {
                std::string filename = entry.path().filename().string();
                if (filename == "metrics.ts")
                {
                    // Read the metrics file
                    std::ifstream file(entry.path());
                    if (!file.is_open())
                    {
                        continue;
                    }

                    std::string line;
                    while (std::getline(file, line))
                    {
                        if (!line.empty())
                        {
                            m_metrics.insert(line);
                        }
                    }
                    file.close();
                }
                else if (filename.substr(0, 3) == "ts_")
                {
                    std::string metric = filename.substr(3, filename.length() - 6);

                    // Load the time series data
                    std::ifstream file(entry.path());
                    if (!file.is_open())
                    {
                        continue;
                    }

                    std::string content;
                    file.seekg(0, std::ios::end);
                    size_t fileSize = file.tellg();
                    content.reserve(fileSize);
                    file.seekg(0, std::ios::beg);
                    content.assign((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
                    file.close();

                    // Store the time series data
                    m_timeSeriesStore[filename.substr(0, filename.length() - 3)] = content;
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
    return std::make_unique<EmbeddedDatabase::Impl>(dbname, dbfolder);
}

void EmbeddedDatabase::Impl::destroy()
{
    if (fs::exists(m_fullpath))
    {
        fs::remove_all(m_fullpath);
    }

    m_timeSeriesStore.clear();
    m_metrics.clear();
}

std::string EmbeddedDatabase::Impl::getDirectory()
{
    return m_fullpath;
}

void EmbeddedDatabase::Impl::saveTimeSeries(const std::string &seriesKey, const std::string &serializedData)
{
    try
    {
        std::string safeKey = sanitizeKeyForFilename(seriesKey);
        std::string filePath = m_fullpath + "/" + safeKey + ".ts";
        std::ofstream os(filePath, std::ios::out | std::ios::trunc);
        if (!os.is_open())
        {
            return;
        }

        os << serializedData;
        os.close();

        // Update in-memory map
        m_timeSeriesStore[safeKey] = serializedData;
    }
    catch (const std::exception &e)
    {
        std::cout << "Exception during saveTimeSeries: " << e.what() << std::endl;
    }
}

std::string EmbeddedDatabase::Impl::loadTimeSeries(const std::string &seriesKey)
{
    std::string safeKey = sanitizeKeyForFilename(seriesKey);

    // Check if already loaded in memory
    auto it = m_timeSeriesStore.find(safeKey);
    if (it != m_timeSeriesStore.end())
    {
        return it->second;
    }

    // Not in memory, try to load from disk
    try
    {
        std::string filePath = m_fullpath + "/" + safeKey + ".ts";

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
        m_timeSeriesStore[safeKey] = fileContent;
        return fileContent;
    }
    catch (const std::exception &e)
    {
        std::cout << "Exception during loading data: " << e.what() << std::endl;
        return "";
    }
}

// Time series operations

void EmbeddedDatabase::Impl::saveMetrics()
{
    try
    {
        std::string filePath = m_fullpath + "/metrics.ts";
        std::ofstream os(filePath, std::ios::out | std::ios::trunc);
        if (!os.is_open())
        {
            return;
        }

        for (const auto &metric : m_metrics)
        {
            os << metric << std::endl;
        }
        os.close();
    }
    catch (const std::exception &e)
    {
        std::cout << "Exception during saveMetrics: " << e.what() << std::endl;
    }
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
    std::string seriesStr = loadTimeSeries(seriesKey);
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

    auto it = std::lower_bound(series.timestamps.begin(), series.timestamps.end(), point.timestamp);
    size_t pos = it - series.timestamps.begin();

    // Insert the new point
    series.timestamps.insert(series.timestamps.begin() + pos, point.timestamp);
    series.values.insert(series.values.begin() + pos, point.value);

    // Serialize and save updated series
    std::string serialized = serializeTimeSeries(series);
    saveTimeSeries(seriesKey, serialized);
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
        std::string seriesStr = loadTimeSeries(seriesKey);
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
            auto it = std::lower_bound(series.timestamps.begin(), series.timestamps.end(), point.timestamp);
            size_t pos = it - series.timestamps.begin();

            // Insert the new point
            series.timestamps.insert(series.timestamps.begin() + pos, point.timestamp);
            series.values.insert(series.values.begin() + pos, point.value);
        }

        // Save updated series
        saveTimeSeries(seriesKey, serializeTimeSeries(series));
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

    // Match by prefix
    std::string metricPrefix = "ts:" + metric;
    std::string metricPrefixUnderscored = "ts_" + metric;

    for (const auto &pair : m_timeSeriesStore)
    {
        if (pair.first.find(metricPrefix) == 0 ||
            pair.first.find(metricPrefixUnderscored) == 0)
        {
            if (!tags.empty())
            {
                TimeSeries series = deserializeTimeSeries(pair.second, metric);
                bool matches = tagsMatch(tags, series.tags);

                if (!matches)
                {
                    continue;
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

// Delete all data related to that metric (Seems problematic at the moment)
// What if we have sth like below
// time | wind speed | humidity
// delete wind speed metric would lead to delete entire as the code is showing below
void EmbeddedDatabase::Impl::deleteMetric(const std::string &metric)
{
    // Remove from metrics set
    m_metrics.erase(metric);
    saveMetrics();

    // Find and remove all files related to this metric
    std::string metricPrefix = "ts_" + metric;

    // Delete from memory
    auto it = m_timeSeriesStore.begin();
    while (it != m_timeSeriesStore.end())
    {
        if (it->first.find(metricPrefix) == 0)
        {
            it = m_timeSeriesStore.erase(it);
        }
        else
        {
            ++it;
        }
    }

    // Delete from disk
    try
    {
        for (const auto &entry : fs::directory_iterator(m_fullpath))
        {
            if (entry.is_regular_file() && entry.path().extension() == ".ts")
            {
                std::string filename = entry.path().filename().string();

                if (filename.find(metricPrefix) == 0)
                {
                    fs::remove(entry.path());
                }
            }
        }
    }
    catch (const std::exception &e)
    {
        std::cout << "Exception during deleteMetric: " << e.what() << std::endl;
    }
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

// Operations
std::string EmbeddedDatabase::getDirectory()
{
    return mImpl->getDirectory();
}

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