#include <iostream>
#include <iomanip>
#include <sstream>
#include <fstream>
#include <vector>
#include <chrono>
#include "cxxopts.hpp"
#include "waffledb.h"

using namespace std;
using namespace waffledb;

cxxopts::Options options("waffledb-cli", "CLI for WaffleDB");

// Helper functions for time series operations
string formatTimestamp(uint64_t timestamp)
{
    time_t time = static_cast<time_t>(timestamp);
    struct tm tm = {};
#if defined(_WIN32) || defined(_WIN64)
    localtime_s(&tm, &time);
#else
    localtime_r(&time, &tm);
#endif

    stringstream ss;
    ss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return ss.str();
}

uint64_t parseTimestamp(const string &timeStr)
{
    struct tm tm = {};
    stringstream ss(timeStr);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");

    if (ss.fail())
    {
        throw std::runtime_error("Invalid timestamp format. Use YYYY-MM-DD HH:MM:SS");
    }

    return static_cast<uint64_t>(mktime(&tm));
}

unordered_map<string, string> parseTags(const string &tagsStr)
{
    unordered_map<string, string> tags;

    if (tagsStr.empty())
    {
        return tags;
    }

    stringstream ss(tagsStr);
    string tagPair;

    while (getline(ss, tagPair, ','))
    {
        size_t pos = tagPair.find('=');
        if (pos != string::npos)
        {
            string key = tagPair.substr(0, pos);
            string value = tagPair.substr(pos + 1);
            tags[key] = value;
        }
    }

    return tags;
}

void printUsage()
{
    cout << "WaffleDB CLI - Time Series Database Manual" << endl;
    cout << endl;
    cout << "  Create database:   waffledb-cli -c -n mydb" << endl;
    cout << "  Write point:       waffledb-cli --write -n mydb -m cpu.usage --val 75.2 -t \"2023-01-01 12:00:00\" --tags \"host=server1,region=us-west\"" << endl;
    cout << "  Query points:      waffledb-cli --query -n mydb -m cpu.usage --start \"2023-01-01 00:00:00\" --end \"2023-01-02 00:00:00\" --tags \"host=server1\"" << endl;
    cout << "  Calculate average: waffledb-cli --avg -n mydb -m cpu.usage --start \"2023-01-01 00:00:00\" --end \"2023-01-02 00:00:00\" --tags \"host=server1\"" << endl;
    cout << "  Calculate sum:     waffledb-cli --sum -n mydb -m cpu.usage --start \"2023-01-01 00:00:00\" --end \"2023-01-02 00:00:00\" --tags \"host=server1\"" << endl;
    cout << "  Calculate min:     waffledb-cli --min -n mydb -m cpu.usage --start \"2023-01-01 00:00:00\" --end \"2023-01-02 00:00:00\" --tags \"host=server1\"" << endl;
    cout << "  Calculate max:     waffledb-cli --max -n mydb -m cpu.usage --start \"2023-01-01 00:00:00\" --end \"2023-01-02 00:00:00\" --tags \"host=server1\"" << endl;
    cout << "  List metrics:      waffledb-cli --list -n mydb" << endl;
    cout << "  Delete metric:     waffledb-cli --delete -n mydb -m cpu.usage" << endl;
    cout << "  Destroy database:  waffledb-cli -d -n mydb" << endl;
}

int main(int argc, char *argv[])
{
    // Grab command line params and determine mode
    options.add_options()("c,create", "Create a database")("d,destroy", "Destroy a database")("n,name", "Database name (required)", cxxopts::value<std::string>())("h,help", "Print usage")("write", "Write a time series data point")("query", "Query time series data points")("avg", "Calculate average of time series data points")("sum", "Calculate sum of time series data points")("min", "Calculate minimum of time series data points")("max", "Calculate maximum of time series data points")("list", "List all metrics")("delete", "Delete a metric")("m,metric", "Metric name", cxxopts::value<string>())("val", "Point value (for write operation)", cxxopts::value<double>())("t,timestamp", "Point timestamp (format: YYYY-MM-DD HH:MM:SS)", cxxopts::value<string>())("start", "Start time (format: YYYY-MM-DD HH:MM:SS)", cxxopts::value<string>())("end", "End time (format: YYYY-MM-DD HH:MM:SS)", cxxopts::value<string>())("tags", "Tags in format key1=value1,key2=value2", cxxopts::value<string>()->default_value(""));

    auto result = options.parse(argc, argv);

    if (result.count("h") == 1)
    {
        printUsage();
        return 0;
    }

    if (result.count("n") == 0 && !result.count("h"))
    {
        cout << "You must specify a database name with -n <name>" << endl;
        printUsage();
        return 1;
    }

    // Commands that operate on databases
    if (result.count("d") == 1)
    {
        // Destroy database
        std::string dbname(result["n"].as<std::string>());
        std::unique_ptr<waffledb::IDatabase> db(WaffleDB::loadDB(dbname));
        db->destroy();
        cout << "Destroyed database: " << dbname << endl;
        return 0;
    }

    if (result.count("c") == 1)
    {
        // Create database
        std::string dbname(result["n"].as<std::string>());
        std::unique_ptr<waffledb::IDatabase> db(WaffleDB::createEmptyDB(dbname));
        cout << "Created database: " << dbname << endl;
        return 0;
    }

    // All other commands need a loaded database
    std::string dbname(result["n"].as<std::string>());
    std::unique_ptr<IDatabase> db(WaffleDB::loadDB(dbname));

    // Write time series point
    if (result.count("write"))
    {
        if (result.count("m") == 0)
        {
            cout << "Error: Metric name is required for write operation" << endl;
            return 1;
        }

        if (result.count("val") == 0)
        {
            cout << "Error: Value is required for write operation" << endl;
            return 1;
        }

        string metric = result["m"].as<string>();
        double value = result["val"].as<double>();

        // Get timestamp or use current time
        uint64_t timestamp;
        if (result.count("t"))
        {
            timestamp = parseTimestamp(result["t"].as<string>());
        }
        else
        {
            timestamp = static_cast<uint64_t>(time(nullptr));
        }

        // Parse tags
        unordered_map<string, string> tags;
        if (result.count("tags"))
        {
            tags = parseTags(result["tags"].as<string>());
        }

        // Create point and write
        TimePoint point;
        point.metric = metric;
        point.timestamp = timestamp;
        point.value = value;
        point.tags = tags;

        db->write(point);

        cout << "Wrote point: " << metric << " = " << value << " at " << formatTimestamp(timestamp);
        if (!tags.empty())
        {
            cout << " with tags: ";
            bool first = true;
            for (const auto &tag : tags)
            {
                if (!first)
                    cout << ", ";
                cout << tag.first << "=" << tag.second;
                first = false;
            }
        }
        cout << endl;

        return 0;
    }

    // Query time series points
    if (result.count("query"))
    {
        if (result.count("m") == 0)
        {
            cout << "Error: Metric name is required" << endl;
            return 1;
        }

        if (result.count("start") == 0 || result.count("end") == 0)
        {
            cout << "Error: Start and end times are required" << endl;
            return 1;
        }

        string metric = result["m"].as<string>();
        uint64_t startTime = parseTimestamp(result["start"].as<string>());
        uint64_t endTime = parseTimestamp(result["end"].as<string>());

        // Parse tags
        unordered_map<string, string> tags;
        if (result.count("tags"))
        {
            tags = parseTags(result["tags"].as<string>());
        }

        // Query points
        vector<TimePoint> points = db->query(metric, startTime, endTime, tags);

        cout << "Query results for " << metric;
        if (!tags.empty())
        {
            cout << " with tags: ";
            bool first = true;
            for (const auto &tag : tags)
            {
                if (!first)
                    cout << ", ";
                cout << tag.first << "=" << tag.second;
                first = false;
            }
        }
        cout << " from " << formatTimestamp(startTime) << " to " << formatTimestamp(endTime) << ":" << endl;

        if (points.empty())
        {
            cout << "  (no data points found)" << endl;
        }
        else
        {
            cout << "  Timestamp               | Value" << endl;
            cout << "  ------------------------|----------" << endl;

            for (const auto &point : points)
            {
                cout << "  " << formatTimestamp(point.timestamp) << " | " << point.value << endl;
            }

            cout << "  Total points: " << points.size() << endl;
        }
        return 0;
    }

    // Aggregate functions
    if (result.count("avg") || result.count("sum") || result.count("min") || result.count("max"))
    {
        if (result.count("m") == 0)
        {
            cout << "Error: Metric name is required" << endl;
            return 1;
        }

        if (result.count("start") == 0 || result.count("end") == 0)
        {
            cout << "Error: Start and end times are required" << endl;
            return 1;
        }

        string metric = result["m"].as<string>();
        uint64_t startTime = parseTimestamp(result["start"].as<string>());
        uint64_t endTime = parseTimestamp(result["end"].as<string>());

        // Parse tags
        unordered_map<string, string> tags;
        if (result.count("tags"))
        {
            tags = parseTags(result["tags"].as<string>());
        }

        double result_value = 0.0;
        string operation;

        if (result.count("avg"))
        {
            result_value = db->avg(metric, startTime, endTime, tags);
            operation = "Average";
        }
        else if (result.count("sum"))
        {
            result_value = db->sum(metric, startTime, endTime, tags);
            operation = "Sum";
        }
        else if (result.count("min"))
        {
            result_value = db->min(metric, startTime, endTime, tags);
            operation = "Minimum";
        }
        else if (result.count("max"))
        {
            result_value = db->max(metric, startTime, endTime, tags);
            operation = "Maximum";
        }

        cout << operation << " for " << metric;
        if (!tags.empty())
        {
            cout << " with tags: ";
            bool first = true;
            for (const auto &tag : tags)
            {
                if (!first)
                    cout << ", ";
                cout << tag.first << "=" << tag.second;
                first = false;
            }
        }
        cout << " from " << formatTimestamp(startTime) << " to " << formatTimestamp(endTime) << ": " << result_value << endl;

        return 0;
    }

    // List metrics
    if (result.count("list"))
    {
        // List all metrics
        vector<string> metrics = db->getMetrics();

        cout << "Metrics in database " << dbname << ":" << endl;
        if (metrics.empty())
        {
            cout << "  (none)" << endl;
        }
        else
        {
            for (const auto &metric : metrics)
            {
                cout << "  " << metric << endl;
            }
        }

        return 0;
    }

    // Delete metric
    if (result.count("delete"))
    {
        if (result.count("m") == 0)
        {
            cout << "Error: Metric name is required" << endl;
            return 1;
        }

        string metric = result["m"].as<string>();
        db->deleteMetric(metric);
        cout << "Deleted metric: " << metric << endl;
        return 0;
    }

    // No operation specified
    cout << "No command specified" << endl;
    printUsage();
    return 1;
}