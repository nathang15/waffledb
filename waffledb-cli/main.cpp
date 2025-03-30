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
    cout << "Error!" << std::endl;
    cout << "WaffleDB CLI - Key-Value and Time Series Database" << endl;
    cout << endl;
    cout << "Key-Value Operations:" << endl;
    cout << "  Create database:   waffledb-cli -c -n mydb" << endl;
    cout << "  Destroy database:  waffledb-cli -d -n mydb" << endl;
    cout << "  Set key-value:     waffledb-cli -s -n mydb -k mykey -v myvalue" << endl;
    cout << "  Get key-value:     waffledb-cli -g -n mydb -k mykey" << endl;
    cout << endl;
    cout << "Time Series Operations:" << endl;
    cout << "  Write point:       waffledb-cli --tswrite -n mydb -m cpu.usage --tsval 75.2 -t \"2023-01-01 12:00:00\" --tags \"host=server1,region=us-west\"" << endl;
    cout << "  Query points:      waffledb-cli --tsquery -n mydb -m cpu.usage --start \"2023-01-01 00:00:00\" --end \"2023-01-02 00:00:00\" --tags \"host=server1\"" << endl;
    cout << "  List metrics:      waffledb-cli --tslist -n mydb" << endl;
}

int main(int argc, char *argv[])
{
    // Grab command line params and determine mode
    options.add_options()("c,create", "Create a DB")("d,destroy", "Destroy a DB")("s,set", "Set a key in a DB")("g,get", "Get a value from key in a DB")("n,name", "Database name (required)", cxxopts::value<std::string>())("k,key", "Key to set/get", cxxopts::value<std::string>())("v,value", "Value to set", cxxopts::value<std::string>())("h,help", "Print usage")("tswrite", "Write a time series data point")("tsquery", "Query time series data points")("tslist", "List all metrics")("m,metric", "Metric name", cxxopts::value<string>())("tsval", "Point value (for write operation)", cxxopts::value<double>())("t,timestamp", "Point timestamp (format: YYYY-MM-DD HH:MM:SS)", cxxopts::value<string>())("start", "Start time (format: YYYY-MM-DD HH:MM:SS)", cxxopts::value<string>())("end", "End time (format: YYYY-MM-DD HH:MM:SS)", cxxopts::value<string>())("tags", "Tags in format key1=value1,key2=value2", cxxopts::value<string>()->default_value(""));

    auto result = options.parse(argc, argv);

    if (result.count("d") == 1)
    {
        if (result.count("n") == 0)
        {
            cout << "You must specify a db name with -n <name>" << endl;
            printUsage();
            return 1;
        }

        // Destroy database
        std::string dbname(result["n"].as<std::string>());
        std::unique_ptr<waffledb::IDatabase> db(WaffleDB::loadDB(dbname));
        db->destroy();
        cout << "Destroyed database: " << dbname << endl;
        return 0;
    }

    if (result.count("c") == 1)
    {
        if (result.count("n") == 0)
        {
            cout << "You must specify a db name with -n <name>" << endl;
            printUsage();
            return 1;
        }

        // create database
        std::string dbname(result["n"].as<std::string>());
        std::unique_ptr<waffledb::IDatabase> db(WaffleDB::createEmptyDB(dbname));
        cout << "Created database: " << dbname << endl;
        return 0;
    }

    if (result.count("s") == 1)
    {
        if (result.count("n") == 0)
        {
            cout << "You must specify a db name with -n <name>" << endl;
            printUsage();
            return 1;
        }

        if (result.count("k") == 0)
        {
            cout << "You must specify a key to set with -k <name>" << endl;
            printUsage();
            return 1;
        }

        if (result.count("v") == 0)
        {
            cout << "You must specify a value to set with -v <value>" << endl;
            printUsage();
            return 1;
        }

        // Set key value
        std::string dbname(result["n"].as<std::string>());
        std::string k(result["k"].as<std::string>());
        std::string v(result["v"].as<std::string>());
        std::unique_ptr<waffledb::IDatabase> db(WaffleDB::loadDB(dbname));
        db->setKeyValue(k, v);
        cout << "Set key: " << k << " = " << v << endl;
        return 0;
    }

    if (result.count("g") == 1)
    {
        if (result.count("n") == 0)
        {
            cout << "You must specify a db name with -n <name>" << endl;
            printUsage();
            return 1;
        }

        if (result.count("k") == 0)
        {
            cout << "You must specify a key to get with -k <name>" << endl;
            printUsage();
            return 1;
        }

        // Get key value
        std::string dbname(result["n"].as<std::string>());
        std::string k(result["k"].as<std::string>());
        std::unique_ptr<waffledb::IDatabase> db(WaffleDB::loadDB(dbname));
        cout << db->getKeyValue(k) << endl;
        return 0;
    }

    // === Time Series Operations ===

    // Check for database name
    if (result.count("n") == 0)
    {
        cout << "Error: Database name is required" << endl;
        printUsage();
        return 1;
    }

    std::string dbname(result["n"].as<std::string>());

    // Handle Time Series operations
    std::unique_ptr<IDatabase> db(WaffleDB::loadDB(dbname));

    // Write time series point
    if (result.count("tswrite"))
    {
        if (result.count("m") == 0)
        {
            cout << "Error: Metric name is required for write operation" << endl;
            return 1;
        }

        if (result.count("tsval") == 0)
        {
            cout << "Error: Value is required for write operation" << endl;
            return 1;
        }

        string metric = result["m"].as<string>();
        double value = result["tsval"].as<double>();

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
    if (result.count("tsquery"))
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

    // List metrics
    if (result.count("tslist"))
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

    // No operation specified
    cout << "No command specified" << endl;
    printUsage();
    return 1;
}
