// waffledb/include/database.h
#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <cstdint>

namespace waffledb
{
    // Time series data point structure
    struct TimePoint
    {
        uint64_t timestamp;
        double value;
        std::string metric;
        std::unordered_map<std::string, std::string> tags;
    };

    // Time series data structure for storing multiple points
    struct TimeSeries
    {
        std::string metric;
        std::vector<uint64_t> timestamps;
        std::vector<double> values;
        std::unordered_map<std::string, std::string> tags;
    };

    class IDatabase
    {
    public:
        IDatabase() = default;
        virtual ~IDatabase() = default;

        // Core operations
        virtual std::string getDirectory(void) = 0;
        virtual void write(const TimePoint &point) = 0;
        virtual void writeBatch(const std::vector<TimePoint> &points) = 0;
        virtual std::vector<TimePoint> query(
            const std::string &metric,
            uint64_t start_time,
            uint64_t end_time,
            const std::unordered_map<std::string, std::string> &tags = {}) = 0;
        virtual double avg(
            const std::string &metric,
            uint64_t start_time,
            uint64_t end_time,
            const std::unordered_map<std::string, std::string> &tags = {}) = 0;
        virtual double sum(
            const std::string &metric,
            uint64_t start_time,
            uint64_t end_time,
            const std::unordered_map<std::string, std::string> &tags = {}) = 0;
        virtual double min(
            const std::string &metric,
            uint64_t start_time,
            uint64_t end_time,
            const std::unordered_map<std::string, std::string> &tags = {}) = 0;
        virtual double max(
            const std::string &metric,
            uint64_t start_time,
            uint64_t end_time,
            const std::unordered_map<std::string, std::string> &tags = {}) = 0;
        virtual std::vector<std::string> getMetrics() = 0;
        virtual void deleteMetric(const std::string &metric) = 0;

        // Extended operations for DSL and file I/O
        virtual std::vector<TimePoint> executeQuery(const std::string &query) = 0;
        virtual void importCSV(const std::string &filename, const std::string &metric) = 0;
        virtual void importJSON(const std::string &filename) = 0;
        virtual void exportCSV(const std::string &filename, const std::string &metric,
                               uint64_t start_time, uint64_t end_time) = 0;

        // Management functions
        static const std::unique_ptr<IDatabase> createEmpty(std::string dbname);
        static const std::unique_ptr<IDatabase> load(std::string dbname);
        virtual void destroy() = 0;
    };
}
#endif // DATABASE_H