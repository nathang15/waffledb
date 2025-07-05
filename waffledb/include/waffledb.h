// waffledb/include/waffledb.h
#ifndef WAFFLEDB_H
#define WAFFLEDB_H

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

namespace waffledb
{
    // Forward declarations
    class QueryDSL;

    // Time point structure
    struct TimePoint
    {
        uint64_t timestamp;
        std::string metric;
        double value;
        std::unordered_map<std::string, std::string> tags;
    };

    // Base database interface
    class IDatabase
    {
    public:
        virtual ~IDatabase() = default;

        // Core operations
        virtual std::string getDirectory() = 0;
        virtual void write(const TimePoint &point) = 0;
        virtual void writeBatch(const std::vector<TimePoint> &points) = 0;
        virtual std::vector<TimePoint> query(
            const std::string &metric,
            uint64_t start_time,
            uint64_t end_time,
            const std::unordered_map<std::string, std::string> &tags = {}) = 0;

        // Aggregate functions
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

        // Metadata operations
        virtual std::vector<std::string> getMetrics() = 0;
        virtual void deleteMetric(const std::string &metric) = 0;
        virtual void destroy() = 0;

        // Extended operations
        virtual std::vector<TimePoint> executeQuery(const std::string &query) = 0;
        virtual void importCSV(const std::string &filename, const std::string &metric) = 0;
        virtual void importJSON(const std::string &filename) = 0;
        virtual void exportCSV(const std::string &filename, const std::string &metric,
                               uint64_t start_time, uint64_t end_time) = 0;
    };

    // Main TimeSeriesDatabase implementation
    class TimeSeriesDatabase : public IDatabase
    {
    private:
        class Impl;
        std::unique_ptr<Impl> pImpl;

    public:
        TimeSeriesDatabase(const std::string &dbname, const std::string &path);
        ~TimeSeriesDatabase();

        // IDatabase interface implementation
        std::string getDirectory() override;
        void write(const TimePoint &point) override;
        void writeBatch(const std::vector<TimePoint> &points) override;
        std::vector<TimePoint> query(
            const std::string &metric,
            uint64_t start_time,
            uint64_t end_time,
            const std::unordered_map<std::string, std::string> &tags = {}) override;

        double avg(
            const std::string &metric,
            uint64_t start_time,
            uint64_t end_time,
            const std::unordered_map<std::string, std::string> &tags = {}) override;
        double sum(
            const std::string &metric,
            uint64_t start_time,
            uint64_t end_time,
            const std::unordered_map<std::string, std::string> &tags = {}) override;
        double min(
            const std::string &metric,
            uint64_t start_time,
            uint64_t end_time,
            const std::unordered_map<std::string, std::string> &tags = {}) override;
        double max(
            const std::string &metric,
            uint64_t start_time,
            uint64_t end_time,
            const std::unordered_map<std::string, std::string> &tags = {}) override;

        std::vector<std::string> getMetrics() override;
        void deleteMetric(const std::string &metric) override;
        void destroy() override;

        std::vector<TimePoint> executeQuery(const std::string &query) override;
        void importCSV(const std::string &filename, const std::string &metric) override;
        void importJSON(const std::string &filename) override;
        void exportCSV(const std::string &filename, const std::string &metric,
                       uint64_t start_time, uint64_t end_time) override;

        // DSL-specific methods - ADDED
        bool validateQuery(const std::string &queryStr, std::vector<std::string> &errors);
        std::string explainQuery(const std::string &queryStr);

        // Factory methods
        static std::unique_ptr<IDatabase> createEmpty(const std::string &dbname);
        static std::unique_ptr<IDatabase> load(const std::string &dbname);
    };

    // Factory class for backward compatibility
    class WaffleDB
    {
    public:
        static std::unique_ptr<IDatabase> createEmptyDB(const std::string &dbname);
        static std::unique_ptr<IDatabase> loadDB(const std::string &dbname);
    };

} // namespace waffledb

#endif // WAFFLEDB_H