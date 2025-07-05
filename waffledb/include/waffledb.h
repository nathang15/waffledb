// waffledb/include/waffledb.h
#ifndef WAFFLEDB_H
#define WAFFLEDB_H

#include <string>
#include <memory>
#include "database.h"

namespace waffledb
{

    // Main database class - High-performance columnar time-series database
    class TimeSeriesDatabase : public IDatabase
    {
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

        // Extended interface for DSL queries
        std::vector<TimePoint> executeQuery(const std::string &query);

        // File format support
        void importCSV(const std::string &filename, const std::string &metric);
        void importJSON(const std::string &filename);
        void exportCSV(const std::string &filename, const std::string &metric,
                       uint64_t start_time, uint64_t end_time);

        // Static factory methods
        static std::unique_ptr<IDatabase> createEmpty(const std::string &dbname);
        static std::unique_ptr<IDatabase> load(const std::string &dbname);

    private:
        class Impl;
        std::unique_ptr<Impl> pImpl;
    };

    // Simple factory class for backward compatibility
    class WaffleDB
    {
    public:
        WaffleDB() = default;

        static std::unique_ptr<IDatabase> createEmptyDB(const std::string &dbname);
        static std::unique_ptr<IDatabase> loadDB(const std::string &dbname);
    };

} // namespace waffledb

#endif // WAFFLEDB_H