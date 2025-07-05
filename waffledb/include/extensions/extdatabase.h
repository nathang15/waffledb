#ifndef EXTDATABASE_H
#define EXTDATABASE_H

#include "database.h"

namespace waffledbext
{
    using namespace waffledb;

    class EmbeddedDatabase : public IDatabase
    {
    public:
        EmbeddedDatabase(std::string dbname, std::string fullpath);

        ~EmbeddedDatabase();
        std::string getDirectory(void);

        void write(const TimePoint &point);

        void writeBatch(const std::vector<TimePoint> &points);

        std::vector<TimePoint> query(const std::string &metric, uint64_t start_time, uint64_t end_time, const std::unordered_map<std::string, std::string> &tags);

        double avg(const std::string &metric, uint64_t start_time, uint64_t end_time, const std::unordered_map<std::string, std::string> &tags);

        double sum(const std::string &metric, uint64_t start_time, uint64_t end_time, const std::unordered_map<std::string, std::string> &tags);

        double min(const std::string &metric, uint64_t start_time, uint64_t end_time, const std::unordered_map<std::string, std::string> &tags);

        double max(const std::string &metric, uint64_t start_time, uint64_t end_time, const std::unordered_map<std::string, std::string> &tags);

        std::vector<std::string> getMetrics();

        void deleteMetric(const std::string &metric);

        std::vector<waffledb::TimePoint> executeQuery(const std::string &query) override;
        void importCSV(const std::string &filename, const std::string &metric) override;
        void importJSON(const std::string &filename) override;
        void exportCSV(const std::string &filename, const std::string &metric,
                       uint64_t start_time, uint64_t end_time) override;
        // management functions
        static const std::unique_ptr<IDatabase> createEmpty(std::string dbname);
        static const std::unique_ptr<IDatabase> load(std::string dbname);
        void destroy();

        class Impl;

    private:
        std::unique_ptr<Impl> mImpl; // because this impl should not be shared
    };
}
#endif // EXTDATABASE_H