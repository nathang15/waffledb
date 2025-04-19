#include "catch.hpp"

#include "waffledb.h"
#include <vector>
#include <ctime>
#include <unordered_map>

TEST_CASE("Time series operations", "[write, query, getMetrics, aggregates, deleteMetric]")
{
    std::string dbname("timeseriesdb");
    std::unique_ptr<waffledb::IDatabase> db(waffledb::WaffleDB::createEmptyDB(dbname));

    SECTION("Write and query a single point")
    {
        waffledb::TimePoint point;
        point.metric = "cpu.usage";
        point.timestamp = static_cast<uint64_t>(time(nullptr));
        point.value = 75.5;

        db->write(point);

        std::vector<waffledb::TimePoint> results = db->query(
            "cpu.usage",
            point.timestamp - 10,
            point.timestamp + 10,
            std::unordered_map<std::string, std::string>());

        REQUIRE(results.size() == 1);
        REQUIRE(results[0].metric == "cpu.usage");
        REQUIRE(results[0].timestamp == point.timestamp);
        REQUIRE(results[0].value == 75.5);
    }

    SECTION("Write batch and query multiple points")
    {
        // Create a batch of points
        std::vector<waffledb::TimePoint> points;

        uint64_t now = static_cast<uint64_t>(time(nullptr));

        waffledb::TimePoint point1;
        point1.metric = "cpu.batch";
        point1.timestamp = now - 60; // 1 minute ago
        point1.value = 10.0;
        points.push_back(point1);

        waffledb::TimePoint point2;
        point2.metric = "cpu.batch";
        point2.timestamp = now - 30; // 30 seconds ago
        point2.value = 20.0;
        points.push_back(point2);

        waffledb::TimePoint point3;
        point3.metric = "cpu.batch";
        point3.timestamp = now; // now
        point3.value = 30.0;
        points.push_back(point3);

        // Write batch to database
        db->writeBatch(points);

        // Query all points
        std::vector<waffledb::TimePoint> results = db->query(
            "cpu.batch",
            now - 120, // 2 minutes ago
            now + 10,
            std::unordered_map<std::string, std::string>());

        REQUIRE(results.size() == 3);
        REQUIRE(results[0].value == 10.0);
        REQUIRE(results[1].value == 20.0);
        REQUIRE(results[2].value == 30.0);
    }

    SECTION("Write and query points with tags")
    {
        // Create points with different tags
        waffledb::TimePoint point1;
        point1.metric = "memory.usage";
        point1.timestamp = static_cast<uint64_t>(time(nullptr));
        point1.value = 4096.0;
        point1.tags["host"] = "server1";
        point1.tags["region"] = "us-west";

        waffledb::TimePoint point2;
        point2.metric = "memory.usage";
        point2.timestamp = static_cast<uint64_t>(time(nullptr));
        point2.value = 2048.0;
        point2.tags["host"] = "server2";
        point2.tags["region"] = "us-east";

        // Write points to database
        db->write(point1);
        db->write(point2);

        // Query with tag filter
        std::unordered_map<std::string, std::string> tags;
        tags["host"] = "server1";

        std::vector<waffledb::TimePoint> results = db->query(
            "memory.usage",
            point1.timestamp - 10,
            point1.timestamp + 10,
            tags);

        REQUIRE(results.size() == 1);
        REQUIRE(results[0].metric == "memory.usage");
        REQUIRE(results[0].value == 4096.0);
        REQUIRE(results[0].tags["host"] == "server1");
    }

    SECTION("Test time range queries")
    {
        // Create points with different timestamps
        uint64_t now = static_cast<uint64_t>(time(nullptr));
        uint64_t hour = 3600;

        waffledb::TimePoint point1;
        point1.metric = "disk.io";
        point1.timestamp = now - (2 * hour); // 2 hours ago
        point1.value = 100.0;

        waffledb::TimePoint point2;
        point2.metric = "disk.io";
        point2.timestamp = now - hour; // 1 hour ago
        point2.value = 200.0;

        waffledb::TimePoint point3;
        point3.metric = "disk.io";
        point3.timestamp = now; // current time
        point3.value = 300.0;

        // Write points to database
        db->write(point1);
        db->write(point2);
        db->write(point3);

        // Query last hour
        std::vector<waffledb::TimePoint> results = db->query(
            "disk.io",
            now - hour + 1, // just after point2
            now + 10,
            std::unordered_map<std::string, std::string>());

        // Verify results (should only get the last point)
        REQUIRE(results.size() == 1);
        REQUIRE(results[0].timestamp == now);
        REQUIRE(results[0].value == 300.0);
    }

    SECTION("Test getMetrics")
    {
        waffledb::TimePoint point1;
        point1.metric = "network.in";
        point1.timestamp = static_cast<uint64_t>(time(nullptr));
        point1.value = 1024.0;

        waffledb::TimePoint point2;
        point2.metric = "network.out";
        point2.timestamp = static_cast<uint64_t>(time(nullptr));
        point2.value = 512.0;

        db->write(point1);
        db->write(point2);

        std::vector<std::string> metrics = db->getMetrics();

        // Check list length
        REQUIRE(metrics.size() >= 2);

        // Check metrics list
        bool hasNetworkIn = false;
        bool hasNetworkOut = false;

        for (const auto &metric : metrics)
        {
            if (metric == "network.in")
                hasNetworkIn = true;
            if (metric == "network.out")
                hasNetworkOut = true;
        }

        REQUIRE(hasNetworkIn);
        REQUIRE(hasNetworkOut);
    }

    SECTION("Test empty query results")
    {
        std::vector<waffledb::TimePoint> results = db->query(
            "non.existent.metric",
            0,
            static_cast<uint64_t>(time(nullptr)),
            std::unordered_map<std::string, std::string>());

        REQUIRE(results.empty());
    }

    SECTION("Test aggregate functions")
    {
        uint64_t now = static_cast<uint64_t>(time(nullptr));
        std::string metric = "test.aggregates";

        waffledb::TimePoint p1;
        p1.metric = metric;
        p1.timestamp = now - 60;
        p1.value = 10.0;

        waffledb::TimePoint p2;
        p2.metric = metric;
        p2.timestamp = now - 40;
        p2.value = 20.0;

        waffledb::TimePoint p3;
        p3.metric = metric;
        p3.timestamp = now - 20;
        p3.value = 30.0;

        waffledb::TimePoint p4;
        p4.metric = metric;
        p4.timestamp = now;
        p4.value = 40.0;

        db->write(p1);
        db->write(p2);
        db->write(p3);
        db->write(p4);

        uint64_t startTime = now - 100;
        uint64_t endTime = now + 10;
        std::unordered_map<std::string, std::string> emptyTags;

        // Test avg
        double avgValue = db->avg(metric, startTime, endTime, emptyTags);
        REQUIRE(avgValue == Approx(25.0));

        // Test sum
        double sumValue = db->sum(metric, startTime, endTime, emptyTags);
        REQUIRE(sumValue == Approx(100.0));

        // Test min
        double minValue = db->min(metric, startTime, endTime, emptyTags);
        REQUIRE(minValue == Approx(10.0));

        // Test max
        double maxValue = db->max(metric, startTime, endTime, emptyTags);
        REQUIRE(maxValue == Approx(40.0));

        // Test aggregate with tag filtering
        waffledb::TimePoint p5;
        p5.metric = metric;
        p5.timestamp = now - 30;
        p5.value = 100.0;
        p5.tags["host"] = "special";
        db->write(p5);

        std::unordered_map<std::string, std::string> tags;
        tags["host"] = "special";

        double taggedAvg = db->avg(metric, startTime, endTime, tags);
        REQUIRE(taggedAvg == Approx(100.0));
    }

    SECTION("Test deleteMetric")
    {
        std::string testMetric = "metric.to.delete";

        waffledb::TimePoint point;
        point.metric = testMetric;
        point.timestamp = static_cast<uint64_t>(time(nullptr));
        point.value = 42.0;

        db->write(point);

        std::vector<std::string> metricsBefore = db->getMetrics();
        bool foundBefore = false;
        for (const auto &m : metricsBefore)
        {
            if (m == testMetric)
            {
                foundBefore = true;
                break;
            }
        }
        REQUIRE(foundBefore);

        // Delete the metric
        db->deleteMetric(testMetric);

        std::vector<std::string> metricsAfter = db->getMetrics();
        bool foundAfter = false;
        for (const auto &m : metricsAfter)
        {
            if (m == testMetric)
            {
                foundAfter = true;
                break;
            }
        }
        REQUIRE_FALSE(foundAfter);

        // Confirm query returns empty results
        std::vector<waffledb::TimePoint> results = db->query(
            testMetric,
            0,
            static_cast<uint64_t>(time(nullptr) + 3600),
            std::unordered_map<std::string, std::string>());

        REQUIRE(results.empty());
    }

    db->destroy();
}