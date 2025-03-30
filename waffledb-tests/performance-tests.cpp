#define CATCH_CONFIG_ENABLE_BENCHMARKING 1
#include "catch.hpp"

#include <unordered_map>
#include <iostream>
#include <chrono>
#include <string>

#include "waffledb.h"

TEST_CASE("Measure basic performance", "[setKeyValue,getKeyValue]")
{
    SECTION("Store and Retrieve 100 000 keys - Memory cached key-value store")
    {
        std::string dbname("myemptydb");
        std::unique_ptr<waffledb::IDatabase> db(waffledb::WaffleDB::createEmptyDB(dbname));

        int total = 100000;

        // Pre-generate the keys and values in memory to not skew the tests
        std::unordered_map<std::string, std::string> keyValues;
        long i = 0;
        std::cout << "Pre-generating key value pairs..." << std::endl;
        for (; i < total; i++)
        {
            keyValues.emplace(std::to_string(i), std::to_string(i));
        }
        std::cout << "Key size is max " << std::to_string(total - 1).length() << " bytes" << std::endl;

        // Store 100 000 key-value pairs (no overlap)
        // Raw storage speed
        std::cout << "====== SET ======" << std::endl;
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        for (auto it = keyValues.begin(); it != keyValues.end(); it++)
        {
            db->setKeyValue(it->first, it->second);
        }
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        std::cout << "  " << keyValues.size() << " completed in "
                  << (std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0)
                  << " seconds" << std::endl;
        std::cout << "  "
                  << (keyValues.size() * 1000000.0 / std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count())
                  << " requests per second" << std::endl;
        std::cout << std::endl;

        // Retrieve 100 000 key-value pairs (no overlap)
        // Raw retrieval speed
        std::string aString("blank");
        std::string &result(aString);
        std::cout << "====== GET ======" << std::endl;
        begin = std::chrono::steady_clock::now();
        for (auto it = keyValues.begin(); it != keyValues.end(); it++)
        {
            result = db->getKeyValue(it->first);
        }
        end = std::chrono::steady_clock::now();
        std::cout << "  " << keyValues.size() << " completed in "
                  << (std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0)
                  << " seconds" << std::endl;
        std::cout << "  "
                  << (keyValues.size() * 1000000.0 / std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count())
                  << " requests per second" << std::endl;

        // Tear down
        std::cout << "Tests complete" << std::endl;
        db->destroy();
    }
}