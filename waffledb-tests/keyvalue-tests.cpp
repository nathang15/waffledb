#include "catch.hpp"

#include "waffledb.h"

TEST_CASE("Store and retrieve a value", "[setKeyValue, getKeyValue]")
{
    SECTION("Base set and get")
    {
        std::string dbname("myemptydb");
        std::unique_ptr<waffledb::IDatabase> db(waffledb::WaffleDB::createEmptyDB(dbname));

        std::string key("simplestring");
        std::string value("some simplevalue");
        db->setKeyValue(key, value);
        REQUIRE(value == db->getKeyValue(key));

        db->destroy();
    }
}