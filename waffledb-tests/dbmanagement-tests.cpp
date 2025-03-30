#include "tests.h"

#include <filesystem>
#include <string>

#include "waffledb.h"

namespace fs = std::filesystem;

TEST_CASE("Create a new empty database", "[createEmptyDB]")
{
    SECTION("Default settings")
    {
        std::string dbname("myemptydb");
        std::unique_ptr<waffledb::IDatabase> db(waffledb::WaffleDB::createEmptyDB(dbname));

        REQUIRE(fs::is_directory(fs::status(db->getDirectory())));

        db->destroy();
        REQUIRE(!fs::is_directory(fs::status(db->getDirectory())));
    }
}

TEST_CASE("Load an existing database", "[loadDB]")
{
    SECTION("Default settings")
    {
        std::string dbname("myemptydb");
        std::unique_ptr<waffledb::IDatabase> db(waffledb::WaffleDB::createEmptyDB(dbname));
        std::unique_ptr<waffledb::IDatabase> db2(waffledb::WaffleDB::loadDB(dbname));
        REQUIRE(fs::is_directory(fs::status(db2->getDirectory())));
        db2->destroy();
        REQUIRE(!fs::exists(fs::status(db2->getDirectory())));
    }
}
