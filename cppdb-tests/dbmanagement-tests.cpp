#include "tests.h"

#include <filesystem>
#include <string>

#include "cppdb.h"

namespace fs = std::filesystem;

TEST_CASE("Create a new empty database", "[createEmptyDB]")
{
    SECTION("Default settings")
    {
        std::string dbname("myemptydb");
        std::unique_ptr<cppdb::IDatabase> db(cppdb::CppDB::createEmptyDB(dbname));

        REQUIRE(fs::is_directory(fs::status(db->getDirectory())));
        const auto &p = fs::directory_iterator(db->getDirectory());
        REQUIRE(p == end(p));

        db->destroy();
        REQUIRE(!fs::is_directory(fs::status(db->getDirectory())));
    }
}

TEST_CASE("Load an existing database", "[loadDB]")
{
    SECTION("Default settings")
    {
        std::string dbname("myemptydb");
        std::unique_ptr<cppdb::IDatabase> db(cppdb::CppDB::createEmptyDB(dbname));
        std::unique_ptr<cppdb::IDatabase> db2(cppdb::CppDB::loadDB(dbname));
        REQUIRE(fs::is_directory(fs::status(db2->getDirectory())));
        const auto &p = fs::directory_iterator(db2->getDirectory());
        REQUIRE(p == end(p));
        db2->destroy();
        REQUIRE(!fs::exists(fs::status(db2->getDirectory())));
    }
}
