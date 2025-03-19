#include "tests.h"

#include <filesystem>
#include <string>

#include "cppdb.h"

namespace fs = std::filesystem;

TEST_CASE("Create a new empty database", "[createEmptyDB]") {
    SECTION("Default settings") {
        std::string dbname("myemptydb");
        Database db(CppDB::createEmptyDB(dbname));

        REQUIRE(fs::is_directory(fs::status(db.getDirectory())));
        const auto& p = fs::directory_iterator(db.getDirectory());
        REQUIRE(p == end(p));
    }
}
