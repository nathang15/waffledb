#include "cppdb.h"
#include "extensions/extdatabase.h"

using namespace cppdb;
using namespace cppdbext;

CppDB::CppDB() {}

std::unique_ptr<IDatabase> CppDB::createEmptyDB(std::string &dbname)
{
    return EmbeddedDatabase::createEmpty(dbname);
}

std::unique_ptr<IDatabase> CppDB::loadDB(std::string &dbname)
{
    return EmbeddedDatabase::load(dbname);
}
