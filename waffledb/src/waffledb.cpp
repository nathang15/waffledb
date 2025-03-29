#include "waffledb.h"
#include "extensions/extdatabase.h"

using namespace waffledb;
using namespace waffledbext;

WaffleDB::WaffleDB() {}

std::unique_ptr<IDatabase> WaffleDB::createEmptyDB(std::string &dbname)
{
    return EmbeddedDatabase::createEmpty(dbname);
}

std::unique_ptr<IDatabase> WaffleDB::loadDB(std::string &dbname)
{
    return EmbeddedDatabase::load(dbname);
}
