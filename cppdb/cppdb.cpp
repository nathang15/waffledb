#include "cppdb.h"

CppDB::CppDB() {}

Database CppDB::createEmptyDB(std::string &dbname) {
    return Database::createEmpty(dbname);
}
