#include "cppdb.h"

CppDB::CppDB() {}

Database CppDB::createEmptyDB(std::string &dbname) {
    return Database::createEmpty(dbname);
}

Database CppDB::loadDB(std::string &dbname) {
    return Database::load(dbname);
}
