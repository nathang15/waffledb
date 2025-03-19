#ifndef CPPDB_H
#define CPPDB_H

#include <string>
#include "database.h"

class CppDB
{
public:
    CppDB();

    static Database createEmptyDB(std::string& dbname);
};

#endif // CPPDB_H
