#ifndef CPPDB_H
#define CPPDB_H

#include <string>
#include "database.h"

namespace cppdb
{

    class CppDB
    {
    public:
        CppDB();

        static std::unique_ptr<IDatabase> createEmptyDB(std::string &dbname);
        static std::unique_ptr<IDatabase> loadDB(std::string &dbname);
    };

}

#endif // CPPDB_H
