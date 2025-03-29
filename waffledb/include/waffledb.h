#ifndef WAFFLEDB_H
#define WAFFLEDB_H

#include <string>
#include "database.h"

namespace waffledb
{

    class WaffleDB
    {
    public:
        WaffleDB();

        static std::unique_ptr<IDatabase> createEmptyDB(std::string &dbname);
        static std::unique_ptr<IDatabase> loadDB(std::string &dbname);
    };

}

#endif // WAFFLEDB_H
