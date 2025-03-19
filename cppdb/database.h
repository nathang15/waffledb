#ifndef DATABASE_H
#define DATABASE_H

#include <string>

class Database
{
public:
    Database(std::string dbname, std::string fullpath);

    std::string getDirectory(void);

    static Database createEmpty(std::string dbname);
};

#endif // DATABASE_H
