#ifndef DATABASE_H
#define DATABASE_H

#include <string>

class Database
{
public:
    Database(std::string dbname, std::string fullpath);

    std::string getDirectory(void);
    void setKeyValue(std::string key, std::string value);
    std::string getKeyValue(std::string key);
    
    // management functions
    static Database createEmpty(std::string dbname);
    void destroy();
    static Database load(std::string dbname);
protected:
    std::string m_name;
    std::string m_fullpath;
};

#endif // DATABASE_H
