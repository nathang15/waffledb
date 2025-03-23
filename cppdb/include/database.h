#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <memory>

namespace cppdb
{
    class IDatabase
    {
    public:
        IDatabase() = default;
        virtual ~IDatabase() = default;

        virtual std::string getDirectory(void) = 0;
        virtual void setKeyValue(std::string key, std::string value) = 0;
        virtual std::string getKeyValue(std::string key) = 0;

        // management functions
        static const std::unique_ptr<IDatabase> createEmpty(std::string dbname);
        static const std::unique_ptr<IDatabase> load(std::string dbname);
        virtual void destroy() = 0;
    };
}
#endif // DATABASE_H
