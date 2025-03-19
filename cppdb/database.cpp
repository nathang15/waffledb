#include "database.h"

Database::Database(std::string dbname, std::string fullpath) {

}

Database Database::createEmpty(std::string dbname) {
    return Database("fred", "fred");
}

std::string Database::getDirectory() {
    return "fred";
}
