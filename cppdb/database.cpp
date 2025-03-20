#include "database.h"

#include <iostream>
#include <fstream>
#include <filesystem>

namespace fs = std::filesystem;

Database::Database(std::string dbname, std::string fullpath) : m_name(dbname), m_fullpath(fullpath) {
    ;
}

void Database::setKeyValue(std::string key, std::string value) {
    std::ofstream os;
    os.open(m_fullpath + "/" + key + "_string.kv", std::ios::out | std::ios::trunc);
    os << value;
    os.close();
} 

std::string Database::getKeyValue(std::string key) {
    std::ifstream t(m_fullpath + "/" + key + "_string.kv");
    std:: string value;

    t.seekg(0, std::ios::end);
    value.reserve(t.tellg());
    t.seekg(0, std::ios::beg);

    value.assign((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
    return value;
}

Database Database::createEmpty(std::string dbname) {
    std::string base_dir(".waffledb");
    if (!fs::exists(base_dir)) {
        fs::create_directory(base_dir);
    }
    std::string db_folder(base_dir + "/" + dbname);
    if (!fs::exists(db_folder)) {
        fs::create_directory(db_folder);
    }

    return Database(dbname, db_folder);
}

std::string Database::getDirectory() {
    return m_fullpath;
}

void Database::destroy() {
    if (fs::exists(m_fullpath)) {
        fs::remove_all(m_fullpath);
    }
}

Database Database::load(std::string dbname) {
    std::string basedir(".waffledb");
    std::string dbfolder(basedir + "/" + dbname);
    return Database(dbname, dbfolder);
}