#include "database.h"
#include "extensions/extdatabase.h"

#include <iostream>
#include <fstream>
#include <unordered_map>
#include <filesystem>

namespace fs = std::filesystem;
using namespace waffledb;
using namespace waffledbext;

class EmbeddedDatabase::Impl : public IDatabase
{
public:
    Impl(std::string dbname, std::string fullpath);

    ~Impl();
    std::string getDirectory(void);
    void setKeyValue(std::string key, std::string value);
    std::string getKeyValue(std::string key);

    // management functions
    static const std::unique_ptr<IDatabase> createEmpty(std::string dbname);
    static const std::unique_ptr<IDatabase> load(std::string dbname);
    void destroy();

private:
    std::string m_name;
    std::string m_fullpath;
    std::unordered_map<std::string, std::string> m_keyValueStore;
};

EmbeddedDatabase::Impl::Impl(std::string dbname, std::string fullpath) : m_name(dbname), m_fullpath(fullpath)
{
    for (auto &p : fs::directory_iterator(getDirectory()))
    {
        if (p.exists() && p.is_regular_file())
        {
            // check if extension if .kv
            // If so, open file
            if (".kv" == p.path().extension())
            {
                std::string keyWithString = p.path().filename().string();
                std::string key = keyWithString.substr(0, keyWithString.length() - 10); // ASSUMPTION, fix later
                std::ifstream t(p.path());
                std::string value;

                t.seekg(0, std::ios::end);
                value.reserve(t.tellg());
                t.seekg(0, std::ios::beg);

                value.assign((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());

                m_keyValueStore.insert({key, value});
            }
        }
    }
}

EmbeddedDatabase::Impl::~Impl()
{
    ;
}

// Management functions

const std::unique_ptr<IDatabase> EmbeddedDatabase::Impl::createEmpty(std::string dbname)
{
    std::string basedir(".waffledb");
    if (!fs::exists(basedir))
    {
        fs::create_directory(basedir);
    }

    std::string dbfolder(basedir + "/" + dbname);
    if (!fs::exists(dbfolder))
    {
        fs::create_directory(dbfolder);
    }

    return std::make_unique<EmbeddedDatabase::Impl>(dbname, dbfolder);
}

const std::unique_ptr<IDatabase> EmbeddedDatabase::Impl::load(std::string dbname)
{
    std::string basedir(".waffledb");
    std::string dbfolder(basedir + "/" + dbname);
    return (std::make_unique<EmbeddedDatabase::Impl>(dbname, dbfolder));
}

void EmbeddedDatabase::Impl::destroy()
{
    if (fs::exists(m_fullpath))
    {
        fs::remove_all(m_fullpath);
    }

    m_keyValueStore.clear();
}

// Instance users function

std::string EmbeddedDatabase::Impl::getDirectory()
{
    return m_fullpath;
}

void EmbeddedDatabase::Impl::setKeyValue(std::string key, std::string value)
{
    std::ofstream os;
    os.open(m_fullpath + "/" + key + "_string.kv", std::ios::out | std::ios::trunc);
    os << value;
    os.close();
    m_keyValueStore.insert({key, value});
}

std::string EmbeddedDatabase::Impl::getKeyValue(std::string key)
{
    // std::ifstream t(m_fullpath + "/" + key + "_string.kv");
    // std::string value;

    // t.seekg(0, std::ios::end);
    // value.reserve(t.tellg());
    // t.seekg(0, std::ios::beg);

    // value.assign((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
    // return value;
    const auto &value = m_keyValueStore.find(key);
    if (value == m_keyValueStore.end())
    {
        return ""; // DANGER: Should be not found (error handling here)
    }
    return value->second;
}

// High Level Database Client API

EmbeddedDatabase::EmbeddedDatabase(std::string dbname, std::string fullpath)
    : mImpl(std::make_unique<EmbeddedDatabase::Impl>(dbname, fullpath))
{
    ;
}

EmbeddedDatabase::~EmbeddedDatabase()
{
    ;
}

const std::unique_ptr<IDatabase> EmbeddedDatabase::createEmpty(std::string dbname)
{
    return EmbeddedDatabase::Impl::createEmpty(dbname);
}
const std::unique_ptr<IDatabase> EmbeddedDatabase::load(std::string dbname)
{
    return EmbeddedDatabase::Impl::load(dbname);
}
void EmbeddedDatabase::destroy()
{
    mImpl->destroy();
}

std::string EmbeddedDatabase::getDirectory()
{
    return mImpl->getDirectory();
}

void EmbeddedDatabase::setKeyValue(std::string key, std::string value)
{
    mImpl->setKeyValue(key, value);
}

std::string EmbeddedDatabase::getKeyValue(std::string key)
{
    return mImpl->getKeyValue(key);
}
