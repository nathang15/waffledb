#include <iostream>
#include "cxxopts.hpp"
#include "waffledb.h"

using namespace std;
using namespace waffledb;

cxxopts::Options options("waffledb-cli", "CLI for WaffleDB");

void printUsage()
{
    cout << "Error!" << std::endl;
}

int main(int argc, char *argv[])
{
    // Grab command line params and determine mode
    options.add_options()("c,create", "Create a DB")("d,destroy", "Destroy a DB")("s,set", "Set a key in a DB")("g,get", "Get a value from key in a DB")("n,name", "Database name (required)", cxxopts::value<std::string>())("k,key", "Key to set/get", cxxopts::value<std::string>())("v,value", "Value to set", cxxopts::value<std::string>());
    auto result = options.parse(argc, argv);

    if (result.count("d") == 1)
    {
        if (result.count("n") == 0)
        {
            cout << "You must specify a db name with -n <name>" << endl;
            printUsage();
            return 1;
        }

        // Destroy database
        std::string dbname(result["n"].as<std::string>());
        std::unique_ptr<waffledb::IDatabase> db(WaffleDB::loadDB(dbname));
        db->destroy();
        return 0;
    }

    if (result.count("c") == 1)
    {
        if (result.count("n") == 0)
        {
            cout << "You must specify a db name with -n <name>" << endl;
            printUsage();
            return 1;
        }

        // create database
        std::string dbname(result["n"].as<std::string>());
        std::unique_ptr<waffledb::IDatabase> db(WaffleDB::createEmptyDB(dbname));
        return 0;
    }

    if (result.count("s") == 1)
    {
        if (result.count("n") == 0)
        {
            cout << "You must specify a db name with -n <name>" << endl;
            printUsage();
            return 1;
        }

        if (result.count("k") == 0)
        {
            cout << "You must specify a key to set with -k <name>" << endl;
            printUsage();
            return 1;
        }

        if (result.count("v") == 0)
        {
            cout << "You must specify a value to set with -v <value>" << endl;
            printUsage();
            return 1;
        }

        // Set key value
        std::string dbname(result["n"].as<std::string>());
        std::string k(result["k"].as<std::string>());
        std::string v(result["v"].as<std::string>());
        std::unique_ptr<waffledb::IDatabase> db(WaffleDB::loadDB(dbname));
        db->setKeyValue(k, v);
        return 0;
    }

    if (result.count("g") == 1)
    {
        if (result.count("n") == 0)
        {
            cout << "You must specify a db name with -n <name>" << endl;
            printUsage();
            return 1;
        }

        if (result.count("k") == 0)
        {
            cout << "You must specify a key to set with -k <name>" << endl;
            printUsage();
            return 1;
        }

        // Get key value
        std::string dbname(result["n"].as<std::string>());
        std::string k(result["k"].as<std::string>());
        std::unique_ptr<waffledb::IDatabase> db(WaffleDB::loadDB(dbname));
        cout << db->getKeyValue(k) << endl;
        return 0;
    }

    cout << "No command specified" << endl;
    printUsage();
    return 1;
}
