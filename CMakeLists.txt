cmake_minimum_required(VERSION 3.14)

project(waffledb 
    VERSION 0.1.0
    DESCRIPTION "C++ RDBMS"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(GNUInstallDirs)

add_subdirectory(waffledb)
add_subdirectory(waffledb-cli)
add_subdirectory(waffledb-tests)