cmake_minimum_required(VERSION 3.14)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
include(GNUInstallDirs)

# Find dependencies
find_package(Threads REQUIRED)

# Check for SIMD support
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-mavx2" COMPILER_SUPPORTS_AVX2)
CHECK_CXX_COMPILER_FLAG("-msse4.2" COMPILER_SUPPORTS_SSE42)

set(HEADERS 
    include/waffledb.h
    include/database.h
    include/columnar_storage.h
    include/lock_free_structures.h
    include/dsl_parser.h
    include/compression.h
    include/wal.h
    include/adaptive_index.h
)

set(SOURCES
    src/waffledb.cpp
    src/columnar_storage.cpp
    src/dsl_parser.cpp
    src/compression.cpp
    src/wal.cpp
    src/adaptive_index.cpp
    src/lock_free_structures.cpp
)

add_library(waffledb STATIC ${SOURCES})

set_target_properties(waffledb PROPERTIES PUBLIC_HEADER "${HEADERS}")

target_include_directories(waffledb 
    PUBLIC 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# Add SIMD flags if supported
if(COMPILER_SUPPORTS_AVX2)
    target_compile_options(waffledb PRIVATE -mavx2)
    target_compile_definitions(waffledb PUBLIC __AVX2__)
elseif(COMPILER_SUPPORTS_SSE42)
    target_compile_options(waffledb PRIVATE -msse4.2)
    target_compile_definitions(waffledb PUBLIC __SSE4_2__)
endif()

if(MSVC)
    target_compile_options(waffledb PRIVATE /W4 /arch:AVX2)  
    target_compile_definitions(waffledb PRIVATE WIN32 _WINDOWS _CRT_SECURE_NO_WARNINGS)
else()
    target_compile_options(waffledb PRIVATE -Wall -Wextra -O3 -march=native)
    if(NOT APPLE)
        target_link_libraries(waffledb PRIVATE stdc++fs)
    endif()
endif()

target_link_libraries(waffledb 
    PUBLIC 
        Threads::Threads
)

find_package(RapidJSON QUIET)
if(RapidJSON_FOUND)
    target_include_directories(waffledb PRIVATE ${RapidJSON_INCLUDE_DIRS})
    target_compile_definitions(waffledb PRIVATE HAS_RAPIDJSON)
else()
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../rapidjson/include/rapidjson/rapidjson.h")
        message(STATUS "Found RapidJSON in parent directory")
        target_include_directories(waffledb PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../rapidjson/include")
        target_compile_definitions(waffledb PRIVATE HAS_RAPIDJSON)
    elseif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/include/rapidjson/rapidjson.h")
        message(STATUS "Found RapidJSON in include directory")
        target_compile_definitions(waffledb PRIVATE HAS_RAPIDJSON)
    else()
        message(STATUS "RapidJSON not found. JSON import/export will be disabled.")
    endif()
endif()

install(TARGETS waffledb 
    EXPORT waffledb
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/waffledb
)

install(EXPORT waffledb 
    NAMESPACE waffledb::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/waffledb
)