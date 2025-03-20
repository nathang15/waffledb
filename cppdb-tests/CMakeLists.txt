cmake_minimum_required(VERSION 3.14)

add_executable(cppdb-tests
	dbmanagement-tests.cpp
	keyvalue-tests.cpp
)

include_directories(${cppdb_SOURCE_DIR})

target_link_libraries(cppdb-tests PRIVATE cppdb)

target_compile_features(cppdb-tests PRIVATE cxx_std_17)