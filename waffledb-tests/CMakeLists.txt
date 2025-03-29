cmake_minimum_required(VERSION 3.14)

add_executable(waffledb-tests
	dbmanagement-tests.cpp
	keyvalue-tests.cpp
)

include_directories(${waffledb_SOURCE_DIR})

target_link_libraries(waffledb-tests PRIVATE waffledb)

target_compile_features(waffledb-tests PRIVATE cxx_std_17)