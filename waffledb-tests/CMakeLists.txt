cmake_minimum_required(VERSION 3.14)

add_executable(waffledb-tests
	dbmanagement-tests.cpp
	operations-tests.cpp
	#performance-tests.cpp
)

target_include_directories(waffledb-tests PRIVATE ${waffledb_SOURCE_DIR}/waffledb/include)

target_link_libraries(waffledb-tests PRIVATE waffledb)

target_compile_features(waffledb-tests PRIVATE cxx_std_17)