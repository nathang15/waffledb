cmake_minimum_required(VERSION 3.14)

add_executable(waffledb-cli main.cpp)

target_link_libraries(waffledb-cli PRIVATE waffledb)

target_compile_features(waffledb-cli PRIVATE cxx_std_17)

target_include_directories(waffledb-cli PRIVATE ${waffledb_SOURCE_DIR}/waffledb/include)

install(TARGETS waffledb-cli
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} 
)