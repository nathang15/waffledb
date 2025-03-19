cmake_minimum_required(VERSION 3.14)

add_executable(cppdb-cli main.cpp)

target_link_libraries(cppdb-cli PRIVATE cppdb)

target_compile_features(cppdb-cli PRIVATE cxx_std_17)

include_directories(${groundupdb_SOURCE_DIR})

install(TARGETS cppdb-cli
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} 
)