cmake_minimum_required(VERSION 3.14)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
include(GNUInstallDirs)

set(HEADERS 
    include/cppdb.h
    include/database.h
)

add_library(cppdb STATIC
    ${HEADERS}
    src/cppdb.cpp
    src/database.cpp
)

set_target_properties(cppdb PROPERTIES PUBLIC_HEADER "${HEADERS}")

target_include_directories(cppdb 
    PUBLIC 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

if(MSVC)
    target_compile_options(cppdb PRIVATE /W4)  
    target_compile_definitions(cppdb PRIVATE WIN32 _WINDOWS)
else()
    target_compile_options(cppdb PRIVATE -Wall -Wextra)
    if(NOT APPLE)
        target_link_libraries(cppdb PRIVATE stdc++fs)
    endif()
endif()

install(TARGETS cppdb 
    EXPORT cppdb
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/cppdb
)

install(EXPORT cppdb 
    NAMESPACE cppdb::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cppdb
)