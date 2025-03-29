cmake_minimum_required(VERSION 3.14)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
include(GNUInstallDirs)

set(HEADERS 
    include/waffledb.h
    include/database.h
)

add_library(waffledb STATIC
    ${HEADERS}
    src/waffledb.cpp
    src/database.cpp
)

set_target_properties(waffledb PROPERTIES PUBLIC_HEADER "${HEADERS}")

target_include_directories(waffledb 
    PUBLIC 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

if(MSVC)
    target_compile_options(waffledb PRIVATE /W4)  
    target_compile_definitions(waffledb PRIVATE WIN32 _WINDOWS)
else()
    target_compile_options(waffledb PRIVATE -Wall -Wextra)
    if(NOT APPLE)
        target_link_libraries(waffledb PRIVATE stdc++fs)
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