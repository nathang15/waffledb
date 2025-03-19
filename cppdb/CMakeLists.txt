cmake_minimum_required(VERSION 3.14)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
include(GNUInstallDirs)

set(HEADERS 
    cppdb.h
    database.h
)

add_library(cppdb STATIC
    ${HEADERS}
    cppdb.cpp
    database.cpp
)

# Set public headers for installation
set_target_properties(cppdb PROPERTIES PUBLIC_HEADER "${HEADERS}")

# Configure include directories for build and install
target_include_directories(cppdb 
    PUBLIC 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
    $<INSTALL_INTERFACE:include>
)

# Platform-specific compiler and linker flags
if(MSVC)
    # MSVC-specific compiler options
    target_compile_options(cppdb PRIVATE /W4)
    
    # Define WINDOWS macro
    target_compile_definitions(cppdb PRIVATE WIN32 _WINDOWS)
else()
    # GCC/Clang compiler options
    target_compile_options(cppdb PRIVATE -Wall -Wextra)
    
    # Link with filesystem library for GCC on Linux
    if(NOT APPLE)
        target_link_libraries(cppdb PRIVATE stdc++fs)
    endif()
endif()

# Install targets
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