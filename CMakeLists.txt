cmake_minimum_required(VERSION 3.14)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

project(RaNT-Graph
        VERSION 0.1
        DESCRIPTION "Random Neighbor Traversal Graph (RaNT-Graph)"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_BUILD_TYPE Release)

include(FetchContent)

#
#  Threads
#
find_package(Threads REQUIRED)


#
#  MPI
find_package(MPI)


#
# Boost
find_package(Boost 1.78 QUIET)
if (NOT Boost_FOUND)
    FetchContent_Declare(Boost
            URL https://boostorg.jfrog.io/artifactory/main/release/1.78.0/source/boost_1_78_0.tar.bz2)
    FetchContent_GetProperties(Boost)
    if (NOT Boost_POPULATED)
        FetchContent_Populate(Boost)
    endif ()
    set(BOOST_ROOT ${boost_SOURCE_DIR})
    find_package(Boost 1.78)
endif ()

#
#  YGM
if (MPI_CXX_FOUND)
    find_package(YGM QUIET)
    if (NOT YGM_FOUND)
        set(JUST_INSTALL_YGM TRUE)
        FetchContent_Declare(YGM
                GIT_REPOSITORY https://github.com/LLNL/ygm.git
                GIT_TAG develop
                )
        FetchContent_MakeAvailable(YGM)
    endif ()
endif ()

add_subdirectory(src)
