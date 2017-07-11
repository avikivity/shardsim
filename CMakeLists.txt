cmake_minimum_required(VERSION 3.1.3)

project(shardsim)

set(CMAKE_CXX_STANDARD 11)

find_package(Boost REQUIRED COMPONENTS program_options)

include_directories(${Boost_INCLUDE_DIRS})

file(GLOB SRC_FILES "*.cc")

add_executable(shardsim ${SRC_FILES})

target_link_libraries(shardsim ${Boost_LIBRARIES})
