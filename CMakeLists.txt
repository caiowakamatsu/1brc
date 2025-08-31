cmake_minimum_required(VERSION 3.26)
project(1brc)

set(CMAKE_CXX_STANDARD 20)

add_executable(1brc main.cpp)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
