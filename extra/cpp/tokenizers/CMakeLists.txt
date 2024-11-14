cmake_minimum_required(VERSION 3.10)
project(Tokenizers)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add GoogleTest
include(FetchContent)
FetchContent_Declare(
   googletest
   DOWNLOAD_EXTRACT_TIMESTAMP ON
   URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_library(tokenizers STATIC
    src/metta_tokenizer.cc)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${GTEST_INCLUDE_DIRS})

add_executable(
    test_metta_tokenizer
    tests/test_metta_tokenizer.cc)

target_link_libraries(
    test_metta_tokenizer
    tokenizers
    gtest_main
    ${GTEST_LIBRARIES}
    pthread)

include(GoogleTest)
gtest_discover_tests(test_metta_tokenizer)
