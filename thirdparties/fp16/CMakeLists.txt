CMAKE_MINIMUM_REQUIRED(VERSION 3.16 FATAL_ERROR)

# ---[ Project
PROJECT(FP16 C)

# ---[ Options.
OPTION(FP16_BUILD_TESTS "Build FP16 unit tests" ON)
OPTION(FP16_BUILD_BENCHMARKS "Build FP16 micro-benchmarks" ON)
OPTION(FP16_BUILD_COMPARATIVE_BENCHMARKS "Build FP16 micro-benchmarks comparing to alternatives" OFF)
OPTION(FP16_INSTALL_LIBRARY "Install the FP16 library headers" ON)

# ---[ CMake options
IF(FP16_BUILD_TESTS OR FP16_BUILD_BENCHMARKS)
  ENABLE_LANGUAGE(CXX)
ENDIF()

IF(FP16_BUILD_TESTS)
  ENABLE_TESTING()
ENDIF()

# ---[ Download deps
IF(FP16_BUILD_TESTS AND NOT DEFINED GOOGLETEST_SOURCE_DIR)
  MESSAGE(STATUS "Downloading Google Test to ${CMAKE_BINARY_DIR}/googletest-source (define GOOGLETEST_SOURCE_DIR to avoid it)")
  CONFIGURE_FILE(cmake/DownloadGoogleTest.cmake "${CMAKE_BINARY_DIR}/googletest-download/CMakeLists.txt")
  EXECUTE_PROCESS(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/googletest-download")
  EXECUTE_PROCESS(COMMAND "${CMAKE_COMMAND}" --build .
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/googletest-download")
  SET(GOOGLETEST_SOURCE_DIR "${CMAKE_BINARY_DIR}/googletest-source" CACHE STRING "Google Test source directory")
ENDIF()

IF(FP16_BUILD_BENCHMARKS AND NOT DEFINED GOOGLEBENCHMARK_SOURCE_DIR)
  MESSAGE(STATUS "Downloading Google Benchmark to ${CMAKE_BINARY_DIR}/googlebenchmark-source (define GOOGLEBENCHMARK_SOURCE_DIR to avoid it)")
  CONFIGURE_FILE(cmake/DownloadGoogleBenchmark.cmake "${CMAKE_BINARY_DIR}/googlebenchmark-download/CMakeLists.txt")
  EXECUTE_PROCESS(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/googlebenchmark-download")
  EXECUTE_PROCESS(COMMAND "${CMAKE_COMMAND}" --build .
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/googlebenchmark-download")
  SET(GOOGLEBENCHMARK_SOURCE_DIR "${CMAKE_BINARY_DIR}/googlebenchmark-source" CACHE STRING "Google Benchmark source directory")
ENDIF()

# ---[ FP16 library
ADD_LIBRARY(fp16 INTERFACE)
TARGET_INCLUDE_DIRECTORIES(fp16 INTERFACE
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:include>")

IF(FP16_INSTALL_LIBRARY)
  INCLUDE(GNUInstallDirs)
  INSTALL(FILES include/fp16.h
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
  INSTALL(FILES
      include/fp16/bitcasts.h
      include/fp16/fp16.h
      include/fp16/macros.h
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/fp16")
ENDIF()

IF(FP16_BUILD_TESTS)
  # ---[ Build google test
  IF(NOT TARGET gtest)
    SET(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    ADD_SUBDIRECTORY(
      "${GOOGLETEST_SOURCE_DIR}"
      "${CMAKE_BINARY_DIR}/googletest")
  ENDIF()

  # ---[ Build FP16 unit tests
  ADD_EXECUTABLE(ieee-to-fp32-bits-test test/ieee-to-fp32-bits.cc test/tables.cc)
  SET_TARGET_PROPERTIES(ieee-to-fp32-bits-test PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS YES)
  TARGET_INCLUDE_DIRECTORIES(ieee-to-fp32-bits-test PRIVATE test)
  TARGET_LINK_LIBRARIES(ieee-to-fp32-bits-test PRIVATE fp16 gtest gtest_main)
  ADD_TEST(NAME ieee-to-fp32-bits COMMAND ieee-to-fp32-bits-test)

  ADD_EXECUTABLE(ieee-to-fp32-value-test test/ieee-to-fp32-value.cc test/tables.cc)
  SET_TARGET_PROPERTIES(ieee-to-fp32-value-test PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS YES)
  TARGET_INCLUDE_DIRECTORIES(ieee-to-fp32-value-test PRIVATE test)
  TARGET_LINK_LIBRARIES(ieee-to-fp32-value-test PRIVATE fp16 gtest gtest_main)
  ADD_TEST(NAME ieee-to-fp32-value COMMAND ieee-to-fp32-value-test)

  ADD_EXECUTABLE(ieee-from-fp32-value-test test/ieee-from-fp32-value.cc test/tables.cc)
  SET_TARGET_PROPERTIES(ieee-from-fp32-value-test PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS YES)
  TARGET_INCLUDE_DIRECTORIES(ieee-from-fp32-value-test PRIVATE test)
  TARGET_LINK_LIBRARIES(ieee-from-fp32-value-test PRIVATE fp16 gtest gtest_main)
  ADD_TEST(NAME ieee-from-fp32-value COMMAND ieee-from-fp32-value-test)

  ADD_EXECUTABLE(alt-to-fp32-bits-test test/alt-to-fp32-bits.cc test/tables.cc)
  SET_TARGET_PROPERTIES(alt-to-fp32-bits-test PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS YES)
  TARGET_INCLUDE_DIRECTORIES(alt-to-fp32-bits-test PRIVATE test)
  TARGET_LINK_LIBRARIES(alt-to-fp32-bits-test PRIVATE fp16 gtest gtest_main)
  ADD_TEST(NAME alt-to-fp32-bits COMMAND alt-to-fp32-bits-test)

  ADD_EXECUTABLE(alt-to-fp32-value-test test/alt-to-fp32-value.cc test/tables.cc)
  SET_TARGET_PROPERTIES(alt-to-fp32-value-test PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS YES)
  TARGET_INCLUDE_DIRECTORIES(alt-to-fp32-value-test PRIVATE test)
  TARGET_LINK_LIBRARIES(alt-to-fp32-value-test PRIVATE fp16 gtest gtest_main)
  ADD_TEST(NAME alt-to-fp32-value COMMAND alt-to-fp32-value-test)

  ADD_EXECUTABLE(alt-from-fp32-value-test test/alt-from-fp32-value.cc test/tables.cc)
  SET_TARGET_PROPERTIES(alt-from-fp32-value-test PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS YES)
  TARGET_INCLUDE_DIRECTORIES(alt-from-fp32-value-test PRIVATE test)
  TARGET_LINK_LIBRARIES(alt-from-fp32-value-test PRIVATE fp16 gtest gtest_main)
  ADD_TEST(NAME alt-from-fp32-value COMMAND alt-from-fp32-value-test)

  ADD_EXECUTABLE(bitcasts-test test/bitcasts.cc)
  SET_TARGET_PROPERTIES(bitcasts-test PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS YES)
  TARGET_LINK_LIBRARIES(bitcasts-test PRIVATE fp16 gtest gtest_main)
  ADD_TEST(NAME bitcasts COMMAND bitcasts-test)
ENDIF()

IF(FP16_BUILD_BENCHMARKS)
  # ---[ Build google benchmark
  IF(NOT TARGET benchmark)
    SET(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "")
    ADD_SUBDIRECTORY(
      "${GOOGLEBENCHMARK_SOURCE_DIR}"
      "${CMAKE_BINARY_DIR}/googlebenchmark")
  ENDIF()

  # ---[ Build FP16 benchmarks
  ADD_EXECUTABLE(ieee-element-bench bench/ieee-element.cc)
  SET_TARGET_PROPERTIES(ieee-element-bench PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS YES)
  TARGET_COMPILE_DEFINITIONS(ieee-element-bench PRIVATE "FP16_COMPARATIVE_BENCHMARKS=$<BOOL:FP16_BUILD_COMPARATIVE_BENCHMARKS>")
  TARGET_INCLUDE_DIRECTORIES(ieee-element-bench PRIVATE "${PROJECT_SOURCE_DIR}")
  TARGET_LINK_LIBRARIES(ieee-element-bench PRIVATE fp16 benchmark)

  ADD_EXECUTABLE(alt-element-bench bench/alt-element.cc)
  SET_TARGET_PROPERTIES(alt-element-bench PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS YES)
  TARGET_LINK_LIBRARIES(alt-element-bench PRIVATE fp16 benchmark)

  ADD_EXECUTABLE(from-ieee-array-bench bench/from-ieee-array.cc)
  SET_TARGET_PROPERTIES(from-ieee-array-bench PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS YES)
  TARGET_COMPILE_DEFINITIONS(from-ieee-array-bench PRIVATE "FP16_COMPARATIVE_BENCHMARKS=$<BOOL:FP16_BUILD_COMPARATIVE_BENCHMARKS>")
  TARGET_INCLUDE_DIRECTORIES(from-ieee-array-bench PRIVATE "${PROJECT_SOURCE_DIR}")
  TARGET_LINK_LIBRARIES(from-ieee-array-bench PRIVATE fp16 benchmark)

  ADD_EXECUTABLE(from-alt-array-bench bench/from-alt-array.cc)
  SET_TARGET_PROPERTIES(from-alt-array-bench PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS YES)
  TARGET_LINK_LIBRARIES(from-alt-array-bench PRIVATE fp16 benchmark)

  ADD_EXECUTABLE(to-ieee-array-bench bench/to-ieee-array.cc)
  SET_TARGET_PROPERTIES(to-ieee-array-bench PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS YES)
  TARGET_COMPILE_DEFINITIONS(to-ieee-array-bench PRIVATE "FP16_COMPARATIVE_BENCHMARKS=$<BOOL:FP16_BUILD_COMPARATIVE_BENCHMARKS>")
  TARGET_INCLUDE_DIRECTORIES(to-ieee-array-bench PRIVATE "${PROJECT_SOURCE_DIR}")
  TARGET_LINK_LIBRARIES(to-ieee-array-bench PRIVATE fp16 benchmark)

  ADD_EXECUTABLE(to-alt-array-bench bench/to-alt-array.cc)
  SET_TARGET_PROPERTIES(to-alt-array-bench PROPERTIES
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS YES)
  TARGET_LINK_LIBRARIES(to-alt-array-bench PRIVATE fp16 benchmark)
ENDIF()
