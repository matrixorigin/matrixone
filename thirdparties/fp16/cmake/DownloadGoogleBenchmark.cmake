CMAKE_MINIMUM_REQUIRED(VERSION 3.16 FATAL_ERROR)

PROJECT(googlebenchmark-download NONE)

INCLUDE(ExternalProject)
ExternalProject_Add(googlebenchmark
	URL https://github.com/google/benchmark/archive/refs/tags/v1.8.4.zip
	URL_HASH SHA256=84c49c4c07074f36fbf8b4f182ed7d75191a6fa72756ab4a17848455499f4286
	SOURCE_DIR "${CMAKE_BINARY_DIR}/googlebenchmark-source"
	BINARY_DIR "${CMAKE_BINARY_DIR}/googlebenchmark"
	CONFIGURE_COMMAND ""
	BUILD_COMMAND ""
	INSTALL_COMMAND ""
	TEST_COMMAND ""
)
