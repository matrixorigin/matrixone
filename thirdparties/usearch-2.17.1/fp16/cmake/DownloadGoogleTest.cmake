CMAKE_MINIMUM_REQUIRED(VERSION 3.16 FATAL_ERROR)

PROJECT(googletest-download NONE)

INCLUDE(ExternalProject)
ExternalProject_Add(googletest
	URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
	URL_HASH SHA256=1f357c27ca988c3f7c6b4bf68a9395005ac6761f034046e9dde0896e3aba00e4
	SOURCE_DIR "${CMAKE_BINARY_DIR}/googletest-source"
	BINARY_DIR "${CMAKE_BINARY_DIR}/googletest"
	CONFIGURE_COMMAND ""
	BUILD_COMMAND ""
	INSTALL_COMMAND ""
	TEST_COMMAND ""
)
