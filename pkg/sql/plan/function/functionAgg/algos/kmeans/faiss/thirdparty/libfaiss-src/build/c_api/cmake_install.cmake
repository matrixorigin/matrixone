# Install script for directory: /Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Release")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

# Set default install directory permissions.
if(NOT DEFINED CMAKE_OBJDUMP)
  set(CMAKE_OBJDUMP "/Library/Developer/CommandLineTools/usr/bin/objdump")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/AutoTune_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/Clustering_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/IndexBinary_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/IndexFlat_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/IndexIVFFlat_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/IndexIVF_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/IndexLSH_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/IndexPreTransform_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/IndexReplicas_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/IndexScalarQuantizer_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/IndexShards_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/Index_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/MetaIndexes_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/VectorTransform_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/clone_index_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/error_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/error_impl.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/faiss_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api/impl" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/impl/AuxIndexStructures_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/index_factory_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/index_io_c.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/macros_impl.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/faiss/c_api/utils" TYPE FILE FILES "/Users/xvamp/Library/CloudStorage/OneDrive-TheUniversityofTexasatDallas/arjun_mbp/DownloadsPP/#mo-work/faiss-cgo-kmeans/cgo/thirdparty/libfaiss-src/c_api/utils/distances_c.h")
endif()

