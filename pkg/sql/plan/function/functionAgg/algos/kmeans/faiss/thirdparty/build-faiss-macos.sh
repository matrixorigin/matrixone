#!/bin/bash

brew install libomp
brew install cmake
export CMAKE_PREFIX_PATH=/opt/homebrew/opt/libomp:/opt/homebrew
git clone --branch v1.7.4 --depth 1 https://github.com/facebookresearch/faiss.git  libfaiss-src
cd libfaiss-src
cmake -B build \
-DBUILD_TESTING=OFF \
-DFAISS_ENABLE_GPU=OFF \
-DFAISS_ENABLE_C_API=ON \
-DFAISS_ENABLE_PYTHON=OFF \
-DBUILD_SHARED_LIBS=OFF \
-DCMAKE_BUILD_TYPE=Release .
sudo make -C build -j faiss
sudo make -C build install

arch=arm64
if [[ $(uname -m) == 'x86_64' ]]; then
  arch=x64
fi

cp build/c_api/libfaiss_c.a ../runtimes/osx-$arch/native/
cp build/faiss/libfaiss.a ../runtimes/osx-$arch/native/
cd ..