#!/bin/bash

sudo pacman -S cmake
sudo pacman -S libomp
sudo pacman -S make
git clone --branch v1.7.4 --depth 1 https://github.com/facebookresearch/faiss.git  libfaiss-src
cd libfaiss-src
cmake -DFAISS_ENABLE_GPU=OFF -DFAISS_ENABLE_PYTHON=OFF -DBUILD_TESTING=OFF -DCMAKE_BUILD_TYPE=Release -DFAISS_ENABLE_C_API=ON -DBUILD_SHARED_LIBS=OFF -B build .
sudo make -C build -j faiss
sudo make -C build install

arch=arm64
if [[ $(uname -m) == 'x86_64' ]]; then
  arch=x64
fi

cp build/c_api/libfaiss_c.a ../runtimes/osx-$arch/native/
cd ..