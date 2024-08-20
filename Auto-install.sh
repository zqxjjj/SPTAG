#!/bin/bash

CDTDIR=$(pwd)


# Run the code in the SPFresh folder
dependencies=(
  cmake
  libjemalloc-dev
  libsnappy-dev
  libgflags-dev
  pkg-config
  swig
  libboost-all-dev
  libtbb-dev
  libisal-dev
)
# dependency
for dependency in "${dependencies[@]}"
do
  echo "install dependency: $dependency"

  apt install -y $dependency
  
  if [ $? -eq 0 ]; then
    echo "denpendency $dependency install success"
  else
    echo "dependency $dependency install fail"
    exit 1
  fi
done
# install libzmq & cppzmq
echo "Install libzmq"
cd ThirdParty/libzmq
mkdir -p build
cd build
cmake ..
sudo make -j4 install

cd $CDTDIR

echo "Install cppzmq"
cd ThirdParty/cppzmq
mkdir -p build
cd build
cmake ..
sudo make -j4 install

cd $CDTDIR

# install SPDK
echo "Install SPDK"
cd ThirdParty/spdk
./scripts/pkgdep.sh
CC=gcc-9 ./configure
CC=gcc-9 make -j

cd $CDTDIR

# install crypto
echo "Install SPDK"
cd ThirdParty/isal-l_crypto
./autogen.sh
./configure
make -j

cd $CDTDIR

# install RocksDB
echo "Install RocksDB"
cd ThirdParty/rocksdb
mkdir -p build && cd build
cmake -DUSE_RTTI=1 -DWITH_JEMALLOC=1 -DWITH_SNAPPY=1 -DCMAKE_C_COMPILER=gcc-9 -DCMAKE_CXX_COMPILER=g++-9 -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-fPIC" ..
make -j
sudo make install

cd $CDTDIR

# install SPFresh
echo "Install SPFresh"
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j

cd $CDTDIR


