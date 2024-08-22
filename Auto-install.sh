#!/bin/bash

CDTDIR=$(pwd)


# Run the code in the SPFresh folder
dependencies=(
  swig
  libssl-dev
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

cd $CDTDIR

# install cmake
wget https://github.com/Kitware/CMake/releases/download/v3.22.0/cmake-3.22.0.tar.gz
tar -zxvf cmake-3.22.0.tar.gz 
cd cmake-3.22.0
./bootstrap 
make -j32
make install

cd $CDTDIR
mv /usr/bin/cmake /usr/bin/cmake_old
ln -s cmake-3.22.0/bin/cmake /usr/bin/

# install boost
wget https://archives.boost.io/release/1.71.0/source/boost_1_71_0.tar.gz
tar -zxvf boost_1_71_0.tar.gz 
cd boost_1_71_0
/bootstrap.sh --prefix=/usr/
./b2
./b2 install

# install libzmq & cppzmq
echo "Install libzmq"
cd ThirdParty/libzmq
mkdir -p build
cd build
cmake ..
make -j4 install

cd $CDTDIR

echo "Install cppzmq"
cd ThirdParty/cppzmq
mkdir -p build
cd build
cmake ..
make -j4 install

cd $CDTDIR

# install SPFresh
echo "Install SPFresh"
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j

cd $CDTDIR


