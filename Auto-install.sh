#!/bin/bash

CDTDIR=$(pwd)


# Run the code in the SPFresh folder
dependencies=(
  cmake
  swig
  libboost-all-dev
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


