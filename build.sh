#!/bin/bash
set -e
CONFIG=$1

if [[ -z "${CONFIG}" ]]; then
    CONFIG=Debug
fi
if [[ -r /opt/rh/devtoolset-6/enable ]]; then
    source /opt/rh/devtoolset-6/enable
fi
#rm -f -r build || true
mkdir -p build-${CONFIG}
pushd  build-${CONFIG} &> /dev/null
if [[ ! -r Makefile ]]; then
    cmake .. -DCMAKE_BUILD_TYPE=${CONFIG}
fi
make -j8 || exit 1
popd &> /dev/null
