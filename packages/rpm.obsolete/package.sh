#!/bin/bash
set -e

CONFIG=$1

if [[ -z "${CONFIG}" ]]; then
    CONFIG=Debug
fi

DIRNAME=`dirname ${BASH_SOURCE[0]}`
DIRNAME=`readlink --canonicalize ${DIRNAME}`

if [[ -r /opt/rh/devtoolset-6/enable ]]; then
    source /opt/rh/devtoolset-6/enable
fi

mkdir -p cmake-build-${CONFIG}
pushd  cmake-build-${CONFIG} &> /dev/null
if [[ ! -r Makefile ]]; then
    cmake .. -DCMAKE_BUILD_TYPE=${CONFIG}
fi
rm -f *.rpm
make -j8 package || exit 1
rm -f *-Unspecified.rpm
popd &> /dev/null
