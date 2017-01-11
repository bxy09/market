#!/bin/bash
curPath=`pwd`
parentPath=`dirname ${curPath}`
echo $parentPath
parentPkg=${parentPath#${GOPATH}src/}
echo $parentPkg
buildImage=daocloud.io/gpx_dev/debian:go-1.7 
docker run -v $parentPath:/go/src/$parentPkg $buildImage /go/src/$parentPkg/cmd/inner.sh && \
docker build . -t market:v0.1
