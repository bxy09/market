#!/bin/bash
curPath=`pwd`
parentPath=`dirname ${curPath}`
parentPath=`dirname ${parentPath}`
echo $parentPath
parentPkg=${parentPath#${GOPATH}src/}
echo $parentPkg
buildImage=daocloud.io/gpx_dev/debian:go-1.7 
docker run -it -v $parentPath:/go/src/$parentPkg $buildImage bash
