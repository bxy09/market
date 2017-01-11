#!/bin/bash
cd `dirname $0`
cd ../../tdf/ && g++ -pthread -fPIC -g -O3 -shared -rdynamic -o lib/libreader.so csrc/reader.cpp -L./lib -lrt -lTDFAPI_v2.7 -I./include && cd - && \
    go build && mkdir -p linux && mv cmd linux/ && cp ../tdf/lib/lib*.so linux/

