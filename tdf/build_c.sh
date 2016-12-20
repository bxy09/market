#!/bin/sh -x
g++ -pthread -fPIC -g -O3 -rdynamic -shared -o lib/libreader.so csrc/reader.cpp -L./lib -lrt -lTDFAPI_v2.7 -I./include 
g++ -pthread -fPIC -g -O3 -shared -o lib/libreader.a csrc/reader.cpp -L./lib -lrt -lTDFAPI_v2.7 -I./include 
#LD_LIBRARY_PATH=./lib/ go run tdf.go
