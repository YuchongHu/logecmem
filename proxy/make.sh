#!/bin/sh

g++ -std=c++11 update/update.cpp -o update/update -lmemcached -lisal
g++ -std=c++11 repair/repair.cpp -o repair/repair -lmemcached -lisal
