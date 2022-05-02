#!/bin/bash

./coordinator -p 5050 &

./server -i 1 -h localhost -p 9910 -t master &
./server -i 2 -h localhost -p 9920 -t master &
./server -i 3 -h localhost -p 9930 -t master &

./server -i 1 -h localhost -p 9810 -t slave &
./server -i 2 -h localhost -p 9820 -t slave &
./server -i 3 -h localhost -p 9830 -t slave &

./synchronizer -i 1 -h localhost -p 9710 &
./synchronizer -i 2 -h localhost -p 9720 &
./synchronizer -i 3 -h localhost -p 9730 &

