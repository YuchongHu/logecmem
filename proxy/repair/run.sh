#!/bin/bash

N=14
K=10
dir="./"
IP="192.168.1.200"

echo -e "($N, $K) in $dir"

ssh root@node "bash cls.sh"
sleep 2
./repair $dir $N $K $IP> /dev/null