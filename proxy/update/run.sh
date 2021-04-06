#!/bin/bash

N=14
K=10
dir="./"
IP="192.168.1.200"

echo -e "($N,$K) in $dir"

#1-> in-palce; 2->full-stripe; 3->LogECMem
ssh root@node "bash cls.sh"
sleep 2
./update 1 $dir $N $K $IP> /dev/null

ssh root@node "bash cls.sh"
sleep 2
./update 2 $dir $N $K $IP> /dev/null

ssh root@node "bash cls.sh"
sleep 2
./update 3 $dir $N $K $IP> /dev/null