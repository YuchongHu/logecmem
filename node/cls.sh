#!/bin/sh

ps -ef|grep "memcached"|cut -c 9-15|xargs kill -9

IP="192.168.1.200"
PORT=11211

#data
for ((i=0;i<16;i++))
do

    memcached -d -m 2048 -u root -l $IP -p $(($PORT + $i))
done

#parity
memcached -d -m 2048 -u root -l $IP -p 20000
memcached -d -m 2048 -u root -l $IP -p 20001
memcached -d -m 2048 -u root -l $IP -p 20002