Welcome
=====

This is the source code for LogECMem, which is tested on Ubuntu 16.04.


Preparation
====
 
These are the required libraries that users need to download separately.

 - make & automake-1.14
 - yasm & nasm
 - libtool
 - boost libraries (libboost-all-dev)
 - libevent (libevent-dev)
`$ sudo apt-get install gcc g++ make cmake autogen autoconf automake yasm nasm libtool libboost-all-dev libevent-dev`

Users can install the following library manually: *Intel®-storage-acceleration-library (ISA-l)*.

    $ tar -zxvf isa-l-2.14.0.tar.gz
    $ cd isa-l-2.14.0
    $ sh autogen.sh
    $ ./configure; make; sudo make install


LogECMem Installation
====

**Memcached Servers**

Users can use apt-get to install Memcached instances in servers.

    $ sudo apt-get install memcached

For standalone setup, users can use `bash cls.sh` to re-set memcached instances with IPs and Ports;
For distributed setup, users can use multiple nodes with different IPs to run memcached instances.


**LogECMem Proxy**

Users use source code to install libmemcached in the proxy.

	$ cd libmemcached-1.0.18
	$ sh configure; make; sudo make install
	$ export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH  #(if it ocuurs library path issue)

Users can setup passwardless SSH login, and use `SSH root@node bash cls.sh` to re-set memcached instances in the proxy.

User use g++ to compile update.cpp and repair.cpp. 
	
	$ bash make.sh

**Workloads**

User can use the provided workloads (`ycsb_set.txt` and `ycsb_test.txt`) in each directory to do demo tests, and further more workloads via YCSB with *basic* parameter in *https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload*.

Benchmarks
====

1.**Update latency and Memory overhead**
Users can configure *N, K, workload dir, and server IP* parameters, and gain all three In-palace, Full-stripe and LogECMem results. **"./update [1|2|3] dir N K IP > /dev/null"**, 1|2|3 indicates In-place|Full-stripe|LogECMem respectively, *N* indicates the number of all data and parity chunks, *K* indicates the number of all data chunks, *dir* indicates the path of workloads and *IP* indicates the DRAM node's IP. Note that users can configure more IPs and Ports in update.cpp and run.sh for distributed setup.

	
	$ cd update
	$ bash run.sh

2.**Multiple chunks failures performance**
Users can configure *N, K, workload dir and server IP* parameters, and gain all schemes' repair performance. **"./repair dir N K > /dev/null"**, *N* indicates the number of all data and parity chunks, *K* indicates the number of all data chunks, *dir* indicates the path of workloads and *IP* indicates the DRAM node's IP. Note that users can configure more in repair.cpp and run.sh for distributed setup.


	$ cd repair
	$ bash run.sh