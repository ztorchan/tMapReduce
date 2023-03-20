# tMapReduce

  [中文版](https://github.com/ztorchan/tMapReduce/README_zh.md)

  **tMapReduce** is a simple implement of MapReduce service based on C++17。It can dynamically load special pre-builded shared library through job information to achieve a mechanism similar to reflect.
  > **tMapReduce** is just an experimental project during my own study of distributed system. It will also be an experimental bed for my further study. At present, it still has many imperfections and cannot be applied to practical production. In further study, I will try to apply any technology that may be available here as an exercise, continuously improve tMapreduce and add new mechanisms for tMapreduce.

## Build && Install
  ### Dependencies
  tMapReduce relies on `brpc` for network communication, and initializes necessary information through `gflags`. Install all dependencies by following steps
  1. Install basic dependencies
  - Centos
    ``` bash
    $ sudo yum install protobuf-devel protobuf-compiler openssl-devel leveldb-devel gflags-devel -y
    ```
  - Ubuntu
    ``` bash
    $ sudo apt install libprotobuf-dev openssl libleveldb-dev libgflags-dev -y
    ```
  2. Install `brpc`
      ``` bash
      $ git clone https://github.com/apache/brpc.git
      $ mkdir build && cd build
      $ cmake -DWITH_DEBUG_SYMBOLS=OFF ..
      $ make
      $ make install
      ```
  ### Build
  Toolchain: C++ Compiler and CMake that support C++ 17. (My own toolchain version：CMake 3.16.0，GNU 8.5.0)  
  ``` bash
  $ mkdir build && cd build
  $ cmake -DCMAKE_BUILD_TYPE=Release ..
  $ make
  $ make install
  ```
  When the installation is complete, there will be files
  ``` bash
  /usr/local/bin/mr_master_server  # master service executable file
  /usr/local/bin/mr_worker_server  # worker service executable file
  /usr/local/lib64/libmrmaster.a   # static library related to master (used when compiling client)
  /usr/local/lib64/libmrworker.a   # static library related to worker
  /usr/local/include/mapreduce/    # mapreduce header directory
  ```

## Test
  A word_count example has been placed in directory `./example`. Build it as followed:  
  ```bash
  # Build client and shared library that will be loaded by the worker
  cd ./example/word_count
  mkdir build && cd build
  cmake ..
  make

  # Place the so file under the specified path
  mkdir ~/mrflib
  cp ./libwordcount.so ~/wordcount.so
  ```
  Perform the test on a single machine. Start three terminals and enter the following commands respectively. One master and two workers will be running:
  ``` bash
  # Terminal 1
  $ mr_master_server -port=9023

  # Terminal 2
  $ mr_worker_server --name=test_worker_1 --worker_port=9024 --master_address=127.0.0.1 --master_port=9023 --mrf_path=./mrflib

  # Terminal 3
  $ mr_worker_server --name=test_worker_2 --worker_port=9025 --master_address=127.0.0.1 --master_port=9024 --mrf_path=./mrflib
  ```
  Start another terminal and run the client. If tMapReduce runs correctly, there will be result:  
  ```
  0 : 273
  1 : 619
  2 : 179
  3 : 127
  4 : 136
  5 : 138
  6 : 99
  7 : 104
  8 : 146
  9 : 106
  a : 199063
  b : 35879
  c : 55287
  d : 119155
  e : 307517
  f : 51261
  g : 51012
  h : 157870
  i : 150479
  j : 3003
  k : 23250
  l : 99952
  m : 61774
  n : 171725
  o : 195689
  p : 36859
  q : 1929
  r : 137724
  s : 145788
  t : 225062
  u : 74943
  v : 22075
  w : 62035
  x : 3046
  y : 54280
  z : 1086
  ```

## Usage
1. **Prepare shared library**  
    Code C++ source file. Include header file `include/mapreduce/mrf.h` and implement the functions therein:   
     ``` C++
     #include "mapreduce/mrf.h"
     /*
      * using MapIn = std::pair<std::string, std::string>;
      * using MapOut = std::vector<std::pair<std::string, std::string>>;
      * using ReduceIn = std::pair<std::string, std::vector<std::string>>;
      * using ReduceOut = std::vector<std::string>;
      */
     mapreduce::MapOut Map(const mapreduce::MapIn& map_kv) {
      // ......
     }
     mapreduce::ReduceOut Reduce(const mapreduce::ReduceIn& reduce_kv) {
      // ......
     }
     ```
    Compile the C++ file with `src/wrapper.cc` to form a shared library. Name the library `(jobtype).so` and place it in the specified path (customized).  

2. **Start master**
    ``` bash
    mr_master_server --id=0                  # master id (Default: 0)
                     --name="test_master"    # master name (Default: empty)
                     --port=9023             # Listening port of the master (necessary)
                     --log_path=""           # Output path of master log. If empty, it will only be output to standard output. (Default: empty)
    ```
3. **Start worker**
    ``` bash
    mr_worker_server --name="test_worker"         # worker name (Default: empty)
                     --worker_port=9024           # Listening port of the worker (necessary)
                     --master_address=127.0.0.1   # IPv4 address of master (necessary)
                     --master_port=9023           # master port (necessary)
                     --mrf_path="./mrflib/"       # so file directory path (necessary)
                     --log_path=""                # Output path of worker log. If empty, it will only be output to standard output. (Default: empty)
    ```
4. **Client calls master RPC to launch jobs. (Take word_count as an example)**
    ``` C++
    #include <iostream>
    #include <fstream>
    #include <filesystem>

    #include "mapreduce/master.h"        // Include header file related to master

    std::string dir_path = "../text/";

    int main() {
      // Create brpc channel options
      brpc::ChannelOptions options;
      options.protocol = "baidu_std";
      options.timeout_ms = 1000;
      options.max_retry = 3;

      // Create brpc channel. Initialize with master address and port
      brpc::Channel channel;            
      if(channel.Init("127.0.0.1", 9023, &options) != 0) {
        std::cout << "Channel initial failed." << std::endl;
        exit(1);
      }
      mapreduce::MasterService_Stub stub(&channel);   // Create master stub with channel

      brpc::Controller cntl;                      // brpc controller
      mapreduce::JobMsg launch_request;           // Launch request
      mapreduce::LaunchReplyMsg launch_response;  // Launch response
      launch_request.set_name("test_word_count"); // Set job name 
      launch_request.set_type("wordcount");       // Set job type (same as so file name)
      launch_request.set_mapper_num(2);           // Set mapper number
      launch_request.set_reducer_num(2);          // Set reducer number
     
      for(auto& p : std::filesystem::directory_iterator(dir_path)) {
        std::ifstream in_file(p.path());
        std::ostringstream buf;
        char ch;
        while(buf && in_file.get(ch)) {
          buf.put(ch);
        }

        auto new_kv = launch_request.add_kvs();   // Add map key-value
        new_kv->set_key(p.path());
        new_kv->set_value(buf.str());
      }

      uint32_t job_id;
      stub.Launch(&cntl, &launch_request, &launch_response, NULL);  // Launch job
        if(!cntl.Failed() && launch_response.reply().ok()) {
          // Successfully launch and get job id distributed by master
          std::cout << "Successfully launch job." << std::endl;
          job_id = launch_response.job_id();
        } else if(cntl.Failed()) {
          // Connection error. Failed to call RPC.
          std::cout << "Failed to launch: Connection error." << std::endl;
        } else {
          // Successfully call RPC but failed to launch job. Output the reason.
          std::cout << "Failed to launch: " << launch_response.reply().msg() << std::endl;
        }
     
      bool finished = false;
      mapreduce::ReduceOuts results;
      while(!finished) {
        // Prepare the request to get results
        mapreduce::GetResultMsg results_request;
        mapreduce::GetResultReplyMsg results_response;
        results_request.set_job_id(job_id);

        // Get results
        cntl.Reset();
        stub.GetResult(&cntl, &results_request, &results_response, NULL);
        if(!cntl.Failed() && results_response.reply().ok()) {
          // Successfully get results
          finished = true;
          for(int i = 0; i < results_response.results_size(); i += 2) {
            std::cout << results_response.results(i) << " : " << results_response.results(i+1) << std::endl;
          }
        } else if(cntl.Failed()) {
          // Connection error. Failed to call RPC.
          std::cout << "Failed to get result: Connection error." << std::endl;
        } else {
          // Successfully call RPC but failed to get result.（Usually because the job is not completed）
          std::cout << "Failed to get result: " << launch_response.reply().msg() << std::endl;
        }
      }

      return 0;
    }
    ```
