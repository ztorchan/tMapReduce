# tMapReduce
  **tMapreduce**是一个MapReduce服务的简单C++实现。它可以通过作业信息动态加载指定的提前编写好的动态库文件，以实现类似反射的机制。  
  > **tMapreduce**仅是本人学习分布式系统过程中作为练习实现的项目，也将是今后学习的实验床，其中仍有大量不完善的地方，并不具备任何实用意义。在后续学习中，对于可能可以使用的技术，都将设法运用在此作为练习，不断改善并添加新的机制。

## 编译安装
  ### 依赖
  本项目依赖于`brpc`进行网络通信，并通过`gflags`进行必要信息的初始化。通过执行以下步骤安装依赖  
  1. 安装基本依赖
  - Centos
    ```
    $ sudo yum install protobuf-devel protobuf-compiler openssl-devel leveldb-devel gflags-devel -y
    ```
  - Ubuntu
    ```
    $ sudo apt install libprotobuf-dev openssl libleveldb-dev libgflags-dev -y
    ```
  2. 安装`brpc`
      ```
      $ git clone https://github.com/apache/brpc.git
      $ mkdir build && cd build
      $ cmake -DWITH_DEBUG_SYMBOLS=OFF ..
      $ make
      $ make install
      ```
  ### 编译
  编译使用CMake，要求编译器至少支持C++17。本人使用工具版本：CMake3.16.0，GNU 8.5.0。  
  通过以下步骤编译安装
  ```
  $ mkdir build && cd build
  $ cmake -DCMAKE_BUILD_TYPE=Release ..
  $ make
  $ make install
  ```
  安装完成后，将有文件
  ```
  /usr/local/bin/mr_master_server  # master服务的可执行文件
  /usr/local/bin/mr_worker_server  # worker服务的可执行文件
  /usr/local/lib64/libmrmaster.a   # master相关的静态链接库（编译客户端需要用到）
  /usr/local/lib64/libmrworker.a   # worker相关的静态链接库
  /usr/local/include/mapreduce/    # mapreduce相关头文件目录
  ```

## 测试
  `./example`目录下放置了用于测试的word_count示例，编译如下：
  ```
  # 编译得到客户端client和用于worker动态加载的so库
  cd ./example/word_count
  mkdir build && cd build
  cmake ..
  make

  # 将编译好的so库放入路径指定路径下
  mkdir ~/mrflib
  cp ./libwordcount.so ~/wordcount.so
  ```
  在单机上做测试，开启三个终端，分别输入如下命令，运行1个master和2个worker服务：
  ```
  # Terminal 1
  $ mr_master_server

  # Terminal 2
  $ mr_worker_server --name=test_worker_1 --worker_port=9024 --master_address=127.0.0.1 --master_port=9023 --mrf_path=./mrflib

  # Terminal 3
  $ mr_worker_server --name=test_worker_2 --worker_port=9025 --master_address=127.0.0.1 --master_port=9024 --mrf_path=./mrflib
  ```
  再开启一个终端，运行上述编译好的客户端client，若正确运行则有以下结果
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

## 使用