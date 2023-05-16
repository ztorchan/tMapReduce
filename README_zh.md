# tMapReduce
  **tMapreduce**是一个MapReduce服务的简单C++实现。它可以通过作业信息动态加载指定的提前编写好的动态库文件，以实现类似反射的机制。  
  > **tMapreduce**仅是本人学习分布式系统过程中作为练习实现的项目，也将是今后学习的实验床，其中仍有大量不完善的地方，并不具备任何实用意义。在后续学习中，对于可能可以使用的技术，都将设法运用在此作为练习，不断改善并添加新的机制。

## 编译安装
  ### 依赖
  本项目依赖于`brpc`进行网络通信，并通过`gflags`进行必要信息的初始化。通过执行以下步骤安装依赖  
  1. 安装基本依赖
  - Centos
    ``` bash
    $ sudo yum install protobuf-devel protobuf-compiler openssl-devel leveldb-devel gflags-devel -y
    ```
  - Ubuntu
    ``` bash
    $ sudo apt install libprotobuf-dev openssl libleveldb-dev libgflags-dev -y
    ```
  2. 安装`brpc`
      ``` bash
      $ git clone https://github.com/apache/brpc.git
      $ mkdir build && cd build
      $ cmake -DWITH_DEBUG_SYMBOLS=OFF ..
      $ make
      $ make install
      ```
  ### 编译
  编译使用CMake，要求编译器至少支持C++17。本人使用工具版本：CMake3.16.0，GNU 8.5.0。  
  通过以下步骤编译安装
  ``` bash
  $ mkdir build && cd build
  $ cmake -DCMAKE_BUILD_TYPE=Release ..
  $ make
  $ make install
  ```
  安装完成后，将有文件
  ``` bash
  /usr/local/bin/mr_master_server  # master服务的可执行文件
  /usr/local/bin/mr_worker_server  # worker服务的可执行文件
  /usr/local/lib64/libmrmaster.a   # master相关的静态链接库（编译客户端需要用到）
  /usr/local/lib64/libmrworker.a   # worker相关的静态链接库
  /usr/local/include/mapreduce/    # mapreduce相关头文件目录
  ```

## 测试
  `./example`目录下放置了用于测试的word_count示例，编译如下：
  ```bash
  # 编译得到客户端client和用于worker动态加载的so库
  cd ./example/word_count
  mkdir build && cd build
  cmake ..
  make

  # 将编译好的so库放入路径指定路径下
  mkdir ~/mrflib
  cp ./libwordcount.so ~/mrflib/wordcount.so
  ```
  在单机上做测试，开启三个终端，分别输入如下命令，运行1个master和2个worker服务：
  ``` bash
  # Terminal 1
  $ mr_master_server -port=9023

  # Terminal 2
  $ mr_worker_server --name=test_worker_1 --worker_port=9024 --master_address=127.0.0.1 --master_port=9023 --mrf_path=./mrflib

  # Terminal 3
  $ mr_worker_server --name=test_worker_2 --worker_port=9025 --master_address=127.0.0.1 --master_port=9023 --mrf_path=./mrflib
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
1. **动态库准备**  
    编写C++源文件，引入头文件`include/mapreduce/mrf.h`，并实现其中的函数：  
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
    将编写好的源文件和`src/wrapper.cc`一同编译，形成动态库文件，并命名为`(jobtype).so`，放置在指定路径（自定）。
2. **master服务启动**
    ``` bash
    mr_master_server --id=0                  # master的id号（默认为0）
                     --name="test_master"    # master的名字（默认为空）
                     --port=9023             # master的监听端口（必填）
                     --log_path=""           # master的运行日志输出文件，若为空则仅仅输出到标准输出（默认为空）
    ```
3. **worker服务启动**
    ``` bash
    mr_worker_server --name="test_worker"         # worker的名字（默认为空）
                     --worker_port=9024           # worker的监听端口（必填）
                     --master_address=127.0.0.1   # master的IPv4地址（必填）
                     --master_port=9023           # master的端口（必填）
                     --mrf_path="./mrflib/"       # so文件目录路径（必填）
                     --log_path=""                # worker的运行日志输出文件，若为空则仅仅输出到标准输出（默认为空）
    ```
4. **客户端调用master的RPC发起作业（以word_count为例）**
    ``` C++
    #include <iostream>
    #include <fstream>
    #include <filesystem>

    #include "mapreduce/master.h"        // 引入master相关头文件

    std::string dir_path = "../text/";

    int main() {
      // 创建brpc channel options
      brpc::ChannelOptions options;
      options.protocol = "baidu_std";
      options.timeout_ms = 1000;
      options.max_retry = 3;

      // 建立brpc channel，并初始化指定master地址和端口
      brpc::Channel channel;            
      if(channel.Init("127.0.0.1", 9023, &options) != 0) {
        std::cout << "Channel initial failed." << std::endl;
        exit(1);
      }
      mapreduce::MasterService_Stub stub(&channel);   // 使用channel构造master服务的stub

      brpc::Controller cntl;                      // brpc控制器
      mapreduce::JobMsg launch_request;           // 作业信息的数据结构
      mapreduce::LaunchReplyMsg launch_response;  // 发起作业回应的数据结构
      launch_request.set_name("test_word_count"); // 设置作业名
      launch_request.set_type("wordcount");       // 设置作业类型（同so文件名一致）
      launch_request.set_mapper_num(2);           // 设置map阶段的worker数目
      launch_request.set_reducer_num(2);          // 设置reduce阶段的worker数目
     
      for(auto& p : std::filesystem::directory_iterator(dir_path)) {
        std::ifstream in_file(p.path());
        std::ostringstream buf;
        char ch;
        while(buf && in_file.get(ch)) {
          buf.put(ch);
        }

        auto new_kv = launch_request.add_kvs();   // 添加用于map的key-value对
        new_kv->set_key(p.path());
        new_kv->set_value(buf.str());
      }

      uint32_t job_id;
      stub.Launch(&cntl, &launch_request, &launch_response, NULL);  // 发起作业
        if(!cntl.Failed() && launch_response.reply().ok()) {
          // 成功发起作业，并获取master分配的作业id
          std::cout << "Successfully launch job." << std::endl;
          job_id = launch_response.job_id();
        } else if(cntl.Failed()) {
          // 网络连接失败，rpc调用失败
          std::cout << "Failed to launch: Connection error." << std::endl;
        } else {
          // rpc调用成功，但发起作业失败，并输出原因
          std::cout << "Failed to launch: " << launch_response.reply().msg() << std::endl;
        }
     
      bool finished = false;
      mapreduce::ReduceOuts results;
      while(!finished) {
        // 准备查询结果的请求信息
        mapreduce::GetResultMsg results_request;
        mapreduce::GetResultReplyMsg results_response;
        results_request.set_job_id(job_id);

        // 查询结果
        cntl.Reset();
        stub.GetResult(&cntl, &results_request, &results_response, NULL);
        if(!cntl.Failed() && results_response.reply().ok()) {
          // 成功获得作业结果
          finished = true;
          for(int i = 0; i < results_response.results_size(); i += 2) {
            std::cout << results_response.results(i) << " : " << results_response.results(i+1) << std::endl;
          }
        } else if(cntl.Failed()) {
          // 网络连接失败，rpc调用失败
          std::cout << "Failed to get result: Connection error." << std::endl;
        } else {
          // rpc调用成功，但查询失败（通常因为作业未完成）
          std::cout << "Failed to get result: " << launch_response.reply().msg() << std::endl;
        }
      }

      return 0;
    }
    ```

## 计划
- 代码优化，模块化
- master日志
- 多master的raft备份容灾
- etcd服务注册+容灾
- master提供restful api
- k8s自动化部署