FROM ubuntu:22.04
COPY ./tmrmaster /home/tmrmaster
COPY ./run_master.sh /home/run_master.sh
COPY ./cpr-1.10.4.tar.gz /home/cpr-1.10.4.tar.gz
RUN sed -i 's#http://archive.ubuntu.com/#http://mirrors.tuna.tsinghua.edu.cn/#' /etc/apt/sources.list \
    && apt-get update --fix-missing \
    && apt-get install libprotobuf-dev openssl libssh-dev libcurlpp-dev libleveldb-dev libgflags-dev git curl make cmake wget gcc g++ -y
RUN cd /home \
    && tar -xzvf ./cpr-1.10.4.tar.gz \
    && cd cpr-1.10.4 && mkdir build && cd build \
    && cmake .. -DCPR_USE_SYSTEM_CURL=ON \
    && cmake --build . \
    && cmake --install .

