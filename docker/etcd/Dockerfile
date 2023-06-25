FROM ubuntu:22.04
RUN apt-get update \
    && apt-get install wget tar -y \
    && wget https://github.com/etcd-io/etcd/releases/download/v3.5.9/etcd-v3.5.9-linux-amd64.tar.gz \
    && tar -xzvf ./etcd-v3.5.9-linux-amd64.tar.gz \
    && cp ./etcd-v3.5.9-linux-amd64/etcd ./etcd
COPY ./run_etcd.sh /home/run_etcd.sh
