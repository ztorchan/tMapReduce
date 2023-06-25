set -ex

# compile
rm -rf ./build
mkdir build
pushd ./build
cmake .. && make -j20
popd

# copy file
cp ./build/tmrmaster ./docker/master/
cp ./scripts/run_master.sh ./docker/master/
cp ./build/tmrworker ./docker/worker/
cp ./scripts/run_worker.sh ./docker/worker/
cp ./build/tmrgateway ./docker/gateway/
cp ./scripts/run_gateway.sh ./docker/gateway/
cp ./scripts/run_etcd.sh ./docker/etcd/

# build image
pushd ./docker/master
sudo docker build -t ztorchan/tmrmaster:latest .
popd
pushd ./docker/worker
sudo docker build -t ztorchan/tmrworker:latest .
popd
pushd ./docker/gateway
sudo docker build -t ztorchan/tmrgateway:latest .
popd
pushd ./docker/etcd
sudo docker build -t ztorchan/tmretcd:latest .
popd

sudo docker push ztorchan/tmrmaster:latest
sudo docker push ztorchan/tmrworker:latest
sudo docker push ztorchan/tmrgateway:latest
sudo docker push ztorchan/tmretcd:latest
