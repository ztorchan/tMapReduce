set -ex

sudo k3s kubectl delete statefulset tmrmaster
sudo k3s kubectl delete service tmrmaster-svc