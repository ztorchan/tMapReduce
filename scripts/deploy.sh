set -ex

sudo k3s kubectl apply -f ./k8s/master_svc.yml
sudo k3s kubectl apply -f ./k8s/master_statefulset.yml