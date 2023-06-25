set -ex

# this node
this_name="$(hostname)"
this_client_url="http://$(hostname):${TMRETCD_PORT}"
this_peers_url="http://$(hostname):${TMRETCD_LISTEN_PEERS_PORT}"

# clusters
clusters=""
for ((i=0;i<${TMRMASTER_REPLICAS};i++));
do
  clusters="${clusters}${STATEFULSET_NAME}-${i}=${STATEFULSET_NAME}-${i}.${SVC_NAME}.${NAMESPACE}.svc.cluster.local:${TMRETCD_LISTEN_PEERS_PORT}"
  if [ ${i} -lt `expr ${TMRMASTER_REPLICAS} - 1` ]; then
    clusters="${clusters},"
  fi
done

./etcd \
  --name "${this_name}" \
  --listen-client-urls "${this_client_url}" \
  --advertise-client-urls "${this_client_url}" \
  --listen-peer-urls "${this_peers_url}" \
  --initial-advertise-peer-urls "${this_peers_url}" \
  --initial-cluster-token "${STATEFULSET_NAME}" \
  --initial-cluster ${clusters} \
  --initial-cluster-state new
