set -ex

# this node endpoint
this_node="$(hostname).${SVC_NAME}.${NAMESPACE}.svc.cluster.local:${TMRMASTER_PORT}"

# add peers
raft_peers=""
for ((i=0;i<${TMRMASTER_REPLICAS};i++));
do
  raft_peers="${raft_peers}${STATEFULSET_NAME}-${i}.${SVC_NAME}.${NAMESPACE}.svc.cluster.local:${TMRMASTER_PORT}:0,"
done

/home/tmrmaster \
  -id=${TMRMASTER_ID} \
  -name="${TMRMASTER_NAME}" \
  -port=${TMRMASTER_PORT} \
  -etcd_ep="${TMRMASTER_ETCD_EP}" \
  -group="${TMRMASTER_GROUP}" \
  -conf="${raft_peers}" \
  -this_endpoint="${this_node}" \
  -data_path="${TMRMASTER_DATA_PATH}"