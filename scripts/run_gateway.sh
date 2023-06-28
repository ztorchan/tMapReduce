set -ex

/home/tmrgateway \
  -master_group="${TMRGATEWAY_MASTER_GROUP}" \
  -master_conf="${TMRGATEWAY_MASTER_CONF}" \
  -gateway_port="${TMRGATEWAY_GATEWAY_PORT}"

