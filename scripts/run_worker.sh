set -ex

/home/tmrworker \
  -name="${TMRWORKER_NAME}" \
  -port=${TMRWORKER_PORT} \
  -this_ep="${TMRWORKER_EP}" \
  -master_eps="${TMRWORKER_MASTER_EPS}" \
  -mrf_path="${TMRWORKER_MRF_PATH}"

