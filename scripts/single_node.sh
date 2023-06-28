./tmrmaster -id=0 -name=tmrmaster-0 -port=2333 -etcd_ep=127.0.0.1:23333 -group=tmrmaster-group-0 -conf=127.0.0.1:2333:0,127.0.0.1:2334:0,127.0.0.1:2335:0 -this_endpoint=127.0.0.1:2333 -data_path=./data/master-0/

./tmrmaster -id=0 -name=tmrmaster-1 -port=2334 -etcd_ep=127.0.0.1:23334 -group=tmrmaster-group-0 -conf=127.0.0.1:2333:0,127.0.0.1:2334:0,127.0.0.1:2335:0 -this_endpoint=127.0.0.1:2334 -data_path=./data/master-1/

./tmrmaster -id=0 -name=tmrmaster-2 -port=2335 -etcd_ep=127.0.0.1:23335 -group=tmrmaster-group-0 -conf=127.0.0.1:2333:0,127.0.0.1:2334:0,127.0.0.1:2335:0 -this_endpoint=127.0.0.1:2335 -data_path=./data/master-2/

./etcd --name tmretcd-0 --listen-client-urls http://127.0.0.1:23333 --advertise-client-urls http://127.0.0.1:23333 --listen-peer-urls http://127.0.0.1:23336 --initial-advertise-peer-urls http://127.0.0.1:23336 --initial-cluster-token tmretcd-group-0 --initial-cluster 'tmretcd-0=http://127.0.0.1:23336,tmretcd-1=http://127.0.0.1:23337,tmretcd-2=http://127.0.0.1:23338' --initial-cluster-state new

./etcd --name tmretcd-1 --listen-client-urls http://127.0.0.1:23334 --advertise-client-urls http://127.0.0.1:23334 --listen-peer-urls http://127.0.0.1:23337 --initial-advertise-peer-urls http://127.0.0.1:23337 --initial-cluster-token tmretcd-group-0 --initial-cluster 'tmretcd-0=http://127.0.0.1:23336,tmretcd-1=http://127.0.0.1:23337,tmretcd-2=http://127.0.0.1:23338' --initial-cluster-state new

./etcd --name tmretcd-2 --listen-client-urls http://127.0.0.1:23335 --advertise-client-urls http://127.0.0.1:23335 --listen-peer-urls http://127.0.0.1:23338 --initial-advertise-peer-urls http://127.0.0.1:23338 --initial-cluster-token tmretcd-group-0 --initial-cluster 'tmretcd-0=http://127.0.0.1:23336,tmretcd-1=http://127.0.0.1:23337,tmretcd-2=http://127.0.0.1:23338' --initial-cluster-state new

./tmrgateway -master_group=tmrmaster-group-0 -master_conf=127.0.0.1:2333:0,127.0.0.1:2334:0,127.0.0.1:2335:0 -gateway_port=2336

./tmrworker -name=tmrworker-0 -port=2337 -this_ep=127.0.0.1:2337 -master_eps=127.0.0.1:2333 -mrf_path=/home/czt/mrf/

./tmrworker -name=tmrworker-1 -port=2338 -this_ep=127.0.0.1:2338 -master_eps=127.0.0.1:2333 -mrf_path=/home/czt/mrf/

curl -H "Content-Type: application/json" -X POST -d '{"name": "wc-test", "type": "wordcount", "mapper_num": 2, "reducer_num": 2, "token": "ztorchan", "kvs": [{"key": "1", "value": "sgdakjsdkashdashdashdjkasdhkjasdhkashdk"}, {"key": "2", "value": "sdakjshdkajsdhkasjhdakshdashdaskjfgsdfgsjdhfsdf6sd4f65sd4af"}, {"key": "3", "value": "dhaksjdhksadhasjkdashdasdhkjasdhakjshdkasjdhksajda"}]}' "http://127.0.0.1:2336/launch"

curl "http://127.0.0.1:2336/getresult?job_id=1&token=ztorchan"

./client -txt_path=../text/ -gateway=127.0.0.1:2336