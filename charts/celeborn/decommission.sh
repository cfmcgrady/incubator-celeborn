#!/bin/bash

while [ $# -gt 0 ]; do
  echo "Argument: $1"
  case $1 in
    (-h|--hosts)
        shift
        ip_list="$1"
        echo "ip_list=$1"
        # 分割 IP 列表为数组
        IFS=',' read -ra ips <<< "$ip_list"
        ;;
    (-d|--decrease)
        shift
        decrease="$1"
        echo "decrease=$1"
        ;;
    (-v|--values)
        shift
        values_file="$1"
        echo "values_file=$1"
        # 从 values.yaml 获取 service.port
        service_port=$(yq eval '.service.port' "$values_file")
        echo "从 $values_file 提取的 service.port: $service_port"
        # 获取 metrics_worker 端口
        metrics_worker=$(yq eval ".portMapping.\"$service_port\".metrics_worker" "$values_file")
        echo "提取的 metrics_worker 端口: $metrics_worker"
        ;;
    (-t|--timeout)
        shift
        deco_timeout=$1
        echo "deco_timeout=$1"
        ;;
    (-r|--release)
        shift
        release="$1"
        echo "release=$1"
        ;;
    (-k|--kubeconfig)
        shift
        kubeconfig="$1"
        echo "kubeconfig=$1"
        ;;
    (*)
        echo "wrong argment $1"
        ;;
   esac
   shift
done




declare -A pod_info
declare -A pod_pvc_info
declare -A pod_exit_status
declare -A node_info


# 获取排序后的IP列表函数
get_ips() {
  if [ -z "${ip_list+x}" ]; then
    ip_list=$(kubectl --kubeconfig $kubeconfig -n celeborn get pods -l app.kubernetes.io/instance=$release -o custom-columns=pod:metadata.name,node:spec.nodeName,ip:status.hostIP | \
      awk '
      {
          # 解析Pod名称中的数字索引
          match($1, /worker-([0-9]+)$/, arr)
          idx = arr[1]

          # 提取关键字段
          ip = $3

          # 只处理数字索引存在的记录
          if (idx != "") {
              print idx,ip
          }
      }' | sort -k1,1nr | head -n "$decrease" | cut -d' ' -f2 | tr '\n' ',' | sed 's/,$//')
    IFS=',' read -ra ips <<< "$ip_list"
    echo "ips: $ip_list"
  fi
}

batch_decommission() {
  get_ips
  # 遍历每个 IP 执行操作
  local ip
  local node_name
  local pod_name
  local pvc
  for ip in "${ips[@]}"; do
      echo
      echo ">>>>>> 开始处理节点 $ip <<<<<<"

      # 1. 暂停调度对应节点
      echo "暂停节点 $ip 的调度..."
      node_name=$(kubectl --kubeconfig $kubeconfig -n celeborn get pods -l app.kubernetes.io/instance=$release --field-selector status.podIP=$ip -o custom-columns=pod:metadata.name,node:spec.nodeName,ip:status.hostIP|grep $ip|awk '{print $2}')
      echo "节点名称：$node_name"
      kubectl  --kubeconfig $kubeconfig -n celeborn cordon "$node_name"
      node_info[$ip]=$node_name

      # 2. 执行 DECOMMISSION 命令
      echo "执行 DECOMMISSION 命令...  http://$ip:$metrics_worker/exit?type=DECOMMISSION"
      pod_exit_status[$ip]="cordon"
      curl -sSf "http://$ip:$metrics_worker/exit?type=DECOMMISSION" || {
          echo "警告: 无法访问 $ip 的退出接口"
          pod_exit_status[$ip]="decommission-fail"
      }
      pod_name=$(kubectl --kubeconfig $kubeconfig -n celeborn get pods -l app.kubernetes.io/instance=$release --field-selector status.podIP=$ip -o custom-columns=pod:metadata.name,node:spec.nodeName,ip:status.hostIP|grep $ip|awk '{print $1}')
      pvc="celeborn-worker-${pod_name}"
      echo "$pvc"
      pod_info[$ip]=$pod_name
      pod_pvc_info[$ip]=$pvc
      if [[ "${pod_exit_status[$ip]}" == "cordon" ]]; then
        pod_exit_status[$ip]="decommissioning"
        kubectl --kubeconfig $kubeconfig -n celeborn delete pod "${pod_name}" --wait=false
      fi
  done
}



# 3. 等待decommission执行完成
wait_decommission() {
    # 循环检查列表中的服务状态
    local start_time=$(date +%s)
    local is_timeout=false
    local all_stopped
    local current_time
    local elapsed_time
    local pod_status
    while true; do
        all_stopped=true
        for ip in "${ips[@]}"; do
            if [[ "${pod_exit_status[$ip]}" != "decommissioned" ]]; then
                echo "$ip status: ${pod_exit_status[$ip]}"
                pod_status=$(kubectl --kubeconfig "$kubeconfig" -n celeborn get pod "${pod_info[$ip]}" -o jsonpath='{.status.phase}' 2>/dev/null)
                kubectl_exit_code=$?
                if [[ $kubectl_exit_code -eq 0 && "$pod_status" == "Pending" ]]; then
                    echo "$ip pod_status: ${pod_status}"
                    pod_exit_status["$ip"]="pending"
                elif [[ $kubectl_exit_code -eq 0 ]]; then
                    echo "$ip pod_status: ${pod_status}"
                    all_stopped=false
                else
                    echo "$ip pod_status: deleted"
                    pod_exit_status["$ip"]="decommissioned"
                fi
            fi
        done
        if [[ "$all_stopped" == true ]]; then
            echo "all stopped"
            break
        fi
        sleep 10
        current_time=$(date +%s)
        elapsed_time=$((current_time - start_time))
        if [[ "$elapsed_time" -gt "$deco_timeout" ]]; then
            echo "timeout, not all stopped"
            is_timeout=true
            break
        fi
    done
    local status_msg="退出成功"
    if [[ "$is_timeout" == true ]]; then
        status_msg="退出超时, 需要手动处理未decommission节点:"
        # delete pod
        for ip in "${ips[@]}"; do
            if [[ "${pod_exit_status[$ip]}" != "decommissioned" && "${pod_exit_status[$ip]}" != "pending" ]]; then
              status_msg="$status_msg $ip"
              echo "pod ${pod_info[$ip]} 退出超时，需要手动处理"
            fi
        done
    fi
    local url="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=0582001e-4cf1-4e5f-ac17-fd26ace6c6d6"
    local data=$(cat <<EOF
{
  "msgtype": "markdown",
  "markdown": {
    "content": "${release} 集群${ip_list} rss worker ${status_msg}",
    "mentioned_list": ["@all"]
  }
}
EOF
)
    echo "weixin notify data=$data"
    local response=$(curl -s -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$data" "$url")
    local http_code=$(echo "$response" | grep -o '[0-9]*$')
    echo "$http_code"
    if [ "$http_code" -eq 200 ]; then
      echo "Succeed in send weixin alert message"
    else
      echo "Weixin webhook response code: $http_code, response content: $response"
    fi
}

# 缩减副本数
decrease_replicas() {
  # 3. 批量修改 workerReplicas 并执行 helm upgrade
  local current_replicas=$(yq eval '.workerReplicas' "$values_file")
  local new_replicas
  if [ -z "${decrease+x}" ]; then
    new_replicas="$current_replicas"
  else
    new_replicas=$((current_replicas - decrease))
  fi
  echo
  local statefulset_name="${release}-worker"
  echo "将 StatefulSet ${statefulset_name} 从 $current_replicas 调整为 $new_replicas"
  yq eval ".workerReplicas = $new_replicas" -i "$values_file"
  helm --kubeconfig $kubeconfig -n celeborn upgrade $release --values $values_file worker/
}

delete_pvcs() {
  local pvc
  local ip
  for ip in "${ips[@]}"; do
    pvc=${pod_pvc_info[$ip]}
    if [[ "${pod_exit_status[$ip]}" == "decommissioned" || "${pod_exit_status[$ip]}" == "pending" ]]; then
      echo "delete $pvc"
      kubectl --kubeconfig $kubeconfig -n celeborn delete pvc $pvc
    else
      echo "pod ${pod_info[$ip]} 未成功退出，暂不删除pvc"
    fi
  done
}

delete_pending_pods() {
  local ip
  local pod_name
  for ip in "${ips[@]}"; do
    pod_name=${pod_info[$ip]}
    if [[ "${pod_exit_status[$ip]}" == "pending" ]]; then
      echo "delete pending pod $pod_name"
      kubectl --kubeconfig $kubeconfig -n celeborn delete pod "$pod_name"
    fi
  done
}

uncordon_nodes() {
  local node_name
  local ip
  for ip in "${ips[@]}"; do
    node_name=${node_info[$ip]}
    echo "uncordon $pvc"
    kubectl  --kubeconfig $kubeconfig -n celeborn uncordon "$node_name"
  done
}

batch_decommission
wait_decommission
if [ -n "${decrease+x}" ]; then
  decrease_replicas
  delete_pvcs
  uncordon_nodes
else
  delete_pvcs
  delete_pending_pods
  if [[ "$values_file" == "alsh1-spark-9097.yaml" ]]; then
    uncordon_nodes
  fi
fi







