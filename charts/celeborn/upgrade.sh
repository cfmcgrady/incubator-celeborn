#!/bin/bash

step=1
while [ $# -gt 0 ]; do
  echo "Argument: $1"
  case $1 in
    (-m|--mode)
        shift
        mode="$1"
        echo "mode=$1"
        ;;
    (-s|--step)
        shift
        step="$1"
        echo "step=$1"
        ;;
    (-i|--interval)
        shift
        interval="$1"
        echo "interval=$1 secs"
        ;;
    (-v|--values)
        shift
        values_file="$1"
        echo "values_file=$1"
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

# 定义通用的更新检查函数
check_pod_updated() {
  local POD_NAME="${release}-$mode-$1"

  # 检查controller-revision-hash
  local current_rev=$(kubectl --kubeconfig "$kubeconfig" -n celeborn get pod "$POD_NAME" -o jsonpath='{.metadata.labels.controller-revision-hash}' | awk -F'-' '{print $NF}')
  local expected_rev=$(kubectl --kubeconfig "$kubeconfig" -n celeborn get statefulset "${release}-$mode" -o jsonpath='{.status.updateRevision}' | awk -F'-' '{print $NF}')
  local pod_status=$(kubectl --kubeconfig "$kubeconfig" -n celeborn get pod "$POD_NAME" -o jsonpath='{.status.phase}')
  echo "pod_name: $POD_NAME, current_rev: $current_rev, expected_rev: $expected_rev, pod_status: $pod_status"
  [[ "$current_rev" == "$expected_rev" && "$pod_status" == "Running" ]] && return 0
  return 1
}

check_statefulset_updated() {
  local expected_rev=$(kubectl --kubeconfig "$kubeconfig" -n celeborn get statefulset "${release}-$mode" -o jsonpath='{.status.updateRevision}' | awk -F'-' '{print $NF}')
  local current_rev=$(kubectl --kubeconfig "$kubeconfig" -n celeborn get statefulset "${release}-$mode" -o jsonpath='{.status.currentRevision}' | awk -F'-' '{print $NF}')
  echo "statefulset: ${release}-$mode, current_rev: $current_rev, expected_rev: $expected_rev"
  [[ "$current_rev" == "$expected_rev" ]] && return 0
  return 1
}

wait_statefulset_updated() {
  # 等待Pod就绪
  while ! check_statefulset_updated; do
    echo "等待statefulset ${release}-$mode 完成更新..."
    sleep 5
  done
  echo "statefulset ${release}-$mode 完成更新"

}

do_upgrade() {
  current_replicas=$(yq eval ".${mode}Replicas" "$values_file")
  local ord=$((current_replicas))
  local POD_NAME
  while true; do
    ord=$((ord-step))
    if [[ $ord -lt 0 ]]; then
      ord=0
    fi
    echo "更新Pod $ord，partition设置为$ord"
    helm --kubeconfig "$kubeconfig" -n celeborn upgrade "$release" --values "$values_file" "$mode/" --set "partition.$mode"=$ord
    POD_NAME="${release}-$mode-$ord"
    echo "等待Pod $POD_NAME 更新完成..."

    # 等待Pod就绪
    while ! check_pod_updated $ord; do
      echo "等待 $POD_NAME 完成更新..."
      sleep 5
    done

    if [[ $ord -gt 0 ]]; then
      echo "Pod $POD_NAME 更新完成，等待 $interval 秒..."
      sleep $interval
    else
      break
    fi
  done

  echo "所有Pod更新完成！"
}


do_upgrade
wait_statefulset_updated




