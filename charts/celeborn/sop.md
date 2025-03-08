# RSS on K8S 操作手册

## 说明
1. clusterName为集群名，deployPort为master端口，分为9097/9197/9297
2. $kubeconfig为kubeconfig文件路径，例如/home/data/.kube/config_alish1_spark(dt-dahlonega-02)和/home/hadoop/.kube/alsh1-spark-gray-config(10.98.15.243)
3. ${release-master}命名规则为$clusterName-master-$deployPort-$datestr，$datestr为日期字符串，如03181145
   1. 可以执行helm --kubeconfig $kubeconfig -n celeborn list查看集群中已经部署的实例
4. ${release-worker}命名规则为$clusterName-worker-$deployPort-$datestr，$datestr为日期字符串，如03181145
5. 操作节点
   1. dt-dahlonega-02节点;生产分支(red-branch-0.4)，操作前在路径~/celeborn/incubator-celeborn/路径下拉取最新代码;data用户;操作路径：~/celeborn/incubator-celeborn/charts/celeborn
   2. 10.98.15.243节点;生产分支(red-branch-0.4)，操作前在路径/mnt/disk1/celeborn/k8s/incubator-celeborn/路径下拉取最新代码;root用户;操作路径：/mnt/disk1/celeborn/k8s/incubator-celeborn/charts/celeborn

## 新集群部署
1. 从生产分支(red-branch-0.4) checkout 新分支 $clusterName-$deployPort，复制charts/celeborn下values.yaml文件至$clusterName-$deployPort.yaml并根据需要修改其中配置，将新分支 $clusterName-$deployPort合入生产分支(red-branch-0.4)
   1. yaml文件中service.port为$deployPort
   2. yaml文件中masterHosts从集群中dedicate=spark-master的节点中取
2. 在操作节点拉取代码并执行
   ```shell
   # 如果没有celeborn namespace，先创建celeborn namespace
   kubectl --kubeconfig $kubeconfig apply -f charts/namespace.yaml
   # 部署master实例
   helm --kubeconfig $kubeconfig -n celeborn install ${release-master} --values $clusterName-$deployPort.yaml master/
   # 部署worker实例
   helm --kubeconfig $kubeconfig -n celeborn install ${release-worker} --values $clusterName-$deployPort.yaml worker/
   ```
## 原地升级
### worker 原地升级
修改$clusterName-$deployPort.yaml中的配置并合入从生产分支(red-branch-0.4)，在操作节点拉取代码并执行
```shell
# 升级worker实例
# step表示每次更新的实例个数
nohup ./upgrade.sh --kubeconfig $kubeconfig --values $clusterName-$deployPort.yaml --mode worker --step $step --interval $sleep_interval --release ${release-worker} >> /tmp/upgrade.log &
```
### master 原地升级
修改$clusterName-$deployPort.yaml中的配置并合入从生产分支(red-branch-0.4)，在操作节点拉取代码并执行
```shell
# 升级master实例
nohup ./upgrade.sh --kubeconfig $kubeconfig --values $clusterName-$deployPort.yaml --mode master --step 1 --interval $sleep_interval --release ${release-master} >> /tmp/upgrade.log &
```
## 集群扩容
修改$clusterName-$deployPort.yaml中的workerReplicas配置，执行原地升级步骤
## 集群缩容
在操作节点拉取代码并执行
```shell
# ${decrease_replicas} 为缩减的节点数
nohup ./decommission.sh --release ${release-worker} --values $clusterName-$deployPort.yaml --decrease ${decrease_replicas} --timeout $timeoutsecs --kubeconfig $kubeconfig > /tmp/decommission.log &
# 退役超时的节点手动处理
kubectl --kubeconfig $kubeconfig -n celeborn delete pod $pod_name --grace-period=0 --force
kubectl --kubeconfig $kubeconfig -n celeborn delete pvc celeborn-worker-$pod_name
kubectl --kubeconfig $kubeconfig -n celeborn delete pod $pod_name
```
## 节点退役
### worker 节点退役
在操作节点拉取代码并执行
```shell
# $ips为英文逗号分隔的ip列表
nohup ./decommission.sh --release ${release-worker} --values $clusterName-$deployPort.yaml --hosts $ips --timeout $timeoutsecs --kubeconfig $kubeconfig > /tmp/decommission.log &
# 退役超时的节点手动处理
kubectl --kubeconfig $kubeconfig -n celeborn delete pod $pod_name --grace-period=0 --force
kubectl --kubeconfig $kubeconfig -n celeborn delete pvc celeborn-worker-$pod_name
kubectl --kubeconfig $kubeconfig -n celeborn delete pod $pod_name
```
### master节点退役
在操作节点拉取代码并执行
```shell
#开两个窗口执行
kubectl --kubeconfig $kubeconfig -n celeborn cordon $node_ip
kubectl --kubeconfig $kubeconfig -n celeborn delete pod $pod_name
kubectl --kubeconfig $kubeconfig -n celeborn delete pvc celeborn-master-$pod_name
```
### 更换master节点
1. 执行master节点退役流程
2. 修改values文件中masterHosts值，更换旧节点ip
3. 执行master原地升级流程
## 集群下线
```shell
# 集群下线
helm --kubeconfig $kubeconfig -n celeborn uninstall ${release-worker}
helm --kubeconfig $kubeconfig -n celeborn uninstall ${release-master}
```
## 其他命令
```shell
# 查看pod日志
kubectl --kubeconfig $kubeconfig -n celeborn logs $pod_name
# 查看pod事件
kubectl --kubeconfig $kubeconfig -n celeborn describe pod $pod_name
# 查看yaml
kubectl --kubeconfig $kubeconfig -n celeborn get pod/storageclass/pvc xxx -o yaml
# 查看pod列表
kubectl --kubeconfig $kubeconfig -n celeborn get pods -o wide
kubectl --kubeconfig $kubeconfig -n celeborn get pods -l app.kubernetes.io/name=celeborn -l app.kubernetes.io/port=9097
kubectl --kubeconfig $kubeconfig -n celeborn get pods --show-labels
# 登陆pod
kubectl --kubeconfig $kubeconfig -n celeborn exec -it  $pod_name -- /bin/bash
```

## 部署方案选型
集群采用 statefulset + pv + hostnetwork 方式部署，基于以下几点考量：
1. worker 是有状态的服务，优雅重启需要从磁盘恢复 metadata 数据，同时 ip 或域名标识 worker 的身份，这意味着
   1. pod 重启，ip 或者域名不能变
   2. 同名 pod 不能飘到其他节点
2. 有可能 spark 任务与 celeborn 不在同一个集群

社区方案是 statefulset + hostpath + service，但集群外不能访问 worker，并且同名 pod 可能飘到其他节点，因而不予采用

## decommission 流程
decommission 分为集群缩容和指定节点退役，以下描述decommission.sh脚本中这两种decommission的流程
### 集群缩容
1. 按 pod index 由大至小取 top n，获取对应 IP 列表
   1. statefulset 按照 pod index 由大至小缩容
2. cordon 这些节点
3. 对这些节点执行 decommission 命令，此命令异步执行，worker 接收到 decommission 命令会进行截流并等待进行中的 shuffle 执行完成才退出
4. 对这些节点异步执行 delete pod 命令
   1. 若不执行 delete pod 命令，则只会在container级别重启，pod 并不会退出
5. 等待直至超时或者这些节点全部退出，并发送退役成功的节点或超时节点
   1. 当 pod 退出后，K8S 会尝试重新调度此 pod，此时由于 pod 绑定的 pv 还在当前节点，所以 K8S 会尝试将 pod 调度到当前节点，又由于当前节点被暂停调度，所以新调度的 pod 会处于 node(s) were unschedulable 导致的 Pending 状态
   2. pod 有可能 decommission 超时，此时处于 Terminating 状态
   3. pod 退出后，index 更小的 pod 还处于 Pending 状态，所以 pod 还可能未被重新调度，处于已删除的状态，即 get pod 得到为空
6. 执行 upgrade，更新集群副本数至 worker 部署组 currentReplicas - n
   1. 此命令异步执行
   2. statefulset 按照 pod index 由大至小缩容(delete pod)，此前这些节点都在第三步接收到了 decommission 命令，针对此时 pod 的状态分析如下：
      1. 已删除/Pending：K8S 继续删除下一个 pod
      2. Terminating：preStop hook 只会在第一次 delete pod 执行，K8S 会等待这一个 pod 删除完成再删除下一个 pod，此时可以手动处理此节点或者等待此节点退出完成
   3. 执行这一步之后，对于我们第一步取到的 IP 列表，statefulset 期望的状态就是终止状态，因此无论是这些节点 pod 绑定的 pv 删掉或者是这些节点被 uncordon，这些 pod 也不会被重新调度
7. 删除退役成功节点的 pvc
   1. 退役成功(已删除/Pending)的节点数据可以删除，因此可以直接删除pvc/pv，storageclass为csi-local-host-path，reclaimPolicy为Delete，删除pvc的同时会删除pv
   2. 处于 Terminating 状态的节点会发消息手动处理，处理完成 K8S 会再删除下一个 pod
8. uncordon 这些节点
### 指定节点退役
1. 指定 IP 列表，cordon 这些节点
2. 对这些节点执行 decommission 命令，此命令异步执行，worker 接收到 decommission 命令会进行截流并等待进行中的 shuffle 执行完成才退出
3. 对这些节点异步执行 delete pod 命令
   1. 若不执行 delete pod 命令，则只会在container级别重启，pod 并不会退出
4. 等待直至超时或者这些节点全部退出，并发送退役成功的节点或超时节点
   1. 当 pod 退出后，K8S 会尝试重新调度此 pod，此时由于 pod 绑定的 pv 还在当前节点，所以 K8S 会尝试将 pod 调度到当前节点，又由于当前节点被暂停调度，所以新调度的 pod 会处于 node(s) were unschedulable 导致的 Pending 状态
   2. pod 有可能 decommission 超时，此时处于 Terminating 状态
   3. pod 退出后，index 更小的 pod 还处于 Pending 状态，所以 pod 还可能未被重新调度，处于已删除的状态，即 get pod 得到为空
5. 删除退役成功节点的 pvc
   1. 退役成功(已删除/Pending)的节点数据可以删除，因此可以直接删除pvc/pv，storageclass为csi-local-host-path，reclaimPolicy为Delete，删除pvc的同时会删除pv
   2. 处于 Terminating 状态的节点会发消息手动处理
6. 删除处于 Pending 状态的 pod
   1. 第四步 index 最小的 pod 处于 node(s) were unschedulable 导致的 Pending 状态，此时还需要重新执行 delete pod，触发此 pod 的重新调度
   2. 第五步删除了 IP 列表 pod 绑定的 pv，statefulset 会按照 pod index 由小到大依次重新调度这些节点，由于 IP 列表被禁止调度，这些 pod 会被调度到新节点

## 原地升级流程
以下描述upgrade.sh脚本中原地升级流程
1. 指定partition的值，由replicas-1至0依次递减，每次递减的步长为step，分别执行upgrade，每一次执行，k8s只会更新pod index >= partition 的pod
2. 每次upgrade之间 sleep $interval 秒
3. 通过比较pod的controller-revision-hash与statefulset的updateRevision是否相等来判断pod是否升级完成
   1. StatefulSet 的 updateRevision 表示目标版本（期望所有Pod最终达到的版本），currentRevision 表示当前稳定版本（所有Pod已完成的版本）。
   2. StatefulSet 会按照 pod index 由大至小更新，因此每一次upgrade只需要检查此次更新最小的index的pod的状态
4. 最后校验StatefulSet 的 updateRevision与currentRevision是否一致
