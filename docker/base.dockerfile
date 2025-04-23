FROM docker-reg.devops.xiaohongshu.com/data-engine/base/centos8/base-centos8-redjdk17:20250218

RUN set -ex && \
    yum install -y bash tini busybox bind bind-utils telnet net-tools procps krb5-workstation krb5-libs jemalloc-5.2.1-3.el8.x86_64 && \
    ossutil cp -f oss://xhs-bigdata-data-engine/celeborn/OpenJDK17U-jdk_x64_linux_hotspot_17.0.9_9.tar.gz ./ && \
    tar -xzvf OpenJDK17U-jdk_x64_linux_hotspot_17.0.9_9.tar.gz && \
    rm -f OpenJDK17U-jdk_x64_linux_hotspot_17.0.9_9.tar.gz && mv jdk-17.0.9+9 /opt/ && \
    ln -snf /bin/bash /bin/sh && \
    rm -rf /var/cache/apt/* && \
    mkdir /opt/busybox && \
    busybox --install /opt/busybox

