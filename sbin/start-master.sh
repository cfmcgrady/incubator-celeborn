#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Starts the celeborn master on the machine this script is executed on.

if [ -z "${CELEBORN_HOME}" ]; then
  export CELEBORN_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${CELEBORN_HOME}/sbin/load-celeborn-env.sh"

# Read master port configurations
CELEBORN_DEFAULTS_CONF="${CELEBORN_CONF_DIR}/celeborn-defaults.conf"

echo "Reading master port configurations from: $CELEBORN_DEFAULTS_CONF"

# Check if master HA is enabled
HA_ENABLED=$(read_config_value "$CELEBORN_DEFAULTS_CONF" "celeborn.master.ha.enabled" 2>/dev/null || echo "false")
HA_ENABLED=$(echo "$HA_ENABLED" | tr '[:upper:]' '[:lower:]')

echo "Master HA enabled: $HA_ENABLED"

# Only perform port checking if HA is enabled
if [ "$HA_ENABLED" = "true" ]; then
  # Dynamically discover all master port configurations
  echo "Discovering master node configurations..."
  MASTER_PORT_CONFIGS_ARRAY=($(discover_master_node_configs "$CELEBORN_DEFAULTS_CONF"))

  # Calculate the number of unique nodes based on discovered configurations
  # Each node should have exactly 2 configurations: port and ratis.port
  UNIQUE_NODE_COUNT=$((${#MASTER_PORT_CONFIGS_ARRAY[@]} / 2))

  # Check if we have at least 3 master nodes configured
  if [ $UNIQUE_NODE_COUNT -lt 3 ]; then
    echo ""
    echo "ERROR: Master cluster requires at least 3 nodes for high availability."
    echo "Currently configured nodes: $UNIQUE_NODE_COUNT"
    echo "Please configure at least 3 master nodes in $CELEBORN_DEFAULTS_CONF"
    exit 1
  fi

  echo "Master cluster configuration validated: $UNIQUE_NODE_COUNT nodes configured"

  # Check each master port configuration
  FAILED_PORTS=""
  for port_config in "${MASTER_PORT_CONFIGS_ARRAY[@]}"; do
    port_value=$(read_config_value "$CELEBORN_DEFAULTS_CONF" "$port_config")

    # Check if read_config_value succeeded
    if [ $? -ne 0 ]; then
      echo "ERROR: Failed to read configuration for $port_config"
      exit 1
    fi

    echo "Checking $port_config: $port_value"
    # Test port availability
    if ! test_port_availability "$port_value"; then
      echo "ERROR: $port_value is not available"
      FAILED_PORTS="$FAILED_PORTS $port_config:$port_value"
    fi
  done

  # Check if any ports failed
  if [ -n "$FAILED_PORTS" ]; then
    echo ""
    echo "ERROR: The following master ports are not available:"
    for failed_port in $FAILED_PORTS; do
      echo "  - ${failed_port%:*}: ${failed_port#*:}"
    done
    echo "Master Start Fail, Please check the master port configuration"
    exit 1
  fi

  echo "All master HA ports are available!"
else
  echo "Master HA is disabled, skipping HA port availability checks."
fi

echo "Checking master prometheus port configuration..."
PROMETHEUS_PORT_CONFIG="celeborn.metrics.master.prometheus.port"
prometheus_port_value=$(read_config_value "$CELEBORN_DEFAULTS_CONF" "$PROMETHEUS_PORT_CONFIG" 2>/dev/null)

if [ $? -eq 0 ]; then
  echo "Checking $PROMETHEUS_PORT_CONFIG: $prometheus_port_value"
  if ! test_port_availability "$prometheus_port_value"; then
    echo "ERROR: Master prometheus port $prometheus_port_value is not available"
    echo "Master Start Fail, Please check the master prometheus port configuration"
    exit 1
  fi
  echo "Master prometheus port is available!"
else
  echo "Master prometheus port is not configured, please check."
  exit 1
fi

if [ "$CELEBORN_MASTER_MEMORY" = "" ]; then
  CELEBORN_MASTER_MEMORY="1g"
fi

CELEBORN_JAVA_OPTS="$CELEBORN_MASTER_JAVA_OPTS"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS -Xmx$CELEBORN_MASTER_MEMORY"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS -Dio.netty.tryReflectionSetAccessible=true"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS --illegal-access=warn"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS --add-opens=java.base/java.lang=ALL-UNNAMED"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS --add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS --add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS --add-opens=java.base/java.io=ALL-UNNAMED"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS --add-opens=java.base/java.net=ALL-UNNAMED"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS --add-opens=java.base/java.nio=ALL-UNNAMED"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS --add-opens=java.base/java.util=ALL-UNNAMED"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS --add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS --add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS --add-opens=java.base/sun.security.action=ALL-UNNAMED"
CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
export CELEBORN_JAVA_OPTS="$CELEBORN_JAVA_OPTS"

exec "${CELEBORN_HOME}/sbin/celeborn-daemon.sh" start org.apache.celeborn.service.deploy.master.Master 1 "$@"