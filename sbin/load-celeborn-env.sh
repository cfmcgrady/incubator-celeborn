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

# included in all the celeborn scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

# symlink and absolute path should rely on CELEBORN_HOME to resolve
if [ -z "${CELEBORN_HOME}" ]; then
  export CELEBORN_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

export CELEBORN_CONF_DIR="${CELEBORN_CONF_DIR:-"${CELEBORN_HOME}/conf"}"

if [ -z "$CELEBORN_ENV_LOADED" ]; then
  export CELEBORN_ENV_LOADED=1

  if [ -f "${CELEBORN_CONF_DIR}/celeborn-env.sh" ]; then
    # Promote all variable declarations to environment (exported) variables
    set -a
    . "${CELEBORN_CONF_DIR}/celeborn-env.sh"
    set +a
  fi
fi

# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  export JAVA="${JAVA_HOME}/bin/java"
else
  if [ "$(command -v java)" ]; then
    export JAVA="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

# Get log directory
if [ "$CELEBORN_LOG_DIR" = "" ]; then
  export CELEBORN_LOG_DIR="${CELEBORN_HOME}/logs"
fi
mkdir -p "$CELEBORN_LOG_DIR"
touch "$CELEBORN_LOG_DIR"/.celeborn_test > /dev/null 2>&1
TEST_LOG_DIR=$?
if [ "${TEST_LOG_DIR}" = "0" ]; then
  rm -f "$CELEBORN_LOG_DIR"/.celeborn_test
else
  chown "$CELEBORN_IDENT_STRING" "$CELEBORN_LOG_DIR"
fi

if [ "$CELEBORN_PID_DIR" = "" ]; then
  export CELEBORN_PID_DIR="${CELEBORN_HOME}/pids"
fi

# The jemalloc memory allocator is disabled by default, but in the docker environment, jemalloc is enabled by default.
# You can set CELEBORN_PREFER_JEMALLOC to true in celeborn-env.sh and configure the path to jemalloc via CELEBORN_JEMALLOC_PATH.
maybe_enable_jemalloc() {
  if [ "${CELEBORN_PREFER_JEMALLOC:-false}" == "true" ]; then
    JEMALLOC_PATH="${CELEBORN_JEMALLOC_PATH:-/usr/lib/$(uname -m)-linux-gnu/libjemalloc.so}"
    JEMALLOC_FALLBACK="/usr/lib/x86_64-linux-gnu/libjemalloc.so"
    if [ -f "$JEMALLOC_PATH" ]; then
      export LD_PRELOAD="$LD_PRELOAD:$JEMALLOC_PATH"
    elif [ -f "$JEMALLOC_FALLBACK" ]; then
      export LD_PRELOAD="$LD_PRELOAD:$JEMALLOC_FALLBACK"
    else
      if [ "$JEMALLOC_PATH" == "$JEMALLOC_FALLBACK" ]; then
        MSG_PATH="$JEMALLOC_PATH"
      else
        MSG_PATH="$JEMALLOC_PATH and $JEMALLOC_FALLBACK"
      fi
      echo "WARNING: attempted to load jemalloc from $MSG_PATH but the library couldn't be found. glibc will be used instead."
    fi
  fi
}
maybe_enable_jemalloc

# Function to read configuration from celeborn-defaults.conf
read_config_value() {
  local config_file="$1"
  local config_key="$2"
  
  if [ -f "$config_file" ]; then
    # Read the configuration value, handling comments and whitespace
    local value=$(grep "^[[:space:]]*${config_key}[[:space:]]*" "$config_file" | head -1 | sed 's/^[[:space:]]*[^[:space:]]*[[:space:]]*//' | sed 's/[[:space:]]*$//' | sed 's/^[[:space:]]*//')
    if [ -n "$value" ]; then
      echo "$value"
      return 0
    else
      echo "$config_key is not configured in $config_file, please check!" >&2
      return 1
    fi
  else
    echo "Celeborn config file is not found, please check $config_file" >&2
    return 1
  fi
}

# Function to test if a port is available
test_port_availability() {
  local port="$1"
  local host="${2:-localhost}"
  
  if [ "$port" = "0" ] || [ -z "$port" ]; then
    echo "Port $port is set to 0 or empty, please check it."
    return 1
  fi
  
  # Test if port is already in use
  if command -v netstat >/dev/null 2>&1; then
    if netstat -ln 2>/dev/null | grep -q ":${port}[[:space:]]"; then
      echo "WARNING: Port $port is already in use"
      return 1
    fi
  elif command -v ss >/dev/null 2>&1; then
    if ss -ln 2>/dev/null | grep -q ":${port}[[:space:]]"; then
      echo "WARNING: Port $port is already in use"
      return 1
    fi
  elif command -v lsof >/dev/null 2>&1; then
    if lsof -i ":${port}" >/dev/null 2>&1; then
      echo "WARNING: Port $port is already in use"
      return 1
    fi
  else
    echo "No port checking utility found (netstat, ss, or lsof), skipping port availability test"
    return 0
  fi
  
  echo "Port $port is available"
  return 0
}

# Function to discover all master node configurations dynamically
discover_master_node_configs() {
  local config_file="$1"
  local configs=()
  
  if [ -f "$config_file" ]; then
    # Extract all celeborn.master.ha.node.X.port and celeborn.master.ha.node.X.ratis.port configurations
    # Use grep to find all matching lines, then extract node numbers
    local node_numbers=$(grep "^[[:space:]]*celeborn\.master\.ha\.node\.[0-9]\+\.\(port\|ratis\.port\)[[:space:]]" "$config_file" | \
      sed 's/^[[:space:]]*celeborn\.master\.ha\.node\.\([0-9]\+\)\..*$/\1/' | \
      sort -n | uniq)
    
    # Build the configuration array for each discovered node
    for node_num in $node_numbers; do
      # Check if both port and ratis.port exist for this node
      local port_config="celeborn.master.ha.node.${node_num}.port"
      local ratis_port_config="celeborn.master.ha.node.${node_num}.ratis.port"
      
      # Check if port configuration exists
      if grep -q "^[[:space:]]*${port_config}[[:space:]]" "$config_file"; then
        configs+=("$port_config")
      fi
      
      # Check if ratis.port configuration exists
      if grep -q "^[[:space:]]*${ratis_port_config}[[:space:]]" "$config_file"; then
        configs+=("$ratis_port_config")
      fi
    done
  else
    echo "Configuration file $config_file not found" >&2
    return 1
  fi
  
  # Output the configurations array
  printf '%s\n' "${configs[@]}"
  return 0
}
