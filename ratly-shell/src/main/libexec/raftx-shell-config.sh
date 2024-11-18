#!/usr/bin/env bash


# resolve links - $0 may be a softlink
this="${BASH_SOURCE-$0}"
common_bin=$(cd -P -- "$(dirname -- "${this}")" && pwd -P)
script="$(basename -- "${this}")"
this="${common_bin}/${script}"

# convert relative path to absolute path
config_bin=$(dirname "${this}")
script=$(basename "${this}")
config_bin=$(cd "${config_bin}"; pwd)
this="${config_bin}/${script}"

# This will set the default installation for a tarball installation while os distributors can
# set system installation locations.
RATLY_SHELL_HOME=$(dirname $(dirname "${this}"))
RATLY_SHELL_ASSEMBLY_CLIENT_JAR="${RATLY_SHELL_HOME}/lib/shell/*"
RATLY_SHELL_CONF_DIR="${RATLY_SHELL_CONF_DIR:-${RATLY_SHELL_HOME}/conf}"
RATLY_SHELL_LOGS_DIR="${RATLY_SHELL_LOGS_DIR:-${RATLY_SHELL_HOME}/logs}"
RATLY_SHELL_LIB_DIR="${RATLY_SHELL_LIB_DIR:-${RATLY_SHELL_HOME}/lib}"

if [[ -e "${RATLY_SHELL_CONF_DIR}/ratly-shell-env.sh" ]]; then
  . "${RATLY_SHELL_CONF_DIR}/ratly-shell-env.sh"
fi

# Check if java is found
if [[ -z "${JAVA}" ]]; then
  if [[ -n "${JAVA_HOME}" ]] && [[ -x "${JAVA_HOME}/bin/java" ]];  then
    JAVA="${JAVA_HOME}/bin/java"
  elif [[ -n "$(which java 2>/dev/null)" ]]; then
    JAVA=$(which java)
  else
    echo "Error: Cannot find 'java' on path or under \$JAVA_HOME/bin/. Please set JAVA_HOME in ratly-shell-env.sh or user bash profile."
    exit 1
  fi
fi

# Check Java version == 1.8 or >= 8
JAVA_VERSION=$(${JAVA} -version 2>&1 | awk -F '"' '/version/ {print $2}')
JAVA_MAJORMINOR=$(echo "${JAVA_VERSION}" | awk -F. '{printf "%d.%d",$1,$2}')
JAVA_MAJOR=$(echo "${JAVA_VERSION}" | awk -F. '{print $1}')
if [[ ${JAVA_MAJORMINOR} != 1.8 && ${JAVA_MAJOR} -lt 8 ]]; then
  echo "Error: ratly-shell requires Java 8+, currently Java $JAVA_VERSION found."
  exit 1
fi

local RATLY_SHELL_CLASSPATH

while read -d '' -r jarfile ; do
    if [[ "$RATLY_SHELL_CLASSPATH" == "" ]]; then
        RATLY_SHELL_CLASSPATH="$jarfile";
    else
        RATLY_SHELL_CLASSPATH="$RATLY_SHELL_CLASSPATH":"$jarfile"
    fi
done < <(find "$RATLY_SHELL_LIB_DIR" ! -type d -name '*.jar' -print0 | sort -z)

RATLY_SHELL_CLIENT_CLASSPATH="${RATLY_SHELL_CONF_DIR}/:${RATLY_SHELL_CLASSPATH}:${RATLY_SHELL_ASSEMBLY_CLIENT_JAR}"

RATLY_SHELL_JAVA_OPTS+=" -Dratly.shell.logs.dir=${RATLY_SHELL_LOGS_DIR}"

RATLY_SHELL_JAVA_OPTS+=" -Dlog4j.configuration=file:${RATLY_SHELL_CONF_DIR}/log4j.properties"
RATLY_SHELL_JAVA_OPTS+=" -Dorg.apache.jasper.compiler.disablejsr199=true"
RATLY_SHELL_JAVA_OPTS+=" -Djava.net.preferIPv4Stack=true"
RATLY_SHELL_JAVA_OPTS+=" -Dio.netty.allocator.useCacheForAllThreads=false"
