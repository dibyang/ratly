#!/usr/bin/env bash


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"
source $DIR/common.sh
if [ -z "$SUSPEND" ]; then
  SUSPEND="n"
fi
echo "DEBUG PORT=$DEBUG_PORT"
JAVA_OPTS="$LOGGER_OPTS"
if [ -n "$DEBUG_PORT" ]; then
  JAVA_OPTS="$JAVA_OPTS -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=$SUSPEND,address=$DEBUG_PORT"
fi
java ${JAVA_OPTS} -cp :$LIB_DIR/* net.xdob.ratly.examples.common.RatlyRunner "$@"
