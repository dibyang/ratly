#!/usr/bin/env bash


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"
source $DIR/common.sh

java ${LOGGER_OPTS} -cp :$LIB_DIR/* net.xdob.ratly.examples.common.RatlyRunner "$@"
