#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"

USAGE="client.sh <example> <command>"

if [ "$#" -lt 2 ]; then
  echo "$USAGE"
  exit 1
fi

source $DIR/common.sh

# One of the examples, e.g. "filestore" or "arithmetic"
example="$1"
shift

subcommand="$1"
shift

java ${LOGGER_OPTS} -cp :$LIB_DIR/* net.xdob.ratly.examples.common.RatlyRunner "$example" "$subcommand" "$@"
