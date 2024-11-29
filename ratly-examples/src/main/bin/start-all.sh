#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"

USAGE="start-all.sh <example> <subcommand>"

if [ "$#" -ne 2 ]; then
  echo "$USAGE"
  exit 1
fi

source $DIR/common.sh

# One of the examples, e.g. "filestore" or "arithmetic"
example="$1"
shift

subcommand="$1"
shift

# Find a tmpdir, defaulting to what the environment tells us
tmp="${TMPDIR:-/tmp}"

echo "Starting 3 Ratly servers with '${example}' with directories in '${tmp}' as local storage"

# The ID needs to be kept in sync with QUORUM_OPTS
$DIR/server.sh "$example" "$subcommand" --id n0 --storage "${tmp}/n0" $QUORUM_OPTS &
$DIR/server.sh "$example" "$subcommand" --id n1 --storage "${tmp}/n1" $QUORUM_OPTS &
$DIR/server.sh "$example" "$subcommand" --id n2 --storage "${tmp}/n2" $QUORUM_OPTS &

echo "Waiting for the servers"
