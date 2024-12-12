#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"

LIB_DIR=${SCRIPT_DIR}/../lib

if [[ -d "$LIB_DIR" ]]; then
   #release directory layout
   LIB_DIR=`cd ${LIB_DIR} > /dev/null; pwd`
   ARTIFACT=`ls -1 ${LIB_DIR}/*.jar`
else
   #development directory layout
   EXAMPLES_DIR=${SCRIPT_DIR}/../../../
   if [[ -d "$EXAMPLES_DIR" ]]; then
      EXAMPLES_DIR=`cd ${EXAMPLES_DIR} > /dev/null; pwd`
   fi
   JAR_PREFIX=`basename ${EXAMPLES_DIR}`
   ARTIFACT=`ls -1 ${EXAMPLES_DIR}/target/${JAR_PREFIX}-*.jar | grep -v test | grep -v javadoc | grep -v sources | grep -v shaded`
   if [[ ! -f "$ARTIFACT" ]]; then
      echo "Jar file is missing. Please do a full build (mvn clean package -DskipTests) first."
      exit -1
   fi
fi

#echo "Found ${ARTIFACT}"

QUORUM_OPTS="--peers n0:localhost:6000,n1:localhost:6001,n2:localhost:6002"

CONF_DIR="$DIR/../conf"
if [[ -d "${CONF_DIR}" ]]; then
  LOGGER_OPTS="-Dlogback.configuration=file:${CONF_DIR}/logback.xml"
else
  LOGGER_OPTS="-Dlogback.configuration=file:${DIR}/../resources/logback.xml"
fi
