#!/bin/bash

set -xe

cp /config/* /usr/src/pipelineserver/config

java ${JAVA_OPTS} -Dspring.profiles.active=${SPRING_PROFILE} -Dspring.config.location=${SPRING_CONFIG_LOCATION} -jar /usr/src/pipelineserver/pipelineserver.jar
