FROM openjdk:11
RUN apt update -y && \
  apt upgrade -y && \
  apt-get clean && apt-get autoremove --purge && \
  apt-get remove python3.9 -y && apt-get autoremove -y
ARG JAR_FILE=target/scala-*/*.jar
RUN mkdir -p /usr/src/pipelineserver /usr/src/pipelineserver/config
COPY ${JAR_FILE} /usr/src/pipelineserver/pipelineserver.jar
COPY docker-init.sh /usr/src/pipelineserver/docker-init.sh
RUN chmod +x /usr/src/pipelineserver/docker-init.sh
ENTRYPOINT ["/usr/src/pipelineserver/docker-init.sh"]
