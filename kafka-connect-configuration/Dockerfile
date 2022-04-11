FROM confluentinc/cp-kafka-connect:latest
USER root
RUN mkdir /plugins
RUN mkdir /config
COPY kafka-connect-cassandra-*.jar /plugins/
COPY config /config/
RUN ["chmod", "+x", "/config/connector-entrypoint.sh"]
ENV EXPOSED_PORT 8083
ENTRYPOINT ["/config/connector-entrypoint.sh"]