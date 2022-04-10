FROM confluentinc/cp-kafka-connect:latest
COPY connector-entrypoint.sh /connector-entrypoint.sh
USER root
RUN ["chmod", "+x", "/connector-entrypoint.sh"]
ENTRYPOINT ["/connector-entrypoint.sh"]