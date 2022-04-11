FROM cassandra:3.11.8
COPY cassandra-entrypoint.sh /cassandra-entrypoint.sh
RUN ["chmod", "+x", "/cassandra-entrypoint.sh"]
ENTRYPOINT ["/cassandra-entrypoint.sh"]
CMD ["cassandra", "-f"]