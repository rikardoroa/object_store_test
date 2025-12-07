FROM spark:latest

USER root

RUN apt-get update && apt-get install -y procps && rm -rf /var/lib/apt/lists/*

# Config hive-site.xml para ThriftServer
RUN mkdir -p /opt/spark/conf && \
    printf "%s\n" \
    "<configuration>" \
    "  <property>" \
    "    <name>hive.server2.thrift.bind.host</name>" \
    "    <value>0.0.0.0</value>" \
    "  </property>" \
    "  <property>" \
    "    <name>hive.server2.thrift.port</name>" \
    "    <value>10000</value>" \
    "  </property>" \
    "</configuration>" > /opt/spark/conf/hive-site.xml

EXPOSE 10000 4040

CMD ["/bin/bash", "-c", "\
  /opt/spark/sbin/start-thriftserver.sh \
    --master local[*] \
    --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
    --hiveconf hive.server2.thrift.port=10000 && \
  tail -f /opt/spark/logs/*"]
