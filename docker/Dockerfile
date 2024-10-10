ARG JAVA_VERSION=11
FROM eclipse-temurin:${JAVA_VERSION}

ARG HADOOP_VERSION=3.4.0
ARG HBASE_VERSION=2.5.10
ARG HALYARD_VERSION=5.2-SNAPSHOT

RUN apt-get update && \
    apt-get install -y wget gpg unzip

ENV PATH="$PATH:/mnt/hadoop/bin:/mnt/hbase/bin:/mnt/halyard"

RUN hadoop_file="hadoop-${HADOOP_VERSION}" && \
    cd /mnt && \
    wget -O hadoop.keys https://downloads.apache.org/hadoop/common/KEYS && \
    gpg --import hadoop.keys && \
    wget -N -nv https://dlcdn.apache.org/hadoop/common/${hadoop_file}/${hadoop_file}.tar.gz && \
    wget -N https://dlcdn.apache.org/hadoop/common/${hadoop_file}/${hadoop_file}.tar.gz.asc && \
    gpg --verify ${hadoop_file}.tar.gz.asc ${hadoop_file}.tar.gz && \
    tar -xzf ${hadoop_file}.tar.gz && \
    rm ${hadoop_file}.tar.gz && \
    ln -s /mnt/hadoop-${HADOOP_VERSION} /mnt/hadoop && \
    mkdir /var/lib/hadoop-hdfs

RUN hbase_file="hbase-${HBASE_VERSION}-bin" && \
    cd /mnt && \
    wget -N https://github.com/pulquero/hbase/releases/download/rel%2F${HBASE_VERSION}%2B${HADOOP_VERSION}/${hbase_file}.tar.gz && \
    tar -xzf ${hbase_file}.tar.gz && \
    rm ${hbase_file}.tar.gz && \
    ln -s /mnt/hbase-${HBASE_VERSION} /mnt/hbase

RUN cd /mnt && \
    wget -O halyard-sdk-${HALYARD_VERSION}.zip https://github.com/pulquero/Halyard/releases/download/v${HALYARD_VERSION}/halyard-sdk-${HALYARD_VERSION}.zip && \
    unzip -o halyard-sdk-${HALYARD_VERSION}.zip -d /mnt/halyard && \
    rm halyard-sdk-${HALYARD_VERSION}.zip
