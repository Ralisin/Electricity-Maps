FROM apache/nifi:1.28.1

USER root

RUN apt-get update && apt-get install -y \
    wget build-essential libssl-dev libffi-dev libbz2-dev libreadline-dev libsqlite3-dev zlib1g-dev \
    && cd /tmp \
    && wget https://www.python.org/ftp/python/3.12.0/Python-3.12.0.tgz \
    && tar -xzf Python-3.12.0.tgz \
    && cd Python-3.12.0 \
    && ./configure --enable-optimizations \
    && make -j$(nproc) \
    && make altinstall \
    && ln -sf /usr/local/bin/python3.12 /usr/bin/python3 \
    && ln -sf /usr/local/bin/pip3.12 /usr/bin/pip3 \
    && cd / && rm -rf /tmp/Python-3.12.0 /tmp/Python-3.12.0.tgz \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Installa le dipendenze Python dal requirements.txt
COPY ../scripts/requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Imposta le versioni di Spark e Hadoop
ENV SPARK_VERSION=3.5.6
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Scarica e installa Spark
RUN curl -fsSL https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

# Copia dei file di configurazione Hadoop
COPY docker-NiFi/hadoop-conf/core-site.xml /opt/nifi/hadoop-conf/core-site.xml
COPY docker-NiFi/hadoop-conf/hdfs-site.xml /opt/nifi/hadoop-conf/hdfs-site.xml

# (Facoltativo) Link simbolici per Spark verso la configurazione Hadoop
RUN mkdir -p /opt/spark/conf && \
    ln -s /opt/nifi/hadoop-conf/core-site.xml /opt/spark/conf/core-site.xml && \
    ln -s /opt/nifi/hadoop-conf/hdfs-site.xml /opt/spark/conf/hdfs-site.xml

USER nifi
