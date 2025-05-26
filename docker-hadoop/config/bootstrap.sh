#!/bin/bash

: ${HADOOP_HOME:=/usr/local/hadoop}
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Start SSH
service ssh start

# Remove old PID files
rm -f /tmp/*.pid

if [[ "$HOSTNAME" == "master" ]]; then
    echo "[BOOTSTRAP] Starting Hadoop master node"

    # Format HDFS (solo se non è già stato fatto)
    if [ ! -d "/hadoop/dfs/name/current" ]; then
        echo "[BOOTSTRAP] Formatting NameNode..."
        hdfs namenode -format -force
    else
        echo "[BOOTSTRAP] NameNode already formatted."
    fi

    # Start HDFS (NameNode + Secondary + tutti i DataNode dichiarati in workers)
    $HADOOP_HOME/sbin/start-dfs.sh

    # Aspetta un attimo che HDFS sia avviato prima di fare operazioni sul filesystem
    sleep 10

    echo "[BOOTSTRAP] Creating /nifi directory with permissions"

    # Crea la directory /nifi in HDFS e imposta permessi
    hdfs dfs -mkdir -p /nifi
    hdfs dfs -chown nifi:supergroup /nifi
    hdfs dfs -chmod 755 /nifi
else
    echo "[BOOTSTRAP] Starting Hadoop slave node"
    hdfs --daemon start datanode
fi

# Tieni il container attivo
tail -f /dev/null