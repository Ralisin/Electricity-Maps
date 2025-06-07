FROM bitnami/spark:3.5.6

USER root

# Installa pip se non è già presente
RUN apt-get update && apt-get install -y python3-pip

# Copia e installa i requirements
COPY ../scripts/requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

USER 1001
