
FROM python:3.12-slim-bookworm

RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless build-essential libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py minio_sink.py spark_stream.py company_metadata.csv start.sh ./

RUN chmod +x start.sh

CMD ["./start.sh"]