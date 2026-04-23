FROM bitnami/spark:3.5.1

USER root
WORKDIR /opt/spark/app

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
      delta-spark==3.2.0 \
      boto3==1.35.0 \
      pyyaml==6.0.2

COPY spark /opt/spark/app/spark
COPY common /opt/spark/app/common
COPY schemas /opt/spark/app/schemas
COPY config /opt/spark/app/config

USER 1001
