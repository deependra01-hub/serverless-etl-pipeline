FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
      confluent-kafka==2.6.0 \
      jsonschema==4.23.0 \
      prometheus-client==0.20.0

COPY producer /app/producer
COPY common /app/common
COPY schemas /app/schemas

CMD ["python", "-m", "producer.app.main"]
