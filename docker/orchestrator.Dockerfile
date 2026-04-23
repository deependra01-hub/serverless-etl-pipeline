FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
      confluent-kafka==2.6.0 \
      kubernetes==30.1.0

COPY orchestrator /app/orchestrator
COPY common /app/common

CMD ["python", "-m", "orchestrator.app.main"]
