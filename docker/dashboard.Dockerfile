FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
      streamlit==1.39.0 \
      deltalake==0.18.2 \
      pandas==2.2.3

COPY dashboard /app/dashboard

EXPOSE 8501

CMD ["streamlit", "run", "dashboard/app/main.py", "--server.port=8501", "--server.address=0.0.0.0"]
