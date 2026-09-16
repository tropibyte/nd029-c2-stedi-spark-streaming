# Replacement for gcr.io/simulation-images/kafka-connect-redis-source, which is
# no longer pullable: Udacity's registry now refuses anonymous requests.
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY redis_source_bridge.py /app/redis_source_bridge.py

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/redis_source_bridge.py"]
