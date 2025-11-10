FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir pika psycopg2-binary requests

COPY trazabilidad.py .

CMD ["python", "-u", "trazabilidad.py"]
