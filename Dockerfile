FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN useradd -m -r appuser && \
    chown -R appuser /app
USER appuser

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app


CMD ["python", "main.py"]