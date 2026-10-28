FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY frontend /static

RUN useradd --create-home app
USER app
EXPOSE 8080

# 2 processes per container; each runs an async event loop (uvloop).
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8080", \
     "--workers", "2", "--loop", "uvloop", "--http", "httptools", "--no-access-log"]
