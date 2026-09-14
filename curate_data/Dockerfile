FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
COPY modelardb_wheels ./modelardb_wheels

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
