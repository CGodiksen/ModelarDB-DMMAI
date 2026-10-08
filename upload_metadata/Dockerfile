FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY upload_metadata.py .

CMD ["python", "-u", "upload_metadata.py"]