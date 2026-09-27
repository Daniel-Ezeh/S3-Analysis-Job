FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY s3_to_sqs.py .

CMD ["python", "s3_to_sqs.py"]
