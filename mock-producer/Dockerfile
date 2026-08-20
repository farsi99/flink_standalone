FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python

COPY generate_mock_events.py .

CMD ["python", "generate_mock_events.py"]
