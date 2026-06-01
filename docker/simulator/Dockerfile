FROM python:3.9-slim

WORKDIR /app

COPY docker/simulator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/simulator/ .

CMD ["python", "simulator.py"]
