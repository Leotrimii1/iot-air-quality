FROM python:3.9-slim

WORKDIR /app

COPY docker/simulator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/simulator/ .
COPY prishtina_pm1_pm2.5.csv .

CMD ["python", "simulator.py"]
