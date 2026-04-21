# IoT Air Quality Monitoring Project

This project simulates an IoT air quality monitoring system using MQTT for data transmission, Apache Cassandra for data storage, and Grafana for visualization.

## Architecture

- **MQTT Broker (Mosquitto)**: Handles message passing between devices and services.
- **Simulator**: Python script that generates fake air quality data and publishes it to MQTT.
- **Consumer**: Python script that subscribes to MQTT topics and stores data in Cassandra.
- **Cassandra**: NoSQL database for storing air quality measurements.
- **Grafana**: Dashboard for visualizing air quality data (requires Cassandra datasource plugin).

## Services

- MQTT: Port 1883
- Cassandra: Port 9042
- Grafana: Port 3000 (http://localhost:3000, admin/admin)

## Running the Project

1. Ensure Docker and Docker Compose are installed.

2. Clone or navigate to the project directory.

3. Run the following command to start all services:

   ```bash
   docker-compose up --build
   ```

4. Access Grafana at http://localhost:3000 (username: admin, password: admin).

5. In Grafana, install the Cassandra datasource plugin manually from the plugin marketplace (search for "Cassandra"), then add Cassandra as a data source:
   - Host: cassandra:9042
   - Keyspace: air_quality
   - Consistency: ONE

6. Create dashboards to visualize the air quality data from the `measurements` table.

## Data Format

The simulator sends JSON data with the following fields:
- `timestamp`: Unix timestamp
- `pm25`: PM2.5 concentration (µg/m³)
- `pm10`: PM10 concentration (µg/m³)
- `co2`: CO2 concentration (ppm)
- `temperature`: Temperature (°C)
- `humidity`: Humidity (%)
- `voc`: VOC index

Data is stored in Cassandra table `measurements` with columns: sensor_id, timestamp, pm25, pm10, co2, temperature, humidity, voc.

## Stopping the Project

To stop all services:

```bash
docker-compose down
```

To also remove volumes (data will be lost):

```bash
docker-compose down -v
```