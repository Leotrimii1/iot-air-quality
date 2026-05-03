<table border="0">
 <tr>
    <td>
      <p>Universiteti i Prishtinës/ University of Prishtina</p>
      <p>Fakulteti i Inxhinierisë Elektrike dhe Kompjuterike</p>
      <p>Inxhinieri Kompjuterike dhe Softuerike - Programi Master</p>
      <p>Profesor:Besmir Sejdiu</p>
    </td>
 </tr>
</table>

# Sistemi IoT per Monitorimin e Cilesise se Ajrit

Ky projekt implementon nje pipeline IoT per monitorimin e cilesise se ajrit duke perdorur te dhena reale nga OpenAQ per Prishtinen si sensor virtual.

```text
Simulator -> MQTT/Mosquitto -> Kafka -> Spark -> Cassandra -> Grafana
```

Sensori i simuluar perfaqeson nje sensor optik te tipit AirGradient dhe transmeton matje PM1 dhe PM2.5 nga dataset-i `prishtina_pm1_pm2.5.csv`.

## Cfare Eshte Implementuar

- Perdor dataset real per Prishtinen.
- Rreshtat renditen sipas kohes para transmetimit.
- Nese dataset-i eshte ne format long OpenAQ, simulatori e kthen ne format wide.
- MQTT publikon nga nje matje JSON cdo 1 sekonde.
- Bridge MQTT-to-Kafka i dergon mesazhet ne topic Kafka `air-quality`.
- Spark Structured Streaming lexon mesazhet nga Kafka dhe llogarit statusin e cilesise se ajrit.
- Cassandra ruan te dhenat e procesuara ne skemen e kerkuar.
- Grafana konfigurohet automatikisht me datasource per Cassandra dhe dashboard te gatshem.

## Formati i te Dhenave

Simulatori publikon mesazhe JSON te ketij tipi:

```json
{
  "timestamp": "2026-02-08T08:00:00+00:00",
  "pm1": 21.304125,
  "pm25": 33.74333312,
  "sensor_id": "airgradient_prishtina_001",
  "location": "Prishtina, Kosovo",
  "latitude": 42.670917,
  "longitude": 21.151694
}
```

## Klasifikimi ne Spark

Spark e llogarit statusin nga PM2.5 gjate procesimit te stream-it:

```text
PM2.5 <= 15  -> Good
PM2.5 <= 35  -> Moderate
PM2.5 <= 55  -> Unhealthy
PM2.5 > 55   -> Very Unhealthy
```

Klasifikimi nuk ruhet ne CSV. Ai llogaritet ne `src/processor/processor.py` dhe pastaj ruhet ne Cassandra.

## Skema e Cassandra

Procesori e krijon automatikisht kete tabele:

```sql
CREATE TABLE air_quality.air_quality (
    sensor_id text,
    timestamp timestamp,
    pm1 float,
    pm25 float,
    status text,
    location text,
    PRIMARY KEY (sensor_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

## Dashboard ne Grafana

Grafana konfigurohet automatikisht nga fajllat ne `docker/grafana`.

Panelet e perfshira:

- Grafiku kohor per PM1
- Grafiku kohor per PM2.5
- Paneli i statusit te cilesise se ajrit
- Paneli me vlerat me te fundit
- Vizualizim me ngjyra sipas pragjeve te PM2.5

## Ekzekutimi

Nga folderi i projektit, ekzekuto:

```powershell
docker compose up --build
```

Sherbimet hapen ketu:

- Grafana: http://localhost:3000
- Spark master UI: http://localhost:8080
- MQTT: `localhost:1883`
- Cassandra: `localhost:9042`


Dashboard-i gjendet ketu:

```text
Dashboards -> Air Quality -> Prishtina Air Quality
```
![Grafana Dashboard](image.png)


## Verifikimi

Mesazhet e simulatorit:

```powershell
docker compose logs -f simulator
```

Bridge-in MQTT-to-Kafka:

```powershell
docker compose logs -f bridge
```

Procesimin ne Spark:

```powershell
docker compose logs -f processor
```

Kontrollimi i te dhenat ne Cassandra:

```powershell
docker compose exec cassandra cqlsh -e "SELECT * FROM air_quality.air_quality LIMIT 10;"
```

Kontrollimi nese Kafka topic ekziston:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Duhet te shfaqet:

```text
air-quality
```

Ndalo container-at duke ruajtur te dhenat:

```powershell
docker compose down
```

Ndalo container-at dhe fshi volumet e Cassandra/Grafana/Kafka:

```powershell
docker compose down -v
```
