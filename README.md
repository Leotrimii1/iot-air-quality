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

Ky projekt implementon nje pipeline IoT per monitorimin e cilesise se ajrit duke perdorur nje simulator GUI qe gjeneron te dhena sintetike ne kohe reale.

```text
Simulator + Dashboard GUI -> MQTT/Mosquitto -> Kafka -> Spark -> Cassandra -> Grafana
```

Simulatori perfaqeson sensore optike te tipit AirGradient dhe transmeton matje PM1 dhe PM2.5 ne MQTT. Numri i sensoreve dhe frekuenca e dergimit kontrollohen nga GUI-ja e simulatorit.

## Cfare Eshte Implementuar

- GUI per simulatorin ne `http://localhost:5001`.
- Paneli i Sensoreve me numrin e sensoreve, Start dhe Stop.
- Konfigurimi i numrit te sensoreve dhe frekuences se dergimit ne milisekonda.
- Live Data shfaq vlerat e fundit per sensoret aktiv.
- Simulatori gjeneron PM1/PM2.5 vazhdimisht, pa lexuar nga CSV/Excel.
- P.sh. 1000 sensore cdo 100ms prodhojne rreth 10,000 matje/sec.
- Bridge MQTT-to-Kafka i dergon mesazhet ne topic Kafka `air-quality` dhe log-on offset-in kur Kafka i pranon.
- Spark Structured Streaming lexon mesazhet nga Kafka dhe llogarit statusin e cilesise se ajrit ne kohe reale.
- Cassandra ruan matjet me rezultatet e AI-se te perfshira ne rresht, ndersa metadata e sensoreve ruhet vecmas.
- Dashboard-i custom ne GUI lexon rezultatet nga Cassandra dhe shfaq PM2.5 Health View, Smart Monitor, alarmet dhe anomalite.
- Grafana eshte e integruar brenda GUI-se si dashboard embedded.
- Compose krijon automatikisht topic-un Kafka `air-quality` perpara se te niset procesori.
- Alarmet ngrihen ne Spark kur PM2.5 kalon pragjet e klasifikimit dhe ruhen si evente ne Cassandra.
- Anomalite zbulohen ne kohe reale perpara se rreshti te ruhet ne Cassandra.
- Dashboard-i tani shfaq nje panel `Smart Monitor` me gjendjen e monitorimit, numrin e mostrave te pastra dhe anomaline e fundit.
- Email alerts dergohen ne Mailpit me identitetin `AirWatch Prishtina`, p.sh. nga `alerts@airwatch-prishtina.com` te `operations@airwatch-prishtina.com`.
- SMS alerts mbeshteten opsionalisht me Twilio kur vendosen variablat perkates.

## Formati i te Dhenave

Simulatori publikon mesazhe JSON te ketij tipi:

```json
{
  "message_id": "4f5dd6c5-2b9f-468e-a9a2-806b7efcdbad",
  "timestamp": "2026-06-01T17:30:00.000000+00:00",
  "sensor": {
    "id": "airgradient_prishtina_001",
    "type": "AirGradient PM Simulator",
    "firmware": "sim-1.0",
    "location": "Prishtina, Kosovo",
    "latitude": 42.670917,
    "longitude": 21.151694,
    "unit": "ug/m3"
  },
  "measurements": {
    "pm1": 13.742,
    "pm2.5": 22.511,
    "relative_humidity": 58.2,
    "temperature": 18.7,
    "um003": 3376.65
  },
  "health": {
    "battery": 96.3,
    "signal": -51,
    "status": "online"
  }
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

Klasifikimi nuk ruhet ne CSV dhe nuk vjen i gatshem nga sensori. Procesori krijon/lexon tabelen `quality_ranks` kur starton Spark Streaming, pastaj perdor ato metadata per te llogaritur statusin nga PM2.5.

## Skema e Cassandra

Procesori krijon automatikisht keto tabela:

```sql
CREATE TABLE air_quality.air_quality (
    sensor_id text,
    timestamp timestamp,
    pm1 double,
    pm2_5 double,
    status text,
    location text,
    anomaly_score double,
    is_anomaly boolean,
    anomaly_reason text,
    PRIMARY KEY (sensor_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE air_quality.sensor_metadata (
    sensor_id text PRIMARY KEY,
    sensor_type text,
    firmware text,
    location text,
    latitude double,
    longitude double,
    unit text,
    updated_at timestamp
);

CREATE TABLE air_quality.quality_ranks (
    pollutant text,
    rank_order int,
    rank_name text,
    max_value double,
    PRIMARY KEY (pollutant, rank_order)
) WITH CLUSTERING ORDER BY (rank_order ASC);
```

Tabela `air_quality` ruan vetem te dhenat e nevojshme per rezultatet operative: `sensor_id`, `timestamp`, `pm1`, `pm2_5`, `status`, `location` dhe fushat e AI-se. Payload-i i plote i sensorit nuk ruhet si raw JSON, sepse `battery`, `signal`, `message_id` dhe fusha te tjera perdoren vetem per transport/simulim dhe nuk jane te nevojshme per query kryesore.

Metadata e sensorit ruhet ndaras ne `sensor_metadata`: tipi, firmware, lokacioni, koordinatat dhe njesia matese. Procesori i mban keto metadata edhe ne memory cache dhe i shkruan ne Cassandra vetem kur sensori eshte i ri ose metadata ka ndryshuar. Pragjet e klasifikimit ruhen ne `quality_ranks` dhe lexohen kur starton Spark Streaming.

Procesori krijon edhe keto tabela per alarmet:

```sql
CREATE TABLE air_quality.alarm_state (
    sensor_id text PRIMARY KEY,
    last_status text,
    last_email_sent_at timestamp,
    last_sms_sent_at timestamp,
    updated_at timestamp
);

CREATE TABLE air_quality.alarm_events (
    sensor_id text,
    event_time timestamp,
    notification_channel text,
    event_type text,
    status text,
    pm2_5 double,
    location text,
    message text,
    PRIMARY KEY ((sensor_id), event_time, notification_channel)
) WITH CLUSTERING ORDER BY (event_time DESC, notification_channel ASC);

CREATE TABLE air_quality.sensor_ai_profiles (
    sensor_id text PRIMARY KEY,
    sample_count int,
    trained_on_samples int,
    last_trained_at timestamp,
    last_scored_at timestamp,
    updated_at timestamp
);

CREATE TABLE air_quality.anomaly_events (
    sensor_id text,
    event_time timestamp,
    anomaly_score double,
    is_anomaly boolean,
    reason text,
    pm1 double,
    pm2_5 double,
    relative_humidity double,
    temperature double,
    status text,
    location text,
    PRIMARY KEY ((sensor_id), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);

CREATE TABLE air_quality.sensor_ai_samples (
    sensor_id text,
    timestamp timestamp,
    pm1 double,
    pm2_5 double,
    relative_humidity double,
    temperature double,
    PRIMARY KEY (sensor_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

Rreshti kalon fillimisht ne motorin e zbulimit te anomalive, pastaj ruhet ne Cassandra bashke me `anomaly_score`, `is_anomaly` dhe `anomaly_reason`. `Isolation Forest` trajnohet ne kohe reale nga mostra te pastra qe ruhen ne `sensor_ai_samples`, ndersa metadata e trajnimit ruhet ne `sensor_ai_profiles`.

Email alerts dergohen kur statusi kalon pragun `ALERT_EMAIL_MIN_STATUS`. Default-i i projektit eshte `Unhealthy`, sepse `Moderate` prodhon shume njoftime dhe nuk eshte i pershtatshem per alarmim operativ. Duplicate shmangen me cooldown. Recovery email mund te dergohet kur statusi zbret nen pragun e alarmit. SMS alerts jane opsionale dhe aktivizohen vetem kur vendosen `ALERT_SMS_PROVIDER=twilio` dhe kredencialet e Twilio.

Konfigurimi demo i email-it perdor adresa te brendshme te sistemit:

```text
From: alerts@airwatch-prishtina.com
To: operations@airwatch-prishtina.com
```

Keto adresa ruhen brenda Mailpit per testim dhe nuk dergojne email ne internet.

## Dashboard dhe Grafana

GUI-ja ne `http://localhost:5001` perfshin dashboard-in e vizualizimit. Te dhenat lexohen nga Cassandra, pra grafet shfaqin rezultatin pas perpunimit me Spark Streaming.

Ne tab-in `Dashboard` ka dy shtresa vizualizimi:

- Dashboard custom me karta, PM2.5 Health View, Smart Monitor, tabela te sensoreve dhe panel alarmesh.
- Grafana Analytics e integruar brenda GUI-se me iframe.

Panelet e perfshira:

- Paneli i statusit te cilesise se ajrit
- Paneli `PM2.5 Health View` me gauge te pragjeve `Good`, `Moderate`, `Unhealthy`, `Very Unhealthy`
- Tabela e metadata-s se sensoreve
- Paneli `Alarmet dhe Anomalite` me email/recovery alerts dhe anomaly events
- Paneli `Smart Monitor` per gjendjen e monitorimit automatik
- Grafana dashboard i integruar nga `http://localhost:3000` per historikun kohor dhe analiza me te detajuara

## AI dhe Alarmet

AI ne projekt eshte implementuar si zbulim anomalish ne kohe reale, jo si model parashikimi. Kjo i pershtatet mire ketij rasti sepse sensoret dergojne rrjedhe te vazhdueshme matjesh dhe sistemi duhet te kape vlera te pazakonta para se te ruhen si rezultat final.

Procesi eshte:

- Spark Streaming lexon mesazhet nga Kafka.
- Rreshti strukturohet ne kolona reale te sensorit: `pm1`, `pm2_5`, `relative_humidity`, `temperature`, `status`, `location` dhe metadata bazike.
- Metadata e sensorit kontrollohet me memory cache dhe ruhet vetem kur eshte e re ose ka ndryshuar.
- `Isolation Forest` trajnohet per secilin sensor nga mostra te pastra ne `sensor_ai_samples`.
- Ne realtime, AI e vlereson rreshtin perpara se rreshti final te ruhet ne `air_quality`.
- Gjendja e modelit ruhet ne `sensor_ai_profiles`, prandaj dashboard-i mund te tregoje nese modeli eshte ende duke mesuar apo eshte trajnuar.
- Nese AI zbulon anomali, eventi ruhet ne `anomaly_events`.
- Alarmet operative perdorin statusin e PM2.5 dhe ruhen ne `alarm_events`; email dergohet vetem nga `Unhealthy` e lart.

Per nivel projekti/enterprise demo, ky kombinim eshte i mire: rregullat e PM2.5 jane te shpjegueshme, ndersa AI kap sjellje te pazakonta qe nuk duken vetem me prag statik. Per nje sistem enterprise te plote do te shtoheshin edhe model versioning, monitorim i drift-it, alert topic ne Kafka per integrime te jashtme dhe ruajtje e metrikave te performances se modelit.

## Ekzekutimi

Nga folderi i projektit, ekzekuto:

```powershell
docker compose up --build
```

Nese projekti eshte ekzekutuar me skemen e vjeter te PM2.5, rekomandohet nje reset i volumave nje here:

```powershell
docker compose down -v
docker compose up --build
```

Sherbimet hapen ketu:

- Simulator GUI: http://localhost:5001
- Grafana: http://localhost:3000
- Mailpit: http://localhost:8025
- Spark master UI: http://localhost:8080
- MQTT: `localhost:1883`
- Cassandra: `localhost:9042`


## Perdorimi i Simulatorit

Hap GUI-ne:

```text
http://localhost:5001
```

Nga aty cakto:

- Numrin e sensoreve, p.sh. `1000`.
- Frekuencen e dergimit, p.sh. `100` ms.
- Kliko `Start` per te filluar publikimin ne MQTT.
- Kliko `Stop` per ta ndalur simulatorin.

Nese numri i sensoreve eshte `3`, simulatori krijon 3 sensore virtuale:

```text
airgradient_prishtina_001
airgradient_prishtina_002
airgradient_prishtina_003
```

Te dhenat vazhdojne te vijne derisa klikohet `Stop` ose ndalet Docker-i. Frekuenca `1000 ms` do te thote qe cdo 1 sekonde dergohet nje batch me matje per te gjithe sensoret. Pra `3` sensore me `1000 ms` japin rreth `3 matje/sec`, ndersa `1000` sensore me `100 ms` japin rreth `10,000 matje/sec`.

Live Data ne GUI shfaq vlerat e fundit, p.sh.:

```text
Sensor 1 -> 22.5
Sensor 2 -> 19.8
Sensor 3 -> 25.1
```

Per grafe dhe rezultate hap tab-in `Dashboard` brenda te njejtes GUI.
Pjesa `Grafana Analytics` shfaq dashboard-in e Grafana-s brenda GUI-se. Nese deshiron ta hapesh vecmas, perdor `http://localhost:3000`.

## Verifikimi

Gjendja dhe matjet live shihen ne Simulator GUI. Per debug mund te kontrollohen edhe log-et:

```powershell
docker compose logs -f simulator
```

Bridge-in MQTT-to-Kafka:

```powershell
docker compose logs -f bridge
```

Kur Kafka e pranon mesazhin, ne log duhet te shfaqet dicka si:

```text
Kafka accepted message: topic=air-quality, partition=0, offset=123
```

Procesimin ne Spark:

```powershell
docker compose logs -f processor
```

Grafana:

```powershell
docker compose logs -f grafana
```

Mailpit:

```powershell
docker compose logs -f mailpit
```

Kontrollimi i te dhenat ne Cassandra:

```powershell
docker compose exec cassandra cqlsh -e "SELECT sensor_id, timestamp, pm1, pm2_5, status, location FROM air_quality.air_quality LIMIT 10;"
```

Kontrollimi i metadata-s se sensoreve:

```powershell
docker compose exec cassandra cqlsh -e "SELECT * FROM air_quality.sensor_metadata LIMIT 10;"
```

Kontrollimi i pragjeve te PM2.5:

```powershell
docker compose exec cassandra cqlsh -e "SELECT * FROM air_quality.quality_ranks;"
```

Kontrollimi i eventeve te alarmit:

```powershell
docker compose exec cassandra cqlsh -e "SELECT * FROM air_quality.alarm_events LIMIT 10;"
```

Kontrollimi i gjendjes se cooldown/recovery per sensore:

```powershell
docker compose exec cassandra cqlsh -e "SELECT * FROM air_quality.alarm_state LIMIT 10;"
```

Kontrollimi i anomalive:

```powershell
docker compose exec cassandra cqlsh -e "SELECT * FROM air_quality.anomaly_events LIMIT 10;"
```

Email alert-et e testit shihen ne:

```text
http://localhost:8025
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
