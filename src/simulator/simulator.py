import json
import logging
import math
import os
import random
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from flask import Flask, jsonify, render_template_string, request


BROKER = os.getenv("MQTT_BROKER", "mqtt")
PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC = os.getenv("MQTT_TOPIC", "air-quality/sensor1")
UI_PORT = int(os.getenv("SIMULATOR_UI_PORT", "5000"))
DEFAULT_SENSOR_COUNT = int(os.getenv("SIMULATOR_SENSOR_COUNT", "3"))
DEFAULT_INTERVAL_MS = int(os.getenv("SIMULATOR_INTERVAL_MS", "1000"))
MAX_LIVE_READINGS = int(os.getenv("SIMULATOR_MAX_LIVE_READINGS", "60"))

LOCATION = os.getenv("SIMULATOR_LOCATION", "Prishtina, Kosovo")
BASE_LATITUDE = float(os.getenv("SIMULATOR_LATITUDE", "42.670917"))
BASE_LONGITUDE = float(os.getenv("SIMULATOR_LONGITUDE", "21.151694"))


app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

state_lock = threading.Lock()
stop_event = threading.Event()
publisher_thread = None
simulator_state = {
    "running": False,
    "sensor_count": DEFAULT_SENSOR_COUNT,
    "interval_ms": DEFAULT_INTERVAL_MS,
    "total_published": 0,
    "last_batch_count": 0,
    "last_batch_duration_ms": 0,
    "started_at": None,
    "client_state": "stopped",
    "last_error": None,
    "live_readings": [],
}


INDEX_HTML = """
<!doctype html>
<html lang="sq">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Air Quality Simulator</title>
  <style>
    :root {
      --bg: #eef5ff;
      --panel: #ffffff;
      --ink: #132238;
      --muted: #64748b;
      --line: #d7e3f5;
      --accent: #2563eb;
      --accent-dark: #1d4ed8;
      --accent-soft: #e8f0ff;
      --amber: #d97706;
      --red: #b8333a;
      --good: #16935f;
      --shadow: 0 14px 34px rgba(37, 99, 235, 0.11);
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      min-height: 100vh;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--ink);
    }

    .shell {
      width: min(1180px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 28px 0;
    }

    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 20px;
      margin-bottom: 18px;
      padding: 24px;
      border-radius: 8px;
      color: #ffffff;
      background: linear-gradient(135deg, #123b86 0%, #2563eb 58%, #38bdf8 100%);
      box-shadow: var(--shadow);
    }

    h1 {
      margin: 0;
      font-size: 30px;
      line-height: 1.1;
      font-weight: 760;
      letter-spacing: 0;
    }

    .subtitle {
      margin: 8px 0 0;
      color: rgba(255, 255, 255, 0.82);
      font-size: 14px;
    }

    .status-pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      height: 34px;
      padding: 0 12px;
      border: 1px solid rgba(255, 255, 255, 0.38);
      background: rgba(255, 255, 255, 0.16);
      border-radius: 8px;
      color: #ffffff;
      font-size: 13px;
      white-space: nowrap;
    }

    .dot {
      width: 9px;
      height: 9px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.75);
    }

    .dot.running {
      background: var(--good);
    }

    .dot.warning {
      background: var(--amber);
    }

    .layout {
      display: grid;
      grid-template-columns: 380px minmax(0, 1fr);
      gap: 18px;
      align-items: start;
    }

    section {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }

    .panel {
      padding: 18px;
    }

    .panel + .panel {
      margin-top: 18px;
    }

    .panel-title {
      margin: 0 0 16px;
      font-size: 16px;
      line-height: 1.2;
      font-weight: 720;
      letter-spacing: 0;
    }

    .field {
      display: grid;
      gap: 7px;
      margin-bottom: 14px;
    }

    label {
      color: var(--muted);
      font-size: 13px;
      font-weight: 640;
    }

    input {
      width: 100%;
      height: 42px;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 0 12px;
      color: var(--ink);
      background: #f8fbff;
      font-size: 15px;
      outline: none;
    }

    input:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.14);
    }

    .button-row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
      margin-top: 8px;
    }

    button {
      height: 42px;
      border: 1px solid transparent;
      border-radius: 8px;
      font-size: 14px;
      font-weight: 720;
      letter-spacing: 0;
      cursor: pointer;
    }

    button.primary {
      background: var(--accent);
      color: #ffffff;
    }

    button.primary:hover {
      background: var(--accent-dark);
    }

    button.secondary {
      background: #ffffff;
      color: #1d4ed8;
      border-color: #b8cdf6;
    }

    button.secondary:hover {
      background: var(--accent-soft);
    }

    button.ghost {
      width: 100%;
      background: #ffffff;
      color: var(--ink);
      border-color: var(--line);
      margin-top: 8px;
    }

    .metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }

    .metric {
      min-height: 92px;
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      box-shadow: 0 8px 22px rgba(37, 99, 235, 0.07);
    }

    .metric-label {
      margin: 0 0 8px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 680;
      text-transform: uppercase;
    }

    .metric-value {
      margin: 0;
      font-size: 25px;
      line-height: 1;
      font-weight: 760;
      color: #1745a1;
    }

    .metric-note {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 12px;
    }

    .live-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      margin-bottom: 12px;
    }

    .live-header .panel-title {
      margin: 0;
    }

    .live-count {
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
    }

    .live-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(176px, 1fr));
      gap: 10px;
    }

    .reading {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      min-height: 46px;
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fbfdff;
      border-left: 4px solid var(--accent);
    }

    .reading-name {
      min-width: 0;
      color: var(--ink);
      font-size: 14px;
      font-weight: 690;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .reading-label {
      display: block;
      margin-top: 2px;
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
    }

    .reading-value {
      color: var(--accent-dark);
      font-size: 16px;
      font-weight: 760;
      white-space: nowrap;
    }

    .empty {
      display: grid;
      place-items: center;
      min-height: 220px;
      border: 1px dashed var(--line);
      border-radius: 8px;
      color: var(--muted);
      font-size: 14px;
      text-align: center;
    }

    .error {
      display: none;
      margin-top: 12px;
      padding: 10px 12px;
      border: 1px solid #e3c6c8;
      border-radius: 8px;
      color: var(--red);
      background: #fff6f6;
      font-size: 13px;
      overflow-wrap: anywhere;
    }

    @media (max-width: 900px) {
      .layout {
        grid-template-columns: 1fr;
      }

      .metrics {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }

    @media (max-width: 560px) {
      .shell {
        width: min(100vw - 22px, 1180px);
        padding: 18px 0;
      }

      header {
        align-items: flex-start;
        flex-direction: column;
      }

      h1 {
        font-size: 24px;
      }

      .metrics {
        grid-template-columns: 1fr;
      }

      .live-header {
        align-items: flex-start;
        flex-direction: column;
      }
    }
  </style>
</head>
<body>
  <main class="shell">
    <header>
      <div>
        <h1>Air Quality Simulator</h1>
        <p class="subtitle">Simulator GUI per pipeline-in IoT</p>
      </div>
      <div class="status-pill">
        <span id="statusDot" class="dot"></span>
        <span id="statusText">Stopped</span>
      </div>
    </header>

    <div class="metrics">
      <div class="metric">
        <p class="metric-label">Sensore</p>
        <p id="metricSensors" class="metric-value">0</p>
        <p class="metric-note">aktive ne konfigurim</p>
      </div>
      <div class="metric">
        <p class="metric-label">Intervali</p>
        <p id="metricInterval" class="metric-value">0 ms</p>
        <p class="metric-note">per batch</p>
      </div>
      <div class="metric">
        <p class="metric-label">Matje/sec</p>
        <p id="metricRate" class="metric-value">0</p>
        <p class="metric-note">target</p>
      </div>
      <div class="metric">
        <p class="metric-label">Publikuar</p>
        <p id="metricTotal" class="metric-value">0</p>
        <p class="metric-note">mesazhe MQTT</p>
      </div>
    </div>

    <div class="layout">
      <aside>
        <section class="panel">
          <h2 class="panel-title">Paneli i Sensoreve</h2>
          <div class="field">
            <label for="sensorCount">Numri i sensoreve</label>
            <input id="sensorCount" type="number" min="1" max="10000" step="1" value="3">
          </div>
          <div class="button-row">
            <button id="startButton" class="primary" type="button">Start</button>
            <button id="stopButton" class="secondary" type="button">Stop</button>
          </div>
          <div id="errorBox" class="error"></div>
        </section>

        <section class="panel">
          <h2 class="panel-title">Konfigurimi</h2>
          <div class="field">
            <label for="configSensorCount">Numri i sensoreve</label>
            <input id="configSensorCount" type="number" min="1" max="10000" step="1" value="3">
          </div>
          <div class="field">
            <label for="intervalMs">Frekuenca e dergimit (ms)</label>
            <input id="intervalMs" type="number" min="10" max="60000" step="10" value="1000">
          </div>
          <button id="applyButton" class="ghost" type="button">Apliko</button>
        </section>
      </aside>

      <section class="panel">
        <div class="live-header">
          <h2 class="panel-title">Live Data</h2>
          <span id="liveCount" class="live-count">0 sensore</span>
        </div>
        <div id="liveGrid" class="live-grid"></div>
        <div id="emptyState" class="empty">Nuk ka matje aktive.</div>
      </section>
    </div>
  </main>

  <script>
    const sensorCount = document.getElementById("sensorCount");
    const configSensorCount = document.getElementById("configSensorCount");
    const intervalMs = document.getElementById("intervalMs");
    const statusDot = document.getElementById("statusDot");
    const statusText = document.getElementById("statusText");
    const errorBox = document.getElementById("errorBox");
    const liveGrid = document.getElementById("liveGrid");
    const emptyState = document.getElementById("emptyState");

    function numberValue(input, fallback) {
      const parsed = Number.parseInt(input.value, 10);
      return Number.isFinite(parsed) ? parsed : fallback;
    }

    function readConfig() {
      return {
        sensor_count: numberValue(configSensorCount, numberValue(sensorCount, 3)),
        interval_ms: numberValue(intervalMs, 1000),
      };
    }

    function formatNumber(value) {
      return new Intl.NumberFormat("en-US").format(Math.round(value));
    }

    function showError(message) {
      if (!message) {
        errorBox.style.display = "none";
        errorBox.textContent = "";
        return;
      }
      errorBox.style.display = "block";
      errorBox.textContent = message;
    }

    async function postJson(url, body) {
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body || {}),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      return response.json();
    }

    async function startSimulator() {
      const config = readConfig();
      sensorCount.value = config.sensor_count;
      await postJson("/api/start", config);
      showError("");
      await refreshStatus();
    }

    async function stopSimulator() {
      await postJson("/api/stop", {});
      showError("");
      await refreshStatus();
    }

    async function applyConfig() {
      const config = readConfig();
      sensorCount.value = config.sensor_count;
      await postJson("/api/config", config);
      showError("");
      await refreshStatus();
    }

    function renderLive(readings, sensorTotal) {
      liveGrid.innerHTML = "";
      document.getElementById("liveCount").textContent =
        `${readings.length} nga ${sensorTotal} sensore`;

      if (!readings.length) {
        emptyState.style.display = "grid";
        return;
      }

      emptyState.style.display = "none";
      const fragment = document.createDocumentFragment();
      readings.forEach((reading) => {
        const row = document.createElement("div");
        row.className = "reading";
        row.innerHTML = `
          <span>
            <span class="reading-name">Sensor ${reading.sensor_number}</span>
            <span class="reading-label">PM2.5</span>
          </span>
          <span class="reading-value">${reading.pm2_5.toFixed(1)}</span>
        `;
        fragment.appendChild(row);
      });
      liveGrid.appendChild(fragment);
    }

    function isEditingConfig() {
      return [sensorCount, configSensorCount, intervalMs].includes(document.activeElement);
    }

    function renderStatus(data) {
      if (!isEditingConfig()) {
        sensorCount.value = data.sensor_count;
        configSensorCount.value = data.sensor_count;
        intervalMs.value = data.interval_ms;
      }

      statusDot.className = data.running ? "dot running" : "dot";
      if (data.running && data.client_state !== "connected") {
        statusDot.className = "dot warning";
      }
      statusText.textContent = data.running ? data.client_state : "Stopped";

      document.getElementById("metricSensors").textContent = formatNumber(data.sensor_count);
      document.getElementById("metricInterval").textContent = `${data.interval_ms} ms`;
      document.getElementById("metricRate").textContent = formatNumber(data.target_rate);
      document.getElementById("metricTotal").textContent = formatNumber(data.total_published);
      renderLive(data.live_readings, data.sensor_count);

      showError(data.last_error || "");
    }

    async function refreshStatus() {
      const response = await fetch("/api/status");
      renderStatus(await response.json());
    }

    document.getElementById("startButton").addEventListener("click", () => {
      startSimulator().catch((error) => showError(error.message));
    });
    document.getElementById("stopButton").addEventListener("click", () => {
      stopSimulator().catch((error) => showError(error.message));
    });
    document.getElementById("applyButton").addEventListener("click", () => {
      applyConfig().catch((error) => showError(error.message));
    });
    sensorCount.addEventListener("input", () => {
      configSensorCount.value = sensorCount.value;
    });

    refreshStatus().catch((error) => showError(error.message));
    setInterval(() => refreshStatus().catch((error) => showError(error.message)), 600);
  </script>
</body>
</html>
"""


def clamp_int(value, default, minimum, maximum):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def normalized_config(payload, current_sensor_count, current_interval_ms):
    sensor_count = clamp_int(
        payload.get("sensor_count"),
        current_sensor_count,
        1,
        10000,
    )
    interval_ms = clamp_int(
        payload.get("interval_ms"),
        current_interval_ms,
        10,
        60000,
    )
    return sensor_count, interval_ms


def sensor_id(sensor_number):
    return f"airgradient_prishtina_{sensor_number:03d}"


def sensor_bias(sensor_number):
    return (((sensor_number * 37) % 21) - 10) * 0.65


def bounded(value, minimum, maximum):
    return max(minimum, min(maximum, value))


def generate_reading(sensor_number, now_epoch):
    day_fraction = (now_epoch % 86400) / 86400
    daily_wave = math.sin((day_fraction * math.tau) - 1.8)
    local_wave = math.sin((now_epoch / 37) + (sensor_number * 0.19))
    pm2_5 = 24 + (daily_wave * 11) + (local_wave * 4.5) + sensor_bias(sensor_number)
    pm2_5 = bounded(pm2_5 + random.gauss(0, 1.8), 1.0, 250.0)
    pm1 = bounded(pm2_5 * random.uniform(0.55, 0.75), 0.2, 180.0)
    temperature = 12 + (math.sin((day_fraction * math.tau) - 0.5) * 7)
    humidity = 58 - (math.sin((day_fraction * math.tau) - 0.5) * 16)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pm1": round(pm1, 3),
        "pm2.5": round(pm2_5, 3),
        "sensor_id": sensor_id(sensor_number),
        "location": LOCATION,
        "latitude": round(BASE_LATITUDE + ((sensor_number % 17) - 8) * 0.00025, 6),
        "longitude": round(BASE_LONGITUDE + ((sensor_number % 19) - 9) * 0.00025, 6),
        "relativehumidity": round(bounded(humidity + random.gauss(0, 2.2), 15, 95), 2),
        "temperature": round(temperature + random.gauss(0, 0.7), 2),
        "um003": round(pm2_5 * random.uniform(120, 180), 2),
        "unit": "ug/m3",
    }


def set_state(**updates):
    with state_lock:
        simulator_state.update(updates)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        set_state(client_state="connected", last_error=None)
    else:
        set_state(client_state="error", last_error=f"MQTT connect returned code {rc}")


def on_disconnect(client, userdata, rc):
    if rc != 0:
        set_state(client_state="disconnected", last_error="MQTT connection lost")


def connect_mqtt(worker_stop_event):
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    while not worker_stop_event.is_set():
        try:
            set_state(client_state="connecting")
            client.connect(BROKER, PORT, 60)
            return client
        except Exception as exc:
            set_state(
                client_state="retrying",
                last_error=f"MQTT unavailable at {BROKER}:{PORT}: {exc}",
            )
            worker_stop_event.wait(5)

    return None


def publish_loop(worker_stop_event):
    global stop_event

    client = connect_mqtt(worker_stop_event)
    if client is None:
        if worker_stop_event is stop_event:
            set_state(running=False, client_state="stopped")
        return

    client.loop_start()

    try:
        while not worker_stop_event.is_set():
            with state_lock:
                sensor_count = simulator_state["sensor_count"]
                interval_ms = simulator_state["interval_ms"]

            batch_started = time.perf_counter()
            now_epoch = time.time()
            live_readings = []
            published = 0

            for sensor_number in range(1, sensor_count + 1):
                if worker_stop_event.is_set():
                    break

                reading = generate_reading(sensor_number, now_epoch)
                payload = json.dumps(reading, separators=(",", ":"))
                client.publish(TOPIC, payload)
                published += 1

                if len(live_readings) < MAX_LIVE_READINGS:
                    live_readings.append(
                        {
                            "sensor_number": sensor_number,
                            "sensor_id": reading["sensor_id"],
                            "pm1": reading["pm1"],
                            "pm2_5": reading["pm2.5"],
                            "timestamp": reading["timestamp"],
                        }
                    )

            batch_duration_ms = round((time.perf_counter() - batch_started) * 1000, 2)
            with state_lock:
                simulator_state["total_published"] += published
                simulator_state["last_batch_count"] = published
                simulator_state["last_batch_duration_ms"] = batch_duration_ms
                simulator_state["live_readings"] = live_readings

            delay_seconds = (interval_ms / 1000) - (batch_duration_ms / 1000)
            if delay_seconds > 0:
                worker_stop_event.wait(delay_seconds)
    finally:
        client.loop_stop()
        client.disconnect()
        if worker_stop_event is stop_event:
            set_state(running=False, client_state="stopped")


def status_snapshot():
    with state_lock:
        snapshot = dict(simulator_state)
        snapshot["live_readings"] = list(simulator_state["live_readings"])

    sensor_count = snapshot["sensor_count"]
    interval_ms = snapshot["interval_ms"]
    snapshot["target_rate"] = sensor_count * (1000 / interval_ms)

    started_at = snapshot.get("started_at")
    snapshot["uptime_seconds"] = round(time.time() - started_at, 1) if started_at else 0
    return snapshot


@app.get("/")
def index():
    return render_template_string(INDEX_HTML)


@app.get("/api/status")
def api_status():
    return jsonify(status_snapshot())


@app.post("/api/config")
def api_config():
    payload = request.get_json(silent=True) or {}
    with state_lock:
        sensor_count, interval_ms = normalized_config(
            payload,
            simulator_state["sensor_count"],
            simulator_state["interval_ms"],
        )
        simulator_state["sensor_count"] = sensor_count
        simulator_state["interval_ms"] = interval_ms
        simulator_state["last_error"] = None

    return jsonify(status_snapshot())


@app.post("/api/start")
def api_start():
    global publisher_thread
    global stop_event

    payload = request.get_json(silent=True) or {}
    already_running = False
    with state_lock:
        sensor_count, interval_ms = normalized_config(
            payload,
            simulator_state["sensor_count"],
            simulator_state["interval_ms"],
        )
        simulator_state["sensor_count"] = sensor_count
        simulator_state["interval_ms"] = interval_ms
        simulator_state["last_error"] = None

        if simulator_state["running"]:
            already_running = True
        else:
            stop_event = threading.Event()
            simulator_state["running"] = True
            simulator_state["client_state"] = "starting"
            simulator_state["total_published"] = 0
            simulator_state["last_batch_count"] = 0
            simulator_state["last_batch_duration_ms"] = 0
            simulator_state["live_readings"] = []
            simulator_state["started_at"] = time.time()

    if already_running:
        return jsonify(status_snapshot())

    publisher_thread = threading.Thread(
        target=publish_loop,
        args=(stop_event,),
        daemon=True,
    )
    publisher_thread.start()
    return jsonify(status_snapshot())


@app.post("/api/stop")
def api_stop():
    stop_event.set()
    set_state(running=False, client_state="stopping")
    return jsonify(status_snapshot())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=UI_PORT, use_reloader=False)
