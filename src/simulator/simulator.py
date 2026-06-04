import json
import logging
import math
import os
import random
import threading
import time
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from cassandra.cluster import Cluster
from flask import Flask, jsonify, render_template_string, request


BROKER = os.getenv("MQTT_BROKER", "mqtt")
PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC = os.getenv("MQTT_TOPIC", "air-quality/sensor1")
UI_PORT = int(os.getenv("SIMULATOR_UI_PORT", "5000"))
DEFAULT_SENSOR_COUNT = int(os.getenv("SIMULATOR_SENSOR_COUNT", "3"))
DEFAULT_INTERVAL_MS = int(os.getenv("SIMULATOR_INTERVAL_MS", "1000"))
DEFAULT_SCENARIO = os.getenv("SIMULATOR_SCENARIO", "normal")
MAX_LIVE_READINGS = int(os.getenv("SIMULATOR_MAX_LIVE_READINGS", "60"))
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "air_quality")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "air_quality")
SENSOR_METADATA_TABLE = os.getenv("SENSOR_METADATA_TABLE", "sensor_metadata")
ALARM_EVENTS_TABLE = os.getenv("ALARM_EVENTS_TABLE", "alarm_events")
ANOMALY_EVENTS_TABLE = os.getenv("ANOMALY_EVENTS_TABLE", "anomaly_events")
ANOMALY_PROFILE_TABLE = os.getenv("ANOMALY_PROFILE_TABLE", "sensor_ai_profiles")
TRAINING_SAMPLE_TABLE = os.getenv("TRAINING_SAMPLE_TABLE", "sensor_ai_samples")
MIN_TRAINING_SAMPLES = int(os.getenv("AI_MODEL_MIN_TRAINING_SAMPLES", "30"))
MODEL_WINDOW_SIZE = int(os.getenv("AI_MODEL_WINDOW_SIZE", "200"))

LOCATION = os.getenv("SIMULATOR_LOCATION", "Prishtina, Kosovo")
BASE_LATITUDE = float(os.getenv("SIMULATOR_LATITUDE", "42.670917"))
BASE_LONGITUDE = float(os.getenv("SIMULATOR_LONGITUDE", "21.151694"))


app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

state_lock = threading.Lock()
cassandra_lock = threading.Lock()
cassandra_cluster = None
cassandra_session = None
stop_event = threading.Event()
publisher_thread = None
simulator_state = {
    "running": False,
    "sensor_count": DEFAULT_SENSOR_COUNT,
    "interval_ms": DEFAULT_INTERVAL_MS,
    "scenario": DEFAULT_SCENARIO,
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

    input,
    select {
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

    input:focus,
    select:focus {
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

    .simulator-insights {
      display: grid;
      grid-template-columns: minmax(0, 1.35fr) minmax(280px, 0.65fr);
      gap: 18px;
      margin-bottom: 18px;
    }

    .flow-panel {
      padding: 18px;
      background:
        linear-gradient(180deg, rgba(232, 240, 255, 0.72) 0%, rgba(255, 255, 255, 0.96) 52%),
        #ffffff;
    }

    .flow-steps {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 10px;
    }

    .flow-step {
      min-height: 88px;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
    }

    .flow-step strong {
      display: block;
      color: #1745a1;
      font-size: 15px;
      overflow-wrap: anywhere;
    }

    .flow-step span {
      display: block;
      margin-top: 8px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
    }

    .scenario-panel {
      padding: 18px;
    }

    .scenario-badge {
      display: inline-flex;
      align-items: center;
      min-height: 30px;
      padding: 0 10px;
      border-radius: 8px;
      background: var(--accent-soft);
      color: var(--accent-dark);
      font-size: 13px;
      font-weight: 760;
      text-transform: capitalize;
    }

    .scenario-badge.hot {
      background: #fff0f0;
      color: #b8333a;
    }

    .mini-stat-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-top: 14px;
    }

    .mini-stat {
      min-height: 70px;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fbfdff;
    }

    .mini-stat p {
      margin: 0 0 8px;
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
    }

    .mini-stat strong {
      color: var(--ink);
      font-size: 18px;
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

    .ai-status {
      margin-top: 18px;
    }

    .ai-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }

    .ai-card {
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: linear-gradient(180deg, #ffffff 0%, #f7fbff 100%);
      box-shadow: 0 8px 22px rgba(37, 99, 235, 0.07);
    }

    .ai-label {
      margin: 0 0 8px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 680;
      text-transform: uppercase;
    }

    .ai-value {
      margin: 0;
      font-size: 20px;
      line-height: 1.1;
      font-weight: 760;
      color: #1745a1;
      overflow-wrap: anywhere;
    }

    .ai-subtext {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 12px;
      overflow-wrap: anywhere;
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

    .tabbar {
      display: inline-grid;
      grid-template-columns: 1fr 1fr;
      gap: 6px;
      margin-bottom: 18px;
      padding: 5px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
      box-shadow: 0 8px 22px rgba(37, 99, 235, 0.07);
    }

    .tab-button {
      min-width: 132px;
      height: 36px;
      background: transparent;
      color: var(--muted);
      border-color: transparent;
    }

    .tab-button.active {
      background: var(--accent);
      color: #ffffff;
    }

    .view {
      display: none;
    }

    .view.active {
      display: block;
    }

    .dashboard-layout {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
      align-items: stretch;
    }

    .health-panel {
      min-height: 360px;
      background:
        linear-gradient(180deg, rgba(232, 240, 255, 0.82) 0%, rgba(255, 255, 255, 0.96) 46%),
        #ffffff;
    }

    .health-hero {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr)) auto;
      gap: 18px;
      align-items: end;
      margin: 16px 0 22px;
    }

    .pollutant-hero {
      min-width: 0;
    }

    .health-value {
      margin: 0;
      color: #1745a1;
      font-size: 56px;
      line-height: 0.95;
      font-weight: 780;
    }

    .health-unit {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 13px;
      font-weight: 650;
    }

    .health-badge {
      display: inline-flex;
      align-items: center;
      min-height: 34px;
      padding: 0 12px;
      border-radius: 8px;
      background: var(--accent-soft);
      color: var(--accent-dark);
      font-size: 13px;
      font-weight: 760;
      white-space: nowrap;
    }

    .health-badge.good {
      background: #e7f7ef;
      color: #16784c;
    }

    .health-badge.warn {
      background: #fff4df;
      color: #b85b00;
    }

    .health-badge.bad {
      background: #fff0f0;
      color: #b8333a;
    }

    .gauge-track {
      position: relative;
      height: 18px;
      border-radius: 8px;
      overflow: hidden;
      background: linear-gradient(90deg, #16a34a 0 15%, #f59e0b 15% 35%, #ef4444 35% 70%, #7f1d1d 70% 100%);
      box-shadow: inset 0 0 0 1px rgba(19, 34, 56, 0.1);
    }

    .gauge-fill {
      position: absolute;
      inset: 0 auto 0 0;
      width: 0%;
      background: rgba(255, 255, 255, 0.34);
      border-right: 3px solid #ffffff;
      transition: width 250ms ease;
    }

    .gauge-marker {
      position: absolute;
      top: -7px;
      left: 0%;
      width: 4px;
      height: 32px;
      border-radius: 8px;
      background: #132238;
      transform: translateX(-2px);
      transition: left 250ms ease;
    }

    .threshold-row {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
      margin-top: 10px;
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
    }

    .reading-detail-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-top: 24px;
    }

    .detail-card {
      min-height: 78px;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
    }

    .detail-card p {
      margin: 0 0 8px;
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
    }

    .detail-card strong {
      color: var(--ink);
      font-size: 18px;
      overflow-wrap: anywhere;
    }

    .sensor-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }

    .sensor-table th,
    .sensor-table td {
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      white-space: nowrap;
    }

    .sensor-table th {
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
    }

    .table-scroll {
      max-height: 360px;
      overflow: auto;
    }

    .sensor-panel {
      min-height: 360px;
    }

    .sensor-panel .table-scroll {
      max-height: 285px;
      overflow-y: auto;
      overflow-x: auto;
    }

    .alerts-panel {
      margin-top: 18px;
    }

    .event-message {
      max-width: 420px;
      white-space: normal;
      color: var(--muted);
    }

    .status-chip {
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      padding: 0 8px;
      border-radius: 8px;
      background: var(--accent-soft);
      color: var(--accent-dark);
      font-weight: 720;
    }

    .status-chip.good {
      background: #e7f7ef;
      color: #16784c;
    }

    .status-chip.warn {
      background: #fff4df;
      color: #b85b00;
    }

    .status-chip.bad {
      background: #fff0f0;
      color: #b8333a;
    }

    .grafana-panel {
      margin-top: 18px;
      padding: 0;
      overflow: hidden;
    }

    .grafana-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      padding: 16px 18px;
      border-bottom: 1px solid var(--line);
    }

    .grafana-frame {
      width: 100%;
      height: 620px;
      display: block;
      border: 0;
      background: #ffffff;
    }

    .external-link {
      color: var(--accent-dark);
      font-size: 13px;
      font-weight: 720;
      text-decoration: none;
      white-space: nowrap;
    }

    @media (max-width: 900px) {
      .layout {
        grid-template-columns: 1fr;
      }

      .dashboard-layout {
        grid-template-columns: 1fr;
      }

      .metrics {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }

      .simulator-insights {
        grid-template-columns: 1fr;
      }

      .flow-steps {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }

      .ai-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }

      .reading-detail-grid {
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

      .flow-steps {
        grid-template-columns: 1fr;
      }

      .mini-stat-grid {
        grid-template-columns: 1fr;
      }

      .ai-grid {
        grid-template-columns: 1fr;
      }

      .health-hero {
        grid-template-columns: 1fr;
      }

      .health-value {
        font-size: 42px;
      }

      .reading-detail-grid {
        grid-template-columns: 1fr;
      }

      .live-header {
        align-items: flex-start;
        flex-direction: column;
      }

      .grafana-frame {
        height: 520px;
      }
    }
  </style>
</head>
<body>
  <main class="shell">
    <header>
      <div>
        <h1>Air Quality Simulator</h1>
        <p class="subtitle">Monitorim i cilesise se ajrit ne kohe reale</p>
      </div>
      <div class="status-pill">
        <span id="statusDot" class="dot"></span>
        <span id="statusText">Stopped</span>
      </div>
    </header>

    <div class="tabbar">
      <button id="simulatorTab" class="tab-button active" type="button">Simulator</button>
      <button id="dashboardTab" class="tab-button" type="button">Dashboard</button>
    </div>

    <div id="simulatorView" class="view active">
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

    <div class="simulator-insights">
      <section class="flow-panel">
        <div class="live-header">
          <h2 class="panel-title">Data Flow</h2>
          <span id="flowState" class="live-count">stopped</span>
        </div>
        <div class="flow-steps">
          <div class="flow-step">
            <strong>Sensor</strong>
            <span>PM1, PM2.5, temperature</span>
          </div>
          <div class="flow-step">
            <strong>MQTT</strong>
            <span>live sensor messages</span>
          </div>
          <div class="flow-step">
            <strong>Kafka</strong>
            <span>stream buffer</span>
          </div>
          <div class="flow-step">
            <strong>Spark</strong>
            <span>status and anomaly check</span>
          </div>
          <div class="flow-step">
            <strong>Cassandra</strong>
            <span>clean structured storage</span>
          </div>
        </div>
      </section>

      <section class="scenario-panel">
        <div class="live-header">
          <h2 class="panel-title">Simulation Profile</h2>
          <span id="scenarioBadge" class="scenario-badge">normal</span>
        </div>
        <div class="mini-stat-grid">
          <div class="mini-stat">
            <p>Batch</p>
            <strong id="miniBatch">0</strong>
          </div>
          <div class="mini-stat">
            <p>Runtime</p>
            <strong id="miniRuntime">0s</strong>
          </div>
          <div class="mini-stat">
            <p>Target</p>
            <strong id="miniTarget">0/sec</strong>
          </div>
          <div class="mini-stat">
            <p>Scenario</p>
            <strong id="miniScenario">Normal</strong>
          </div>
        </div>
      </section>
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
          <div class="field">
            <label for="scenario">Skenari</label>
            <select id="scenario">
              <option value="normal">Normal</option>
              <option value="pollution_spike">Pollution spike</option>
            </select>
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
    </div>

    <div id="dashboardView" class="view">
      <div class="metrics">
        <div class="metric">
          <p class="metric-label">Sensore aktive</p>
          <p id="dbActiveSensors" class="metric-value">0</p>
          <p class="metric-note">ne monitorim</p>
        </div>
        <div class="metric">
          <p class="metric-label">PM1 i fundit</p>
          <p id="dbLatestPm1" class="metric-value">0.0</p>
          <p class="metric-note">ug/m3</p>
        </div>
        <div class="metric">
          <p class="metric-label">PM2.5 i fundit</p>
          <p id="dbLatestPm25" class="metric-value">0.0</p>
          <p class="metric-note">ug/m3</p>
        </div>
        <div class="metric">
          <p class="metric-label">Statusi</p>
          <p id="dbStatus" class="metric-value">-</p>
          <p class="metric-note">nga matjet e fundit</p>
        </div>
        <div class="metric">
          <p class="metric-label">Lokacioni</p>
          <p id="dbLocation" class="metric-value">-</p>
          <p class="metric-note">zona e monitoruar</p>
        </div>
        <div class="metric">
          <p class="metric-label">Alarme</p>
          <p id="dbAlertCount" class="metric-value">0</p>
          <p class="metric-note">njoftime aktive</p>
        </div>
        <div class="metric">
          <p class="metric-label">Anomali</p>
          <p id="dbAnomalyCount" class="metric-value">0</p>
          <p class="metric-note">nga monitori automatik</p>
        </div>
      </div>

      <div class="dashboard-layout">
        <section class="panel health-panel">
          <div class="live-header">
            <h2 class="panel-title">PM1 / PM2.5 Overview</h2>
            <span id="chartSensor" class="live-count">airgradient_prishtina_001</span>
          </div>
          <div class="health-hero">
            <div>
              <p id="healthPm1" class="health-value">-</p>
              <p class="health-unit">PM1 ug/m3 nga matja e fundit</p>
            </div>
            <div>
              <p id="healthPm25" class="health-value">-</p>
              <p class="health-unit">PM2.5 ug/m3 nga matja e fundit</p>
            </div>
            <span id="healthBadge" class="health-badge">No data</span>
          </div>
          <div class="gauge-track" aria-label="PM2.5 threshold gauge">
            <div id="pmGaugeFill" class="gauge-fill"></div>
            <div id="pmGaugeMarker" class="gauge-marker"></div>
          </div>
          <div class="threshold-row">
            <span>Good</span>
            <span>Moderate</span>
            <span>Unhealthy</span>
            <span>Very Unhealthy</span>
          </div>
          <div class="reading-detail-grid">
            <div class="detail-card">
              <p>PM1</p>
              <strong id="detailPm1">-</strong>
            </div>
            <div class="detail-card">
              <p>PM2.5</p>
              <strong id="detailPm25">-</strong>
            </div>
            <div class="detail-card">
              <p>Status</p>
              <strong id="detailStatus">-</strong>
            </div>
            <div class="detail-card">
              <p>Monitor</p>
              <strong id="detailAi">-</strong>
            </div>
          </div>
        </section>

        <section class="panel sensor-panel">
          <div class="live-header">
            <h2 class="panel-title">Sensoret</h2>
            <span id="sensorTableCount" class="live-count">0</span>
          </div>
          <div class="table-scroll">
            <table class="sensor-table">
              <thead>
                <tr>
                  <th>Sensor</th>
                  <th>Lokacion</th>
                  <th>Njesia</th>
                </tr>
              </thead>
              <tbody id="sensorTableBody"></tbody>
            </table>
          </div>
        </section>
      </div>

      <section class="panel ai-status">
        <div class="live-header">
          <h2 class="panel-title">Smart Monitor</h2>
          <span id="aiStatusBadge" class="status-chip">Warming up</span>
        </div>
        <div class="ai-grid">
          <div class="ai-card">
            <p class="ai-label">Monitor</p>
            <p id="aiModelName" class="ai-value">Automatic</p>
            <p id="aiModelNote" class="ai-subtext">Learning live sensor patterns</p>
          </div>
          <div class="ai-card">
            <p class="ai-label">Learning</p>
            <p id="aiTrainingState" class="ai-value">0 / 30</p>
            <p id="aiTrainingNote" class="ai-subtext">clean samples collected</p>
          </div>
          <div class="ai-card">
            <p class="ai-label">Anomaly Level</p>
            <p id="aiLatestScore" class="ai-value">-</p>
            <p id="aiLatestReason" class="ai-subtext">No anomaly evaluated yet</p>
          </div>
          <div class="ai-card">
            <p class="ai-label">Last Updated</p>
            <p id="aiLastTrained" class="ai-value">-</p>
            <p id="aiLastScored" class="ai-subtext">Not scored yet</p>
          </div>
        </div>
      </section>

      <section class="panel alerts-panel">
        <div class="live-header">
          <h2 class="panel-title">Alarmet dhe Anomalite</h2>
          <span id="alertTableCount" class="live-count">0 evente</span>
        </div>
        <div class="table-scroll">
          <table class="sensor-table">
            <thead>
              <tr>
                <th>Koha</th>
                <th>Sensor</th>
                <th>Tipi</th>
                <th>Statusi</th>
                <th>PM1</th>
                <th>PM2.5</th>
                <th>Mesazhi</th>
              </tr>
            </thead>
            <tbody id="alertTableBody"></tbody>
          </table>
        </div>
      </section>

      <section class="panel grafana-panel">
        <div class="grafana-header">
          <div>
            <h2 class="panel-title">Analiza e te dhenave</h2>
            <span class="live-count">Historiku dhe krahasimet e fundit</span>
          </div>
          <a class="external-link" href="http://localhost:3000/d/prishtina-air-quality/prishtina-air-quality?orgId=1&from=now-30m&to=now&theme=light" target="_blank" rel="noreferrer">Hap analizen</a>
        </div>
        <iframe
          class="grafana-frame"
          src="http://localhost:3000/d/prishtina-air-quality/prishtina-air-quality?orgId=1&from=now-30m&to=now&theme=light&kiosk"
          title="Air Quality Analytics"
        ></iframe>
      </section>
    </div>
  </main>

  <script>
    const sensorCount = document.getElementById("sensorCount");
    const configSensorCount = document.getElementById("configSensorCount");
    const intervalMs = document.getElementById("intervalMs");
    const scenario = document.getElementById("scenario");
    const statusDot = document.getElementById("statusDot");
    const statusText = document.getElementById("statusText");
    const errorBox = document.getElementById("errorBox");
    const liveGrid = document.getElementById("liveGrid");
    const emptyState = document.getElementById("emptyState");
    const simulatorTab = document.getElementById("simulatorTab");
    const dashboardTab = document.getElementById("dashboardTab");
    const simulatorView = document.getElementById("simulatorView");
    const dashboardView = document.getElementById("dashboardView");
    let activeDashboardSensor = "airgradient_prishtina_001";

    function numberValue(input, fallback) {
      const parsed = Number.parseInt(input.value, 10);
      return Number.isFinite(parsed) ? parsed : fallback;
    }

    function readConfig() {
      return {
        sensor_count: numberValue(configSensorCount, numberValue(sensorCount, 3)),
        interval_ms: numberValue(intervalMs, 1000),
        scenario: scenario.value,
      };
    }

    function formatNumber(value) {
      return new Intl.NumberFormat("en-US").format(Math.round(value));
    }

    function formatDecimal(value) {
      if (!Number.isFinite(Number(value))) {
        return "-";
      }
      return Number(value).toFixed(1);
    }

    function scenarioLabel(value) {
      return value === "pollution_spike" ? "Pollution spike" : "Normal";
    }

    function anomalyLevel(score, isAnomaly) {
      const parsed = Number(score);
      if (!isAnomaly || !Number.isFinite(parsed)) {
        return "Normal";
      }
      if (parsed >= 0.12) {
        return "High";
      }
      if (parsed >= 0.05) {
        return "Elevated";
      }
      return "Low";
    }

    function anomalyMessage(ai) {
      if (ai && ai.latest && ai.latest.is_anomaly) {
        return "Unusual pattern detected in the latest reading";
      }
      if (ai && ai.trained) {
        return "Latest reading follows the expected pattern";
      }
      return "Learning normal sensor behavior";
    }

    function formatDateTime(value) {
      if (!value) {
        return "-";
      }
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) {
        return "-";
      }
      return date.toLocaleString();
    }

    function escapeHtml(value) {
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }

    function setView(viewName) {
      const dashboardActive = viewName === "dashboard";
      simulatorTab.classList.toggle("active", !dashboardActive);
      dashboardTab.classList.toggle("active", dashboardActive);
      simulatorView.classList.toggle("active", !dashboardActive);
      dashboardView.classList.toggle("active", dashboardActive);
      if (dashboardActive) {
        refreshDashboard().catch((error) => showError(error.message));
      }
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
            <span class="reading-label">PM1 ${reading.pm1.toFixed(1)} / PM2.5</span>
          </span>
          <span class="reading-value">${reading.pm2_5.toFixed(1)}</span>
        `;
        fragment.appendChild(row);
      });
      liveGrid.appendChild(fragment);
    }

    function isEditingConfig() {
      return [sensorCount, configSensorCount, intervalMs, scenario].includes(document.activeElement);
    }

    function renderStatus(data) {
      if (!isEditingConfig()) {
        sensorCount.value = data.sensor_count;
        configSensorCount.value = data.sensor_count;
        intervalMs.value = data.interval_ms;
        scenario.value = data.scenario || "normal";
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
      document.getElementById("flowState").textContent = data.running ? "streaming" : "stopped";
      document.getElementById("miniBatch").textContent = formatNumber(data.last_batch_count || 0);
      document.getElementById("miniRuntime").textContent = `${formatNumber(data.uptime_seconds || 0)}s`;
      document.getElementById("miniTarget").textContent = `${formatNumber(data.target_rate || 0)}/sec`;
      document.getElementById("miniScenario").textContent = scenarioLabel(data.scenario);
      const scenarioBadge = document.getElementById("scenarioBadge");
      scenarioBadge.className = "scenario-badge";
      if (data.scenario === "pollution_spike") {
        scenarioBadge.classList.add("hot");
      }
      scenarioBadge.textContent = scenarioLabel(data.scenario);
      renderLive(data.live_readings, data.sensor_count);

      showError(data.last_error || "");
    }

    function renderSensors(sensors) {
      const body = document.getElementById("sensorTableBody");
      body.innerHTML = "";
      document.getElementById("sensorTableCount").textContent = `${sensors.length}`;
      sensors.slice(0, 80).forEach((sensor) => {
        const row = document.createElement("tr");
        row.innerHTML = `
          <td>${escapeHtml(sensor.sensor_id)}</td>
          <td>${escapeHtml(sensor.location || "-")}</td>
          <td>${escapeHtml(sensor.unit || "ug/m3")}</td>
        `;
        row.addEventListener("click", () => {
          activeDashboardSensor = sensor.sensor_id;
          refreshDashboard().catch((error) => showError(error.message));
        });
        body.appendChild(row);
      });
    }

    function statusChipClass(status) {
      if (status === "Good") {
        return "good";
      }
      if (status === "Moderate") {
        return "warn";
      }
      if (status === "Unhealthy" || status === "Very Unhealthy") {
        return "bad";
      }
      return "";
    }

    function renderHealthGauge(latest, ai) {
      const pm25 = latest ? Number(latest.pm2_5) : NaN;
      const percent = Number.isFinite(pm25) ? Math.max(0, Math.min(100, (pm25 / 100) * 100)) : 0;
      const badge = document.getElementById("healthBadge");
      document.getElementById("healthPm1").textContent = latest ? formatDecimal(latest.pm1) : "-";
      document.getElementById("healthPm25").textContent = Number.isFinite(pm25) ? formatDecimal(pm25) : "-";
      document.getElementById("detailPm1").textContent = latest ? formatDecimal(latest.pm1) : "-";
      document.getElementById("detailPm25").textContent = Number.isFinite(pm25) ? formatDecimal(pm25) : "-";
      document.getElementById("detailStatus").textContent = latest ? latest.status : "-";
      document.getElementById("detailAi").textContent =
        ai && ai.latest && ai.latest.is_anomaly ? "Anomaly" : (ai && ai.trained ? "Normal" : "Learning");
      document.getElementById("pmGaugeFill").style.width = `${percent}%`;
      document.getElementById("pmGaugeMarker").style.left = `${percent}%`;
      badge.className = "health-badge";
      if (latest && statusChipClass(latest.status)) {
        badge.classList.add(statusChipClass(latest.status));
      }
      badge.textContent = latest ? latest.status : "No data";
    }

    function renderAlerts(alerts, anomalies) {
      const body = document.getElementById("alertTableBody");
      const events = [
        ...(alerts || []).map((event) => ({
          time: event.event_time,
          sensor_id: event.sensor_id,
          type: event.event_type || event.notification_channel || "alert",
          status: event.status || "-",
          pm1: event.pm1,
          pm2_5: event.pm2_5,
          message: event.message || "-",
        })),
        ...(anomalies || []).map((event) => ({
          time: event.event_time,
          sensor_id: event.sensor_id,
          type: event.is_anomaly ? "anomaly" : "normal",
          status: event.status || "-",
          pm1: event.pm1,
          pm2_5: event.pm2_5,
          message: event.is_anomaly ? "Unusual reading pattern detected" : "Reading follows expected pattern",
        })),
      ].sort((left, right) => new Date(right.time || 0) - new Date(left.time || 0));

      body.innerHTML = "";
      document.getElementById("alertTableCount").textContent = `${events.length} evente`;

      if (!events.length) {
        const row = document.createElement("tr");
        row.innerHTML = `<td colspan="7">Nuk ka ende alarme ose anomali.</td>`;
        body.appendChild(row);
        return;
      }

      events.slice(0, 40).forEach((event) => {
        const row = document.createElement("tr");
        row.innerHTML = `
          <td>${escapeHtml(formatDateTime(event.time))}</td>
          <td>${escapeHtml(event.sensor_id)}</td>
          <td><span class="status-chip">${escapeHtml(event.type)}</span></td>
          <td>${escapeHtml(event.status)}</td>
          <td>${escapeHtml(formatDecimal(event.pm1))}</td>
          <td>${escapeHtml(formatDecimal(event.pm2_5))}</td>
          <td class="event-message">${escapeHtml(event.message)}</td>
        `;
        body.appendChild(row);
      });
    }

    function renderDashboard(data) {
      const latest = data.latest;
      const ai = data.ai_status || {};
      showError(data.error || "");
      document.getElementById("dbActiveSensors").textContent = formatNumber(data.sensors.length);
      document.getElementById("dbLatestPm1").textContent = latest ? formatDecimal(latest.pm1) : "-";
      document.getElementById("dbLatestPm25").textContent = latest ? formatDecimal(latest.pm2_5) : "-";
      document.getElementById("dbStatus").textContent = latest ? latest.status : "-";
      document.getElementById("dbLocation").textContent = latest ? latest.location : "-";
      document.getElementById("dbAlertCount").textContent = formatNumber((data.alerts || []).length);
      document.getElementById("dbAnomalyCount").textContent = formatNumber((data.anomalies || []).length);
      document.getElementById("chartSensor").textContent = data.sensor_id;
      renderSensors(data.sensors);
      renderHealthGauge(latest, ai);
      renderAlerts(data.alerts || [], data.anomalies || []);

      const aiBadge = document.getElementById("aiStatusBadge");
      aiBadge.className = "status-chip";
      if (ai.status === "Anomaly detected") {
        aiBadge.classList.add("bad");
      } else if (ai.trained) {
        aiBadge.classList.add("good");
      } else {
        aiBadge.classList.add("warn");
      }
      aiBadge.textContent = ai.status || "Warming up";
      document.getElementById("aiModelName").textContent = "Automatic";
      document.getElementById("aiModelNote").textContent = ai.trained ? "Monitoring live sensor patterns" : "Learning normal sensor behavior";
      document.getElementById("aiTrainingState").textContent =
        ai.trained ? "Ready" : `${ai.trained_on_samples || 0} / ${ai.min_training_samples || 30}`;
      document.getElementById("aiTrainingNote").textContent =
        `${ai.clean_samples || 0} normal readings learned`;
      document.getElementById("aiLatestScore").textContent =
        ai.latest ? anomalyLevel(ai.latest.anomaly_score, ai.latest.is_anomaly) : "-";
      document.getElementById("aiLatestReason").textContent = anomalyMessage(ai);
      document.getElementById("aiLastTrained").textContent = formatDateTime(ai.last_trained_at);
      document.getElementById("aiLastScored").textContent =
        `Last checked: ${formatDateTime(ai.last_scored_at)}`;
    }

    async function refreshStatus() {
      const response = await fetch("/api/status");
      renderStatus(await response.json());
    }

    async function refreshDashboard() {
      const response = await fetch(`/api/dashboard?sensor_id=${encodeURIComponent(activeDashboardSensor)}`);
      if (!response.ok) {
        throw new Error(await response.text());
      }
      renderDashboard(await response.json());
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
    simulatorTab.addEventListener("click", () => setView("simulator"));
    dashboardTab.addEventListener("click", () => setView("dashboard"));
    window.addEventListener("resize", () => {
      if (dashboardView.classList.contains("active")) {
        refreshDashboard().catch((error) => showError(error.message));
      }
    });

    refreshStatus().catch((error) => showError(error.message));
    setInterval(() => refreshStatus().catch((error) => showError(error.message)), 600);
    setInterval(() => {
      if (dashboardView.classList.contains("active")) {
        refreshDashboard().catch((error) => showError(error.message));
      }
    }, 2500);
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


def normalized_config(payload, current_sensor_count, current_interval_ms, current_scenario):
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
    scenario = payload.get("scenario") or current_scenario or "normal"
    if scenario not in {"normal", "pollution_spike"}:
        scenario = "normal"
    return sensor_count, interval_ms, scenario


def sensor_id(sensor_number):
    return f"airgradient_prishtina_{sensor_number:03d}"


def sensor_bias(sensor_number):
    return (((sensor_number * 37) % 21) - 10) * 0.65


def bounded(value, minimum, maximum):
    return max(minimum, min(maximum, value))


def generate_reading(sensor_number, now_epoch, scenario="normal"):
    day_fraction = (now_epoch % 86400) / 86400
    daily_wave = math.sin((day_fraction * math.tau) - 1.8)
    local_wave = math.sin((now_epoch / 37) + (sensor_number * 0.19))
    pm2_5 = 24 + (daily_wave * 11) + (local_wave * 4.5) + sensor_bias(sensor_number)
    pm2_5 = pm2_5 + random.gauss(0, 1.8)
    if scenario == "pollution_spike":
        spike_wave = math.sin((now_epoch / 9) + (sensor_number * 0.41))
        pm2_5 += 48 + (spike_wave * 12) + random.gauss(0, 3.5)
    pm2_5 = bounded(pm2_5, 1.0, 250.0)
    pm1 = bounded(pm2_5 * random.uniform(0.55, 0.75), 0.2, 180.0)
    temperature = 12 + (math.sin((day_fraction * math.tau) - 0.5) * 7)
    humidity = 58 - (math.sin((day_fraction * math.tau) - 0.5) * 16)

    sensor_identifier = sensor_id(sensor_number)

    return {
        "message_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sensor": {
            "id": sensor_identifier,
            "type": "AirGradient PM Simulator",
            "firmware": "sim-1.0",
            "location": LOCATION,
            "latitude": round(BASE_LATITUDE + ((sensor_number % 17) - 8) * 0.00025, 6),
            "longitude": round(BASE_LONGITUDE + ((sensor_number % 19) - 9) * 0.00025, 6),
            "unit": "ug/m3",
        },
        "measurements": {
            "pm1": round(pm1, 3),
            "pm2.5": round(pm2_5, 3),
            "relative_humidity": round(
                bounded(humidity + random.gauss(0, 2.2), 15, 95),
                2,
            ),
            "temperature": round(temperature + random.gauss(0, 0.7), 2),
            "um003": round(pm2_5 * random.uniform(120, 180), 2),
        },
        "health": {
            "battery": round(random.uniform(82, 100), 1),
            "signal": random.randint(-72, -38),
            "status": "online",
        },
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
                scenario = simulator_state["scenario"]

            batch_started = time.perf_counter()
            now_epoch = time.time()
            live_readings = []
            published = 0

            for sensor_number in range(1, sensor_count + 1):
                if worker_stop_event.is_set():
                    break

                reading = generate_reading(sensor_number, now_epoch, scenario)
                payload = json.dumps(reading, separators=(",", ":"))
                client.publish(TOPIC, payload)
                published += 1

                if len(live_readings) < MAX_LIVE_READINGS:
                    live_readings.append(
                        {
                            "sensor_number": sensor_number,
                            "sensor_id": reading["sensor"]["id"],
                            "pm1": reading["measurements"]["pm1"],
                            "pm2_5": reading["measurements"]["pm2.5"],
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


def row_timestamp(value):
    return value.isoformat() if value is not None else None


def get_cassandra_session():
    global cassandra_cluster
    global cassandra_session

    with cassandra_lock:
        if cassandra_session is None:
            cassandra_cluster = Cluster([CASSANDRA_HOST])
            cassandra_session = cassandra_cluster.connect()
        return cassandra_session


def execute_cassandra(query, params=None):
    global cassandra_cluster
    global cassandra_session

    try:
        return get_cassandra_session().execute(query, params or [])
    except Exception:
        with cassandra_lock:
            if cassandra_cluster is not None:
                cassandra_cluster.shutdown()
            cassandra_cluster = None
            cassandra_session = None
        raise


def get_sensor_metadata():
    rows = execute_cassandra(
        f"""
        SELECT sensor_id, location, unit, updated_at
        FROM {CASSANDRA_KEYSPACE}.{SENSOR_METADATA_TABLE}
        LIMIT 200
        """
    )
    sensors = [
        {
            "sensor_id": row.sensor_id,
            "location": row.location,
            "unit": row.unit,
            "updated_at": row_timestamp(row.updated_at),
        }
        for row in rows
    ]
    return sorted(sensors, key=lambda item: item["sensor_id"])


def get_sensor_timeseries(sensor_id, limit=80):
    safe_limit = max(1, min(int(limit), 200))
    rows = execute_cassandra(
        f"""
        SELECT sensor_id, timestamp, pm1, pm2_5, status, location, anomaly_score, is_anomaly, anomaly_reason
        FROM {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}
        WHERE sensor_id = %s
        LIMIT {safe_limit}
        """,
        (sensor_id,),
    )
    readings = [
        {
            "sensor_id": row.sensor_id,
            "timestamp": row_timestamp(row.timestamp),
            "pm1": row.pm1,
            "pm2_5": row.pm2_5,
            "status": row.status,
            "location": row.location,
            "anomaly_score": row.anomaly_score,
            "is_anomaly": row.is_anomaly,
            "anomaly_reason": row.anomaly_reason,
        }
        for row in rows
    ]
    return list(reversed(readings))


def get_alarm_events(limit=40):
    safe_limit = max(1, min(int(limit), 100))
    rows = execute_cassandra(
        f"""
        SELECT sensor_id, event_time, notification_channel, event_type, status, pm2_5, location, message
        FROM {CASSANDRA_KEYSPACE}.{ALARM_EVENTS_TABLE}
        LIMIT {safe_limit}
        """
    )
    events = [
        {
            "sensor_id": row.sensor_id,
            "event_time": row_timestamp(row.event_time),
            "notification_channel": row.notification_channel,
            "event_type": row.event_type,
            "status": row.status,
            "pm2_5": row.pm2_5,
            "location": row.location,
            "message": row.message,
        }
        for row in rows
    ]
    return sorted(events, key=lambda item: item["event_time"] or "", reverse=True)


def get_anomaly_events(limit=40):
    safe_limit = max(1, min(int(limit), 100))
    rows = execute_cassandra(
        f"""
        SELECT sensor_id, event_time, anomaly_score, is_anomaly, reason, pm1, pm2_5, relative_humidity, temperature, status, location
        FROM {CASSANDRA_KEYSPACE}.{ANOMALY_EVENTS_TABLE}
        LIMIT {safe_limit}
        """
    )
    events = [
        {
            "sensor_id": row.sensor_id,
            "event_time": row_timestamp(row.event_time),
            "anomaly_score": row.anomaly_score,
            "is_anomaly": row.is_anomaly,
            "reason": row.reason,
            "pm1": row.pm1,
            "pm2_5": row.pm2_5,
            "relative_humidity": row.relative_humidity,
            "temperature": row.temperature,
            "status": row.status,
            "location": row.location,
        }
        for row in rows
    ]
    return sorted(events, key=lambda item: item["event_time"] or "", reverse=True)


def get_ai_status(sensor_id):
    profile_rows = execute_cassandra(
        f"""
        SELECT sensor_id, sample_count, trained_on_samples, last_trained_at, last_scored_at, updated_at
        FROM {CASSANDRA_KEYSPACE}.{ANOMALY_PROFILE_TABLE}
        WHERE sensor_id = %s
        """,
        (sensor_id,),
    )
    profile = profile_rows.one()

    latest_row = execute_cassandra(
        f"""
        SELECT sensor_id, timestamp, pm1, pm2_5, status, location, anomaly_score, is_anomaly, anomaly_reason
        FROM {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}
        WHERE sensor_id = %s
        LIMIT 1
        """,
        (sensor_id,),
    ).one()

    latest_anomaly = execute_cassandra(
        f"""
        SELECT sensor_id, event_time, anomaly_score, is_anomaly, reason, pm1, pm2_5, relative_humidity, temperature, status, location
        FROM {CASSANDRA_KEYSPACE}.{ANOMALY_EVENTS_TABLE}
        WHERE sensor_id = %s
        LIMIT 1
        """,
        (sensor_id,),
    ).one()

    training_rows = execute_cassandra(
        f"""
        SELECT sensor_id, timestamp, pm1, pm2_5, relative_humidity, temperature
        FROM {CASSANDRA_KEYSPACE}.{TRAINING_SAMPLE_TABLE}
        WHERE sensor_id = %s
        LIMIT {MODEL_WINDOW_SIZE}
        """,
        (sensor_id,),
    )

    clean_count = len(list(training_rows))
    trained_on_samples = int(profile.trained_on_samples) if profile and profile.trained_on_samples is not None else 0
    sample_count = int(profile.sample_count) if profile and profile.sample_count is not None else clean_count
    trained = trained_on_samples >= MIN_TRAINING_SAMPLES

    if profile and profile.last_trained_at:
        last_trained_at = row_timestamp(profile.last_trained_at)
    else:
        last_trained_at = None

    if profile and profile.last_scored_at:
        last_scored_at = row_timestamp(profile.last_scored_at)
    else:
        last_scored_at = None

    status = "Warming up"
    if trained:
        status = "Trained"
    if latest_anomaly and latest_anomaly.is_anomaly:
        status = "Anomaly detected"

    latest = None
    if latest_row is not None:
        latest = {
            "timestamp": row_timestamp(latest_row.timestamp),
            "pm1": latest_row.pm1,
            "pm2_5": latest_row.pm2_5,
            "status": latest_row.status,
            "location": latest_row.location,
            "anomaly_score": latest_row.anomaly_score,
            "is_anomaly": latest_row.is_anomaly,
            "anomaly_reason": latest_row.anomaly_reason,
        }

    return {
        "sensor_id": sensor_id,
        "model_name": "Smart monitor",
        "status": status,
        "trained": trained,
        "clean_samples": clean_count,
        "sample_count": sample_count,
        "trained_on_samples": trained_on_samples,
        "min_training_samples": MIN_TRAINING_SAMPLES,
        "last_trained_at": last_trained_at,
        "last_scored_at": last_scored_at,
        "latest": latest,
        "latest_anomaly": None
        if latest_anomaly is None
        else {
            "event_time": row_timestamp(latest_anomaly.event_time),
            "anomaly_score": latest_anomaly.anomaly_score,
            "is_anomaly": latest_anomaly.is_anomaly,
            "reason": latest_anomaly.reason,
            "status": latest_anomaly.status,
            "location": latest_anomaly.location,
        },
    }


@app.get("/")
def index():
    return render_template_string(INDEX_HTML)


@app.get("/api/status")
def api_status():
    return jsonify(status_snapshot())


@app.get("/api/dashboard")
def api_dashboard():
    requested_sensor_id = request.args.get("sensor_id") or "airgradient_prishtina_001"
    try:
        sensors = get_sensor_metadata()
        sensor_id = requested_sensor_id
        if sensors and not any(sensor["sensor_id"] == sensor_id for sensor in sensors):
            sensor_id = sensors[0]["sensor_id"]

        timeseries = get_sensor_timeseries(sensor_id)
        latest = timeseries[-1] if timeseries else None
        ai_status = get_ai_status(sensor_id)
        alerts = get_alarm_events()
        anomalies = get_anomaly_events()
        return jsonify(
            {
                "sensor_id": sensor_id,
                "sensors": sensors,
                "timeseries": timeseries,
                "latest": latest,
                "ai_status": ai_status,
                "alerts": alerts,
                "anomalies": anomalies,
                "error": None,
            }
        )
    except Exception as exc:
        return jsonify(
            {
                "sensor_id": requested_sensor_id,
                "sensors": [],
                "timeseries": [],
                "latest": None,
                "ai_status": None,
                "alerts": [],
                "anomalies": [],
                "error": f"Cassandra dashboard data unavailable: {exc}",
            }
        )


@app.get("/api/ai-status")
def api_ai_status():
    requested_sensor_id = request.args.get("sensor_id") or "airgradient_prishtina_001"
    try:
        sensors = get_sensor_metadata()
        sensor_id = requested_sensor_id
        if sensors and not any(sensor["sensor_id"] == sensor_id for sensor in sensors):
            sensor_id = sensors[0]["sensor_id"]
        return jsonify(get_ai_status(sensor_id))
    except Exception as exc:
        return jsonify(
            {
                "sensor_id": requested_sensor_id,
                "model_name": "Smart monitor",
                "status": "Unavailable",
                "trained": False,
                "clean_samples": 0,
                "sample_count": 0,
                "trained_on_samples": 0,
                "min_training_samples": MIN_TRAINING_SAMPLES,
                "last_trained_at": None,
                "last_scored_at": None,
                "latest": None,
                "latest_anomaly": None,
                "error": f"Smart monitor unavailable: {exc}",
            }
        )


@app.post("/api/config")
def api_config():
    payload = request.get_json(silent=True) or {}
    with state_lock:
        sensor_count, interval_ms, scenario = normalized_config(
            payload,
            simulator_state["sensor_count"],
            simulator_state["interval_ms"],
            simulator_state["scenario"],
        )
        simulator_state["sensor_count"] = sensor_count
        simulator_state["interval_ms"] = interval_ms
        simulator_state["scenario"] = scenario
        simulator_state["last_error"] = None

    return jsonify(status_snapshot())


@app.post("/api/start")
def api_start():
    global publisher_thread
    global stop_event

    payload = request.get_json(silent=True) or {}
    already_running = False
    with state_lock:
        sensor_count, interval_ms, scenario = normalized_config(
            payload,
            simulator_state["sensor_count"],
            simulator_state["interval_ms"],
            simulator_state["scenario"],
        )
        simulator_state["sensor_count"] = sensor_count
        simulator_state["interval_ms"] = interval_ms
        simulator_state["scenario"] = scenario
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
