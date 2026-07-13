require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const mqtt = require('mqtt');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// State management for status dashboard
let mqttStatus = 'disconnected';
let dbStatus = 'disconnected';
let lastError = null;

// ----------------------------------------------------
// MongoDB Connection & Schema
// ----------------------------------------------------
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/apfc';

mongoose.connect(MONGODB_URI)
  .then(() => {
    dbStatus = 'connected';
    console.log('Successfully connected to MongoDB.');
  })
  .catch((err) => {
    dbStatus = 'error';
    lastError = `MongoDB Connection Error: ${err.message}`;
    console.error('MongoDB connection error:', err);
  });

// Schema to store the MQTT messages
const MqttDataSchema = new mongoose.Schema({
  topic: { type: String, required: true, index: true },
  payload: { type: String, required: true },
  data: { type: mongoose.Schema.Types.Mixed }, // Auto-parsed JSON (if applicable)
  timestamp: { type: Date, default: Date.now, index: true }
}, {
  timestamps: true
});

const MqttData = mongoose.model('MqttData', MqttDataSchema);

// ----------------------------------------------------
// MQTT Client setup
// ----------------------------------------------------
const MQTT_BROKER_URL = process.env.MQTT_BROKER_URL || 'mqtt://broker.hivemq.com:1883';
const MQTT_TOPIC = process.env.MQTT_TOPIC || 'apfc/data';

const mqttOptions = {
  reconnectPeriod: 5000, // Reconnect every 5s if disconnected
};

if (process.env.MQTT_USERNAME) mqttOptions.username = process.env.MQTT_USERNAME;
if (process.env.MQTT_PASSWORD) mqttOptions.password = process.env.MQTT_PASSWORD;
if (process.env.MQTT_CLIENT_ID) mqttOptions.clientId = process.env.MQTT_CLIENT_ID;

console.log(`Connecting to MQTT broker at: ${MQTT_BROKER_URL}`);
const client = mqtt.connect(MQTT_BROKER_URL, mqttOptions);

client.on('connect', () => {
  mqttStatus = 'connected';
  console.log(`MQTT client connected. Subscribing to: ${MQTT_TOPIC}`);
  client.subscribe(MQTT_TOPIC, (err) => {
    if (err) {
      console.error(`Subscription failed for topic: ${MQTT_TOPIC}`, err);
      lastError = `MQTT Sub Error: ${err.message}`;
    } else {
      console.log(`Successfully subscribed to topic: ${MQTT_TOPIC}`);
    }
  });
});

client.on('reconnect', () => {
  mqttStatus = 'reconnecting';
  console.log('MQTT client reconnecting...');
});

client.on('close', () => {
  mqttStatus = 'disconnected';
  console.log('MQTT connection closed.');
});

client.on('error', (err) => {
  mqttStatus = 'error';
  lastError = `MQTT Client Error: ${err.message}`;
  console.error('MQTT error:', err);
});

// Keep track of meter IDs that are currently in an error state to throttle duplicate error reports
const metersInError = new Set();

// Message handler
client.on('message', async (topic, message) => {
  const rawPayload = message.toString();

  let parsedData = null;
  try {
    parsedData = JSON.parse(rawPayload);
  } catch (e) {
    // Payload is not JSON, save as raw payload only
  }

  // Throttle logic: check if this is an error message
  if (parsedData && typeof parsedData === 'object' && !Array.isArray(parsedData)) {
    const meterId = parsedData.meterId !== undefined ? parsedData.meterId : '__global__';
    const isError = parsedData.status === 'error';

    if (isError) {
      if (metersInError.has(meterId)) {
        return; // Stop saving this message, no console log
      } else {
        metersInError.add(meterId);
        console.log('device error');
      }
    } else {
      // It's a normal payload (status is not 'error')
      if (metersInError.has(meterId)) {
        metersInError.delete(meterId);
        console.log('device retrieved');
      }
    }
  }

  try {
    const document = new MqttData({
      topic: topic,
      payload: rawPayload,
      data: parsedData
    });
    
    await document.save();
  } catch (dbErr) {
    console.error('Failed to save message to MongoDB:', dbErr);
    lastError = `DB Save Error: ${dbErr.message}`;
  }
});

// ----------------------------------------------------
// Express Routes
// ----------------------------------------------------
app.use(express.json());

// API Endpoint to check server status & statistics
app.get('/api/status', async (req, res) => {
  try {
    const totalMessages = dbStatus === 'connected' ? await MqttData.countDocuments() : 0;
    const recentMessages = dbStatus === 'connected' 
      ? await MqttData.find().sort({ timestamp: -1 }).limit(10) 
      : [];

    res.json({
      status: 'online',
      connections: {
        mongodb: {
          status: dbStatus,
          uri: MONGODB_URI.replace(/\/\/([^:]+):([^@]+)@/, '//***:***@') // Mask credentials in URI
        },
        mqtt: {
          status: mqttStatus,
          broker: MQTT_BROKER_URL.replace(/\/\/([^:]+):([^@]+)@/, '//***:***@'),
          topic: MQTT_TOPIC
        }
      },
      stats: {
        totalMessages
      },
      recentMessages,
      lastError
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Clean JSON response endpoint for querying recent database records
app.get('/api/messages', async (req, res) => {
  if (dbStatus !== 'connected') {
    return res.status(503).json({ error: 'Database is not connected' });
  }
  try {
    const limit = Math.min(parseInt(req.query.limit) || 20, 100);
    const messages = await MqttData.find().sort({ timestamp: -1 }).limit(limit);
    res.json(messages);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// UI Route - Beautiful, modern dashboard (Single Page Application styled with CSS grid & glassmorphism)
app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>APFC MQTT to MongoDB Broker</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg: #090a0f;
      --card-bg: rgba(18, 22, 33, 0.6);
      --card-border: rgba(255, 255, 255, 0.08);
      --accent-primary: #6366f1;
      --accent-primary-glow: rgba(99, 102, 241, 0.15);
      --text: #f3f4f6;
      --text-muted: #9ca3af;
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      font-family: 'Outfit', sans-serif;
      background-color: var(--bg);
      color: var(--text);
      min-height: 100vh;
      overflow-x: hidden;
      padding: 2rem 1.5rem;
      background-image: 
        radial-gradient(circle at 10% 20%, rgba(99, 102, 241, 0.05) 0%, transparent 40%),
        radial-gradient(circle at 90% 80%, rgba(16, 185, 129, 0.03) 0%, transparent 40%);
    }

    .container {
      max-width: 1200px;
      margin: 0 auto;
    }

    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 2.5rem;
      border-bottom: 1px solid var(--card-border);
      padding-bottom: 1.5rem;
    }

    .brand h1 {
      font-size: 2.25rem;
      font-weight: 800;
      background: linear-gradient(135deg, #fff 30%, var(--accent-primary) 100%);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      letter-spacing: -0.5px;
    }

    .brand p {
      color: var(--text-muted);
      font-size: 0.95rem;
      margin-top: 0.25rem;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      padding: 0.35rem 0.85rem;
      border-radius: 9999px;
      font-size: 0.85rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      background: rgba(16, 185, 129, 0.1);
      color: var(--success);
      border: 1px solid rgba(16, 185, 129, 0.2);
    }

    .grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 1.5rem;
      margin-bottom: 2rem;
    }

    @media (min-width: 768px) {
      .grid {
        grid-template-columns: 1fr 1fr;
      }
    }

    @media (min-width: 1024px) {
      .grid {
        grid-template-columns: 2fr 1fr;
      }
    }

    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 16px;
      backdrop-filter: blur(16px);
      padding: 1.5rem;
      box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
      transition: border-color 0.3s ease, box-shadow 0.3s ease;
    }

    .card:hover {
      border-color: rgba(99, 102, 241, 0.25);
      box-shadow: 0 8px 32px 0 var(--accent-primary-glow);
    }

    .card-title {
      font-size: 1.25rem;
      font-weight: 600;
      margin-bottom: 1.25rem;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }

    .status-group {
      display: grid;
      grid-template-columns: 1fr;
      gap: 1rem;
    }

    .status-item {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 1rem;
      background: rgba(255, 255, 255, 0.02);
      border: 1px solid var(--card-border);
      border-radius: 12px;
    }

    .status-info {
      display: flex;
      flex-direction: column;
    }

    .status-name {
      font-size: 0.9rem;
      color: var(--text-muted);
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }

    .status-value {
      font-size: 1.05rem;
      font-weight: 500;
      margin-top: 0.25rem;
      word-break: break-all;
    }

    .state-indicator {
      display: inline-block;
      width: 10px;
      height: 10px;
      border-radius: 50%;
      margin-right: 0.5rem;
    }

    .state-connected {
      background-color: var(--success);
      box-shadow: 0 0 10px var(--success);
    }

    .state-disconnected {
      background-color: var(--text-muted);
      box-shadow: 0 0 10px var(--text-muted);
    }

    .state-error {
      background-color: var(--danger);
      box-shadow: 0 0 10px var(--danger);
    }

    .state-reconnecting {
      background-color: var(--warning);
      box-shadow: 0 0 10px var(--warning);
      animation: pulse 1.5s infinite;
    }

    @keyframes pulse {
      0% { opacity: 0.5; }
      50% { opacity: 1; }
      100% { opacity: 0.5; }
    }

    .stat-number {
      font-size: 3rem;
      font-weight: 800;
      color: var(--accent-primary);
      text-shadow: 0 0 20px var(--accent-primary-glow);
      margin-bottom: 0.25rem;
    }

    .stat-label {
      color: var(--text-muted);
      font-size: 0.95rem;
    }

    .error-banner {
      background: rgba(239, 68, 68, 0.1);
      border: 1px solid rgba(239, 68, 68, 0.2);
      color: #fca5a5;
      padding: 1rem;
      border-radius: 12px;
      margin-bottom: 1.5rem;
      font-size: 0.95rem;
      display: none;
      word-break: break-all;
    }

    .messages-list {
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
      max-height: 500px;
      overflow-y: auto;
      padding-right: 0.25rem;
    }

    /* Custom scrollbar */
    .messages-list::-webkit-scrollbar {
      width: 6px;
    }
    .messages-list::-webkit-scrollbar-track {
      background: transparent;
    }
    .messages-list::-webkit-scrollbar-thumb {
      background: rgba(255, 255, 255, 0.1);
      border-radius: 3px;
    }
    .messages-list::-webkit-scrollbar-thumb:hover {
      background: rgba(255, 255, 255, 0.2);
    }

    .message-item {
      background: rgba(255, 255, 255, 0.02);
      border: 1px solid var(--card-border);
      border-radius: 12px;
      padding: 1rem;
      transition: background 0.2s ease;
    }

    .message-item:hover {
      background: rgba(255, 255, 255, 0.04);
    }

    .message-header {
      display: flex;
      justify-content: space-between;
      font-size: 0.85rem;
      margin-bottom: 0.5rem;
    }

    .message-topic {
      font-weight: 600;
      color: var(--accent-primary);
    }

    .message-time {
      color: var(--text-muted);
    }

    .message-payload {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.9rem;
      background: rgba(0, 0, 0, 0.2);
      padding: 0.75rem;
      border-radius: 8px;
      white-space: pre-wrap;
      word-break: break-all;
      overflow-x: auto;
    }

    .empty-state {
      text-align: center;
      padding: 3rem 1rem;
      color: var(--text-muted);
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="brand">
        <h1>APFC Data Server</h1>
        <p>Real-time MQTT ingestion to MongoDB</p>
      </div>
      <span class="badge">Active Collector</span>
    </header>

    <div id="error-banner" class="error-banner"></div>

    <div class="grid">
      <!-- Left Column: Messages List -->
      <div class="card">
        <div class="card-title">
          <span>Live Ingestion Feed</span>
          <span style="font-size: 0.85rem; color: var(--text-muted); font-weight: normal;">Updates every 3s</span>
        </div>
        <div id="messages-container" class="messages-list">
          <div class="empty-state">Waiting for first message...</div>
        </div>
      </div>

      <!-- Right Column: Status & Stats -->
      <div style="display: flex; flex-direction: column; gap: 1.5rem;">
        <!-- Status Card -->
        <div class="card">
          <div class="card-title">Connection Status</div>
          <div class="status-group">
            <div class="status-item">
              <div class="status-info">
                <span class="status-name">MongoDB Database</span>
                <span class="status-value" id="db-uri">Connecting...</span>
              </div>
              <div style="display: flex; align-items: center;">
                <span id="db-indicator" class="state-indicator state-disconnected"></span>
                <span id="db-status-text" style="font-weight: 600; font-size: 0.9rem;">Offline</span>
              </div>
            </div>
            
            <div class="status-item">
              <div class="status-info">
                <span class="status-name">MQTT Broker</span>
                <span class="status-value" id="mqtt-broker">Connecting...</span>
              </div>
              <div style="display: flex; align-items: center;">
                <span id="mqtt-indicator" class="state-indicator state-disconnected"></span>
                <span id="mqtt-status-text" style="font-weight: 600; font-size: 0.9rem;">Offline</span>
              </div>
            </div>

            <div class="status-item">
              <div class="status-info">
                <span class="status-name">Listening Topic</span>
                <span class="status-value" id="mqtt-topic" style="font-family: 'JetBrains Mono', monospace; font-size: 0.9rem;">-</span>
              </div>
            </div>
          </div>
        </div>

        <!-- Statistics Card -->
        <div class="card">
          <div class="card-title">Data Ingestion Statistics</div>
          <div style="text-align: center; padding: 1rem 0;">
            <div class="stat-number" id="total-messages">0</div>
            <div class="stat-label">Total Packets Persisted</div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <script>
    async function updateStatus() {
      try {
        const response = await fetch('/api/status');
        if (!response.ok) throw new Error('Failed to retrieve system status');
        const data = await response.json();

        // Update Error Banner
        const errorBanner = document.getElementById('error-banner');
        if (data.lastError) {
          errorBanner.innerText = data.lastError;
          errorBanner.style.display = 'block';
        } else {
          errorBanner.style.display = 'none';
        }

        // Update DB Connection UI
        const dbStatus = data.connections.mongodb.status;
        const dbText = document.getElementById('db-status-text');
        const dbInd = document.getElementById('db-indicator');
        dbText.innerText = dbStatus.toUpperCase();
        dbText.style.color = dbStatus === 'connected' ? 'var(--success)' : dbStatus === 'error' ? 'var(--danger)' : 'var(--text-muted)';
        dbInd.className = 'state-indicator state-' + dbStatus;
        document.getElementById('db-uri').innerText = data.connections.mongodb.uri;

        // Update MQTT Connection UI
        const mqttStatus = data.connections.mqtt.status;
        const mqttText = document.getElementById('mqtt-status-text');
        const mqttInd = document.getElementById('mqtt-indicator');
        mqttText.innerText = mqttStatus.toUpperCase();
        mqttText.style.color = mqttStatus === 'connected' ? 'var(--success)' : mqttStatus === 'error' ? 'var(--danger)' : mqttStatus === 'reconnecting' ? 'var(--warning)' : 'var(--text-muted)';
        mqttInd.className = 'state-indicator state-' + mqttStatus;
        document.getElementById('mqtt-broker').innerText = data.connections.mqtt.broker;
        document.getElementById('mqtt-topic').innerText = data.connections.mqtt.topic;

        // Update Statistics
        document.getElementById('total-messages').innerText = data.stats.totalMessages;

        // Update Ingestion Feed
        const container = document.getElementById('messages-container');
        if (data.recentMessages.length === 0) {
          container.innerHTML = '<div class="empty-state">No messages persisted yet. Publish to the MQTT topic to see them here!</div>';
        } else {
          container.innerHTML = data.recentMessages.map(msg => {
            const timeStr = new Date(msg.timestamp).toLocaleString();
            let payloadDisplay = msg.payload;
            
            // Format JSON prettily if it was parsed successfully
            if (msg.data && typeof msg.data === 'object') {
              payloadDisplay = JSON.stringify(msg.data, null, 2);
            }

            return \`
              <div class="message-item">
                <div class="message-header">
                  <span class="message-topic">\${msg.topic}</span>
                  <span class="message-time">\${timeStr}</span>
                </div>
                <pre class="message-payload">\${escapeHtml(payloadDisplay)}</pre>
              </div>
            \`;
          }).join('');
        }
      } catch (err) {
        console.error('Error fetching status:', err);
        const errorBanner = document.getElementById('error-banner');
        errorBanner.innerText = 'Lost connection to APFC Server: ' + err.message;
        errorBanner.style.display = 'block';
      }
    }

    function escapeHtml(text) {
      return text
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
    }

    // Refresh every 3 seconds
    setInterval(updateStatus, 3000);
    // Initial load
    updateStatus();
  </script>
</body>
</html>
  `);
});

// Start Express Server
app.listen(PORT, () => {
  console.log(`Express status server running on port ${PORT}`);
  console.log(`Verify Dashboard at: http://localhost:${PORT}`);
});
