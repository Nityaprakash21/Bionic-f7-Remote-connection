const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const cors = require("cors");

const PORT = process.env.PORT || 8080;
const app = express();

app.use(cors());
app.use(express.json());

// In-memory registry
const devices = new Map();   // deviceId -> { ws, secret, connectedAt, lastSeen }
const operators = new Map(); // socketId -> { ws, targetDeviceId }

let socketCounter = 0;

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// HTTP Endpoints
app.get("/api/health", (req, res) => {
    res.json({ status: "ok", timestamp: new Date().toISOString() });
});

app.get("/api/devices", (req, res) => {
    const list = [];
    devices.forEach((val, id) => {
        list.push({
            deviceId: id,
            connectedAt: val.connectedAt,
            lastSeen: val.lastSeen
        });
    });
    res.json({ count: list.length, devices: list });
});

// WebSocket Signaling Handler
wss.on("connection", (ws) => {
    const socketId = "sock_" + (++socketCounter);
    console.log(`[${socketId}] New WebSocket connection established.`);

    ws.on("message", (rawMessage) => {
        try {
            const message = JSON.parse(rawMessage.toString());
            handleMessage(ws, socketId, message);
        } catch (err) {
            console.error(`[${socketId}] Failed to parse message:`, err.message);
        }
    });

    ws.on("close", () => {
        console.log(`[${socketId}] WebSocket closed.`);
        // Clean up if this was a registered device
        devices.forEach((val, id) => {
            if (val.ws === ws) {
                devices.delete(id);
                console.log(`[Device Disconnected] ${id}`);
            }
        });
        // Clean up if operator
        operators.delete(socketId);
    });

    ws.on("error", (err) => {
        console.error(`[${socketId}] WebSocket error:`, err.message);
    });
});

function handleMessage(ws, socketId, msg) {
    const { type, role, deviceId, secret, target, sdp, candidate, payload } = msg;

    if (type === "register") {
        if (role === "device" && deviceId) {
            devices.set(deviceId, {
                ws,
                secret: secret || "",
                connectedAt: new Date().toISOString(),
                lastSeen: new Date().toISOString()
            });
            console.log(`[Device Registered] DeviceID: ${deviceId}`);
            ws.send(JSON.stringify({ type: "registered", status: "success", deviceId }));
        } else if (role === "operator") {
            operators.set(socketId, { ws, targetDeviceId: target });
            console.log(`[Operator Connected] ${socketId} targeting ${target}`);
            ws.send(JSON.stringify({ type: "operator_ready", socketId }));
        }
        return;
    }

    // Relay SDP Offer (Operator -> F7 Device)
    if (type === "offer") {
        const deviceObj = devices.get(target);
        if (deviceObj && deviceObj.ws.readyState === WebSocket.OPEN) {
            deviceObj.ws.send(JSON.stringify({
                type: "offer",
                sender: socketId,
                sdp
            }));
            console.log(`[Signaling] Relayed OFFER from ${socketId} -> Device ${target}`);
        } else {
            ws.send(JSON.stringify({ type: "error", message: `Device ${target} not online.` }));
        }
        return;
    }

    // Relay SDP Answer (F7 Device -> Operator)
    if (type === "answer") {
        const opObj = operators.get(target);
        if (opObj && opObj.ws.readyState === WebSocket.OPEN) {
            opObj.ws.send(JSON.stringify({
                type: "answer",
                sender: socketId,
                sdp
            }));
            console.log(`[Signaling] Relayed ANSWER -> Operator ${target}`);
        }
        return;
    }

    // Relay ICE Candidate (Bi-directional)
    if (type === "candidate") {
        const deviceObj = devices.get(target);
        const opObj = operators.get(target);

        if (deviceObj && deviceObj.ws.readyState === WebSocket.OPEN) {
            deviceObj.ws.send(JSON.stringify({ type: "candidate", candidate }));
        } else if (opObj && opObj.ws.readyState === WebSocket.OPEN) {
            opObj.ws.send(JSON.stringify({ type: "candidate", candidate }));
        }
        return;
    }

    // Relay Remote Touch / Keyboard Commands (Operator -> F7 Device)
    if (type === "command" && target) {
        const deviceObj = devices.get(target);
        if (deviceObj && deviceObj.ws.readyState === WebSocket.OPEN) {
            deviceObj.ws.send(JSON.stringify({ type: "command", payload }));
        }
        return;
    }
}

server.listen(PORT, "0.0.0.0", () => {
    console.log(`=======================================================`);
    console.log(` F7 Remote VPS Signaling & Auth Server Running on 0.0.0.0:${PORT}`);
    console.log(`=======================================================`);
});
