import asyncio
import json
import traceback
from typing import Dict, Any

import websockets


class IntifaceClient:
    def __init__(self, host="127.0.0.1", port=12345, client_name="StreamConnector"):
        self.host = host
        self.port = port
        self.client_name = client_name

        # IMPORTANT: Intiface Central uses root path, NOT /buttplug
        self.uri = f"ws://{self.host}:{self.port}"

        self.devices: Dict[int, Dict[str, Any]] = {}
        self._id_counter = 1

    # ─────────────────────────────────────────────────────────────
    # Logging
    # ─────────────────────────────────────────────────────────────

    def log(self, msg, level="INFO", data=None):
        prefix = f"[INTIFACE][{level}]"
        print(f"{prefix} {msg}")
        if data is not None:
            try:
                print(f"{prefix} DATA: {json.dumps(data, indent=2)}")
            except Exception:
                print(f"{prefix} DATA: {data}")

    def _next_id(self) -> int:
        val = self._id_counter
        self._id_counter += 1
        return val

    # ─────────────────────────────────────────────────────────────
    # Core Connection
    # ─────────────────────────────────────────────────────────────

    async def connect(self):
        self.log(f"Connecting to {self.uri}", "INFO")

        async with websockets.connect(
            self.uri,
            subprotocols=["buttplug-json"],
            max_size=None
        ) as ws:
            self.log("Connected to Intiface Central", "INFO")

            await self._send_handshake(ws)

            async for message in ws:
                await self._handle_message(ws, message)

    # ─────────────────────────────────────────────────────────────
    # Send Helpers (ARRAY FRAMING + V3 HANDSHAKE)
    # ─────────────────────────────────────────────────────────────

    async def _send(self, ws, msg: Dict[str, Any]):
        payload = json.dumps([msg])  # ALWAYS ARRAY
        await ws.send(payload)

    async def _send_handshake(self, ws):
        handshake = {
            "RequestServerInfo": {
                "Id": self._next_id(),
                "ClientName": self.client_name,
                "MessageVersion": 3  # 🔴 THIS IS WHAT INTIFACE EXPECTS
            }
        }

        await self._send(ws, handshake)
        self.log("Handshake sent", "DEBUG", handshake)

    async def start_scanning(self, ws):
        cmd = {
            "StartScanning": {
                "Id": self._next_id()
            }
        }
        await self._send(ws, cmd)
        self.log("StartScanning sent", "DEBUG", cmd)

    async def request_device_list(self, ws):
        cmd = {
            "RequestDeviceList": {
                "Id": self._next_id()
            }
        }
        await self._send(ws, cmd)
        self.log("RequestDeviceList sent", "DEBUG", cmd)

    # ─────────────────────────────────────────────────────────────
    # Message Handling
    # ─────────────────────────────────────────────────────────────

    async def _handle_message(self, ws, raw: str):
        try:
            msg = json.loads(raw)
        except Exception:
            self.log("Received non-JSON message", "WARN", raw)
            return

        if not isinstance(msg, list):
            self.log("Invalid frame (expected array)", "ERROR", msg)
            return

        for entry in msg:
            if isinstance(entry, dict):
                await self._dispatch(ws, entry)

    async def _dispatch(self, ws, msg: Dict[str, Any]):
        key = next(iter(msg.keys()), None)
        data = msg.get(key, {})

        if key == "ServerInfo":
            self.log("Handshake complete with Intiface", "INFO", data)
            await self.request_device_list(ws)
            await self.start_scanning(ws)

        elif key == "DeviceList":
            self._handle_device_list(data)

        elif key == "DeviceAdded":
            self._handle_device_added(data)

        elif key == "DeviceRemoved":
            self._handle_device_removed(data)

        elif key == "Ok":
            self.log("Server OK", "DEBUG", data)

        elif key == "Error":
            self.log("Server error", "ERROR", data)

        else:
            self.log("Unhandled message", "DEBUG", msg)

    # ─────────────────────────────────────────────────────────────
    # Device Handling
    # ─────────────────────────────────────────────────────────────

    def _handle_device_list(self, data: Dict[str, Any]):
        devices = data.get("Devices", [])
        self.log(f"Received device list ({len(devices)} devices)", "INFO")

        for dev in devices:
            self._register_device(dev)

    def _handle_device_added(self, data: Dict[str, Any]):
        self.log("Device added", "INFO", data)
        self._register_device(data)

    def _handle_device_removed(self, data: Dict[str, Any]):
        idx = data.get("DeviceIndex")
        if idx in self.devices:
            removed = self.devices.pop(idx)
            self.log(f"Device removed: {removed.get('name')}", "INFO")

    def _register_device(self, device: Dict[str, Any]):
        idx = device.get("DeviceIndex")
        name = device.get("DeviceName")

        structured = {
            "index": idx,
            "name": name,
            "raw": device,
            "features": device.get("DeviceMessages", {})
        }

        self.devices[idx] = structured
        self.log(f"Registered device [{idx}] {name}", "INFO", structured)


# ─────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    client = IntifaceClient()

    try:
        asyncio.run(client.connect())
    except KeyboardInterrupt:
        print("\n[INTIFACE] Shutting down...")
    except Exception as e:
        print("\n[INTIFACE] Fatal error:", e)
        traceback.print_exc()
