import asyncio
import struct
from bleak import BleakClient, BleakError, BleakScanner
import time
import json

# ───────────────────────────────────────────────
# CONFIG
# ───────────────────────────────────────────────
WRITE_UUID = "7764e556-c6a9-11e4-8731-1681e6b88ec1"

LOG_PATH = "saved/logs/owo/ble-probe.jsonl"
AUTH_PAYLOAD = b"0*AUTH*"
GAME_AVAILABLE = b"0*GAMEAVAILABLE*"
GAME_UNAVAILABLE = b"0*GAME*"
PING_PAYLOAD = b"ping"
OWO_HINT = "OWO"  # HINT for identifying the correct device

# ───────────────────────────────────────────────
# Logging
# ───────────────────────────────────────────────
def log(event, data=None):
    entry = {
        "time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "event": event,
        "data": data or {},
    }
    print(json.dumps(entry, indent=2))
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


async def _write(client, payload: bytes, label: str, probe=True, response=True):
    """Write data to the device."""
    log("write.tx", {
        "label": label,
        "hex": payload.hex(),
        "ascii": payload.decode("utf-8", "replace"),
        "response": response,
    })
    try:
        await client.write_gatt_char(WRITE_UUID, payload, response=response)
        log("write.success", {"label": label, "time_ms": 100})
    except BleakError as e:
        log("write.fail", {"error": str(e), "label": label, "hex": payload.hex()})
        if "Invalid Attribute Value Length" in str(e):
            log("error", {"message": f"Payload size too large for {label}, adjusting size."})
            # Dynamically reduce payload size if the error occurs
            adjusted_payload = payload[:len(payload) // 2]  # Reduce the payload size by half
            log("adjusted_payload", {"label": label, "hex": adjusted_payload.hex()})
            await _write(client, adjusted_payload, label, probe=False)  # Retry with reduced payload
        elif "Not connected" in str(e):
            log("error", {"message": "Connection lost, attempting to reconnect."})
            await client.disconnect()
            await client.connect()

    if probe:
        await asyncio.sleep(0.5)
        log("probe.window", {
            "label": label,
            "note": "Beep / ACK occurs here if at all"
        })


# Format variations for GAMEAVAILABLE
async def send_gameavailable_formats(client):
    formats = [
        {"label": "GAMEAVAILABLE_RAW", "payload": b"0*GAMEAVAILABLE*"},
        {"label": "GAMEAVAILABLE_ASCII", "payload": b"0*GA*"},
        {"label": "GAMEAVAILABLE_HEX", "payload": bytes.fromhex("30 2a 47 41 2a")},
        {"label": "GAMEAVAILABLE_UTF8", "payload": "0*GAMEAVAILABLE*".encode("utf-8")},
        {"label": "GAMEAVAILABLE_STRUCT", "payload": struct.pack('<B B B B B', 48, 42, 71, 65, 42)},
        {"label": "GAMEAVAILABLE_SPLIT", "payload": b"0*GA" + b"*" + b"AVAILABLE"},
        {"label": "GAMEAVAILABLE_SIMPLE", "payload": b"0GAAVAILABLE"},
        {"label": "GAMEAVAILABLE_INDIVIDUAL", "payload": b"0*G" + b"A" + b"V" + b"E" + b"R" + b"S" + b"A" + b"V" + b"E" + b"N" + b"T*"}
    ]
    
    # Send each payload and dynamically adjust if size exceeds limit
    for format in formats:
        payload = format["payload"]
        await _write(client, payload, format["label"], probe=True)


# ───────────────────────────────────────────────
# Client Class with Connection Logic and Reconnection
# ───────────────────────────────────────────────
class OWOBleProbeClient:
    def __init__(self, device_address: str = None):
        self.client = None
        self.device_address = device_address

    async def scan_for_device(self):
        """Scan for devices and find the one with the OWO hint in the name."""
        log("scan.start")
        devices = await BleakScanner.discover(timeout=6)
        for d in devices:
            if d.name and OWO_HINT in d.name:
                log("scan.found", {"name": d.name, "addr": d.address})
                return d
        log("scan.fail")
        return None

    async def connect(self):
        """Attempt to connect to the discovered OWO device."""
        if not self.device_address:
            device = await self.scan_for_device()
            if device:
                self.device_address = device.address
            else:
                log("error", {"message": "No OWO device found."})
                return False

        try:
            log(f"Connecting to device: {self.device_address}")
            self.client = BleakClient(self.device_address)
            await self.client.connect()
            log("ble.connected", {"addr": self.client.address})
        except BleakError as e:
            log("connect.fail", {"error": str(e)})
            await self.reconnect()
            return False
        return True

    async def disconnect(self):
        """Disconnect from the device."""
        try:
            await self.client.disconnect()
            log("ble.disconnected", {"addr": self.client.address})
        except BleakError as e:
            log("disconnect.fail", {"error": str(e)})

    async def reconnect(self):
        """Attempt to reconnect if the connection is lost or if authentication fails."""
        log("reconnect.start")
        await self.client.disconnect()
        await asyncio.sleep(2)
        await self.connect()
        log("reconnect.success")

    async def run(self):
        """Main method to connect, send formats, and disconnect."""
        if await self.connect():
            await send_gameavailable_formats(self.client)
            await self.disconnect()
        else:
            log("error", {"message": "Unable to establish connection."})


async def main():
    # Optionally, provide the address here if already known, or let the script scan
    device_address = None  # Set this to your device address if known
    client = OWOBleProbeClient(device_address)
    await client.run()


if __name__ == "__main__":
    asyncio.run(main())
