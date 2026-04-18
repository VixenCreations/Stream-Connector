import asyncio
import json
import time
from pathlib import Path
from typing import Optional
from bleak import BleakScanner, BleakClient, BleakError
import struct  # For packing the data

# ───────────────────────────────────────────────
# CONFIG
# ───────────────────────────────────────────────
LOG_PATH = Path("saved/logs/owo/ble-probe.jsonl")
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

OWO_HINT = "OWO"

AUTH_PAYLOAD = b"0*AUTH*"
GAME_UNAVAILABLE = b"0*GAME*"  # Reduced size to fit the characteristic's expected payload size
GAME_AVAILABLE = b"0*GAMEAVAILABLE*"
PING_PAYLOAD = b"ping"

WRITE_UUID = "7764e556-c6a9-11e4-8731-1681e6b88ec1"
NOTIFY_UUIDS = [
    "7764eda8-c6a9-11e4-8731-1681e6b88ec1",
    "966e06c6-c717-11e4-8731-1681e6b88ec1",
    "b1f5a058-c717-11e4-8731-1681e6b88ec1",
    "b1f5a059-c717-11e4-8731-1681e6b88ec1",
    "776416cb-c717-11e4-8731-1681e6b88ec1",
]

PING_INTERVAL = 2.25
CONTROL_REFRESH = 5.0
PROBE_WINDOW = 0.5

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
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def notify_handler(uuid):
    def _handler(_, data: bytearray):
        log("notify.rx", {
            "uuid": uuid,
            "len": len(data),
            "hex": data.hex(),
            "ascii": data.decode("utf-8", errors="replace"),
            "raw": list(data),
        })
    return _handler


# ───────────────────────────────────────────────
# Client Class with Full Firmware Probing Logic
# ───────────────────────────────────────────────
class OWOBleProbeClient:
    def __init__(self):
        self.client: Optional[BleakClient] = None
        self.tx_lock = asyncio.Lock()

    async def scan(self):
        log("scan.start")
        devices = await BleakScanner.discover(timeout=6)
        for d in devices:
            if d.name and OWO_HINT in d.name:
                log("scan.found", {"name": d.name, "addr": d.address})
                return d
        log("scan.fail")
        return None

    async def connect(self):
        """Attempt to connect to a discovered OWO device."""
        dev = await self.scan()
        if not dev:
            log("error", {"message": "No OWO device found."})
            return False

        self.client = BleakClient(dev.address)
        await self.client.connect()

        log("ble.connected", {"addr": dev.address})

        # Subscribe notifications
        for u in NOTIFY_UUIDS:
            try:
                await self.client.start_notify(u, notify_handler(u))
                log("notify.subscribed", {"uuid": u})
            except Exception as e:
                log("notify.fail", {"uuid": u, "error": str(e)})

        return True

    async def _write(self, payload: bytes, label: str, probe=True, response=True, min_size=1):
        """Write data to the device."""
        async with self.tx_lock:
            log("write.tx", {
                "label": label,
                "hex": payload.hex(),
                "ascii": payload.decode("utf-8", "replace"),
                "response": response,
            })

            t0 = time.perf_counter()

            try:
                await self.client.write_gatt_char(
                    WRITE_UUID,
                    payload,
                    response=response
                )
                log("write.success", {"label": label, "time_ms": int((time.perf_counter() - t0) * 1000)})
                return True
            except BleakError as e:
                log("write.fail", {"error": str(e), "label": label, "hex": payload.hex()})
                if "Not connected" in str(e):
                    log("error", {"message": "Connection lost, attempting to reconnect."})
                    await self.reconnect()
                if "Invalid Attribute Value Length" in str(e):
                    log("error", {"message": "Payload too large, adjusting size."})
                    # Dynamically reduce payload size and try again if it's above the minimum size
                    if len(payload) > min_size:
                        payload = payload[:len(payload) // 2]
                        log("adjusted_payload", {"label": label, "hex": payload.hex()})
                        return await self._write(payload, label, probe=False, min_size=min_size)
                    else:
                        log("error", {"message": "Payload is too small to continue adjustment."})
                        return False
                return False

            if probe:
                await asyncio.sleep(PROBE_WINDOW)
                log("probe.window", {
                    "label": label,
                    "elapsed_ms": int((time.perf_counter() - t0) * 1000),
                    "note": "Beep / ACK occurs here if at all"
                })

    async def authenticate(self):
        """Authenticate with the OWO app."""
        await self._write(AUTH_PAYLOAD, "AUTH")
        await asyncio.sleep(0.2)
        await self._write(GAME_UNAVAILABLE, "GAME")

    async def configure(self):
        """Configure the OWO app."""
        await self._write(b"0*PROFILE*", "PROFILE")
        await asyncio.sleep(0.1)

        # Using struct to pack the muscle values
        muscle_data = [
            (0, 100), (1, 100), (2, 100), (3, 100), (4, 100),
            (5, 100), (6, 100), (7, 100), (8, 100), (9, 100)
        ]
        for i in range(0, len(muscle_data), 1):  # Send 1 muscle at a time
            muscle_id, muscle_pct = muscle_data[i]
            muscle_frame = struct.pack('<B B', muscle_id, muscle_pct)
            await self._write(muscle_frame, "MUSCLE")
            await asyncio.sleep(0.1)

        control_data = [(100, 100)]
        for value in control_data:
            control_frame = struct.pack('<B B', *value)  # Pack the control data into 2-byte pairs
            await self._write(control_frame, "CONTROL")
            log("control.success")

        # Send the reduced GAMEAVAILABLE payload
        game_available_reduced = b"0*GA*"  # Reduce to 4 characters
        await self._write(game_available_reduced, "GAMEAVAILABLE")
        await self._write(b"0*GAMEAVAILABLE*", "GAME_AVAILABLE", response=True, probe=True)

    async def stop_sensation(self):
        """Stop the current sensation."""
        await self._write(b"0*STOP*", "STOP")
        log("sensation.stop", {"status": "stopped"})

    async def send_sensation(self, sensation: str, intensity: int, duration_ms: int):
        """Send a specific sensation to the device."""
        payload = f"0*SENSATION*{{\"sensationId\":{sensation},\"intensity\":{intensity},\"duration\":{duration_ms}}}".encode("utf-8")
        await self._write(payload, "SENSATION")

    async def reconnect(self):
        """Attempt to reconnect if the connection is lost or if authentication fails."""
        log("reconnect.start")
        await self.client.disconnect()
        await asyncio.sleep(2)
        await self.connect()
        log("reconnect.success")

    async def run(self):
        """Main method to run the probe."""
        if not await self.connect():
            return

        await self.authenticate()
        await self.configure()

        log("ready", {"state": "running + probing"})

        sensation_variations = [
            {"sensation": 1, "intensity": 50, "duration_ms": 500},
            {"sensation": 2, "intensity": 75, "duration_ms": 1000},
            {"sensation": 3, "intensity": 100, "duration_ms": 1500},
        ]

        for var in sensation_variations:
            await self.send_sensation(var["sensation"], var["intensity"], var["duration_ms"])
            await asyncio.sleep(2)

        await self.stop_sensation()

        asyncio.create_task(self.keepalive())

        while True:
            await asyncio.sleep(1)

    async def keepalive(self):
        """Send a periodic ping to maintain connection.""" 
        while True:
            await self._write(PING_PAYLOAD, "PING", probe=False)
            await asyncio.sleep(PING_INTERVAL)


async def main():
    client = OWOBleProbeClient()
    await client.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("exit", {"reason": "ctrl+c"})
