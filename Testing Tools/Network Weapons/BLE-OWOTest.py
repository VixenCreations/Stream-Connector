# BLE-OWO-FullClient.py
# Fully stabilized OWO BLE client with keepalive + auth + sensation support

import asyncio
import json
import time
import traceback
from pathlib import Path
from typing import Optional, List

from bleak import BleakScanner, BleakClient


# ─────────────────────────────
# Logging
# ─────────────────────────────
LOG_PATH = Path("saved/logs/owo/ble-client-log.jsonl")
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

def log(event, data=None):
    entry = {
        "time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "event": event,
        "data": data or {},
    }
    print(json.dumps(entry, indent=2))
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


# ─────────────────────────────
# Constants
# ─────────────────────────────
OWO_HINT = "OWO"

AUTH_FRAME = "0*AUTH*"
GAME_FRAME = "0*GAMEUNAVAILABLE"
PING_FRAME = "0*PING*"

KEEPALIVE_INTERVAL = 0.5   # seconds — critical
REAUTH_INTERVAL = 8.0      # seconds


# ─────────────────────────────
# BLE Client
# ─────────────────────────────
class OWOBleClient:
    def __init__(self):
        self.client: Optional[BleakClient] = None
        self.device = None
        self.write_uuid = None
        self.notify_uuids = []

        self.connected = False
        self.authed = False

        self._tx_lock = asyncio.Lock()
        self._stop_evt = asyncio.Event()

    # -------------------------
    # Discovery
    # -------------------------
    async def scan(self):
        log("scan.start")
        devices = await BleakScanner.discover(timeout=6)

        for d in devices:
            if d.name and OWO_HINT in d.name:
                log("scan.found", {"name": d.name, "address": d.address})
                return d

        log("scan.fail", {"reason": "OWO not found"})
        return None

    # -------------------------
    # Connect
    # -------------------------
    async def connect(self):
        self.device = await self.scan()
        if not self.device:
            return False

        self.client = BleakClient(
            self.device.address,
            disconnected_callback=self._on_disconnect,
        )

        await self.client.connect()
        self.connected = True
        log("ble.connected", {"address": self.device.address})

        await self._discover_chars()
        return True

    def _on_disconnect(self, _):
        log("ble.disconnected")
        self.connected = False
        self.authed = False
        self._stop_evt.set()

    # -------------------------
    # GATT Discovery
    # -------------------------
    async def _discover_chars(self):
        services = self.client.services

        for s in services:
            if s.uuid.startswith("000018"):
                continue

            for ch in s.characteristics:
                if "write" in ch.properties:
                    self.write_uuid = ch.uuid
                if "notify" in ch.properties:
                    self.notify_uuids.append(ch.uuid)

        log("gatt.ready", {
            "write": self.write_uuid,
            "notify": self.notify_uuids
        })

        for u in self.notify_uuids:
            try:
                await self.client.start_notify(u, self._notify_handler)
                log("notify.subscribed", {"uuid": u})
            except:
                pass

    # -------------------------
    # Notify
    # -------------------------
    def _notify_handler(self, sender, data):
        log("notify", {
            "handle": sender,
            "hex": data.hex(),
            "raw": list(data),
        })

    # -------------------------
    # Low-level send
    # -------------------------
    async def send(self, text: str):
        async with self._tx_lock:
            payload = text.encode()
            await self.client.write_gatt_char(
                self.write_uuid,
                payload,
                response=False
            )
            log("write", {
                "ascii": text,
                "hex": payload.hex(),
                "len": len(payload)
            })

    # -------------------------
    # Auth + Keepalive
    # -------------------------
    async def authenticate(self):
        await self.send(AUTH_FRAME)
        await asyncio.sleep(0.1)
        await self.send(GAME_FRAME)
        self.authed = True
        log("auth.ok")

    async def keepalive_loop(self):
        log("keepalive.start")
        while not self._stop_evt.is_set():
            try:
                if self.connected:
                    await self.send(PING_FRAME)
                await asyncio.sleep(KEEPALIVE_INTERVAL)
            except:
                break

    async def reauth_loop(self):
        while not self._stop_evt.is_set():
            await asyncio.sleep(REAUTH_INTERVAL)
            if self.connected and not self.authed:
                await self.authenticate()

    # -------------------------
    # Sensation
    # -------------------------
    async def send_sensation(self, text: str):
        if not self.authed:
            await self.authenticate()
        await self.send(f"0*SENSATION*{text}")

    # -------------------------
    # Main
    # -------------------------
    async def run(self):
        ok = await self.connect()
        if not ok:
            return

        await self.authenticate()

        asyncio.create_task(self.keepalive_loop())
        asyncio.create_task(self.reauth_loop())

        log("client.ready")

        while self.connected:
            await asyncio.sleep(1)


# ─────────────────────────────
# Entrypoint
# ─────────────────────────────
async def main():
    client = OWOBleClient()
    await client.run()

if __name__ == "__main__":
    asyncio.run(main())
