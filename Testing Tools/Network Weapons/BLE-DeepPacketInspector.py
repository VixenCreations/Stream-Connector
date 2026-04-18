# BLE-OWO-ConfiguredClient.py
# Full OWO BLE "real client" with:
#  - Deep TX/RX packet logging (ascii/hex/raw)
#  - AUTH + GAMEUNAVAILABLE handshake
#  - configure_vest(profile) traffic (PROFILE/MUSCLES/CONTROL/PROFILE_READY)
#  - Background keepalive ping loop
#  - Notify sniffer
#  - .owo template sender
#
# pip install bleak
#
# Run:
#   python BLE-OWO-ConfiguredClient.py

import asyncio
import json
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

from bleak import BleakScanner, BleakClient

# ─────────────────────────────────────────────
# Logging Controls
# ─────────────────────────────────────────────
DEBUG_PING = False      # <-- set True if you want ping spam
LOG_PING_TO_FILE = False

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
LOG_PATH = Path("saved/logs/owo/ble-session.jsonl")
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

OWO_HINT = "OWO"

# Frames you already validated on your vest
AUTH_PAYLOAD = b"0*AUTH*"
GAME_UNAVAILABLE = b"0*GAMEUNAVAILABLE"
PING_PAYLOAD = b"ping"
PASSIVE_OBSERVER = True

# Keepalive
PING_INTERVAL = 1.25  # seconds
KEEPALIVE_CONTROL_INTERVAL = 5.0  # seconds
REAUTH_INTERVAL = 0.0  # set to e.g. 30.0 if you want periodic re-auth (0 = disabled)

# Vendor UUIDs discovered from your scan (stable in your logs)
WRITE_UUID = "7764e556-c6a9-11e4-8731-1681e6b88ec1"

NOTIFY_UUIDS = [
    "7764eda8-c6a9-11e4-8731-1681e6b88ec1",
    "966e06c6-c717-11e4-8731-1681e6b88ec1",
    "b1f5a058-c717-11e4-8731-1681e6b88ec1",
    "b1f5a059-c717-11e4-8731-1681e6b88ec1",
    "776416cb-c717-11e4-8731-1681e6b88ec1",
]

TEMPLATE_DIR = Path("saved") / "controls" / "owo"


# ─────────────────────────────────────────────
# Logging + helpers
# ─────────────────────────────────────────────
def _ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def _hex(b: bytes) -> str:
    return b.hex()

def _hex_preview(b: bytes, n: int = 96) -> str:
    hx = b.hex()
    lim = n * 2
    return hx[:lim] + ("..." if len(hx) > lim else "")

def _safe_ascii(b: bytes) -> str:
    # show printable-ish ASCII without exploding logs
    try:
        s = b.decode("utf-8", errors="replace")
        return s
    except Exception:
        return "<decode-failed>"

def log(event: str, data: Optional[dict] = None):
    entry = {"time": _ts(), "event": event, "data": data or {}}
    print(json.dumps(entry, indent=2))
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


def make_notify_handler(uuid: str):
    def _handler(sender: int, data: bytearray):
        b = bytes(data)
        log("notify.rx", {
            "uuid": uuid,
            "handle": sender,
            "len": len(b),
            "hex": b.hex(),
            "ascii": _safe_ascii(b),
            "raw": list(b),
        })
    return _handler


# ─────────────────────────────────────────────
# Client
# ─────────────────────────────────────────────
@dataclass
class VestProfile:
    """
    High-level profile that we translate into the "config traffic" frames.

    name: profile name (optional; used in PROFILE frame if provided)
    muscles: either:
        - list of (muscle_id:int/str, percent:int) pairs
        - or dict muscle_id->percent
        - or None => defaults to 0-9 all 100%
    control: tuple (a,b) => "0*CONTROL*a,b"
    """
    name: str = ""
    muscles: Optional[Any] = None
    control: Tuple[int, int] = (100, 100)


class OWOBleConfiguredClient:
    def __init__(self):
        self._client: Optional[BleakClient] = None
        self._device_name: Optional[str] = None
        self._device_addr: Optional[str] = None

        self._disconnected = asyncio.Event()
        self._stop = asyncio.Event()

        self._tx_lock = asyncio.Lock()

        self._keepalive_task: Optional[asyncio.Task] = None
        self._authed = False

        TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)

    async def scan_find(self, timeout: float = 6.0):
        log("scan.start", {"timeout": timeout, "hint": OWO_HINT})
        devices = await BleakScanner.discover(timeout=timeout)
        vest = next((d for d in devices if d.name and OWO_HINT in d.name), None)
        if not vest:
            log("scan.fail", {"reason": "OWO device not found"})
            return None
        log("scan.found", {"name": vest.name, "address": vest.address})
        return vest

    def _on_disconnect(self, _):
        log("ble.disconnected", {"address": self._device_addr})
        self._authed = False
        self._disconnected.set()

    async def connect(self) -> bool:
        if self._client and self._client.is_connected:
            return True

        vest = await self.scan_find()
        if not vest:
            return False

        self._device_name = vest.name
        self._device_addr = vest.address
        self._disconnected.clear()
        self._stop.clear()

        try:
            self._client = BleakClient(
                vest.address,
                disconnected_callback=self._on_disconnect,
                timeout=15.0,
            )
            await self._client.connect()
            log("ble.connected", {"name": vest.name, "address": vest.address})
        except Exception as e:
            log("ble.connect.fail", {"error": str(e), "trace": traceback.format_exc()})
            return False

        # Subscribe notifies (best-effort with retries)
        for uuid in NOTIFY_UUIDS:
            for attempt in (1, 2, 3):
                if not self._client.is_connected:
                    log("notify.subscribe.abort", {"uuid": uuid, "reason": "not connected"})
                    break
                try:
                    await self._client.start_notify(uuid, make_notify_handler(uuid))
                    log("notify.subscribed", {"uuid": uuid, "attempt": attempt})
                    break
                except Exception as e:
                    log("notify.subscribe.fail", {"uuid": uuid, "attempt": attempt, "error": str(e)})
                    await asyncio.sleep(0.75)

        return True

    async def _write(
        self,
        payload: bytes,
        *,
        label: str = "",
        response: bool = False,
        silent: bool = False,
        expect_notify: bool = False,
        notify_timeout: float = 0.35,
    ) -> bool:
        if not self._client or not self._client.is_connected:
            ok = await self.connect()
            if not ok:
                return False

        t0 = time.perf_counter()

        if not silent:
            log("write.send", {
                "label": label or "TX",
                "uuid": WRITE_UUID,
                "len": len(payload),
                "hex": _hex(payload),
                "ascii": _safe_ascii(payload),
                "response": response,
            })

        async with self._tx_lock:
            try:
                await self._client.write_gatt_char(
                    WRITE_UUID,
                    payload,
                    response=response
                )

                t1 = time.perf_counter()

                if not silent:
                    log("write.ok", {
                        "label": label or "TX",
                        "dt_ms": round((t1 - t0) * 1000, 2),
                    })

                # 🔍 Probe for firmware reaction (beep / ACK)
                if expect_notify:
                    await self._probe_notify_window(label, notify_timeout)

                return True

            except Exception as e:
                log("write.fail", {
                    "label": label or "TX",
                    "error": str(e),
                    "trace": traceback.format_exc()
                })
                return False
                
    async def _probe_notify_window(self, label: str, timeout: float):
        """
        Watches for any notify activity immediately after a write.
        This is where the vest acknowledges / beeps internally.
        """
        start = time.perf_counter()

        while (time.perf_counter() - start) < timeout:
            await asyncio.sleep(0.01)

        log("notify.window", {
            "label": label,
            "window_ms": int(timeout * 1000),
            "note": "If beep occurred, it happened here"
        })

    async def authenticate(self) -> bool:
        if not await self.connect():
            return False

        log("configure.start", {})

        ok = await self._write(AUTH_PAYLOAD, label="AUTH", response=False)
        if not ok:
            return False
        await asyncio.sleep(0.20)

        ok2 = await self._write(GAME_UNAVAILABLE, label="GAMEUNAVAILABLE", response=False)
        if not ok2:
            # not always fatal, but we track it
            log("game.state.fail", {"state": "GAMEUNAVAILABLE"})
        else:
            log("game.state", {"state": "GAMEUNAVAILABLE"})
        await asyncio.sleep(0.25)

        self._authed = True
        return True

    def _normalize_muscles(self, muscles: Any) -> List[Tuple[str, int]]:
        if muscles is None:
            return [(str(i), 100) for i in range(10)]

        if isinstance(muscles, dict):
            out = []
            for k, v in muscles.items():
                out.append((str(k), int(v)))
            return out

        if isinstance(muscles, list) or isinstance(muscles, tuple):
            out = []
            for item in muscles:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    out.append((str(item[0]), int(item[1])))
            return out

        # fallback
        return [(str(i), 100) for i in range(10)]

    async def configure_vest(self, profile: VestProfile) -> bool:
        """
        Wires in the "config traffic" you saw in your logs.

        Current working sequence (per your 2026-01-19 logs):
          AUTH
          GAMEUNAVAILABLE
          0*PROFILE*{optional name}
          0*MUSCLES*0%100,1%100,...,9%100
          0*CONTROL*{a},{b}
          0*PROFILE_READY*
        """
        if not self._authed:
            if not await self.authenticate():
                return False

        # PROFILE frame
        # If no name, you confirmed the vest doesn't shut you out with a bare header.
        prof_name = (profile.name or "").strip()
        if prof_name:
            profile_frame = f"0*PROFILE*{prof_name}".encode("utf-8")
        else:
            profile_frame = b"0*PROFILE*"

        muscles = self._normalize_muscles(profile.muscles)
        # Clamp & format: "0*MUSCLES*0%100,1%100..."
        parts = []
        for mid, pct in muscles:
            pct_i = max(0, min(100, int(pct)))
            parts.append(f"{mid}%{pct_i}")
        muscles_frame = ("0*MUSCLES*" + ",".join(parts)).encode("utf-8")

        a, b = profile.control
        a = max(0, min(100, int(a)))
        b = max(0, min(100, int(b)))
        control_frame = f"0*CONTROL*{a},{b}".encode("utf-8")

        ready_frame = b"0*PROFILE_READY*"

        log("configure.profile", {
            "name": prof_name,
            "muscles_count": len(muscles),
            "control": [a, b],
        })

        # Send in order with small pacing (WinRT BLE is timing-sensitive)
        if not await self._write(profile_frame, label="PROFILE"):
            return False
        await asyncio.sleep(0.10)

        if not await self._write(muscles_frame, label="MUSCLES"):
            return False
        await asyncio.sleep(0.10)

        if not await self._write(control_frame, label="CONTROL"):
            return False
        await asyncio.sleep(0.10)

        if not await self._write(ready_frame, label="PROFILE_READY"):
            return False

        log("configure.complete", {})
        return True

    async def _keepalive_loop(self):
        log("keepalive.start", {
            "ping_interval_s": PING_INTERVAL,
            "control_refresh_s": KEEPALIVE_CONTROL_INTERVAL,
        })

        last_auth = time.time()
        last_control = 0.0

        while not self._stop.is_set():
            if not self._client or not self._client.is_connected or self._disconnected.is_set():
                await asyncio.sleep(0.25)
                continue

            now = time.time()

            # ─── Silent ping ─────────────────────────
            await self._write(
                PING_PAYLOAD,
                label="PING",
                response=False,
                silent=True
            )

            # ─── Periodic CONTROL refresh (CRITICAL) ───
            if (now - last_control) >= KEEPALIVE_CONTROL_INTERVAL:
                await self._write(
                    b"0*CONTROL*100,100",
                    label="CONTROL.keepalive",
                    silent=True
                )
                last_control = now

            # ─── Optional re-auth (rarely needed) ───
            if REAUTH_INTERVAL and (now - last_auth) >= REAUTH_INTERVAL:
                await self._write(AUTH_PAYLOAD, label="AUTH.reauth", silent=True)
                await self._write(GAME_UNAVAILABLE, label="GAMEUNAVAILABLE.reauth", silent=True)
                last_auth = now

            await asyncio.sleep(PING_INTERVAL)

        log("keepalive.stop", {})

    async def start_keepalive(self):
        if self._keepalive_task and not self._keepalive_task.done():
            return
        self._keepalive_task = asyncio.create_task(self._keepalive_loop())

    async def stop(self):
        self._stop.set()
        if self._keepalive_task:
            try:
                await asyncio.sleep(0)  # allow task to observe stop
            except Exception:
                pass
        try:
            if self._client and self._client.is_connected:
                await self._client.disconnect()
        except Exception:
            pass

    async def send_raw_ascii(self, text: str) -> bool:
        return await self._write(text.encode("utf-8"), label="RAW_ASCII")

    async def send_sensation_json(self, sensation_id: int, intensity: int, duration_ms: int) -> bool:
        if not self._authed:
            if not await self.authenticate():
                return False
        payload = {
            "sensationId": int(sensation_id),
            "intensity": max(0, min(100, int(intensity))),
            "duration": max(50, min(5000, int(duration_ms))),
        }
        frame = ("0*SENSATION*" + json.dumps(payload, separators=(",", ":"))).encode("utf-8")
        return await self._write(frame, label="SENSATION_JSON")

    async def send_template(self, name: str) -> bool:
        path = TEMPLATE_DIR / f"{name}.owo"
        if not path.exists():
            log("template.missing", {"name": name, "path": str(path)})
            return False
        content = path.read_text(encoding="utf-8").strip()
        if not content:
            log("template.empty", {"name": name})
            return False
        frame = ("0*SENSATION*" + content).encode("utf-8")
        log("template.send", {"name": name, "bytes": len(frame)})
        return await self._write(frame, label=f"TEMPLATE:{name}")

    def list_templates(self) -> List[str]:
        if not TEMPLATE_DIR.exists():
            return []
        return sorted({p.stem for p in TEMPLATE_DIR.glob("*.owo")})

    async def run_console(self):
        log("app.start", {"log_file": str(LOG_PATH)})

        if not await self.connect():
            log("app.stop", {"reason": "connect failed"})
            return

        # baseline handshake + start keepalive
        await self.authenticate()
        await self.start_keepalive()

        # baseline “safe” profile to match your working logs
        await self.configure_vest(VestProfile(
            name="",  # can set if you want
            muscles=None,  # defaults to 0-9 @ 100%
            control=(100, 100),
        ))

        log("ready", {"status": "configured + alive"})

        print(
            "\nCommands:\n"
            "  help\n"
            "  auth\n"
            "  config [name]                 (re-send PROFILE/MUSCLES/CONTROL/READY)\n"
            "  configmus <id%pct,...>        (example: configmus 0%100,1%0,2%50)\n"
            "  control <a> <b>               (0-100)\n"
            "  ping                          (manual ping)\n"
            "  sense <id> <intensity> <ms>   (JSON sensation)\n"
            "  templates\n"
            "  send <template>\n"
            "  raw <ascii text>\n"
            "  quit\n"
        )

        # mutable “current profile” so you can iterate quickly
        current_profile = VestProfile(name="", muscles=None, control=(100, 100))

        while self._client and self._client.is_connected and not self._disconnected.is_set():
            try:
                cmd = await asyncio.to_thread(input, "OWO> ")
            except (EOFError, KeyboardInterrupt):
                break

            cmd = (cmd or "").strip()
            if not cmd:
                continue

            parts = cmd.split(" ", 1)
            head = parts[0].lower()
            tail = parts[1] if len(parts) > 1 else ""

            try:
                if head in ("quit", "exit", "q"):
                    break

                if head == "help":
                    print("help | auth | config [name] | configmus <id%pct,...> | control <a> <b> | ping | sense <id> <intensity> <ms> | templates | send <name> | raw <text> | quit")
                    continue

                if head == "auth":
                    await self.authenticate()
                    continue

                if head == "config":
                    current_profile.name = (tail or "").strip()
                    await self.configure_vest(current_profile)
                    continue

                if head == "configmus":
                    # expects: "0%100,1%0,2%50"
                    spec = (tail or "").strip()
                    mus = []
                    for chunk in spec.split(","):
                        chunk = chunk.strip()
                        if not chunk or "%" not in chunk:
                            continue
                        mid, pct = chunk.split("%", 1)
                        mus.append((mid.strip(), int(pct.strip())))
                    current_profile.muscles = mus if mus else None
                    await self.configure_vest(current_profile)
                    continue

                if head == "control":
                    a_str, b_str = tail.split()
                    current_profile.control = (int(a_str), int(b_str))
                    await self.configure_vest(current_profile)
                    continue

                if head == "ping":
                    await self._write(PING_PAYLOAD, label="PING.manual", response=False)
                    continue

                if head == "sense":
                    sid_s, inten_s, ms_s = tail.split()
                    await self.send_sensation_json(int(sid_s), int(inten_s), int(ms_s))
                    continue

                if head == "templates":
                    t = self.list_templates()
                    log("templates.list", {"count": len(t), "templates": t})
                    continue

                if head == "send":
                    name = (tail or "").strip()
                    await self.send_template(name)
                    continue

                if head == "raw":
                    await self.send_raw_ascii(tail)
                    continue

                log("cmd.unknown", {"cmd": cmd})

            except Exception as e:
                log("cmd.error", {"cmd": cmd, "error": str(e), "trace": traceback.format_exc()})

        log("app.stop", {"reason": "disconnected or user exit"})
        await self.stop()


async def main():
    c = OWOBleConfiguredClient()
    await c.run_console()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("exit", {"reason": "ctrl+c"})
    except Exception:
        log("fatal", {"trace": traceback.format_exc()})
