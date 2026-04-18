import socket
import json
import time
import threading
import traceback
from pathlib import Path


class OWOVestManager:
    """
    Diagnostic OWO Visualizer client
    Clean, state-aware, but with targeted connection instrumentation
    """

    APP_NAME        = "Stream Connector"
    AUTH_PREFIX     = "0*AUTH*"
    VISUALIZER_ADDR = ("127.0.0.1", 54020)

    PING_INTERVAL       = 1.0
    FILE_WATCH_INTERVAL = 5.0

    LOG_LEVELS        = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
    CURRENT_LOG_LEVEL = LOG_LEVELS["DEBUG"]  # force debug for diagnostics

    @classmethod
    def _resolve_log_path(cls) -> Path:
        p = Path("owo_test_logs")
        p.mkdir(parents=True, exist_ok=True)
        return p / "owo_diagnostic.log"

    _log_path = _resolve_log_path.__func__(None)

    def _log(self, message: str, data: dict | None = None,
             level: str = "INFO", exc: BaseException | None = None):

        lvl = level.upper()
        if self.LOG_LEVELS[lvl] < self.CURRENT_LOG_LEVEL:
            return

        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        header = f"{ts} [{lvl}]"
        lines = [f"{header} {message}"]

        if data is not None:
            try:
                lines.append(json.dumps(data, indent=2))
            except Exception:
                lines.append(str(data))

        if exc:
            lines.append("[EXCEPTION]")
            lines.extend(traceback.format_exception(type(exc), exc, exc.__traceback__))

        entry = "\n".join(lines) + "\n"

        print(entry, end="")
        with self._log_path.open("a", encoding="utf-8") as f:
            f.write(entry)

    # ─────────────────────────────────────────────────────────
    # Init
    # ─────────────────────────────────────────────────────────

    def __init__(self):
        self._log("=== OWO DIAGNOSTIC TEST START ===", level="INFO")

        # ── Socket setup ────────────────────────────────────
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.settimeout(1.0)
            self._log("UDP socket created", level="INFO")
        except Exception as e:
            self._log("Failed creating UDP socket", level="CRITICAL", exc=e)
            raise

        # ── Flags & frames ─────────────────────────────────
        self.udp_enabled  = True
        self.AUTH_MESSAGE = f"{self.AUTH_PREFIX}{self.APP_NAME}".encode("utf-8")

        self._log("AUTH payload built", {
            "raw": self.AUTH_MESSAGE.decode("utf-8", errors="replace"),
            "hex": self.AUTH_MESSAGE.hex()
        }, level="DEBUG")

        # ── State tracking ─────────────────────────────────
        self._owo_state = {
            "registered": False,
            "last_auth": 0
        }

        # ── Template directory & cache ─────────────────────
        self._template_dir = Path("saved") / "controls" / "owo"
        self._templates    = set()

        self._log("Template directory resolved", {
            "path": str(self._template_dir),
            "exists": self._template_dir.exists()
        }, level="INFO")

        self._load_templates()
        self._start_file_watcher()

        self._log("OWO manager initialized", {
            "visualizer": self.VISUALIZER_ADDR,
            "templates": sorted(self._templates)
        }, level="INFO")

        # Register + heartbeat
        self._broadcast_presence(silent=False)
        self._start_heartbeat()

    # ─────────────────────────────────────────────────────────
    # Template Handling
    # ─────────────────────────────────────────────────────────

    def _load_templates(self):
        try:
            if not self._template_dir.exists():
                self._log("Template directory does not exist", {
                    "dir": str(self._template_dir)
                }, level="WARNING")
                self._templates = set()
                return

            files = {p.stem for p in self._template_dir.glob("*.owo")}
            self._templates = files

            self._log("Templates scanned", {
                "count": len(files),
                "files": sorted(files)
            }, level="DEBUG")

        except Exception as e:
            self._log("Failed scanning OWO template directory",
                      {"dir": str(self._template_dir)},
                      level="ERROR", exc=e)

    def _start_file_watcher(self):
        def watcher():
            while True:
                time.sleep(self.FILE_WATCH_INTERVAL)
                try:
                    before = set(self._templates)
                    self._load_templates()
                    after = set(self._templates)

                    if before != after:
                        self._log("OWO templates changed", {
                            "before": sorted(before),
                            "after": sorted(after)
                        }, level="INFO")
                except Exception as e:
                    self._log("File watcher error", level="ERROR", exc=e)

        threading.Thread(target=watcher, daemon=True).start()
        self._log("Template watcher thread started", level="INFO")

    # ─────────────────────────────────────────────────────────
    # UDP Core
    # ─────────────────────────────────────────────────────────

    def _send_udp(self, msg: bytes, addr: tuple[str, int], label: str = "UDP"):
        try:
            self.sock.sendto(msg, addr)
            self._log(f"{label} sent", {
                "to": addr,
                "bytes": len(msg),
                "preview": msg[:64].decode("utf-8", errors="replace")
            }, level="DEBUG")
        except Exception as exc:
            self._log(f"{label} send failed",
                      {"to": addr, "bytes": len(msg)},
                      level="ERROR", exc=exc)

    def _broadcast_presence(self, silent: bool = False):
        if not self.udp_enabled:
            self._log("UDP disabled, skipping AUTH", level="WARNING")
            return

        self._log("Sending AUTH to Visualizer", {
            "addr": self.VISUALIZER_ADDR
        }, level="INFO")

        try:
            self.sock.sendto(self.AUTH_MESSAGE, self.VISUALIZER_ADDR)
        except Exception as exc:
            self._log("AUTH send failed",
                      {"to": self.VISUALIZER_ADDR},
                      level="CRITICAL", exc=exc)
            return

        self._owo_state["registered"] = True
        self._owo_state["last_auth"] = time.time()

        if not silent:
            self._log("AUTH sent successfully", {
                "time": self._owo_state["last_auth"]
            }, level="INFO")

    # ─────────────────────────────────────────────────────────
    # Heartbeat
    # ─────────────────────────────────────────────────────────

    def _heartbeat_loop(self):
        self._log("Heartbeat loop entered", level="INFO")
        while True:
            time.sleep(self.PING_INTERVAL)
            try:
                self._broadcast_presence(silent=True)
            except Exception as e:
                self._log("Heartbeat error", level="ERROR", exc=e)

    def _start_heartbeat(self):
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        self._log("Heartbeat thread started", level="INFO")

    # ─────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────

    def apply(self, sensation_id: int, intensity: int, duration: int = 1000):
        payload = {
            "sensationId": max(0, sensation_id),
            "intensity":   max(0, min(intensity, 100)),
            "duration":    max(50, min(duration, 5000))
        }

        msg = f"0*SENSATION*{json.dumps(payload)}".encode("utf-8")
        self._send_udp(msg, self.VISUALIZER_ADDR, label="SENSATION")

    def send_file(self, name: str):
        self._log("send_file invoked", {"template": name}, level="INFO")

        try:
            available = {p.stem for p in self._template_dir.glob("*.owo")}
        except Exception as e:
            self._log("Failed scanning OWO template directory",
                      {"dir": str(self._template_dir)},
                      level="ERROR", exc=e)
            return

        if name not in available:
            self._log("OWO template not found",
                      {"requested": name, "available": sorted(available)},
                      level="ERROR")
            return

        path = self._template_dir / f"{name}.owo"

        try:
            content = path.read_text(encoding="utf-8").strip()
        except Exception as e:
            self._log("Failed reading OWO template file",
                      {"path": str(path)},
                      level="ERROR", exc=e)
            return

        if not content:
            self._log("OWO template is empty", {"path": str(path)}, level="WARNING")
            return

        msg = f"0*SENSATION*{content}".encode("utf-8")
        self._send_udp(msg, self.VISUALIZER_ADDR, label="TEMPLATE")

        self._log("Template sent successfully", {
            "template": name,
            "bytes": len(msg)
        }, level="INFO")

    def reconnect(self):
        self._log("Reconnect requested", level="INFO")
        self._owo_state["registered"] = False
        self._broadcast_presence(silent=False)


# ─────────────────────────────────────────────────────────────
# MAIN TEST HARNESS
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Starting OWO diagnostic test...")
    print("Logs will be written to ./owo_test_logs/owo_diagnostic.log\n")

    owo = OWOVestManager()

    # Give time for initial AUTH + heartbeat
    time.sleep(2)

    print("\n--- Sending test sensation ---")
    owo.apply(sensation_id=1, intensity=50, duration=1000)

    time.sleep(2)

    print("\n--- Listing templates ---")
    files = owo.list_available_files() if hasattr(owo, "list_available_files") else []
    print("Templates:", files)

    if files:
        print("\n--- Sending first template ---")
        owo.send_file(files[0])

    print("\n--- Forcing reconnect ---")
    owo.reconnect()

    print("\n--- Running. Press Ctrl+C to exit. ---")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nExiting OWO diagnostic test.")
