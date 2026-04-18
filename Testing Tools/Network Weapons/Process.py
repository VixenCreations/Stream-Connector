import psutil
import time
from datetime import datetime

TARGET_EXE = "myowo_flutter.exe"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def scan():
    found = False

    for proc in psutil.process_iter(["pid", "name"]):
        if proc.info["name"] != TARGET_EXE:
            continue

        found = True
        pid = proc.info["pid"]

        try:
            conns = proc.connections(kind="inet")
        except Exception:
            continue

        for c in conns:
            proto = "TCP" if c.type == 1 else "UDP"
            laddr = f"{c.laddr.ip}:{c.laddr.port}" if c.laddr else "?"
            raddr = f"{c.raddr.ip}:{c.raddr.port}" if c.raddr else "—"
            status = c.status if proto == "TCP" else "LISTEN"

            log("FOUND SOCKET")
            log(f"  PID      : {pid}")
            log(f"  Protocol : {proto}")
            log(f"  Local    : {laddr}")
            log(f"  Remote   : {raddr}")
            log(f"  Status   : {status}")
            print()

    if not found:
        log("MyOWO not running")

if __name__ == "__main__":
    log("OWO SOCKET PROBE STARTED")
    print()

    try:
        while True:
            scan()
            time.sleep(1)
    except KeyboardInterrupt:
        log("Stopped.")
