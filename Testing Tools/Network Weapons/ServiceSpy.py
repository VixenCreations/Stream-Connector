import time
import socket
from pathlib import Path
from zeroconf import Zeroconf, ServiceBrowser

LOG_PATH = Path("owo_mdns_probe.log")


def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


# -----------------------------
# Service Type Listener
# -----------------------------
class ServiceTypeListener:
    def __init__(self, zc):
        self.zc = zc

    def add_service(self, zc, service_type, name):
        log(f"[SERVICE TYPE] {name}")

        # Now enumerate actual instances of this service
        ServiceBrowser(
            self.zc,
            name,
            ServiceInstanceListener(name)
        )

    def update_service(self, zc, service_type, name):
        pass

    def remove_service(self, zc, service_type, name):
        pass


# -----------------------------
# Instance Resolver
# -----------------------------
class ServiceInstanceListener:
    def __init__(self, service_type):
        self.service_type = service_type

    def add_service(self, zc, service_type, name):
        log(f"[INSTANCE FOUND] {name}")

        try:
            info = zc.get_service_info(service_type, name, timeout=3000)
            if not info:
                log("  └─ No service info resolved")
                return

            addresses = []
            for addr in info.addresses:
                try:
                    addresses.append(socket.inet_ntoa(addr))
                except Exception:
                    pass

            log("  └─ DETAILS")
            log(f"     Service : {service_type}")
            log(f"     Name    : {name}")
            log(f"     Host    : {info.server}")
            log(f"     Port    : {info.port}")
            log(f"     IPs     : {addresses}")
            log(f"     TXT     : {dict(info.properties)}")

        except Exception as e:
            log(f"  └─ ERROR resolving instance: {e}")

    def update_service(self, zc, service_type, name):
        pass

    def remove_service(self, zc, service_type, name):
        log(f"[INSTANCE REMOVED] {name}")


# -----------------------------
# Entry
# -----------------------------
def main():
    log("=" * 70)
    log("OWO NETWORK DISCOVERY STARTED")
    log("Waiting for mDNS services…")
    log("=" * 70)

    zc = Zeroconf()

    # Step 1: discover service TYPES
    ServiceBrowser(
        zc,
        "_services._dns-sd._udp.local.",
        ServiceTypeListener(zc)
    )

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log("Stopping probe.")
    finally:
        zc.close()


if __name__ == "__main__":
    main()
