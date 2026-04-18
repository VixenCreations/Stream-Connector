import json
import time
import threading
import websocket

URL = "ws://127.0.0.1:12345/buttplug"

TEST_PAYLOADS = {
    "object_keyed": {
        "RequestServerInfo": {
            "ClientName": "SchemaProbe",
            "MessageVersion": 3
        }
    },
    "array_keyed": [
        {
            "RequestServerInfo": {
                "ClientName": "SchemaProbe",
                "MessageVersion": 3
            }
        }
    ],
    "object_type": {
        "Type": "RequestServerInfo",
        "ClientName": "SchemaProbe",
        "MessageVersion": 3
    },
    "array_type": [
        {
            "Type": "RequestServerInfo",
            "ClientName": "SchemaProbe",
            "MessageVersion": 3
        }
    ],
}


def run_test(name, payload):
    print("\n" + "=" * 80)
    print(f"TEST: {name}")
    print("SENDING:")
    print(json.dumps(payload, indent=2))
    print("=" * 80)

    response_received = {"flag": False}

    def on_message(ws, message):
        print("\n--- RAW SERVER MESSAGE ---")
        print(message)
        print("--- END SERVER MESSAGE ---\n")
        response_received["flag"] = True
        ws.close()

    def on_error(ws, error):
        print("\n*** WS ERROR ***")
        print(error)
        print("*** END WS ERROR ***\n")
        ws.close()

    def on_close(ws, code, msg):
        print(f"[WS CLOSED] code={code} msg={msg}")

    def on_open(ws):
        print("[WS OPEN] Sending payload...\n")
        ws.send(json.dumps(payload))

    ws = websocket.WebSocketApp(
        URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    t = threading.Thread(target=ws.run_forever, daemon=True)
    t.start()

    # Wait up to 2 seconds for response
    timeout = 2.0
    start = time.time()
    while time.time() - start < timeout:
        if response_received["flag"]:
            break
        time.sleep(0.05)

    if not response_received["flag"]:
        print("\n!!! No response received within timeout !!!\n")
        ws.close()

    time.sleep(0.5)  # small delay before next test


if __name__ == "__main__":
    print("Connecting to:", URL)

    for name, payload in TEST_PAYLOADS.items():
        run_test(name, payload)

    print("\nAll tests complete.\n")
