import json
import websocket
import threading
import time

URL = "ws://127.0.0.1:8080/"

def pretty(obj):
    try:
        return json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        return str(obj)

def on_open(ws):
    print("=== CONNECTED TO STREAMER.BOT ===")

    # Fire every known config-related request
    requests = [
        {
            "request": "GetActions",
            "id": "dump-actions"
        },
        {
            "request": "GetGlobalVariables",
            "id": "dump-globals"
        },
        {
            "request": "GetSettings",
            "id": "dump-settings"
        },
        {
            "request": "GetTwitchUsers",
            "id": "dump-twitch-users"
        },
        {
            "request": "GetYouTubeUsers",
            "id": "dump-youtube-users"
        }
    ]

    for payload in requests:
        print("\n>>> SENDING REQUEST")
        print(pretty(payload))
        ws.send(json.dumps(payload))
        time.sleep(0.25)

    print("\n=== ALL REQUESTS SENT ===\n")

def on_message(ws, message):
    print("\n=== RAW FRAME RECEIVED ===")
    try:
        obj = json.loads(message)
        print(pretty(obj))
    except Exception:
        print(message)

def on_error(ws, error):
    print("\n=== WS ERROR ===")
    print(error)

def on_close(ws, code, msg):
    print("\n=== WS CLOSED ===")
    print(code, msg)

def run():
    ws = websocket.WebSocketApp(
        URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever(ping_interval=20, ping_timeout=10)

if __name__ == "__main__":
    run()
