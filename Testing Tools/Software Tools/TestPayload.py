import requests
import json
import time
import sys
import os
import traceback
import uuid
from datetime import datetime
from typing import Any

# ─────────────────────────────────────────────────────────────
# Stream Connector - External API Developer Reference Test
# Author: @Vixenlicious
# Purpose:
#   Official test harness for External API plugin developers.
#   Clean logging, full stack traces, request_id correlation.
# ─────────────────────────────────────────────────────────────

BASE_URL = "http://127.0.0.1:8840"
TIMEOUT = 5

LOG_DIR = os.path.join(os.getcwd(), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(
    LOG_DIR,
    f"external_api_dev_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

# ─────────────────────────────────────────────────────────────
# Request ID
# ─────────────────────────────────────────────────────────────

def new_request_id() -> str:
    return uuid.uuid4().hex

# ─────────────────────────────────────────────────────────────
# Logging System
# ─────────────────────────────────────────────────────────────

LOG_LINE = "{timestamp} | {level:<5} | {request_id} | {message}"

def _write_to_file(message: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(message + "\n")
        f.flush()


def log(message: str, level: str = "INFO", request_id: str = "-"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = LOG_LINE.format(
        timestamp=timestamp,
        level=level,
        request_id=request_id,
        message=message
    )
    print(line)
    _write_to_file(line)


def log_json(title: str, data: Any, level: str = "INFO", request_id: str = "-"):
    log(title, level=level, request_id=request_id)
    try:
        payload = json.dumps(data, indent=2)
    except Exception:
        payload = str(data)

    for line in payload.splitlines():
        formatted = f"    {line}"
        print(formatted)
        _write_to_file(formatted)


def log_exception(title: str, request_id: str = "-"):
    log(title, level="ERROR", request_id=request_id)
    trace = traceback.format_exc()
    for line in trace.splitlines():
        formatted = f"    {line}"
        print(formatted)
        _write_to_file(formatted)

# ─────────────────────────────────────────────────────────────
# HTTP Safety Wrapper
# ─────────────────────────────────────────────────────────────

def safe_request(method: str, url: str, request_id: str, **kwargs) -> requests.Response:
    try:
        return requests.request(method, url, timeout=TIMEOUT, **kwargs)
    except requests.exceptions.Timeout:
        log(f"Request timed out: {method} {url}", level="ERROR", request_id=request_id)
        log_exception("Timeout stack trace", request_id=request_id)
        sys.exit(1)
    except requests.exceptions.ConnectionError:
        log(f"Connection error: {url} is unreachable", level="ERROR", request_id=request_id)
        log_exception("Connection error stack trace", request_id=request_id)
        sys.exit(1)
    except Exception:
        log("Unexpected request failure", level="ERROR", request_id=request_id)
        log_exception("Request exception stack trace", request_id=request_id)
        sys.exit(1)

# ─────────────────────────────────────────────────────────────
# 1. API Info
# ─────────────────────────────────────────────────────────────

def test_api_info():
    request_id = new_request_id()
    log("Testing /api/external/info endpoint", request_id=request_id)

    try:
        url = f"{BASE_URL}/api/external/info"
        resp = safe_request("GET", url, request_id=request_id)

        log(f"Status Code: {resp.status_code}", request_id=request_id)
        data = resp.json()
        log_json("API Info Response:", data, level="OK", request_id=request_id)
    except Exception:
        log("Failure during /api/external/info test", level="ERROR", request_id=request_id)
        log_exception("API info test stack trace", request_id=request_id)
        sys.exit(1)

# ─────────────────────────────────────────────────────────────
# 2. External Hook List
# ─────────────────────────────────────────────────────────────

def test_external_list():
    request_id = new_request_id()
    log("Testing /api/external/list endpoint", request_id=request_id)

    try:
        url = f"{BASE_URL}/api/external/list"
        resp = safe_request("GET", url, request_id=request_id)

        log(f"Status Code: {resp.status_code}", request_id=request_id)
        data = resp.json()
        log_json("Registered External Hooks:", data, level="OK", request_id=request_id)
    except Exception:
        log("Failure during /api/external/list test", level="ERROR", request_id=request_id)
        log_exception("External list stack trace", request_id=request_id)
        sys.exit(1)

# ─────────────────────────────────────────────────────────────
# 3. Command Execution (POST)
# ─────────────────────────────────────────────────────────────

def test_external_exec_post():
    request_id = new_request_id()
    log("Testing /api/external/exec (POST)", request_id=request_id)

    try:
        url = f"{BASE_URL}/api/external/exec"

        payload = {
            "provider": "streamconnector",
            "commandId": "test",
            "request_id": request_id,
            "context": {
                "user": "Vixenlicious",
                "source": "python_reference_test",
                "note": "auto-registered chain test",
                "timestamp": time.time()
            }
        }

        log_json("Sending Payload:", payload, request_id=request_id)

        resp = safe_request("POST", url, request_id=request_id, json=payload)

        log(f"Status Code: {resp.status_code}", request_id=request_id)
        data = resp.json()
        log_json("Execution Response:", data, level="OK", request_id=request_id)
    except Exception:
        log("Failure during POST execution test", level="ERROR", request_id=request_id)
        log_exception("POST execution stack trace", request_id=request_id)
        sys.exit(1)

# ─────────────────────────────────────────────────────────────
# 4. Command Execution (GET)
# ─────────────────────────────────────────────────────────────

def test_external_exec_get():
    request_id = new_request_id()
    log("Testing /api/external/exec (GET)", request_id=request_id)

    try:
        url = f"{BASE_URL}/api/external/exec"
        params = {
            "provider": "streamconnector",
            "commandId": "test_get",
            "user": "Vixenlicious",
            "source": "python_reference_test",
            "request_id": request_id
        }

        log_json("Sending Params:", params, request_id=request_id)

        resp = safe_request("GET", url, request_id=request_id, params=params)

        log(f"Status Code: {resp.status_code}", request_id=request_id)
        data = resp.json()
        log_json("Execution Response:", data, level="OK", request_id=request_id)
    except Exception:
        log("Failure during GET execution test", level="ERROR", request_id=request_id)
        log_exception("GET execution stack trace", request_id=request_id)
        sys.exit(1)

# ─────────────────────────────────────────────────────────────
# 5. Invalid Payload
# ─────────────────────────────────────────────────────────────

def test_invalid_payload():
    request_id = new_request_id()
    log("Testing invalid payload handling", request_id=request_id)

    try:
        url = f"{BASE_URL}/api/external/exec"

        bad_payload = {
            "provider": "streamconnector",
            "request_id": request_id
        }

        log_json("Sending Bad Payload:", bad_payload, request_id=request_id)

        resp = safe_request("POST", url, request_id=request_id, json=bad_payload)

        log(f"Status Code: {resp.status_code}", request_id=request_id)
        data = resp.json()
        log_json("Error Response:", data, level="WARN", request_id=request_id)
    except Exception:
        log("Failure during invalid payload test", level="ERROR", request_id=request_id)
        log_exception("Invalid payload stack trace", request_id=request_id)
        sys.exit(1)

# ─────────────────────────────────────────────────────────────
# 6. Chain Test
# ─────────────────────────────────────────────────────────────

def test_command_chain():
    log("Testing command chain burst")

    try:
        url = f"{BASE_URL}/api/external/exec"

        for i in range(1, 6):
            request_id = new_request_id()

            payload = {
                "provider": "streamconnector",
                "commandId": f"chain_step_{i}",
                "request_id": request_id,
                "context": {
                    "user": "Vixenlicious",
                    "source": "python_chain_test",
                    "step": i,
                    "note": "sequential chain test"
                }
            }

            log_json(f"Dispatching Chain Step {i}:", payload, request_id=request_id)

            resp = safe_request("POST", url, request_id=request_id, json=payload)

            log(f"Status Code: {resp.status_code}", request_id=request_id)
            data = resp.json()
            log_json(f"Chain Step {i} Response:", data, level="OK", request_id=request_id)

            time.sleep(0.25)
    except Exception:
        log("Failure during command chain test", level="ERROR")
        log_exception("Command chain stack trace")
        sys.exit(1)

# ─────────────────────────────────────────────────────────────
# Main Runner
# ─────────────────────────────────────────────────────────────

def main():
    log("=== Stream Connector External API Developer Reference Test Suite START ===", level="OK")
    log(f"Log File: {LOG_FILE}")

    test_api_info()
    test_external_list()
    test_external_exec_post()
    test_external_exec_get()
    test_invalid_payload()
    test_command_chain()

    log("=== Test Suite Complete: External API layer is healthy ===", level="OK")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        log("Fatal unhandled exception in main()", level="ERROR")
        log_exception("Fatal stack trace")
        sys.exit(1)
