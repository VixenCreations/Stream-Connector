#!/usr/bin/env python3

# ────────────────────────────────────────────────────────────────────────────────
# Stream Connector - Glass-Dark Edition
# Version: 6.3.0 (2025-06-08)
# Date Updated: 01-23-2026
# ────────────────────────────────────────────────────────────────────────────────

# ───────────────────────────── standard-library imports
import asyncio
import functools
import glob
import itertools
import copy
import json
import os
import pathlib
from pathlib import Path
import queue
import random
import subprocess
import re
import shutil
import socket
import ssl
import sys
import threading
import time
import traceback
import inspect
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, Sequence
import urllib.request  # Required for fetch_parameters()
from tempfile import NamedTemporaryFile
import webbrowser
import errno
import signal

# ───────────────────────────── Threading Imports
import concurrent.futures
from collections import deque
from concurrent.futures import ThreadPoolExecutor

# Special capture buffer: high-rate support (~5 seconds)
capture_buffer = deque(maxlen=5000)

from queue import PriorityQueue
from threading import Lock, RLock
EXEC = ThreadPoolExecutor(max_workers=6)

# ───────────────────────────── tkinter imports
import tkinter
import tkinter as tk
import tkinter.ttk as ttk
from tkinter import filedialog as fd
from tkinter import messagebox
from tkinter import scrolledtext
from tkinter import simpledialog
from tkinter import Canvas
from tkinter.ttk import Spinbox  # (Py >= 3.11 - falls back to tk.Spinbox below)

# ───────────────────────────── third-party imports
import flask
import requests
import websockets
from TikTokLive import TikTokLiveClient
from TikTokLive import events
from flask import jsonify, request, Response, abort
from pythonosc import dispatcher, osc_server, udp_client
from zeroconf import ServiceBrowser, ServiceInfo, Zeroconf
from http.server import BaseHTTPRequestHandler, HTTPServer
from pythonosc.udp_client import SimpleUDPClient

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ───────────────────────────── Cryptography imports
import platform
import hashlib
import difflib
from difflib import SequenceMatcher
from hashlib import sha256
from hashlib import blake2s

# ─── branding ──────────────────────────────────────────────────────────────
APP_NAME = "Stream Connector"
APP_VERSION = "6.3.0"

def resolve_runtime_path(rel: str) -> str:
    """
    Resolves paths correctly for:
    - PyInstaller (frozen)
    - Normal Python execution
    - Portable mode

    NEVER use this for writable paths.
    """
    if hasattr(sys, "_MEIPASS"):
        base = sys._MEIPASS  # PyInstaller temp dir
    else:
        base = os.path.dirname(os.path.abspath(sys.argv[0]))

    return os.path.normpath(os.path.join(base, rel))


# ─────────────────────────────────────────── saved/ scaffold (clean-only)
def initialize_saved_structure():
    base = Path.cwd() / "saved"
    structure = [
        base,
        base / "changelogs",
        base / "config",
        base / "config" / "devices",
        base / "config" / "filters",
        base / "config" / "routing",
        base / "config" / "userdata",
        base / "controls",
        base / "controls" / "avatarControls",
        base / "controls" / "avatarControls" / "backup",
        base / "controls" / "backup",
        base / "controls" / "chains",
        base / "controls" / "chains" / "backup",
        base / "controls" / "export",
        base / "controls" / "owo",
        base / "logs",
        base / "logs" / "core",
        base / "logs" / "controls",
        base / "logs" / "errors",
        base / "logs" / "haptics",
        base / "logs" / "webhook",
    ]

    for d in structure:
        d.mkdir(parents=True, exist_ok=True)

    # Clean log contents but keep directories
    logs_base = base / "logs"
    for log_dir in logs_base.rglob("*"):
        if log_dir.is_dir():
            for f in log_dir.glob("*"):
                try:
                    if f.is_file():
                        f.unlink()
                except Exception:
                    pass

# ─── kick-off early in runtime
initialize_saved_structure()

# ─────────────────────────────────────────────────────────── 0. constants
DEBUG_CAPTURE = False  # Set to True to save capture logs (before/after)
OSC_LOG_TO_CONSOLE = False  # Silence the spam!

OSC_CFG_DIR = "saved/config/routing"
OSC_CFG_FILE = os.path.join(OSC_CFG_DIR, "osc_config.json")

SAVE_DIR = "saved"
CFG_CTLS_DIR = "saved/controls/"
CFG_DIR = "saved/config"
AVTR_CFG_CTLS_DIR = "saved/controls/avatarControls/"
AVTR_CHAIN_CFG_CTLS_DIR = "saved/controls/chains/"
CTRL_CFG = os.path.join(AVTR_CFG_CTLS_DIR, "controls.json")
CHAINS_FILE = os.path.join(AVTR_CHAIN_CFG_CTLS_DIR, "chains.json")
USER_CFG_FILE = os.path.join(CFG_DIR, "user.json")
PISHOCK_DEVICE_FILE = os.path.join(CFG_DIR, "devices", "devices.json")
EXPORT_PATH = os.path.join(CFG_CTLS_DIR, "export", "exported_chains.json")

DEFAULT_USER_CFG = {
    "tiktok_username": "",
    "pishock_username": "",
    "pishock_apikey": "",
}

# --- LOAD FUNCTION ---
def load_user_cfg() -> Dict[str, Any]:
    try:
        if not os.path.exists(USER_CFG_FILE):
            print(f"[WARN] Config file missing, creating: {USER_CFG_FILE}")

            # Seed with defaults
            with open(USER_CFG_FILE, "w", encoding="utf-8") as fp:
                json.dump(DEFAULT_USER_CFG, fp, indent=2)

            return dict(DEFAULT_USER_CFG)

        with open(USER_CFG_FILE, "r", encoding="utf-8") as fp:
            data = json.load(fp)

        # Ensure required keys exist (forward compatible)
        mutated = False
        for k, v in DEFAULT_USER_CFG.items():
            if k not in data:
                data[k] = v
                mutated = True

        if mutated:
            print("[INFO] Config updated with missing default keys")
            save_user_cfg(data)

        print(f"[DEBUG] Loaded config from {USER_CFG_FILE}")
        return data

    except Exception as e:
        print(f"[ERROR] Failed to load config, falling back to defaults: {e}")
        return dict(DEFAULT_USER_CFG)

# --- SAVE FUNCTION ---
def save_user_cfg(cfg: Dict[str, Any]) -> None:
    with open(USER_CFG_FILE, "w", encoding="utf-8") as fp:
        json.dump(cfg, fp, indent=2)

# --- LOAD CONFIG INTO GLOBAL ---
user_cfg: Dict[str, Any] = load_user_cfg()

# --- OPTIONAL CONVENIENCE ALIAS ---
TIKTOK_USER: str = user_cfg.get("tiktok_username", "").strip()

AVATAR_PREFIX: str = "/avatar/parameters/"
AVATAR_STATE_DIR = os.path.join(SAVE_DIR, "config")
AVATAR_STATE_FILE = os.path.join(AVATAR_STATE_DIR, "current_avatar.json")

q: "queue.Queue[tuple[str, str, Dict[str, Any]]]" = queue.Queue()

# somewhere central (only once!)
event_dispatch_queue = queue.Queue()

# Skip these parameters (because they are always changing in VRChat)
NOISY_PARAMETERS = {
    "/avatar/parameters/Upright",
    "/avatar/parameters/AngularY",
    "/avatar/parameters/VRMode",
    "/avatar/parameters/Voice"
}

# ── Hyroe “TikTok-to-OSC” gift timing table (path ➜ delay-seconds)
HYROE_GIFTS: dict[str, int] = {
    # present in avatar + explicit delay in Hyroe config
    "/avatar/parameters/Gifts/Galaxy":            7,
    "/avatar/parameters/Gifts/Heart_Me":          2,
    "/avatar/parameters/Gifts/Doughnut":          7,
    "/avatar/parameters/Gifts/Fireworks":         7,
    "/avatar/parameters/Gifts/Hat_and_Mustache":  7,
    "/avatar/parameters/Gifts/Sunglasses":        7,
    "/avatar/parameters/Gifts/Rose":              6,
    "/avatar/parameters/Gifts/Tiny_Diny":         7,
    "/avatar/parameters/Gifts/Game_Controller":   7,
    "/avatar/parameters/Gifts/Lightning_Bolt":    8,
    "/avatar/parameters/Gifts/Finger_Heart":      2,
    "/avatar/parameters/Gifts/Ghost":             7,
    "/avatar/parameters/Gifts/Pumpkin":           7,
    # present in avatar but *not* in Hyroe list → fallback = 6 s
    "/avatar/parameters/Gifts/Dog_Bone":          6,
    "/avatar/parameters/Gifts/Money_Gun":         6,   # matches “Money Gun”
    "/avatar/parameters/Gifts/Ice_Cream":         6,   # matches “Ice Cream Cone”
    "/avatar/parameters/Gifts/Corn":              6,
    "/avatar/parameters/Gifts/Boop":              1,   # this is Hyroe’s command; keep 1 s
}

# ── Fooma "Twitch-to-OSC" INT Series Table
INT_ROOT_ADDR   = "/avatar/parameters/twitch"   # Hyroe INT series root
INT_STEPS_COUNT = 25
INT_TIMER_DEF   = 1.0
CHAIN_COLS = 2  # number of columns for chain cards

# ─────────────────────────────────────────────────────────── OWO Support
OWO_PRESETS = {
    "default":     {"sensationId": 42, "intensity": 80,  "duration": 1000},
    "light_buzz":  {"sensationId": 12, "intensity": 40,  "duration": 500},
    "heavy_shock": {"sensationId": 99, "intensity": 100, "duration": 2000},
}

# ─────────────────────────────────────────────
# GLOBAL LICENSE STATE (LEGACY-COMPAT)
# ─────────────────────────────────────────────
LICENSE_TIER = "gold"

ADVANCED_MODES_ENABLED = True
PATTERN_EDITOR_ENABLED = True
FALLBACK_V3_ALLOWED = True
OWO_ENABLED = True

# ─────────────────────────────────────────────────────────── 1. Global Logger
SHOW_GUI_POPUPS = False

LOG_LEVELS = {
    "TRACE": 10,
    "DEBUG": 20,
    "INFO": 30,
    "WARN": 40,
    "ERROR": 50,
    "CRITICAL": 60,
}

CURRENT_LOG_LEVEL = LOG_LEVELS["TRACE"]

def _resolve_log_path() -> Path:
    import inspect
    stack = inspect.stack()
    if len(stack) < 3:
        module_name = "core"
    else:
        caller_file = stack[2].filename
        module_name = Path(caller_file).stem.lower().replace(" ", "_")

    log_dir = Path("saved") / "logs" / module_name
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"{module_name}-debug.log"

LOG_PATH = _resolve_log_path()

def log_verbose(
    message: str,
    data: Optional[dict] = None,
    level: str = "DEBUG",
    exc: Optional[BaseException] = None,
):
    level = level.upper()
    level_code = LOG_LEVELS.get(level, LOG_LEVELS["DEBUG"])
    if level_code < CURRENT_LOG_LEVEL:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tag = f"[{level:<8}]"
    lines = [f"{timestamp} {tag} {message}"]

    if data:
        try:
            json_payload = json.dumps(data, indent=2, ensure_ascii=False)
            lines.append(json_payload)
        except Exception as e:
            lines.append(f"[WARN] Failed to serialize payload: {e}")

    if exc:
        trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        lines.append(f"[EXCEPTION]\n{trace}")

    full_message = "\n".join(lines)

    # Write to stdout and log file
    print(full_message)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(full_message + "\n")

    # Show popup for ERROR or CRITICAL
    if SHOW_GUI_POPUPS and level_code >= LOG_LEVELS["ERROR"]:
        try:
            root = tkinter.Tk()
            root.withdraw()  # Hide root window
            messagebox.showerror(
                title="Stream Connector Error",
                message=f"{message}\n\nPlease contact the developer.\n\n"
                        f"Details logged to: {LOG_PATH}"
            )
            root.destroy()
        except Exception as popup_err:
            print("[WARN] Failed to show error popup:", popup_err)
            

# ─────────────────────────────────────────────────────────── 1.1 Global Gui Logger
GUI_LOG_PATH = Path("saved/logs/core/gui.log")
GUI_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

def log_gui_action(
    message: str,
    data: Optional[dict] = None,
    level: str = "DEBUG",
    exc: Optional[BaseException] = None,
):
    level = level.upper()
    level_code = LOG_LEVELS.get(level, LOG_LEVELS["DEBUG"])
    if level_code < CURRENT_LOG_LEVEL:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tag = f"[{level:<8}]"
    lines = [f"{timestamp} {tag} {message}"]

    if data:
        try:
            json_payload = json.dumps(data, indent=2, ensure_ascii=False)
            lines.append(json_payload)
        except Exception as e:
            lines.append(f"[WARN] Failed to serialize payload: {e}")

    if exc:
        trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        lines.append(f"[EXCEPTION]\n{trace}")

    full_message = "\n".join(lines)

    print(full_message)
    with GUI_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(full_message + "\n")

    if SHOW_GUI_POPUPS and level_code >= LOG_LEVELS["ERROR"]:
        try:
            root = tkinter.Tk()
            root.withdraw()
            messagebox.showerror(
                title="UI Error",
                message=f"{message}\n\nDetails logged to: {GUI_LOG_PATH}"
            )
            root.destroy()
        except Exception as popup_err:
            print("[WARN] Failed to show GUI error popup:", popup_err)
            
# ─────────────────────────────────────────────────────────── 1.2 Chain System Logger
CHAIN_SYSTEM_LOG_PATH = Path("saved/logs/controls/chain_system.log")
CHAIN_SYSTEM_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

def log_chain_system(
    message: str,
    data: Optional[dict] = None,
    level: str = "DEBUG",
    action_type: str = "runner",  # 'runner' or 'editor'
    exc: Optional[BaseException] = None,
):
    level = level.upper()
    action_type = action_type.lower()
    level_code = LOG_LEVELS.get(level, LOG_LEVELS["DEBUG"])

    if level_code < CURRENT_LOG_LEVEL:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tag = f"[{level:<8}] [chain:{action_type}]"
    lines = [f"{timestamp} {tag} {message}"]

    if data:
        try:
            json_payload = json.dumps(data, indent=2, ensure_ascii=False)
            lines.append(json_payload)
        except Exception as e:
            lines.append(f"[WARN] Failed to serialize payload: {e}")

    if exc:
        trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        lines.append(f"[EXCEPTION]\n{trace}")

    full_message = "\n".join(lines)

    print(full_message)
    with CHAIN_SYSTEM_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(full_message + "\n")

    if SHOW_GUI_POPUPS and level_code >= LOG_LEVELS["ERROR"]:
        try:
            root = tkinter.Tk()
            root.withdraw()
            messagebox.showerror(
                title="Chain System Error",
                message=f"{message}\n\nDetails logged to: {CHAIN_SYSTEM_LOG_PATH}"
            )
            root.destroy()
        except Exception as popup_err:
            print("[WARN] Failed to show chain error popup:", popup_err)

# ─────────────────────────────────────────────────────────── 1.3 OSC Core Logger
OSC_CORE_LOG_PATH = Path("saved/logs/controls/osc_core.log")
OSC_CORE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

def log_osc_core(
    message: str,
    data: Optional[dict] = None,
    level: str = "DEBUG",
    action_type: str = "bridge",  # e.g. 'bridge', 'send', 'receive', 'init'
    exc: Optional[BaseException] = None,
):
    level = level.upper()
    action_type = action_type.lower()
    level_code = LOG_LEVELS.get(level, LOG_LEVELS["DEBUG"])

    if level_code < CURRENT_LOG_LEVEL:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tag = f"[{level:<8}] [osc:{action_type}]"
    lines = [f"{timestamp} {tag} {message}"]

    if data:
        try:
            json_payload = json.dumps(data, indent=2, ensure_ascii=False)
            lines.append(json_payload)
        except Exception as e:
            lines.append(f"[WARN] Failed to serialize payload: {e}")

    if exc:
        trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        lines.append(f"[EXCEPTION]\n{trace}")

    full_message = "\n".join(lines)

    print(full_message)
    with OSC_CORE_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(full_message + "\n")

    if SHOW_GUI_POPUPS and level_code >= LOG_LEVELS["ERROR"]:
        try:
            root = tkinter.Tk()
            root.withdraw()
            messagebox.showerror(
                title="OSC Core Error",
                message=f"{message}\n\nDetails logged to: {OSC_CORE_LOG_PATH}"
            )
            root.destroy()
        except Exception as popup_err:
            print("[WARN] Failed to show OSC error popup:", popup_err)
            
# ─────────────────────────────────────────────────────────── 1.4 Controls Logger
CONTROLS_LOG_PATH = Path("saved/logs/controls/control_actions.log")
CONTROLS_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

def log_controls_action(
    message: str,
    data: Optional[dict] = None,
    level: str = "DEBUG",
    action_type: str = "config",  # e.g., 'config', 'update', 'delete', 'load'
    exc: Optional[BaseException] = None,
):
    level = level.upper()
    action_type = action_type.lower()
    level_code = LOG_LEVELS.get(level, LOG_LEVELS["DEBUG"])

    if level_code < CURRENT_LOG_LEVEL:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tag = f"[{level:<8}] [controls:{action_type}]"
    lines = [f"{timestamp} {tag} {message}"]

    if data:
        try:
            json_payload = json.dumps(data, indent=2, ensure_ascii=False)
            lines.append(json_payload)
        except Exception as e:
            lines.append(f"[WARN] Failed to serialize payload: {e}")

    if exc:
        trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        lines.append(f"[EXCEPTION]\n{trace}")

    full_message = "\n".join(lines)

    print(full_message)
    with CONTROLS_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(full_message + "\n")

    if SHOW_GUI_POPUPS and level_code >= LOG_LEVELS["ERROR"]:
        try:
            root = tkinter.Tk()
            root.withdraw()
            messagebox.showerror(
                title="Controls Error",
                message=f"{message}\n\nDetails logged to: {CONTROLS_LOG_PATH}"
            )
            root.destroy()
        except Exception as popup_err:
            print("[WARN] Failed to show Controls error popup:", popup_err)

# ───────────────────────────────────────────────────────────
# OWO Runtime Logger
# ───────────────────────────────────────────────────────────

OWO_LOG_PATH = Path("saved/logs/haptics/owoRuntime.log")
OWO_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

def log_owo(
    message: str,
    data: Optional[dict] = None,
    level: str = "DEBUG",
    action: str = "runtime",  # e.g. runtime | connect | send | template
    exc: Optional[BaseException] = None,
):
    """
    Structured logger for OWO runtime operations.
    Mirrors Chain System logger behavior.
    """

    level = level.upper()
    action = action.lower()
    level_code = LOG_LEVELS.get(level, LOG_LEVELS["DEBUG"])

    if level_code < CURRENT_LOG_LEVEL:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tag = f"[{level:<8}] [owo:{action}]"

    lines = [f"{timestamp} {tag} {message}"]

    # Structured data
    if data:
        try:
            payload = json.dumps(data, indent=2, ensure_ascii=False)
            lines.append(payload)
        except Exception as e:
            lines.append(f"[WARN] Failed to serialize payload: {e}")

    # Exception trace
    if exc:
        trace = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )
        lines.append("[EXCEPTION]")
        lines.append(trace)

    final = "\n".join(lines)

    # Console
    print(final)

    # File
    with OWO_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(final + "\n")

    # GUI popup for critical errors
    if SHOW_GUI_POPUPS and level_code >= LOG_LEVELS["ERROR"]:
        try:
            root = tkinter.Tk()
            root.withdraw()
            messagebox.showerror(
                title="OWO Runtime Error",
                message=f"{message}\n\nSee log:\n{OWO_LOG_PATH}"
            )
            root.destroy()
        except Exception as popup_err:
            print("[WARN] Failed to show OWO error popup:", popup_err)
            
# ─────────────────────────────────────────────────────────── Intiface Logger
INTIFACE_LOG_PATH = Path("saved/logs/haptics/intiface.log")
INTIFACE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

def log_intiface_action(
    message: str,
    data: Optional[dict] = None,
    level: str = "DEBUG",
    action_type: str = "runtime",  # e.g., 'connect', 'device', 'command', 'error'
    exc: Optional[BaseException] = None,
):
    level = level.upper()
    action_type = action_type.lower()
    level_code = LOG_LEVELS.get(level, LOG_LEVELS["DEBUG"])

    if level_code < CURRENT_LOG_LEVEL:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tag = f"[{level:<8}] [intiface:{action_type}]"
    lines = [f"{timestamp} {tag} {message}"]

    if data:
        try:
            json_payload = json.dumps(data, indent=2, ensure_ascii=False)
            lines.append(json_payload)
        except Exception as e:
            lines.append(f"[WARN] Failed to serialize payload: {e}")

    if exc:
        trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        lines.append(f"[EXCEPTION]\n{trace}")

    full_message = "\n".join(lines)

    print(full_message)
    with INTIFACE_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(full_message + "\n")

    if SHOW_GUI_POPUPS and level_code >= LOG_LEVELS["ERROR"]:
        try:
            root = tkinter.Tk()
            root.withdraw()
            messagebox.showerror(
                title="Intiface Error",
                message=f"{message}\n\nDetails logged to: {INTIFACE_LOG_PATH}"
            )
            root.destroy()
        except Exception as popup_err:
            print("[WARN] Failed to show Intiface error popup:", popup_err)

# ─────────────────────────────────────────────────────────────
# Stream Connector - External Hook Integration Layer (INLINE)
# Structured Logger Edition
# SAFE FOR SINGLE-FILE ARCHITECTURE
# ─────────────────────────────────────────────────────────────

_EXTERNAL_HOOKS: Dict[str, Dict[str, Any]] = {}
_EXTERNAL_LOCK = RLock()
_UI_DISPATCH: Optional[Callable[[Callable[[], None]], None]] = None


def _norm(val: str) -> str:
    return str(val or "").strip().lower()


def log_external(
    message: str,
    level: str = "INFO",
    data: dict | None = None,
    exc: Exception | None = None
):
    """
    Thin wrapper around global structured logger.
    Adds EXTERNAL scope tag.
    """
    payload = {"scope": "external"}
    if data:
        payload.update(data)

    log_verbose(
        message=message,
        data=payload,
        level=level,
        exc=exc
    )


def set_ui_dispatcher(fn: Callable[[Callable[[], None]], None]) -> None:
    global _UI_DISPATCH
    _UI_DISPATCH = fn
    log_external("UI dispatcher set", level="INFO")


def _safe_execute(fn: Callable[[], None]) -> None:
    try:
        fn()
    except Exception as exc:
        log_external("External hook execution error", level="ERROR", exc=exc)


def _run_async(fn: Callable[[], None]) -> None:
    # 1) Preferred: injected UI dispatcher (your app.after_idle)
    if _UI_DISPATCH:
        try:
            _UI_DISPATCH(lambda: _safe_execute(fn))
            return
        except Exception as exc:
            log_external("UI dispatcher failed, falling back", level="WARN", exc=exc)

    # 2) Tk fallback
    try:
        root = tk._default_root
        if root and root.winfo_exists():
            root.after_idle(lambda: _safe_execute(fn))
            return
    except Exception:
        pass

    # 3) ThreadPool fallback
    try:
        EXEC.submit(lambda: _safe_execute(fn))
        return
    except Exception as exc:
        log_external("ThreadPool submit failed", level="WARN", exc=exc)

    # 4) Hard fallback
    _safe_execute(fn)


def register_external_hook(
    provider: str,
    command_id: str,
    name: str,
    handler: Callable[[dict], None],
    description: str = "",
) -> None:
    pid = _norm(provider)
    cid = _norm(command_id)

    if not pid or not cid:
        raise ValueError("provider and command_id are required")

    key = f"{pid}:{cid}"

    with _EXTERNAL_LOCK:
        if key in _EXTERNAL_HOOKS:
            log_external(
                "Duplicate external hook ignored",
                level="WARN",
                data={"provider": pid, "commandId": cid}
            )
            return

        _EXTERNAL_HOOKS[key] = {
            "provider": pid,
            "commandId": cid,
            "name": name.strip() or command_id,
            "description": description.strip(),
            "handler": handler,
        }

    log_external(
        "Registered external hook",
        level="INFO",
        data={
            "provider": pid,
            "commandId": cid,
            "name": name.strip() or command_id,
        }
    )


def unregister_external_hook(provider: str, command_id: str) -> None:
    pid = _norm(provider)
    cid = _norm(command_id)
    key = f"{pid}:{cid}"

    with _EXTERNAL_LOCK:
        removed = _EXTERNAL_HOOKS.pop(key, None)

    if removed:
        log_external(
            "Unregistered external hook",
            level="INFO",
            data={"provider": pid, "commandId": cid}
        )
    else:
        log_external(
            "Attempted to unregister unknown hook",
            level="WARN",
            data={"provider": pid, "commandId": cid}
        )


def list_external_hooks() -> list[dict]:
    with _EXTERNAL_LOCK:
        return [
            {
                "provider": v["provider"],
                "commandId": v["commandId"],
                "name": v["name"],
                "description": v["description"],
            }
            for v in _EXTERNAL_HOOKS.values()
        ]


def execute_external_hook(provider: str, command_id: str, context: dict | None = None) -> bool:
    pid = _norm(provider)
    cid = _norm(command_id)
    key = f"{pid}:{cid}"
    ctx = context or {}

    with _EXTERNAL_LOCK:
        entry = _EXTERNAL_HOOKS.get(key)

    if not entry:
        log_external(
            "Unknown external hook",
            level="WARN",
            data={"provider": pid, "commandId": cid}
        )
        return False

    def _runner():
        log_external(
            "Executing external hook",
            level="DEBUG",
            data={
                "provider": pid,
                "commandId": cid,
                "context": ctx,
            }
        )
        entry["handler"](ctx)

    _run_async(_runner)
    return True


def auto_register_chains_as_external_hooks(app):
    """
    Automatically expose every chain as an external hook.
    Provider: "streamconnector"
    CommandId: chain name (normalized)
    """

    try:
        chains = load_chains()
        log_external("Loaded chains for external registration", level="INFO", data={"count": len(chains)})
    except Exception as e:
        log_external("Failed to load chains for external registration", level="ERROR", exc=e)
        return

    for chain in chains:
        try:
            name = chain.get("name", "").strip()
            if not name:
                continue

            command_id = _norm(name)

            def make_handler(chain_def):
                def _handler(ctx):
                    log_external(
                        "External chain trigger",
                        level="INFO",
                        data={
                            "chain": chain_def.get("name"),
                            "context": ctx,
                        }
                    )

                    payload = (
                        chain_def["steps"],
                        chain_def.get("delay", 0.025),
                        chain_def.get("reset_before", False),
                        chain_def.get("reset_after", False),
                        {**chain_def, "external_context": ctx},
                    )

                    app.chain_queue.put((
                        chain_def.get("priority", 100),
                        time.time(),
                        payload
                    ))
                    app._try_next_chain()
                return _handler

            register_external_hook(
                provider="streamconnector",
                command_id=command_id,
                name=name,
                handler=make_handler(chain),
                description=f"Chain: {name}"
            )

        except Exception as e:
            log_external(
                "Failed to register chain as external hook",
                level="ERROR",
                data={"chain": chain.get("name")},
                exc=e
            )

            
# ── Flask glue ────────────────────────────────────────────────────────────
app: flask.Flask = flask.Flask(__name__)
RLOCK: Lock = Lock()
EXEC = concurrent.futures.ThreadPoolExecutor()

# ── Scoped Logger for Webhook ────────────────────────────────────────────
def _resolve_log_path() -> Path:
    log_dir = Path("saved") / "logs" / "webhook"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "webhook-debug.log"

WEBHOOK_LOG_PATH = _resolve_log_path()

def log_webhook(
    message: str,
    data: Optional[dict] = None,
    level: str = "DEBUG",
    exc: Optional[BaseException] = None,
):
    from datetime import datetime

    level = level.upper()
    level_code = LOG_LEVELS.get(level, LOG_LEVELS["DEBUG"])
    if level_code < CURRENT_LOG_LEVEL:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tag = f"[{level:<8}]"
    lines = [f"{timestamp} {tag} {message}"]

    if data:
        try:
            json_payload = json.dumps(data, indent=2, ensure_ascii=False)
            lines.append(json_payload)
        except Exception as e:
            lines.append(f"[WARN] Failed to serialize payload: {e}")

    if exc:
        trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        lines.append(f"[EXCEPTION]\n{trace}")

    full_message = "\n".join(lines)

    print(full_message)
    with WEBHOOK_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(full_message + "\n")
        
# ── Scoped Logger for External Hooks ──────────────────────────────────────

def _resolve_external_log_path() -> Path:
    log_dir = Path("saved") / "logs" / "webhook"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "external-hooks.log"

EXTERNAL_LOG_PATH = _resolve_external_log_path()


def log_external(
    message: str,
    data: Optional[dict] = None,
    level: str = "INFO",
    exc: Optional[BaseException] = None,
):
    from datetime import datetime

    level = level.upper()
    level_code = LOG_LEVELS.get(level, LOG_LEVELS["DEBUG"])
    if level_code < CURRENT_LOG_LEVEL:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tag = f"[{level:<8}]"
    prefix = "[EXTERNAL HOOKS]"

    lines = [f"{timestamp} {prefix} {tag} {message}"]

    if data:
        try:
            json_payload = json.dumps(data, indent=2, ensure_ascii=False)
            lines.append(json_payload)
        except Exception as e:
            lines.append(f"[WARN] Failed to serialize payload: {e}")

    if exc:
        trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        lines.append(f"[EXCEPTION]\n{trace}")

    full_message = "\n".join(lines)

    # Console
    print(full_message)

    # File
    with EXTERNAL_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(full_message + "\n")


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

DOCS_PORT = 8899
DOCS_ROOT = Path("saved/Docs").resolve()
DOCS_ROOT.mkdir(parents=True, exist_ok=True)

docs_app = flask.Flask("stream_connector_docs", static_folder=None)

_ALLOWED_EXT = {".html", ".css", ".js", ".map", ".txt"}

def _load_doc_file(name: str) -> Optional[tuple[str, str]]:
    """
    Returns (content, mimetype) or None.
    Hard blocks traversal and only serves known safe extensions.
    """
    try:
        path = (DOCS_ROOT / name).resolve()

        # traversal block
        if not str(path).startswith(str(DOCS_ROOT)):
            return None

        if not path.exists() or not path.is_file():
            return None

        ext = path.suffix.lower()
        if ext not in _ALLOWED_EXT:
            return None

        # small, safe mime map
        mimetype = "text/plain"
        if ext == ".html":
            mimetype = "text/html"
        elif ext == ".css":
            mimetype = "text/css"
        elif ext == ".js":
            mimetype = "application/javascript"
        elif ext == ".map":
            mimetype = "application/json"

        return (path.read_text(encoding="utf-8", errors="replace"), mimetype)
    except Exception:
        return None

@docs_app.route("/")
def docs_index():
    hit = _load_doc_file("index.html")
    if not hit:
        abort(404)
    content, mimetype = hit
    return Response(content, mimetype=mimetype)

@docs_app.route("/<path:filename>")
def docs_file(filename: str):
    if filename == "favicon.ico":
        return "", 204

    hit = _load_doc_file(filename)
    if not hit:
        # keep this log if you want
        # log_docs("Docs file not found", level="WARN", data={"file": filename})
        abort(404)

    content, mimetype = hit
    return Response(content, mimetype=mimetype)

def run_docs_server():
    # log_docs("Docs server starting", data={"port": DOCS_PORT, "root": str(DOCS_ROOT)})
    docs_app.run(
        host="127.0.0.1",
        port=DOCS_PORT,
        debug=False,
        threaded=False,      # less “server-ish”
        use_reloader=False
    )

def start_docs_server():
    t = threading.Thread(target=run_docs_server, name="DocsServer", daemon=True)
    t.start()
    
# ── CORS ─────────────────────────────────────────────────────────────────
def _cors(resp: flask.Response) -> flask.Response:
    hdr = resp.headers
    hdr["Access-Control-Allow-Origin"] = "*"
    hdr["Access-Control-Allow-Headers"] = "*"
    hdr["Access-Control-Allow-Methods"] = "*"
    return resp

@app.route("/<path:anything>", methods=["OPTIONS"])
def _options(anything: str) -> Response:
    return _cors(Response())

@app.after_request
def after_request(resp: flask.Response):
    return _cors(resp)

@app.route("/api/app/info")
def api_info():
    return _cors(
        jsonify({
            "data": {"author": "@Vixenlicious", "name": APP_NAME, "version": "6.3.0"}
        })
    )

def _natkey(text: str) -> List[Any]:
    parts = []
    for chunk in re.split(r"(\d+)", text):
        parts.append(int(chunk) if chunk.isdigit() else chunk.lower())
    return parts

@app.route("/api/features/categories")
def api_categories():
    seen = set()
    rows = []
    with RLOCK:
        for e in REGISTRY:
            raw_cid   = str(e.get("categoryId", "")).strip()
            raw_name  = str(e.get("categoryName", "")).strip()

            # chains stays as-is; everything else gets collapsed
            if raw_cid == "chains":
                cid, cname = raw_cid, raw_name
            else:
                cid, cname = "avatar_parameters", "Avatar Parameters"

            # only add each one once
            if cid not in seen:
                seen.add(cid)
                rows.append({"categoryId": cid, "categoryName": cname})

    # optional: sort so "chains" appears before "parameters"
    rows.sort(key=lambda x: x["categoryId"] != "chains")

    log_webhook("Served grouped categories API", data={"count": len(rows)}, level="DEBUG")
    return _cors(jsonify({"data": rows}))

@app.route("/api/features/actions")
def api_actions():
    cid = request.args.get("categoryId")

    with RLOCK:
        if cid == "chains":
            # only real chain entries
            entries = [e for e in REGISTRY if e["categoryId"] == "chains"]
        else:
            # EVERY other action (i.e. "parameters")
            entries = [e for e in REGISTRY if e["categoryId"] != "chains"]

        rows = []
        for e in entries:
            # if we're in the parameters bucket, force the full OSC path
            if cid != "chains":
                # e["actionName"] *is* the full path (we set it this way in register_action)
                name = e["actionName"]
                # just in case somebody slipped in a leaf-only name, guaranteed slash
                if not name.startswith("/"):
                    name = f"/avatar/parameters/{name}"
            else:
                # chains keep their friendly name
                name = e["actionName"]

            rows.append({
                "actionId":   e["actionId"],
                "actionName": name
            })

    # sort naturally (numbers in the path will sort in numeric order)
    rows.sort(key=lambda x: _natkey(x["actionName"]))

    log_webhook(
        "Served grouped actions API",
        data={"categoryId": cid, "count": len(rows)},
        level="DEBUG"
    )
    return _cors(jsonify({"data": rows}))

@app.route("/api/features/actions/exec", methods=["POST"])
def api_exec() -> flask.Response:
    body = request.get_json(force=True, silent=True) or {}

    cid = _norm(body.get("categoryId", ""))
    aid = _norm(body.get("actionId", ""))
    ctx = body.get("context") or body.get("data") or {}

    with RLOCK:
        action = next((row for row in REGISTRY if row["categoryId"] == cid and row["actionId"] == aid), None)

    if action is None:
        log_webhook("Unknown action requested", data={"categoryId": cid, "actionId": aid}, level="WARN")
        return _cors(jsonify({"message": f"Unknown action-id: {aid}"})), 400

    def _run_action() -> None:
        try:
            log_webhook("Executing action", data={"categoryId": cid, "actionId": aid, "context": ctx}, level="INFO")
            action["func"](ctx)
        except Exception as exc:
            log_webhook(f"Error in action: {cid}/{aid}", data=ctx, level="ERROR", exc=exc)

    try:
        import tkinter as tk
        root = getattr(tk, "_default_root", None)
        if root and root.winfo_exists():
            root.after_idle(_run_action)
        else:
            EXEC.submit(_run_action)
    except Exception as exc:
        log_webhook("Failed to enqueue action", data={"categoryId": cid, "actionId": aid}, level="ERROR", exc=exc)
        EXEC.submit(_run_action)

    return _cors(jsonify({"data": []}))
    

def run_flask() -> None:
    log_webhook("Starting Flask server", level="INFO")
    app.run(host="127.0.0.1", port=8832, threaded=True)

# ─────────────────────────────────────────────────────────────
# External Integration Flask Server (Dedicated Port)
# ─────────────────────────────────────────────────────────────

external_app: flask.Flask = flask.Flask("external_api")

# Reuse same CORS helper
@external_app.route("/<path:anything>", methods=["OPTIONS"])
def _external_options(anything: str) -> Response:
    return _cors(Response())

@external_app.after_request
def _external_after_request(resp: flask.Response):
    return _cors(resp)

@external_app.route("/api/external/info", methods=["GET"])
def external_info():
    return _cors(jsonify({
        "data": {
            "name": "Stream Connector External API",
            "author": "@Vixenlicious",
            "version": APP_VERSION
        }
    }))

@external_app.route("/api/external/list", methods=["GET"])
def external_list():
    return _cors(jsonify({
        "data": list_external_hooks()
    }))

@external_app.route("/api/external/exec", methods=["POST", "GET"])
def external_exec():
    try:
        # ── 1) Try JSON body (normal case)
        body = request.get_json(force=True, silent=True) or {}

        # ── 2) Fallback to form data
        if not body:
            body = request.form.to_dict() or {}

        # ── 3) Fallback to query params (GET)
        if not body:
            body = request.args.to_dict() or {}

        provider   = body.get("provider", "")
        command_id = body.get("commandId") or body.get("command_id") or body.get("command", "")
        context    = body.get("context", {})

        # Allow context to be JSON string
        if isinstance(context, str):
            try:
                context = json.loads(context)
            except Exception:
                context = {"raw": context}

        if not provider or not command_id:
            log_external(
                "External exec missing provider/commandId",
                level="WARN",
                data={"body": body}
            )
            return _cors(jsonify({
                "ok": False,
                "error": "provider and commandId required"
            })), 200   # ← important: do NOT 400 for bot tools

        log_external(
            "External exec request",
            data={"provider": provider, "commandId": command_id, "context": context}
        )

        ok = execute_external_hook(provider, command_id, context)

        return _cors(jsonify({
            "ok": ok
        }))

    except Exception as exc:
        log_external("Fatal error in external_exec", level="ERROR", exc=exc)
        return _cors(jsonify({
            "ok": False,
            "error": "internal error"
        })), 200


def run_external_flask() -> None:
    log_webhook("Starting External Flask server", level="INFO")
    external_app.run(host="127.0.0.1", port=8840, threaded=True)
    

# ─────────────────────────────────────────────────────────────
# External Integration Flask Server (Dedicated Port)
# ─────────────────────────────────────────────────────────────

from dataclasses import dataclass

@dataclass
class BaseEvent:
    provider: str
    command_id: str
    timestamp: Optional[str]
    raw: Dict[str, Any]


@dataclass
class ExternalHookEvent(BaseEvent):
    user: Optional[str]
    message: Optional[str]
    source: Optional[str]


@dataclass
class TwitchChatEvent(BaseEvent):
    user: str
    message: str


@dataclass
class TwitchSubEvent(BaseEvent):
    user: str
    tier: str
    months: int
    is_gift: bool
    gifter: Optional[str]


@dataclass
class TwitchCheerEvent(BaseEvent):
    user: str
    bits: int
    message: Optional[str]


@dataclass
class ObsSceneEvent(BaseEvent):
    scene: str


@dataclass
class TimerEvent(BaseEvent):
    timer: str


@dataclass
class ButtonEvent(BaseEvent):
    button: str
    
def _safe_int(val, default=0):
    try:
        return int(val)
    except Exception:
        return default


def _safe_bool(val):
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("1", "true", "yes", "y", "on")
    return False


def parse_external_hook(data: dict) -> ExternalHookEvent:
    ctx = data.get("context") or {}
    return ExternalHookEvent(
        provider=data.get("provider", ""),
        command_id=data.get("commandId", ""),
        timestamp=ctx.get("timestamp"),
        user=ctx.get("user"),
        message=ctx.get("message"),
        source=ctx.get("source"),
        raw=data
    )


def parse_twitch_chat(data: dict) -> TwitchChatEvent:
    ctx = data.get("context") or {}
    return TwitchChatEvent(
        provider=data.get("provider", ""),
        command_id=data.get("commandId", ""),
        timestamp=ctx.get("timestamp"),
        user=ctx.get("user", "unknown"),
        message=ctx.get("message", ""),
        raw=data
    )


def parse_twitch_sub(data: dict) -> TwitchSubEvent:
    ctx = data.get("context") or {}
    return TwitchSubEvent(
        provider=data.get("provider", ""),
        command_id=data.get("commandId", ""),
        timestamp=ctx.get("timestamp"),
        user=ctx.get("user", "unknown"),
        tier=ctx.get("tier", "unknown"),
        months=_safe_int(ctx.get("months", 1)),
        is_gift=_safe_bool(ctx.get("isGift")),
        gifter=ctx.get("gifter"),
        raw=data
    )


def parse_twitch_cheer(data: dict) -> TwitchCheerEvent:
    ctx = data.get("context") or {}
    return TwitchCheerEvent(
        provider=data.get("provider", ""),
        command_id=data.get("commandId", ""),
        timestamp=ctx.get("timestamp"),
        user=ctx.get("user", "unknown"),
        bits=_safe_int(ctx.get("bits", 0)),
        message=ctx.get("message"),
        raw=data
    )


def parse_obs_scene(data: dict) -> ObsSceneEvent:
    ctx = data.get("context") or {}
    return ObsSceneEvent(
        provider=data.get("provider", ""),
        command_id=data.get("commandId", ""),
        timestamp=ctx.get("timestamp"),
        scene=ctx.get("scene", "unknown"),
        raw=data
    )


def parse_timer(data: dict) -> TimerEvent:
    ctx = data.get("context") or {}
    return TimerEvent(
        provider=data.get("provider", ""),
        command_id=data.get("commandId", ""),
        timestamp=ctx.get("timestamp"),
        timer=ctx.get("timer", "unknown"),
        raw=data
    )


def parse_button(data: dict) -> ButtonEvent:
    ctx = data.get("context") or {}
    return ButtonEvent(
        provider=data.get("provider", ""),
        command_id=data.get("commandId", ""),
        timestamp=ctx.get("timestamp"),
        button=ctx.get("button", "unknown"),
        raw=data
    )

EVENT_PARSERS = {
    "external_hook": parse_external_hook,
    "twitch_chat": parse_twitch_chat,
    "twitch_sub": parse_twitch_sub,
    "twitch_cheer": parse_twitch_cheer,
    "obs_scene": parse_obs_scene,
    "timer": parse_timer,
    "button": parse_button,
}


def start_streamerbot_ws_listener(port=8080):
    """
    Streamer.bot 1.0.1 WebSocket listener
    - Correct endpoint
    - Correct subscribe shape
    - Typed event handling
    - Clean reconnect
    """

    import json
    import time
    import threading
    import websocket

    url = f"ws://127.0.0.1:{port}/"
    retry_delay = 2
    max_delay = 30

    stop_flag = {"stop": False}

    def safe_json(raw):
        try:
            return json.loads(raw)
        except Exception:
            return None

    def on_open(ws):
        nonlocal retry_delay
        retry_delay = 2
        log_external("Streamer.bot WS connected", level="INFO")

        subscribe_payload = {
            "request": "Subscribe",
            "id": "streamconnector-sub",
            "events": {
                "General": ["Custom"]
            }
        }

        try:
            ws.send(json.dumps(subscribe_payload))
            log_external("Streamer.bot subscription sent", data=subscribe_payload)
        except Exception as exc:
            log_external("Failed to send subscription", level="ERROR", exc=exc)

    def on_message(ws, message):
        msg = safe_json(message)
        if not isinstance(msg, dict):
            return

        # ── Subscription ACK ─────────────────────────────
        if msg.get("id") == "streamconnector-sub":
            if msg.get("status") == "ok":
                log_external("Streamer.bot subscription confirmed", level="INFO")
            else:
                log_external("Streamer.bot subscription failed", level="ERROR", data=msg)
            return

        event = msg.get("event")
        if not isinstance(event, dict):
            return

        # Streamer.bot 1.0.x mapping
        if event.get("source") != "General":
            return

        if event.get("type") != "Custom":
            return

        data = msg.get("data")
        if not isinstance(data, dict):
            return

        event_type = data.get("type")
        provider = str(data.get("provider", "")).strip()
        command_id = str(data.get("commandId", "")).strip()

        if not event_type or not provider or not command_id:
            log_external("Malformed event payload", level="WARN", data=data)
            return

        parser = EVENT_PARSERS.get(event_type)
        if not parser:
            log_external("Unhandled event type", level="DEBUG", data={"type": event_type})
            return

        try:
            typed_event = parser(data)
        except Exception as exc:
            log_external(
                "Event parsing failed",
                level="ERROR",
                data={"type": event_type, "error": str(exc)},
                exc=exc
            )
            return

        log_external(
            "Streamer.bot event received",
            data={
                "type": event_type,
                "provider": provider,
                "commandId": command_id,
                "parsed": typed_event.__dict__
            }
        )

        try:
            # You can now route on typed_event instead of raw dict
            execute_external_hook(
                typed_event.provider,
                typed_event.command_id,
                typed_event.raw.get("context") or {}
            )
        except Exception as exc:
            log_external(
                "Event execution failed",
                level="ERROR",
                data={
                    "type": event_type,
                    "provider": provider,
                    "commandId": command_id,
                    "error": str(exc)
                },
                exc=exc
            )

    def on_error(ws, error):
        log_external("Streamer.bot WS error", level="WARN", data={"error": str(error)})

    def on_close(ws, status_code, close_msg):
        log_external(
            "Streamer.bot WS closed",
            level="WARN",
            data={
                "status_code": status_code,
                "message": close_msg
            }
        )

    def run_loop():
        nonlocal retry_delay

        while not stop_flag["stop"]:
            try:
                ws = websocket.WebSocketApp(
                    url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close
                )

                ws.run_forever(
                    ping_interval=20,
                    ping_timeout=10,
                    reconnect=0
                )

            except Exception as exc:
                log_external("Streamer.bot WS crash", level="ERROR", exc=exc)

            if stop_flag["stop"]:
                break

            log_external(
                "Streamer.bot WS reconnecting",
                level="WARN",
                data={"retry_in_seconds": retry_delay}
            )

            time.sleep(retry_delay)
            retry_delay = min(max_delay, retry_delay * 2)

    threading.Thread(target=run_loop, daemon=True).start()
    return stop_flag


# ─── TikFinity Client Listener with Deferred I/O ────────────────────────
# guard & buffer
RLOCK = RLock()

# Dedicated logger for TikFinity I/O hook
def _resolve_tikfinity_hook_path() -> Path:
    log_dir = Path("saved") / "logs" / "webhook"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "tikfinity_hook.log"

TIKFINITY_HOOK_LOG_PATH = _resolve_tikfinity_hook_path()

def log_tikfinity_hook(
    message: str,
    data: Optional[dict] = None,
    level: str = "DEBUG",
    exc: Optional[BaseException] = None,
):
    level = level.upper()
    level_code = LOG_LEVELS.get(level, LOG_LEVELS["DEBUG"])
    if level_code < CURRENT_LOG_LEVEL:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tag = f"[{level:<8}]"
    lines = [f"{timestamp} {tag} {message}"]

    if data:
        try:
            json_payload = json.dumps(data, indent=2, ensure_ascii=False)
            lines.append(json_payload)
        except Exception as e:
            lines.append(f"[WARN] Failed to serialize payload: {e}")

    if exc:
        trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        lines.append(f"[EXCEPTION]\n{trace}")

    full_message = "\n".join(lines)

    print(full_message)
    with TIKFINITY_HOOK_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(full_message + "\n")

# ---------------------------------------------------------------------------  
# Registry-cache helpers (aligned save / load)  
# ---------------------------------------------------------------------------  

def _to_serializable(actions: set[tuple]) -> list[list]:
    """Convert set of tuples → list of plain lists for JSON encoding."""
    return [list(a) for a in actions]

def _hash_payload(payload) -> str:
    """Stable SHA-256 over a JSON-serialisable object."""
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
    return sha256(blob).hexdigest()

# ─────────────────────────────────────────────────────────────
# TikFinity Action Registry (HALF-LIVE, AUTHORITATIVE)
# ─────────────────────────────────────────────────────────────

REGISTRY: list[dict] = []
RLOCK = threading.Lock()

def _norm(x: str | int) -> str:
    return str(x).strip().lower()


def register_action(
    category_id: str,
    category_name: str,
    action_id: str | None,
    action_name: str | None,
    func: Callable,
) -> None:
    """
    Register a TikFinity action.

    HALF-LIVE MODEL:
    • No caching
    • No deferral
    • No reload lists
    • Registry reflects current runtime state only
    """

    # ─── Hard guards ────────────────────────────────────────
    if not action_id or not isinstance(action_id, str):
        log_verbose(
            "Skipping action registration (missing action_id)",
            level="DEBUG",
            data={
                "category_id": category_id,
                "category_name": category_name,
                "action_id": action_id,
                "action_name": action_name,
            }
        )
        return

    raw_aid = action_id.strip()
    if not raw_aid:
        return

    cid  = _norm(category_id)
    aid  = _norm(raw_aid)
    name = (action_name or raw_aid).strip()

    with RLOCK:
        # ─── Duplicate guard (STRICT) ───────────────────────
        for row in REGISTRY:
            if row["categoryId"] == cid and row["actionId"] == aid:
                # Update function in-place if changed
                if row["func"] is not func:
                    row["func"] = func
                    log_verbose(
                        "Action updated in-place",
                        data={"category": cid, "action": aid},
                        level="DEBUG",
                    )
                return

        # ─── Insert new action ─────────────────────────────
        REGISTRY.append({
            "categoryId":   cid,
            "categoryName": category_name.strip(),
            "actionId":     aid,
            "actionName":   name,
            "func":         func,
        })

    log_verbose(
        "Action registered",
        data={
            "category": cid,
            "action": aid,
            "name": name,
        },
        level="INFO",
    )

def unregister_action(aid: str, cid: str | None = None) -> None:
    """
    Remove an action from the live registry.

    HALF-LIVE MODEL:
    • No cache writes
    • No disk I/O
    • Immediate effect
    """

    if not aid:
        return

    aid = _norm(aid)
    cid = _norm(cid) if cid else None

    with RLOCK:
        before = len(REGISTRY)

        if cid:
            REGISTRY[:] = [
                r for r in REGISTRY
                if not (r["categoryId"] == cid and r["actionId"] == aid)
            ]
        else:
            REGISTRY[:] = [
                r for r in REGISTRY
                if r["actionId"] != aid
            ]

        removed = before - len(REGISTRY)

    if removed:
        log_verbose(
            "Action unregistered",
            data={
                "action_id": aid,
                "category_id": cid,
                "removed": removed,
            },
            level="INFO",
        )
    else:
        log_verbose(
            "Unregister requested but no matching action found",
            data={
                "action_id": aid,
                "category_id": cid,
            },
            level="DEBUG",
        )

def sync_tikfinity_registry(app) -> None:
    """
    GLOBAL REGISTRY SYNC - SINGLE SOURCE OF TRUTH

    • Rebuilds REGISTRY from:
        - app.rows        (Avatar Parameters)
        - loaded chains   (Chains category)
    • No caching
    • No deferral
    • Deterministic every time
    """

    rebuilt: list[dict] = []

    # ───────────────────────── Avatar Parameters ─────────────────────────
    for addr, row in app.rows.items():
        action_id = row.get("action_id")
        action_name = row.get("action_name")

        if not action_id or not isinstance(action_id, str):
            continue

        aid = _norm(action_id)
        cid = _norm(row.get("category_id", "avatar_parameters"))

        v = row["value_var"]
        t = row["timer_var"]
        send_addr = row["send_addr"]

        def _make_func(a=send_addr, v=v, t=t):
            return lambda _ctx=None: app.send_osc(
                a,
                app._normalize(v.get()),
                t.get()
            )

        rebuilt.append({
            "categoryId":   cid,
            "categoryName": row.get("category_name", "Avatar Parameters"),
            "actionId":     aid,
            "actionName":   action_name or action_id,
            "func":         _make_func(),
        })

    # ───────────────────────── Chains ─────────────────────────
    chains = load_chains()

    for chain in chains:
        name = chain.get("name", "").strip()
        if not name:
            continue

        trigger = str(chain.get("trigger", "manual")).lower()
        queue_mode = chain.get("queue_mode", True)

        def make_chain_runner(cfg):
            return lambda _ctx=None: app._run_chain(
                cfg.get("steps", []),
                cfg.get("delay", 0.025),
                cfg.get("reset_before", False),
                cfg.get("reset_after", False),
                cfg.get("queue_mode", True),
                cfg=cfg,
            )

        rebuilt.append({
            "categoryId":   "chains",
            "categoryName": "Chains",
            "actionId":     _norm(name),
            "actionName":   name,
            "func":         make_chain_runner(chain),
        })

    # ───────────────────────── Commit ─────────────────────────
    with RLOCK:
        REGISTRY.clear()
        REGISTRY.extend(rebuilt)

    log_verbose(
        "TikFinity registry globally synchronized",
        level="INFO",
        data={
            "avatar_actions": len(app.rows),
            "chains": len(chains),
            "total": len(rebuilt),
        }
    )

# ─── TikFinity Client Listener ────────────────────────
def tikfinity_listener_thread(self):  # called from Dash instance
    import websocket
    import json
    import time
    import threading
    from collections import defaultdict

    # ─── Ensure counters exist ────────────────────────────────
    if not hasattr(self, "_gift_counters"):
        self._gift_counters = defaultdict(int)

    if not hasattr(self, "_streak_counters"):
        self._streak_counters = defaultdict(int)

    if not hasattr(self, "_total_diamonds"):
        self._total_diamonds = 0

    # ─── TikFinity state tracking ─────────────────────────────
    if not hasattr(self, "_tikfinity_state"):
        self._tikfinity_state = {
            "connected": False,
            "last_heartbeat": 0.0,
            "reconnecting": False,
            "last_emit_kind": None,
            "last_emit_ts": 0.0,
        }

    # ─── Renderer state emitter (deduped) ────────────────────
    def _emit_state(kind: str, payload: dict | None = None, debounce_s: float = 1.0):
        payload = payload or {}
        now = time.time()

        last_kind = self._tikfinity_state.get("last_emit_kind")
        last_ts   = self._tikfinity_state.get("last_emit_ts", 0.0)

        if kind != last_kind or (now - last_ts) >= debounce_s:
            self._tikfinity_state["last_emit_kind"] = kind
            self._tikfinity_state["last_emit_ts"] = now
            q.put(("tikfinity", kind, payload))

    # ─── Helpers ─────────────────────────────────────────────
    def _normalize(name: str) -> str:
        return name.strip().lower()

    # ─── WebSocket handlers ──────────────────────────────────
    def on_message(ws, message):
        try:
            payload = json.loads(message)
            if not isinstance(payload, dict):
                log_tikfinity_hook("Skipping malformed message", {"message": message}, level="WARN")
                return

            # ─── silent heartbeat ───────────────────────────
            now = time.time()
            if now - self._tikfinity_state.get("last_heartbeat", 0.0) > 2.0:
                self._tikfinity_state["last_heartbeat"] = now
                q.put(("tikfinity", "heartbeat", {}))

            # --- Extract data ---
            data = payload.get("data", {}) or {}
            info = data.get("data") if isinstance(data.get("data"), dict) else data

            event_type = str(payload.get("event") or data.get("event", "")).strip().lower()

            # ─── Gift events ────────────────────────────────
            if event_type == "gift":
                raw_name = info.get("giftName") or info.get("name", "")
                norm_name = raw_name.strip().lower()
                if not norm_name:
                    log_tikfinity_hook("Skipping empty gift name", {"gift_info": info}, level="WARN")
                    return

                if not hasattr(self, "_gift_mapping") or norm_name not in {n.strip().lower() for n in self._gift_mapping.keys()}:
                    log_tikfinity_hook("Unknown gift received", {"gift": raw_name, "normalized": norm_name}, level="DEBUG")
                    return

                repeat_count = int(info.get("repeatCount", 1))
                repeat_end   = bool(info.get("repeatEnd", False))
                streak_key   = f"{info.get('userId')}:{norm_name}"

                prev  = self._streak_counters.get(streak_key, 0)
                delta = repeat_count - prev

                if delta > 0:
                    self._gift_counters[norm_name] += delta
                    self._total_diamonds += info.get("diamondCount", 0) * delta
                    self._streak_counters[streak_key] = repeat_count
                else:
                    return

                if not repeat_end:
                    return

                # --- Trigger chains ---
                for chain in load_chains():
                    if chain.get("trigger") != "gift":
                        continue
                    if chain.get("giftName", "").strip().lower() != norm_name:
                        continue

                    queue_mode   = chain.get("queue_mode", False)
                    allow_repeat = chain.get("allowRepeat", False)

                    passed = True
                    for key, check in {
                        "diamondCountMin": lambda: info.get("diamondCount", 0) >= chain.get("diamondCountMin", 0),
                        "diamondCountMax": lambda: info.get("diamondCount", 0) <= chain.get("diamondCountMax", float("inf")),
                        "isSubscriber":    lambda: not chain.get("isSubscriber", False) or info.get("isSubscriber", False),
                        "isModerator":     lambda: not chain.get("isModerator", False) or info.get("isModerator", False),
                        "gifterLevelMin":  lambda: info.get("gifterLevel", 0) >= chain.get("gifterLevelMin", 0),
                        "gifterLevelMax":  lambda: info.get("gifterLevel", 0) <= chain.get("gifterLevelMax", float("inf")),
                    }.items():
                        if key in chain and not check():
                            passed = False
                            break
                    if not passed:
                        continue

                    req  = chain.get("giftCount")
                    curr = self._gift_counters[norm_name]
                    do_it = False

                    if req is not None:
                        if curr >= req:
                            do_it = True
                            self._gift_counters[norm_name] = 0
                    else:
                        do_it = True

                    if not do_it:
                        continue

                    prio  = chain.get("priority", 100) if queue_mode else 100
                    times = repeat_count if allow_repeat else 1

                    payload_chain = (
                        chain["steps"],
                        chain.get("delay", 0.025),
                        chain.get("reset_before", False),
                        chain.get("reset_after", False),
                        {**chain, "event_data": info},
                    )

                    for _ in range(times):
                        self.chain_queue.put((prio, time.time(), payload_chain))
                        self._try_next_chain()

                if streak_key in self._streak_counters:
                    del self._streak_counters[streak_key]

                _emit_state("connected", {"trigger": "gift", "data": info})
                return

            # ─── Subscribe events (left as-is) ─────────────
            if event_type == "subscribe":
                return

            # ─── Custom diamonds ──────────────────────────
            if event_type == "custom_diamonds":
                self._handle_custom_diamonds(info, payload)
                return

        except Exception as e:
            log_tikfinity_hook("Invalid WebSocket message", {"message": message}, level="ERROR", exc=e)

    def on_error(ws, error):
        was_connected = self._tikfinity_state.get("connected", False)

        if was_connected:
            log_tikfinity_hook("TikFinity socket error", {"error": str(error)}, level="ERROR")

        self._tikfinity_state["connected"] = False
        _emit_state("disconnected", {}, debounce_s=1.0)

    def on_close(ws, close_status_code, close_msg):
        was_connected = self._tikfinity_state.get("connected", False)

        if was_connected:
            log_tikfinity_hook(
                "TikFinity disconnected",
                {"code": close_status_code, "message": close_msg},
                level="WARN"
            )

        self._tikfinity_state["connected"] = False
        _emit_state("disconnected", {}, debounce_s=1.0)

    def on_open(ws):
        first_connect = not self._tikfinity_state.get("connected", False)

        self._tikfinity_state["connected"] = True
        self._tikfinity_state["reconnecting"] = False

        if first_connect:
            log_tikfinity_hook("TikFinity connected (half-live mode)", level="INFO")

        # UI / status signal only - NO REGISTRY LOGIC
        _emit_state("connected", {}, debounce_s=0.5)

        # Ensure gift mapping is loaded
        if not getattr(self, "_gift_mapping", {}):
            self._gift_mapping = self._load_gift_mapping()

        # Prime gift chains (listener-only responsibility)
        chains = load_chains()
        existing_names = {c.get("name") for c in self.pending_gift_chains}
        incoming_gift_chains = [c for c in chains if c.get("trigger") == "gift"]

        for chain in incoming_gift_chains:
            name = chain.get("name", "")
            if name in existing_names:
                continue

            gift_name = chain.get("giftName", "")
            gift_id = self._gift_mapping.get(gift_name, {}).get("id")

            if gift_id:
                log_tikfinity_hook(
                    "Listening for giftId",
                    {"id": gift_id, "chain": name},
                    level="DEBUG"
                )
            else:
                log_tikfinity_hook(
                    "Listening for gift chain",
                    {"chain": name},
                    level="DEBUG"
                )

            self.pending_gift_chains.append(chain)

    # ─── Start dispatcher (only once) ─────────────────────────
    if not getattr(self, "_tikfinity_dispatcher_started", False):
        self._tikfinity_dispatcher_started = True

        def event_dispatch_worker(q):
            while True:
                try:
                    func, ctx = q.get()
                    repeat = int(ctx.get("repeat", 1))
                    for _ in range(repeat):
                        try:
                            func(ctx)
                        except Exception as inner_e:
                            log_tikfinity_hook("TikFinity action dispatch error", level="ERROR", exc=inner_e)
                except Exception as e:
                    log_tikfinity_hook("Fatal error in TikFinity dispatch loop", level="CRITICAL", exc=e)

        threading.Thread(
            target=event_dispatch_worker,
            args=(event_dispatch_queue,),
            daemon=True
        ).start()

    # ─── Connect ──────────────────────────────────────────────
    def _run_ws_forever():
        import random

        backoff = 2.0
        max_backoff = 30.0

        while True:
            try:
                log_tikfinity_hook(
                    "Connecting to TikFinity WebSocket…",
                    level="INFO"
                )

                ws = websocket.WebSocketApp(
                    "ws://localhost:21213/",
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close
                )

                self._tikfinity_state["connected"] = False

                ws.run_forever(
                    ping_interval=15,
                    ping_timeout=10
                )

                log_tikfinity_hook(
                    "TikFinity socket closed — reconnecting",
                    level="WARN"
                )

            except Exception as e:
                log_tikfinity_hook(
                    "TikFinity socket crashed",
                    level="ERROR",
                    exc=e
                )

            # ─── Backoff ───
            delay = min(backoff, max_backoff) + random.uniform(0.25, 0.75)

            log_tikfinity_hook(
                "Reconnecting to TikFinity",
                {"retry_in": round(delay, 2)},
                level="DEBUG"
            )

            time.sleep(delay)
            backoff = min(backoff * 1.6, max_backoff)

    # ─── Start WebSocket thread (once) ──────────────────────
    if not getattr(self, "_tikfinity_ws_started", False):
        self._tikfinity_ws_started = True

        threading.Thread(
            target=_run_ws_forever,
            daemon=True,
            name="TikFinity-WebSocket"
        ).start()

        log_tikfinity_hook(
            "TikFinity WebSocket thread started",
            level="INFO"
        )

# ─────────────────────────────────────────────────────────── 3. TikTok worker
def tiktok_thread(username: str) -> None:
    import asyncio, sys
    import os
    import json
    import logging
    import time
    from collections import defaultdict
    from TikTokLive import TikTokLiveClient, events

    # ───── Logger Setup ─────
    log_dir = os.path.join("saved", "logs", "webhook")
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger(f"tiktok_listener_{username}")
    log_path = os.path.join(log_dir, "tiktok_listener.log")

    if not logger.handlers:
        fh = logging.FileHandler(log_path, encoding="utf-8")
        formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        logger.setLevel(logging.DEBUG)

    if not username:
        logger.error("TikTok username not provided")
        q.put(("tiktok", "error", {"msg": "TikTok username is required"}))
        return

    # ───── Inline Gift Loader ─────
    def _load_gift_mapping(path: str = "saved/giftMapping.json") -> dict[str, dict[str, any]]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            gift_map = {}
            if isinstance(data, dict):  # Original format
                for gift_name, info in data.items():
                    if isinstance(info, dict):
                        gift_map[gift_name] = {
                            "id": str(info.get("id", "")),
                            "coinCost": info.get("coinCost", 0),
                            "image": info.get("image", "")
                        }
            elif isinstance(data, list):  # Merged format
                for entry in data:
                    if isinstance(entry, dict) and "name" in entry:
                        gift_map[entry["name"]] = {
                            "id": str(entry.get("id", "")),
                            "coinCost": entry.get("coinCost") or entry.get("coins", 0),
                            "image": entry.get("image", "")
                        }

            logger.info(f"Loaded {len(gift_map)} gifts from {path}")
            return gift_map
        except Exception as e:
            logger.exception(f"Failed to load gift mapping from {path}")
            return {}

    gift_mapping = _load_gift_mapping()
    gift_counters = defaultdict(int)
    total_diamonds = 0
    client = TikTokLiveClient(unique_id=username)

    def _normalize(name: str) -> str:
        return name.strip().lower()

    @client.on(events.GiftEvent)
    async def on_gift(event: events.GiftEvent):
        nonlocal total_diamonds
        info = event.dict()
        raw_name = info.get("gift", {}).get("name", "")
        norm_name = _normalize(raw_name)

        if not norm_name:
            logger.warning("Skipping gift with no name")
            return

        if norm_name not in {n.lower() for n in gift_mapping}:
            logger.debug(f"Unknown gift: {raw_name}")
            return

        repeat_count = info.get("repeat_count", 1)
        gift_counters[norm_name] += repeat_count
        total_diamonds += info.get("diamond_count", 0)

        logger.info(f"Gift: {raw_name} x{repeat_count} from {info.get('user', {}).get('nickname')}")

        gift_triggered = False
        for chain in load_chains():
            if chain.get("trigger") != "gift":
                continue
            if _normalize(chain.get("giftName", "")) != norm_name:
                continue

            filters_passed = True
            filter_checks = {
                "diamondCountMin": lambda: info.get("diamond_count", 0) >= chain.get("diamondCountMin", 0),
                "diamondCountMax": lambda: info.get("diamond_count", 0) <= chain.get("diamondCountMax", 999999),
                "isSubscriber":    lambda: not chain.get("isSubscriber") or info.get("user", {}).get("is_subscriber", False),
                "isModerator":     lambda: not chain.get("isModerator")  or info.get("user", {}).get("is_moderator",  False),
                "gifterLevelMin":  lambda: info.get("user", {}).get("gifter_level", 0) >= chain.get("gifterLevelMin", 0),
                "gifterLevelMax":  lambda: info.get("user", {}).get("gifter_level", 0) <= chain.get("gifterLevelMax", 999999),
            }
            for key, check in filter_checks.items():
                if key in chain and not check():
                    filters_passed = False
                    break

            count_required = chain.get("giftCount")
            current_count = gift_counters[norm_name]
            should_trigger = False

            if count_required is not None:
                if current_count >= count_required and filters_passed:
                    should_trigger = True
                    gift_counters[norm_name] = 0
            elif filters_passed:
                should_trigger = True

            if should_trigger:
                times = repeat_count if chain.get("allowRepeat", False) else 1
                logger.info(f"Triggering chain '{chain['name']}' x{times}")

                for _ in range(times):
                    payload = (
                        chain["steps"],
                        chain.get("delay", 0.025),
                        chain.get("reset_before", False),
                        chain.get("reset_after", False),
                        {**chain, "event_data": info},
                    )
                    chain_queue.put((chain.get("priority", 100), time.time(), payload))
                    try_next_chain()
                gift_triggered = True

        if not gift_triggered:
            for chain in load_chains():
                if chain.get("trigger") != "custom_diamonds":
                    continue
                if total_diamonds < chain.get("diamondCountTrigger", 0):
                    continue
                if not all(check() for key, check in filter_checks.items() if key in chain):
                    continue

                logger.info(f"Triggering diamond chain '{chain['name']}'")
                total_diamonds = 0
                payload = (
                    chain["steps"],
                    chain.get("delay", 0.025),
                    chain.get("reset_before", False),
                    chain.get("reset_after", False),
                    {**chain, "event_data": info}
                )
                chain_queue.put((chain.get("priority", 100), time.time(), payload))
                try_next_chain()

        q.put(("tiktok", "Connected", {"trigger": "gift", "data": info}))

    @client.on(events.SubscribeEvent)
    async def on_subscribe(event: events.SubscribeEvent):
        info = event.dict()
        logger.info(f"Subscribe from {info.get('user', {}).get('nickname')}")

        for chain in load_chains():
            if chain.get("trigger") != "subscribe":
                continue

            payload = (
                chain["steps"],
                chain.get("delay", 0.025),
                chain.get("reset_before", False),
                chain.get("reset_after", False),
                {**chain, "event_data": info}
            )
            logger.info(f"Triggering subscribe chain '{chain['name']}'")
            chain_queue.put((chain.get("priority", 100), time.time(), payload))
            try_next_chain()

        q.put(("tiktok", "Connected", {"trigger": "subscribe", "data": info}))

    @client.on(events.ConnectEvent)
    async def on_connect(event: events.ConnectEvent):
        logger.info(f"Connected to @{event.unique_id} - Room ID: {client.room_id}")
        q.put(("tiktok", "connected", {
            "room_id": client.room_id,
            "username": event.unique_id
        }))

    @client.on(events.DisconnectEvent)
    async def on_disconnect(event: events.DisconnectEvent):
        logger.warning("Disconnected from TikTok Live")
        q.put(("tiktok", "disconnected", {
            "reason": getattr(event, "reason", "unknown")
        }))

    try:
        logger.info(f"Starting TikTok listener for @{username}")
        client.run()
    except Exception as e:
        logger.exception("Fatal error in TikTok listener")
        q.put(("tiktok", "error", {"msg": str(e)}))
    finally:
        logger.warning("TikTok listener stopped")
        q.put(("tiktok", "disconnected", {}))

# ─────────────────────────────── OSC Filter Logger
def _resolve_osc_log_path() -> Path:
    log_dir = Path("saved") / "logs" / "controls"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "osc_filter.log"

OSC_FILTER_LOG_PATH = _resolve_osc_log_path()

def osc_log_verbose(
    message: str,
    data: Optional[dict] = None,
    level: str = "DEBUG",
    exc: Optional[BaseException] = None,
):
    level = level.upper()
    level_code = LOG_LEVELS.get(level, LOG_LEVELS["DEBUG"])
    if level_code < CURRENT_LOG_LEVEL:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tag = f"[{level:<8}]"
    lines = [f"{timestamp} {tag} {message}"]

    if data:
        try:
            json_payload = json.dumps(data, indent=2, ensure_ascii=False)
            lines.append(json_payload)
        except Exception as e:
            lines.append(f"[WARN] Failed to serialize payload: {e}")

    if exc:
        trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        lines.append(f"[EXCEPTION]\n{trace}")

    full_message = "\n".join(lines)

    # ONLY print if toggle is True
    if OSC_LOG_TO_CONSOLE:
        print(full_message)

    # Always write to file
    with OSC_FILTER_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(full_message + "\n")

# ─────────────────────────────── Filter Config
FILTERS_DIR = os.path.join(os.getcwd(), "saved", "config", "filters")
NOISY_FILE = os.path.join(FILTERS_DIR, "noisy_parameters.json")
FUZZY_THRESHOLD = 0.85

OSC_LOGGING_ENABLED = True  # 🔇 ← set this to False to suppress console logs

_noisy_exact: dict[str, bool] = {}
_noisy_prefixes: dict[str, bool] = {}
_vrcfury_fuzzy: dict[str, bool] = {}
_last_mtime: float = 0.0

HIGH_NOISE_PREFIXES = [
    "/avatar/parameters/Voice",
]

def _clean_json_common_errors(text: str) -> str:
    text = re.sub(r",\s*(\]|\})", r"\1", text)
    return text

def _load_noisy_params() -> None:
    global _noisy_exact, _noisy_prefixes, _vrcfury_fuzzy, _last_mtime

    try:
        mtime = os.path.getmtime(NOISY_FILE)
        if mtime == _last_mtime:
            return

        with open(NOISY_FILE, "r", encoding="utf-8") as f:
            raw_data = f.read().strip()
            if not raw_data:
                raise ValueError("noisy_parameters.json is empty")

            try:
                data = json.loads(raw_data)
            except json.JSONDecodeError as e:
                cleaned_data = _clean_json_common_errors(raw_data)
                try:
                    data = json.loads(cleaned_data)
                    backup_path = NOISY_FILE + ".fixed.bak"
                    shutil.copyfile(NOISY_FILE, backup_path)
                    with open(NOISY_FILE, "w", encoding="utf-8") as f_fix:
                        f_fix.write(cleaned_data)
                    osc_log_verbose(f"Fixed malformed JSON and updated file. Backup saved at {backup_path}", level="INFO", exc=e)
                except json.JSONDecodeError as e2:
                    backup_path = NOISY_FILE + ".corrupt.bak"
                    shutil.copyfile(NOISY_FILE, backup_path)
                    osc_log_verbose(f"Failed to auto-fix JSON - backup saved at {backup_path}", level="ERROR", exc=e2)
                    return

        upgraded = False
        upgraded_data = {}

        for section in ("noisy_exact", "noisy_prefixes", "vrcfury_high_noise"):
            entries = data.get(section, [])
            fixed = []
            for entry in entries:
                if isinstance(entry, dict) and "pattern" in entry:
                    fixed.append({
                        "pattern": str(entry["pattern"]),
                        "sanitize": bool(entry.get("sanitize", False))
                    })
                elif isinstance(entry, str):
                    fixed.append({"pattern": entry, "sanitize": False})
                    upgraded = True
                else:
                    osc_log_verbose(f"Ignoring malformed entry in {section}: {entry}", level="WARN")
                    upgraded = True
            upgraded_data[section] = fixed

        if upgraded:
            backup_path = NOISY_FILE + ".upgrade.bak"
            shutil.copyfile(NOISY_FILE, backup_path)
            with open(NOISY_FILE, "w", encoding="utf-8") as f:
                json.dump(upgraded_data, f, indent=2)
            osc_log_verbose(f"Upgraded structure and saved. Backup at {backup_path}", level="INFO")
            data = upgraded_data

        # ── Auto-patch required entries ─────────────────────────────
        required_exact = [
            "/avatar/parameters/VRMode"
        ]
        required_prefixes = [
            "/avatar/parameters/Go/",
            "/avatar/parameters/OGB/",
            "/avatar/parameters/OSCm/"
        ]
        required_fuzzy = [
            "Toggle", "Blend", "LastSynced", "Slider"
        ]

        changes = False

        def patch_section(section_key, required):
            nonlocal changes
            existing_set = {e["pattern"] for e in data.get(section_key, [])}
            missing = [p for p in required if p not in existing_set]
            if missing:
                osc_log_verbose(f"Auto-patching missing {section_key}: {missing}", level="INFO")
                patched = data.get(section_key, []) + [{"pattern": p, "sanitize": True} for p in missing]
                data[section_key] = patched
                changes = True

        patch_section("noisy_exact", required_exact)
        patch_section("noisy_prefixes", required_prefixes)
        patch_section("vrcfury_high_noise", required_fuzzy)

        if changes:
            try:
                with open(NOISY_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2)
                osc_log_verbose("noisy_parameters.json updated with required patterns", level="INFO")
            except Exception as e:
                osc_log_verbose("Failed to write updated noisy_parameters.json", level="ERROR", exc=e)

        _noisy_exact = {e["pattern"]: e["sanitize"] for e in data.get("noisy_exact", [])}
        _noisy_prefixes = {e["pattern"]: e["sanitize"] for e in data.get("noisy_prefixes", [])}
        _vrcfury_fuzzy = {e["pattern"]: e["sanitize"] for e in data.get("vrcfury_high_noise", [])}
        _last_mtime = mtime

        if OSC_LOGGING_ENABLED:
            osc_log_verbose(
                f"Loaded {len(_noisy_exact)} exact, {len(_noisy_prefixes)} prefixes, "
                f"{len(_vrcfury_fuzzy)} VRCFURY fuzzy terms", level="INFO"
            )

    except FileNotFoundError:
        os.makedirs(FILTERS_DIR, exist_ok=True)
        defaults = {
            "noisy_exact": [
                {"pattern": "/avatar/parameters/VRMode", "sanitize": True}
            ],
            "noisy_prefixes": [
                {"pattern": "EyeTrackingActive", "sanitize": True},
                {"pattern": "ExpressionTrackingActive", "sanitize": True},
                {"pattern": "LipTrackingActive", "sanitize": True},
                {"pattern": "LastSynced", "sanitize": True},
                {"pattern": "/avatar/parameters/Go/", "sanitize": True},
                {"pattern": "/avatar/parameters/OGB/", "sanitize": True},
                {"pattern": "/avatar/parameters/OSCm/", "sanitize": True}
            ],
            "vrcfury_high_noise": [
                {"pattern": "Toggle", "sanitize": True},
                {"pattern": "Blend", "sanitize": True},
                {"pattern": "Slider", "sanitize": True}
            ]
        }

        with open(NOISY_FILE, "w", encoding="utf-8") as f:
            json.dump(defaults, f, indent=2)

        _noisy_exact = {e["pattern"]: e["sanitize"] for e in defaults["noisy_exact"]}
        _noisy_prefixes = {e["pattern"]: e["sanitize"] for e in defaults["noisy_prefixes"]}
        _vrcfury_fuzzy = {e["pattern"]: e["sanitize"] for e in defaults["vrcfury_high_noise"]}
        _last_mtime = os.path.getmtime(NOISY_FILE)

        if OSC_LOGGING_ENABLED:
            osc_log_verbose(f"Created default filter file at {NOISY_FILE}", level="INFO")

def _add_to_noisy_filter(addr: str, sanitize: bool = True) -> None:
    _load_noisy_params()  # ensure it's loaded before adding

    if addr in _noisy_exact:
        osc_log_verbose(f"[FILTER] Already exists in noisy filter: {addr}", level="DEBUG")
        return

    _noisy_exact[addr] = sanitize

    try:
        with open(NOISY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        osc_log_verbose("Failed to read noisy filter file, rebuilding from memory", level="WARN", exc=e)
        data = {"noisy_exact": [], "noisy_prefixes": [], "vrcfury_high_noise": []}

    data.setdefault("noisy_exact", []).append({"pattern": addr, "sanitize": sanitize})

    try:
        with open(NOISY_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        osc_log_verbose(f"[FILTER] Added address to noisy filter: {addr}", level="INFO")
    except Exception as e:
        osc_log_verbose("Failed to write updated noisy filter", level="ERROR", exc=e)

def _remove_from_noisy_filter(addr: str) -> None:
    _load_noisy_params()

    if addr not in _noisy_exact:
        osc_log_verbose(f"[FILTER] Not in filter: {addr}", level="DEBUG")
        return

    del _noisy_exact[addr]

    try:
        with open(NOISY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        osc_log_verbose("Failed to read noisy filter file", level="ERROR", exc=e)
        return

    before_count = len(data.get("noisy_exact", []))
    data["noisy_exact"] = [e for e in data.get("noisy_exact", []) if e.get("pattern") != addr]
    after_count = len(data["noisy_exact"])

    if before_count == after_count:
        osc_log_verbose(f"[FILTER] No matching entry to remove for: {addr}", level="DEBUG")
        return

    try:
        with open(NOISY_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        osc_log_verbose(f"[FILTER] Removed address from noisy filter: {addr}", level="INFO")
    except Exception as e:
        osc_log_verbose("Failed to save noisy filter after removal", level="ERROR", exc=e)
        
# ─────────────────────────────── OSC Handler ───────────────────────────────
def osc_thread() -> None:
    import os
    import json
    import re
    import shutil
    import time
    import threading
    from pathlib import Path
    from collections import defaultdict
    from datetime import datetime
    from pythonosc import dispatcher as osc_dispatcher, osc_server
    from difflib import SequenceMatcher as FuzzyCompare
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler

    # ─── Paths & Constants ────────────────────────────────────────────────
    # CFG_DIR should be defined elsewhere in your app
    NUCLEAR_FILE = Path(CFG_DIR) / "filters" / "nuclear.json"
    NUCLEAR_FILE.parent.mkdir(parents=True, exist_ok=True)
    RATE_THRESHOLD      = 25
    HIGH_NOISE_PREFIXES = ["/avatar/parameters/Voice"]
    FUZZY_THRESHOLD     = 0.85

    # ─── Default nuclear.json content ─────────────────────────────────────
    DEFAULT_NUCLEAR = {
      "tracking_prefixes": [
        "/avatar/parameters/FT/",
        "/avatar/parameters/OSCm/",
        "/avatar/parameters/OGB/",
        "/avatar/parameters/Viseme",
        "/avatar/parameters/Grounded",
        "/avatar/parameters/AngularX",
        "/avatar/parameters/AngularY",
        "/avatar/parameters/AngularZ",
        "/avatar/parameters/VelocityX",
        "/avatar/parameters/VelocityY",
        "/avatar/parameters/VelocityZ",
        "/avatar/parameters/VelocityMagnitude",
        "/avatar/parameters/Upright",
        "/avatar/parameters/Voice",
        "/avatar/parameters/Vulper/PB/",
        "/avatar/parameters/Meowper/PB/",
        "/avatar/parameters/GestureRightWeight",
        "/avatar/parameters/GestureRight",
        "/avatar/parameters/GestureLeftWeight",
        "/avatar/parameters/GestureLeft",
        "/avatar/parameters/Go/",
        "/avatar/parameters/EyeHeightAsMeters",
        "/avatar/parameters/EyeHeightAsPercent",
        "/avatar/parameters/ScaleModified",
        "/avatar/parameters/ScaleFactorInverse",
        "/avatar/parameters/ScaleFactor",
        "/avatar/parameters/TrackingType",
        "/avatar/parameters/FloorHeight",
        "/avatar/parameters/DoubleThumbs"
      ],
      "tracking_fuzzy_suffixes": [
        "AnkleCor_L_Angle",
        "AnkleCor_L_IsGrabbed",
        "AnkleCor_L_IsPosed",
        "AnkleCor_L_Squish",
        "AnkleCor_L_Stretch",
        "AnkleCor_R_Angle",
        "AnkleCor_R_IsGrabbed",
        "AnkleCor_R_IsPosed",
        "AnkleCor_R_Squish",
        "AnkleCor_R_Stretch",
        "ArmDownC_L_Angle",
        "ArmDownC_R_Angle",
        "ArmForwardC_L_Angle",
        "ArmForwardC_R_Angle",
        "BrowExpressionLeft",
        "BrowExpressionLeft1",
        "BrowExpressionLeft2",
        "BrowExpressionLeft4",
        "BrowExpressionLeftNegative",
        "BrowExpressionRight",
        "BrowExpressionRight1",
        "BrowExpressionRight2",
        "BrowExpressionRight4",
        "BrowExpressionRightNegative",
        "CheekPuffSuckLeft",
        "CheekPuffSuckLeft1",
        "CheekPuffSuckLeft2",
        "CheekPuffSuckLeft4",
        "CheekPuffSuckLeftNegative",
        "CheekPuffSuckRight",
        "CheekPuffSuckRight1",
        "CheekPuffSuckRight2",
        "CheekPuffSuckRight4",
        "CheekPuffSuckRightNegative",
        "ChestC",
        "CorrectivesOn",
        "Credits_For_Self",
        "Credits_Out",
        "Debug",
        "DigiAnklesWeight",
        "DigiLegsMain",
        "EarsBack",
        "EarsDown",
        "EarsMainDriver",
        "EarsTwitch",
        "EarsUp",
        "EarLookup",
        "EarLookDown",
        "Ear_L_Angle",
        "Ear_R_Angle",
        "Ear_L_IsPosed",
        "Ear_R_IsPosed",
        "Ear_L_Stretch",
        "Ear_R_Stretch",
        "EarPokedL",
        "EarPokedR",
        "Ear_L_Squish",
        "Ear_R_Squish",
        "Ear_L_IsPosed",
        "Ear_R_IsPosed",
        "Ear_L_IsGrabbed",
        "Ear_R_IsGrabbed",
        "ElbowCor_L_Angle",
        "ElbowCor_L_IsGrabbed",
        "ElbowCor_L_IsPosed",
        "ElbowCor_L_Squish",
        "ElbowCor_L_Stretch",
        "ElbowCor_R_Angle",
        "ElbowCor_R_IsGrabbed",
        "ElbowCor_R_IsPosed",
        "ElbowCor_R_Squish",
        "ElbowCor_R_Stretch",
        "EyeDilationEnable",
        "EyeLeftX",
        "EyeLidLeft",
        "EyeLidRight",
        "EyeRightX",
        "EyeSquintLeft",
        "EyeSquintLeft1",
        "EyeSquintLeft2",
        "EyeSquintLeft4",
        "EyeSquintRight",
        "EyeSquintRight1",
        "EyeSquintRight2",
        "EyeSquintRight4",
        "EyeTrackingActive",
        "EyeY",
        "EyesAim_LeftRight_Angle",
        "EyesAim_LeftRight_IsGrabbed",
        "EyesAim_LeftRight_IsPosed",
        "EyesAim_LeftRight_Squish",
        "EyesAim_LeftRight_Stretch",
        "EyesAim_UpDown_Angle",
        "EyesAim_UpDown_IsGrabbed",
        "EyesAim_UpDown_IsPosed",
        "EyesAim_UpDown_Squish",
        "EyesAim_UpDown_Stretch",
        "FacialExpressionsDisabled",
        "FootPlantL_Angle",
        "FootPlantR_Angle",
        "FootPlantL",
        "FootPlantLL",
        "FootPlantLR",
        "FootPlantR",
        "FootPlantRL",
        "FootPlantRR",
        "Head_Tilt_LeftRight_Angle",
        "Head_Tilt_LeftRight_IsGrabbed",
        "Head_Tilt_LeftRight_IsPosed",
        "Head_Tilt_LeftRight_Squish",
        "Head_Tilt_LeftRight_Stretch",
        "Head_Tilt_UpDown_Angle",
        "Head_Tilt_UpDown_IsGrabbed",
        "Head_Tilt_UpDown_IsPosed",
        "Head_Tilt_UpDown_Squish",
        "Head_Tilt_UpDown_Stretch",
        "Head_Twist_Angle",
        "Head_Twist_IsGrabbed",
        "Head_Twist_IsPosed",
        "Head_Twist_Squish",
        "Head_Twist_Stretch",
        "HipOutC_L_Angle",
        "HipOutC_R_Angle",
        "HipTwistCor_L_Angle",
        "HipTwistCor_L_IsGrabbed",
        "HipTwistCor_L_IsPosed",
        "HipTwistCor_L_Squish",
        "HipTwistCor_L_Stretch",
        "HipTwistCor_R_Angle",
        "HipTwistCor_R_IsGrabbed",
        "HipTwistCor_R_IsPosed",
        "HipTwistCor_R_Squish",
        "HipTwistCor_R_Stretch",
        "HipUpBackCor_L_Angle",
        "HipUpBackCor_L_IsGrabbed",
        "HipUpBackCor_L_IsPosed",
        "HipUpBackCor_L_Squish",
        "HipUpBackCor_L_Stretch",
        "HipUpBackCor_R_Angle",
        "HipUpBackCor_R_IsGrabbed",
        "HipUpBackCor_R_IsPosed",
        "HipUpBackCor_R_Squish",
        "HipUpBackCor_R_Stretch",
        "JawForward",
        "JawForward1",
        "JawForward2",
        "JawForward4",
        "JawOpen",
        "JawX",
        "JawX1",
        "JawX2",
        "JawX4",
        "JawXNegative",
        "KneeCor_L_Angle",
        "KneeCor_L_IsGrabbed",
        "KneeCor_L_IsPosed",
        "KneeCor_L_Squish",
        "KneeCor_L_Stretch",
        "KneeCor_R_Angle",
        "KneeCor_R_IsGrabbed",
        "KneeCor_R_IsPosed",
        "KneeCor_R_Squish",
        "KneeCor_R_Stretch",
        "LLDigi",
        "LLDigiToe",
        "LastSynced",
        "LeftThumbPuffCheeks",
        "LipFunnel",
        "LipFunnel1",
        "LipFunnel2",
        "LipFunnel4",
        "LipPucker",
        "LipPucker1",
        "LipPucker2",
        "LipPucker4",
        "LipSuckLower",
        "LipSuckLower1",
        "LipSuckLower2",
        "LipSuckLower4",
        "LipSuckUpper",
        "LipSuckUpper1",
        "LipSuckUpper2",
        "LipSuckUpper4",
        "LipTrackingActive",
        "Local",
        "LocomotionReplace_trackingHead",
        "LocomotionReplace_trackingHip",
        "LocomotionReplace_trackingLeftFoot",
        "LocomotionReplace_trackingLeftHand",
        "LocomotionReplace_trackingRightFoot",
        "LocomotionReplace_trackingRightHand",
        "Main_Dynamics_Off",
        "MouthClosed",
        "MouthDimple",
        "MouthDimple1",
        "MouthDimple2",
        "MouthDimple4",
        "MouthLowerDown",
        "MouthLowerDown1",
        "MouthLowerDown2",
        "MouthLowerDown4",
        "MouthPress",
        "MouthPress1",
        "MouthPress2",
        "MouthPress4",
        "MouthRaiserLower",
        "MouthRaiserLower1",
        "MouthRaiserLower2",
        "MouthRaiserLower4",
        "MouthRaiserUpper",
        "MouthRaiserUpper1",
        "MouthRaiserUpper2",
        "MouthRaiserUpper4",
        "MouthStretchLeft",
        "MouthStretchLeft1",
        "MouthStretchLeft2",
        "MouthStretchLeft4",
        "MouthStretchRight",
        "MouthStretchRight1",
        "MouthStretchRight2",
        "MouthStretchRight4",
        "MouthTightenerLeft",
        "MouthTightenerLeft1",
        "MouthTightenerLeft2",
        "MouthTightenerLeft4",
        "MouthTightenerRight",
        "MouthTightenerRight1",
        "MouthTightenerRight2",
        "MouthTightenerRight4",
        "MouthUpperUp",
        "MouthX",
        "MouthX1",
        "MouthX2",
        "MouthX4",
        "MouthX8",
        "MouthXNegative",
        "NeckC",
        "NoFTEyePupils",
        "NoseItchL",
        "NoseItchR",
        "NoseSneer",
        "NoseSneer1",
        "NoseSneer2",
        "NoseSneer4",
        "OGB",
        "OSCm",
        "PupilConstrictOrSlit",
        "PupilDilation",
        "PupilDilation1",
        "PupilDilation2",
        "PupilDilation4",
        "PupilSize",
        "RLDigi",
        "RLDigiToe",
        "RightThumbPuffCheeks",
        "SmileFrownLeft",
        "SmileFrownLeft1",
        "SmileFrownLeft2",
        "SmileFrownLeft4",
        "SmileFrownLeftNegative",
        "SmileFrownRight",
        "SmileFrownRight1",
        "SmileFrownRight2",
        "SmileFrownRight4",
        "SmileFrownRightNegative",
        "SpineC",
        "SyncDataNum",
        "SyncPointer",
        "TailUp",
        "Tail_Down",
        "Tail_Left",
        "Tail_Right",
        "ToeCurlLD",
        "ToeCurlLU",
        "ToeCurlRD",
        "ToeCurlRU",
        "ToeSpread",
        "ToeTwistLToL",
        "ToeTwistLToR",
        "ToeTwistRToL",
        "ToeTwistRToR",
        "Toes_Dynamics_Off",
        "TongueOut",
        "TongueOut1",
        "TongueOut2",
        "TongueOut4",
        "TongueX",
        "TongueX1",
        "TongueX2",
        "TongueX4",
        "TongueXNegative",
        "TongueY",
        "TongueY1",
        "TongueY2",
        "TongueY4",
        "TongueYNegative",
        "TrackingActive",
        "DigiAnkleOnly",
        "DigiWeight",
        "Extendo",
        "Idle",
        "Size",
        "VisemesEnable",
        "WristCor_L_Angle",
        "WristCor_L_IsGrabbed",
        "WristCor_L_IsPosed",
        "WristCor_L_Squish",
        "WristCor_L_Stretch",
        "WristCor_R_Angle",
        "WristCor_R_IsGrabbed",
        "WristCor_R_IsPosed",
        "WristCor_R_Squish",
        "WristCor_R_Stretch",
        "WristTwistCor_L_Angle",
        "WristTwistCor_L_IsGrabbed",
        "WristTwistCor_L_IsPosed",
        "WristTwistCor_L_Squish",
        "WristTwistCor_L_Stretch",
        "WristTwistCor_R_Angle",
        "WristTwistCor_R_IsGrabbed",
        "WristTwistCor_R_IsPosed",
        "WristTwistCor_R_Squish",
        "WristTwistCor_R_Stretch",
        "collar_tag_Angle",
        "collar_tag_Squish",
        "collar_tag_Stretch",
        "collar_tag_IsPosed",
        "collar_tag_IsGrabbed",
        "Leash_Angle",
        "Leash_IsGrabbed",
        "Leash_Squish",
        "Leash_Stretch",
        "Leash_IsPosed",
        "SpeedUp"
      ]
    }

    # ─── Bootstrap on-disk nuclear.json ──────────────────────────────────
    try:
        if not NUCLEAR_FILE.exists():
            NUCLEAR_FILE.write_text(json.dumps(DEFAULT_NUCLEAR, indent=2), encoding="utf-8")
        else:
            raw = NUCLEAR_FILE.read_text(encoding="utf-8")
            data = json.loads(raw)
            if not isinstance(data, dict) or \
               "tracking_prefixes" not in data or \
               "tracking_fuzzy_suffixes" not in data:
                raise ValueError("Invalid nuclear.json structure")
    except Exception:
        backup = NUCLEAR_FILE.with_suffix(".corrupt.bak")
        shutil.copyfile(NUCLEAR_FILE, backup)
        NUCLEAR_FILE.write_text(json.dumps(DEFAULT_NUCLEAR, indent=2), encoding="utf-8")

    # ─── In-memory State & Locks ───────────────────────────────────────────
    TRACKING_PREFIXES: list[str]           = []
    TRACKING_FUZZY_SUFFIXES: list[str]    = []
    _fuzzy_suffix_matchers: list[re.Pattern] = []
    _nuclear_lock = threading.Lock()

    _rate_counts: dict[str,int] = defaultdict(int)
    _rate_lock   = threading.Lock()

    # ─── Logging Helper ────────────────────────────────────────────────────
    def _log(msg: str, data: dict=None, level: str="DEBUG"):
        if OSC_LOG_TO_CONSOLE:
            ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
            payload = f" {json.dumps(data, ensure_ascii=False)}" if data else ""
            print(f"{ts} [{level}] {msg}{payload}")

    # ─── Robust auto-add to nuclear.json ────────────────────────────────────
    def _add_to_nuclear(addr: str):
        with _nuclear_lock:
            try:
                raw = NUCLEAR_FILE.read_text(encoding="utf-8")
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    # fix trailing commas
                    cleaned = re.sub(r",\s*(\]|\})", r"\1", raw)
                    backup = NUCLEAR_FILE.with_suffix(".corrupt.bak")
                    shutil.copyfile(NUCLEAR_FILE, backup)
                    NUCLEAR_FILE.write_text(cleaned, encoding="utf-8")
                    data = json.loads(cleaned)
                    _log("Fixed trailing-comma in nuclear.json", {"backup": str(backup)}, level="WARN")
            except FileNotFoundError:
                data = {"tracking_prefixes": [], "tracking_fuzzy_suffixes": []}
            except Exception as e:
                _log("Rebuilding nuclear.json from default", {"error": str(e)}, level="ERROR")
                data = {"tracking_prefixes": [], "tracking_fuzzy_suffixes": []}

            prefixes = data.setdefault("tracking_prefixes", [])
            if addr not in prefixes:
                prefixes.append(addr)
                TRACKING_PREFIXES.append(addr)
                NUCLEAR_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
                _log("Auto-added to nuclear.json & in-memory filters", {"addr": addr}, level="INFO")

    # ─── Live-reload Watchdog ───────────────────────────────────────────────
    class _ReloadHandler(FileSystemEventHandler):
        def on_modified(self, event):
            if Path(event.src_path) == NUCLEAR_FILE:
                _load_nuclear()

    def _load_nuclear():
        nonlocal TRACKING_PREFIXES, TRACKING_FUZZY_SUFFIXES, _fuzzy_suffix_matchers
        try:
            cfg = json.loads(NUCLEAR_FILE.read_text(encoding="utf-8"))
            with _nuclear_lock:
                TRACKING_PREFIXES        = cfg.get("tracking_prefixes", [])[:]
                TRACKING_FUZZY_SUFFIXES  = cfg.get("tracking_fuzzy_suffixes", [])[:]
                _fuzzy_suffix_matchers   = [
                    re.compile(rf"^(VF\d*_?)?{re.escape(s)}$", re.IGNORECASE)
                    for s in TRACKING_FUZZY_SUFFIXES
                ]
            _log("Reloaded nuclear.json filters",
                 {"prefixes": len(TRACKING_PREFIXES), "fuzzy": len(TRACKING_FUZZY_SUFFIXES)}, level="INFO")
        except Exception as e:
            _log("Failed loading nuclear.json", {"error": str(e)}, level="ERROR")

    observer = Observer()
    observer.schedule(_ReloadHandler(), path=str(NUCLEAR_FILE.parent), recursive=False)
    observer.daemon = True
    observer.start()

    # ─── Rate-counter Reset Thread ─────────────────────────────────────────
    def _reset_rates():
        while True:
            time.sleep(1)
            with _rate_lock:
                _rate_counts.clear()
    threading.Thread(target=_reset_rates, daemon=True).start()

    # initial load
    _load_nuclear()

    # ─── OSC Dispatcher Setup ─────────────────────────────────────────────
    disp = osc_dispatcher.Dispatcher()

    def _handler(addr: str, *args):
        # 1) rate-limit & auto-ban
        with _rate_lock:
            cnt = _rate_counts[addr] + 1
            _rate_counts[addr] = cnt
        if cnt > RATE_THRESHOLD:
            _add_to_nuclear(addr)
            return

        # 2) nuclear.json filters
        with _nuclear_lock:
            if any(addr.startswith(p) for p in TRACKING_PREFIXES):
                return
            tail = addr.rsplit("/", 1)[-1]
            if any(rx.match(tail) for rx in _fuzzy_suffix_matchers):
                return

        # 3) drop “tracking” spam
        if "tracking" in addr.lower():
            return

        # 4) existing noisy filters
        _load_noisy_params()
        if any(addr.startswith(p) for p in HIGH_NOISE_PREFIXES):
            return
        if addr in _noisy_exact:
            return
        if any(addr.startswith(p) for p in _noisy_prefixes):
            return

        # 5) VRCFURY fuzzy-suffix
        tail = addr.rsplit("/", 1)[-1]
        if "_" in tail and tail.upper().startswith("VF"):
            _, suffix = tail.split("_", 1)
            if any(FuzzyCompare(None, suffix, pat).ratio() >= FUZZY_THRESHOLD
                   for pat in _vrcfury_fuzzy):
                return

        # 6) log & enqueue
        _log("OSC msg →", {"addr": addr, "args": args}, level="DEBUG")
        q.put(("osc", "msg", {"addr": addr, "args": args}))

    disp.set_default_handler(_handler)

    server = osc_server.ThreadingOSCUDPServer((OSC_IN_ADDR, OSC_IN_PORT), disp)
    _log(f"OSC listening on {OSC_IN_ADDR}:{OSC_IN_PORT}", level="INFO")
    server.serve_forever()

# ─────────────────────────────── Controls Logger
def _resolve_controls_log_path() -> Path:
    log_dir = Path("saved") / "logs" / "controls"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "controls.log"

CONTROLS_LOG_PATH = _resolve_controls_log_path()

def controls_log_verbose(
    message: str,
    data: Optional[dict] = None,
    level: str = "DEBUG",
    exc: Optional[BaseException] = None,
):
    level = level.upper()
    level_code = LOG_LEVELS.get(level, LOG_LEVELS["DEBUG"])
    if level_code < CURRENT_LOG_LEVEL:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tag = f"[{level:<8}]"
    lines = [f"{timestamp} {tag} {message}"]

    if data:
        try:
            json_payload = json.dumps(data, indent=2, ensure_ascii=False)
            lines.append(json_payload)
        except Exception as e:
            lines.append(f"[WARN] Failed to serialize payload: {e}")

    if exc:
        trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        lines.append(f"[EXCEPTION]\n{trace}")

    full_message = "\n".join(lines)

    print(full_message)
    with CONTROLS_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(full_message + "\n")
        
# ─────────────────────────────────────────────────────────── 5. helpers
def migrate_avatar_controls():
    """Move all 'controls_avtr_*.json' files into the /avatarControls/ folder."""
    legacy_dir = CFG_CTLS_DIR
    new_dir = os.path.join(legacy_dir, "avatarControls")
    os.makedirs(new_dir, exist_ok=True)

    moved_files = []

    for fname in os.listdir(legacy_dir):
        # Only move files that match avatar-specific naming
        if fname.startswith("controls_avtr_") and fname.endswith(".json"):
            src_path = os.path.join(legacy_dir, fname)
            dst_path = os.path.join(new_dir, fname)

            if not os.path.exists(dst_path):
                shutil.move(src_path, dst_path)
                moved_files.append(dst_path)

    if moved_files:
        controls_log_verbose("Migrated avatar control files", data={"moved": moved_files}, level="INFO")
    else:
        controls_log_verbose("No avatar control files needed migration", level="DEBUG")

# ─────────────────────────────────────────────────────────── helpers PATHING
def get_controls_path() -> str:
    """
    Full path to the *current avatar’s* control file (inside avatarControls/).

    ✱ 2025-08-02 revision
    ─────────────────────
    • Standardised on the `controls_avtr_<id>.json` naming convention.
    • Falls back to the legacy name if the new file doesn’t exist yet, so older
      installs remain readable.
    """
    avatar_id = user_cfg.get("current_avatar_id") or "default"
    new_path  = os.path.join(CFG_CTLS_DIR, "avatarControls",
                             f"controls_{avatar_id}.json")
    legacy    = os.path.join(CFG_CTLS_DIR, "avatarControls",
                             f"controls_{avatar_id}.json")

    return new_path if os.path.exists(new_path) or not os.path.exists(legacy) else legacy
    
def load_controls() -> List[Dict[str, Any]]:
    """
    Load OSC controls for the current avatar. Supports both:
    - legacy format: list of control dicts
    - modern format: {"controls": [...]}
    """
    path = get_controls_path()

    if not os.path.exists(path):
        controls_log_verbose("No control file found for avatar", level="DEBUG", data={"path": path})
        return []

    try:
        with open(path, "r", encoding="utf-8") as fp:
            data = json.load(fp)

        if isinstance(data, dict) and "controls" in data:
            return data["controls"]
        elif isinstance(data, list):
            return data
        else:
            controls_log_verbose("Unrecognized control file structure", level="ERROR", data={"path": path})
            return []
    except Exception as e:
        controls_log_verbose("Failed to load controls", level="ERROR", exc=e, data={"path": path})
        return []

# ─────────────────────────────────────────────────────────── helpers SAVE I/O
_warned_empty_once   = False
_warned_shrink_once  = False          # reset each successful save

def save_controls(lst: List[Dict[str, Any]], avatar_id: Optional[str] = None) -> None:
    """
    Persist the supplied control list, creating a timestamped backup when
    overwriting an existing file.

    ✱ 2025-08-02 revision
    ─────────────────────
    • Path alignment with `get_controls_path()` (uses *avtr* prefix).
    • Shrinking a file is now **allowed** - the guard is informational only.
    • Comprehensive backup strategy retained for every overwrite.
    """
    global _warned_empty_once, _warned_shrink_once

    avatar_id = avatar_id or user_cfg.get("current_avatar_id") or "default"
    dir_path  = os.path.join(CFG_CTLS_DIR, "avatarControls")
    os.makedirs(dir_path, exist_ok=True)
    path      = os.path.join(dir_path, f"controls_{avatar_id}.json")

    # ── Guard 1: refuse completely empty / malformed payloads
    if not isinstance(lst, list) or not lst:
        if not _warned_empty_once:
            controls_log_verbose(
                "Refusing to overwrite controls - empty or invalid list.",
                level="GUARD", data={"avatar_id": avatar_id})
            _warned_empty_once = True
        return
    _warned_empty_once = False

    # ── Read current on-disk data (if any) for change detection
    existing: List[Dict[str, Any]] = []
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                existing_data = json.load(fh)
                existing = existing_data.get("controls") if isinstance(existing_data, dict) else existing_data
        except Exception as exc:
            controls_log_verbose("Could not read existing controls file",
                                 level="WARN", exc=exc,
                                 data={"avatar_id": avatar_id})

    # Stable JSON for cheap deep-equality
    stable = lambda x: json.dumps(x, sort_keys=True, ensure_ascii=False)

    if stable(existing) == stable(lst):
        controls_log_verbose("Controls unchanged - skipping save.",
                             level="INFO", data={"avatar_id": avatar_id})
        return

    # ── Informational guard if list shrank, but do NOT block the save
    if len(lst) < len(existing) and not _warned_shrink_once:
        controls_log_verbose("New control set is smaller; old entries will be pruned.",
                             level="GUARD", data={
                                 "avatar_id": avatar_id,
                                 "old_count": len(existing),
                                 "new_count": len(lst)})
        _warned_shrink_once = True
    _warned_shrink_once = False

    # ── Safety-net: backup before overwrite
    try:
        backup_dir = os.path.join(dir_path, "backup")
        os.makedirs(backup_dir, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(
            backup_dir,
            os.path.basename(path).replace(".json", f"_{ts}.bak"))

        if os.path.exists(path):
            shutil.copy2(path, backup_path)
            controls_log_verbose("Previous controls file backed up",
                                 level="BACKUP", data={"backup": backup_path})

        with open(path, "w", encoding="utf-8") as fh:
            json.dump({"controls": lst}, fh, indent=2, ensure_ascii=False)

        controls_log_verbose(
            "Controls saved",
            level="INFO",
            data={"avatar_id": avatar_id, "entries": len(lst), "file": path})

    except Exception as exc:
        controls_log_verbose("Failed while saving controls",
                             level="ERROR", exc=exc,
                             data={"avatar_id": avatar_id})
                             
# Paths that must never be removed, even if not validated through OSCQuery or JSON
UNTOUCHABLE_ADDR_PREFIXES = (
    "/avatar/change",
    "/chatbox/input",
    "/tracking/vrsystem",
)

# ─────────────────────────────────────────────────────────── helpers SANITISE
def sanitize_controls(
    controls: List[Dict[str, Any]],
    osc_bridge,
    avatar_json_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Validate every control row against:
      • hard-coded whitelists (UNTOUCHABLE_ADDR_PREFIXES)
      • avatar JSON parameter list
      • the live OSCQuery cache

    ✱ 2025-08-02 revision
    ─────────────────────
    • No more unconditional keep for `/avatar/parameters/**`.
    • Auto-primes `osc_bridge._param_cache` if empty.
    • Emits granular logs for each drop.
    """

    # ── Prime OSCQuery cache if missing
    if getattr(osc_bridge, "_param_cache", None) is None:
        try:
            osc_bridge.fetch_parameters()
        except Exception as e:
            controls_log_verbose(
                "Failed to fetch parameters for validation",
                level="WARN", exc=e)

    cache = osc_bridge._param_cache or {}
    valid, dropped = [], 0

    # ── Gather addresses declared in avatar JSON
    json_valid: set[str] = set()
    if avatar_json_path and os.path.exists(avatar_json_path):
        try:
            with open(avatar_json_path, "r", encoding="utf-8-sig") as f:
                avatar_data = json.load(f)
            for param in avatar_data.get("parameters", []):
                addr = param.get("input", {}).get("address", "").strip()
                if addr:
                    json_valid.add(addr)
            controls_log_verbose("Loaded addresses from avatar JSON",
                                 data={"count": len(json_valid)})
        except Exception as e:
            controls_log_verbose("Failed to parse avatar JSON",
                                 level="WARN", exc=e)

    # ── Helper utilities
    resolve_base = lambda a: re.sub(r"::#\d+$", "", a)

    def is_whitelisted(addr: str) -> bool:
        base = resolve_base(addr)
        return any(base.startswith(p) for p in UNTOUCHABLE_ADDR_PREFIXES)

    def is_valid_addr(addr: str) -> bool:
        base = resolve_base(addr)
        return (
            base in json_valid
            or osc_bridge._find_param(cache, base) is not None
        )

    # ── Main filter loop
    for ctl in controls:
        addr = ctl.get("addr", "")

        if is_whitelisted(addr) or is_valid_addr(addr):
            valid.append(ctl)
        else:
            dropped += 1
            controls_log_verbose(
                "Dropped stale control entry",
                level="INFO",
                data={"addr": addr})

    controls_log_verbose(
        "Sanitized controls list",
        data={
            "original_count": len(controls),
            "kept":           len(valid),
            "dropped":        dropped})

    return valid


# ───────────────────────────────────────────── Chains I/O
_load_chains_logged_error = False
_last_save_snapshot = {}

# ─── helpers ─────────────────────────────────────────────────────────────
def _normalize_chain_ids(chains: list[dict]) -> list[dict]:
    """Canonicalise categoryId / actionId for every step."""
    for chain in chains:
        for step in chain.get("steps", []):
            if "categoryId" in step:
                step["categoryId"] = _norm(step["categoryId"])
            if "actionId" in step:
                step["actionId"] = _norm(step["actionId"])
    return chains
    
def normalize_layout_indexes(chains: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    used = {c.get("layout_index") for c in chains if isinstance(c.get("layout_index"), int)}
    used = set(used)
    next_index = 0

    for ch in chains:
        if not isinstance(ch.get("layout_index"), int):
            while next_index in used:
                next_index += 1
            ch["layout_index"] = next_index
            used.add(next_index)

    return chains

def migrate_chain_files():
    """Move all 'chains_avtr_*.json' files into the /controls/chains/ folder."""
    src_dir = CFG_CTLS_DIR
    dst_dir = os.path.join(src_dir, "chains")
    os.makedirs(dst_dir, exist_ok=True)

    moved = []

    for fname in os.listdir(src_dir):
        if fname.startswith("chains_avtr_") and fname.endswith(".json"):
            src = os.path.join(src_dir, fname)
            dst = os.path.join(dst_dir, fname)
            if not os.path.exists(dst):
                shutil.move(src, dst)
                moved.append(dst)

    if moved:
        controls_log_verbose("Migrated chain files to /chains/", data={"moved": moved}, level="INFO")
    else:
        controls_log_verbose("No chain files needed migration", level="DEBUG")

# ─────────────────────────────────────────────────────────────────────────
def load_chains() -> List[Dict[str, Any]]:
    global _load_chains_logged_error
    avatar_id = user_cfg.get("current_avatar_id") or "default"
    CHAIN_SUBFOLDER = os.path.join(CFG_CTLS_DIR, "chains")
    os.makedirs(CHAIN_SUBFOLDER, exist_ok=True)
    path = os.path.join(CHAIN_SUBFOLDER, f"chains_{avatar_id}.json")

    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fp:
                chains = json.load(fp)
                chains = normalize_layout_indexes(chains)
                chains = _normalize_chain_ids(chains)
                return chains
        except Exception as e:
            if not _load_chains_logged_error:
                controls_log_verbose("Failed to load chains", level="ERROR", exc=e)
                _load_chains_logged_error = True
    return []

def save_chains(lst: list[dict]) -> None:
    import hashlib, shutil, json, os
    global _last_save_snapshot

    avatar_id = user_cfg.get("current_avatar_id") or "default"
    os.makedirs(CFG_CTLS_DIR, exist_ok=True)
    CHAIN_SUBFOLDER = os.path.join(CFG_CTLS_DIR, "chains")
    os.makedirs(CHAIN_SUBFOLDER, exist_ok=True)
    path = os.path.join(CHAIN_SUBFOLDER, f"chains_{avatar_id}.json")

    if not lst:
        controls_log_verbose(f"Not saving empty chain list to {path}", level="GUARD")
        return

    # -- ❶ Normalise IDs right away --------------------------------------
    lst = _normalize_chain_ids(lst)
    # --------------------------------------------------------------------

    # Fix / fill layout_index
    used = set()
    for i, ch in enumerate(lst):
        idx = ch.get("layout_index")
        if not isinstance(idx, int) or idx in used:
            ch["layout_index"] = i
        used.add(ch["layout_index"])

    lst.sort(key=lambda c: c.get("layout_index", 0))

    # For change-detection ignore private keys *and* keep key order stable
    def _strip_priv(chain: dict) -> dict:
        return {k: chain[k] for k in sorted(chain) if not k.startswith("_")}

    normalized = [_strip_priv(c) for c in lst]
    new_data = json.dumps(normalized, indent=2, ensure_ascii=False, sort_keys=True)
    new_hash = hashlib.sha256(new_data.encode()).hexdigest()

    if _last_save_snapshot.get(path) == new_hash:
        controls_log_verbose("No layout/content changes - skipped write.", level="DEBUG")
        return

    # Compare with current on disk (if any) before writing
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                current = json.load(f)
            current_norm = [_strip_priv(c) for c in current]
            current_norm.sort(key=lambda c: c.get("layout_index", 0))
            if current_norm == normalized:
                _last_save_snapshot[path] = new_hash
                controls_log_verbose("No meaningful changes - skipped write.", level="DEBUG")
                return
            shutil.copy2(path, path + ".bak")
            controls_log_verbose(f"Backup created → {path}.bak", level="BACKUP")
        except Exception as e:
            controls_log_verbose("Could not compare/backup chain file", level="WARN", exc=e)

    # Finally write the full (unnormalised) list so GUI keeps comments, etc.
    try:
        with open(path, "w", encoding="utf-8") as fp:
            json.dump(lst, fp, indent=2, ensure_ascii=False)
        _last_save_snapshot[path] = new_hash
        controls_log_verbose(f"Chain list written → {path}", level="INFO")
    except Exception as e:
        controls_log_verbose("Failed to save chains", level="ERROR", exc=e)

def save_chains_force(lst: List[Dict[str, Any]], layout_map: Dict[str, int]) -> None:
    import hashlib
    global _last_save_snapshot

    avatar_id = user_cfg.get("current_avatar_id") or "default"
    os.makedirs(CFG_CTLS_DIR, exist_ok=True)
    CHAIN_SUBFOLDER = os.path.join(CFG_CTLS_DIR, "chains")
    os.makedirs(CHAIN_SUBFOLDER, exist_ok=True)
    path = os.path.join(CHAIN_SUBFOLDER, f"chains_{avatar_id}.json")

    # Apply layout_map to enforce ordering and indexing
    updated_chains = []
    for name, index in sorted(layout_map.items(), key=lambda kv: kv[1]):
        match = next((c for c in lst if c.get("name") == name), None)
        if match:
            match["layout_index"] = index
            updated_chains.append(match)

    # Optionally append unmatched chains at the end
    matched_names = set(layout_map.keys())
    remaining = [c for c in lst if c.get("name") not in matched_names]
    updated_chains.extend(remaining)

    # Serialize and write
    try:
        new_data = json.dumps(updated_chains, indent=2, ensure_ascii=False)
        new_hash = hashlib.sha256(new_data.encode("utf-8")).hexdigest()

        # Backup existing if needed
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                current_data = f.read()
            try:
                if json.loads(current_data) != updated_chains:
                    shutil.copy2(path, path + ".bak")
                    controls_log_verbose(f"Backup created → {path}.bak", level="BACKUP")
            except Exception as e:
                controls_log_verbose("Could not compare JSON for backup", level="WARN", exc=e)

        with open(path, "w", encoding="utf-8") as f:
            f.write(new_data)
        _last_save_snapshot[path] = new_hash

        controls_log_verbose(f"[FORCE] Chain file forcibly written → {path}", level="INFO")

    except Exception as e:
        controls_log_verbose("Failed to forcibly save chains", level="ERROR", exc=e)

def export_chains(ui_ref=None) -> None:
    import tkinter.messagebox as messagebox

    avatar_id = user_cfg.get("current_avatar_id") or "default"
    chains = load_chains()
    os.makedirs(os.path.dirname(EXPORT_PATH), exist_ok=True)

    try:
        with open(EXPORT_PATH, "w", encoding="utf-8") as fp:
            json.dump(chains, fp, indent=2, ensure_ascii=False)

        controls_log_verbose(
            f"Exported chains for avatar '{avatar_id}' → {EXPORT_PATH}",
            level="INFO"
        )

        messagebox.showinfo(
            "Export Complete",
            f"Exported {len(chains)} chain(s) for avatar '{avatar_id}' to:\n{EXPORT_PATH}"
        )

    except Exception as e:
        controls_log_verbose("Failed to export chains", level="ERROR", exc=e)
        messagebox.showerror("Export Failed", f"Could not export chains:\n{e}")
        
def import_chains_and_register(ui_ref) -> None:
    """
    Import chains from EXPORT_PATH and add them to the current set without
    destroying anything already on-screen. Duplicate names are skipped.
    Also automatically remaps any imported pishock_devices to the user's
    default PiShock devices (as defined in user.json and devices.json).
    """
    import os, json
    from tkinter import messagebox

    # 1) ensure export file exists
    if not os.path.exists(EXPORT_PATH):
        controls_log_verbose("Import failed: file does not exist", level="ERROR")
        messagebox.showerror("Import Failed", "Export file not found.")
        return

    try:
        # 2) read the incoming chains
        with open(EXPORT_PATH, "r", encoding="utf-8") as fp:
            incoming = json.load(fp)
        incoming = normalize_layout_indexes(incoming)
        incoming = ui_ref._upgrade_chain_data(incoming)

        # 3) tag with avatar ID
        for ch in incoming:
            ch["avatar_id"] = user_cfg.get("current_avatar_id") or "default"

        # 4) load existing chains by name
        existing = load_chains()  # returns List[dict]
        existing_names = {c.get("name", "").strip().lower() for c in existing}

        # 5) filter out duplicates
        new_chains = [
            c for c in incoming
            if c.get("name", "").strip().lower() not in existing_names
        ]
        if not new_chains:
            messagebox.showinfo("Import Complete", "No new chains were found in the file.")
            controls_log_verbose("Import contained only duplicates; nothing added", level="INFO")
            return

        # 6) determine your default PiShock username
        default_user = None
        try:
            with open(USER_JSON_PATH, "r", encoding="utf-8") as uf:
                user_data = json.load(uf)
            default_user = user_data.get("pishock_username")
        except Exception:
            controls_log_verbose("Could not read default PiShock username from user.json", level="WARN")

        # 7) load all devices and pick only your default_user’s
        local_map = {}
        if default_user:
            try:
                with open(DEVICES_JSON_PATH, "r", encoding="utf-8") as df:
                    all_devices = json.load(df)
                for dev in all_devices.get(default_user, []):
                    sid = str(dev.get("shockerId"))
                    local_map[sid] = dev
            except Exception:
                controls_log_verbose("Could not read devices.json or no devices for user", level="WARN")

        # 8) remap each new chain’s pishock_devices to your own devices
        for ch in new_chains:
            imported = ch.get("pishock_devices", [])
            converted = []
            for d in imported:
                dev_id = str(d.get("id", "")).strip()
                if dev_id in local_map:
                    # preserve imported name or override with local device name:
                    dev_name = local_map[dev_id].get("name", d.get("name"))
                    converted.append({"id": dev_id, "name": dev_name})
                else:
                    controls_log_verbose(
                        f"Imported PiShock device {dev_id} not found for user {default_user}; skipping",
                        level="WARN"
                    )
            ch["pishock_devices"] = converted

        # 9) merge, save, re-index
        merged = normalize_layout_indexes(existing + new_chains)
        save_chains(merged)

        # 10) register only the newly added chains in the UI
        for ch in new_chains:
            ui_ref._register_chain(ch)

        controls_log_verbose(
            f"Imported {len(new_chains)} new chain(s); total now {len(merged)}",
            level="INFO"
        )
        messagebox.showinfo(
            "Import Complete",
            f"Added {len(new_chains)} new chain(s) (total {len(merged)})."
        )

    except Exception as e:
        controls_log_verbose("Failed to import chains", level="ERROR", exc=e)
        messagebox.showerror("Import Failed", f"Could not import chains:\n{e}")

# ─────────────────────────────────────────────── Avatar Logger
def _resolve_avatar_log_path() -> Path:
    log_dir = Path("saved") / "logs" / "controls"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / "avatar.log"

AVATAR_LOG_PATH = _resolve_avatar_log_path()

def avatar_log_verbose(
    message: str,
    data: Optional[dict] = None,
    level: str = "DEBUG",
    exc: Optional[BaseException] = None
):
    level = level.upper()
    level_code = LOG_LEVELS.get(level, LOG_LEVELS["DEBUG"])
    if level_code < CURRENT_LOG_LEVEL:
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    tag = f"[{level:<8}]"
    lines = [f"{timestamp} {tag} {message}"]

    if data:
        try:
            json_payload = json.dumps(data, indent=2, ensure_ascii=False)
            lines.append(json_payload)
        except Exception as e:
            lines.append(f"[WARN] Failed to serialize payload: {e}")

    if exc:
        trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        lines.append(f"[EXCEPTION]\n{trace}")

    full_message = "\n".join(lines)

    print(full_message)
    with AVATAR_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(full_message + "\n")

# ───────────────────────────────────────────── Avatar JSON Parsing
def parse_avatar_json(path: str):
    import codecs
    import json as _json

    try:
        with codecs.open(path, "r", "utf-8-sig") as fp:
            data = _json.load(fp)
        avatar_log_verbose(f"Parsed avatar JSON at {path}", level="INFO")
    except Exception as e:
        avatar_log_verbose(f"Failed to load avatar JSON: {path}", level="ERROR", exc=e)
        return

    for prm in data.get("parameters", []):
        inp = prm.get("input") or {}
        address = (inp.get("address") or "").strip()
        if not address.startswith(AVATAR_PREFIX):
            continue

        parts = address[len(AVATAR_PREFIX):].strip("/").split("/")
        if not parts:
            continue

        cat_parts = parts[:-1] or ["root"]
        action = parts[-1]

        param_type = (inp.get("type") or "").lower()
        default_val = [False] if param_type == "bool" else [0]

        category_id = "/".join(cat_parts).lower()
        category_name = " / ".join(cat_parts)

        yield (
            category_id,
            category_name,
            action,
            action,
            address,
            default_val,
        )

def build_rows_from_avatar_json(self, avatar_json_path: str) -> None:
    """
    Authoritative avatar ingestion path.

    • Clears existing avatar-derived rows
    • Creates rows ONLY
    • Does NOT touch TikFinity registry
    """

    avatar_log_verbose(
        "Building rows from avatar JSON",
        level="INFO",
        data={"path": avatar_json_path}
    )

    for (
        category_id,
        category_name,
        action_id,
        action_name,
        address,
        default_args,
    ) in parse_avatar_json(avatar_json_path):

        self._create_row(
            address=address,
            default_args=default_args,
            category_id=category_id,
            category_name=category_name,
            action_id=address,          # 🔥 FULL OSC PATH (REQUIRED)
            action_name=action_name,
            persist=False,
        )

    avatar_log_verbose(
        "Avatar rows built",
        level="INFO",
        data={"rows": len(self.rows)}
    )

def find_avatar_json(avatar_id: str) -> str | None:
    osc_base = pathlib.Path.home() / "AppData/LocalLow/VRChat/VRChat/OSC"
    for user_dir in osc_base.glob("usr_*/Avatars/*.json"):
        try:
            with open(user_dir, "r", encoding="utf-8") as f:
                content = f.read()
                if avatar_id in content:
                    avatar_log_verbose(f"Found avatar {avatar_id} at {user_dir}", level="INFO")
                    return str(user_dir)
        except Exception as e:
            avatar_log_verbose(f"Failed to read file during avatar search: {user_dir}", level="WARN", exc=e)
    avatar_log_verbose(f"No avatar match found for ID {avatar_id}", level="DEBUG")
    return None

def register_avatar_json(self, path: str) -> None:
    self._avatar_json_path = path
    self._avatar_id = os.path.splitext(os.path.basename(path))[0]

    avatar_log_verbose(
        "Avatar JSON registered",
        level="INFO",
        data={
            "avatar_id": self._avatar_id,
            "path": path
        }
    )

    # ➊ Clear existing rows (avatar scope)
    self.rows.clear()

    # ➋ Build rows from avatar
    self.build_rows_from_avatar_json(path)

    # ➌ ONE global sync (authoritative)
    sync_tikfinity_registry(self)

# ────────────────────────────────────────────────── helper: flatten_params
def flatten_params(params: dict, prefix: str = "") -> list[tuple[str, Any]]:
    flat = []

    if not isinstance(params, dict):
        return flat

    full = params.get("FULL_PATH", prefix)

    if "TYPE" in params and "ACCESS" in params and params["ACCESS"] in {1, 3}:
        flat.append((full, params.get("VALUE", [0])[0]))

    for sub in params.get("CONTENTS", {}).values():
        flat.extend(flatten_params(sub, full))

    return flat

# ────────────────────────────────────────────────── helper: sanitizer
def should_sanitize(address: str) -> bool:
    if address in _noisy_exact and _noisy_exact[address]:
        return True

    for prefix, flag in _noisy_prefixes.items():
        if address.startswith(prefix) and flag:
            return True

    leaf = address.rsplit("/", 1)[-1]
    if "_" in leaf and leaf.upper().startswith("VF"):
        _, suffix = leaf.split("_", 1)
        for pattern, flag in _vrcfury_fuzzy.items():
            similarity = SequenceMatcher(None, suffix, pattern).ratio()
            if flag and similarity >= FUZZY_THRESHOLD:
                return True

    return False
    
# ─────────────────────────────────────────────────────────── 6. GlobalTheme
# Theme constants
BG      = "#0a0022"   # Deep Indigo background (main frame)
PANEL   = "#140033"   # Slightly lighter panel background (inputs, toolbar)
BUTTON  = "#7a00cc"   # Rich neon purple (brand buttons)
ACCENT  = "#00c4b4"   # Aqua (selection and highlight)
FG      = "#f2f2f2"   # Soft white text

ROW_BG_IDLE      = BG
ROW_BG_HOVER     = "#140033"
ROW_BG_ACTIVE    = "#1e004f"

ROW_GLOW_COLORS  = ["#00c4b4", "#7a00cc", "#c44dff"]
ROW_PULSE_MS     = 60
ROW_PULSE_STEPS  = 6

def apply_dark(root: tk.Tk) -> None:
    style = ttk.Style(root)
    style.theme_use("clam")

    root.configure(bg=BG)

    # Global fallback
    style.configure(
        ".",
        background=BG,
        foreground=FG,
        fieldbackground=PANEL,
        bordercolor=PANEL,
        focuscolor=ACCENT,
        selectbackground=ACCENT,
        selectforeground=FG,
        troughcolor="#1c0033",
        relief="flat",
    )

    # Buttons
    style.configure(
        "TButton",
        background=BUTTON,
        foreground=FG,
        borderwidth=1,
        focusthickness=2,
        focuscolor=ACCENT
    )
    style.map(
        "TButton",
        background=[
            ("active", "#8a1be5"),
            ("pressed", ACCENT),
            ("!active", BUTTON),
        ],
        foreground=[
            ("active", FG),
            ("pressed", FG),
            ("!active", FG),
        ],
        relief=[
            ("pressed", "sunken"),
            ("!pressed", "flat"),
        ],
    )

    # Entries
    style.configure(
        "TEntry",
        background=PANEL,
        fieldbackground=PANEL,
        foreground=FG,
        insertcolor=FG,
        bordercolor=ACCENT,
    )

    # Combobox
    style.configure(
        "TCombobox",
        fieldbackground=PANEL,
        background=PANEL,
        foreground=FG,
        arrowcolor=FG,
    )
    
    #Header
    style.configure(
    "Header.TLabel",
    background=BG,
    foreground=FG,
    font=("Segoe UI", 9, "bold"),
    anchor="center"
    )
    
    #Header Label
    style.configure(
        "HeaderAddr.TLabel",
        background=BG,
        foreground=FG,
        font=("Segoe UI", 9, "bold"),
        anchor="w"
    )

    # Status bar labels
    style.configure(
        "StatusName.TLabel",
        background=BG,
        foreground=FG,
        font=("Segoe UI", 10, "bold"),
        anchor="w"
    )

    style.configure(
        "StatusDisconnected.TLabel",
        background=BG,
        foreground="#ff4d4d",  # soft neon red
        font=("Segoe UI", 10),
        anchor="w"
    )

    style.configure(
        "StatusConnected.TLabel",
        background=BG,
        foreground=ACCENT,     # aqua
        font=("Segoe UI", 10),
        anchor="w"
    )

    style.configure(
        "StatusWaiting.TLabel",
        background=BG,
        foreground="#ffcc66",  # warm amber
        font=("Segoe UI", 10),
        anchor="w"
    )
    
    style.configure(
        "StatusError.TLabel",
        background=BG,
        foreground="#ff9933",  # warm orange (error / warning)
        font=("Segoe UI", 10),
        anchor="w"
    )

    style.map(
        "TCombobox",
        fieldbackground=[("readonly", PANEL), ("active", "#1e0050")],
        background=[("readonly", PANEL), ("active", "#1e0050")],
        foreground=[("readonly", FG), ("active", FG)]
    )

    # Scrollbars
    style.configure(
        "Vertical.TScrollbar",
        background=BUTTON,
        troughcolor=PANEL,
        arrowcolor=FG,
        bordercolor=BG,
    )

    # Chain Buttons - Electric Glow Style
    style.configure(
        "ChainPlay.TButton",
        background="#7a00cc",       # rich base
        foreground=FG,
        padding=(10, 6),
        anchor="w",
        relief="flat",
        font=("Segoe UI", 10, "bold"),
        borderwidth=2,
        focusthickness=3,
        focuscolor="#c44dff",       # inner neon edge
    )

    style.map(
        "ChainPlay.TButton",
        background=[
            ("pressed", "#00c4b4"),     # Aqua flash
            ("active", "#a400ff"),      # Brighter violet on hover
            ("!active", "#7a00cc"),     # Normal
        ],
        foreground=[
            ("pressed", FG),
            ("active", FG),
            ("!active", FG)
        ],
        relief=[
            ("pressed", "sunken"),
            ("!pressed", "flat")
        ],
        focuscolor=[
            ("focus", "#ff77ff")        # subtle aura
        ]
    )

    # Spinbox
    try:
        style.configure(
            "TSpinbox",
            background=PANEL,
            fieldbackground=PANEL,
            foreground=FG,
            bordercolor=BUTTON,
        )
    except Exception:
        pass

    # Danger button
    style.configure(
        "Danger.TButton",
        background="#a10000",
        foreground="white",
        font=("Segoe UI", 10, "bold"),
        borderwidth=1,
        focusthickness=2,
        focuscolor="#ff4444"
    )
    style.map(
        "Danger.TButton",
        background=[
            ("active", "#cc0000"),
            ("pressed", "#ff2222"),
            ("!active", "#a10000"),
        ],
        foreground=[
            ("active", "white"),
            ("pressed", "white"),
            ("!active", "white"),
        ],
        relief=[
            ("pressed", "sunken"),
            ("!pressed", "flat"),
        ],
    )

    # Checkbuttons
    style.configure(
        "TCheckbutton",
        background=BG,
        foreground=FG,
        indicatorcolor=ACCENT,
        indicatordiameter=12,
        indicatormargin=4,
        indicatorrelief="flat",
        focuscolor=BUTTON
    )
    style.map(
        "TCheckbutton",
        background=[("selected", BG), ("active", BG)],
        foreground=[("selected", FG), ("active", FG)],
        indicatorbackground=[("selected", "#ffffff"), ("!selected", PANEL)],
        indicatorforeground=[("selected", "#000000")],
        indicatorcolor=[("selected", "#ffffff")],
    )


class GlowButton(Canvas):
    def __init__(self, parent, text, command=None, width=200, height=36,
                 glow="#a020f0", fg="#ffffff", bg="#14001a", font=None,
                 bold=True, anchor="center", wraplength=None):
        super().__init__(parent, width=width, height=height,
                         bg=bg, highlightthickness=0, bd=0)

        self.command = command
        self.fg = fg
        self.bg = bg
        self.glow = glow
        self._animating = False
        self._glow_index = 0
        self.font = font or ("Segoe UI", 10, "bold" if bold else "normal")
        self.anchor = anchor
        self.wraplength = wraplength or width
        self._raw_text = text

        # Base visuals
        self.shadow = self.create_rectangle(2, 2, width - 2, height - 2,
                                            fill=bg, outline="", width=0)
        self.rect = self.create_rectangle(4, 4, width - 4, height - 4,
                                          fill=bg, outline=glow, width=2)

        # Position text based on anchor
        x = {
            "w": 10,
            "e": width - 10,
            "center": width // 2
        }.get(anchor, width // 2)

        self.text = self.create_text(
            x, height // 2,
            text=self._truncate_text(text),
            fill=fg,
            font=self.font,
            anchor=anchor,
            width=self.wraplength  # used for text wrapping/truncation
        )

        self.bind("<Enter>", self._hover)
        self.bind("<Leave>", self._leave)
        self.bind("<Button-1>", lambda e: command() if command else None)

    def set_glow(self, glow_color: str):
        self.glow = glow_color
        self.itemconfig(self.rect, outline=self.glow)

    def _hover(self, _):
        self._animating = True
        self._pulse_glow()

    def _leave(self, _):
        self._animating = False
        self.itemconfig(self.rect, outline=self.glow, width=2)

    def _pulse_glow(self):
        if not self._animating or not self.winfo_exists():
            return

        pulse_widths = [2, 3, 4, 3, 2]
        width = pulse_widths[self._glow_index % len(pulse_widths)]
        self.itemconfig(self.rect, width=width)
        self._glow_index += 1
        self.after(75, self._pulse_glow)

    def _truncate_text(self, txt: str) -> str:
        max_chars = int(self["width"]) // 10  # crude estimate
        return txt if len(txt) <= max_chars else txt[:max_chars - 1] + "…"
        
# ─────────────────────────────────────────────────────────── 7. GUI
def post_gui_event(service: str, event: str,
                   payload: dict[str, Any] | None = None) -> None:
    q.put((service, event, payload or {}))

class DarkAskString(tk.Toplevel):
    def __init__(self, parent, title: str, prompt: str, initialvalue: str = ""):
        super().__init__(parent)
        self.title(title)
        self.configure(bg=BG)
        self.transient(parent)  # Stay above parent
        self.grab_set()         # Modal (block clicks to main window)

        self.result = None

        # Icon (optional)
        try:
            if os.name == "nt":
                ico_path = resolve_runtime_path("assets/logo.ico")
                self.iconbitmap(ico_path)
            else:
                self.iconphoto(False, parent._icon_img)
        except Exception:
            pass

        # Prompt Label
        tk.Label(self, text=prompt, bg=BG, fg=FG, font=("Segoe UI", 10)).pack(padx=10, pady=(10,5))

        # Entry Field
        self.entry_var = tk.StringVar(value=initialvalue)
        entry = ttk.Entry(self, textvariable=self.entry_var, width=40)
        entry.pack(padx=10, pady=5)
        entry.focus()

        # Buttons
        btn_frame = tk.Frame(self, bg=BG)
        btn_frame.pack(pady=(5,10))

        ttk.Button(btn_frame, text="OK", command=self._on_ok).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Cancel", command=self._on_cancel).pack(side="left", padx=5)

        self.bind("<Return>", lambda e: self._on_ok())
        self.bind("<Escape>", lambda e: self._on_cancel())

        # Center over parent window
        self.update_idletasks()
        x = parent.winfo_rootx() + (parent.winfo_width() // 2) - (self.winfo_width() // 2)
        y = parent.winfo_rooty() + (parent.winfo_height() // 2) - (self.winfo_height() // 2)
        self.geometry(f"+{x}+{y}")

        self.wait_window()

    def _on_ok(self):
        self.result = self.entry_var.get()
        self.destroy()

    def _on_cancel(self):
        self.result = None
        self.destroy()

class DarkAskFloat(tk.Toplevel):
    def __init__(self, parent, title: str, prompt: str, initialvalue: float = 0.0, minvalue: float = None, maxvalue: float = None):
        super().__init__(parent)
        self.title(title)
        self.configure(bg=BG)
        self.transient(parent)
        self.grab_set()

        self.result = None
        self.minvalue = minvalue
        self.maxvalue = maxvalue

        # Icon
        try:
            if os.name == "nt":
                ico_path = resolve_runtime_path("assets/logo.ico")
                self.iconbitmap(ico_path)
            else:
                self.iconphoto(False, parent._icon_img)
        except Exception:
            pass

        # Prompt
        tk.Label(self, text=prompt, bg=BG, fg=FG, font=("Segoe UI", 10)).pack(padx=10, pady=(10,5))

        # Entry
        self.entry_var = tk.StringVar(value=str(initialvalue))
        entry = ttk.Entry(self, textvariable=self.entry_var, width=20)
        entry.pack(padx=10, pady=5)
        entry.focus()

        # Buttons
        btn_frame = tk.Frame(self, bg=BG)
        btn_frame.pack(pady=(5,10))

        ttk.Button(btn_frame, text="OK", command=self._on_ok).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Cancel", command=self._on_cancel).pack(side="left", padx=5)

        self.bind("<Return>", lambda e: self._on_ok())
        self.bind("<Escape>", lambda e: self._on_cancel())

        # Center over parent
        self.update_idletasks()
        x = parent.winfo_rootx() + (parent.winfo_width() // 2) - (self.winfo_width() // 2)
        y = parent.winfo_rooty() + (parent.winfo_height() // 2) - (self.winfo_height() // 2)
        self.geometry(f"+{x}+{y}")

        self.wait_window()

    def _on_ok(self):
        val = self.entry_var.get().strip()
        try:
            fval = float(val)
            if self.minvalue is not None and fval < self.minvalue:
                raise ValueError("Below min")
            if self.maxvalue is not None and fval > self.maxvalue:
                raise ValueError("Above max")
            self.result = fval
            self.destroy()
        except Exception:
            messagebox.showerror("Invalid Input", "Please enter a valid number.", parent=self)

    def _on_cancel(self):
        self.result = None
        self.destroy()

class ToolTip:
    _active_tooltip = None  # GLOBAL singleton guard

    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tip_window = None

        # ttk-safe hover detection
        self.widget.bind("<Motion>", self.show_tip, add="+")
        self.widget.bind("<Leave>", self.hide_tip, add="+")

    def show_tip(self, event=None):
        if not self.text:
            return

        # If another tooltip is active, kill it
        if ToolTip._active_tooltip and ToolTip._active_tooltip is not self:
            ToolTip._active_tooltip.hide_tip()

        # Already visible → do nothing
        if self.tip_window:
            return

        try:
            x = self.widget.winfo_rootx()
            y = self.widget.winfo_rooty()
            h = self.widget.winfo_height()
        except Exception:
            return

        x += 12
        y += h + 8

        self.tip_window = tw = tk.Toplevel(self.widget.winfo_toplevel())
        ToolTip._active_tooltip = self

        tw.wm_overrideredirect(True)
        tw.attributes("-topmost", True)
        tw.wm_geometry(f"+{x}+{y}")

        label = tk.Label(
            tw,
            text=self.text,
            justify="left",
            background="#ffffe0",
            foreground="#000000",
            relief="solid",
            borderwidth=1,
            font=("Segoe UI", 9)
        )
        label.pack(ipadx=4, ipady=2)

        # Windows reliability
        tw.update_idletasks()
        tw.lift()

    def hide_tip(self, event=None):
        if self.tip_window:
            try:
                self.tip_window.destroy()
            except Exception:
                pass
            self.tip_window = None

        if ToolTip._active_tooltip is self:
            ToolTip._active_tooltip = None

class AutoCompleteCombobox(ttk.Combobox):
    def set_completion_list(self, completion_dict):
        self._completion_dict = completion_dict
        self._completion_list = sorted(completion_dict.keys(), key=str.lower)
        self._hits = []
        self._hit_index = 0
        self.position = 0

        # Load values into the Combobox dropdown
        self['values'] = self._completion_list
        self.bind('<KeyRelease>', self._handle_keyrelease)

    def _autocomplete(self, delta=0):
        typed = self.get().lower()
        if delta:
            self.delete(self.position, tk.END)
        else:
            self.position = len(self.get())

        self._hits = [item for item in self._completion_list if typed in item.lower()]

        if self._hits:
            self.delete(0, tk.END)
            self.insert(0, self._hits[0])
            self.select_range(self.position, tk.END)
            self['values'] = self._hits
        else:
            self['values'] = self._completion_list  # fallback

    def _handle_keyrelease(self, event):
        if event.keysym in ("BackSpace", "Left", "Right"):
            self.position = self.index(tk.END)
        elif len(event.keysym) == 1 or event.keysym in ("space",):
            self._autocomplete()

# ------------------------------------------------------ scrollbar (THEMED + SCOPED)
def build_scrollable_root(parent, bg, *, use_grid=True, row=1, column=0):
    """
    Creates a vertically scrollable content area that:
    - Respects custom ttk styles (Vertical.TScrollbar)
    - Auto-expands horizontally
    - Is mouse-wheel scoped (no global hijack)
    - Works with GRID or PACK safely
    """

    # Outer container (keeps scrollbar visually aligned)
    container = tk.Frame(parent, bg=bg)

    if use_grid:
        container.grid(row=row, column=column, sticky="nsew")
        parent.grid_rowconfigure(row, weight=1)
        parent.grid_columnconfigure(column, weight=1)
    else:
        container.pack(fill="both", expand=True)

    # Canvas (scroll surface)
    canvas = tk.Canvas(
        container,
        bg=bg,
        highlightthickness=0,
        bd=0,
        relief="flat"
    )

    # Themed scrollbar - CRITICAL
    vbar = ttk.Scrollbar(
        container,
        orient="vertical",
        command=canvas.yview,
        style="Vertical.TScrollbar"
    )

    canvas.configure(yscrollcommand=vbar.set)

    # Layout INSIDE container (grid is safe here)
    canvas.grid(row=0, column=0, sticky="nsew")
    vbar.grid(row=0, column=1, sticky="ns")

    container.grid_rowconfigure(0, weight=1)
    container.grid_columnconfigure(0, weight=1)

    # Inner content frame (actual UI root)
    content = tk.Frame(canvas, bg=bg)

    window_id = canvas.create_window(
        (0, 0),
        window=content,
        anchor="nw"   # REQUIRED for width sync
    )

    # --------------------------------------------------
    # Sync scroll region + width locking
    # --------------------------------------------------
    def _sync_content(_=None):
        # Update scroll height only
        canvas.configure(scrollregion=(0, 0, canvas.winfo_width(), content.winfo_reqheight()))

        # Lock content width to canvas width
        cw = canvas.winfo_width()
        if cw > 1:
            canvas.itemconfigure(window_id, width=cw)

    content.bind("<Configure>", _sync_content)
    canvas.bind("<Configure>", _sync_content)

    # --------------------------------------------------
    # Mouse wheel (SCOPED - no global hijack)
    # --------------------------------------------------
    def _on_mousewheel(event):
        if event.delta:
            canvas.yview_scroll(int(-1 * event.delta / 120), "units")

    def _bind_mousewheel(_):
        canvas.bind_all("<MouseWheel>", _on_mousewheel)
        canvas.bind_all("<Button-4>", lambda e: canvas.yview_scroll(-1, "units"))
        canvas.bind_all("<Button-5>", lambda e: canvas.yview_scroll(1, "units"))

    def _unbind_mousewheel(_):
        canvas.unbind_all("<MouseWheel>")
        canvas.unbind_all("<Button-4>")
        canvas.unbind_all("<Button-5>")

    canvas.bind("<Enter>", _bind_mousewheel)
    canvas.bind("<Leave>", _unbind_mousewheel)

    return canvas, content

# ────────────────────────────────────────────────────────────────────────────────
# PiShock WebSocket (v2) Client - StreamConnector Edition
# Compatible with https://broker.pishock.com/v2   - 2025-05-13
# ────────────────────────────────────────────────────────────────────────────────

_cache_path = pathlib.Path(PISHOCK_DEVICE_FILE)
_cache_path.parent.mkdir(parents=True, exist_ok=True)

# --- User-Provided Global Context ---
# NOTE: The user's full code context (imports, LOG_LEVELS, CURRENT_LOG_LEVEL) 
# is assumed to be available. We will mock the minimum requirements for a runnable function.
# In a real environment, LOG_LEVELS and CURRENT_LOG_LEVEL would be defined elsewhere.

# --- User-Provided Path ---
PISHOCK_LOG_PATH = Path("saved/logs/haptics/pishock.log")
PISHOCK_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
# -----------------------------------

def pishock_log_verbose(
    message: str,
    data: Optional[dict[str, Any]] = None,
    level: str = "DEBUG",
    exc: Optional[BaseException] = None,
):
    """
    Pretty-print PiShock logs with structured formatting for commands,
    JSON-like nested data, and exceptions.
    """

    def _pretty_format(value: Any, indent: int = 2) -> list[str]:
        """Recursive pretty printer for dicts/lists/tuples."""
        spacer = " " * indent
        
        if isinstance(value, dict):
            lines = []
            for k, v in value.items():
                k_str = str(k)
                if isinstance(v, (dict, list, tuple)) and v:
                    # Use bold/star markers for keys that lead to nested data
                    lines.append(f"{spacer}**{k_str}**:")
                    lines.extend(_pretty_format(v, indent + 2))
                else:
                    lines.append(f"{spacer}{k_str}: {v}")
            return lines
            
        elif isinstance(value, (list, tuple)):
            lines = []
            for i, item in enumerate(value):
                if isinstance(item, (dict, list, tuple)) and item:
                    lines.append(f"{spacer}- Item {i+1}:")
                    lines.extend(_pretty_format(item, indent + 2))
                else:
                    lines.append(f"{spacer}- {item}")
            return lines
            
        else:
            return [f"{spacer}{value}"]

    try:
        level = level.upper()
        level_code = LOG_LEVELS.get(level, LOG_LEVELS["DEBUG"])
        if level_code < CURRENT_LOG_LEVEL:
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        tag = f"[{level:<8}]"

        # ── Header ─────────────────────────────
        lines: list[str] = [f"{timestamp} {tag} {message}"]
        
        if data:
            # Prepare to display data, checking for nested 'payload' for Websocket messages
            display_data = data.copy() if isinstance(data, dict) else data
            
            # Extract nested payload if present (common for WS frames)
            payload = None
            if isinstance(display_data, dict) and "payload" in display_data:
                payload = display_data.pop("payload")
            
            # --- Special Command Handling (e.g., PUBLISH) ---
            publish_commands = None
            if isinstance(display_data, dict) and "PublishCommands" in display_data:
                 publish_commands = display_data.pop("PublishCommands")
            elif isinstance(payload, dict) and "PublishCommands" in payload:
                 publish_commands = payload.pop("PublishCommands")

            if publish_commands:
                lines.append("── PUBLISH COMMANDS ───────────────────")
                
                for idx, cmd in enumerate(publish_commands, start=1):
                    lines.append(f"📦 Command {idx}:")
                    target = cmd.get("Target", "<No Target>")
                    body = cmd.get("Body", {})
                    lines.append(f"  Target: **{target}**")
                    lines.append("  Body:")
                    lines.extend(_pretty_format(body, indent=4))
            # -----------------------------------------------

            # ── General Data / Remaining Payload ───────────
            if isinstance(display_data, dict) and display_data:
                 lines.append("── Data ───────────────────────────────")
                 lines.extend(_pretty_format(display_data, indent=2))
            
            if payload:
                 lines.append("── Payload ────────────────────────────")
                 lines.extend(_pretty_format(payload, indent=2))
            
            # Handle Raw Frame/Message data from the log example
            if message in ("Raw WebSocket frame", "WebSocket message received") and isinstance(data, dict):
                 if "frame" in data:
                     lines.append("── Raw Frame ──────────────────────────")
                     lines.append(f"  {data['frame']}")
                 if "message" in data:
                     lines.append("── Message Body ───────────────────────")
                     lines.extend(_pretty_format(data["message"], indent=2))
            # -----------------------------------------------

        # ── Exception pretty-print ─────────────
        if exc:
            lines.append("── Exception ───────────────────────────")
            trace = ''.join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)
            )
            # Format traceback lines with a consistent indent
            lines.extend([f"  {line.rstrip()}" for line in trace.splitlines()])

        # ── Output ─────────────────────────────
        full_message = "\n".join(lines)

        print(full_message)
        with PISHOCK_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(full_message + "\n")

    except Exception as outer_exc:
        # last-ditch fallback if the logger itself fails
        fallback_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(f"[{fallback_timestamp} [LOGGER ERROR]] Failed to log message: '{message}'")
        print(f"  Logger Exception: {outer_exc}")

class PiShockClient:

    # ─────────────────────────── initialisation ────────────────────────────
    def __init__(
        self,
        username: str,
        apikey:   str,
        *,
        on_message:      Optional[Callable[[Dict[str, Any]], None]] = None,
        on_connected:    Optional[Callable[[], None]]               = None,
        on_disconnected: Optional[Callable[[], None]]               = None,
        q=None,
    ):

        #Call our logger for pishock
        self.log = pishock_log_verbose
        
        # credentials
        self.username = username.strip().lower()
        self.apikey   = apikey.strip()
        self.ws_url   = (
            "wss://broker.pishock.com/v2"
            f"?Username={self.username}&ApiKey={self.apikey}"
        )

        # runtime state
        self.loop        : Optional[asyncio.AbstractEventLoop] = None
        self.ws          = None
        self.connected   = False
        self.subscribed  = False
        self._hb_task    = None          # heartbeat coroutine handle
        self._bo         = 5             # reconnect back-off seconds (seed)

        # ── NEW: busy gating ─────────────────────────────
        self._busy_global  : float = 0        # epoch-secs: longest in-flight op
        self._busy_lock    = asyncio.Lock()   # guards _busy_global updates
        self._thread_busy_lock = threading.Lock()
        self._other_shock_active_until = 0.0
        self._other_shock_lock = asyncio.Lock()

        # device bookkeeping
        self.pishock_user_id : Optional[int]      = None
        self.device_client_map: Dict[str, str]    = {}   # shockerId → clientId
        self.devices:            List[dict]       = []

        # callbacks / IPC
        self.on_message      = on_message
        self.on_connected    = on_connected
        self.on_disconnected = on_disconnected
        self.q               = q

        # warm-start device map from cache (non-blocking)
        self._load_cached_devices()
        self._e_stop_active = False
        self._active_sessions: dict[str, int] = {}  # shockerId → endTime (epoch ms)
        self._suppress_support_popup = False

    # ───────────────────────────── public API ───────────────────────────────
    def start(self) -> None:
        if self.loop:
            return  # already running

        def _runner():
            try:
                asyncio.set_event_loop(asyncio.new_event_loop())
                self.loop = asyncio.get_event_loop()
                self.log("Connector thread spun-up", level="DEBUG")
                self.loop.create_task(self._connect_loop())
                self.loop.run_forever()
            except Exception as exc:
                self.log("Unhandled error in connector thread", level="CRITICAL", exc=exc)

        threading.Thread(target=_runner, daemon=True).start()

    def is_running(self) -> bool:
        return self.loop is not None and self.loop.is_running()

# ───────────────────────────── central zap call ───────────────────────────────
    def send_shock_ws(
        self,
        device_ids: Union[str, List[str]] = "all",
        mode: str = "shock",
        intensity: int = 50,
        duration: int = 1000,
        retries: int = 3,
    ) -> None:
        """
        Enqueue shock(s) via WS, with start/dispatched/error events.
        """
        # 🎬 emit start event
        if self.q:
            try:
                self.q.put(("pishock", "ws_send_start", {
                    "device_ids": device_ids,
                    "mode": mode,
                    "intensity": intensity,
                    "duration": duration
                }))
            except Exception:
                pass

        # resolve IDs
        if device_ids == "all":
            ids = [str(d["shockerId"]) for d in self.devices if d.get("shockerId") is not None]
        elif isinstance(device_ids, str):
            ids = [device_ids]
        else:
            ids = [str(d) for d in device_ids]

        def _worker():
            try:
                # wait for busy
                self._thread_busy_lock.acquire()
                self._thread_busy_lock.release()

                # retry loop
                for attempt in range(1, retries + 1):
                    if self.connected and self.loop:
                        try:
                            asyncio.run_coroutine_threadsafe(
                                self._send_publish_commands(ids, mode, intensity, duration),
                                self.loop,
                            )
                            #dispatched event
                            if self.q:
                                self.q.put(("pishock", "ws_send_dispatched", {
                                    "device_ids": ids,
                                    "mode": mode,
                                    "intensity": intensity,
                                    "duration": duration,
                                    "attempt": attempt
                                }))
                            self.log("Shock commands dispatched (after busy)", {
                                "device_ids": ids,
                                "mode": mode,
                                "intensity": intensity,
                                "duration": duration,
                                "attempt": attempt
                            }, level="DEBUG")
                            return
                        except Exception as exc:
                            self.log("Failed to submit shock command to loop", level="ERROR", exc=exc)
                            raise

                    time.sleep(0.15)

                # abort
                self.log("WebSocket not ready after retries - aborting.", {
                    "device_ids": ids,
                    "mode": mode,
                    "intensity": intensity,
                    "duration": duration,
                    "retries": retries
                }, level="WARN")
                if self.q:
                    self.q.put(("pishock", "ws_send_abort", {
                        "device_ids": ids,
                        "retries": retries
                    }))

            except Exception as exc_outer:
                # 💥 error event
                self.log("Error in send_shock_ws worker", level="ERROR", exc=exc_outer)
                if self.q:
                    self.q.put(("pishock", "ws_send_error", {"error": str(exc_outer)}))

        threading.Thread(target=_worker, daemon=True).start()

    # ───────────────────────────── internals ────────────────────────────────
    # --------------- heartbeat helper (pushes to global queue) --------------
    def _heartbeat(self, state: str) -> None:
        if self.q:
            self.q.put(("pishock", state, {}))
        self.log("Heartbeat signal sent", {"state": state}, level="DEBUG")

    # --------------------------- device-cache helpers -----------------------
    def _load_cached_devices(self):
        if not _cache_path.exists():
            self.log("No cached devices found", level="DEBUG")
            return
        try:
            with _cache_path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)

            # Backward compatibility: flat list → wrap
            if isinstance(data, list):
                self.log("Detected legacy flat cache format - wrapping under '(you)'", level="WARN")
                data = {"(you)": data}

            self._ingest_device_records(data)
            self.log(f"Loaded {len(self.devices)} device records from cache", level="DEBUG")
        except Exception as exc:
            self.log("Failed to read device cache", level="ERROR", exc=exc)

    def _save_device_cache(self):
        try:
            self._dedupe_devices()
            grouped: Dict[str, List[dict]] = {}

            for device in self.devices:
                owner = device.get("fromUser") or "(unknown)"
                grouped.setdefault(owner, []).append(device)

            with _cache_path.open("w", encoding="utf-8") as fh:
                json.dump(grouped, fh, indent=2)

            self.log(f"Saved {len(self.devices)} unique device records", {
                "path": str(_cache_path)
            }, level="DEBUG")
        except Exception as exc:
            self.log("Failed to write device cache", level="ERROR", exc=exc)

    def _dedupe_devices(self):
        from collections import defaultdict

        seen_ids = {}
        for device in self.devices:
            sid = str(device.get("shockerId"))
            cid = str(device.get("clientId"))
            key = f"{sid}:{cid}"

            existing = seen_ids.get(key)
            if not existing:
                seen_ids[key] = device
            else:
                # Keep the better (more complete) record
                is_placeholder = lambda d: (
                    d.get("fromUser") in ("(cache)", "(you)", None)
                    or d.get("name", "").startswith("(cache)")
                )
                if is_placeholder(existing) and not is_placeholder(device):
                    seen_ids[key] = device

        removed = len(self.devices) - len(seen_ids)
        self.devices = list(seen_ids.values())
        if removed > 0:
            self.log(f"Removed {removed} duplicate/incomplete device records", level="DEBUG")

# ----------------------------- support popup -----------------------------
    def _show_support_popup(self, custom_message: str | None = None):
        import tkinter as tk
        from tkinter import ttk
        import time
        import webbrowser

        # ─────────────────────────────────────────────────────────
        # Guards
        # ─────────────────────────────────────────────────────────

        # Prevent re-entrancy (only one popup at a time)
        if getattr(self, "_support_popup_active", False):
            return

        # Cooldown suppression (do not spam user)
        cooldown_s = 120  # 2 minutes
        last_ts = getattr(self, "_last_support_popup_ts", 0)
        now = time.time()

        if now - last_ts < cooldown_s:
            return

        self._support_popup_active = True
        self._last_support_popup_ts = now

        root = tk._default_root
        if not root or not root.winfo_exists():
            self._support_popup_active = False
            return

        def _launch():
            try:
                win = tk.Toplevel(root)
                win.title("PiShock Connection Issue")
                win.grab_set()
                win.transient(root)
                win.geometry("425x170")
                win.resizable(False, False)

                if hasattr(root, "apply_theme_to_toplevel"):
                    root.apply_theme_to_toplevel(win)

                default_msg = (
                    "A connection problem with PiShock was detected.\n\n"
                    "Please restart your PiShock hub (unplug and replug).\n\n"
                )
                msg_text = custom_message if custom_message else default_msg

                BG = root.cget("bg")
                FG = "white"

                tk.Label(
                    win,
                    text=msg_text,
                    wraplength=360,
                    justify="left",
                    bg=BG,
                    fg=FG,
                    font=("Segoe UI", 10)
                ).pack(padx=20, pady=(18, 12))

                btn_frame = tk.Frame(win, bg=BG)
                btn_frame.pack(pady=(0, 16))

                def _ack_and_open_support():
                    webbrowser.open("https://discord.gg/MrNb9CQyYA")
                    _close()

                def _ack_and_close():
                    _close()

                def _close():
                    try:
                        win.destroy()
                    finally:
                        self._support_popup_active = False

                ttk.Button(btn_frame, text="Contact Support", command=_ack_and_open_support).pack(side="left", padx=6)
                ttk.Button(btn_frame, text="OK", command=_ack_and_close).pack(side="left", padx=6)

                win.update_idletasks()
                x = root.winfo_rootx() + (root.winfo_width() // 2) - (win.winfo_width() // 2)
                y = root.winfo_rooty() + (root.winfo_height() // 2) - (win.winfo_height() // 2)
                win.geometry(f"+{x}+{y}")

                # Ensure flag clears if user closes window manually
                win.protocol("WM_DELETE_WINDOW", _close)

            except Exception:
                self._support_popup_active = False

        # Schedule safely on main thread
        root.after(0, _launch)
    
#------------------------------ websocket driver / loop -----------------------
    async def _safe_close(self):
        try:
            if self.ws:
                await self.ws.close(code=4000, reason="reconnect")
                self.log("WebSocket safely closed", level="DEBUG")
        except Exception as e:
            self.log("WebSocket close failed", level="WARN", exc=e)
        finally:
            self.ws = None

    async def _keepalive(self):
        while self.connected:
            try:
                await asyncio.sleep(25)
                await self._send_simple("PING")
                self.log("Sent keepalive PING", level="DEBUG")
            except Exception as e:
                self.log("Keepalive failed, triggering reconnect", level="WARN", exc=e)
                break  # _connect_loop will handle reconnect
                
# ----------------------------- connect loop driver -----------------------------
    async def _connect_loop(self):
        import ssl, random, asyncio, websockets

        attempt_count = 0
        backoff = getattr(self, "_bo", 1)

        if not hasattr(self, "_broker_error_count"):
            self._broker_error_count = 0

        while True:
            try:
                self.log("Attempting WebSocket connection", {"url": self.ws_url}, level="INFO")
                ssl_ctx = ssl.create_default_context()
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=None,
                    max_size=2**20,
                    ssl=ssl_ctx,
                ) as ws:
                    self.ws = ws
                    self.connected = True
                    attempt_count = 0
                    self.log("WebSocket connected", level="INFO")

                    # heartbeat + callbacks
                    self._heartbeat("connected")
                    if self.on_connected:
                        self.on_connected()
                    self._hb_task = asyncio.create_task(self._keepalive())

                    # initialise session
                    self.log("➤ initialise_session()", level="DEBUG")
                    await self._initialise_session()

                    # start receive loop (important!)
                    self.log("➤ Starting WebSocket receive loop", level="DEBUG")
                    self._ws_task = asyncio.create_task(self._ws_receive_loop())

                    # keep connect loop alive while WS is open
                    await self._ws_task

            except Exception as exc:
                self.log("WebSocket error during _connect_loop", level="ERROR", exc=exc)
                attempt_count += 1
                if attempt_count >= 3:
                    self._show_support_popup(
                        "A Critical Error has occurred with PiShock retrieval! "
                    )
                    attempt_count = 0

            finally:
                if getattr(self, "_hb_task", None):
                    self._hb_task.cancel()
                await self._safe_close()

                if self.connected:
                    self._heartbeat("disconnected")
                    if self.on_disconnected:
                        self.on_disconnected()

                self.connected = False
                self.subscribed = False
                self._last_subscribe_payload = None

                # exponential backoff
                backoff = min(backoff * 2, 60)
                delay = backoff + random.uniform(0, 2)
                self._bo = backoff
                self.log("Reconnecting delay", {"backoff_seconds": round(delay, 2)}, level="DEBUG")
                await asyncio.sleep(delay)

# ----------------------------- initialise_session -----------------------------
    async def _initialise_session(self):
        try:
            if self.q:
                self.q.put(("pishock", "session_init_start", {}))

            self.log("→ Sending PING", level="DEBUG")
            if self.q:
                self.q.put(("pishock", "session_ping", {}))
            await self._send_simple("PING")

            self.log("→ Fetching devices", level="DEBUG")
            if self.q:
                self.q.put(("pishock", "session_fetch_devices", {}))
            await self._fetch_devices()

            self.log("→ Building SUBSCRIBE targets", level="DEBUG")
            targets = [f"{d['clientId']}-{t}" for d in self.devices for t in ("ping","log") if d.get("clientId")]
            self.log("→ Targets for SUBSCRIBE", {"targets": targets}, level="DEBUG")

            self.log("→ Sending SUBSCRIBE", level="DEBUG")
            if self.q:
                self.q.put(("pishock", "session_subscribe", {"targets": targets}))
            await self._send_simple("SUBSCRIBE", {"Targets": targets})
        except Exception as exc:
            self.log("Exception in _initialise_session", level="ERROR", exc=exc)
            if self.q:
                self.q.put(("pishock", "session_init_error", {"error": str(exc)}))

# ----------------- Central WebSocket Receiver -----------------
    async def _ws_receive_loop(self):
        """Continuously receive messages from WebSocket and dispatch to _listen."""
        while self.connected and self.ws:
            try:
                raw_msg = await self.ws.recv()
                try:
                    msg = json.loads(raw_msg)
                except Exception:
                    self.log("Failed to parse WS message", {"raw": raw_msg}, level="WARN")
                    continue
                await self._listen(msg)
            except websockets.ConnectionClosedOK as ok:
                self.log("WebSocket closed normally", {"code": ok.code, "reason": ok.reason}, level="INFO")
                break
            except websockets.ConnectionClosedError as cerr:
                self.log("WebSocket forced closed", {"code": cerr.code, "reason": cerr.reason}, level="ERROR")
                break
            except Exception as e:
                self.log("Exception in WebSocket receive loop", level="ERROR", exc=e)
                await asyncio.sleep(1)

# ----------------- WebSocket message handler -----------------
    async def _listen(self, msg: dict):
        """
        Handles BROKER errors, PUBLISH-ACKs, SUBSCRIBE and UNSUBSCRIBE confirmations,
        and forwards other messages to `on_message`.
        """
        try:
            src = msg.get("Source")
            orig = msg.get("OriginalCommand")
            op = (msg.get("Operation") or "").upper()
            is_error = msg.get("IsError", False)

            # ---------------- BROKER errors ----------------
            if src == "BROKER" and is_error:
                code = (msg.get("ErrorCode") or "").upper()

                # Known connection errors - DO NOT SKIP
                if code in ("CONNECT_ERROR", "CONNECTION_ERROR"):
                    self.log(f"Broker connection error: {code}", msg, level="ERROR")

                    # Ensure counter exists
                    if not hasattr(self, "_broker_error_count"):
                        self._broker_error_count = 0

                    self._broker_error_count += 1

                    # Trip global busy gate to prevent overlapping executions
                    try:
                        import time
                        with self._thread_busy_lock:
                            self._busy_global = max(self._busy_global, time.time() + 5.0)
                    except Exception:
                        pass

                    # Ensure suppress flag exists
                    if not hasattr(self, "_suppress_support_popup"):
                        self._suppress_support_popup = False

                    # Escalate after threshold
                    if self._broker_error_count >= 3 and not self._suppress_support_popup:
                        self._show_support_popup(
                            "PiShock Broker connection is failing repeatedly.\n\n"
                            "This is NOT a cosmetic issue.\n\n"
                            "Please check:\n"
                            "• Internet connection\n"
                            "• PiShock service status\n"
                            "• API key validity\n\n"
                        )
                        self._broker_error_count = 0

                    return

            # ---------------- PUBLISH-ACKs ----------------
            if src == "BROKER" and orig:
                try:
                    cmd = json.loads(orig)
                    cmd_op = cmd.get("Operation", "").upper()
                except Exception:
                    cmd_op = None

                # PUBLISH chunk ACK
                if cmd_op == "PUBLISH":
                    ack_count = len(cmd.get("PublishCommands", [])) if cmd.get("PublishCommands") else 0
                    if self.q:
                        try:
                            self.q.put(("pishock", "chunk_ack", {"count": ack_count}))
                        except Exception as exc:
                            self.log("Failed to emit chunk_ack event", level="ERROR", exc=exc)
                    self.log(f"Chunk ACK (count={ack_count})", level="DEBUG")
                    return

                # UNSUBSCRIBE confirmation
                if cmd_op == "UNSUBSCRIBE":
                    self.log("UNSUBSCRIBE successful", msg, level="INFO")
                    if hasattr(self, "_unsubscribe_event"):
                        self._unsubscribe_event.set()
                    self.subscribed = False
                    return

                # SUBSCRIBE confirmation
                if cmd_op == "SUBSCRIBE":
                    if is_error:
                        self.log("SUBSCRIBE_ERROR received", msg, level="WARN")
                        self.subscribed = False
                    else:
                        self.subscribed = True
                        self.log("Subscription successful", msg, level="INFO")
                    if hasattr(self, "_subscribe_event"):
                        self._subscribe_event.set()
                    return

            # ---------------- Forward other messages ----------------
            if self.on_message:
                try:
                    self.on_message(msg)
                except Exception as exc:
                    self.log("Error in on_message callback", level="ERROR", exc=exc)

        except Exception as exc:
            self.log("Unexpected error in _listen", level="CRITICAL", exc=exc)

# ----------------------------- get_user_id -----------------------------
    async def _get_user_id(self) -> Optional[int]:
        try:
            url = f"https://auth.pishock.com/Auth/GetUserIfAPIKeyValid?apikey={self.apikey}&username={self.username}"
            if self.q:
                self.q.put(("pishock", "user_id_fetch_start", {}))

            def fetch_with_retries():
                for attempt in range(1, 6):
                    try:
                        resp = requests.get(url, timeout=5)
                        resp.raise_for_status()
                        return resp.json()
                    except Exception as e:
                        self.log("REST request failed", {"attempt": attempt, "error": str(e)}, level="WARN")
                        time.sleep(2 * attempt)
                self.log("All attempts to fetch UserId failed", level="ERROR")
                return {}

            data = await asyncio.to_thread(fetch_with_retries)
            uid = data.get("UserId")
            if uid:
                self.pishock_user_id = uid
                self.log("Retrieved PiShock UserId", {"UserId": uid}, level="INFO")
                if self.q:
                    self.q.put(("pishock", "user_id_fetched", {"UserId": uid}))
            else:
                self.log("UserId missing in response", {"response": data}, level="ERROR")
                if self.q:
                    self.q.put(("pishock", "user_id_error", {"response": data}))
            return uid
        except Exception as exc:
            self.log("Exception in _get_user_id", level="ERROR", exc=exc)
            if self.q:
                self.q.put(("pishock", "user_id_error", {"error": str(exc)}))
            return None

# ----------------------------- fetch_devices -----------------------------
    async def _fetch_devices(self):
        """
        Fetch both owned and shared devices, flatten multiple shockers per hub, and ingest them.
        """
        try:
            uid = await self._get_user_id()
            if not uid:
                self.log("No user ID available", level="WARN")
                return

            # --------------------- 🔹 Owned Devices ---------------------
            owned_devices = await self._get_owned_devices(uid)
            self._ingest_device_records(owned_devices)

            # --------------------- 🔹 Shared Devices ---------------------
            share_url = f"https://ps.pishock.com/PiShock/GetShareCodesByOwner?UserId={uid}&Token={self.apikey}&api=true"
            shares = {}
            for attempt in range(1, 6):
                try:
                    self.log("Fetching share codes", {"url": share_url, "attempt": attempt}, level="DEBUG")
                    shares = await asyncio.to_thread(lambda: requests.get(share_url, timeout=8).json())
                    if self.q:
                        total_ids = sum(len(v) for v in shares.values())
                        self.q.put(("pishock", "share_codes_loaded", {"total_share_ids": total_ids}))
                    break
                except Exception as e:
                    self.log("Failed to fetch share codes", {"attempt": attempt, "error": str(e)}, level="WARN")
                    if attempt < 5:
                        await asyncio.sleep(1.5 * attempt)
                    else:
                        self._show_support_popup(
                            "A Critical Error has occurred with PiShock retrieval!"
                        )
                        return

            # Flatten share IDs
            share_ids = [sid for codes in shares.values() for sid in codes]
            if share_ids:
                shared = await self.get_shared_shockers_by_share_ids(uid, share_ids)
                self._ingest_device_records(shared)

        except Exception as exc:
            self.log("Exception in _fetch_devices", level="ERROR", exc=exc)
            if self.q:
                self.q.put(("pishock", "fetch_devices_error", {"error": str(exc)}))

# --------------------- 🔹 Owned Devices ---------------------
    async def _get_owned_devices(self, user_id: int) -> dict:
        """
        Fetch owned devices from PiShock API and flatten multiple shockers per hub.
        """
        url = f"https://ps.pishock.com/PiShock/GetUserDevices?UserId={user_id}&Token={self.apikey}&api=true"
        self.log("Requesting owned devices", {"url": url}, level="DEBUG")

        for attempt in range(1, 6):
            try:
                response = await asyncio.to_thread(lambda: requests.get(url, timeout=10))
                response.raise_for_status()
                data = response.json()

                if not isinstance(data, list):
                    self.log("Unexpected owned devices structure", {"raw": data}, level="ERROR")
                    return {}

                owned_devices = {}
                for hub in data:
                    owner = hub.get("username", "(unknown)")
                    if owner not in owned_devices:
                        owned_devices[owner] = []

                    for shocker in hub.get("shockers", []):
                        device = {
                            "name": shocker.get("name"),
                            "shockerId": shocker.get("shockerId"),
                            "clientId": hub.get("clientId"),
                            "shareId": None,
                            "shareCode": None,
                            "isPaused": shocker.get("isPaused"),
                            "fromUser": owner,
                            "ownerName": owner,
                            "code": shocker.get("shockerId"),
                            "maxIntensity": shocker.get("maxIntensity"),
                            "canContinuous": shocker.get("canContinuous"),
                            "canShock": shocker.get("canShock"),
                            "canVibrate": shocker.get("canVibrate"),
                            "canBeep": shocker.get("canBeep"),
                            "canLog": shocker.get("canLog"),
                            "canControl": shocker.get("canControl"),
                            "userId": hub.get("userId"),
                            "deviceType": hub.get("deviceType"),
                            "allowPublicLogs": hub.get("allowPublicLogs"),
                            "isPrivate": hub.get("isPrivate"),
                            "updatedAt": shocker.get("updatedAt"),
                        }
                        owned_devices[owner].append(device)
                return owned_devices

            except Exception as e:
                self.log("Failed to fetch owned devices", {"attempt": attempt, "error": str(e)}, level="WARN")
                if attempt < 5:
                    await asyncio.sleep(1.5 * attempt)
                else:
                    return {}

    async def get_shared_shockers_by_share_ids(self, user_id: int, share_ids: list[str | int]) -> dict:
        import requests, time

        if not share_ids:
            self.log("No share IDs provided to retrieve shared shockers", level="WARN")
            return {}

        # Build full URL with repeated &shareIds entries
        params = "&".join(f"shareIds={sid}" for sid in share_ids)
        url = (
            f"https://ps.pishock.com/PiShock/GetShockersByShareIds"
            f"?UserId={user_id}&Token={self.apikey}&api=true&{params}"
        )

        self.log("Requesting shared shockers", {
            "user_id": user_id,
            "share_ids": share_ids,
            "url": url
        }, level="DEBUG")

        for attempt in range(1, 6):
            try:
                response = await asyncio.to_thread(lambda: requests.get(url, timeout=10))
                response.raise_for_status()
                data = response.json()

                # Validate structure
                if not isinstance(data, dict):
                    self.log("Unexpected shared shockers structure", {"raw": data}, level="ERROR")
                    return {}

                self.log("Retrieved shared shockers", {
                    "attempt": attempt,
                    "owners": list(data.keys()),
                    "total": sum(len(v) for v in data.values())
                }, level="INFO")
                return data

            except Exception as e:
                self.log("Failed to retrieve shared shockers", {
                    "attempt": attempt,
                    "error": str(e)
                }, level="WARN")

                if attempt < 5:
                    time.sleep(1.5 * attempt)
                else:
                    self.log("Max retries exceeded for shared shockers", level="ERROR")
                    return {}

# --------------------- 🔹 Ingest Devices ---------------------
    def _ingest_device_records(self, user_device_map: dict[str, list[dict]]) -> None:
        """
        Ingest devices into self.devices with unique key per shocker, preserving individual control.
        """
        save_path = pathlib.Path("saved/config/devices/devices.json")
        save_path.parent.mkdir(parents=True, exist_ok=True)

        def make_key(d: dict) -> str:
            return f"{d.get('clientId')}::{d.get('shockerId')}::{d.get('fromUser')}"

        def is_placeholder(d: dict) -> bool:
            return not d.get("fromUser") or str(d.get("name", "")).startswith("(cache)")

        def is_newer(a: dict, b: dict) -> bool:
            sa, sb = a.get("shareId"), b.get("shareId")
            if isinstance(sa, int) and isinstance(sb, int) and sa != sb:
                return sa > sb
            return (a.get("updatedAt") or 0) > (b.get("updatedAt") or 0)

        live: dict[str, dict] = {make_key(d): d for d in getattr(self, "devices", [])}
        seen_now: set[str] = set()

        def _merge_device(device: dict) -> None:
            key = make_key(device)
            seen_now.add(key)
            if key not in live:
                self.devices.append(device)
                live[key] = device
            else:
                existing = live[key]
                should_replace = (
                    (is_placeholder(existing) and not is_placeholder(device))
                    or (is_placeholder(existing) == is_placeholder(device) and is_newer(device, existing))
                )
                if should_replace:
                    idx = self.devices.index(existing)
                    self.devices[idx] = live[key] = device

            sid, cid = str(device.get("shockerId")), str(device.get("clientId"))
            if sid and cid:
                self.device_client_map[sid] = cid

        # Flatten all devices per owner
        for owner, rows in user_device_map.items():
            for row in rows:
                if not ("shockerId" in row and "clientId" in row):
                    self.log("Skipping invalid device", {"raw": row}, level="WARN")
                    continue
                _merge_device(row)

        # Prune disappeared devices
        processed_owners = set(user_device_map.keys())
        self.devices = [
            d for d in self.devices
            if make_key(d) in seen_now
        ]

        # Rebuild device_client_map
        self.device_client_map = {
            str(d["shockerId"]): str(d["clientId"])
            for d in self.devices if d.get("shockerId") and d.get("clientId")
        }

        # Persist
        try:
            with save_path.open("w", encoding="utf-8") as f:
                json.dump(self.devices, f, indent=2)
            self.log(f"Saved {len(self.devices)} devices to {save_path}", level="DEBUG")
        except Exception as e:
            self.log("Failed to persist devices", {"error": str(e)}, level="ERROR")

# ----------------- Refresh + Incremental Subscribe -----------------
    async def _refresh_and_subscribe(self):
        """
        Refresh owned & shared devices and subscribe only to new ones.
        Prevents duplicates and avoids SUBSCRIBE_ERROR from old/wrong IDs.
        """
        try:
            self.log("→ Starting incremental device refresh + subscription", level="DEBUG")

            #Fetch updated devices
            await self._fetch_devices()  # updates self.devices

            if not self.devices:
                self.log("No devices found - skipping subscription", level="DEBUG")
                return

            #Build the set of current correct targets
            current_targets = {f"{str(d.get('clientId'))}-ping" for d in self.devices if d.get('clientId')}
            current_targets.update({f"{str(d.get('clientId'))}-log" for d in self.devices if d.get('clientId')})

            if not current_targets:
                self.log("No valid devices to subscribe", level="DEBUG")
                return

            #Determine which targets are new (avoid re-subscribing to existing)
            existing_targets = getattr(self, "_subscribed_targets", set())
            new_targets = current_targets - existing_targets

            if not new_targets:
                self.log("No new devices to subscribe - skipping SUBSCRIBE", level="DEBUG")
                return

            #Send SUBSCRIBE for only new targets
            self.log("Subscribing to new device targets", {"targets": sorted(new_targets)}, level="DEBUG")
            await self._send_simple("SUBSCRIBE", {"Targets": sorted(new_targets), "PublishCommands": []})

            #Update the subscribed targets set
            self._subscribed_targets = existing_targets.union(new_targets)
            self.subscribed = True

            self.log("Incremental SUBSCRIBE complete", level="DEBUG")

        except Exception as exc:
            self.log("Exception during incremental device refresh & subscription", level="ERROR", exc=exc)
            if self.q:
                self.q.put(("pishock", "refresh_subscribe_error", {"error": str(exc)}))

# ----------------- Device Refresh & Subscribe Flow -----------------
    async def refresh_devices(self):
        """
        Refresh owned and shared PiShock devices, update internal state, and resubscribe.
        This method ensures no stale subscriptions are sent.
        """
        try:
            self.log("→ Refreshing devices...", level="DEBUG")

            #Fetch owned devices
            user_id = self.user_id  # assume already fetched
            owned_url = f"https://ps.pishock.com/PiShock/GetUserDevices?UserId={user_id}&Token={self.api_token}&api=true"
            owned_devices = await self._fetch_json(owned_url)  # your async HTTP helper
            if not owned_devices:
                self.log("No owned devices found", level="WARN")

            #Fetch shared devices
            share_codes_url = f"https://ps.pishock.com/PiShock/GetShareCodesByOwner?UserId={user_id}&Token={self.api_token}&api=true"
            share_codes = await self._fetch_json(share_codes_url) or []
            shared_devices = []
            if share_codes:
                share_ids = [str(s["shareId"]) for s in share_codes]
                shared_url = f"https://ps.pishock.com/PiShock/GetShockersByShareIds?UserId={user_id}&Token={self.api_token}&api=true"
                shared_devices = await self._fetch_json(shared_url) or []

            #Combine owned + shared devices into a fresh list
            new_devices = owned_devices + shared_devices

            #Atomically update internal device list
            self.devices = new_devices

            self.log(
                "Devices refreshed",
                {"total": len(new_devices), "owned": len(owned_devices), "shared": len(shared_devices)},
                level="DEBUG"
            )

            #Trigger safe subscription
            await self._subscribe_devices()

        except Exception as exc:
            self.log("Error refreshing devices", level="ERROR", exc=exc)

# ----------------- Unsubscribe Devices (fixed) -----------------
    async def _unsubscribe_devices(self, targets: list | None = None):
        """
        Safely unsubscribe from PiShock WebSocket targets.
        If targets is None, unsubscribes from currently subscribed devices only.
        """
        if not self.ws or not self.connected:
            self.log("Cannot unsubscribe - WebSocket not connected", level="WARN")
            return

        # Build unsubscribe list from currently subscribed targets if none provided
        if targets is None:
            targets = getattr(self, "_last_subscribe_targets", [])  # store last SUBSCRIBE targets
            if not targets:
                self.log("No previous subscription targets - skipping UNSUBSCRIBE", level="DEBUG")
                return

        payload = {"Operation": "UNSUBSCRIBE", "Targets": targets}

        if not hasattr(self, "_unsubscribe_event"):
            self._unsubscribe_event = asyncio.Event()
        self._unsubscribe_event.clear()

        self.log("Sending UNSUBSCRIBE command", {"targets": targets}, level="DEBUG")
        await self._send_simple("UNSUBSCRIBE", payload)

        try:
            await asyncio.wait_for(self._unsubscribe_event.wait(), timeout=5)
            self.log("UNSUBSCRIBE ACK received", level="DEBUG")
        except asyncio.TimeoutError:
            self.log("Timeout waiting for UNSUBSCRIBE ACK - proceeding anyway", level="WARN")

        self.subscribed = False

# ----------------- _subscribe_devices (fixed) -----------------
    async def _subscribe_devices(self):
        """
        Subscribe only to valid PiShock devices.
        Uses correct clientId from device records.
        Only subscribes to ping/log for each device.
        """
        if not self.devices:
            self.log("No devices available - skipping SUBSCRIBE", level="WARN")
            return

        targets = []
        for d in self.devices:
            client_id = str(d.get("clientId") or "").strip()
            if not client_id:
                continue
            # Only use clientId, ignore any userId or extra numbers
            targets.append(f"{client_id}-ping")
            targets.append(f"{client_id}-log")

        if not targets:
            self.log("No valid targets found - skipping SUBSCRIBE", level="WARN")
            return

        self.log("Preparing SUBSCRIBE targets", {"targets": targets}, level="DEBUG")

        # Send SUBSCRIBE directly - no extra IDs
        await self._send_simple("SUBSCRIBE", {"Targets": targets, "PublishCommands": []})
        self.subscribed = True

# ----------------------------- send helpers (_send_simple) -----------------------------
    async def _send_simple(self, op: str, extra: Dict[str, Any] | None = None):
        """
        Send non-PUBLISH or PUBLISH under busy gating, with start/done/error events.
        Handles SUBSCRIBE and UNSUBSCRIBE payloads correctly.
        """
        try:
            #start event
            if self.q:
                self.q.put(("pishock", "simple_send_start", {"op": op}))

            if not (self.connected and self.ws):
                self.log("Skipping send - WebSocket not connected", {"operation": op}, level="WARN")
                return
            if getattr(self.ws, "closed", False):
                self.log("Send aborted - WebSocket is already closed", {"operation": op}, level="WARN")
                return

            # busy gating for non-PUBLISH
            if op != "PUBLISH":
                while True:
                    async with self._busy_lock:
                        remain = self._busy_global - asyncio.get_event_loop().time()
                    if remain <= 0:
                        break
                    await asyncio.sleep(min(remain, 0.1))

            # PUBLISH-sync window
            if op == "PUBLISH":
                while True:
                    async with self._other_shock_lock:
                        wait_until = self._other_shock_active_until
                    now = asyncio.get_event_loop().time()
                    if wait_until <= now:
                        break
                    remaining = wait_until - now
                    self.log(
                        "Waiting for remote shock window to end",
                        {"remaining_sec": round(remaining, 3)},
                        level="DEBUG"
                    )
                    await asyncio.sleep(min(remaining, 0.01))

            # Build payload
            payload = {"Operation": op}
            extra = extra or {}

            # Correctly handle SUBSCRIBE & UNSUBSCRIBE
            if op in ("SUBSCRIBE", "UNSUBSCRIBE"):
                targets = extra.get("Targets", [])
                if op == "UNSUBSCRIBE" and not targets:
                    # Default to "*" if nothing specified
                    targets = ["*"]
                payload["Targets"] = targets

                # Ensure PublishCommands exists for SUBSCRIBE (prevents broker SUBSCRIBE_ERROR)
                if op == "SUBSCRIBE":
                    publish_commands = extra.get("PublishCommands")
                    if publish_commands is None:
                        payload["PublishCommands"] = []
                    else:
                        payload["PublishCommands"] = publish_commands

            # (PUBLISH logic handled by _send_publish_commands or extra["PublishCommands"])

            # Send JSON payload
            raw = json.dumps(payload)
            await self.ws.send(raw)
            self.log("Sent WebSocket frame", {"operation": op, "payload": payload}, level="DEBUG")

            # done event
            if self.q:
                self.q.put(("pishock", "simple_send_done", {"op": op}))

        except Exception as exc:
            self.log("Exception in _send_simple", level="ERROR", exc=exc)
            if self.q:
                self.q.put(("pishock", "simple_send_error", {"op": op, "error": str(exc)}))

# ----------------------------- send publish V2 -----------------------------
    async def _send_publish_commands(
        self,
        device_ids: list[Union[str, int]],
        mode: str,
        intensity: int,
        duration: int
    ):
        """
        Send one chunk (batch) of PUBLISH commands over WebSocket,
        emit chunk_sent (with count, duration_ms, intensity, device_ids)
        and handle exceptions.
        """
        try:
            # ── Pre-flight checks ────────────────────────────────────────────────
            if not (self.connected and self.ws):
                self.log("Cannot send - WebSocket not connected", level="WARN")
                return
            if not self.pishock_user_id:
                self.log("Cannot send - missing userId", level="ERROR")
                return

            short = {"vibrate": "v", "shock": "s", "beep": "b", "echo": "e"}.get(mode.lower())
            if not short:
                self.log("Invalid mode", {"mode": mode}, level="ERROR")
                return

            # ── Build PublishCommands ────────────────────────────────────────────
            publish_commands = []
            for device_id in device_ids:
                try:
                    device = next(d for d in self.devices if str(d.get("shockerId")) == str(device_id))
                    client_id = device.get("clientId")
                    if not client_id:
                        raise ValueError("Missing clientId")
                    is_shared = bool(device.get("shareCode") and device.get("fromUser"))

                    if is_shared:
                        target = f"c{client_id}-sops-{device['shareCode']}"
                        body = {
                            "id": int(device_id),
                            "m": short,
                            "i": intensity,
                            "d": duration,
                            "r": True,
                            "ty": "sc",
                            "ob": device["fromUser"]
                        }
                    else:
                        target = f"c{client_id}-ops"
                        body = {
                            "id": int(device_id),
                            "m": short,
                            "i": intensity,
                            "d": duration,
                            "r": True,
                            "ty": "ow",
                            "l": {
                                "u": self.pishock_user_id,
                                "ty": "api",
                                "w": False,
                                "h": False,
                                "o": "StreamConnector"
                            }
                        }

                    publish_commands.append({"Target": target, "Body": body})
                except Exception as exc:
                    self.log("Failed to build PublishCommand", {"device_id": device_id}, level="ERROR", exc=exc)
                    continue

            if not publish_commands:
                self.log("No valid devices to send commands to", level="WARN")
                return

            payload = {"Operation": "PUBLISH", "PublishCommands": publish_commands}

            # ── Send & chunk event ───────────────────────────────────────────────
            await self.ws.send(json.dumps(payload))

            # emit chunk_sent event with extra context
            if self.q:
                try:
                    self.q.put(("pishock", "chunk_sent", {
                        "count": len(publish_commands),
                        "duration_ms": duration,
                        "intensity": intensity,
                        "device_ids": device_ids
                    }))
                except Exception as exc:
                    self.log("Failed to emit chunk_sent event", level="ERROR", exc=exc)

            # detailed debug log
            self.log("Sent PUBLISH command", {
                "count": len(publish_commands),
                "duration_ms": duration,
                "intensity": intensity,
                "device_ids": device_ids
            }, level="DEBUG")

            # ── Extend busy window ───────────────────────────────────────────────
            now = asyncio.get_event_loop().time()
            async with self._busy_lock:
                self._busy_global = max(self._busy_global, now + duration / 1000)

        except Exception as exc:
            # catch any unexpected error in this chunk
            self.log("Exception in _send_publish_commands", level="ERROR", exc=exc)
            if self.q:
                try:
                    self.q.put(("pishock", "chunk_error", {"error": str(exc)}))
                except Exception:
                    pass

# ----------------------------- send publish V3 -----------------------------
    async def _send_publish_command_v3(
        self,
        device_id: str,
        mode: str,
        intensity: int,
        duration: int,
    ):
        """
        Fallback V3 HTTP operate calls, with start/ack/error events.
        """
        try:
            if not getattr(self, "fallback_v3", False):
                self.log("Fallback V3 mode is not active", level="WARN")
                return
            if not self.apikey or not self.username:
                self.log("Missing API credentials for fallback", level="ERROR")
                return

            # start V3 send
            if self.q:
                self.q.put(("pishock", "v3_send_start", {
                    "device_id": device_id,
                    "mode": mode,
                    "intensity": intensity,
                    "duration": duration
                }))

            device = next((d for d in self.devices if str(d.get("shockerId")) == str(device_id)), None)
            if not device or not device.get("code"):
                raise ValueError("Device missing or lacks share code")

            op_map = {"vibrate": 1, "shock": 0, "beep": 2}
            op = op_map.get(mode.lower())
            if op is None:
                raise ValueError(f"Invalid mode: {mode}")

            payload = {
                "code": device["code"],
                "duration": duration,
                "intensity": intensity,
                "op": op,
                "apikey": self.apikey,
                "username": self.username,
                "name": device.get("name", "FallbackControl"),
                "random": True,
                "scale": True
            }

            async with aiohttp.ClientSession() as session:
                async with session.post("https://ps.pishock.com/PiShock/Operate", json=payload) as resp:
                    resp_data = await resp.json()
                    self.log("V3 operate response", {
                        "status": resp.status,
                        "data": resp_data
                    }, level="INFO")

                    # ack event
                    if self.q:
                        self.q.put(("pishock", "v3_chunk_ack", {
                            "device_id": device_id,
                            "status": resp.status,
                            "success": resp_data.get("success", False)
                        }))

                    if resp.status != 200 or not resp_data.get("success", False):
                        raise RuntimeError(f"V3 operation failed: {resp_data!r}")

        except Exception as exc:
            self.log("Exception in _send_publish_command_v3", level="ERROR", exc=exc)
            if self.q:
                self.q.put(("pishock", "v3_chunk_error", {
                    "device_id": device_id,
                    "error": str(exc)
                }))

# ----------------------------- pause_all_devices -----------------------------
    async def pause_all_devices(self):
        """
        Pause each device via REST, with per-device events.
        """
        try:
            if not self.devices:
                self.log("No devices to pause", level="WARN")
                return
            #start pause-all
            if self.q:
                self.q.put(("pishock", "pause_all_start", {"device_count": len(self.devices)}))

            async with aiohttp.ClientSession() as session:
                for device in self.devices:
                    sid = device.get("shockerId")
                    client_id = device.get("clientId")
                    if not (self.pishock_user_id and client_id and sid):
                        self.log("Skipping pause - missing ID(s)", {"device": device}, level="WARN")
                        continue

                    payload = {
                        "token": self.apikey,
                        "userId": self.pishock_user_id,
                        "clientId": client_id,
                        "shockerId": sid
                    }

                    try:
                        async with session.post("https://ps.pishock.com/PiShock/PauseShocker", json=payload) as resp:
                            data = await resp.json()
                            self.log("Pause response", {"shockerId": sid, "response": data}, level="INFO")
                            # pause_sent
                            if self.q:
                                self.q.put(("pishock", "pause_sent", {"shockerId": sid, "response": data}))
                    except Exception as exc:
                        self.log("Failed to send pause request", {"shockerId": sid}, level="ERROR", exc=exc)
                        if self.q:
                            self.q.put(("pishock", "pause_error", {"shockerId": sid, "error": str(exc)}))

        except Exception as exc_outer:
            self.log("Exception in pause_all_devices", level="ERROR", exc=exc_outer)
            if self.q:
                self.q.put(("pishock", "pause_all_error", {"error": str(exc_outer)}))
                   
# ────────────────────────────────────────────────────────────────────────────────
# Send Special Operation (per-device, threaded, min-1s oscillation)
# ────────────────────────────────────────────────────────────────────────────────
    def send_special_mode(
        self,
        device_id: str,
        mode: str,
        intensity: int,
        total_duration_ms: int,
        *,
        delay_ms: int = 1000,
        step: int = 10,
        pattern: Optional[list] = None
    ) -> threading.Thread:
        """
        Dispatch one device through a special PiShock sequence.
        Emits start/completed/error via self.q and logs all pulses.
        """
        import time, random, asyncio

        # enforce ≥1 s per pulse
        MIN_MS = 1000
        delay_ms = max(MIN_MS, delay_ms)
        total_duration_ms = max(MIN_MS, total_duration_ms)

        def send_shock(dev: str, inten: int, dur: int):
            """Helper: fire one PUBLISH with a valid mode ('shock') pulse."""
            try:
                fut = asyncio.run_coroutine_threadsafe(
                    self._send_publish_commands([dev], "shock", inten, dur),
                    self.loop
                )
                fut.result()
                pishock_log_verbose(
                    "→ special-mode pulse sent",
                    {"device_id": dev, "intensity": inten, "duration_ms": dur},
                    level="DEBUG"
                )
                if self.q:
                    self.q.put(("pishock", "special_pulse", {
                        "device_id": dev,
                        "intensity": inten,
                        "duration_ms": dur
                    }))
            except Exception as pulse_exc:
                pishock_log_verbose(
                    "Failed to send special-mode pulse",
                    {"device_id": dev, "error": str(pulse_exc)},
                    level="ERROR",
                    exc=pulse_exc
                )
                if self.q:
                    self.q.put(("pishock", "special_error", {
                        "device_id": dev,
                        "mode": mode,
                        "error": str(pulse_exc)
                    }))

        # ─── MISSING HELPER: allow vibrate/beep as well ────────────────────────
        def send_pulse(dev: str, pulse_mode: str, inten: int, dur: int):
            VALID_MODES = {"shock", "vibrate", "beep"}
            if pulse_mode not in VALID_MODES:
                pishock_log_verbose(
                    f"Invalid API mode '{pulse_mode}', defaulting to 'shock'",
                    {"device_id": dev},
                    level="WARNING"
                )
                pulse_mode = "shock"
            try:
                fut = asyncio.run_coroutine_threadsafe(
                    self._send_publish_commands([dev], pulse_mode, inten, dur),
                    self.loop
                )
                fut.result()
                pishock_log_verbose(
                    f"→ special-mode pulse sent ({pulse_mode})",
                    {"device_id": dev, "intensity": inten, "duration_ms": dur},
                    level="DEBUG"
                )
                if self.q:
                    self.q.put(("pishock", "special_pulse", {
                        "device_id": dev,
                        "mode": pulse_mode,
                        "intensity": inten,
                        "duration_ms": dur
                    }))
            except Exception as pulse_exc:
                pishock_log_verbose(
                    f"Failed to send special-mode pulse ({pulse_mode})",
                    {"device_id": dev, "error": str(pulse_exc)},
                    level="ERROR",
                    exc=pulse_exc
                )
                if self.q:
                    self.q.put(("pishock", "special_error", {
                        "device_id": dev,
                        "mode": pulse_mode,
                        "error": str(pulse_exc)
                    }))

        def runner():
            # use locals so we don’t rebind outer args
            cur_intensity = intensity
            cur_step = step
            start_ts = time.time()
            elapsed = 0

            # 🏁 start event
            if self.q:
                self.q.put(("pishock", "special_started", {
                    "device_id": device_id,
                    "mode": mode,
                    "intensity": intensity,
                    "total_duration_ms": total_duration_ms
                }))

            try:
                # ─── Pattern mode ─────────────────────────────────
                if mode == "pattern" and pattern:
                    for entry in pattern:
                        m = entry.get("mode", "shock").lower()
                        inten = entry.get("intensity", cur_intensity)
                        dur   = max(entry.get("duration", MIN_MS), MIN_MS)
                        entry_step = entry.get("step", cur_step)

                        if m == "pulse":
                            send_shock(device_id, inten, dur)

                        elif m == "burst":
                            for _ in range(3):
                                send_shock(device_id, inten, MIN_MS)
                                time.sleep(MIN_MS/1000.0)

                        elif m == "rage":
                            # ← now handled
                            rage_dur = entry.get("duration", 100)
                            send_shock(device_id, inten, rage_dur)
                            time.sleep(rage_dur/1000.0)

                        elif m == "randomized_wobble":
                            rand_m   = random.choice(["shock","vibrate","beep"])
                            rand_int = random.randint(5, 100)
                            rand_dur = random.randint(MIN_MS, 15000)
                            send_pulse(device_id, rand_m, rand_int, rand_dur)

                        elif m == "oscillation":
                            osc_intensity = inten
                            direction = 1
                            t0 = time.time()
                            while int((time.time() - t0)*1000) < dur:
                                send_shock(device_id, osc_intensity, delay_ms)
                                osc_intensity += entry_step * direction
                                if osc_intensity >= 100:
                                    osc_intensity = 100
                                    direction = -1
                                elif osc_intensity <= 1:
                                    osc_intensity = 1
                                    direction = 1
                                time.sleep(delay_ms/1000.0)

                        elif m in {"shock","vibrate","beep"}:
                            send_pulse(device_id, m, inten, dur)

                        else:
                            pishock_log_verbose(
                                f"Unknown mode '{m}' in pattern, defaulting to 'shock'",
                                {"device_id": device_id},
                                level="WARNING"
                            )
                            send_shock(device_id, inten, dur)

                        # delay after each entry
                        time.sleep(max(entry.get("delay", delay_ms), MIN_MS)/1000.0)

                    elapsed = total_duration_ms  # done

                else:
                    # ─── Other special modes ───────────────────────────
                    while elapsed < total_duration_ms:
                        if mode == "pulse":
                            send_shock(device_id, cur_intensity, delay_ms)
                            step_dur = delay_ms

                        elif mode == "burst":
                            for _ in range(3):
                                send_shock(device_id, cur_intensity, MIN_MS)
                                time.sleep(MIN_MS/1000.0)
                            step_dur = delay_ms

                        elif mode == "rage":
                            # one quick shock, then a fixed delay
                            rage_dur   = 100
                            rage_delay = 500
                            send_shock(device_id, cur_intensity, rage_dur)
                            time.sleep(rage_dur / 1000.0)
                            step_dur = rage_delay

                        elif mode == "randomized_wobble":
                            rand_m   = random.choice(["shock","vibrate","beep"])
                            rand_int = random.randint(5, 100)
                            rand_dur = random.randint(MIN_MS, 15000)
                            send_pulse(device_id, rand_m, rand_int, rand_dur)
                            step_dur = rand_dur + delay_ms

                        elif mode == "oscillation":
                            send_shock(device_id, cur_intensity, delay_ms)
                            cur_intensity += cur_step
                            if cur_intensity >= 100 or cur_intensity <= 0:
                                cur_step = -cur_step
                                cur_intensity = max(0, min(100, cur_intensity))
                            step_dur = delay_ms

                        else:
                            pishock_log_verbose(
                                f"Unknown special mode: {mode}",
                                {"device_id": device_id},
                                level="ERROR"
                            )
                            break

                        time.sleep(step_dur/1000.0)
                        elapsed = int((time.time() - start_ts)*1000)
                        
                # 🏁 completion event
                if self.q:
                    self.q.put(("pishock", "special_completed", {
                        "device_id": device_id,
                        "mode": mode
                    }))

            except Exception as exc:
                self.log("Exception in send_special_mode runner", level="ERROR", exc=exc)
                if self.q:
                    self.q.put(("pishock", "special_error", {
                        "device_id": device_id,
                        "mode": mode,
                        "error": str(exc)
                    }))

        t = threading.Thread(target=runner, daemon=True)
        t.name = f"pishock-special-{device_id}"
        t.start()
        return t

# ----------------------------- emergency_stop_all -----------------------------
    def emergency_stop_all(self):
        """
        E-Stop: hammer all devices, then kill the process immediately.
        """
        if self.q:
            self.q.put(("pishock", "estop_start", {}))

        self.log("[E-STOP] IMMEDIATE TERMINATION REQUESTED", level="CRITICAL")

        async def hammer_devices():
            async with aiohttp.ClientSession() as session:
                for device in self.devices:
                    code = device.get("code")
                    sid = device.get("shockerId")
                    if not code:
                        continue
                    payload = {
                        "code": code,
                        "duration": 15000,
                        "intensity": 0,
                        "op": 0,
                        "apikey": self.apikey,
                        "username": self.username,
                        "name": "E-Stop",
                        "random": False,
                        "scale": True,
                    }
                    try:
                        await session.post("https://ps.pishock.com/PiShock/Operate", json=payload)
                        self.log("E-STOP signal sent", {"shockerId": sid}, level="DEBUG")
                        if self.q:
                            self.q.put(("pishock", "estop_sent", {"shockerId": sid}))
                    except Exception as exc:
                        self.log("Failed E-STOP to device", {"shockerId": sid}, level="ERROR", exc=exc)
                        if self.q:
                            self.q.put(
                                ("pishock", "estop_error", {"shockerId": sid, "error": str(exc)})
                            )

        def shutdown():
            try:
                asyncio.run(hammer_devices())
            except Exception:
                pass
            time.sleep(0.3)

            # HARD KILL - cross platform
            if sys.platform.startswith("win"):
                os._exit(1)  # Windows: immediate hard exit
            else:
                os.kill(os.getpid(), signal.SIGKILL)  # Unix: hard kill

        threading.Thread(target=shutdown, daemon=False).start()

# ───────────────────────────── GUI API ───────────────────────────────
    def claim_share_ids(self, share_codes: Union[str, List[str]]):
        """
        Calls the PiShock/AddShares endpoint in a separate thread to claim
        one or more shared device IDs. This method is non-blocking and suitable 
        for linking directly to a GUI button.

        Args:
            share_codes: A single share code string or a list of share code strings.
        """
        codes = [share_codes] if isinstance(share_codes, str) else list(share_codes)
        
        if not codes:
            self.log("No share codes provided to claim.", level="WARN")
            return
        
        # Ensure we have the user ID. This is a crucial requirement for the endpoint.
        if not self.pishock_user_id:
            self.log("PiShock User ID not available, cannot claim shares.", level="ERROR")
            # In a real app, you might try a synchronous fetch here if you didn't trust
            # the asynchronous startup process to have set it. For brevity, we assume 
            # the client is initialized correctly or we abort.
            return

        # Start the claim process in a separate thread to avoid blocking the GUI
        threading.Thread(
            target=self._claim_share_codes_worker,
            args=(codes, self.pishock_user_id),
            daemon=True
        ).start()

# ----------------------------- claim share codes -----------------------------
    def _claim_share_codes_worker(self, codes: list[str], user_id: int):
        """
        Claim share codes and automatically refresh devices + re-subscribe.
        """
        ADD_SHARES_URL = "https://ps.pishock.com/PiShock/AddShares"
        payload = {
            "token": self.apikey,
            "userId": user_id,
            "shareCodes": codes
        }

        self.log(f"Attempting to claim {len(codes)} share codes...", level="INFO")

        try:
            response = requests.post(
                ADD_SHARES_URL,
                headers={"accept": "application/json", "Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=15
            )
            response.raise_for_status()
            result = response.json()

            # Corrected logging
            self.log(
                "Successfully submitted share code claim.",
                level="INFO",
                data={"result": result}
            )

            if self.q:
                self.q.put(("pishock", "share_claim_success", {"codes": codes, "response": result}))

            # Trigger device refresh + auto re-subscribe
            if self.loop and self.connected:
                self.log("Triggering device refresh after successful claim.", level="DEBUG")
                asyncio.run_coroutine_threadsafe(self._refresh_and_subscribe(), self.loop)
            else:
                self.log("Client not running/connected - skip auto-refresh", level="WARN")
        
        except requests.exceptions.RequestException as err:
            error_msg = f"Network or HTTP error claiming share codes: {err}. Status: {response.status_code if 'response' in locals() else 'N/A'}"
            self.log(f"{error_msg}", level="ERROR")
            if self.q:
                self.q.put(("pishock", "share_claim_error", {"error": error_msg}))

        except Exception as err:
            error_msg = f"Unexpected error claiming share codes: {err}"
            self.log(f"{error_msg}", level="CRITICAL", exc=err)
            if self.q:
                self.q.put(("pishock", "share_claim_error", {"error": error_msg}))

# ────────────────────────────────────────────────────────────────────────────────
# Custom OSC Query – Dynamic Port Mapping Node (VRChat schema-compatible)
# ────────────────────────────────────────────────────────────────────────────────

# Global lock to prevent overlapping execution
_avatar_change_lock = threading.Lock()

def load_osc_cfg() -> None:
    global OSC_IN_ADDR, OSC_IN_PORT, OSC_OUT_PORT, OSC_ADVERTISE_PORT, OSCQUERY_PORT

    OSC_IN_ADDR = "127.0.0.1"
    OSC_IN_PORT = 9001
    OSC_OUT_PORT = 9000
    OSC_ADVERTISE_PORT = 9011
    OSCQUERY_PORT = 8085

    try:
        if os.path.exists(OSC_CFG_FILE):
            with open(OSC_CFG_FILE, "r", encoding="utf-8") as fp:
                data = json.load(fp)

                OSC_IN_ADDR = data.get("OSC_IN_ADDR", OSC_IN_ADDR)
                OSC_IN_PORT = int(data.get("OSC_IN_PORT", OSC_IN_PORT))
                OSC_OUT_PORT = int(data.get("OSC_OUT_PORT", OSC_OUT_PORT))
                OSC_ADVERTISE_PORT = int(data.get("OSC_ADVERTISE_PORT", OSC_ADVERTISE_PORT))
                OSCQUERY_PORT = int(data.get("OSCQUERY_PORT", OSCQUERY_PORT))

                log_osc_core(
                    "Loaded OSC config from file",
                    {
                        "OSC_IN_ADDR": OSC_IN_ADDR,
                        "OSC_IN_PORT": OSC_IN_PORT,
                        "OSC_OUT_PORT": OSC_OUT_PORT,
                        "OSC_ADVERTISE_PORT": OSC_ADVERTISE_PORT,
                        "OSCQUERY_PORT": OSCQUERY_PORT,
                    },
                    level="INFO",
                    action_type="config"
                )
        else:
            log_osc_core(
                f"OSC config missing: {OSC_CFG_FILE} (using defaults)",
                level="WARNING",
                action_type="config"
            )

    except Exception as e:
        log_osc_core(
            "Failed to load OSC config from file",
            level="ERROR",
            action_type="config",
            exc=e
        )
        
load_osc_cfg()

def refresh_dynamic_osc_mapping():
    global OSC_IN_ADDR, OSC_IN_PORT, OSC_OUT_PORT, OSC_ADVERTISE_PORT, OSCQUERY_PORT

    try:
        url = f"http://{OSC_IN_ADDR}:{OSCQUERY_PORT}/stream_connector/ports"
        with urllib.request.urlopen(url, timeout=1.0) as response:
            payload = json.loads(response.read().decode("utf-8"))

        contents = payload.get("CONTENTS", {})

        def _get_int(key, fallback):
            try:
                return int(contents[key]["VALUE"][0])
            except Exception:
                return fallback

        def _get_str(key, fallback):
            try:
                return str(contents[key]["VALUE"][0])
            except Exception:
                return fallback

        OSC_IN_ADDR = _get_str("OSC_IN_ADDR", OSC_IN_ADDR)
        OSC_IN_PORT = _get_int("OSC_IN_PORT", OSC_IN_PORT)
        OSC_OUT_PORT = _get_int("OSC_OUT_PORT", OSC_OUT_PORT)
        OSC_ADVERTISE_PORT = _get_int("OSC_ADVERTISE_PORT", OSC_ADVERTISE_PORT)
        OSCQUERY_PORT = _get_int("OSCQUERY_PORT", OSCQUERY_PORT)

        log_osc_core(
            "Dynamic OSC mapping loaded from OSCQuery",
            {
                "OSC_IN_ADDR": OSC_IN_ADDR,
                "OSC_IN_PORT": OSC_IN_PORT,
                "OSC_OUT_PORT": OSC_OUT_PORT,
                "OSC_ADVERTISE_PORT": OSC_ADVERTISE_PORT,
                "OSCQUERY_PORT": OSCQUERY_PORT,
            },
            level="INFO",
            action_type="config"
        )

    except Exception as e:
        log_osc_core(
            "Dynamic OSC mapping not yet available",
            level="DEBUG",
            action_type="config",
            exc=e
        )

class OSCBridge:
    _last_known_ports = {
        "query_port": None,
        "vrchat_osc_port": None,
        "oscquery_port": None
    }

    def __init__(self, service_name="Stream-Connector"):
        self.service_name = service_name

        # ─── obey config ───────────────────────────────
        self._host = OSC_IN_ADDR
        self.vrchat_osc_port = int(OSC_OUT_PORT)             # 9000 → VRChat
        self.advertise_osc_port = int(OSC_ADVERTISE_PORT)    # 9011 → us (advertised OSC UDP)
        self.oscquery_port = int(OSCQUERY_PORT)              # 8085 → us (OSCQuery HTTP)
        self.query_port = None                               # discovered from VRChat (OSCQuery TCP)

        self._param_cache = None
        self._zeroconf = Zeroconf()
        self._discover_event = threading.Event()

        self._osc_query_endpoint = "/avatar/parameters"
        
        self._sent_state = {}          # addr -> last value sent
        self._sent_generation = {}     # addr -> monotonic counter
        self._state_lock = threading.Lock()

    # ────────────────────────────────────────────────────── Public Introspection
    def get_port_mapping(self) -> dict:
        """
        Dynamic, machine-readable runtime topology.
        Exposed via OSCQuery as /stream_connector/ports/* leaf nodes.
        """
        return {
            "OSC_IN_ADDR": self._host,
            "OSC_IN_PORT": int(OSC_IN_PORT),                 # where we listen for inbound OSC (if applicable elsewhere)
            "OSC_OUT_PORT": int(self.vrchat_osc_port),       # VRChat target port (usually 9000)
            "OSC_ADVERTISE_PORT": int(self.advertise_osc_port),  # our advertised OSC UDP port (usually 9011)
            "OSCQUERY_PORT": int(self.oscquery_port),        # our OSCQuery HTTP port (usually 8085)
            "VRCHAT_QUERY_PORT": int(self.query_port or 0),  # VRChat OSCQuery TCP port (dynamic)
        }

    def _emit(self, ev: str, data: dict | None = None):
        try:
            q.put(("osc", ev, data or {}))
        except Exception:
            pass

    # ────────────────────────────────────────────────────── DISCOVERY (VRChat ONLY)
    def _add_service(self, zeroconf, service_type, name, state_change):
        info = zeroconf.get_service_info(service_type, name)
        if not info:
            return

        if name.startswith("VRChat-Client"):
            if service_type == "_oscjson._tcp.local.":
                self.query_port = info.port
            elif service_type == "_osc._udp.local.":
                self.vrchat_osc_port = info.port

        if self.query_port and self.vrchat_osc_port:
            self._discover_event.set()
            OSCBridge._last_known_ports["query_port"] = self.query_port
            OSCBridge._last_known_ports["vrchat_osc_port"] = self.vrchat_osc_port

    def discover_vrchat_ports(self, timeout=5):
        # Start Zeroconf discovery for VRChat services
        ServiceBrowser(
            self._zeroconf,
            "_oscjson._tcp.local.",
            handlers=[self._add_service]
        )
        ServiceBrowser(
            self._zeroconf,
            "_osc._udp.local.",
            handlers=[self._add_service]
        )

        # Wait for discovery
        if not self._discover_event.wait(timeout=timeout):
            log_osc_core(
                "VRChat OSC services not found.",
                level="ERROR",
                action_type="discover"
            )

            # Emit OSC error state for UI / heartbeat
            try:
                q.put(("osc", "error", {"stage": "discover"}))
            except Exception:
                pass

        else:
            log_osc_core(
                "VRChat ports discovered",
                {
                    "query_port": self.query_port,
                    "vrchat_osc_port": self.vrchat_osc_port
                },
                level="INFO",
                action_type="discover"
            )

            # Emit VRChat-ready state
            try:
                q.put((
                    "osc",
                    "vrchat_ready",
                    {
                        "query_port": self.query_port,
                        "vrchat_osc_port": self.vrchat_osc_port
                    }
                ))
            except Exception:
                pass

        return self.query_port, self.vrchat_osc_port

    # ────────────────────────────────────────────────────── ADVERTISE (US ONLY)
    def advertise_self(self):
        try:
            ip = socket.inet_aton(self._host)

            # ---- OSC (UDP) – OUR PORT ----
            info_osc = ServiceInfo(
                "_osc._udp.local.",
                f"{self.service_name}._osc._udp.local.",
                addresses=[ip],
                port=self.advertise_osc_port,
                properties={b"role": b"output"},
                server="stream-connector.local."
            )
            self._zeroconf.register_service(info_osc)

            # ---- OSCQuery (TCP) – OUR PORT ----
            info_query = ServiceInfo(
                "_oscjson._tcp.local.",
                f"{self.service_name}._oscjson._tcp.local.",
                addresses=[ip],
                port=self.oscquery_port,
                properties={
                    b"role": b"output",
                    b"EXTENSIONS": b"ACCESS,VALUE,DESCRIPTION,RANGE,LISTEN,PATH_CHANGED"
                },
                server="stream-connector.local."
            )
            self._zeroconf.register_service(info_query)

            # Cache last-known ports
            OSCBridge._last_known_ports["oscquery_port"] = self.oscquery_port

            log_osc_core(
                "Advertising Stream Connector OSC services",
                {
                    "advertise_osc_port": self.advertise_osc_port,
                    "oscquery_port": self.oscquery_port
                },
                level="INFO",
                action_type="init"
            )

            # Emit advertised state for UI / heartbeat
            try:
                q.put((
                    "osc",
                    "advertised",
                    {
                        "osc_port": self.advertise_osc_port,
                        "oscquery_port": self.oscquery_port
                    }
                ))
            except Exception:
                pass

            # Start OSCQuery metadata server
            threading.Thread(
                target=self._start_metadata_server,
                args=(self.oscquery_port,),
                daemon=True
            ).start()

        except Exception as exc:
            log_osc_core(
                "Failed advertising OSC services",
                level="ERROR",
                action_type="init",
                exc=exc
            )

            # Emit failure state
            try:
                q.put(("osc", "error", {"stage": "advertise"}))
            except Exception:
                pass

    # ────────────────────────────────────────────────────── PARAM FETCHING (VRChat)
    def fetch_parameters(self):
        if not self.query_port:
            log_osc_core(
                "Cannot fetch parameters – VRChat query port unknown",
                level="ERROR",
                action_type="receive"
            )
            return {}

        url = f"http://{self._host}:{self.query_port}{self._osc_query_endpoint}"
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                self._param_cache = json.loads(response.read().decode())
                log_osc_core(
                    "Parameters fetched successfully.",
                    {"url": url},
                    level="INFO",
                    action_type="receive"
                )
        except Exception as e:
            log_osc_core(
                f"Failed to fetch parameters from {url}",
                {"url": url},
                level="ERROR",
                action_type="receive",
                exc=e
            )
            self._param_cache = {}
        return self._param_cache

    def _find_param(self, node: dict, full_path: str):
        if not isinstance(node, dict):
            return None
        if node.get("FULL_PATH") == full_path:
            return node
        for child in (node.get("CONTENTS") or {}).values():
            result = self._find_param(child, full_path)
            if result:
                return result
        return None

# ────────────────────────────────────────────────────── SENDING (TO VRCHAT)
    def _normalize_addr(self, addr: str) -> str:
        try:
            addr = addr.strip()
            if "::#" in addr:
                addr = addr.split("::#", 1)[0]
            return addr
        except Exception:
            return addr


    def _record_send(self, addr: str, value):
        addr = self._normalize_addr(addr)
        with self._state_lock:
            self._sent_state[addr] = value
            self._sent_generation[addr] = self._sent_generation.get(addr, 0) + 1


    def get_last_sent(self, addr: str):
        addr = self._normalize_addr(addr)
        with self._state_lock:
            return self._sent_state.get(addr)


    def send(
        self,
        path: str,
        value,
        timer_secs: float = 0.0,
        *,
        reset_to=None,
        pulse: bool = False,
    ):
        import threading
        from pythonosc.udp_client import SimpleUDPClient

        path = self._normalize_addr(path)

        # ─────────────────────────────────────────
        # Avatar change passthrough
        # ─────────────────────────────────────────
        if path.startswith("/avatar/change"):
            try:
                client = SimpleUDPClient(self._host, self.vrchat_osc_port)
                client.send_message("/avatar/change", str(value))
                self._record_send(path, value)
                return True
            except Exception as e:
                log_osc_core("Avatar change failed", exc=e, level="ERROR")
                return False

        # ─────────────────────────────────────────
        # Validate parameter
        # ─────────────────────────────────────────
        if self._param_cache is None:
            self.fetch_parameters()

        meta = self._find_param(self._param_cache, path)
        if not meta or meta.get("ACCESS") != 3:
            log_osc_core("Param not writable", {"path": path}, level="ERROR")
            return False

        param_type = meta.get("TYPE")

        # ─────────────────────────────────────────
        # Type coercion (SEND)
        # ─────────────────────────────────────────
        try:
            if param_type == "T":
                value = int(bool(value))
            elif param_type == "i":
                value = int(value)
            elif param_type == "f":
                value = float(value)
            elif param_type == "s":
                value = str(value)
            else:
                return False
        except Exception as e:
            log_osc_core("Type coercion failed", {"path": path}, exc=e, level="ERROR")
            return False

        # ─────────────────────────────────────────
        # SEND
        # ─────────────────────────────────────────
        try:
            client = SimpleUDPClient(self._host, self.vrchat_osc_port)
            client.send_message(path, value)

            with self._state_lock:
                self._sent_state[path] = value
                self._sent_generation[path] = self._sent_generation.get(path, 0) + 1
                gen = self._sent_generation[path]

            log_osc_core("Sent OSC", {"path": path, "value": value}, level="INFO")

        except Exception as e:
            log_osc_core("OSC send failed", {"path": path}, exc=e, level="ERROR")
            return False

        # ─────────────────────────────────────────
        # RESET LOGIC (CORRECT & DETERMINISTIC)
        # ─────────────────────────────────────────
        if timer_secs <= 0 and not pulse:
            return True

        # --- TOGGLE LOGIC ---
        if param_type in ("T", "i"):
            reset_value = 0 if int(value) else 1

        elif param_type == "f":
            reset_value = 0.0 if float(value) > 0.0 else 1.0

        elif param_type == "s":
            return True

        else:
            return True

        def _reset(expected_gen=gen, rv=reset_value):
            with self._state_lock:
                if self._sent_generation.get(path) != expected_gen:
                    return

            try:
                rc = SimpleUDPClient(self._host, self.vrchat_osc_port)
                rc.send_message(path, rv)

                with self._state_lock:
                    self._sent_state[path] = rv
                    self._sent_generation[path] = self._sent_generation.get(path, 0) + 1

                log_osc_core("OSC reset", {"path": path, "value": rv}, level="INFO")

            except Exception as e:
                log_osc_core("Reset failed", {"path": path}, exc=e, level="ERROR")

        threading.Timer(0.05 if pulse else float(timer_secs), _reset).start()
        return True

    # ────────────────────────────────────────────────────── CLEANUP
    def close(self):
        try:
            self._zeroconf.close()
        except Exception:
            pass

    # ────────────────────────────────────────────────────── OSCQUERY SERVER
    def _start_metadata_server(self, port: int):
        server = HTTPServer((self._host, port), OSCQueryHandler)
        server.service_name = self.service_name
        server.bridge = self

        log_osc_core(
            "OSCQuery HTTP server running",
            {"host": self._host, "port": port},
            level="INFO",
            action_type="init"
        )

        server.serve_forever()

class OSCQueryHandler(BaseHTTPRequestHandler):
    """
    OSCQuery response schema aligned to VRChat's library expectations:
    - /?HOST_INFO returns NAME, EXTENSIONS, OSC_IP, OSC_PORT, OSC_TRANSPORT :contentReference[oaicite:3]{index=3}
    - Nodes use FULL_PATH, ACCESS, CONTENTS (+ optional TYPE/VALUE/DESCRIPTION) 
    """

    def _send_json(self, payload: dict, status: int = 200):
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("pragma", "no-cache")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        bridge = self.server.bridge

        # Normalize path like the C# server does (RootNodeMiddleware uses LocalPath) :contentReference[oaicite:5]{index=5}
        # But we also need query checks (HOST_INFO).
        raw = self.path or "/"
        path_only = raw.split("?", 1)[0].rstrip("/") or "/"

        # ─────────────────────────────────────────────
        # HOST_INFO (VRChat schema)
        # ─────────────────────────────────────────────
        if "HOST_INFO" in raw:
            # In the VRChat library HostInfo includes OSC_IP + OSC_PORT + OSC_TRANSPORT :contentReference[oaicite:6]{index=6}
            # For Stream Connector, "OSC_PORT" should advertise where others send OSC to us (advertise_osc_port),
            # not VRChat's 9000.
            self._send_json({
                "NAME": self.server.service_name,
                "EXTENSIONS": {
                    "ACCESS": True,
                    "CLIPMODE": False,
                    "RANGE": True,
                    "TYPE": True,
                    "VALUE": True,
                    "DESCRIPTION": True,
                },
                "OSC_IP": bridge._host,
                "OSC_PORT": int(bridge.advertise_osc_port),
                "OSC_TRANSPORT": "UDP"
            })
            return

        # ─────────────────────────────────────────────
        # ROOT "/"
        # ─────────────────────────────────────────────
        if path_only == "/":
            self._send_json({
                "FULL_PATH": "/",
                "ACCESS": 0,
                "CONTENTS": {
                    "avatar": {
                        "FULL_PATH": "/avatar",
                        "ACCESS": 3,
                        "CONTENTS": {
                            "change": {
                                "FULL_PATH": "/avatar/change",
                                "ACCESS": 3,
                                "TYPE": "s",
                                "VALUE": [""]
                            },
                            "parameters": {
                                "FULL_PATH": "/avatar/parameters",
                                "ACCESS": 1,
                                "DESCRIPTION": "VRChat parameter mirror (cached)"
                            }
                        }
                    },
                    "stream_connector": {
                        "FULL_PATH": "/stream_connector",
                        "ACCESS": 1,
                        "DESCRIPTION": "Stream Connector runtime namespace",
                        "CONTENTS": {
                            "ports": {
                                "FULL_PATH": "/stream_connector/ports",
                                "ACCESS": 1,
                                "DESCRIPTION": "Dynamic port mapping (live)"
                            }
                        }
                    }
                }
            })
            return

        # ─────────────────────────────────────────────
        # /avatar/parameters (cached mirror)
        # ─────────────────────────────────────────────
        if path_only == "/avatar/parameters":
            # If empty, serve a minimal node shape
            payload = bridge._param_cache
            if not isinstance(payload, dict) or "FULL_PATH" not in payload:
                payload = {
                    "FULL_PATH": "/avatar/parameters",
                    "ACCESS": 1,
                    "CONTENTS": {}
                }
            self._send_json(payload)
            return

        # ─────────────────────────────────────────────
        # /stream_connector
        # ─────────────────────────────────────────────
        if path_only == "/stream_connector":
            self._send_json({
                "FULL_PATH": "/stream_connector",
                "ACCESS": 1,
                "DESCRIPTION": "Stream Connector runtime namespace",
                "CONTENTS": {
                    "ports": {
                        "FULL_PATH": "/stream_connector/ports",
                        "ACCESS": 1,
                        "DESCRIPTION": "Dynamic port mapping (live)"
                    }
                }
            })
            return

        # ─────────────────────────────────────────────
        # /stream_connector/ports (dynamic tree)
        # ─────────────────────────────────────────────
        if path_only == "/stream_connector/ports":
            ports = bridge.get_port_mapping()

            def leaf(full_path: str, osc_type: str, value):
                return {
                    "FULL_PATH": full_path,
                    "ACCESS": 1,
                    "TYPE": osc_type,
                    "VALUE": [value]
                }

            self._send_json({
                "FULL_PATH": "/stream_connector/ports",
                "ACCESS": 1,
                "DESCRIPTION": "Dynamic port mapping (live)",
                "CONTENTS": {
                    "OSC_IN_ADDR": leaf("/stream_connector/ports/OSC_IN_ADDR", "s", str(ports["OSC_IN_ADDR"])),
                    "OSC_IN_PORT": leaf("/stream_connector/ports/OSC_IN_PORT", "i", int(ports["OSC_IN_PORT"])),
                    "OSC_OUT_PORT": leaf("/stream_connector/ports/OSC_OUT_PORT", "i", int(ports["OSC_OUT_PORT"])),
                    "OSC_ADVERTISE_PORT": leaf("/stream_connector/ports/OSC_ADVERTISE_PORT", "i", int(ports["OSC_ADVERTISE_PORT"])),
                    "OSCQUERY_PORT": leaf("/stream_connector/ports/OSCQUERY_PORT", "i", int(ports["OSCQUERY_PORT"])),
                    "VRCHAT_QUERY_PORT": leaf("/stream_connector/ports/VRCHAT_QUERY_PORT", "i", int(ports["VRCHAT_QUERY_PORT"])),
                }
            })
            return

        # ─────────────────────────────────────────────
        # /stream_connector/ports/<key> leaf access
        # ─────────────────────────────────────────────
        if path_only.startswith("/stream_connector/ports/"):
            key = path_only.split("/")[-1]
            ports = bridge.get_port_mapping()
            if key in ports:
                val = ports[key]
                if isinstance(val, int):
                    osc_type = "i"
                    v = int(val)
                else:
                    osc_type = "s"
                    v = str(val)
                self._send_json({
                    "FULL_PATH": path_only,
                    "ACCESS": 1,
                    "TYPE": osc_type,
                    "VALUE": [v]
                })
                return

        # ─────────────────────────────────────────────
        # Not Found
        # ─────────────────────────────────────────────
        self._send_json({"error": "OSC Path not found", "path": path_only}, status=404)


# ─────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────
OWO_DIR = resolve_runtime_path("saved/config/owo")
OWO_DLL = os.path.join(OWO_DIR, "OWO.dll")
TEMPLATE_DIR = Path("saved") / "controls" / "owo"

if not os.path.exists(OWO_DLL):
    raise FileNotFoundError(f"OWO.dll missing: {OWO_DLL}")

import clr
from System.Reflection import Assembly
if not os.path.isfile(OWO_DLL):
    raise RuntimeError("OWO SDK missing")

Assembly.LoadFrom(OWO_DLL)  # safer than UnsafeLoadFrom


from OWOGame import OWO, SensationsFactory, Muscle, ConnectionState, GameAuth

class OWOVestManager:
    """
    Hybrid OWO controller
    - SDK-based connection
    - Legacy .owo template support
    - Structured logging
    - Stream Connector compatible
    """

    GAME_ID = "7924195"

    # ─────────────────────────────────────────────
    # Init
    # ─────────────────────────────────────────────

    def __init__(self):
        self._connecting = False
        self._template_dir = TEMPLATE_DIR
        self._templates: set[str] = set()

        log_owo("Initializing OWO subsystem")

        self._configure()
        self._load_templates()
        self._last_state = None
        self._start_watchdog()

        self._template_lock = threading.Lock()
        self._template_queue = deque()
        self._template_worker_running = False

        self._sensation_queue = deque()
        self._sensation_worker_running = False
        self._sensation_lock = threading.Lock()

        # ─────────────────────────────────────────────
        # Busy gate (prevents cross-chain collisions)
        # ─────────────────────────────────────────────
        self._busy_lock = threading.Lock()
        self._busy_until = 0.0  # epoch seconds
        
    # ─────────────────────────────────────────────
    # SDK Setup
    # ─────────────────────────────────────────────

    def _configure(self):
        log_owo("Configuring OWO GameAuth")

        auth = GameAuth.Create().WithId(self.GAME_ID)
        OWO.Configure(auth)

        # ✅ REQUIRED: kick off initial connection
        try:
            log_owo("Initializing OWO connection")
            OWO.AutoConnect()
        except Exception as exc:
            log_owo(
                "OWO AutoConnect failed",
                level="ERROR",
                exc=exc
            )

    def connect(self) -> bool:
        """
        Passive attach only.
        OWO initiates the connection — we just observe.
        """
        if self._connecting:
            return False

        self._connecting = True

        try:
            if self.is_connected():
                log_owo("OWO already connected", action="connect")
                return True

            log_owo("Waiting for OWO to connect…", action="connect")
            return False

        finally:
            self._connecting = False

    def is_connected(self) -> bool:
        try:
            state = OWO.ConnectionState
        except Exception:
            return False

        if state != self._last_state:
            log_owo(f"OWO state changed → {state}", action="connect")
            self._last_state = state

        return state == ConnectionState.Connected

    # ─────────────────────────────────────────────
    # Watchdog
    # ─────────────────────────────────────────────
    def _start_watchdog(self):
        if getattr(self, "_owo_watchdog_running", False):
            return

        self._owo_watchdog_running = True

        def loop():
            log_owo("OWO watchdog started", action="runtime")

            while True:
                try:
                    # This already logs transitions internally
                    self.is_connected()
                    time.sleep(1.0)

                except Exception as exc:
                    log_owo(
                        "OWO watchdog error",
                        level="ERROR",
                        action="runtime",
                        exc=exc
                    )
                    time.sleep(2)

        threading.Thread(
            target=loop,
            daemon=True,
            name="OWO-Watchdog"
        ).start()

    # ─────────────────────────────────────────────
    # Busy Gate API (authoritative)
    # ─────────────────────────────────────────────

    def get_busy_until(self) -> float:
        try:
            with self._busy_lock:
                return float(self._busy_until or 0.0)
        except Exception:
            return 0.0

    def is_busy(self) -> bool:
        return self.get_busy_until() > time.time()

    def _mark_busy_for(self, seconds: float, *, reason: str = "") -> float:
        """
        Extends busy window by 'seconds' starting from max(now, existing_busy).
        Returns new busy_until.
        """
        try:
            sec = float(seconds or 0.0)
        except Exception:
            sec = 0.0

        if sec <= 0:
            return self.get_busy_until()

        now = time.time()
        with self._busy_lock:
            base = max(now, float(self._busy_until or 0.0))
            self._busy_until = base + sec
            new_until = self._busy_until

        log_owo(
            "OwO busy window extended",
            {"add_s": round(sec, 3), "busy_until": round(new_until, 3), "reason": reason},
            action="runtime",
        )
        return float(new_until)

    # ─────────────────────────────────────────────
    # Duration Estimation (templates / patterns)
    # ─────────────────────────────────────────────

    def _estimate_template_duration_s(self, content: str) -> float:
        """
        Estimates *actual* wall time the template worker will occupy the timing lock.
        Must match the sleeps used in _run_* methods.
        """
        try:
            content = (content or "").strip()
            if not content:
                return 0.0

            t = self._detect_template_type(content)

            # Dynamic: one step
            if t == "dynamic":
                try:
                    value_part, _ = content.split("|", 1)
                except ValueError:
                    return 0.0
                vals = self._parse_value_block(value_part, duration_scale=1.0)
                # include delay as wall-time spacing
                return max(0.0, float(vals["duration"]) + float(vals.get("delay", 0.0) or 0.0))

            # Baked: body is one dynamic step
            if t == "baked":
                parts = content.split("~")
                if len(parts) < 3:
                    return 0.0
                body = (parts[2] or "").strip().rstrip("#")
                try:
                    value_part, _ = body.split("|", 1)
                except ValueError:
                    return 0.0
                vals = self._parse_value_block(value_part, duration_scale=1.0)
                return max(0.0, float(vals["duration"]) + float(vals.get("delay", 0.0) or 0.0))

            # Legacy: multiple parts; duration is in tenths (duration_scale=0.1)
            if t == "legacy":
                pieces = [p.strip() for p in content.split("|") if p.strip()]
                if not pieces:
                    return 0.0

                # first is values
                first_vals = self._parse_value_block(pieces[0], duration_scale=0.1)
                total = float(first_vals["duration"]) + float(first_vals.get("delay", 0.0) or 0.0)

                current = first_vals
                for part in pieces[1:]:
                    if "&" in part:
                        try:
                            _, value_part = part.split("&", 1)
                            current = self._parse_value_block(value_part, duration_scale=0.1)
                        except Exception:
                            # if parsing fails, assume no additional sleep
                            continue

                    total += float(current["duration"]) + float(current.get("delay", 0.0) or 0.0)

                return max(0.0, total)

            return 0.0
        except Exception:
            return 0.0

    def _estimate_pattern_duration_s(self, pattern: list) -> float:
        """
        Pattern steps use fields:
          duration (seconds), delay_s (seconds)
        """
        total = 0.0
        try:
            if not isinstance(pattern, list):
                return 0.0
            for s in pattern:
                if not isinstance(s, dict):
                    continue
                try:
                    dur = float(s.get("duration", 0.0) or 0.0)
                except Exception:
                    dur = 0.0
                try:
                    dly = float(s.get("delay_s", 0.0) or 0.0)
                except Exception:
                    dly = 0.0
                if dur < 0: dur = 0.0
                if dly < 0: dly = 0.0
                total += dur + dly
        except Exception:
            return 0.0
        return max(0.0, total)

    # ─────────────────────────────────────────────
    # Template Handling (SDK-Compatible)
    # ─────────────────────────────────────────────

    def _load_templates(self):
        if not self._template_dir.exists():
            self._templates = set()
            log_owo(
                "Template directory missing",
                {"dir": str(self._template_dir)},
                level="WARNING"
            )
            return

        self._templates = {p.stem for p in self._template_dir.glob("*.owo")}

        log_owo(
            "Templates loaded",
            {"count": len(self._templates)},
            level="INFO"
        )

    def list_available_files(self) -> list[str]:
        """
        Return available .owo template names (without extension).
        Used by UI and chain editor.
        """
        try:
            if not self._template_dir.exists():
                return []

            return sorted(
                p.stem for p in self._template_dir.glob("*.owo")
            )
        except Exception as e:
            log_owo(
                "Failed to list OWO templates",
                {"dir": str(self._template_dir)},
                level="ERROR",
                exc=e
            )
            return []

    # ─────────────────────────────────────────────
    # Template Dispatcher
    # ─────────────────────────────────────────────

    def send_file(self, name: str):
        if not self.is_connected():
            log_owo("Cannot send template — not connected", level="WARNING")
            return

        path = self._template_dir / f"{name}.owo"
        if not path.exists():
            log_owo("Template not found", {"template": name}, level="ERROR")
            return

        content = path.read_text(encoding="utf-8").strip()
        
        # Predict wall time and extend busy window immediately
        est = self._estimate_template_duration_s(content)
        self._mark_busy_for(est, reason=f"template:{name}")

        # ✅ Always enqueue as (name, content)
        self._template_queue.append((name, content))

        log_owo("Template queued", {"template": name})

        if not self._template_worker_running:
            threading.Thread(target=self._template_worker, daemon=True).start()

    def _template_worker(self):
        self._template_worker_running = True

        while self._template_queue:
            item = self._template_queue.popleft()

            try:
                if isinstance(item, tuple):
                    name, content = item
                else:
                    name = str(item)
                    content = (self._template_dir / f"{name}.owo").read_text(encoding="utf-8")

                self._execute_template(content)

            except Exception as e:
                log_owo(
                    "Template execution failed",
                    {"template": name},
                    level="ERROR",
                    exc=e
                )

        self._template_worker_running = False
        
    def _execute_template(self, content: str):
        with self._template_lock:
            template_type = self._detect_template_type(content)

            if template_type == "sdk":
                self._run_sdk_template(content)
            elif template_type == "legacy":
                self._run_legacy_as_sdk(content)
            else:
                raise ValueError("Unknown template format")

    # ─────────────────────────────────────────────
    # Template Detection + Unified Queue Dispatcher
    # ─────────────────────────────────────────────

    def _detect_template_type(self, content: str) -> str:
        """
        Types:
          - baked  : starts with "0~..."
          - legacy : legacy stream format (stateful), identified by '&' and/or legacy timing semantics
          - dynamic: "freq,dur,int,ru,rd,dl,<tag>|0%100,1%50" (no leading 0~)
        """
        content = (content or "").strip()
        if not content:
            return "unknown"
        if content.startswith("0~"):
            return "baked"
        # Legacy templates are the ONLY ones in your corpus that use '&' as a step operator
        if "&" in content:
            return "legacy"
        # Dynamic SDK always has a single values|muscles split
        if "|" in content and "%" in content:
            return "dynamic"
        return "unknown"


    def _ensure_template_runtime(self):
        """Idempotent runtime init for template queue + timing lock."""
        if not hasattr(self, "_template_queue"):
            from collections import deque
            self._template_queue = deque()
        if not hasattr(self, "_template_worker_running"):
            self._template_worker_running = False
        # Single timing lock used by BOTH templates and live sensations
        if not hasattr(self, "_owo_timing_lock"):
            self._owo_timing_lock = threading.Lock()


    def queue_template(self, template_name: str, content: str):
        """
        Queue-aware entrypoint for templates (file-backed or raw content).
        """
        self._ensure_template_runtime()
        self._template_queue.append((template_name, content))
        log_owo("Template queued", {"template": template_name}, action="runtime")

        if not self._template_worker_running:
            self._template_worker_running = True
            threading.Thread(target=self._template_worker, daemon=True).start()


    def _template_worker(self):
        """
        Single-threaded template executor.
        IMPORTANT: This is where timing MUST be enforced (do not sleep in callers).
        """
        self._ensure_template_runtime()

        while self._template_queue:
            name, content = self._template_queue.popleft()
            try:
                self._execute_template(content)
            except Exception as e:
                log_owo(
                    "Template execution failed",
                    {"template": name},
                    level="ERROR",
                    action="runtime",
                    exc=e
                )

        self._template_worker_running = False


    def _execute_template(self, content: str):
        """
        Unified dispatcher: detect type, run appropriate parser.
        """
        if not self.is_connected():
            log_owo("Cannot execute template — not connected", level="WARNING", action="runtime")
            return

        content = (content or "").strip()
        t = self._detect_template_type(content)

        if t == "baked":
            self._run_baked_sdk_template(content)
        elif t == "legacy":
            self._run_legacy_as_sdk(content)
        elif t == "dynamic":
            self._run_dynamic_sdk_template(content)
        else:
            log_owo("Unknown template format", {"preview": content[:160]}, level="ERROR", action="runtime")


    # ─────────────────────────────────────────────
    # Shared Parsing Helpers
    # ─────────────────────────────────────────────

    def _parse_value_block(self, value_str: str, *, duration_scale: float = 1.0) -> dict:
        """
        Reads the FIRST 6 numeric-ish fields out of a value block.
        Extra tokens (like names/tags) are ignored.
        Missing tokens are treated as 0.

        Returns:
          frequency(int), duration(float seconds), intensity(int 0-100),
          ramp_up(float), ramp_down(float), delay(float)
        """
        tokens = [t.strip() for t in (value_str or "").split(",")]

        nums = []
        for t in tokens:
            if t == "":
                continue
            if len(nums) >= 6:
                break
            try:
                nums.append(float(t))
            except Exception:
                # non-numeric tag (e.g. "Hit", "Jump") → ignore
                continue

        while len(nums) < 6:
            nums.append(0.0)

        frequency = int(nums[0])
        duration  = float(nums[1]) * float(duration_scale)
        intensity = int(nums[2])
        ramp_up   = float(nums[3])
        ramp_down = float(nums[4])
        delay     = float(nums[5])

        # Clamp safety
        if frequency < 0:
            frequency = 0
        if duration < 0:
            duration = 0.0
        if intensity < 0:
            intensity = 0
        if intensity > 100:
            intensity = 100

        return {
            "frequency": frequency,
            "duration": duration,
            "intensity": intensity,
            "ramp_up": ramp_up,
            "ramp_down": ramp_down,
            "delay": delay,
        }


    # ─────────────────────────────────────────────
    # SDK Template Handler (Unified)
    # ─────────────────────────────────────────────

    def _run_sdk_template(self, content: str):
        """
        Executes SDK-format OWO templates.
        Supports:
          - Dynamic sensations (no leading "0~")
          - Baked sensations (leading "0~")
        """
        content = (content or "").strip()
        if not content:
            return
        if content.startswith("0~"):
            self._run_baked_sdk_template(content)
        else:
            self._run_dynamic_sdk_template(content)


    # ─────────────────────────────────────────────
    # Dynamic Template Handler (SDK)
    # ─────────────────────────────────────────────

    def _run_dynamic_sdk_template(self, content: str):
        """
        Dynamic sensation (guide):
          "60,1,70,0,0,0,Hit|0%100"
        Meaning:
          freq,duration(s),intensity(0-100),rampUp,rampDown,delay,<tag>|<muscles>

        We ignore the tag (if present) and only parse first 6 numeric fields.
        """
        try:
            value_part, muscle_part = content.split("|", 1)
        except ValueError:
            log_owo("Invalid dynamic SDK template (missing '|')", {"preview": content[:160]}, level="ERROR", action="runtime")
            return

        vals = self._parse_value_block(value_part, duration_scale=1.0)
        muscles = self._parse_legacy_muscles(muscle_part)

        if not muscles:
            log_owo("Dynamic SDK has no muscles", {"preview": content[:160]}, level="WARNING", action="runtime")
            return

        with self._owo_timing_lock:
            for muscle, strength in muscles:
                final_intensity = int(vals["intensity"] * (strength / 100.0))
                if final_intensity < 0:
                    final_intensity = 0
                if final_intensity > 100:
                    final_intensity = 100

                sensation = SensationsFactory.Create(
                    int(vals["frequency"]),
                    float(vals["duration"]),
                    int(final_intensity),
                    float(vals["ramp_up"]),
                    float(vals["ramp_down"]),
                    float(vals["delay"]),
                )
                OWO.Send(sensation, muscle)

            time.sleep(float(vals["duration"]) + float(vals.get("delay", 0.0) or 0.0))

        log_owo("Dynamic SDK template executed", action="runtime")


    # ─────────────────────────────────────────────
    # Baked Template Handler (SDK)
    # ─────────────────────────────────────────────

    def _run_baked_sdk_template(self, content: str):
        """
        Baked sensation (guide):
          "0~Jump~100,4,50,0,400,0,Jump|2%100~environment-5~Parkour#"

        We:
          - split on "~"
          - body is the 3rd field (index 2)
          - body contains "values|muscles" (single dynamic step in practice)
        """
        parts = content.split("~")
        if len(parts) < 3:
            log_owo("Invalid baked SDK template", {"preview": content[:160]}, level="ERROR", action="runtime")
            return

        body = (parts[2] or "").strip()
        if not body:
            log_owo("Invalid baked SDK template (empty body)", level="ERROR", action="runtime")
            return

        # Body may have trailing markers (e.g. '#') → harmless
        body = body.strip().rstrip("#")

        try:
            value_part, muscle_part = body.split("|", 1)
        except ValueError:
            log_owo("Invalid baked SDK body (missing '|')", {"body": body[:160]}, level="ERROR", action="runtime")
            return

        vals = self._parse_value_block(value_part, duration_scale=1.0)
        muscles = self._parse_legacy_muscles(muscle_part)

        if not muscles:
            log_owo("Baked SDK has no muscles", {"preview": body[:160]}, level="WARNING", action="runtime")
            return

        with self._owo_timing_lock:
            for muscle, strength in muscles:
                final_intensity = int(vals["intensity"] * (strength / 100.0))
                if final_intensity < 0:
                    final_intensity = 0
                if final_intensity > 100:
                    final_intensity = 100

                sensation = SensationsFactory.Create(
                    int(vals["frequency"]),
                    float(vals["duration"]),
                    int(final_intensity),
                    float(vals["ramp_up"]),
                    float(vals["ramp_down"]),
                    float(vals["delay"]),
                )
                OWO.Send(sensation, muscle)

            time.sleep(float(vals["duration"]) + float(vals.get("delay", 0.0) or 0.0))

        log_owo("Baked SDK template executed", action="runtime")


    # ─────────────────────────────────────────────
    # Legacy Template Handler (Translate → SDK)
    # ─────────────────────────────────────────────

    def _run_legacy_as_sdk(self, content: str):
        """
        Legacy stream format (stateful):
            VALUES |
            MUSCLES & VALUES |
            MUSCLES & VALUES |
            MUSCLES

        Notes:
          - The FIRST chunk is a VALUES block (legacy duration is in tenths -> /10 seconds)
          - Subsequent chunks may be:
              * "muscles&values"  (update state)
              * "muscles"         (reuse last values)
          - Per-muscle strength comes from the muscle block (e.g. 7%53)
          - Base intensity comes from the values block (3rd numeric)
        """
        parts = [p.strip() for p in (content or "").split("|") if p.strip()]
        if not parts:
            return

        # First chunk: values (duration in tenths for legacy)
        try:
            current = self._parse_value_block(parts.pop(0), duration_scale=0.1)
        except Exception as e:
            log_owo("Failed to parse legacy initial values", level="ERROR", action="runtime", exc=e)
            return

        with self._owo_timing_lock:
            for part in parts:
                if "&" in part:
                    muscle_part, value_part = part.split("&", 1)

                    muscles = self._parse_legacy_muscles(muscle_part)
                    if not muscles:
                        continue

                    # Legacy value blocks: duration in tenths
                    try:
                        current = self._parse_value_block(value_part, duration_scale=0.1)
                    except Exception:
                        log_owo("Invalid legacy value block", {"block": value_part[:160]}, level="WARNING", action="runtime")
                        continue
                else:
                    muscles = self._parse_legacy_muscles(part)
                    if not muscles:
                        continue

                for muscle, strength in muscles:
                    final_intensity = int(current["intensity"] * (strength / 100.0))
                    if final_intensity < 0:
                        final_intensity = 0
                    if final_intensity > 100:
                        final_intensity = 100

                    sensation = SensationsFactory.Create(
                        int(current["frequency"]),
                        float(current["duration"]),
                        int(final_intensity),
                        float(current["ramp_up"]),
                        float(current["ramp_down"]),
                        float(current["delay"]),
                    )
                    OWO.Send(sensation, muscle)

                time.sleep(float(current["duration"]) + float(current.get("delay", 0.0) or 0.0))

        log_owo("Legacy template executed via SDK", action="runtime")


    # ─────────────────────────────────────────────
    # Muscle Parsing
    # ─────────────────────────────────────────────

    def _parse_legacy_muscles(self, block: str) -> list[tuple[Muscle, int]]:
        """
        Parses muscle blocks like:
          "7%100,6%100,9%100,8%100"
        Returns:
          [(Muscle, intensityPct), ...]
        """
        muscle_map = {
            "0": Muscle.Abdominal_L,
            "1": Muscle.Abdominal_R,
            "2": Muscle.Pectoral_L,
            "3": Muscle.Pectoral_R,
            "4": Muscle.Dorsal_L,
            "5": Muscle.Dorsal_R,
            "6": Muscle.Arm_L,
            "7": Muscle.Arm_R,
            "8": Muscle.Lumbar_L,
            "9": Muscle.Lumbar_R,
        }

        results: list[tuple[Muscle, int]] = []
        if not block:
            return results

        for token in block.split(","):
            token = token.strip()
            if "%" not in token:
                continue
            try:
                idx, intensity = token.split("%", 1)
                idx = idx.strip()
                inten = int(float(intensity.strip()))
                if inten < 0:
                    inten = 0
                if inten > 100:
                    inten = 100
                m = muscle_map.get(idx)
                if m:
                    results.append((m, inten))
            except Exception:
                continue

        return results

    def _parse_muscle_map(self, token: str) -> list[Muscle]:
        """
        Legacy helper (no intensities) for old call sites that expect only muscles.
        Accepts '&' separated entries like:
          "0%100&1%50&7%100"
        """
        muscle_map = {
            "0": Muscle.Abdominal_L,
            "1": Muscle.Abdominal_R,
            "2": Muscle.Pectoral_L,
            "3": Muscle.Pectoral_R,
            "4": Muscle.Dorsal_L,
            "5": Muscle.Dorsal_R,
            "6": Muscle.Arm_L,
            "7": Muscle.Arm_R,
            "8": Muscle.Lumbar_L,
            "9": Muscle.Lumbar_R,
        }

        muscles: list[Muscle] = []
        if not token:
            return muscles

        for entry in token.split("&"):
            entry = entry.strip()
            if "%" not in entry:
                continue
            try:
                idx, _ = entry.split("%", 1)
                m = muscle_map.get(idx.strip())
                if m:
                    muscles.append(m)
            except Exception:
                continue

        return muscles

    # ─────────────────────────────────────────────
    # Muscle ID ↔ SDK mapping (authoritative)
    # ─────────────────────────────────────────────
    _MUSCLE_ID_MAP = {
        0: Muscle.Abdominal_L,
        1: Muscle.Abdominal_R,
        2: Muscle.Pectoral_L,
        3: Muscle.Pectoral_R,
        4: Muscle.Dorsal_L,
        5: Muscle.Dorsal_R,
        6: Muscle.Arm_L,
        7: Muscle.Arm_R,
        8: Muscle.Lumbar_L,
        9: Muscle.Lumbar_R,
    }

    _MUSCLE_NAME_BY_ID = {
        0: "Abdominal_L",
        1: "Abdominal_R",
        2: "Pectoral_L",
        3: "Pectoral_R",
        4: "Dorsal_L",
        5: "Dorsal_R",
        6: "Arm_L",
        7: "Arm_R",
        8: "Lumbar_L",
        9: "Lumbar_R",
    }

    def _muscle_from_pattern_id(self, mid: int):
        try:
            mid = int(mid)
        except Exception:
            return None

        if mid in self._MUSCLE_ID_MAP:
            return self._MUSCLE_ID_MAP[mid]

        return None

    def _muscle_debug(self, muscle: Muscle, pct: int = 100) -> dict:
        """Stable debug payload for logs (never returns 'OWOGame.Muscle')."""
        mid = None
        try:
            mid = int(muscle)  # sometimes works with pythonnet enums
        except Exception:
            try:
                mid = int(getattr(muscle, "value"))
            except Exception:
                mid = None

        name = self._MUSCLE_NAME_BY_ID.get(mid) if mid is not None else None
        if not name:
            # pythonnet enums: ToString() is often available
            try:
                name = muscle.ToString()
            except Exception:
                name = repr(muscle)

        return {"id": mid, "name": name, "pct": int(pct)}

    # ─────────────────────────────────────────────
    # Pattern Sensations (SDK) — Queue Aware + Timing Lock
    # ─────────────────────────────────────────────
    def _execute_pattern(self, steps: list[dict]):
        if not steps:
            return

        log_owo("Executing pattern", {"steps": len(steps)}, action="pattern")

        for step in steps:
            try:
                intensity = float(step.get("intensity", 1.0))
                duration  = float(step.get("duration", 1.0))
                frequency = float(step.get("frequency", 50))
                delay     = float(step.get("delay_s", 0.0))

                muscles = []
                for m in step.get("muscles", []):
                    try:
                        mid = int(m.get("id"))
                        muscle = self._MUSCLE_ID_MAP.get(mid)
                        if muscle:
                            muscles.append(muscle)
                    except Exception:
                        continue

                if not muscles:
                    log_owo("Pattern step has no valid muscles", level="WARNING")
                    continue

                self.send_sensation(
                    intensity=intensity,
                    duration=duration,
                    frequency=frequency,
                    muscles=muscles,
                )

                if delay > 0:
                    time.sleep(delay)

            except Exception as exc:
                log_owo(
                    "Pattern step failed",
                    level="ERROR",
                    exc=exc,
                    action="pattern",
                )

    def run_pattern(self, pattern: list[dict]):
        if not pattern:
            return

        for step in pattern:
            try:
                intensity = float(step.get("intensity", 1.0))
                duration  = float(step.get("duration", 1.0))
                frequency = int(step.get("frequency", 50))

                # ✅ FIX: Convert IDs → Muscle enum HERE
                muscles = []
                for m in step.get("muscles", []):
                    try:
                        mid = int(m.get("id"))
                        pct = int(m.get("pct", 100))
                        muscle = Muscle(mid)
                        muscles.append((muscle, pct))
                    except Exception:
                        continue

                if not muscles:
                    log_owo(
                        "Pattern step has no valid muscles — skipping",
                        {"step": step},
                        level="WARNING",
                        action="pattern",
                    )
                    continue

                self.send_sensation(
                    intensity=intensity,
                    duration=duration,
                    frequency=frequency,
                    muscles=muscles,
                )

                delay = float(step.get("delay_s", 0.0))
                if delay > 0:
                    time.sleep(delay)

            except Exception as exc:
                log_owo(
                    "Pattern execution failed",
                    level="ERROR",
                    exc=exc,
                    action="pattern",
                )

    # ─────────────────────────────────────────────
    # Live Sensations (SDK) — Queue Aware + Timing Lock
    # ─────────────────────────────────────────────
    def send_sensation(
        self,
        intensity: float,
        duration: float = 0.3,
        frequency: float = 50,
        muscles=None,
    ):
        if not self.is_connected():
            log_owo("Blocked send — not connected", level="WARNING", action="sensation")
            return

        self._ensure_template_runtime()

        # ─────────────────────────────────────────────
        # Normalize muscles → List[OWOGame.Muscle]
        # ─────────────────────────────────────────────
        resolved = []

        if muscles:
            for m in muscles:
                try:
                    # If tuple like (Muscle, pct)
                    if isinstance(m, tuple):
                        m = m[0]

                    # If int ID
                    if isinstance(m, int):
                        m = self._MUSCLE_ID_MAP.get(m)

                    # If already Muscle enum
                    if isinstance(m, Muscle):
                        resolved.append(m)
                except Exception:
                    continue

        # Safety fallback
        if not resolved:
            resolved = [
                Muscle.Abdominal_L,
                Muscle.Abdominal_R,
            ]

        # Normalize intensity
        intensity = int(max(0, min(100, intensity if intensity > 1 else intensity * 100)))
        duration = float(max(0.05, duration))
        frequency = int(frequency)

        with self._owo_timing_lock:
            sensation = SensationsFactory.Create(
                frequency,
                duration,
                intensity,
                0.0,
                0.0,
                0.0,
            )

            # ✅ THIS IS THE IMPORTANT PART
            OWO.Send(sensation, resolved)

            time.sleep(duration)

        log_owo(
            "Sensation executed",
            {
                "intensity": intensity,
                "duration": duration,
                "frequency": frequency,
                "muscles": [m.ToString() for m in resolved],
            },
            action="sensation",
        )

    def _sensation_worker(self):
        while self._sensation_queue:
            job = self._sensation_queue.popleft()

            try:
                if job.get("type") == "pattern":
                    self._execute_pattern(job["steps"])
                else:
                    self._execute_sensation(job)

            except Exception as e:
                log_owo(
                    "Sensation execution failed",
                    job,
                    level="ERROR",
                    action="sensation",
                    exc=e
                )

        self._sensation_worker_running = False

    def _execute_sensation(self, job: dict):
        base_intensity = int(job.get("intensity", 0))
        duration = float(job.get("duration", 0.3))
        freq = int(job.get("frequency", 50))

        muscles = job.get("muscles") or []

        with self._owo_timing_lock:
            for (mid, muscle, pct) in muscles:
                scaled = int(base_intensity * (pct / 100.0))
                scaled = max(0, min(100, scaled))

                sensation = SensationsFactory.Create(
                    freq,
                    duration,
                    scaled,
                    0.0,
                    0.0,
                    0.0
                )

                OWO.Send(sensation, muscle)

            time.sleep(duration)

        log_owo(
            "Sensation executed",
            {
                "intensity": intensity,
                "duration": duration,
                "frequency": frequency,
                "muscles": [
                    self._MUSCLE_NAME_BY_ID.get(
                        int(m) if hasattr(m, "__int__") else None,
                        str(m)
                    )
                    for m in resolved
                ],
            },
            action="sensation",
        )

    # ─────────────────────────────────────────────
    # Legacy API Compatibility
    # ─────────────────────────────────────────────

    def apply(self, sensation_id: int, intensity: int, duration: int = 1000):
        self.send_sensation(
            intensity=intensity / 100.0,
            duration=duration / 1000.0,
        )

# ─────────────────────────────────────────────
# Intiface Central Class
# ─────────────────────────────────────────────        
class IntifaceCentralClient:
    """
    Stream Connector - Intiface Central Client (Buttplug JSON v3)

    - Persistent WS connection to Intiface Central
    - Proper v3 handshake (RequestServerInfo MessageVersion=3)
    - Tracks device list + add/remove events
    - Exposes vibrate/stop commands
    - Emits events to shared queue `q` like PiShock client does
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 12345,
        *,
        client_name: str = "StreamConnector",
        on_message: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_connected: Optional[Callable[[], None]] = None,
        on_disconnected: Optional[Callable[[], None]] = None,
        q=None,
        logger: Optional[Callable[..., None]] = None,
    ):
        self.host = host
        self.port = port
        self.uri = f"ws://{self.host}:{self.port}"
        self.client_name = client_name

        # callbacks / IPC
        self.on_message = on_message
        self.on_connected = on_connected
        self.on_disconnected = on_disconnected
        self.q = q
        # unified busy gate (used by queue / random / executor)
        self._busy_until = 0.0

        # logging (match your ecosystem pattern)
        self.log = logger or (lambda msg, data=None, level="INFO", exc=None: print(f"[INTIFACE][{level}] {msg} {data or ''}"))

        # runtime state
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.ws = None
        self.connected = False
        self._ws_task = None
        self._keepalive_task = None

        # reconnect backoff
        self._bo = 2.0

        # buttplug message ids
        self._id = 1
        self._id_lock = threading.Lock()

        # device registry: deviceIndex -> device info
        self.devices: Dict[int, Dict[str, Any]] = {}

        # handshake state
        self.server_info: Optional[Dict[str, Any]] = None

        # command gating
        self._send_lock = asyncio.Lock()

    # ─────────────────────────────────────────────────────────────
    # Public lifecycle
    # ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self.loop:
            return

        def _runner():
            try:
                asyncio.set_event_loop(asyncio.new_event_loop())
                self.loop = asyncio.get_event_loop()
                self.log("Connector thread spun-up", level="DEBUG")
                self.loop.create_task(self._connect_loop())
                self.loop.run_forever()
            except Exception as exc:
                self.log("Unhandled error in Intiface connector thread", level="CRITICAL", exc=exc)

        threading.Thread(target=_runner, daemon=True).start()

    def is_running(self) -> bool:
        return self.loop is not None and self.loop.is_running()

    def stop(self) -> None:
        # stop loop safely
        if not self.loop:
            return

        async def _stop():
            try:
                if self.ws:
                    await self.ws.close(code=4000, reason="client stop")
            except Exception:
                pass
            try:
                self.loop.stop()
            except Exception:
                pass

        asyncio.run_coroutine_threadsafe(_stop(), self.loop)

    # ─────────────────────────────────────────────────────────────
    # Queue/event helpers
    # ─────────────────────────────────────────────────────────────

    def _emit(self, event: str, payload: Dict[str, Any] | None = None) -> None:
        if not self.q:
            return
        try:
            self.q.put(("intiface", event, payload or {}))
        except Exception:
            pass

    # ─────────────────────────────────────────────────────────────
    # Buttplug protocol helpers
    # ─────────────────────────────────────────────────────────────

    def _next_id(self) -> int:
        with self._id_lock:
            cur = self._id
            self._id += 1
            return cur

    async def _send_frame(self, obj: Dict[str, Any]) -> None:
        """
        Buttplug requires message frames to be a JSON ARRAY of message objects.
        """
        if not (self.connected and self.ws):
            raise RuntimeError("Intiface WS not connected")

        frame = json.dumps([obj], separators=(",", ":"))
        async with self._send_lock:
            await self.ws.send(frame)

    async def _request_server_info(self) -> None:
        msg = {
            "RequestServerInfo": {
                "Id": self._next_id(),
                "ClientName": self.client_name,
                "MessageVersion": 3,
            }
        }
        await self._send_frame(msg)
        self.log("Handshake sent", level="DEBUG", data=msg)

    async def _request_device_list(self) -> None:
        msg = {"RequestDeviceList": {"Id": self._next_id()}}
        await self._send_frame(msg)
        self.log("RequestDeviceList sent", level="DEBUG", data=msg)

    async def _start_scanning(self) -> None:
        msg = {"StartScanning": {"Id": self._next_id()}}
        await self._send_frame(msg)
        self.log("StartScanning sent", level="DEBUG", data=msg)

    async def _stop_all_devices(self) -> None:
        # Buttplug has StopAllDevices
        msg = {"StopAllDevices": {"Id": self._next_id()}}
        await self._send_frame(msg)
        self.log("StopAllDevices sent", level="DEBUG", data=msg)

    async def _stop_device(self, device_index: int) -> None:
        msg = {"StopDeviceCmd": {"Id": self._next_id(), "DeviceIndex": int(device_index)}}
        await self._send_frame(msg)
        self.log("StopDeviceCmd sent", level="DEBUG", data=msg)

    async def _scalar_vibrate(self, device_index: int, strength: float) -> None:
        # ScalarCmd supports vibrate devices; clamp strength
        s = max(0.0, min(1.0, float(strength)))
        msg = {
            "ScalarCmd": {
                "Id": self._next_id(),
                "DeviceIndex": int(device_index),
                "Scalars": [
                    {
                        "Index": 0,
                        "Scalar": s,
                        "ActuatorType": "Vibrate",
                    }
                ],
            }
        }
        await self._send_frame(msg)
        self.log("ScalarCmd(vibrate) sent", level="DEBUG", data=msg)

    async def _scalar_constrict(self, device_index: int, strength: float) -> None:
        """
        Send Constrict (Air Pump) command respecting StepCount.
        """

        # Clamp input
        strength = max(0.0, min(1.0, float(strength)))

        # Resolve device
        dev = self.devices.get(int(device_index))
        if not dev:
            return

        # Find constrict feature
        features = dev.get("features", {}).get("ScalarCmd", [])
        constrict = None

        for f in features:
            if f.get("ActuatorType") == "Constrict":
                constrict = f
                break

        if not constrict:
            self.log("Device has no Constrict actuator", level="WARN")
            return

        steps = int(constrict.get("StepCount", 1))
        if steps <= 1:
            scalar = 1.0 if strength > 0 else 0.0
        else:
            # Quantize to supported step
            step_index = round(strength * (steps - 1))
            scalar = step_index / (steps - 1)

        msg = {
            "ScalarCmd": {
                "Id": self._next_id(),
                "DeviceIndex": int(device_index),
                "Scalars": [
                    {
                        "Index": 0,
                        "Scalar": scalar,
                        "ActuatorType": "Constrict",
                    }
                ],
            }
        }

        await self._send_frame(msg)

        self.log(
            "ScalarCmd(constrict)",
            level="DEBUG",
            data={
                "device": device_index,
                "requested": strength,
                "quantized": scalar,
                "steps": steps,
            },
        )
       
    # ─────────────────────────────────────────────────────────────
    # Public command surface (sync-friendly)
    # ─────────────────────────────────────────────────────────────

    def vibrate(
        self,
        device_ids: Union[int, str, List[Union[int, str]]] = "all",
        strength: float = 0.5,
        duration_ms: int = 0,
    ) -> None:
        """
        Dispatch vibrate (ScalarCmd).
        Intiface does NOT support shock - this is pure scalar output.
        """
        if not self.loop:
            self.log("Cannot vibrate - loop not running", level="WARN")
            return

        ids = self._resolve_device_ids(device_ids)

        self._emit("send_start", {
            "command": "vibrate",
            "device_ids": ids,
            "strength": strength,
            "duration_ms": duration_ms
        })

        async def _do():
            try:
                for did in ids:
                    await self._scalar_vibrate(int(did), float(strength))

                self._emit("send_dispatched", {"command": "vibrate", "device_ids": ids})

                if duration_ms and duration_ms > 0:
                    await asyncio.sleep(max(0.0, duration_ms / 1000.0))
                    for did in ids:
                        await self._stop_device(int(did))
                    self._emit("auto_stop", {
                        "device_ids": ids,
                        "duration_ms": duration_ms
                    })

            except Exception as exc:
                self.log("Vibrate dispatch failed", level="ERROR", exc=exc)
                self._emit("send_error", {
                    "command": "vibrate",
                    "error": str(exc)
                })

        asyncio.run_coroutine_threadsafe(_do(), self.loop)


    def stop(self, device_ids: Union[int, str, List[Union[int, str]]] = "all") -> None:
        """
        Stop one or more Intiface devices.
        """
        if not self.loop:
            self.log("Cannot stop - loop not running", level="WARN")
            return

        ids = self._resolve_device_ids(device_ids)

        self._emit("send_start", {"command": "stop", "device_ids": ids})

        async def _do():
            try:
                if device_ids == "all":
                    await self._stop_all_devices()
                else:
                    for did in ids:
                        await self._stop_device(int(did))

                self._emit("send_dispatched", {
                    "command": "stop",
                    "device_ids": ids
                })

            except Exception as exc:
                self.log("Stop dispatch failed", level="ERROR", exc=exc)
                self._emit("send_error", {
                    "command": "stop",
                    "error": str(exc)
                })

        asyncio.run_coroutine_threadsafe(_do(), self.loop)


    def refresh_devices(self) -> None:
        """
        Force refresh device list from Intiface.
        """
        if not self.loop:
            return

        async def _do():
            try:
                await self._request_device_list()
                self._emit("device_list_requested", {})
            except Exception as exc:
                self.log("Refresh devices failed", level="ERROR", exc=exc)
                self._emit("device_list_error", {"error": str(exc)})

        asyncio.run_coroutine_threadsafe(_do(), self.loop)


    # ─────────────────────────────────────────────────────────────
    # Mode execution (software-driven behavior)
    # ─────────────────────────────────────────────────────────────

    async def execute_mode(
        self,
        *,
        mode: str,
        device_ids: list[int],
        intensity: float,
        duration: float,
        step: int | None = None,
    ):
        """
        Execute an Intiface mode.

        All modes are implemented as scalar modulation.
        """
        
        self._mark_busy(max(0.05, float(duration)))
        
        mode = (mode or "").lower()

        if mode == "vibrate":
            await self._mode_vibrate(device_ids, intensity, duration)

        elif mode == "pulse":
            await self._mode_pulse(device_ids, intensity, duration)

        elif mode == "burst":
            await self._mode_burst(device_ids, intensity, duration)

        elif mode == "oscillation":
            await self._mode_oscillation(device_ids, intensity, duration, step)

        elif mode == "randomized_wobble":
            await self._mode_randomized(device_ids, duration)
            
        elif mode == "constrict":
            await self._mode_constrict(device_ids, intensity, duration, step)

        elif mode == "stop":
            await self._stop_all_devices()
            
        else:
            raise ValueError(f"Unknown Intiface mode: {mode}")

    async def execute_pattern(self, *, device_ids: list[int], steps: list[dict]):
        """
        Execute a normalized Intiface pattern.

        Step schema:
          {
            "actuator": "vibrate" | "constrict",
            "intensity": 0.0–1.0,
            "duration": seconds,
            "delay": seconds,
            "osc_step": int
          }
        """
        total_time = 0.0   # ← ADD
        
        for step in steps:
            actuator = step.get("actuator", "vibrate")
            intensity = float(step.get("intensity", 1.0))
            duration = float(step.get("duration", 1.0))
            delay = float(step.get("delay", 0.0))
            osc_step = int(step.get("osc_step", 10))

            total_time += duration + delay   # ← ADD
            
            # ---- ACTUATION ----
            if actuator == "constrict":
                await self._mode_constrict(
                    device_ids=device_ids,
                    intensity=intensity,
                    duration=duration,
                    step=osc_step,
                )
            else:
                # vibrate default
                for dev in device_ids:
                    await self._scalar_vibrate(dev, intensity)

                if duration > 0:
                    await asyncio.sleep(duration)
                    for dev in device_ids:
                        await self._stop_device(dev)

            # ---- STEP DELAY ----
            if delay > 0:
                await asyncio.sleep(delay)
                
        # 🔒 IMPORTANT: mark busy AFTER pattern completes
        self._mark_busy(total_time)
        
    # ─────────────────────────────────────────────────────────────
    # Mode implementations
    # ─────────────────────────────────────────────────────────────
    def _mark_busy(self, seconds: float):
        try:
            now = time.time()
            self._busy_until = max(
                float(getattr(self, "_busy_until", 0) or 0),
                now + float(seconds),
            )
        except Exception:
            pass

    async def _mode_vibrate(self, device_ids, intensity, duration):
        for dev in device_ids:
            await self._scalar_vibrate(dev, intensity)

        if duration > 0:
            await asyncio.sleep(duration)
            for dev in device_ids:
                await self._stop_device(dev)

    async def _mode_pulse(self, device_ids, intensity, duration):
        pulse_on = 0.25
        pulse_off = 0.25

        end = asyncio.get_event_loop().time() + duration
        while asyncio.get_event_loop().time() < end:
            for dev in device_ids:
                await self._scalar_vibrate(dev, intensity)
            await asyncio.sleep(pulse_on)

            for dev in device_ids:
                await self._stop_device(dev)
            await asyncio.sleep(pulse_off)


    async def _mode_burst(self, device_ids, intensity, duration):
        """
        Burst mode:
        - Repeated short pulses
        - Respects duration
        - Works with vibrate & constrict
        """

        on_time  = 0.15
        off_time = 0.10

        end = asyncio.get_event_loop().time() + float(duration)

        while asyncio.get_event_loop().time() < end:
            # ON
            for dev in device_ids:
                await self._scalar_vibrate(dev, intensity)

            await asyncio.sleep(on_time)

            # OFF
            for dev in device_ids:
                await self._stop_device(dev)

            await asyncio.sleep(off_time)


    async def _mode_oscillation(self, device_ids, base, duration, step):
        """
        TRUE stepped oscillation.

        Rules:
          - step is absolute percent (25 = 0.25)
          - base is max cap
          - no drift
          - no interpolation
          - no decay
          - deterministic ladder

        Examples:
          base=0.5, step=25 → 0.25 ↔ 0.50
          base=1.0, step=25 → 0.25 → 0.50 → 0.75 → 1.00 → 0.75 → ...
        """

        import asyncio

        loop = asyncio.get_running_loop()

        # ─────────────────────────────
        # Normalize inputs
        # ─────────────────────────────
        base = float(base)
        base = max(0.0, min(1.0, base))

        step_pct = int(step or 10)
        step_pct = max(1, min(100, step_pct))
        step_val = step_pct / 100.0

        # ─────────────────────────────
        # Build exact step ladder
        # ─────────────────────────────
        ladder = []

        v = step_val
        while v < base:
            ladder.append(round(v, 4))
            v += step_val

        ladder.append(round(base, 4))

        # Mirror down (no duplicate peak)
        if len(ladder) > 1:
            ladder = ladder + ladder[-2:0:-1]

        # Absolute safety
        if not ladder:
            ladder = [base]

        # ─────────────────────────────
        # Execute
        # ─────────────────────────────
        tick = 0.25
        end = loop.time() + float(duration)
        i = 0

        while loop.time() < end:
            value = ladder[i % len(ladder)]

            for dev in device_ids:
                await self._scalar_vibrate(dev, value)

            i += 1
            await asyncio.sleep(tick)

        # Hard stop
        for dev in device_ids:
            await self._stop_device(dev)

    async def _mode_randomized(
        self,
        device_ids,
        duration,
        *,
        base: float = 0.5,
        min_val: float = 0.0,
        max_val: float = 1.0,
    ):
        """
        True randomized stimulation.

        Rules:
        - base controls probability weight (higher = stronger on average)
        - min/max define absolute bounds
        - each tick is independent
        - no smoothing
        """

        import random
        import asyncio

        base = max(0.0, min(1.0, float(base)))
        min_val = max(0.0, min(1.0, float(min_val)))
        max_val = max(min_val, min(1.0, float(max_val)))

        loop = asyncio.get_event_loop()
        end_time = loop.time() + float(duration)

        while loop.time() < end_time:
            # 🎲 Weighted randomness
            # Bias curve: higher base → stronger average pull upward
            r = random.random()
            strength = min_val + (max_val - min_val) * (r ** (1.0 - base))

            # Time jitter
            on_time  = random.uniform(0.10, 0.45)
            off_time = random.uniform(0.08, 0.35)

            # Activate
            for dev in device_ids:
                await self._scalar_vibrate(dev, strength)

            await asyncio.sleep(on_time)

            # Release
            for dev in device_ids:
                await self._stop_device(dev)

            await asyncio.sleep(off_time)

        # Safety stop
        for dev in device_ids:
            await self._stop_device(dev)


    async def _mode_constrict(
        self,
        device_ids: list[int],
        intensity: float,
        duration: float,
        step: int | None = None,
    ):
        """
        Air-pump constriction with step smoothing.
        """

        steps = max(1, int(step or 5))
        delay = max(0.05, duration / steps)

        # Ramp up
        for i in range(steps):
            value = (i + 1) / steps * intensity
            for dev in device_ids:
                await self._scalar_constrict(dev, value)
            await asyncio.sleep(delay)

        # Hold
        remain = max(0.0, duration - (steps * delay))
        if remain:
            await asyncio.sleep(remain)

        # Release
        for dev in device_ids:
            await self._scalar_constrict(dev, 0.0)


    # ─────────────────────────────────────────────────────────────
    # Ecosystem integration surface: provider/commandId/context
    # ─────────────────────────────────────────────────────────────

    def exec_hook(self, command_id: str, context: Dict[str, Any] | None = None) -> bool:
        """
        Designed to be called by your existing execute_external_hook() router.
        Example:
          provider="intiface", commandId="vibrate", context={...}

        Supported command_id:
          - "vibrate": {device|device_ids, strength, duration_ms}
          - "stop":    {device|device_ids}
          - "refresh": {}
        """
        context = context or {}
        cmd = (command_id or "").strip().lower()

        try:
            if cmd == "vibrate":
                device_ids = context.get("device_ids", context.get("device", "all"))
                strength = float(context.get("strength", context.get("value", 0.5)))
                duration_ms = int(context.get("duration_ms", context.get("duration", 0)))
                self.vibrate(device_ids=device_ids, strength=strength, duration_ms=duration_ms)
                return True

            if cmd == "stop":
                device_ids = context.get("device_ids", context.get("device", "all"))
                self.stop(device_ids=device_ids)
                return True

            if cmd == "refresh":
                self.refresh_devices()
                return True

            self.log("Unknown intiface command_id", level="WARN", data={"command_id": command_id, "context": context})
            return False

        except Exception as exc:
            self.log("exec_hook failed", level="ERROR", exc=exc, data={"command_id": command_id, "context": context})
            return False

    # ─────────────────────────────────────────────────────────────
    # Internal: resolve ids
    # ─────────────────────────────────────────────────────────────

    def _resolve_device_ids(self, device_ids) -> List[int]:
        if device_ids == "all":
            return sorted([int(k) for k in self.devices.keys()])

        if isinstance(device_ids, (int, float)):
            return [int(device_ids)]

        if isinstance(device_ids, str):
            s = device_ids.strip().lower()
            if s == "all":
                return sorted([int(k) for k in self.devices.keys()])
            # allow comma list "1,2,3"
            if "," in s:
                out = []
                for part in s.split(","):
                    part = part.strip()
                    if part.isdigit():
                        out.append(int(part))
                return out
            if s.isdigit():
                return [int(s)]
            return []

        # list/tuple
        out = []
        for x in device_ids:
            try:
                if isinstance(x, str) and x.strip().isdigit():
                    out.append(int(x.strip()))
                elif isinstance(x, (int, float)):
                    out.append(int(x))
            except Exception:
                continue
        return out

    # ─────────────────────────────────────────────────────────────
    # Main connection loop
    # ─────────────────────────────────────────────────────────────

    async def _connect_loop(self):
        import random
        import time
        import asyncio
        import websockets

        backoff = max(1.0, float(self._bo))
        last_warn = 0.0

        while True:
            try:
                self.log("Attempting Intiface WS connection", level="INFO", data={"uri": self.uri})

                async with websockets.connect(
                    self.uri,
                    subprotocols=["buttplug-json"],
                    ping_interval=None,   # buttplug handles its own; avoid interfering
                    max_size=None,
                ) as ws:
                    self.ws = ws
                    self.connected = True
                    backoff = 2.0
                    self._bo = backoff

                    self._emit("connected", {})
                    self.log("Intiface connected", level="INFO", action_type="connect")

                    if self.on_connected:
                        try:
                            self.on_connected()
                        except Exception as exc:
                            self.log("on_connected error", level="WARN", action_type="callback", exc=exc)

                    # handshake + initial discovery
                    await self._request_server_info()

                    # receive loop
                    await self._receive_loop()

            except (ConnectionRefusedError, OSError, asyncio.TimeoutError, websockets.exceptions.InvalidURI) as exc:
                # Expected when Intiface Central is not running / not listening
                now = time.time()
                if now - last_warn > 15:   # rate-limit noise
                    last_warn = now
                    self.log(
                        "Intiface not available (connection refused) - waiting for Intiface Central…",
                        level="WARN",
                        action_type="connect",
                        data={"error": str(exc)}
                    )

            except websockets.exceptions.InvalidStatusCode as exc:
                # Server responded but rejected us (rare, but useful to see)
                self.log(
                    "Intiface WS rejected connection (status code)",
                    level="WARN",
                    action_type="connect",
                    data={"status_code": getattr(exc, 'status_code', None)},
                    exc=exc
                )

            except Exception as exc:
                # Real unexpected failure - log full stack
                self.log(
                    "Intiface WS fatal error",
                    level="ERROR",
                    action_type="runtime",
                    exc=exc
                )

            finally:
                # cleanup
                try:
                    if self.connected:
                        self.connected = False
                        self.ws = None
                        self.server_info = None
                except Exception:
                    pass

                self._emit("disconnected", {})
                if self.on_disconnected:
                    try:
                        self.on_disconnected()
                    except Exception as exc:
                        self.log("on_disconnected error", level="WARN", action_type="callback", exc=exc)

                # backoff
                backoff = min(backoff * 2.0, 30.0)
                delay = backoff + random.uniform(0.0, 1.5)
                self._bo = backoff

                self.log("Reconnecting delay", level="DEBUG", action_type="connect", data={"seconds": round(delay, 2)})
                await asyncio.sleep(delay)

    async def _receive_loop(self):
        while self.connected and self.ws:
            raw = await self.ws.recv()
            await self._handle_frame(raw)

    async def _handle_frame(self, raw: str):
        try:
            payload = json.loads(raw)
        except Exception:
            self.log("WS frame not JSON", level="WARN", data={"raw": raw})
            return

        # Buttplug frames are arrays
        if not isinstance(payload, list):
            self.log("WS frame invalid (expected array)", level="WARN", data={"payload": payload})
            return

        for msg in payload:
            if not isinstance(msg, dict):
                continue
            await self._dispatch(msg)

    async def _dispatch(self, msg: Dict[str, Any]):
        # forward raw to callback if you want
        if self.on_message:
            try:
                self.on_message(msg)
            except Exception as exc:
                self.log("on_message error", level="WARN", exc=exc)

        # and handle core messages
        key = next(iter(msg.keys()), None)
        if not key:
            return
        data = msg.get(key) or {}

        if key == "ServerInfo":
            self.server_info = data
            self.log("Handshake complete", level="INFO", data=data)
            self._emit("server_info", data)
            await self._request_device_list()
            await self._start_scanning()
            return

        if key == "DeviceList":
            devices = data.get("Devices") or []
            self._handle_device_list(devices)
            return

        if key == "DeviceAdded":
            self._handle_device_added(data)
            return

        if key == "DeviceRemoved":
            self._handle_device_removed(data)
            return

        if key == "Error":
            # surface errors into q for UX + telemetry
            self.log("Buttplug error", level="ERROR", data=data)
            self._emit("error", data)
            return

        # ok/ack
        if key == "Ok":
            self._emit("ok", data)
            return

        # anything else: keep as debug
        self._emit("message", {"type": key, "data": data})

    def _handle_device_list(self, devices: List[Dict[str, Any]]):
        # Replace registry with latest list
        new_registry: Dict[int, Dict[str, Any]] = {}

        for dev in devices:
            idx = dev.get("DeviceIndex")
            name = dev.get("DeviceName") or "(unknown)"
            if idx is None:
                continue

            new_registry[int(idx)] = {
                "index": int(idx),
                "name": name,
                "features": dev.get("DeviceMessages") or {},
                "raw": dev,
            }

        self.devices = new_registry
        self.log("DeviceList updated", level="INFO", data={"count": len(self.devices)})
        self._emit("device_list", {"devices": list(self.devices.values())})

    def _handle_device_added(self, dev: Dict[str, Any]):
        idx = dev.get("DeviceIndex")
        name = dev.get("DeviceName") or "(unknown)"
        if idx is None:
            return

        self.devices[int(idx)] = {
            "index": int(idx),
            "name": name,
            "features": dev.get("DeviceMessages") or {},
            "raw": dev,
        }
        self.log("DeviceAdded", level="INFO", data={"index": idx, "name": name})
        self._emit("device_added", self.devices[int(idx)])

    def _handle_device_removed(self, dev: Dict[str, Any]):
        idx = dev.get("DeviceIndex")
        if idx is None:
            return

        removed = self.devices.pop(int(idx), None)
        self.log("DeviceRemoved", level="INFO", data={"index": idx, "removed": removed})
        self._emit("device_removed", {"index": int(idx), "removed": removed})

# ──────────────────────────────────────────────────────────────────────────────── Main App
class Dash(tk.Tk):
    _pending_reset: Dict[str, str] = {}
    
    # OSC Buffer and Avatar Capture System
    ROLLING_BUFFER_SECONDS = 3

    osc_message_buffer = deque()  # (timestamp, svc, ev, data)
    post_capture_buffer = []      # post-change messages
    _capture_after_task = None
    _capture_timeout_job = None

    _avatar_capturing = False
    _avatar_capture_count = 0
    _avatar_capture_start = 0.0
    avatar_live_params = {}
    avatar_id = None
    
    chain_queue = PriorityQueue()
    chain_lock = Lock()
    chain_running = False
    
    COLS = {
    0: 22,    # checkbox
    1: 360,   # address
    2: 70,    # default
    3: 70,    # value
    4: 70,    # timer
    5: 60,    # send
    }
    
    # ───────────────────────────────────────── util
    @staticmethod
    def _normalize(value: Any) -> Any:
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, (int, float)):
            return value
        s = str(value).strip().lower()
        if s in {"true", "on"}:
            return 1
        if s in {"false", "off"}:
            return 0
        if re.fullmatch(r"-?\d+\.\d+", s):
            return float(s)
        if s.lstrip("-").isdigit():
            return int(s)
        return value

    def add_context_menu(self, widget):
        menu = tk.Menu(widget, tearoff=0)
        menu.add_command(label="Cut", command=lambda: widget.event_generate("<<Cut>>"))
        menu.add_command(label="Copy", command=lambda: widget.event_generate("<<Copy>>"))
        menu.add_command(label="Paste", command=lambda: widget.event_generate("<<Paste>>"))
        menu.add_separator()
        menu.add_command(label="Select All", command=lambda: widget.event_generate("<<SelectAll>>"))

        def show_context_menu(event):
            menu.tk_popup(event.x_root, event.y_root)

        widget.bind("<Button-3>", show_context_menu)   # Right-click (Windows/Linux)
        widget.bind("<Button-2>", show_context_menu)   # Right-click (macOS)

    def _toggle_dock(self):
        if self.dock_window.winfo_viewable():
            self._slide_out()
        else:
            self._slide_in()

    def _slide_in(self):
        self.update_idletasks()
        self.dock_window.deiconify()
        self._dock_target_x = self.winfo_x() + self.winfo_width()
        self._dock_y = self.winfo_y()
        self._dock_h = self.winfo_height()
        self._dock_anim_x = self._dock_target_x + 200  # Start far right

        def animate():
            if self._dock_anim_x > self._dock_target_x:
                self._dock_anim_x -= 20
                self.dock_window.geometry(f"200x{self._dock_h}+{self._dock_anim_x}+{self._dock_y}")
                self.after(10, animate)
            else:
                self.dock_window.geometry(f"200x{self._dock_h}+{self._dock_target_x}+{self._dock_y}")

        animate()

    def _slide_out(self):
        self._dock_anim_x = self.winfo_x() + self.winfo_width()
        self._dock_y = self.winfo_y()
        self._dock_h = self.winfo_height()

        def animate():
            if self._dock_anim_x < self.winfo_x() + self.winfo_width() + 200:
                self._dock_anim_x += 20
                self.dock_window.geometry(f"200x{self._dock_h}+{self._dock_anim_x}+{self._dock_y}")
                self.after(10, animate)
            else:
                self.dock_window.withdraw()

        animate()

    def _load_window_geometry(self, key):
        try:
            return self.user_settings.get(f"window_geom_{key}")
        except Exception:
            return None

    def _save_window_geometry(self, key, geom):
        try:
            self.user_settings[f"window_geom_{key}"] = geom
            self._save_user_settings()
        except Exception:
            pass

    def filter_out_selected(self) -> None:
        """
        Filters out all currently selected controls.
        Adds each to noisy filter, hides UI rows, and persists config.
        """

        selected_addrs = [addr for addr, row in self.rows.items() if row["sel_var"].get()]

        if not selected_addrs:
            log_gui_action(
                "Filter out selected: no rows were selected.",
                data={"action": "filter_out_selected"},
                level="DEBUG"
            )
            return

        log_gui_action(
            "Filtering out selected controls.",
            data={"count": len(selected_addrs), "addresses": selected_addrs},
            level="INFO"
        )

        # ── Add all to noisy filter + hide UI rows ────────────────
        for addr in selected_addrs:
            try:
                _add_to_noisy_filter(addr, sanitize=True)

                row = self.rows.get(addr)
                if row:
                    for widget in row["widgets"].values():
                        try:
                            widget.grid_remove()
                        except Exception as e:
                            log_gui_action(
                                "Failed to hide widget during filter out.",
                                data={"addr": addr},
                                level="WARN",
                                exc=e
                            )

            except Exception as e:
                log_gui_action(
                    "Failed to filter out control.",
                    data={"addr": addr},
                    level="ERROR",
                    exc=e
                )

        # ── Persist config (remove filtered rows) ────────────────
        try:
            cfg = load_controls()

            base_selected = {addr.split("::#")[0] for addr in selected_addrs}

            cfg = [
                c for c in cfg
                if c["addr"] not in selected_addrs and c["addr"].split("::#")[0] not in base_selected
            ]

            save_controls(cfg)

            log_gui_action(
                "Updated control configuration after filter out.",
                data={"remaining_controls": len(cfg)},
                level="DEBUG"
            )

        except Exception as e:
            log_gui_action(
                "Failed to persist control configuration after filter out.",
                level="ERROR",
                exc=e
            )

        self._repack_table()

    def _filter_out_row(self, address: str) -> None:
        """
        Backwards-compatible single-row filter.
        Internally routes through batch filter logic.
        """
        row = self.rows.get(address)
        if not row:
            return

        # Temporarily mark as selected
        try:
            row["sel_var"].set(True)
            self.filter_out_selected()
        finally:
            # Clean up selection state
            row["sel_var"].set(False)

    # ───────────────────────────────────────── ctor
    def __init__(self):
        super().__init__()
        os.makedirs(SAVE_DIR, exist_ok=True)
        os.makedirs(AVATAR_STATE_DIR, exist_ok=True)
        apply_dark(self)
        self.title(f"{APP_NAME} v{APP_VERSION}")
        self.avatar_id = None
        self.avatar_live_params = {}
        self._gift_mapping = self._load_gift_mapping()
        self.queue_len_var = tk.StringVar(value="Queue: 0")
        self.queue_label = ttk.Label(self, textvariable=self.queue_len_var, anchor="w")
        self.queue_label.pack(fill="x", padx=8)
        self._update_queue_label()
        self.tikfinity_registered = set()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.rows = {}
        self.chains = []
        self.pending_gift_chains = []
        self._avatar_ready = False
        self.q = q 

        # tracks OSC addresses we just sent - shields self-echoes
        self._recent_sent: deque[tuple[float, str]] = deque()
        self._SELF_ECHO_WINDOW = 0.6      # seconds to ignore
        
        # tracks OSC Avatar State
        self._avatar_tx_active = False
        self._avatar_tx_dirty = False
        self._avatar_tx_major_dirty = False   # ← NEW: only this triggers snapshot
        self._snapshot_debounce_job = None
        
        # per-address reset versioning + timer handles
        self._osc_reset_gen = {}        # addr -> int
        self._osc_reset_timers = {}     # addr -> threading.Timer

        # Initialize pishock client with username and apikey from config
        self.pishock = PiShockClient(
            username=user_cfg.get("pishock_username", ""),
            apikey=user_cfg.get("pishock_apikey", ""),
            q=self.q
        )
        
        self.intiface = IntifaceCentralClient(
            host="127.0.0.1",
            port=12345,
            q=self.q,
            logger=log_intiface_action
        )
        self.intiface.start()
        
        # Setup a basic fallback log method
        self.log = lambda msg, *, level="INFO", exc=None: print(f"[{level}] {msg}", exc)
        
        # OwO Vest manager
        self.owo = OWOVestManager()

        # ---------------- Main asyncio loop ----------------
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
    
        # ───────── Windows Taskbar and App Icon ─────────
        try:
            if os.name == "nt":
                import ctypes
                user32   = ctypes.windll.user32
                kernel32 = ctypes.windll.kernel32

                WM_SETICON      = 0x0080
                IMAGE_ICON      = 1
                LR_LOADFROMFILE = 0x0010
                LR_DEFAULTSIZE  = 0x0040
                LR_SHARED       = 0x8000
                ICON_SMALL, ICON_BIG = 0, 1

                hwnd  = self.winfo_id()
                hinst = kernel32.GetModuleHandleW(None)

                ico_path = resolve_runtime_path("assets/logo.ico")
                log_gui_action("Attempting to load Windows ICO icon", data={"ico_path": str(ico_path)})

                if getattr(sys, "frozen", False):
                    hicon_big = ctypes.c_void_p(1)
                    hicon_small = hicon_big
                else:
                    hicon_big = user32.LoadImageW(None, ico_path, IMAGE_ICON, 32, 32,
                                                  LR_LOADFROMFILE | LR_DEFAULTSIZE)
                    hicon_small = user32.LoadImageW(None, ico_path, IMAGE_ICON, 16, 16,
                                                    LR_LOADFROMFILE | LR_DEFAULTSIZE)

                if hicon_big:
                    user32.SendMessageW(hwnd, WM_SETICON, ICON_BIG, hicon_big)
                if hicon_small:
                    user32.SendMessageW(hwnd, WM_SETICON, ICON_SMALL, hicon_small)

                try:
                    self.iconbitmap(ico_path)
                    log_gui_action("Windows taskbar icon successfully set", level="DEBUG")
                except Exception:
                    log_gui_action("Icon bitmap set failed, continuing", level="DEBUG")

            else:
                try:
                    self._icon_img = tk.PhotoImage(file=resolve_runtime_path("assets/logo.png"))
                    self.iconphoto(False, self._icon_img)
                    log_gui_action("Linux/macOS fallback icon loaded", level="DEBUG")
                except Exception:
                    log_gui_action("No icon available for non-Windows OS", level="WARN")
        except Exception as e:
            log_gui_action("Could not set window icon", level="WARN", exc=e)

        # ───────── geometry & layout ─────────
        self.geometry("1080x720")
        self.configure(padx=8, pady=8, bg=BG)
        log_gui_action("Main window geometry and padding configured", level="DEBUG")

        # ───────── status bar (THEMED)
        header = ttk.Frame(self, style="TFrame")
        header.pack(fill="x")

        for idx, svc in enumerate(("TikTok", "OSC", "TikFinity", "PiShock", "Intiface")):
            # Service name
            name_lbl = ttk.Label(
                header,
                text=f"{svc}:",
                style="StatusName.TLabel"
            )
            name_lbl.grid(row=0, column=idx * 3, sticky="w")
            ToolTip(name_lbl, f"Status indicator for {svc} connection.")

            # Service status (default = disconnected)
            status_lbl = ttk.Label(
                header,
                text="Disconnected",
                style="StatusDisconnected.TLabel"
            )
            status_lbl.grid(row=0, column=idx * 3 + 1, padx=6, sticky="w")
            ToolTip(status_lbl, f"Shows whether {svc} is connected or disconnected.")

            # Expose reference for runtime updates
            setattr(self, f"{svc.lower()}_lbl", status_lbl)

        log_gui_action(
            "Status header bar created (themed)",
            data={"services": ["TikTok", "OSC", "TikFinity", "PiShock", "Intiface"]}
        )

        self.top_status_frame = header

        # ───────── Top-right floating icon bar (absolute placement)
        try:
            from PIL import Image, ImageTk
            import webbrowser

            def load_icon(path, size=(32, 32)):
                resample_mode = getattr(Image, "LANCZOS", Image.BICUBIC)
                img = Image.open(resolve_runtime_path(path)).resize(size, resample=resample_mode)
                return ImageTk.PhotoImage(img)

            self.icon_bar_frame = tk.Frame(self, bg=BG)
            self.icon_bar_frame.place(relx=1.0, x=-12, y=6, anchor="ne")

            links = [
                ("assets/Tikfinity.png", "Tikfinity Website", "https://tikfinity.zerody.one/"),
                ("assets/intiface.png", "Intiface Website", "https://intiface.com/central/"),
                ("assets/Pishock.png", "Pishock Website", "https://www.pishock.com/#/"),
                ("assets/OWO.png", "OWO Game Website", "https://owogame.com/"),
            ]

            self._top_right_icons = []

            def make_tooltip(widget, text):
                def show(event):
                    tooltip = tk.Toplevel(self)
                    tooltip.overrideredirect(True)
                    tooltip.configure(bg="#222222")
                    label = tk.Label(tooltip, text=text, bg="#222222", fg="white", font=("Segoe UI", 9))
                    label.pack(ipadx=6, ipady=2)
                    x = widget.winfo_rootx() + 12
                    y = widget.winfo_rooty() + 48
                    tooltip.geometry(f"+{x}+{y}")
                    widget.tooltip_window = tooltip

                def hide(event):
                    if hasattr(widget, "tooltip_window"):
                        widget.tooltip_window.destroy()
                        del widget.tooltip_window

                widget.bind("<Enter>", show)
                widget.bind("<Leave>", hide)

            for icon_path, tooltip_text, url in links:
                try:
                    icon_img = load_icon(icon_path)
                    self._top_right_icons.append(icon_img)

                    label = tk.Label(self.icon_bar_frame, image=icon_img, bg=BG, cursor="hand2")
                    label.pack(side="left", padx=4)
                    label.bind("<Button-1>", lambda e, link=url: webbrowser.open_new(link))
                    make_tooltip(label, tooltip_text)
                    log_gui_action("Top-right icon loaded", level="DEBUG", data={"icon": icon_path, "url": url})
                except Exception as icon_err:
                    log_gui_action("Failed to load branding icon", level="WARN", data={"icon": icon_path}, exc=icon_err)

        except Exception as e:
            log_gui_action("Branding section failed", level="ERROR", exc=e)

        # ───────── Continue Gui Controls ─────────
        tb = tk.Frame(self, bg=PANEL)
        tb.pack(fill="x", pady=(6, 4))
        log_gui_action("Toolbar frame initialized")

        # Dropdown to switch toolbar mode
        mode_var = tk.StringVar(value="Support")  # default to Support Docs
        ttk.Combobox(
            tb,
            textvariable=mode_var,
            values=["OSC Controls", "Chain Controls", "Configuration", "Support", "PiShock Controls"],
            state="readonly",
            width=20
        ).pack(side="left", padx=4)
        log_gui_action("Toolbar mode dropdown created", data={"default": "Support"})

        # Dynamic buttons container
        button_frame = tk.Frame(tb, bg=PANEL)
        button_frame.pack(side="left", padx=6)

        def render_toolbar_buttons(*_):
            for widget in button_frame.winfo_children():
                widget.destroy()

            mode = mode_var.get()
            if mode == "OSC Controls":
                b1 = ttk.Button(button_frame, text="Add OSC Control", command=self.add_osc)
                b1.pack(side="left", padx=4, pady=4)
                ToolTip(b1, "Add a new OSC control to the interface.")

                b2 = ttk.Button(button_frame, text="Delete Selected", command=self.delete_selected)
                b2.pack(side="left", padx=4)
                ToolTip(b2, "Delete the currently selected control.")
                
                b3 = ttk.Button(button_frame, text="Filter Selected", command=self.filter_out_selected)
                b3.pack(side="left", padx=4)
                ToolTip(b3, "Filter the selected rows out.")

                b4 = ttk.Button(button_frame, text="Add INT Series", command=self.add_int_series)
                b4.pack(side="left", padx=4)
                ToolTip(b4, "Add a predefined series of integer controls.")
                
                b5 = ttk.Button(button_frame, text="Save Avatar Snapshot", command=self._schedule_snapshot_commit)
                b5.pack(side="left", padx=4)
                ToolTip(b5, "Manually save the current avatar state.")
                
                b6 = ttk.Button(button_frame, text="Save Layout", command=self.save_layout)
                b6.pack(side="left", padx=4)
                ToolTip(b6, "Save the current layout and control setup.")

            elif mode == "Chain Controls":
                b7 = ttk.Button(button_frame, text="Create Chain", command=self.create_chain)
                b7.pack(side="left", padx=4)
                ToolTip(b7, "Create a new control chain.")

                b8 = ttk.Button(button_frame, text="Export Chains", command=export_chains)
                b8.pack(side="left", padx=4)
                ToolTip(b8, "Export current control chains to a file.")

                b9 = ttk.Button(button_frame, text="Import Chains", command=lambda: import_chains_and_register(self))
                b9.pack(side="left", padx=4)
                ToolTip(b9, "Import control chains from a file.")

                #Clear Queue button
                b10 = ttk.Button(
                    button_frame,
                    text="Clear Queue",
                    command=self._clear_queue
                )
                b10.pack(side="left", padx=4)
                ToolTip(b10, "Empty all pending items from the event queue.")
                
            elif mode == "Configuration":

                b11 = ttk.Button(button_frame, text="Connect TikTok (manual fallback)", command=self.connect_tiktok)
                b11.pack(side="left", padx=4)
                ToolTip(b11, "Manually connect to TikTok in fallback mode.")

                b12 = ttk.Button(button_frame, text="Config", command=self.edit_config)
                b12.pack(side="left", padx=4)
                ToolTip(b12, "Open the advanced configuration editor.")

            elif mode == "Support":
                b13 = ttk.Button(button_frame, text="Support Docs", command=self._Open_SupportDocs)
                b13.pack(side="left", padx=4)
                ToolTip(b13, "View the latest changes and updates.")

            elif mode == "PiShock Controls":
                # ----------------- EMERGENCY STOP -----------------
                b15 = ttk.Button(
                    button_frame,
                    text="EMERGENCY STOP",
                    style="Danger.TButton",
                    command=lambda: threading.Thread(target=self.pishock.emergency_stop_all, daemon=True).start()
                )
                b15.pack(side="left", padx=4, pady=4)
                ToolTip(b15, "IMMEDIATE full stop for all PiShock devices (kills process).")

                # ----------------- CLAIM SHARED DEVICES -----------------
                def claim_shares_dialog():
                    share_input = DarkAskString(
                        self,
                        title="Claim Shared Devices",
                        prompt="Enter one or more share codes (comma or space separated):"
                    ).result
                    if not share_input:
                        return
                    codes = [c.strip() for c in share_input.replace(",", " ").split() if c.strip()]
                    if codes:
                        self.pishock.claim_share_ids(codes)

                b16 = ttk.Button(button_frame, text="Claim Shared Devices", command=claim_shares_dialog)
                b16.pack(side="left", padx=4, pady=4)
                ToolTip(b16, "Enter share codes to claim shared PiShock devices to your account.")

                # ----------------- DEVICE DROPDOWN -----------------
                selected_device_var = tk.StringVar(value="")
                device_dropdown = ttk.Combobox(button_frame, textvariable=selected_device_var, values=[], state="readonly", width=30)
                device_dropdown.pack(side="left", padx=4, pady=4)
                ToolTip(device_dropdown, "Select a PiShock device to test.")

                def refresh_device_dropdown():
                    device_names = [
                        f"[Shared] {d.get('name') or d.get('shockerId')} ({d.get('fromUser')})" if d.get("shareId")
                        else f"[Owned] {d.get('name') or d.get('shockerId')} ({d.get('fromUser')})"
                        for d in self.pishock.devices
                    ]
                    device_dropdown['values'] = device_names
                    if device_names:
                        selected_device_var.set(device_names[0])
                    else:
                        selected_device_var.set("")

                # ----------------- REFRESH DEVICES -----------------
                def refresh_devices():
                    def run_fetch():
                        if not hasattr(self, "loop"):
                            pishock_log_verbose("No asyncio loop found for device fetch", level="ERROR")
                            return

                        future = asyncio.run_coroutine_threadsafe(self.pishock._fetch_devices(), self.loop)
                        try:
                            future.result(timeout=15)  # wait for completion
                        except Exception as e:
                            pishock_log_verbose("Failed fetching devices", {"error": str(e)}, level="ERROR")
                        self.after(50, refresh_device_dropdown)

                    threading.Thread(target=run_fetch, daemon=True).start()

                b16 = ttk.Button(button_frame, text="Refresh Devices", command=refresh_devices)
                b16.pack(side="left", padx=4, pady=4)
                ToolTip(b16, "Re-fetch owned and shared PiShock devices from the cloud.")

                # ----------------- TEST DEVICE BUTTON -----------------
                def test_selected_device():
                    idx = device_dropdown.current()
                    if idx < 0:
                        pishock_log_verbose("No device selected for test.", level="WARN")
                        return
                    device = self.pishock.devices[idx]

                    def worker():
                        device_name = device.get('name') or device.get('shockerId')
                        pishock_log_verbose(f"Testing device {device_name}", level="INFO")

                        async def send_test():
                            await self.pishock._send_publish_commands(
                                device_ids=[str(device.get("shockerId"))],
                                mode="vibrate",
                                intensity=50,
                                duration=3000
                            )

                        asyncio.run(send_test())
                        # Optional: refresh dropdown in case device status changed
                        self.after(50, refresh_device_dropdown)

                    threading.Thread(target=worker, daemon=True).start()

                b_test = ttk.Button(button_frame, text="Test Device", command=test_selected_device)
                b_test.pack(side="left", padx=4, pady=4)
                ToolTip(b_test, "Send a short vibration pulse to the selected PiShock device.")

                # ----------------- INITIAL POPULATION -----------------
                refresh_device_dropdown()

            log_gui_action("Toolbar buttons rendered", data={"mode": mode})

        mode_var.trace_add("write", render_toolbar_buttons)
        render_toolbar_buttons()

        # Right-side Search
        self.right_tools = tk.Frame(tb, bg=PANEL)
        self.right_tools.pack(side="right", padx=6)
        log_gui_action("Search bar toolbar initialized")

        ttk.Label(self.right_tools, text="Search:", style="HeaderAddr.TLabel") \
            .pack(side="left", padx=(0, 4))

        self.search_var = tk.StringVar()
        ent = ttk.Entry(self.right_tools, textvariable=self.search_var, width=22)
        ent.pack(side="left")
        ent.bind("<KeyRelease>", self._apply_filter)
        ent.bind("<Control-f>", lambda e: ent.focus())
        log_gui_action("Search entry field created")

        # ───────────────────────── Paned layout
        paned = tk.PanedWindow(
            self,
            orient="vertical",
            bg=BG,
            bd=0,
            sashwidth=2,
            opaqueresize=True
        )
        paned.pack(fill="both", expand=True)
        log_gui_action("Main paned window layout created")

        table_outer = tk.Frame(paned, bg=BG)
        paned.add(table_outer, stretch="always")

        # ───────────────────────── Scrollable table body (THEMED + FAST)
        table_outer.grid_rowconfigure(1, weight=1)
        table_outer.grid_columnconfigure(0, weight=1)

        canvas, body = build_scrollable_root(
            table_outer,
            BG,
            use_grid=True,
            row=1,
            column=0
        )

        # This is the table root rows attach to
        self.table = body

        log_gui_action("Scrollable table body initialized (themed + scoped)")

        # ───────────────────────── Chain Status
        self.chain_status_var = tk.StringVar(value="No chain running.")
        self.chain_status_label = ttk.Label(self, textvariable=self.chain_status_var, anchor="w")
        self.chain_status_label.pack(fill="x", pady=4, padx=8)

        log_gui_action("Chain status label added", data={"initial_value": self.chain_status_var.get()})

        # ───────────────────────── Live Log Box
        # THEN create the self.log
        self.log = scrolledtext.ScrolledText(paned,
            bg="#000", fg="#88ffdd", insertbackground=FG,
            font=("Consolas",9), wrap="word", height=10)
        paned.add(self.log, stretch="always")
        log_gui_action("Log console initialized in GUI")

        # restore rows and chains
        self.osc = udp_client.SimpleUDPClient(OSC_IN_ADDR, OSC_OUT_PORT)
        self.last_osc = 0.0
        self.rows = {}
        self.chain_frames = []

        # Call this FIRST to ensure filters are loaded and active
        _load_noisy_params()

        loaded_controls = load_controls()
        for item in loaded_controls:
            self._create_row(
                item["addr"],
                default_args=item.get("default", [0]),
                initial_timer=item.get("timer", ""),
                persist=False
            )
            if "value" in item and item["addr"] in self.rows:
                self.rows[item["addr"]]["value_var"].set(str(item["value"][0]))

        log_gui_action("OSC rows restored from saved config", data={"count": len(loaded_controls)})

        chains = self._upgrade_chain_data(load_chains())
        for ch in chains:
            unregister_action(ch["name"], "chains")  # <- unregister only chain-category actions
            self.tikfinity_registered.discard(ch["name"])  # <- reset memory cache
            self._register_chain(ch)

        log_gui_action("Chains restored and registered", data={"count": len(chains)})

        self._after_pump_q_id = self.after(20, self._pump_q)
        self._after_heartbeat_id = self.after(1000, self._heartbeat)
        self.auto_load_last_avatar(self.load_avatar_from_path)
        log_gui_action("Queue pump + heartbeat scheduled; attempting auto-load of last avatar")

    def _on_close(self):
        try:
            if hasattr(self, "_after_pump_q_id"):
                self.after_cancel(self._after_pump_q_id)
            if hasattr(self, "_after_heartbeat_id"):
                self.after_cancel(self._after_heartbeat_id)
            log_gui_action("Graceful shutdown: after-callbacks cancelled")
        except Exception as e:
            log_gui_action("Error cancelling after jobs", level="WARN", exc=e)
        self.destroy()
        log_gui_action("Application shutdown triggered")

    def apply_theme_to_toplevel(self, window: tk.Toplevel) -> None:
        window.configure(bg=BG)

        try:
            if os.name == "nt":
                ico_path = resolve_runtime_path("assets/logo.ico")
                window.iconbitmap(ico_path)
            else:
                window.iconphoto(False, self._icon_img)
        except Exception as e:
            log_gui_action("Failed to apply icon to toplevel window", level="WARN", exc=e)

        style = ttk.Style(window)
        style.theme_use("clam")

        style.configure(".", background=BG, foreground=FG, fieldbackground=PANEL,
                        selectbackground="#444444", selectforeground=FG)
        style.configure("TEntry", background=PANEL, foreground=FG, fieldbackground=PANEL)
        style.configure("TCombobox", fieldbackground=PANEL, background=PANEL,
                        foreground=FG, selectbackground="#444444", selectforeground=FG)
        style.configure("TButton", background=PANEL, foreground=FG)
        style.configure("Vertical.TScrollbar", gripcount=0, background=PANEL,
                        troughcolor=BG, bordercolor=BG, lightcolor=BG, darkcolor=BG)

        log_gui_action("Theme applied to toplevel window", data={"os": os.name})

    def ask_string(self, title: str, prompt: str, initialvalue: str = "") -> str | None:
        result = DarkAskString(self, title, prompt, initialvalue).result
        log_gui_action("ask_string dialog used", data={"title": title, "initial": initialvalue})
        return result

    def ask_float(self, title: str, prompt: str, initialvalue: float = 0.0,
                  minvalue: float = None, maxvalue: float = None) -> float | None:
        result = DarkAskFloat(self, title, prompt, initialvalue, minvalue, maxvalue).result
        log_gui_action("ask_float dialog used", data={"title": title, "initial": initialvalue})
        return result

    def _handle_osc_event(self, ev: str, data: dict):
        if ev == "advertised":
            self._osc_advertised = True
            self.osc_lbl.config(
                text="Advertised",
                style="StatusConnected.TLabel"
            )

        elif ev == "vrchat_ready":
            self.osc_lbl.config(
                text="VRChat Ready",
                style="StatusConnected.TLabel"
            )

        elif ev == "error":
            self.osc_lbl.config(
                text="Error",
                style="StatusError.TLabel"
            )

        elif ev == "msg":
            # heartbeat handles visual state
            pass

    def _handle_intiface_event(self, ev: str, data: dict):
        try:
            if ev == "connected":
                self.intiface_lbl.config(
                    text="Connected",
                    style="StatusConnected.TLabel"
                )
                log_gui_action("Intiface connected", level="INFO")

            elif ev == "disconnected":
                self.intiface_lbl.config(
                    text="Disconnected",
                    style="StatusDisconnected.TLabel"
                )
                log_gui_action("Intiface disconnected", level="WARN")

            elif ev == "device_list":
                count = len(data.get("devices", []))
                self.intiface_lbl.config(
                    text=f"Devices: {count}",
                    style="StatusConnected.TLabel"
                )
                log_gui_action(
                    "Intiface device list updated",
                    data={"count": count},
                    level="DEBUG"
                )

            elif ev == "device_added":
                name = data.get("name", "unknown")
                log_gui_action(
                    "Intiface device added",
                    data={"name": name},
                    level="INFO"
                )

            elif ev == "device_removed":
                idx = data.get("index")
                log_gui_action(
                    "Intiface device removed",
                    data={"index": idx},
                    level="INFO"
                )

            elif ev == "error":
                self.intiface_lbl.config(
                    text="Error",
                    style="StatusError.TLabel"
                )
                log_gui_action(
                    "Intiface error",
                    data=data,
                    level="ERROR"
                )

            # Optional noise-level events (no UI change)
            elif ev in ("send_start", "send_dispatched", "auto_stop", "ok"):
                log_gui_action(
                    "Intiface event",
                    data={"event": ev, "data": data},
                    level="DEBUG"
                )

            else:
                log_gui_action(
                    "Unhandled Intiface event",
                    data={"event": ev, "data": data},
                    level="DEBUG"
                )

        except Exception as exc:
            log_gui_action(
                "Failed handling Intiface event",
                level="ERROR",
                exc=exc
            )

    def _handle_tikfinity_event(self, ev: str, data: dict) -> None:
        try:
            # ─────────────────────────────────────────────
            # State cache (prevents UI spam)
            # ─────────────────────────────────────────────
            if not hasattr(self, "_tikfinity_ui_state"):
                self._tikfinity_ui_state = {
                    "last": None,
                    "last_ts": 0.0,
                }

            now = time.time()

            # Debounce duplicate events
            if (
                ev == self._tikfinity_ui_state["last"]
                and (now - self._tikfinity_ui_state["last_ts"]) < 0.75
            ):
                return

            self._tikfinity_ui_state["last"] = ev
            self._tikfinity_ui_state["last_ts"] = now

            # ─────────────────────────────────────────────
            # UI-safe update wrapper
            # ─────────────────────────────────────────────
            def _apply():
                try:
                    if ev == "connected":
                        self.tikfinity_lbl.config(
                            text="Connected",
                            style="StatusConnected.TLabel"
                        )

                    elif ev == "heartbeat":
                        # Soft signal — only update if not already connected
                        if self.tikfinity_lbl.cget("text") != "Connected":
                            self.tikfinity_lbl.config(
                                text="Active",
                                style="StatusConnected.TLabel"
                            )

                    elif ev == "disconnected":
                        self.tikfinity_lbl.config(
                            text="Disconnected",
                            style="StatusDisconnected.TLabel"
                        )

                    elif ev == "error":
                        self.tikfinity_lbl.config(
                            text="Error",
                            style="StatusError.TLabel"
                        )

                    else:
                        # Ignore noisy/internal events
                        log_gui_action(
                            "Unhandled TikFinity event",
                            level="DEBUG",
                            data={"event": ev}
                        )

                except Exception as exc:
                    log_gui_action(
                        "UI update failed (TikFinity)",
                        level="ERROR",
                        exc=exc
                    )

            # ─────────────────────────────────────────────
            # Always execute on UI thread
            # ─────────────────────────────────────────────
            try:
                self.root.after(0, _apply)
            except Exception:
                # Fallback (during shutdown)
                _apply()

        except Exception as exc:
            log_gui_action(
                "Failed handling TikFinity event",
                level="ERROR",
                exc=exc
            )

    def _handle_tiktok_event(self, ev: str, data: dict) -> None:
        try:
            if ev == "connected":
                self.tiktok_lbl.config(
                    text="Connected",
                    style="StatusConnected.TLabel"
                )

            elif ev.lower() == "connected":
                # Gift / subscribe activity pulse
                self.tiktok_lbl.config(
                    text="Active",
                    style="StatusConnected.TLabel"
                )

            elif ev == "disconnected":
                self.tiktok_lbl.config(
                    text="Disconnected",
                    style="StatusDisconnected.TLabel"
                )

            elif ev == "error":
                self.tiktok_lbl.config(
                    text="Error",
                    style="StatusError.TLabel"
                )

            else:
                log_gui_action(
                    "Unhandled TikTok event",
                    level="DEBUG",
                    data={"event": ev, "data": data}
                )

        except Exception as exc:
            log_gui_action(
                "Failed handling TikTok event",
                level="ERROR",
                exc=exc
            )

    def _handle_pishock_event(self, ev: str, data: dict) -> None:
        try:
            # ───────────────────── Connection lifecycle
            if ev == "connected":
                self.pishock_lbl.config(
                    text="Connected",
                    style="StatusConnected.TLabel"
                )

            elif ev == "disconnected":
                self.pishock_lbl.config(
                    text="Disconnected",
                    style="StatusDisconnected.TLabel"
                )

            elif ev == "session_subscribe":
                self.pishock_lbl.config(
                    text="Ready",
                    style="StatusConnected.TLabel"
                )

            # ───────────────────── REAL device operation ONLY
            elif ev == "chunk_sent":
                # Device is actively operating
                self.pishock_lbl.config(
                    text="Sending…",
                    style="StatusWaiting.TLabel"
                )

            elif ev == "chunk_ack":
                # Operation window has completed
                self.pishock_lbl.config(
                    text="Ready",
                    style="StatusConnected.TLabel"
                )

            # ───────────────────── Errors
            elif ev in (
                "error",
                "chunk_error",
                "simple_send_error",
                "ws_send_error",
                "v3_chunk_error",
            ):
                self.pishock_lbl.config(
                    text="Error",
                    style="StatusError.TLabel"
                )

            # ───────────────────── Explicitly ignored (intent / transport)
            elif ev in (
                "simple_send_start",
                "simple_send_done",
                "ws_send_start",
                "ws_send_dispatched",
                "send_start",
                "send_done",
            ):
                return  # intentionally ignored

            # ───────────────────── Silent ignore
            else:
                return

        except Exception as exc:
            log_gui_action(
                "Failed handling PiShock event",
                level="ERROR",
                exc=exc
            )

# ------------------------------------------------------ edit_config
    def edit_config(self) -> None:
        import tkinter as tk
        from tkinter import ttk, messagebox
        import os

        log_gui_action("Opening configuration editor window")

        cfg_win = tk.Toplevel(self)
        cfg_win.title("Edit Configuration")
        cfg_win.geometry("410x240")
        self.apply_theme_to_toplevel(cfg_win)

        current_cfg = load_user_cfg()

        tiktok_val = current_cfg.get("tiktok_username", "")
        pishock_user_val = current_cfg.get("pishock_username", "")
        pishock_key_val = current_cfg.get("pishock_apikey", "")

        # ─────────────────────────────────────────────
        # UI
        # ─────────────────────────────────────────────

        ttk.Label(cfg_win, text="TikTok Username").grid(row=0, column=0, padx=8, pady=6, sticky="e")
        tiktok_entry = ttk.Entry(cfg_win, width=30)
        tiktok_entry.insert(0, tiktok_val)
        tiktok_entry.grid(row=0, column=1, padx=8, pady=6)
        self.add_context_menu(tiktok_entry)

        ttk.Label(cfg_win, text="PiShock Username").grid(row=1, column=0, padx=8, pady=6, sticky="e")
        pishock_user_entry = ttk.Entry(cfg_win, width=30)
        pishock_user_entry.insert(0, pishock_user_val)
        pishock_user_entry.grid(row=1, column=1, padx=8, pady=6)
        self.add_context_menu(pishock_user_entry)

        ttk.Label(cfg_win, text="PiShock API Key").grid(row=2, column=0, padx=8, pady=6, sticky="e")
        pishock_key_entry = ttk.Entry(cfg_win, width=30, show="*")
        pishock_key_entry.insert(0, pishock_key_val)
        pishock_key_entry.grid(row=2, column=1, padx=8, pady=6)
        self.add_context_menu(pishock_key_entry)

        # ─────────────────────────────────────────────
        # SAVE HANDLER
        # ─────────────────────────────────────────────

        def save_changes():
            updated = False
            changes = {}

            new_tiktok = tiktok_entry.get().strip()
            new_user = pishock_user_entry.get().strip()
            new_key = pishock_key_entry.get().strip()

            if new_tiktok != current_cfg.get("tiktok_username", ""):
                current_cfg["tiktok_username"] = new_tiktok
                changes["tiktok_username"] = new_tiktok
                updated = True

            if new_user != current_cfg.get("pishock_username", ""):
                current_cfg["pishock_username"] = new_user
                changes["pishock_username"] = new_user
                updated = True

            if new_key != current_cfg.get("pishock_apikey", ""):
                current_cfg["pishock_apikey"] = new_key
                changes["pishock_apikey"] = "***"
                updated = True

            if not updated:
                log_gui_action("Save config aborted — no changes detected", level="DEBUG")
                messagebox.showinfo("No Changes", "Nothing to save.")
                return

            try:
                save_user_cfg(current_cfg)
                log_gui_action("User configuration saved", level="INFO", data=changes)
            except Exception as e:
                log_gui_action("Failed to save user config", level="ERROR", exc=e)
                messagebox.showerror("Error", f"Failed to save configuration:\n{e}")
                return

            # Restart PiShock client if needed
            try:
                if hasattr(self, "pishock") and self.pishock.is_running():
                    log_gui_action("PiShock already running, skipping restart")
                else:
                    self.pishock = PiShockClient(
                        username=current_cfg.get("pishock_username"),
                        apikey=current_cfg.get("pishock_apikey"),
                        q=self.q
                    )
                    self.pishock.start()
                    log_gui_action("PiShock client started")
            except Exception as e:
                log_gui_action("Failed to start PiShock client", level="ERROR", exc=e)

            messagebox.showinfo("Saved", "Configuration updated successfully.")
            cfg_win.destroy()

        ttk.Button(
            cfg_win,
            text="Save",
            command=save_changes
        ).grid(row=4, column=1, pady=12, sticky="e")

# ------------------------------------------------------ search filter
    def _apply_filter(self, _ev=None) -> None:
        import difflib

        term = self.search_var.get().lower().strip()
        log_gui_action("Search input changed", data={"term": term})

        # Fast path: empty search → show everything
        if not term:
            for row in self.rows.values():
                row["widgets"]["row"].grid()
            self.table.update_idletasks()
            log_gui_action("Cleared search filter - all rows shown", level="DEBUG")
            return

        # Precompute fuzzy matches once
        all_addrs = list(self.rows.keys())
        close_matches = set(
            difflib.get_close_matches(term, all_addrs, n=50, cutoff=0.3)
        )

        visible_count = 0

        for addr, row in self.rows.items():
            addr_l = addr.lower()

            label_text = ""
            try:
                label_text = row["widgets"]["label"].cget("text").lower()
            except Exception:
                pass

            score = difflib.SequenceMatcher(None, term, addr_l).ratio()

            visible = (
                term in addr_l
                or term in label_text
                or addr in close_matches
                or score > 0.35
            )

            if visible:
                row["widgets"]["row"].grid()
                visible_count += 1
            else:
                row["widgets"]["row"].grid_remove()

        self.table.update_idletasks()

        log_gui_action(
            "Applied search filter",
            level="DEBUG",
            data={"visible_rows": visible_count}
        )

# ------------------------------------------------------ Changelog Button   
    def _Open_SupportDocs(self):
        import webbrowser

        DOCS_URL = "http://127.0.0.1:8899/"

        try:
            webbrowser.open(DOCS_URL, new=2)
            log_gui_action(
                "Opened documentation browser",
                level="INFO",
                data={"url": DOCS_URL}
            )
        except Exception as e:
            log_gui_action(
                "Failed to open docs browser",
                level="ERROR",
                exc=e
            )

# ------------------------------------------------------ Gift Loader 
    def _load_gift_mapping(self, path: str = "saved/giftMapping.json") -> Dict[str, Dict[str, Any]]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            gift_map = {}

            if isinstance(data, dict):  # Original format
                for gift_name, info in data.items():
                    if isinstance(info, dict):
                        gift_map[gift_name] = {
                            "id": str(info.get("id", "")),
                            "coinCost": info.get("coinCost", 0),
                            "image": info.get("image", "")
                        }

            elif isinstance(data, list):  # Merged or new list format
                for entry in data:
                    if isinstance(entry, dict) and "name" in entry:
                        gift_map[entry["name"]] = {
                            "id": str(entry.get("id", "")),
                            "coinCost": entry.get("coinCost") or entry.get("coins", 0),
                            "image": entry.get("image", "")
                        }

            log_gui_action(
                "Loaded gift mapping",
                level="INFO",
                data={"gift_count": len(gift_map), "path": path}
            )
            return gift_map

        except Exception as e:
            log_gui_action(
                "Failed to load gift mapping",
                level="ERROR",
                data={"path": path},
                exc=e
            )
            return {}
            
    # ------------------------------------------------------ Avatar Transaction Helper
    def _mark_minor_change(self, reason: str = "") -> None:
        self._avatar_tx_dirty = True
        avatar_log_verbose(
            "Minor avatar state change",
            data={"reason": reason},
            level="DEBUG"
        )

    def _mark_major_change(self, reason: str = "") -> None:
        self._avatar_tx_dirty = True
        self._avatar_tx_major_dirty = True
        avatar_log_verbose(
            "Major avatar state change",
            data={"reason": reason},
            level="INFO"
        )
            
# ────────────────────────────────────────────────────────────────
# Row Builder Ui With Control Logic
# ────────────────────────────────────────────────────────────────
    def _create_row(
        self,
        address: str,
        default_args: list,
        initial_timer: str = "",
        *,
        category_id: str = "avatar_parameters",
        category_name: str = "Avatar Parameters",
        action_id: str | None = None,
        action_name: str | None = None,
        persist: bool = True,
        is_integer: bool = False,
        int_min: int = 0,
        int_max: int = 100,
        initial_value: int | None = None,
    ) -> None:

        # ─────────────────────────────────────────
        # Guards
        # ─────────────────────────────────────────
        if address in self.rows and not address.startswith(AVATAR_PREFIX):
            return

        if should_sanitize(address):
            log_gui_action("Sanitized row skipped", data={"address": address}, level="DEBUG")
            return

        base_addr = address.split("::#")[0]

        # ─────────────────────────────────────────
        # Action identity
        # ─────────────────────────────────────────
        action_id = action_id or base_addr
        action_name = action_name or base_addr.split("/")[-1]

        # ─────────────────────────────────────────
        # LOAD SAVED CONTROLS (CORRECTLY)
        # ─────────────────────────────────────────
        saved_default = None
        saved_value = None
        saved_timer = None

        try:
            cfg = load_controls()
            if isinstance(cfg, dict):
                cfg = cfg.get("controls", [])

            for entry in cfg:
                if entry.get("addr") == address:
                    # IMPORTANT: keep default and value separate
                    sd = entry.get("default", [None])
                    sv = entry.get("value", [None])

                    saved_default = sd[0] if isinstance(sd, list) and sd else None
                    saved_value   = sv[0] if isinstance(sv, list) and sv else None
                    saved_timer   = entry.get("timer")
                    break

        except Exception as e:
            log_gui_action("Failed loading saved controls", level="WARN", exc=e)

        # ─────────────────────────────────────────
        # LIVE OSC (FALLBACK ONLY)
        # ─────────────────────────────────────────
        live_val = None
        try:
            if hasattr(self, "avatar_live_params") and isinstance(self.avatar_live_params, dict):
                if base_addr in self.avatar_live_params:
                    live_val = self._normalize(self.avatar_live_params.get(base_addr))
        except Exception:
            pass

        # ─────────────────────────────────────────
        # FINAL RESOLUTION (DO NOT CHANGE ORDER)
        # ─────────────────────────────────────────
        # DEFAULT: Saved JSON default → Live OSC → Initial → Avatar JSON default
        resolved_default = (
            self._normalize(saved_default)
            if saved_default is not None
            else self._normalize(live_val)
            if live_val is not None
            else self._normalize(initial_value)
            if initial_value is not None
            else self._normalize(default_args[0] if default_args else 0)
        )

        # VALUE: Saved JSON value → Live OSC → Initial → resolved_default
        resolved_value = (
            self._normalize(saved_value)
            if saved_value is not None
            else self._normalize(live_val)
            if live_val is not None
            else self._normalize(initial_value)
            if initial_value is not None
            else resolved_default
        )

        # ─────────────────────────────────────────
        # UI BUILD
        # ─────────────────────────────────────────
        row_index = len(self.rows) + 1

        sel_var = tk.BooleanVar()
        default_var = tk.StringVar(value=str(resolved_default))
        value_var   = tk.StringVar(value=str(resolved_value))

        # IMPORTANT: do NOT treat "0" as falsy-missing
        timer_seed = saved_timer if saved_timer is not None else initial_timer
        timer_var  = tk.StringVar(value=str(timer_seed))

        row_outer = tk.Frame(self.table, bg=ROW_BG_IDLE)
        row_outer.grid(row=row_index, column=0, columnspan=6, sticky="ew", pady=1)

        rail = tk.Frame(row_outer, bg=ROW_BG_IDLE, width=3)
        rail.pack(side="left", fill="y")

        row_frame = tk.Frame(row_outer, bg=ROW_BG_IDLE)
        row_frame.pack(side="left", fill="x", expand=True)

        for c, w in self.COLS.items():
            row_frame.grid_columnconfigure(c, minsize=w)
        row_frame.grid_columnconfigure(1, weight=1)

        # ───── Widgets
        sel_cb = ttk.Checkbutton(row_frame, variable=sel_var)
        sel_cb.grid(row=0, column=0, padx=4)

        lbl = tk.Label(row_frame, text=address, bg=ROW_BG_IDLE, fg=FG, anchor="w")
        lbl.grid(row=0, column=1, sticky="ew", padx=(6, 4))

        ent_def = ttk.Entry(row_frame, textvariable=default_var, width=8)
        ent_def.grid(row=0, column=2, padx=4)

        if is_integer:
            ent_val = ttk.Spinbox(
                row_frame,
                from_=int_min,
                to=int_max,
                textvariable=value_var,
                width=6
            )
            send_addr = base_addr
        else:
            ent_val = ttk.Entry(row_frame, textvariable=value_var, width=8)
            send_addr = address

        ent_val.grid(row=0, column=3, padx=4)

        ent_tim = ttk.Entry(row_frame, textvariable=timer_var, width=8)
        ent_tim.grid(row=0, column=4, padx=4)

        send_btn = GlowButton(
            row_frame,
            text="SEND",
            command=lambda a=send_addr, v=value_var, t=timer_var:
                EXEC.submit(self.send_osc, a, self._normalize(v.get()), t.get()),
            width=84,
            height=30,
            glow=ROW_GLOW_COLORS[1],
            bg=ROW_BG_IDLE,
            fg=FG,
            font=("Segoe UI", 10, "bold"),
        )
        send_btn.grid(row=0, column=5, padx=6)

        # ─────────────────────────────────────────
        # STORE ROW
        # ─────────────────────────────────────────
        self.rows[address] = {
            "sel_var": sel_var,
            "default_var": default_var,
            "value_var": value_var,
            "timer_var": timer_var,
            "send_addr": send_addr,

            # AUTHORITATIVE VALUES (separate from UI strings)
            "default": resolved_default,
            "last_default": resolved_default,

            "address": address,
            "category_id": category_id,
            "category_name": category_name,
            "action_id": action_id,
            "action_name": action_name,

            "widgets": {
                "row": row_outer,
                "label": lbl,
                "value": ent_val,
                "timer": ent_tim,
                "send": send_btn,
            },
        }

        # ─────────────────────────────────────────
        # KEEP ROW DEFAULT IN SYNC WITH UI EDITS
        # ─────────────────────────────────────────
        def _sync_default_from_ui(*_):
            try:
                self.rows[address]["default"] = self._normalize(default_var.get())
            except Exception:
                pass

        try:
            default_var.trace_add("write", _sync_default_from_ui)
        except Exception:
            default_var.trace("w", _sync_default_from_ui)

        # ─────────────────────────────────────────
        # Persist (ONLY if requested)
        # ─────────────────────────────────────────
        if persist:
            try:
                cfg = load_controls()
                if isinstance(cfg, dict):
                    cfg = cfg.get("controls", [])

                found = False
                for entry in cfg:
                    if entry.get("addr") == address:
                        # Preserve existing default unless it's truly missing
                        if entry.get("default", None) is None or entry.get("default", [None])[0] is None:
                            entry["default"] = [self.rows[address]["default"]]

                        # Always update value + timer from UI
                        entry["value"] = [self._normalize(value_var.get())]
                        entry["timer"] = timer_var.get()
                        found = True
                        break

                if not found:
                    cfg.append({
                        "addr": address,
                        "default": [self.rows[address]["default"]],
                        "value": [self._normalize(value_var.get())],
                        "timer": timer_var.get(),
                    })

                save_controls(cfg)

            except Exception as e:
                log_gui_action("Failed persisting control state", level="ERROR", exc=e)

# ────────────────────────────────────────────────────────────────
# Queue Ui System
# ────────────────────────────────────────────────────────────────
    def _update_queue_label(self):
        # get the main chain queue
        q = getattr(self, "chain_queue", None)
        size = q.qsize() if q else 0

        # update the label
        self.queue_len_var.set(f"Queue: {size}")

        # schedule the next update
        self.after(500, self._update_queue_label)

# ────────────────────────────────────────────────────────────────
# Snapshot Ui Paramaters
# ────────────────────────────────────────────────────────────────
    def _reset_avatar_parameters(self):
        import os, json

        # Cancel all pending chain resets so snapshot restore is authoritative
        try:
            for a, t in list(getattr(self, "_osc_reset_timers", {}).items()):
                try:
                    t.cancel()
                except Exception:
                    pass
            self._osc_reset_timers = {}
            self._osc_reset_gen = {}
        except Exception:
            pass

        if not self.avatar_id:
            avatar_log_verbose("No avatar ID set for reset", level="WARN")
            return

        snapshot_path = os.path.join(AVATAR_STATE_DIR, f"{self.avatar_id}.json")

        if not os.path.exists(snapshot_path):
            avatar_log_verbose(
                "No snapshot found for avatar reset",
                data={"path": snapshot_path},
                level="WARN"
            )
            return

        try:
            with open(snapshot_path, "r", encoding="utf-8") as f:
                snapshot = json.load(f)
                param_dict = snapshot.get("parameters", {})
        except Exception as e:
            avatar_log_verbose(
                "Failed to read avatar snapshot",
                data={"path": snapshot_path},
                level="ERROR",
                exc=e
            )
            return

        if not isinstance(param_dict, dict) or not param_dict:
            avatar_log_verbose(
                "Avatar snapshot has no parameters to reset",
                data={"path": snapshot_path},
                level="WARN"
            )
            return

        avatar_log_verbose(
            "Resetting avatar parameters from snapshot",
            data={"avatar_id": self.avatar_id, "count": len(param_dict)},
            level="INFO"
        )

        for addr, raw_val in param_dict.items():
            try:
                addr = str(addr).strip()
                if not addr:
                    continue

                # Normalize value (prevents bool/int/float drift)
                try:
                    val = self._normalize(raw_val)
                except Exception:
                    val = raw_val

                # Record outgoing for echo suppression / tracking
                try:
                    self._record_outgoing_osc(addr)
                except Exception:
                    pass

                sent = False

                # Prefer bridge
                if hasattr(self, "osc_bridge") and self.osc_bridge:
                    try:
                        sent = self.osc_bridge.send(addr, val)
                    except Exception as e:
                        avatar_log_verbose(
                            "OSC bridge send failed during avatar reset",
                            data={"addr": addr, "value": val},
                            level="WARN",
                            exc=e
                        )

                # Fallback client
                if not sent:
                    if not hasattr(self, "_osc_fallback_client"):
                        from pythonosc.udp_client import SimpleUDPClient
                        self._osc_fallback_client = SimpleUDPClient("127.0.0.1", 9000)
                    self._osc_fallback_client.send_message(addr, val)

                avatar_log_verbose(
                    "Reset avatar parameter",
                    data={"addr": addr, "value": val},
                    level="DEBUG"
                )

            except Exception as e:
                avatar_log_verbose(
                    "Failed to reset parameter via OSC",
                    data={"addr": addr, "value": raw_val},
                    level="ERROR",
                    exc=e
                )

# ────────────────────────────────────────────────────────────────
# Regrid System
# ────────────────────────────────────────────────────────────────
    def _regrid_rows(self) -> None:
        """
        Compact and reassign grid rows for all OSC rows.
        """
        for i, row in enumerate(self.rows.values(), start=1):
            fr = row["widgets"]["row"]
            fr.grid_forget()
            fr.grid(row=i, column=0, columnspan=6, sticky="ew", pady=1)

    def _regrid_chains(self) -> None:
        # Clear existing chain layout
        for fr in self.chain_frames:
            fr.grid_forget()

        # ---- FIX: compute base row from actual grid, not self.rows ----
        # Count how many row widgets actually exist
        row_widgets = [
            r["widgets"]["row"]
            for r in self.rows.values()
            if r.get("widgets", {}).get("row")
        ]

        base_row = len(row_widgets)

        # Spacer (always directly after rows)
        if not hasattr(self, "_chain_spacer") or not self._chain_spacer.winfo_exists():
            self._chain_spacer = tk.Frame(self.table, height=12, bg=self.table["bg"])

        self._chain_spacer.grid(
            row=base_row,
            column=0,
            columnspan=2,
            sticky="nsew"
        )

        base_row += 1

        # Sort chains deterministically
        self.chain_frames = sorted(
            self.chain_frames,
            key=lambda fr: fr._chain_meta.get("layout_index", 0)
        )

        # Regrid chains
        for idx, fr in enumerate(self.chain_frames):
            row_idx = base_row + (idx // 2)
            col_idx = idx % 2
            fr.grid(row=row_idx, column=col_idx, sticky="ew", padx=6, pady=4)

        # Ensure columns expand
        for col in range(2):
            self.table.grid_columnconfigure(col, weight=1)

        log_gui_action(
            "Chain frames regridded",
            data={"frame_count": len(self.chain_frames)},
            level="DEBUG"
        )

    def _clear_all_rows(self) -> None:
        """
        Hard clear of all rows without triggering repack storms.
        Safe for bulk teardown during avatar reload.
        """

        # Destroy row widgets
        for addr, row in list(self.rows.items()):
            for widget in row.get("widgets", {}).values():
                try:
                    widget.destroy()
                except Exception as e:
                    log_gui_action(
                        "Failed to destroy widget during bulk clear",
                        data={"addr": addr},
                        level="WARN",
                        exc=e
                    )

        # Clear row registry
        self.rows.clear()

        # IMPORTANT: Reset grid geometry
        for child in self.table.grid_slaves():
            child.grid_forget()

        # Re-pack table so next insert starts clean
        self._repack_table()


# ────────────────────────────────────────────────────────────────
# Connect to TikTok Manual
# ────────────────────────────────────────────────────────────────
    def connect_tiktok(self) -> None:
        username = user_cfg.get("tiktok_username", "").strip()
        if not username:
            messagebox.showerror("Error", "TikTok username not configured.\nCheck Config first.")
            log_gui_action(
                "Manual TikTok connection aborted - username not set",
                level="WARN"
            )
            return

        proceed = messagebox.askyesno(
            "Manual Connect",
            "This fallback is only needed if TikFinity is DOWN.\n\nConnect manually anyway?"
        )

        if not proceed:
            log_gui_action("Manual TikTok connect canceled by user", level="INFO")
            return

        log_gui_action(
            "Manual TikTok connect initiated",
            data={"username": username},
            level="INFO"
        )

        try:
            threading.Thread(
                target=tiktok_thread,
                args=(username,),
                daemon=True
            ).start()
        except Exception as e:
            log_gui_action(
                "Failed to start TikTok connection thread",
                data={"username": username},
                level="ERROR",
                exc=e
            )
            messagebox.showerror("Connection Error", f"Manual TikTok connection failed:\n\n{str(e)}")

    # ------------------------------------------------------ manual add using new system
    def add_osc(self) -> None:
        address = self.ask_string("OSC Address", "/avatar/parameters/Param")
        if not address:
            return
        default_val = self.ask_string("Default Value", "0") or "0"

        self._create_row(
            address,
            [self._normalize(default_val)],
        )

        self._mark_minor_change("manual add_osc")
        
    # ------------------------------------------------------ Avatar Loader
    def load_avatar_from_path(self, json_path: str) -> None:
        if not os.path.exists(json_path):
            avatar_log_verbose("Avatar path does not exist", level="WARN", data={"path": json_path})
            log_gui_action("Avatar path missing", level="WARN", data={"path": json_path})
            return

        # ───────────────────────────────────────────────────────────
        # OSC BRIDGE BOOTSTRAP (CORRECT ORDER + DYNAMIC MAPPING)
        # ───────────────────────────────────────────────────────────
        try:
            if not hasattr(self, "osc_bridge") or not self.osc_bridge:
                log_osc_core("Initializing OSCBridge", level="INFO", action_type="init")

                self.osc_bridge = OSCBridge()

                # 1. Discover VRChat ports (authoritative input side)
                self.osc_bridge.discover_vrchat_ports()

                # 2. Advertise Stream Connector services (authoritative output side)
                self.osc_bridge.advertise_self()

                # 3. Delayed dynamic mapping refresh (critical)
                def _delayed_dynamic_refresh():
                    try:
                        time.sleep(0.5)  # allow HTTP server + zeroconf to settle
                        refresh_dynamic_osc_mapping()
                        log_osc_core(
                            "OSCQuery dynamic mapping refreshed",
                            level="INFO",
                            action_type="config"
                        )
                    except Exception as e:
                        log_osc_core(
                            "OSCQuery dynamic mapping refresh failed",
                            level="ERROR",
                            action_type="config",
                            exc=e
                        )

                threading.Thread(target=_delayed_dynamic_refresh, daemon=True).start()

            # 4. Only now fetch VRChat parameters
            self.osc_bridge.fetch_parameters()

        except Exception as e:
            avatar_log_verbose("OSCBridge init failed", level="ERROR", exc=e)
            log_gui_action("OSCBridge connection failed", level="WARN", exc=e)

        # ───────────────────────────────────────────────────────────
        # AVATAR JSON LOAD
        # ───────────────────────────────────────────────────────────
        try:
            with open(json_path, "r", encoding="utf-8-sig") as f:
                avatar_data = json.load(f)
            avatar_id = avatar_data.get("id", "").strip()
            avatar_name = avatar_data.get("name", "").strip() or os.path.splitext(os.path.basename(json_path))[0]
        except Exception as e:
            avatar_log_verbose("Failed to load avatar JSON", level="ERROR", exc=e)
            log_gui_action("Avatar JSON load failed", level="ERROR", exc=e)
            return

        self.avatar_id = avatar_id
        self.avatar_json_path = json_path  # Store the path for validation use

        try:
            user_cfg.setdefault("avatars", {})[avatar_id] = {"path": json_path, "name": avatar_name}
            user_cfg["current_avatar_id"] = avatar_id
            save_user_cfg(user_cfg)
        except Exception as e:
            avatar_log_verbose("Failed to update config with avatar info", level="ERROR", exc=e)
            return

        # ───────────────────────────────────────────────────────────
        # CONTROLS LOAD + SANITIZE
        # ───────────────────────────────────────────────────────────
        saved_controls = load_controls()

        # Sanitize ONLY for runtime safety
        sanitized_controls = sanitize_controls(
            saved_controls,
            self.osc_bridge,
            avatar_json_path=json_path
        )
        loaded_addresses = set(item["addr"] for item in saved_controls)

        # ───────────────────────────────────────────────────────────
        # UI RESET
        # ───────────────────────────────────────────────────────────
        self._clear_all_rows()
        self.rows.clear()
        self.chains.clear()
        for fr in self.chain_frames:
            fr.destroy()
        self.chain_frames.clear()
        self._regrid_chains()

        # ───────────────────────────────────────────────────────────
        # SNAPSHOT SAVE
        # ───────────────────────────────────────────────────────────
        try:
            os.makedirs(CFG_DIR, exist_ok=True)
            with open(AVATAR_STATE_FILE, "w", encoding="utf-8") as f:
                json.dump({"id": avatar_id, "timestamp": datetime.utcnow().isoformat() + "Z"}, f)
        except Exception as e:
            avatar_log_verbose("Could not save avatar snapshot", level="WARN", exc=e)

        # ───────────────────────────────────────────────────────────
        # DEFAULT LOADERS
        # ───────────────────────────────────────────────────────────
        self._load_static_avatar_defaults(json_path)
        self._load_live_avatar_defaults()

        # ───────────────────────────────────────────────────────────
        # REBUILD ROWS FROM SAVED CONTROLS
        # ───────────────────────────────────────────────────────────
        for item in saved_controls:
            addr = item["addr"]
            if addr not in self.rows:
                self._create_row(
                    addr,
                    default_args=item.get("default", [0]),
                    initial_timer=item.get("timer", ""),
                    persist=False,
                    initial_value=item.get("value", [0])[0]
                )

        # ───────────────────────────────────────────────────────────
        # IMPORT FROM AVATAR JSON
        # ───────────────────────────────────────────────────────────
        parsed_controls = list(parse_avatar_json(json_path))
        imported = 0

        for cid, cname, aid, aname, addr, default in parsed_controls:
            if addr not in self.rows and addr not in loaded_addresses:
                self._create_row(
                    addr,
                    default,
                    category_id=cid,
                    category_name=cname,
                    action_id=aid,
                    action_name=aname,
                    persist=False
                )
                imported += 1

        # ───────────────────────────────────────────────────────────
        # LEGACY IMPORT
        # ───────────────────────────────────────────────────────────
        try:
            with open(json_path, "r", encoding="utf-8-sig") as f:
                legacy_data = json.load(f)

            for item in legacy_data.get("controls", []):
                addr = item.get("addr")
                if addr and addr not in self.rows:
                    self._create_row(
                        addr,
                        default_args=item.get("default", [0]),
                        initial_timer=item.get("timer", ""),
                        persist=False,
                        initial_value=item.get("value", [0])[0]
                    )
                    imported += 1

            avatar_log_verbose("Imported legacy controls", data={"count": len(legacy_data.get("controls", []))})

        except Exception as e:
            avatar_log_verbose("Legacy import failed", level="WARN", exc=e)

        avatar_log_verbose("Loaded avatar", data={"avatar_id": avatar_id, "control_count": imported})
        log_gui_action("Avatar controls loaded", level="INFO", data={"avatar_id": avatar_id, "imported": imported})

        # ───────────────────────────────────────────────────────────
        # CHAINS
        # ───────────────────────────────────────────────────────────
        chains = self._upgrade_chain_data(load_chains())
        for chain in chains:
            self._register_chain(chain)

        avatar_log_verbose("Chains loaded for avatar", data={"avatar_id": avatar_id, "chain_count": len(chains)})

        # ───────────────────────────────────────────────────────────
        # LAST AVATAR PATH SAVE
        # ───────────────────────────────────────────────────────────
        try:
            os.makedirs(CFG_DIR, exist_ok=True)
            last_path = os.path.join(CFG_DIR, "last_avatar.txt")
            with open(last_path, "w", encoding="utf-8") as f:
                f.write(json_path)
            avatar_log_verbose("Wrote last_avatar.txt", data={"path": last_path})
        except Exception as e:
            avatar_log_verbose("Failed to write last_avatar.txt", level="WARN", exc=e)
            log_gui_action("Failed to write last_avatar.txt", level="WARN", exc=e)

    def _load_static_avatar_defaults(self, json_path: str) -> None:
        try:
            with open(json_path, "r", encoding="utf-8-sig") as f:
                data = json.load(f)

            loaded = 0
            for param in data.get("parameters", []):
                inp = param.get("input", {})
                addr = inp.get("address", "")
                if addr.startswith("/avatar/parameters/") and addr not in NOISY_PARAMETERS:
                    typ = (inp.get("type") or "").lower()
                    self.avatar_live_params[addr] = {
                        "bool": False,
                        "float": 0.0,
                        "int": 0
                    }.get(typ, 0)
                    loaded += 1

            avatar_log_verbose(
                "Loaded static avatar defaults",
                data={"file": json_path, "loaded_count": loaded}
            )

        except Exception as e:
            avatar_log_verbose(
                f"Could not load static defaults from {json_path}",
                level="ERROR",
                exc=e
            )

    def _load_live_avatar_defaults(self) -> None:
        if not self.avatar_id:
            avatar_log_verbose(
                "No avatar ID set - skipping live snapshot load.",
                level="INFO"
            )
            return

        snapshot_path = os.path.join(AVATAR_STATE_DIR, f"{self.avatar_id}.json")
        if not os.path.exists(snapshot_path):
            avatar_log_verbose(
                f"No live snapshot for avatar {self.avatar_id}",
                data={"snapshot_path": snapshot_path},
                level="INFO"
            )
            return

        try:
            with open(snapshot_path, "r", encoding="utf-8") as f:
                snapshot = json.load(f)
            self.avatar_live_params.update(snapshot.get("parameters", {}))
            avatar_log_verbose(
                "Loaded live avatar snapshot",
                data={
                    "avatar_id": self.avatar_id,
                    "param_count": len(self.avatar_live_params),
                    "snapshot_path": snapshot_path
                }
            )
        except Exception as e:
            avatar_log_verbose(
                f"Could not load live avatar snapshot from {snapshot_path}",
                level="ERROR",
                exc=e
            )

    def _load_avatar_parameters(self, path: str) -> Dict[str, Any]:
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                data = json.load(f)
        except Exception as e:
            avatar_log_verbose(
                "Failed to load avatar JSON.",
                data={"path": path},
                level="ERROR",
                exc=e
            )
            return {}

        result = {}
        for prm in data.get("parameters", []):
            addr = ((prm.get("input") or {}).get("address") or "").strip()
            if addr:
                result[addr] = prm.get("defaultValue", 0)

        avatar_log_verbose(
            "Parsed avatar parameters.",
            data={"path": path, "parameter_count": len(result)}
        )
        return result

    def auto_load_last_avatar(self, load_avatar_callback: Callable[[str], None]) -> None:
        try:
            avatar_path = None
            last_txt = os.path.join(CFG_DIR, "last_avatar.txt")
            user_json = os.path.join("saved/config/user.json")

            if os.path.exists(last_txt):
                with open(last_txt, "r", encoding="utf-8") as f:
                    avatar_path = f.read().strip()

            if (not avatar_path or not os.path.exists(avatar_path)) and os.path.exists(user_json):
                with open(user_json, "r", encoding="utf-8") as f:
                    user_data = json.load(f)
                aid = user_data.get("current_avatar_id")
                av_entry = user_data.get("avatars", {}).get(aid)
                if isinstance(av_entry, dict):
                    avatar_path = av_entry.get("path")
                elif isinstance(av_entry, str):
                    avatar_path = av_entry

            if avatar_path and os.path.exists(avatar_path):
                try:
                    with open(avatar_path, "r", encoding="utf-8-sig") as f:
                        data = json.load(f)
                    name = data.get("name") or os.path.splitext(os.path.basename(avatar_path))[0]
                    avatar_log_verbose(
                        "Auto-loaded avatar metadata.",
                        data={"path": avatar_path, "name": name},
                        level="INFO"
                    )
                except Exception as e:
                    avatar_log_verbose(
                        "Could not read avatar name during auto-load.",
                        data={"path": avatar_path},
                        level="WARN",
                        exc=e
                    )

                avatar_log_verbose(
                    "Auto-loading avatar.",
                    data={"path": avatar_path},
                    level="INFO"
                )
                load_avatar_callback(avatar_path)
            else:
                avatar_log_verbose("No valid avatar path found.", level="INFO")

        except Exception as e:
            avatar_log_verbose(
                "Failed to auto-load last avatar.",
                level="ERROR",
                exc=e
            )
          
    # ------------------------------------------------------
    # layout save
    # ------------------------------------------------------
    def save_layout(self) -> None:
        avatar_id = self.avatar_id or user_cfg.get("current_avatar_id")

        if not avatar_id:
            messagebox.showwarning(
                "No Avatar",
                "No avatar is currently loaded. Cannot save layout."
            )
            return

        if not self.rows:
            log_gui_action(
                "Save aborted: no rows loaded.",
                data={"reason": "empty rows", "avatar_id": avatar_id},
                level="WARN"
            )
            messagebox.showerror(
                "Save Aborted",
                "No controls are loaded. Save skipped to prevent data loss."
            )
            return

        # Persist avatar reference
        self.avatar_id = avatar_id
        user_cfg["current_avatar_id"] = avatar_id
        save_user_cfg(user_cfg)

        # --------------------------------------------------
        # BUILD CONTROL PAYLOAD (UI IS SOURCE OF TRUTH)
        # --------------------------------------------------
        controls = []

        for addr, row in self.rows.items():
            try:
                controls.append({
                    "addr": addr,

                    # EXACT value shown in Default column
                    "default": [
                        float(row["default_var"].get())
                        if row["default_var"].get() != ""
                        else 0
                    ],

                    # EXACT value shown in Value column
                    "value": [
                        float(row["value_var"].get())
                        if row["value_var"].get() != ""
                        else 0
                    ],

                    # EXACT timer string (do not coerce)
                    "timer": row["timer_var"].get(),
                })
            except Exception as e:
                log_gui_action(
                    "Failed serializing control row",
                    data={"addr": addr},
                    level="ERROR",
                    exc=e
                )

        controls_path = os.path.join(
            AVTR_CFG_CTLS_DIR,
            f"controls_{avatar_id}.json"
        )

        # --------------------------------------------------
        # Backup existing file
        # --------------------------------------------------
        if os.path.exists(controls_path):
            try:
                shutil.copy2(controls_path, controls_path + ".bak")
                log_gui_action(
                    "Backed up existing control layout.",
                    data={"backup": controls_path + ".bak"},
                    level="INFO"
                )
            except Exception as e:
                log_gui_action(
                    "Failed to create layout backup.",
                    data={"path": controls_path},
                    level="ERROR",
                    exc=e
                )
                messagebox.showerror(
                    "Backup Failed",
                    f"Could not back up existing layout:\n{e}"
                )
                return

        # --------------------------------------------------
        # Write new layout
        # --------------------------------------------------
        try:
            with open(controls_path, "w", encoding="utf-8") as f:
                json.dump({"controls": controls}, f, indent=2)

            log_gui_action(
                "Saved layout successfully.",
                data={
                    "path": controls_path,
                    "controls": len(controls)
                },
                level="INFO"
            )

            messagebox.showinfo(
                "Saved",
                f"Layout for avatar '{avatar_id}' saved successfully."
            )

        except Exception as e:
            log_gui_action(
                "Failed to write controls file.",
                data={"path": controls_path},
                level="ERROR",
                exc=e
            )
            messagebox.showerror(
                "Error",
                f"Failed to save controls:\n{e}"
            )

    # ------------------------------------------------------ delete selected
    def delete_selected(self) -> None:
        cfg = load_controls()
        selected_addrs = [addr for addr, row in self.rows.items() if row["sel_var"].get()]
        if not selected_addrs:
            log_gui_action(
                "Delete selected: no rows were selected.",
                data={"action": "delete_selected"},
                level="DEBUG"
            )
            return

        log_gui_action(
            "Deleting selected controls.",
            data={"count": len(selected_addrs), "addresses": selected_addrs},
            level="INFO"
        )

        for addr in selected_addrs:
            if addr in self._pending_reset:
                try:
                    self.after_cancel(self._pending_reset.pop(addr))
                except Exception as e:
                    log_gui_action(
                        "Failed to cancel scheduled reset.",
                        data={"addr": addr},
                        level="WARN",
                        exc=e
                    )

            row = self.rows.pop(addr, None)
            if row:
                for widget in row["widgets"].values():
                    try:
                        widget.destroy()
                    except Exception as e:
                        log_gui_action(
                            "Failed to destroy widget during row deletion.",
                            data={"addr": addr},
                            level="WARN",
                            exc=e
                        )

        base_selected = {addr.split("::#")[0] for addr in selected_addrs}
        cfg = [
            c for c in cfg
            if c["addr"] not in selected_addrs and c["addr"].split("::#")[0] not in base_selected
        ]
        save_controls(cfg)

        log_gui_action(
            "Updated control configuration after deletion.",
            data={"remaining_controls": len(cfg)},
            level="DEBUG"
        )

        self._repack_table()

    # Controls Helpers
    def _row_for_addr(self, osc_addr: str) -> dict[str, Any] | None:
        for key, row in self.rows.items():
            base = key.split("::#")[0]  # strip INT-series suffix
            if base == osc_addr:
                return row
        return None

    def _delete_row(self, address: str, *, suppress_repack: bool = False) -> None:
        row = self.rows.pop(address, None)
        if row:
            for widget in row.get("widgets", {}).values():
                try:
                    widget.destroy()
                except Exception as e:
                    log_gui_action(
                        "Failed to destroy widget during row deletion.",
                        data={"address": address},
                        level="WARN",
                        exc=e
                    )

        try:
            cfg = [c for c in load_controls() if c["addr"] != address]
            save_controls(cfg)
            controls_log_verbose(
                "Deleted row and updated config.",
                {"address": address, "remaining": len(cfg)},
                level="INFO"
            )
        except Exception as e:
            controls_log_verbose(
                "Failed to update config after row deletion.",
                {"address": address},
                level="ERROR",
                exc=e
            )

        if not suppress_repack:
            self._repack_table()

    def _duplicate_row(self, address: str) -> None:
        row = self.rows.get(address)
        if not row:
            controls_log_verbose("Attempted to duplicate a nonexistent row.", {"address": address}, level="WARN")
            return

        new_addr = self.ask_string("Duplicate Row", "New address for duplicated row:", initialvalue=address)
        if not new_addr:
            log_gui_action("Duplicate row cancelled by user.", {"source_address": address}, level="DEBUG")
            return

        new_addr = new_addr.strip()
        if new_addr in self.rows:
            messagebox.showerror("Duplicate Error", "Address already exists.")
            log_gui_action("User attempted to duplicate to an existing address.", {"conflict": new_addr}, level="WARN")
            return

        try:
            self._create_row(
                new_addr,
                default_args=[self._normalize(row["default_var"].get())],
                initial_timer=row["timer_var"].get(),
                is_integer=isinstance(self._normalize(row["value_var"].get()), int),
                initial_value=self._normalize(row["value_var"].get())
            )
            controls_log_verbose("Duplicated control row.", {"from": address, "to": new_addr}, level="INFO")
        except Exception as e:
            controls_log_verbose("Failed to duplicate control row.", {"from": address, "to": new_addr}, level="ERROR", exc=e)

    def edit_row(self, address: str) -> None:
        row = self.rows.get(address)
        if not row:
            controls_log_verbose("Attempted to edit nonexistent row.", {"address": address}, level="WARN")
            return

        new_address = self.ask_string("Edit Address", f"Edit address for:\n{address}", initialvalue=address)
        if not new_address:
            log_gui_action("Edit address cancelled by user.", {"original": address}, level="DEBUG")
            return

        new_value = self.ask_string("Edit Value", f"New value for {new_address}:", initialvalue=row["value_var"].get())
        if new_value is None:
            log_gui_action("Edit value cancelled by user.", {"address": new_address}, level="DEBUG")
            return

        try:
            if new_address != address:
                old_row = self.rows.pop(address)
                old_row["widgets"]["label"].config(text=new_address)
                self.rows[new_address] = old_row

                unregister_action(address, "avatar_parameters")
                register_action(
                    "avatar_parameters", "Avatar Parameters",
                    new_address, new_address,
                    lambda _c, a=new_address, v=old_row["value_var"], t=old_row["timer_var"]:
                        self.send_osc(a, self._normalize(v.get()), t.get())
                )

                controls_log_verbose("Control address edited.", {"from": address, "to": new_address}, level="INFO")

            self.rows[new_address]["value_var"].set(new_value)
            controls_log_verbose("Control value updated after edit.", {"address": new_address, "new_value": new_value}, level="DEBUG")

            self.save_layout()

        except Exception as e:
            controls_log_verbose("Failed to edit control row.", {"original": address, "new_address": new_address}, level="ERROR", exc=e)
            log_gui_action("Error occurred during row edit.", {"address": address}, level="ERROR", exc=e)

    # ────────── revised helper  add_int_series  ──────────
    def add_int_series(self) -> None:
        addr = self.ask_string("OSC Address (integer series)", "/avatar/parameters/twitch")
        if not addr:
            return

        try:
            rng = self.ask_string("Range", "min,max (e.g. 1,25):", initialvalue="1,25")
            lo, hi = [int(x.strip()) for x in rng.split(",")]
            if lo >= hi:
                raise ValueError
        except Exception:
            messagebox.showerror("Bad input", "Range must be two ascending integers.")
            return

        timer = self.ask_string("Timer (s)", "Auto-reset timer for each row (blank = none):", initialvalue="")

        created = 0
        for val in range(lo, hi + 1):
            key = f"{addr}::#{val}"
            self._create_row(
                key,
                [0],
                initial_timer=timer,
                action_id=f"{addr}={val}",
                action_name=f"{addr} → {val}",
                is_integer=True,
                int_min=lo,
                int_max=hi,
                initial_value=val
            )
            created += 1

        log_data = {
            "base_address": addr,
            "range": f"{lo} to {hi}",
            "count": created,
            "timer": timer
        }

        controls_log_verbose("Created integer series rows via GUI", log_data)
        log_gui_action("User created integer series rows", log_data)

    def create_int_series_rows(
        self,
        addr: str,
        lo: int = 1,
        hi: int = 25,
        timer: int = 1,
    ) -> None:
        created = 0
        for val in range(lo, hi + 1):
            key = f"{addr}::#{val}"
            if key in self.rows:
                continue
            self._create_row(
                key,
                [0],
                initial_timer=timer,
                action_id=f"{addr}={val}",
                action_name=f"{addr} → {val}",
                is_integer=True,
                int_min=lo,
                int_max=hi,
                initial_value=val
            )
            created += 1

        log_data = {
            "base_address": addr,
            "range": f"{lo} to {hi}",
            "count": created,
            "timer": timer
        }

        controls_log_verbose("Silently created integer series rows", log_data)
        log_gui_action("Programmatically created integer series rows", log_data)

    # ───────────────────────────────────────── send_osc (spam-tolerant, INT-safe)
    def send_osc(self, address: str, value: Any, timer_str: str) -> None:
        import re
        import time

        # ─────────────────────────────────────────
        # Normalize address
        # ─────────────────────────────────────────
        is_int_series = "::#" in address
        wire_address = address.split("::#", 1)[0] if is_int_series else address
        wire_address = re.sub(r'^!+', '', wire_address)

        try:
            value = self._normalize(value)
        except Exception:
            pass

        if is_int_series:
            try:
                value = int(round(float(value)))
            except Exception:
                value = 0

        # ─────────────────────────────────────────
        # Snapshot PREVIOUS VALUE (CRITICAL)
        # ─────────────────────────────────────────
        prev_value = None
        if hasattr(self, "osc_bridge"):
            try:
                prev_value = self.osc_bridge.get_last_sent(wire_address)
            except Exception:
                prev_value = None

        # If we've never sent this param before, do NOT guess
        # Leave reset disabled unless explicitly requested
        # (this prevents destructive resets)
        allow_reset = prev_value is not None

        # ─────────────────────────────────────────
        # Send OSC
        # ─────────────────────────────────────────
        sent = False
        if hasattr(self, "osc_bridge"):
            try:
                sent = self.osc_bridge.send(
                    wire_address,
                    value,
                    restore_previous=False,
                )
            except Exception:
                sent = False

        if not sent:
            try:
                if not hasattr(self, "_osc_fallback_client"):
                    from pythonosc.udp_client import SimpleUDPClient
                    self._osc_fallback_client = SimpleUDPClient("127.0.0.1", 9000)

                self._osc_fallback_client.send_message(wire_address, value)
            except Exception:
                return

        self._record_outgoing_osc(wire_address)

        # ─────────────────────────────────────────
        # RESET (only if we have a real previous value)
        # ─────────────────────────────────────────
        try:
            delay = float(timer_str)
        except Exception:
            return

        if delay <= 0 or not allow_reset:
            return

        def _reset():
            try:
                # Ensure no newer send happened
                current = self.osc_bridge.get_last_sent(wire_address)
                if current != value:
                    return

                self.osc_bridge.send(
                    wire_address,
                    prev_value,
                    restore_previous=False
                )

                self._record_outgoing_osc(wire_address)

                log_osc_core(
                    "OSC reset (restored previous)",
                    {"addr": wire_address, "value": prev_value},
                    level="DEBUG",
                    action_type="reset"
                )

            except Exception as e:
                log_osc_core(
                    "OSC reset failed",
                    {"addr": wire_address},
                    level="ERROR",
                    exc=e
                )

        self.after(int(delay * 1000), _reset)

    # ───────────────────────────── self-send tracker ──────────────────────
    def _record_outgoing_osc(self, addr: str) -> None:
        """
        Records an outgoing OSC message for echo-suppression only.

        IMPORTANT:
        - This does NOT imply state ownership
        - This does NOT track resets
        - This does NOT infer current value
        - This is ONLY for preventing self-echo loops
        """

        try:
            now = time.time()
            addr = addr.strip()

            # Lazily init
            if not hasattr(self, "_recent_sent"):
                from collections import deque
                self._recent_sent = deque()

            # Record event
            self._recent_sent.append((now, addr))

            log_osc_core(
                "Recorded outgoing OSC",
                {"addr": addr, "timestamp": now},
                level="DEBUG",
                action_type="echo"
            )

            # ─────────────────────────────────────────
            # Purge old entries
            # ─────────────────────────────────────────
            cutoff = now - self._SELF_ECHO_WINDOW

            purged = 0
            while self._recent_sent and self._recent_sent[0][0] < cutoff:
                self._recent_sent.popleft()
                purged += 1

            if purged:
                log_osc_core(
                    "Purged OSC echo history",
                    {"purged": purged, "window_s": self._SELF_ECHO_WINDOW},
                    level="DEBUG",
                    action_type="echo"
                )

        except Exception as e:
            log_osc_core(
                "Failed recording outgoing OSC",
                {"addr": addr},
                level="ERROR",
                exc=e,
                action_type="echo"
            )

# ────────────────────────────────────────────────────────────────────────────────
# Timing Lock Integration Helpers
# ────────────────────────────────────────────────────────────────────────────────
    def _get_integration_busy_until(self) -> float:
        import time
        now = time.time()

        busy = 0.0

        # PiShock global busy (already exists)
        try:
            busy = max(busy, float(getattr(getattr(self, "pishock", None), "_busy_global", 0) or 0))
        except Exception:
            pass

        # OwO busy (new API)
        try:
            if getattr(self, "owo", None) and hasattr(self.owo, "get_busy_until"):
                busy = max(busy, float(self.owo.get_busy_until() or 0))
        except Exception:
            pass

        # Intiface predicted busy (we'll set this at dispatch time)
        try:
            busy = max(busy, float(getattr(self, "_intiface_busy_until", 0) or 0))
        except Exception:
            pass

        # Optional: global integration busy (if you want one umbrella)
        try:
            busy = max(busy, float(getattr(self, "_integration_busy_until", 0) or 0))
        except Exception:
            pass

        return busy if busy > now else 0.0


    def _estimate_osc_post_block(self, steps: list, delay: float) -> float:
        """
        Returns additional wall time we should wait AFTER executor finishes:
          max(osc_timers, osc_pacing)
        """
        import math

        try:
            d = float(delay or 0.0)
        except Exception:
            d = 0.0
        if d < 0:
            d = 0.0

        # pacing time
        try:
            pacing = max(0.0, (len(steps) if steps else 0) * d)
        except Exception:
            pacing = 0.0

        # timers: longest timer_secs used in osc_bridge.send
        longest_timer = 0.0
        try:
            for s in (steps or []):
                if isinstance(s, dict):
                    t = float(s.get("timer", 0) or 0)
                    if t > longest_timer:
                        longest_timer = t
                else:
                    # list form: [addr, value, timer]
                    if isinstance(s, (list, tuple)) and len(s) >= 3:
                        try:
                            t = float(s[2] or 0)
                            if t > longest_timer:
                                longest_timer = t
                        except Exception:
                            pass
        except Exception:
            longest_timer = 0.0

        if longest_timer < 0:
            longest_timer = 0.0

        return max(pacing, longest_timer)


    def _predict_intiface_busy(self, cfg: dict) -> float:
        """
        Since Intiface is async in hybrid mode, we predict how long it will run and set a busy gate.
        This matches your executor normalization logic.
        """
        import time

        if not cfg or not cfg.get("intiface_enabled"):
            return 0.0
        if not getattr(self, "intiface", None):
            return 0.0

        mode = (cfg.get("intiface_mode") or "vibrate").strip().lower()

        if mode == "pattern":
            # sum normalized pattern durations
            total = 0.0
            try:
                pat_key = (cfg.get("intiface_pattern_key") or "Default").strip()
                patterns = cfg.get("intiface_patterns") or {}
                steps = patterns.get(pat_key) or cfg.get("intiface_pattern") or []
                if not isinstance(steps, list):
                    steps = []

                for s in steps:
                    if not isinstance(s, dict):
                        continue
                    try:
                        dur = float(s.get("duration", 1.0))
                    except Exception:
                        dur = 1.0
                    if dur < 0.05:
                        dur = 0.05
                    if dur > 1800.0:
                        dur = 1800.0
                    total += dur
            except Exception:
                total = float(cfg.get("intiface_duration", 1.0) or 1.0)

            return time.time() + max(0.0, total)

        # non-pattern: duration field
        try:
            dur = float(cfg.get("intiface_duration", 1.0) or 1.0)
        except Exception:
            dur = 1.0
        if dur < 0.05:
            dur = 0.05
        if dur > 1800.0:
            dur = 1800.0

        return time.time() + dur

# ────────────────────────────────────────────────────────────────────────────────
# Start the PiShock connector
# ────────────────────────────────────────────────────────────────────────────────
    def start_pishock(self):
        latest_cfg = load_user_cfg()
        username   = latest_cfg.get("pishock_username")
        apikey     = latest_cfg.get("pishock_apikey")

        if not username or not apikey:
            pishock_log_verbose(
                "PiShock credentials not found in config - skipping startup.",
                level="WARN"
            )
            return

        # Prevent double start
        if hasattr(self, "pishock") and self.pishock and self.pishock.is_running():
            pishock_log_verbose(
                "PiShock client already running - skipping reinitialization.",
                level="INFO"
            )
            return

        # Unified queue emitter
        def _emit(ev: str, data: dict | None = None):
            try:
                self.q.put(("pishock", ev, data or {}))
            except Exception:
                pass

        # Raw websocket echo (debug only)
        def _echo(msg):
            pishock_log_verbose(
                "PiShock WebSocket message received",
                {"message": msg},
                level="DEBUG"
            )

        try:
            self.pishock = PiShockClient(
                username=username,
                apikey=apikey,
                q=self.q,  # still used internally if needed
                on_message=_echo,
                on_connected=lambda: _emit("connected"),
                on_disconnected=lambda: _emit("disconnected"),
            )

            self.pishock.start()

            # Explicit post-start lifecycle signal
            _emit("session_subscribe")

            pishock_log_verbose(
                "PiShock WebSocket client started successfully.",
                level="INFO"
            )

        except Exception as exc:
            pishock_log_verbose(
                "Failed to start PiShock client",
                level="ERROR",
                exc=exc
            )
            _emit("error", {"exception": str(exc)})

# ────────────────────────────────────────────────────────────────────────────────
# Parallel shock helper
# ────────────────────────────────────────────────────────────────────────────────
    def _run_pishock_parallel(
        self,
        device_objs: List[Dict],
        mode: str,
        intensity: int,
        total_duration_ms: int,
        *,
        delay_ms: int = 1000,
        step: int = 10,
        pattern: Optional[List] = None
    ) -> List[threading.Thread]:
        """
        Send shocks to multiple devices in parallel (one thread per device).

        HARDENED VERSION:
        - Deep snapshot + dedupe to avoid shared-mutation bugs
        - No internal randomization (randomness is handled upstream)
        - Defensive ID normalization
        - Safe under concurrent chains, random loops, queue worker, hybrid mode
        """

        import threading
        import time
        import asyncio
        import copy

        threads: List[threading.Thread] = []

        MAX_TOTAL_MS = 1_800_000
        total_duration_ms = min(int(total_duration_ms or 0), MAX_TOTAL_MS)

        # ─────────────────────────────────────────────────────────
        # HARD SNAPSHOT + DEDUPE + ISOLATION (CRITICAL FIX)
        # ─────────────────────────────────────────────────────────
        devices: List[Dict] = []

        try:
            if device_objs:
                snapshot = [copy.deepcopy(d) for d in device_objs if isinstance(d, dict)]

                seen_ids = set()
                for d in snapshot:
                    dev_id = str(d.get("id") or d.get("shockerId") or "").strip()
                    if not dev_id:
                        continue

                    # Hard dedupe
                    if dev_id in seen_ids:
                        continue

                    seen_ids.add(dev_id)
                    devices.append(d)

        except Exception as e:
            pishock_log_verbose(
                "Failed to snapshot PiShock devices (isolation layer)",
                level="ERROR",
                exc=e
            )
            devices = []

        # Build flat list of IDs (defensive)
        device_ids: List[str] = []
        for d in devices:
            try:
                dev_id = str(d.get("id") or d.get("shockerId") or "").strip()
                if dev_id:
                    device_ids.append(dev_id)
            except Exception:
                continue

        # Order-preserving hard dedupe (safety net)
        device_ids = list(dict.fromkeys(device_ids))

        if not device_ids:
            pishock_log_verbose(
                "⚠ Parallel helper: no valid device IDs after snapshot isolation",
                {
                    "input_len": len(device_objs) if device_objs else 0,
                    "snapshot_len": len(devices),
                    "mode": mode,
                },
                level="WARN"
            )
            return []

        # Debug log
        pishock_log_verbose(
            "Parallel helper start",
            {
                "mode": mode,
                "intensity": intensity,
                "total_duration_ms": total_duration_ms,
                "delay_ms": delay_ms,
                "step": step,
                "pattern_len": len(pattern or []),
                "device_ids": device_ids,
            },
            level="DEBUG"
        )

        # ─────────────────────────────────────────────────────────
        # Prime busy-gate so no other operations overlap
        # ─────────────────────────────────────────────────────────
        try:
            now = time.time()
            expire = now + (total_duration_ms / 1000.0)
            with self.pishock._thread_busy_lock:
                self.pishock._busy_global = max(self.pishock._busy_global, expire)
        except Exception as e:
            pishock_log_verbose("Failed to set PiShock busy flag", level="WARN", exc=e)

        # ─────────────────────────────────────────────────────────
        # Special Modes Handling
        # ─────────────────────────────────────────────────────────
        special_modes = {"pulse", "burst", "oscillation", "randomized_wobble", "pattern", "rage"}

        if mode in special_modes:
            for dev_id in device_ids:
                try:
                    t = self.pishock.send_special_mode(
                        dev_id,
                        mode,
                        intensity,
                        total_duration_ms,
                        delay_ms=delay_ms,
                        step=step,
                        pattern=(pattern if mode == "pattern" else None)
                    )

                    pishock_log_verbose(
                        f"Special-mode worker spawned for {dev_id}",
                        {"mode": mode, "device_id": dev_id},
                        level="DEBUG"
                    )

                    if isinstance(t, threading.Thread):
                        threads.append(t)

                except Exception as e:
                    log_chain_system(
                        "Failed to spawn special-mode worker",
                        level="ERROR",
                        exc=e,
                        action_type="runner",
                        data={"device_id": dev_id, "mode": mode}
                    )

            log_chain_system(
                "Parallel special modes launched",
                {"mode": mode, "device_ids": device_ids, "thread_count": len(threads)},
                action_type="runner",
                level="INFO"
            )
            return threads

        # ─────────────────────────────────────────────────────────
        # Per-device chunked PUBLISH batches (non-special modes)
        # ─────────────────────────────────────────────────────────
        def _device_batch(dev_id: str):
            remaining = total_duration_ms
            part_idx = 0

            pishock_log_verbose(
                f"Worker started for device {dev_id}",
                {"mode": mode, "device_id": dev_id},
                level="DEBUG"
            )

            try:
                while remaining > 0:
                    chunk = min(remaining, 15_000)

                    fut = asyncio.run_coroutine_threadsafe(
                        self.pishock._send_publish_commands([dev_id], mode, intensity, chunk),
                        self.pishock.loop
                    )

                    fut.result()  # let it raise if it fails

                    part_idx += 1
                    remaining -= chunk

                    pishock_log_verbose(
                        "Batch shock sent (per-device chunk)",
                        {
                            "device_id": dev_id,
                            "chunk_ms": chunk,
                            "intensity": intensity,
                            "mode": mode,
                            "part": part_idx,
                            "remaining_ms": remaining
                        },
                        level="INFO"
                    )

                    # Pace to avoid overlap on same device
                    if remaining > 0:
                        time.sleep(chunk / 1000.0)

                pishock_log_verbose(
                    f"✔ Worker complete for device {dev_id}",
                    {"parts": part_idx},
                    level="INFO"
                )

            except Exception as exc:
                log_chain_system(
                    "Exception in pishock device worker",
                    level="ERROR",
                    exc=exc,
                    action_type="runner",
                    data={"device_id": dev_id}
                )

        # ─────────────────────────────────────────────────────────
        # Spawn threads
        # ─────────────────────────────────────────────────────────
        for dev_id in device_ids:
            t = threading.Thread(target=_device_batch, args=(dev_id,), daemon=True)
            t.start()
            threads.append(t)

        log_chain_system(
            "Parallel pishock threads launched",
            {"mode": mode, "device_ids": device_ids, "thread_count": len(threads)},
            action_type="runner",
            level="INFO"
        )

        return threads

# ────────────────────────────────────────────────────────────────────────────────
# PiShock - Pattern Editor (real editor: select/delete/renumber + polished UI)
# ────────────────────────────────────────────────────────────────────────────────
    def open_pattern_editor(self, cfg):
        """
        Scrollable PiShock pattern editor using stable canvas+frame scrolling.
        Durations shown in seconds, saved as ms.

        Features:
          - Multi-select rows + Delete Selected
          - Per-row delete button
          - Auto-renumber steps after add/delete
          - Sticky header (outside scroll area)
          - GlowButton UI for primary actions
          - OSC Δ column always pinned + aligned
          - NO inverted scrollbar / no drifting
        """
        import random
        import tkinter as tk
        from tkinter import ttk, Toplevel, Frame, StringVar, BooleanVar

        # ───────────────────────── helpers ─────────────────────────
        def _ms_to_s(val_ms):
            try:
                return str(round(int(val_ms) / 1000, 3)).rstrip("0").rstrip(".")
            except Exception:
                return "1"

        def _s_to_ms(val_s):
            try:
                return int(float(val_s) * 1000)
            except Exception:
                return 1000

        def _safe_int(s, default=0):
            try:
                return int(str(s).strip())
            except Exception:
                return default

        def _safe_float(s, default=1.0):
            try:
                return float(str(s).strip())
            except Exception:
                return default

        # ───────────────────────── window ──────────────────────────
        editor = Toplevel(self)
        self.apply_theme_to_toplevel(editor)
        editor.title("Pattern Editor")
        editor.geometry("720x600")
        editor.minsize(640, 480)
        editor.grab_set()

        editor.grid_rowconfigure(0, weight=1)
        editor.grid_columnconfigure(0, weight=1)

        step_rows: list[dict] = []  # authoritative in-memory row model

        # shell
        shell = Frame(editor, bg=BG)
        shell.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        shell.grid_rowconfigure(2, weight=1)
        shell.grid_columnconfigure(0, weight=1)

        # ───────────────────────── header row (sticky) ─────────────
        hdr = Frame(shell, bg=BG)
        hdr.grid(row=0, column=0, sticky="ew", pady=(0, 6))

        # Columns:
        # 0 Sel | 1 Step | 2 Mode | 3 Intensity | 4 Duration | 5 Delay | 6 Δ Int | 7 Del
        headers = ["", "Step", "Mode", "Intensity", "Duration (s)", "Delay (s)", "Δ Int.", ""]
        col_widths = {
            0: 34,
            1: 50,
            2: 160,
            3: 90,
            4: 120,
            5: 120,
            6: 80,
            7: 44,
        }

        # IMPORTANT: lock header columns (minsize + weights)
        for c, w in col_widths.items():
            hdr.grid_columnconfigure(c, minsize=w, weight=(1 if c == 2 else 0))
        hdr.grid_columnconfigure(2, weight=1)  # Mode stretches

        for col, text in enumerate(headers):
            lbl = ttk.Label(hdr, text=text, style="Header.TLabel")
            lbl.grid(row=0, column=col, sticky="nsew", padx=4)
            try:
                ToolTip(lbl, f"{text or 'Select'} column")
            except Exception:
                pass

        # ───────────────────────── toolbar (sticky) ─────────────
        topbar = Frame(shell, bg=BG)
        topbar.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        topbar.grid_columnconfigure(0, weight=1)

        info_lbl = ttk.Label(
            topbar,
            text="Tip: Select rows then Delete Selected. Double-click values to edit.",
            style="HeaderAddr.TLabel"
        )
        info_lbl.grid(row=0, column=0, sticky="w", padx=(0, 8))

        actions = Frame(topbar, bg=BG)
        actions.grid(row=0, column=1, sticky="e")

        # ───────────────────────── scrollable body (stable) ─────────────────
        scroll_frame = Frame(shell, bg=BG)
        scroll_frame.grid(row=2, column=0, sticky="nsew")
        shell.grid_rowconfigure(2, weight=1)
        scroll_frame.grid_rowconfigure(0, weight=1)
        scroll_frame.grid_columnconfigure(0, weight=1)

        canvas = tk.Canvas(scroll_frame, bg=BG, highlightthickness=0, bd=0)
        canvas.grid(row=0, column=0, sticky="nsew")

        vscroll = ttk.Scrollbar(scroll_frame, orient="vertical", command=canvas.yview)
        vscroll.grid(row=0, column=1, sticky="ns")

        canvas.configure(yscrollcommand=vscroll.set)

        # table lives inside the canvas
        table = Frame(canvas, bg=BG)
        table_window = canvas.create_window((0, 0), window=table, anchor="nw")

        # IMPORTANT: lock table columns EXACTLY like header (minsize + weights)
        for c, w in col_widths.items():
            table.grid_columnconfigure(c, minsize=w, weight=(1 if c == 2 else 0))
        table.grid_columnconfigure(2, weight=1)

        def _update_scroll_region(_=None):
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _sync_table_width(event):
            # Make the inner table match the canvas width so columns align with header
            canvas.itemconfigure(table_window, width=event.width)

        table.bind("<Configure>", _update_scroll_region)
        canvas.bind("<Configure>", _sync_table_width)

        # Optional mousewheel (Windows). If you already have a global wheel binder, delete this.
        def _on_mousewheel(e):
            try:
                canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
            except Exception:
                pass

        canvas.bind_all("<MouseWheel>", _on_mousewheel)

        # ───────────────────────── row utilities ───────────────────
        def _renumber_steps():
            for i, r in enumerate(step_rows, start=1):
                r["step_var"].set(str(i))

        def _refresh_delta_column_state():
            # Column stays pinned ALWAYS
            table.grid_columnconfigure(6, minsize=col_widths[6])
            hdr.grid_columnconfigure(6, minsize=col_widths[6])

            for r in step_rows:
                if r["mode_var"].get() == "oscillation":
                    # Show Δ Int
                    r["e_osc"].grid()
                    r["e_osc"].configure(state="normal")
                else:
                    # Hide Δ Int visually, but keep column width
                    r["e_osc"].grid_remove()

        def _destroy_row_widgets(r):
            for w in r["widgets"]:
                try:
                    w.destroy()
                except Exception:
                    pass

        def _rebuild_grid_positions():
            """
            Grid rows deterministically.
            IMPORTANT: first row in the scroll body is row=0 (header is outside canvas).
            """
            for row_idx, r in enumerate(step_rows, start=0):
                r["row_index"] = row_idx

                # Use identical padding and sticky across all cells to keep alignment perfect.
                r["sel_cb"].grid(row=row_idx, column=0, padx=4, pady=2, sticky="nsew")
                r["e_step"].grid(row=row_idx, column=1, padx=4, pady=2, sticky="nsew")
                r["c_mode"].grid(row=row_idx, column=2, padx=4, pady=2, sticky="nsew")
                r["e_int"].grid(row=row_idx, column=3, padx=4, pady=2, sticky="nsew")
                r["e_dur"].grid(row=row_idx, column=4, padx=4, pady=2, sticky="nsew")
                r["e_del"].grid(row=row_idx, column=5, padx=4, pady=2, sticky="nsew")

                r["del_btn"].grid(row=row_idx, column=7, padx=4, pady=2, sticky="nsew")

            _renumber_steps()
            _refresh_delta_column_state()
            _update_scroll_region()

        def _delete_selected():
            keep = []
            for r in step_rows:
                if r["sel_var"].get():
                    _destroy_row_widgets(r)
                else:
                    keep.append(r)

            step_rows[:] = keep
            _rebuild_grid_positions()

        def _delete_one(row_ref):
            idx = None
            for i, r in enumerate(step_rows):
                if r is row_ref:
                    idx = i
                    break
            if idx is None:
                return

            _destroy_row_widgets(step_rows[idx])
            del step_rows[idx]
            _rebuild_grid_positions()

        # ───────────────────────── row factory ─────────────────────
        def add_step_row(data=None):
            data = data or {}

            step_var      = StringVar(value=str(len(step_rows) + 1))
            mode_var      = StringVar(value=str(data.get("mode", "shock")))
            intensity_var = StringVar(value=str(data.get("intensity", 50)))
            duration_var  = StringVar(value=_ms_to_s(data.get("duration", 1000)))
            delay_var     = StringVar(value=_ms_to_s(data.get("delay", 1000)))
            osc_step_var  = StringVar(value=str(data.get("osc_step", 10)))
            sel_var       = BooleanVar(value=False)

            sel_cb = ttk.Checkbutton(table, variable=sel_var)

            # Step is display-only
            e_step = ttk.Entry(table, width=4, textvariable=step_var, state="readonly", justify="center")

            c_mode = ttk.Combobox(
                table,
                values=["shock", "vibrate", "beep", "pulse", "burst", "oscillation", "randomized_wobble"],
                state="readonly",
                width=14,
                textvariable=mode_var
            )

            e_int = ttk.Entry(table, width=6,  textvariable=intensity_var, justify="center")
            e_dur = ttk.Entry(table, width=10, textvariable=duration_var, justify="center")
            e_del = ttk.Entry(table, width=10, textvariable=delay_var, justify="center")

            # Oscillation Δ intensity field
            e_osc = ttk.Entry(table, width=6, textvariable=osc_step_var, justify="center")

            # GRID IT ONCE - THIS IS REQUIRED
            e_osc.grid(
                row=len(step_rows),      # correct row
                column=6,                # Δ Int column
                padx=4,
                pady=2,
                sticky="nsew"
            )

            # Then immediately hide it
            e_osc.grid_remove()

            del_btn = ttk.Button(table, text="✕", width=3)

            # tooltips (safe)
            try:
                ToolTip(sel_cb, "Select this step")
                ToolTip(e_step, "Step number (auto-managed)")
                ToolTip(c_mode, "PiShock action type")
                ToolTip(e_int, "0–100")
                ToolTip(e_dur, "Duration (seconds)")
                ToolTip(e_del, "Delay before next step (seconds)")
                ToolTip(e_osc, "Oscillation Δ intensity (1–100)")
                ToolTip(del_btn, "Delete this step")
            except Exception:
                pass

            def _randomise():
                intensity_var.set(str(random.randint(5, 100)))
                dur = round(random.uniform(1, 15), 2)
                duration_var.set(str(dur))
                delay_var.set(str(dur))

            def on_mode_change(*_):
                mode = mode_var.get()

                # Always re-enable defaults first
                for w in (e_int, e_dur, e_del):
                    w.configure(state="normal")

                if mode == "oscillation":
                    # SHOW osc field
                    e_osc.configure(state="normal")
                    e_osc.grid()   # <-- important

                elif mode == "randomized_wobble":
                    _randomise()
                    for w in (e_int, e_dur, e_del):
                        w.configure(state="disabled")

                    # HIDE osc field
                    e_osc.configure(state="disabled")
                    e_osc.grid_remove()

                else:
                    # Normal modes: hide osc
                    e_osc.configure(state="disabled")
                    e_osc.grid_remove()

                _refresh_delta_column_state()
                _update_scroll_region()

            mode_var.trace_add("write", on_mode_change)

            row_obj = {
                "row_index": len(step_rows),
                "sel_var": sel_var,
                "step_var": step_var,
                "mode_var": mode_var,
                "intensity_var": intensity_var,
                "duration_var": duration_var,
                "delay_var": delay_var,
                "osc_step_var": osc_step_var,

                "sel_cb": sel_cb,
                "e_step": e_step,
                "c_mode": c_mode,
                "e_int": e_int,
                "e_dur": e_dur,
                "e_del": e_del,
                "e_osc": e_osc,
                "del_btn": del_btn,

                "widgets": (sel_cb, e_step, c_mode, e_int, e_dur, e_del, e_osc, del_btn),
            }

            del_btn.configure(command=lambda r=row_obj: _delete_one(r))

            step_rows.append(row_obj)

            _rebuild_grid_positions()
            on_mode_change()

        # ───────────────────────── action buttons (Glow) ────────────
        def _make_glow(parent, text, cmd, *, w=160, anchor="center"):
            gb = GlowButton(
                parent,
                text=text,
                command=cmd,
                width=w,
                height=34,
                bg=PANEL,
                glow=BUTTON,
                fg=FG,
                anchor=anchor
            )
            gb.pack(side="left", padx=6)
            return gb

        _make_glow(actions, "Add Step", lambda: add_step_row({}), w=140)
        _make_glow(actions, "Delete Selected", _delete_selected, w=170)

        # ───────────────────────── preload pattern ────────────────
        for entry in cfg.get("pishock_pattern", []):
            add_step_row(entry)

        if not step_rows:
            add_step_row({})

        # ───────────────────────── footer (Glow) ───────────────────
        footer = Frame(shell, bg=BG)
        footer.grid(row=3, column=0, sticky="ew", pady=(10, 0))
        footer.grid_columnconfigure(0, weight=1)

        right_footer = Frame(footer, bg=BG)
        right_footer.grid(row=0, column=0, sticky="e")

        def save_and_close():
            MIN_SEC = 0.05
            out = []

            _renumber_steps()

            for i, r in enumerate(step_rows, start=1):
                try:
                    mode = (r["mode_var"].get() or "shock").strip() or "shock"

                    inten = _safe_int(r["intensity_var"].get(), 50)
                    inten = max(0, min(100, inten))

                    dur_s = max(_safe_float(r["duration_var"].get(), 1.0), MIN_SEC)
                    del_s = max(_safe_float(r["delay_var"].get(), 1.0), MIN_SEC)

                    entry = {
                        "step": i,
                        "mode": mode,
                        "intensity": inten if mode != "randomized_wobble" else 0,
                        "duration": _s_to_ms(dur_s),
                        "delay": _s_to_ms(del_s),
                    }

                    if mode == "oscillation":
                        osc = _safe_int(r["osc_step_var"].get(), 10)
                        osc = max(1, min(100, osc))
                        entry["osc_step"] = osc

                    out.append(entry)
                except Exception:
                    continue

            cfg["pishock_pattern"] = out
            editor.destroy()

        gb_save = GlowButton(
            right_footer,
            text="Save Pattern",
            command=save_and_close,
            width=170,
            height=36,
            bg=PANEL,
            glow=ACCENT,
            fg=FG
        )
        gb_save.pack(side="right", padx=6)

        editor.bind("<Delete>", lambda e: _delete_selected())
        editor.bind("<Control-n>", lambda e: add_step_row({}))
        editor.bind("<Control-s>", lambda e: save_and_close())

# ─────────────────────────────────────────────────────────────
# INTIFACE PATTERN EDITOR (MODELED ON YOUR PiSHOCK EDITOR)
# ─────────────────────────────────────────────────────────────
    def open_intiface_pattern_editor(self, cfg):
        """
        Intiface pattern editor.

        Saves to:
          cfg["intiface_pattern"] = [
            {"step": 1, "actuator": "vibrate"|"constrict", "intensity": 75, "duration": 0.25, "delay": 0.25},
            ...
          ]

        - intensity is 0–100 (UI)
        - duration/delay are seconds (float)
        """
        import tkinter as tk
        from tkinter import ttk, Toplevel, Frame, StringVar, BooleanVar

        def _safe_int(s, default=0):
            try:
                return int(str(s).strip())
            except Exception:
                return default

        def _safe_float(s, default=0.25):
            try:
                return float(str(s).strip())
            except Exception:
                return default

        editor = Toplevel(self)
        self.apply_theme_to_toplevel(editor)
        editor.title("Intiface Pattern Editor")
        editor.geometry("720x600")
        editor.minsize(640, 480)
        editor.grab_set()

        editor.grid_rowconfigure(0, weight=1)
        editor.grid_columnconfigure(0, weight=1)

        step_rows: list[dict] = []

        shell = Frame(editor, bg=BG)
        shell.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        shell.grid_rowconfigure(2, weight=1)
        shell.grid_columnconfigure(0, weight=1)

        # Sticky header
        hdr = Frame(shell, bg=BG)
        hdr.grid(row=0, column=0, sticky="ew", pady=(0, 6))

        # 0 Sel | 1 Step | 2 Actuator | 3 Intensity | 4 Duration | 5 Delay | 6 Del
        headers = ["", "Step", "Actuator", "Intensity (%)", "Duration (s)", "Delay (s)", ""]
        col_widths = {0: 34, 1: 50, 2: 160, 3: 110, 4: 120, 5: 120, 6: 44}

        for c, w in col_widths.items():
            hdr.grid_columnconfigure(c, minsize=w, weight=(1 if c == 2 else 0))
        hdr.grid_columnconfigure(2, weight=1)

        for col, text in enumerate(headers):
            lbl = ttk.Label(hdr, text=text, style="Header.TLabel")
            lbl.grid(row=0, column=col, sticky="nsew", padx=4)

        # Toolbar
        topbar = Frame(shell, bg=BG)
        topbar.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        topbar.grid_columnconfigure(0, weight=1)

        info_lbl = ttk.Label(
            topbar,
            text="Tip: Select rows then Delete Selected. Ctrl+S to save.",
            style="HeaderAddr.TLabel"
        )
        info_lbl.grid(row=0, column=0, sticky="w", padx=(0, 8))

        actions = Frame(topbar, bg=BG)
        actions.grid(row=0, column=1, sticky="e")

        # Scroll body
        scroll_frame = Frame(shell, bg=BG)
        scroll_frame.grid(row=2, column=0, sticky="nsew")
        scroll_frame.grid_rowconfigure(0, weight=1)
        scroll_frame.grid_columnconfigure(0, weight=1)

        canvas = tk.Canvas(scroll_frame, bg=BG, highlightthickness=0, bd=0)
        canvas.grid(row=0, column=0, sticky="nsew")

        vscroll = ttk.Scrollbar(scroll_frame, orient="vertical", command=canvas.yview)
        vscroll.grid(row=0, column=1, sticky="ns")
        canvas.configure(yscrollcommand=vscroll.set)

        table = Frame(canvas, bg=BG)
        table_window = canvas.create_window((0, 0), window=table, anchor="nw")

        for c, w in col_widths.items():
            table.grid_columnconfigure(c, minsize=w, weight=(1 if c == 2 else 0))
        table.grid_columnconfigure(2, weight=1)

        def _update_scroll_region(_=None):
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _sync_table_width(event):
            canvas.itemconfigure(table_window, width=event.width)

        table.bind("<Configure>", _update_scroll_region)
        canvas.bind("<Configure>", _sync_table_width)

        def _on_mousewheel(e):
            try:
                canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
            except Exception:
                pass

        canvas.bind_all("<MouseWheel>", _on_mousewheel)

        # Row utilities
        def _renumber_steps():
            for i, r in enumerate(step_rows, start=1):
                r["step_var"].set(str(i))

        def _destroy_row_widgets(r):
            for w in r["widgets"]:
                try:
                    w.destroy()
                except Exception:
                    pass

        def _rebuild_grid_positions():
            for row_idx, r in enumerate(step_rows, start=0):
                r["row_index"] = row_idx
                r["sel_cb"].grid(row=row_idx, column=0, padx=4, pady=2, sticky="nsew")
                r["e_step"].grid(row=row_idx, column=1, padx=4, pady=2, sticky="nsew")
                r["c_act"].grid(row=row_idx, column=2, padx=4, pady=2, sticky="nsew")
                r["e_int"].grid(row=row_idx, column=3, padx=4, pady=2, sticky="nsew")
                r["e_dur"].grid(row=row_idx, column=4, padx=4, pady=2, sticky="nsew")
                r["e_del"].grid(row=row_idx, column=5, padx=4, pady=2, sticky="nsew")
                r["del_btn"].grid(row=row_idx, column=6, padx=4, pady=2, sticky="nsew")

            _renumber_steps()
            _update_scroll_region()

        def _delete_selected():
            keep = []
            for r in step_rows:
                if r["sel_var"].get():
                    _destroy_row_widgets(r)
                else:
                    keep.append(r)
            step_rows[:] = keep
            _rebuild_grid_positions()

        def _delete_one(row_ref):
            idx = None
            for i, r in enumerate(step_rows):
                if r is row_ref:
                    idx = i
                    break
            if idx is None:
                return
            _destroy_row_widgets(step_rows[idx])
            del step_rows[idx]
            _rebuild_grid_positions()

        # Row factory
        def add_step_row(data=None):
            data = data or {}

            step_var = StringVar(value=str(len(step_rows) + 1))
            sel_var  = BooleanVar(value=False)

            actuator = (data.get("actuator") or data.get("type") or "vibrate")
            actuator = str(actuator).strip().lower()
            if actuator not in ("vibrate", "constrict"):
                actuator = "vibrate"
            act_var = StringVar(value=actuator)

            inten = _safe_int(data.get("intensity", 75), 75)
            inten = max(0, min(100, inten))
            int_var = StringVar(value=str(inten))

            dur = _safe_float(data.get("duration", 0.25), 0.25)
            dur = max(0.05, min(60.0, dur))
            dur_var = StringVar(value=str(round(dur, 3)).rstrip("0").rstrip("."))

            dly = _safe_float(data.get("delay", 0.25), 0.25)
            dly = max(0.0, min(60.0, dly))
            dly_var = StringVar(value=str(round(dly, 3)).rstrip("0").rstrip("."))

            sel_cb = ttk.Checkbutton(table, variable=sel_var)
            e_step = ttk.Entry(table, width=4, textvariable=step_var, state="readonly", justify="center")

            # Pattern supports mixed actuators — never restrict this
            ACTUATOR_OPTIONS = ["vibrate", "constrict"]

            c_act = ttk.Combobox(
                table,
                values=ACTUATOR_OPTIONS,
                state="readonly",
                width=14,
                textvariable=act_var
            )

            e_int = ttk.Entry(table, width=8, textvariable=int_var, justify="center")
            e_dur = ttk.Entry(table, width=10, textvariable=dur_var, justify="center")
            e_del = ttk.Entry(table, width=10, textvariable=dly_var, justify="center")

            del_btn = ttk.Button(table, text="✕", width=3)

            row_obj = {
                "row_index": len(step_rows),
                "sel_var": sel_var,
                "step_var": step_var,
                "act_var": act_var,
                "int_var": int_var,
                "dur_var": dur_var,
                "dly_var": dly_var,

                "sel_cb": sel_cb,
                "e_step": e_step,
                "c_act": c_act,
                "e_int": e_int,
                "e_dur": e_dur,
                "e_del": e_del,
                "del_btn": del_btn,

                "widgets": (sel_cb, e_step, c_act, e_int, e_dur, e_del, del_btn),
            }

            del_btn.configure(command=lambda r=row_obj: _delete_one(r))
            step_rows.append(row_obj)
            _rebuild_grid_positions()

        # Glow buttons
        def _make_glow(parent, text, cmd, *, w=160):
            gb = GlowButton(
                parent,
                text=text,
                command=cmd,
                width=w,
                height=34,
                bg=PANEL,
                glow=BUTTON,
                fg=FG,
                anchor="center"
            )
            gb.pack(side="left", padx=6)
            return gb

        _make_glow(actions, "Add Step", lambda: add_step_row({}), w=140)
        _make_glow(actions, "Delete Selected", _delete_selected, w=170)

        # preload
        for entry in cfg.get("intiface_pattern", []) or []:
            if isinstance(entry, dict):
                add_step_row(entry)

        if not step_rows:
            add_step_row({})

        # footer
        footer = Frame(shell, bg=BG)
        footer.grid(row=3, column=0, sticky="ew", pady=(10, 0))
        footer.grid_columnconfigure(0, weight=1)

        right_footer = Frame(footer, bg=BG)
        right_footer.grid(row=0, column=0, sticky="e")

        def save_and_close():
            out = []
            _renumber_steps()

            for i, r in enumerate(step_rows, start=1):
                try:
                    act = (r["act_var"].get() or "").strip().lower()

                    # Pattern supports mixed actuators; fallback only if invalid
                    if act not in ("vibrate", "constrict"):
                        act = "vibrate"

                    inten = _safe_int(r["int_var"].get(), 75)
                    inten = max(0, min(100, inten))

                    dur = _safe_float(r["dur_var"].get(), 0.25)
                    dur = max(0.05, min(60.0, dur))

                    dly = _safe_float(r["dly_var"].get(), 0.25)
                    dly = max(0.0, min(60.0, dly))

                    out.append({
                        "step": i,
                        "actuator": act,
                        "intensity": inten,
                        "duration": dur,
                        "delay": dly,
                    })
                except Exception:
                    continue

            cfg["intiface_pattern"] = out
            editor.destroy()

        gb_save = GlowButton(
            right_footer,
            text="Save Pattern",
            command=save_and_close,
            width=170,
            height=36,
            bg=PANEL,
            glow=ACCENT,
            fg=FG
        )
        gb_save.pack(side="right", padx=6)

        editor.bind("<Delete>", lambda e: _delete_selected())
        editor.bind("<Control-n>", lambda e: add_step_row({}))
        editor.bind("<Control-s>", lambda e: save_and_close())

# ─────────────────────────────────────────────────────────────
# OWO PATTERN EDITOR (SDK-FORMAT, QUEUE-FRIENDLY SHAPE)
# ─────────────────────────────────────────────────────────────
    def open_owo_pattern_editor(self, cfg):
        """
        OwO pattern editor (SDK-shaped).

        Saves to:
          cfg["owo_pattern"] = [
            {
              "step": 1,
              "frequency": 60,         # int
              "duration": 1.0,         # seconds float
              "intensity": 70,         # 0-100 (base intensity)
              "fade_in_ms": 0,         # ms int
              "fade_out_ms": 0,        # ms int
              "delay_s": 0.0,          # seconds float (optional spacing)
              "muscles": [ {"id":"0","pct":100}, ... ],  # per-muscle percent
              "label": "Hit"           # optional label/tag for readability
            },
            ...
          ]
        """
        import tkinter as tk
        from tkinter import ttk, Toplevel, Frame, StringVar, BooleanVar

        # ----------------------------------------------------------
        # helpers
        def _safe_int(s, default=0):
            try:
                return int(float(str(s).strip()))
            except Exception:
                return default

        def _safe_float(s, default=0.0):
            try:
                return float(str(s).strip())
            except Exception:
                return default

        def _clamp(v, lo, hi):
            return max(lo, min(hi, v))

        # muscle display map (id -> name)
        MUSCLE_CHOICES = [
            ("0", "Abdominal_L"),
            ("1", "Abdominal_R"),
            ("2", "Pectoral_L"),
            ("3", "Pectoral_R"),
            ("4", "Dorsal_L"),
            ("5", "Dorsal_R"),
            ("6", "Arm_L"),
            ("7", "Arm_R"),
            ("8", "Lumbar_L"),
            ("9", "Lumbar_R"),
        ]

        def _format_muscle_preview(muscles_list):
            # muscles_list: [{"id":"7","pct":100}, ...]
            parts = []
            for m in muscles_list or []:
                mid = str(m.get("id", "")).strip()
                pct = _safe_int(m.get("pct", 0), 0)
                if mid and 0 <= pct <= 100:
                    parts.append(f"{mid}%{pct}")
            return ",".join(parts)

        # ----------------------------------------------------------
        # window
        editor = Toplevel(self)
        self.apply_theme_to_toplevel(editor)
        editor.title("OWO Pattern Editor")
        editor.geometry("860x620")
        editor.minsize(760, 520)
        editor.grab_set()

        editor.grid_rowconfigure(0, weight=1)
        editor.grid_columnconfigure(0, weight=1)

        step_rows = []

        shell = Frame(editor, bg=BG)
        shell.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        shell.grid_rowconfigure(2, weight=1)
        shell.grid_columnconfigure(0, weight=1)

        # ----------------------------------------------------------
        # header
        hdr = Frame(shell, bg=BG)
        hdr.grid(row=0, column=0, sticky="ew", pady=(0, 6))

        # 0 Sel | 1 Step | 2 Label | 3 Freq | 4 Dur | 5 Int | 6 FadeIn | 7 FadeOut | 8 Delay | 9 Muscles | 10 Del
        headers = ["", "Step", "Label", "Hz", "Dur (s)", "Int (0-100)", "Fade In (ms)", "Fade Out (ms)", "Delay (s)", "Muscles", ""]
        col_widths = {0: 34, 1: 50, 2: 120, 3: 60, 4: 90, 5: 100, 6: 110, 7: 120, 8: 90, 9: 220, 10: 44}

        for c, w in col_widths.items():
            hdr.grid_columnconfigure(c, minsize=w, weight=(1 if c in (2, 9) else 0))
        hdr.grid_columnconfigure(9, weight=1)

        for col, text in enumerate(headers):
            lbl = ttk.Label(hdr, text=text, style="Header.TLabel")
            lbl.grid(row=0, column=col, sticky="nsew", padx=4)

        # ----------------------------------------------------------
        # toolbar
        topbar = Frame(shell, bg=BG)
        topbar.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        topbar.grid_columnconfigure(0, weight=1)

        info_lbl = ttk.Label(
            topbar,
            text="OwO SDK pattern steps. Select rows → Delete Selected. Ctrl+S saves.",
            style="HeaderAddr.TLabel"
        )
        info_lbl.grid(row=0, column=0, sticky="w", padx=(0, 8))

        actions = Frame(topbar, bg=BG)
        actions.grid(row=0, column=1, sticky="e")

        # ----------------------------------------------------------
        # scroll body
        scroll_frame = Frame(shell, bg=BG)
        scroll_frame.grid(row=2, column=0, sticky="nsew")
        scroll_frame.grid_rowconfigure(0, weight=1)
        scroll_frame.grid_columnconfigure(0, weight=1)

        canvas = tk.Canvas(scroll_frame, bg=BG, highlightthickness=0, bd=0)
        canvas.grid(row=0, column=0, sticky="nsew")

        vscroll = ttk.Scrollbar(scroll_frame, orient="vertical", command=canvas.yview)
        vscroll.grid(row=0, column=1, sticky="ns")
        canvas.configure(yscrollcommand=vscroll.set)

        table = Frame(canvas, bg=BG)
        table_window = canvas.create_window((0, 0), window=table, anchor="nw")

        for c, w in col_widths.items():
            table.grid_columnconfigure(c, minsize=w, weight=(1 if c in (2, 9) else 0))
        table.grid_columnconfigure(9, weight=1)

        def _update_scroll_region(_=None):
            canvas.configure(scrollregion=canvas.bbox("all"))

        def _sync_table_width(event):
            canvas.itemconfigure(table_window, width=event.width)

        table.bind("<Configure>", _update_scroll_region)
        canvas.bind("<Configure>", _sync_table_width)

        def _on_mousewheel(e):
            try:
                canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
            except Exception:
                pass

        canvas.bind_all("<MouseWheel>", _on_mousewheel)

        # ----------------------------------------------------------
        # row utilities
        def _renumber_steps():
            for i, r in enumerate(step_rows, start=1):
                r["step_var"].set(str(i))

        def _destroy_row_widgets(r):
            for w in r["widgets"]:
                try:
                    w.destroy()
                except Exception:
                    pass

        def _rebuild_grid_positions():
            for row_idx, r in enumerate(step_rows, start=0):
                r["row_index"] = row_idx
                r["sel_cb"].grid(row=row_idx, column=0, padx=4, pady=2, sticky="nsew")
                r["e_step"].grid(row=row_idx, column=1, padx=4, pady=2, sticky="nsew")
                r["e_lbl"].grid(row=row_idx, column=2, padx=4, pady=2, sticky="nsew")
                r["e_hz"].grid(row=row_idx, column=3, padx=4, pady=2, sticky="nsew")
                r["e_dur"].grid(row=row_idx, column=4, padx=4, pady=2, sticky="nsew")
                r["e_int"].grid(row=row_idx, column=5, padx=4, pady=2, sticky="nsew")
                r["e_fi"].grid(row=row_idx, column=6, padx=4, pady=2, sticky="nsew")
                r["e_fo"].grid(row=row_idx, column=7, padx=4, pady=2, sticky="nsew")
                r["e_dly"].grid(row=row_idx, column=8, padx=4, pady=2, sticky="nsew")
                r["mus_btn"].grid(row=row_idx, column=9, padx=4, pady=2, sticky="nsew")
                r["del_btn"].grid(row=row_idx, column=10, padx=4, pady=2, sticky="nsew")

            _renumber_steps()
            _update_scroll_region()

        def _delete_selected():
            keep = []
            for r in step_rows:
                if r["sel_var"].get():
                    _destroy_row_widgets(r)
                else:
                    keep.append(r)
            step_rows[:] = keep
            _rebuild_grid_positions()

        def _delete_one(row_ref):
            idx = None
            for i, r in enumerate(step_rows):
                if r is row_ref:
                    idx = i
                    break
            if idx is None:
                return
            _destroy_row_widgets(step_rows[idx])
            del step_rows[idx]
            _rebuild_grid_positions()

        # ----------------------------------------------------------
        # muscle picker popup (per-row)
        def _open_muscle_picker(row_obj):
            pop = Toplevel(editor)
            self.apply_theme_to_toplevel(pop)
            pop.title("Select Muscles")
            pop.geometry("420x420")
            pop.minsize(380, 360)
            pop.grab_set()

            wrap = Frame(pop, bg=BG)
            wrap.pack(fill="both", expand=True, padx=10, pady=10)

            tk.Label(
                wrap,
                text="Per-muscle intensity (%). These are multiplied against the step base intensity.",
                bg=BG,
                fg=FG,
                wraplength=380,
                justify="left"
            ).pack(anchor="w", pady=(0, 8))

            # working copy
            current = list(row_obj.get("muscles", []) or [])
            by_id = {str(m.get("id")): _safe_int(m.get("pct", 0), 0) for m in current if m.get("id") is not None}

            rows = []
            for mid, name in MUSCLE_CHOICES:
                r = Frame(wrap, bg=BG)
                r.pack(fill="x", pady=2)

                enabled = BooleanVar(value=mid in by_id)
                pct_var = StringVar(value=str(by_id.get(mid, 100)))

                ttk.Checkbutton(r, text=f"{name}  ({mid})", variable=enabled).pack(side="left", padx=(0, 8))
                ttk.Entry(r, textvariable=pct_var, width=6, justify="center").pack(side="left")

                tk.Label(r, text="%", bg=BG, fg=FG).pack(side="left", padx=(4, 0))

                rows.append((mid, enabled, pct_var))

            btns = Frame(wrap, bg=BG)
            btns.pack(fill="x", pady=(10, 0))

            def _apply():
                out = []
                for mid, enabled, pct_var in rows:
                    if not enabled.get():
                        continue
                    pct = _clamp(_safe_int(pct_var.get(), 100), 0, 100)
                    out.append({"id": mid, "pct": pct})

                # Store into row
                row_obj["muscles"] = out

                # Update preview label
                row_obj["mus_preview_var"].set(_format_muscle_preview(out) or "(none)")

                pop.destroy()

            ttk.Button(btns, text="Apply", command=_apply).pack(side="right")

        # ----------------------------------------------------------
        # row factory
        def add_step_row(data=None):
            data = data or {}

            step_var = StringVar(value=str(len(step_rows) + 1))
            sel_var  = BooleanVar(value=False)

            label = str(data.get("label", "") or "").strip()
            lbl_var = StringVar(value=label)

            hz = _clamp(_safe_int(data.get("frequency", 60), 60), 1, 200)
            hz_var = StringVar(value=str(hz))

            dur = _clamp(_safe_float(data.get("duration", 1.0), 1.0), 0.05, 60.0)
            dur_var = StringVar(value=str(round(dur, 3)).rstrip("0").rstrip("."))

            inten = _clamp(_safe_int(data.get("intensity", 70), 70), 0, 100)
            int_var = StringVar(value=str(inten))

            fi = _clamp(_safe_int(data.get("fade_in_ms", 0), 0), 0, 5000)
            fi_var = StringVar(value=str(fi))

            fo = _clamp(_safe_int(data.get("fade_out_ms", 0), 0), 0, 5000)
            fo_var = StringVar(value=str(fo))

            dly = _clamp(_safe_float(data.get("delay_s", 0.0), 0.0), 0.0, 60.0)
            dly_var = StringVar(value=str(round(dly, 3)).rstrip("0").rstrip("."))

            muscles_list = data.get("muscles") if isinstance(data.get("muscles"), list) else []
            mus_preview_var = StringVar(value=_format_muscle_preview(muscles_list) or "(none)")

            sel_cb = ttk.Checkbutton(table, variable=sel_var)
            e_step = ttk.Entry(table, width=4, textvariable=step_var, state="readonly", justify="center")

            e_lbl  = ttk.Entry(table, width=12, textvariable=lbl_var, justify="center")
            e_hz   = ttk.Entry(table, width=6,  textvariable=hz_var,  justify="center")
            e_dur  = ttk.Entry(table, width=9,  textvariable=dur_var, justify="center")
            e_int  = ttk.Entry(table, width=9,  textvariable=int_var, justify="center")
            e_fi   = ttk.Entry(table, width=10, textvariable=fi_var,  justify="center")
            e_fo   = ttk.Entry(table, width=10, textvariable=fo_var,  justify="center")
            e_dly  = ttk.Entry(table, width=9,  textvariable=dly_var, justify="center")

            mus_btn = ttk.Button(table, textvariable=mus_preview_var)
            del_btn = ttk.Button(table, text="✕", width=3)

            row_obj = {
                "row_index": len(step_rows),
                "sel_var": sel_var,
                "step_var": step_var,

                "lbl_var": lbl_var,
                "hz_var": hz_var,
                "dur_var": dur_var,
                "int_var": int_var,
                "fi_var": fi_var,
                "fo_var": fo_var,
                "dly_var": dly_var,

                "muscles": muscles_list,
                "mus_preview_var": mus_preview_var,

                "sel_cb": sel_cb,
                "e_step": e_step,
                "e_lbl": e_lbl,
                "e_hz": e_hz,
                "e_dur": e_dur,
                "e_int": e_int,
                "e_fi": e_fi,
                "e_fo": e_fo,
                "e_dly": e_dly,
                "mus_btn": mus_btn,
                "del_btn": del_btn,

                "widgets": (sel_cb, e_step, e_lbl, e_hz, e_dur, e_int, e_fi, e_fo, e_dly, mus_btn, del_btn),
            }

            mus_btn.configure(command=lambda r=row_obj: _open_muscle_picker(r))
            del_btn.configure(command=lambda r=row_obj: _delete_one(r))

            step_rows.append(row_obj)
            _rebuild_grid_positions()

        # ----------------------------------------------------------
        # glow buttons
        def _make_glow(parent, text, cmd, *, w=160):
            gb = GlowButton(
                parent,
                text=text,
                command=cmd,
                width=w,
                height=34,
                bg=PANEL,
                glow=BUTTON,
                fg=FG,
                anchor="center"
            )
            gb.pack(side="left", padx=6)
            return gb

        _make_glow(actions, "Add Step", lambda: add_step_row({}), w=140)
        _make_glow(actions, "Delete Selected", _delete_selected, w=170)

        # preload
        for entry in (cfg.get("owo_pattern") or []):
            if isinstance(entry, dict):
                add_step_row(entry)

        if not step_rows:
            add_step_row({})

        # ----------------------------------------------------------
        # footer
        footer = Frame(shell, bg=BG)
        footer.grid(row=3, column=0, sticky="ew", pady=(10, 0))
        footer.grid_columnconfigure(0, weight=1)

        right_footer = Frame(footer, bg=BG)
        right_footer.grid(row=0, column=0, sticky="e")

        def save_and_close():
            out = []
            _renumber_steps()

            for i, r in enumerate(step_rows, start=1):
                try:
                    freq = _clamp(_safe_int(r["hz_var"].get(), 60), 1, 200)
                    dur  = _clamp(_safe_float(r["dur_var"].get(), 1.0), 0.05, 60.0)
                    base = _clamp(_safe_int(r["int_var"].get(), 70), 0, 100)
                    fi   = _clamp(_safe_int(r["fi_var"].get(), 0), 0, 5000)
                    fo   = _clamp(_safe_int(r["fo_var"].get(), 0), 0, 5000)
                    dly  = _clamp(_safe_float(r["dly_var"].get(), 0.0), 0.0, 60.0)

                    label = (r["lbl_var"].get() or "").strip()

                    muscles = []
                    for m in (r.get("muscles") or []):
                        mid = str(m.get("id", "")).strip()
                        pct = _clamp(_safe_int(m.get("pct", 0), 0), 0, 100)
                        if mid in dict(MUSCLE_CHOICES):
                            muscles.append({"id": mid, "pct": pct})

                    out.append({
                        "step": i,
                        "frequency": int(freq),
                        "duration": float(dur),
                        "intensity": int(base),
                        "fade_in_ms": int(fi),
                        "fade_out_ms": int(fo),
                        "delay_s": float(dly),
                        "muscles": muscles,
                        "label": label,
                    })
                except Exception:
                    continue

            cfg["owo_pattern"] = out
            editor.destroy()

        gb_save = GlowButton(
            right_footer,
            text="Save Pattern",
            command=save_and_close,
            width=170,
            height=36,
            bg=PANEL,
            glow=ACCENT,
            fg=FG
        )
        gb_save.pack(side="right", padx=6)

        editor.bind("<Delete>", lambda e: _delete_selected())
        editor.bind("<Control-n>", lambda e: add_step_row({}))
        editor.bind("<Control-s>", lambda e: save_and_close())

# ────────────────────────────────────────────────────────────────────────────────
# Chain UI Utility Function
# ────────────────────────────────────────────────────────────────────────────────
    def _chain_selector(self, initial_steps=None, initial_config=None):
        import difflib, tkinter as tk, threading
        from tkinter import ttk, messagebox

        # ── Prevent multiple editors at once ─────────────────────────────
        if getattr(self, "_chain_editor_win", None):
            try:
                if self._chain_editor_win.winfo_exists():
                    self._chain_editor_win.lift()
                    self._chain_editor_win.focus_force()
                    return None, None
            except tk.TclError:
                self._chain_editor_win = None

        # ── Availability check ──────────────────────────────────────────
        available = list(self.rows.keys())
        if not available:
            log_chain_system(
                "No OSC controls available to configure in chain editor.",
                level="WARN",
                action_type="editor"
            )
            log_gui_action(
                "User attempted to open chain editor with no OSC controls present.",
                level="WARN"
            )
            available = []

        # ── Build window ────────────────────────────────────────────────
        win = tk.Toplevel(self)
        self._chain_editor_win = win

        win.withdraw()
        win.title("Chain Configuration")

        # Apply theme before geometry
        self.apply_theme_to_toplevel(win)
        win.update_idletasks()

        # Restore geometry
        geom = self._load_window_geometry("chain_editor")
        try:
            win.geometry(geom if geom else "736x370")
        except Exception as e:
            log_gui_action(
                "Failed to restore chain editor geometry",
                level="WARN",
                exc=e
            )
            win.geometry("736x370")

        win.deiconify()
        win.lift()
        win.focus_force()
        win.grab_set()

        # ── Window close guard ──────────────────────────────────────────
        def _on_close():
            try:
                geom = win.geometry()
                self._save_window_geometry("chain_editor", geom)
                log_gui_action(
                    "Saved chain editor geometry",
                    data={"geometry": geom}
                )
            finally:
                self._chain_editor_win = None
                win.grab_release()
                win.destroy()

        win.protocol("WM_DELETE_WINDOW", _on_close)

        log_chain_system(
            "Opened chain editor UI.",
            level="INFO",
            action_type="editor"
        )
        log_gui_action(
            "Chain editor window opened.",
            level="INFO"
        )

        # ────────────────────────────────────────────────────────────────
        # Global scroll root
        # ────────────────────────────────────────────────────────────────
        _, content = build_scrollable_root(win, BG)

        # ALL UI MUST ATTACH TO content FROM HERE ON
        settings_frame = tk.Frame(content, bg=BG)
        settings_frame.pack(fill="x", expand=True, padx=10, pady=10)
        
    # ──────────────────────────────────────────────────────────────────────

        log_chain_system("Opened chain editor UI.", level="INFO", action_type="editor")
        log_gui_action("Chain editor window opened.", level="INFO")

        def _fix_nested_steps(steps):
            if isinstance(steps, list) and len(steps) == 2:
                inner, maybe_cfg = steps
                if isinstance(inner, list) and isinstance(maybe_cfg, dict) and all(isinstance(s, dict) for s in inner):
                    return inner
            return steps

        fixed_steps = _fix_nested_steps(initial_steps or [])
        cfg = dict(initial_config or {})
        self._gift_mapping = getattr(self, "_gift_mapping", {}) or self._load_gift_mapping()
        steps_map = {s["addr"]: s.copy() for s in fixed_steps if isinstance(s, dict) and "addr" in s}
        selected = {}

        log_chain_system("Loaded initial chain steps and config.", data={
            "step_count": len(fixed_steps),
            "config_keys": list(cfg.keys())
        }, level="DEBUG", action_type="editor")

        # Config Vars
        name_var = tk.StringVar(value=cfg.get("name", ""))
        delay_var = tk.StringVar(value=str(cfg.get("delay", 0.025)))
        rb = tk.BooleanVar(value=cfg.get("reset_before", False))
        ra = tk.BooleanVar(value=cfg.get("reset_after", False))
        trig_var = tk.StringVar(value=cfg.get("trigger", "manual"))
        qm = tk.BooleanVar(value=cfg.get("queue_mode", True))
        priority_var = tk.StringVar(value=str(cfg.get("priority", 100)))
        delay_min_var = tk.StringVar(value=str(cfg.get("delay_min", "0.1")))
        delay_max_var = tk.StringVar(value=str(cfg.get("delay_max", "0.5")))
        step_limit_var = tk.StringVar(value=str(cfg.get("step_limit", "")))
        repeat_var = tk.BooleanVar(value=cfg.get("repeat", False))
        gift_name = tk.StringVar(value=cfg.get("giftName", ""))
        gift_count = tk.StringVar(value=str(cfg.get("giftCount", 1)))
        diamond_min = tk.StringVar(value=str(cfg.get("diamondCountMin", "")))
        diamond_max = tk.StringVar(value=str(cfg.get("diamondCountMax", "")))
        gifter_min = tk.StringVar(value=str(cfg.get("gifterLevelMin", "")))
        gifter_max = tk.StringVar(value=str(cfg.get("gifterLevelMax", "")))
        is_sub_var = tk.BooleanVar(value=cfg.get("isSubscriber", False))
        is_mod_var = tk.BooleanVar(value=cfg.get("isModerator", False))
        custom_diamond_trigger = tk.StringVar(value=str(cfg.get("diamondCountTrigger", "")))

        pishock_enabled = tk.BooleanVar(value=cfg.get("pishock_enabled", False))
        pishock_only    = tk.BooleanVar(value=cfg.get("pishock_only",    False))  # ⚡ NEW
        pishock_random  = tk.BooleanVar(value=cfg.get("pishock_random_devices", False))  # 🆕 NEW
        pulse_delay_var = tk.StringVar(value=str(cfg.get("pishock_delay", "750")))
        osc_step_var    = tk.StringVar(value=str(cfg.get("pishock_step", "10")))
        mode_var = tk.StringVar(value=cfg.get("pishock_mode", "vibrate"))
        intensity_var = tk.StringVar(value=str(cfg.get("pishock_intensity", 50)))
        duration_var = tk.StringVar(value=str(cfg.get("pishock_duration", 1000) / 1000))  # ← now reads in seconds
        self._selected_device_vars = {}


        # ─────────────────────────────────────────────────────────────
        # INTIFACE VARS (AUTHORITATIVE STATE)
        # ─────────────────────────────────────────────────────────────

        # Core toggles
        intiface_enabled = tk.BooleanVar(value=bool(cfg.get("intiface_enabled", False)))
        intiface_only    = tk.BooleanVar(value=bool(cfg.get("intiface_only", False)))
        intiface_random  = tk.BooleanVar(value=bool(cfg.get("intiface_random_devices", False)))

        # ── Mode ─────────────────────────────────────────────
        VALID_INTIFACE_MODES = (
            "vibrate",
            "pulse",
            "burst",
            "oscillation",
            "randomized_wobble",
            "pattern",
        )

        raw_mode = str(cfg.get("intiface_mode", "vibrate")).lower()
        if raw_mode not in VALID_INTIFACE_MODES:
            raw_mode = "vibrate"
        intiface_mode_var = tk.StringVar(value=raw_mode)

        # ── Intensity (% UI → normalized later)
        raw_intensity = cfg.get("intiface_intensity", 0.75)
        try:
            percent_intensity = int(float(raw_intensity) * 100)
        except Exception:
            percent_intensity = 75
        percent_intensity = max(0, min(100, percent_intensity))
        intiface_intensity_var = tk.StringVar(value=str(percent_intensity))

        # ── Duration (seconds)
        raw_duration = cfg.get("intiface_duration", 1.0)
        try:
            duration_sec = float(raw_duration)
        except Exception:
            duration_sec = 1.0
        duration_sec = max(0.05, min(1800.0, duration_sec))
        intiface_duration_var = tk.StringVar(value=str(round(duration_sec, 2)))

        # ── Oscillation step
        raw_step = cfg.get("intiface_step", 10)
        try:
            step_val = int(raw_step)
        except Exception:
            step_val = 10
        step_val = max(1, min(50, step_val))
        intiface_step_var = tk.StringVar(value=str(step_val))

        # ── Randomized wobble
        raw_rand_min = cfg.get("intiface_random_min", 0.2)
        raw_rand_max = cfg.get("intiface_random_max", 1.0)
        try:
            rand_min = float(raw_rand_min)
            rand_max = float(raw_rand_max)
        except Exception:
            rand_min, rand_max = 0.2, 1.0

        rand_min = max(0.0, min(1.0, rand_min))
        rand_max = max(rand_min, min(1.0, rand_max))

        intiface_random_min_var = tk.StringVar(value=str(int(rand_min * 100)))
        intiface_random_max_var = tk.StringVar(value=str(int(rand_max * 100)))

        # ── Pattern system
        pattern = cfg.get("intiface_pattern", [])
        if not isinstance(pattern, list):
            pattern = []
        cfg["intiface_pattern"] = pattern

        # Pattern key (dropdown selection)
        pattern_key = cfg.get("intiface_pattern_key", "Default")
        if not isinstance(pattern_key, str) or not pattern_key:
            pattern_key = "Default"

        self._intiface_pattern_key_var = tk.StringVar(value=pattern_key)

        # Pattern library
        if not isinstance(cfg.get("intiface_patterns"), dict):
            cfg["intiface_patterns"] = {}

        if pattern_key not in cfg["intiface_patterns"]:
            cfg["intiface_patterns"][pattern_key] = pattern

        # ── Per-device state (populated later)
        self._selected_intiface_device_vars = {}   # str(id) → BooleanVar
        self._intiface_device_actuators = {}       # int(id) → StringVar


        # ─── OWO Vest Vars ─────────────────────────────────────────────

        # Master enable
        owo_enabled = tk.BooleanVar(value=bool(cfg.get("owo_enabled", False)))

        # Template / Pattern mode
        # "template" = .owo file
        # "pattern"  = pattern editor
        owo_pattern_mode = tk.StringVar(
            value=str(cfg.get("owo_pattern_mode", "template")).lower()
        )

        # Selected template file
        owo_file_var = tk.StringVar(
            value=str(cfg.get("owo_file", "")).strip()
        )

        # Queue behavior (respects chain scheduler)
        owo_queue_mode = tk.BooleanVar(
            value=bool(cfg.get("owo_queue", True))
        )

        
        # ─── VRChat OSC Vars ───
        osc_enabled = tk.BooleanVar(value=False)
        
        # Split layout
        left = tk.Frame(settings_frame, bg=BG)
        right = tk.Frame(settings_frame, bg=BG)
        left.pack(side="left", fill="both", expand=True, padx=(0, 5))
        right.pack(side="right", fill="both", expand=True, padx=(5, 0))

        def labeled_entry(parent, label, var, row, tooltip, width=20):
            l = tk.Label(parent, text=label, bg=BG, fg=FG)
            l.grid(row=row, column=0, sticky="w")
            e = ttk.Entry(parent, textvariable=var, width=width)
            e.grid(row=row, column=1, sticky="w")
            ToolTip(l, tooltip)
            ToolTip(e, tooltip)

        labeled_entry(left, "Name:", name_var, 0, "Friendly name for this chain.")
        labeled_entry(left, "Delay (s):", delay_var, 1, "Time delay between each step.")
        labeled_entry(left, "Priority:", priority_var, 2, "Lower value means higher priority.")

        rb_check = ttk.Checkbutton(left, text="Reset Before", variable=rb)
        rb_check.grid(row=3, column=0, sticky="w")
        ToolTip(rb_check, "Reset all OSC values before this chain starts.")

        ra_check = ttk.Checkbutton(left, text="Reset After", variable=ra)
        ra_check.grid(row=3, column=1, sticky="w")
        ToolTip(ra_check, "Reset all OSC values after this chain finishes.")

        qm_check = ttk.Checkbutton(left, text="Queue Mode", variable=qm)
        qm_check.grid(row=4, column=0, sticky="w")
        ToolTip(qm_check, "Queue this chain to run after the previous one completes.")

        tk.Label(left, text="Trigger:", bg=BG, fg=FG).grid(row=5, column=0, sticky="w")
        trig_box = ttk.Combobox(left, textvariable=trig_var, values=["manual", "gift", "subscribe", "random", "custom_diamonds"], state="readonly")
        trig_box.grid(row=5, column=1, sticky="w")
        ToolTip(trig_box, "Select how this chain is triggered.")

        # Gift UI
        gift_frame = tk.Frame(left, bg=BG)

        def layout_gift(*_):
            gift_frame.grid_forget()
            if trig_var.get() == "gift":
                log_chain_system("Gift trigger selected in chain editor.", level="DEBUG", action_type="editor")
                repeat_var.set(cfg.get("allowRepeat", False))
                gift_frame.grid(row=6, column=0, columnspan=2, sticky="w", pady=4)

                lbl_dmin = tk.Label(gift_frame, text="Diamond ≥", bg=BG, fg=FG)
                lbl_dmin.grid(row=2, column=0, sticky="w")
                ent_dmin = ttk.Entry(gift_frame, textvariable=diamond_min, width=6)
                ent_dmin.grid(row=2, column=1, sticky="w", padx=(0, 6))
                ToolTip(ent_dmin, "Minimum number of diamonds required to trigger this chain.")

                lbl_dmax = tk.Label(gift_frame, text="Diamond ≤", bg=BG, fg=FG)
                lbl_dmax.grid(row=2, column=2, sticky="w")
                ent_dmax = ttk.Entry(gift_frame, textvariable=diamond_max, width=6)
                ent_dmax.grid(row=2, column=3, sticky="w")
                ToolTip(ent_dmax, "Maximum number of diamonds allowed to trigger this chain.")

                lbl_lmin = tk.Label(gift_frame, text="Level ≥", bg=BG, fg=FG)
                lbl_lmin.grid(row=3, column=0, sticky="w")
                ent_lmin = ttk.Entry(gift_frame, textvariable=gifter_min, width=6)
                ent_lmin.grid(row=3, column=1, sticky="w", padx=(0, 6))
                ToolTip(ent_lmin, "Minimum gifter level required to trigger.")

                lbl_lmax = tk.Label(gift_frame, text="Level ≤", bg=BG, fg=FG)
                lbl_lmax.grid(row=3, column=2, sticky="w")
                ent_lmax = ttk.Entry(gift_frame, textvariable=gifter_max, width=6)
                ent_lmax.grid(row=3, column=3, sticky="w")
                ToolTip(ent_lmax, "Maximum gifter level allowed to trigger.")

                sub_check = ttk.Checkbutton(gift_frame, text="Subscriber", variable=is_sub_var)
                sub_check.grid(row=4, column=0, columnspan=2, sticky="w")
                ToolTip(sub_check, "Only allow subscribers to trigger this chain.")

                mod_check = ttk.Checkbutton(gift_frame, text="Moderator", variable=is_mod_var)
                mod_check.grid(row=4, column=2, columnspan=2, sticky="w")
                ToolTip(mod_check, "Only allow moderators to trigger this chain.")

                lbl_gift = tk.Label(gift_frame, text="Gift:", bg=BG, fg=FG)
                lbl_gift.grid(row=0, column=0)
                gift_entry = AutoCompleteCombobox(gift_frame, textvariable=gift_name, width=25)
                gift_entry.set_completion_list(self._gift_mapping)
                gift_entry.grid(row=0, column=1)
                ToolTip(gift_entry, "Name of the gift that triggers the chain.")

                lbl_count = tk.Label(gift_frame, text="Count:", bg=BG, fg=FG)
                lbl_count.grid(row=0, column=2)
                ent_count = ttk.Entry(gift_frame, textvariable=gift_count, width=5)
                ent_count.grid(row=0, column=3)
                ToolTip(ent_count, "Number of gifts required to trigger.")

                icon_preview = tk.Label(gift_frame, bg=BG)
                icon_preview.grid(row=0, column=4, padx=(8, 0))
                ToolTip(icon_preview, "Preview of the selected gift icon.")

                repeat_check = ttk.Checkbutton(gift_frame, text="Allow Repeat Mode", variable=repeat_var)
                repeat_check.grid(row=1, column=0, columnspan=4, sticky="w", pady=(4, 0))
                ToolTip(repeat_check, "Allow chain to trigger repeatedly for the same gift event.")

                # cache to avoid repeated downloads
                _icon_cache = {}

                def update_icon(*_):
                    name = gift_name.get().strip()
                    gift = self._gift_mapping.get(name)

                    if not gift or not gift.get("image"):
                        icon_preview.configure(image="")
                        icon_preview.image = None
                        return

                    url = gift["image"]

                    # ---- HARD SAFETY GATES ----
                    if not url.startswith("https://"):
                        return

                    from urllib.parse import urlparse
                    host = urlparse(url).hostname

                    ALLOWED_HOSTS = {
                        "p16-sign.tiktokcdn.com",
                        "p16-tiktokcdn.com",
                        "p19-sign.tiktokcdn.com",
                        "p77-sign.tiktokcdn.com",
                    }

                    if host not in ALLOWED_HOSTS:
                        return

                    # ---- CACHE HIT ----
                    if url in _icon_cache:
                        icon_preview.configure(image=_icon_cache[url])
                        icon_preview.image = _icon_cache[url]
                        return

                    try:
                        from urllib.request import Request, urlopen
                        from PIL import Image, ImageTk
                        import io

                        req = Request(
                            url,
                            headers={"User-Agent": "StreamConnector/1.0"}
                        )

                        with urlopen(req, timeout=3) as resp:
                            raw = resp.read(256 * 1024)  # 256KB max

                        img = Image.open(io.BytesIO(raw))
                        img = img.resize((24, 24), Image.LANCZOS)

                        tkimg = ImageTk.PhotoImage(img)

                        _icon_cache[url] = tkimg
                        icon_preview.configure(image=tkimg)
                        icon_preview.image = tkimg

                    except Exception as e:
                        icon_preview.configure(image="")
                        icon_preview.image = None
                        log_gui_action(
                            "Failed to load gift icon",
                            level="WARN",
                            data={"url": url},
                            exc=e
                        )

                # ---- SAFE TRACE BINDING ----

                def _on_gift_name_change(*_):
                    try:
                        job = getattr(icon_preview, "_icon_job", None)
                        if job:
                            icon_preview.after_cancel(job)
                    except Exception:
                        pass  # widget might be destroyed, ignore safely

                    # debounce network activity
                    icon_preview._icon_job = icon_preview.after(200, update_icon)


                gift_name.trace_add("write", _on_gift_name_change)

                # DO NOT force immediate download on startup
                # Let the UI settle first
                icon_preview.after(300, update_icon)

        # Attach and run layout logic for gift UI
        trig_var.trace_add("write", layout_gift)
        layout_gift()

        # Random frame
        random_frame = tk.Frame(left, bg=BG)

        def layout_random(*_):
            random_frame.grid_forget()
            if trig_var.get() == "random":
                log_chain_system("Random trigger selected in chain editor.", level="DEBUG", action_type="editor")
                random_frame.grid(row=7, column=0, columnspan=2, sticky="w", pady=4)

            delay_min_label = tk.Label(random_frame, text="Min Delay:", bg=BG, fg=FG)
            delay_min_label.grid(row=0, column=0)
            delay_min_entry = ttk.Entry(random_frame, textvariable=delay_min_var, width=6)
            delay_min_entry.grid(row=0, column=1)
            ToolTip(delay_min_label, "Minimum delay between random triggers.")
            ToolTip(delay_min_entry, "Minimum delay between random triggers.")

            delay_max_label = tk.Label(random_frame, text="Max Delay:", bg=BG, fg=FG)
            delay_max_label.grid(row=0, column=2)
            delay_max_entry = ttk.Entry(random_frame, textvariable=delay_max_var, width=6)
            delay_max_entry.grid(row=0, column=3)
            ToolTip(delay_max_label, "Maximum delay between random triggers.")
            ToolTip(delay_max_entry, "Maximum delay between random triggers.")

            step_limit_label = tk.Label(random_frame, text="Step Limit:", bg=BG, fg=FG)
            step_limit_label.grid(row=1, column=0)
            step_limit_entry = ttk.Entry(random_frame, textvariable=step_limit_var, width=6)
            step_limit_entry.grid(row=1, column=1)
            ToolTip(step_limit_label, "Max number of steps to trigger in random mode.")
            ToolTip(step_limit_entry, "Max number of steps to trigger in random mode.")

            random_repeat_check = ttk.Checkbutton(random_frame, text="Repeat", variable=repeat_var)
            random_repeat_check.grid(row=1, column=2, sticky="w")
            ToolTip(random_repeat_check, "Repeat this chain in random mode.")

        trig_var.trace_add("write", layout_random)
        layout_random()

        custom_frame = tk.Frame(left, bg=BG)

        def layout_custom(*_):
            custom_frame.grid_forget()
            if trig_var.get() == "custom_diamonds":
                log_chain_system("Custom diamond trigger selected.", level="DEBUG", action_type="editor")
                custom_frame.grid(row=8, column=0, columnspan=2, sticky="w", pady=4)

                # Trigger Threshold
                lbl_thresh = tk.Label(custom_frame, text="Diamonds ≥", bg=BG, fg=FG)
                lbl_thresh.grid(row=0, column=0, sticky="w")
                ent_thresh = ttk.Entry(custom_frame, textvariable=custom_diamond_trigger, width=10)
                ent_thresh.grid(row=0, column=1, sticky="w")
                ToolTip(ent_thresh, "Minimum total diamonds needed to trigger this chain.")
                ToolTip(custom_frame, "This chain triggers once the total diamond count meets this threshold.")

                # Optional Filters Label
                lbl_filter = tk.Label(custom_frame, text="Gift Filters (optional):", bg=BG, fg="lightgrey")
                lbl_filter.grid(row=1, column=0, columnspan=2, sticky="w", pady=(10, 2))

                # Diamond Min
                lbl_dmin = tk.Label(custom_frame, text="Diamond ≥", bg=BG, fg=FG)
                lbl_dmin.grid(row=2, column=0, sticky="w")
                ent_dmin = ttk.Entry(custom_frame, textvariable=diamond_min, width=6)
                ent_dmin.grid(row=2, column=1, sticky="w", padx=(0, 6))
                ToolTip(ent_dmin, "Minimum number of diamonds for filters.")

                # Diamond Max
                lbl_dmax = tk.Label(custom_frame, text="Diamond ≤", bg=BG, fg=FG)
                lbl_dmax.grid(row=2, column=2, sticky="w")
                ent_dmax = ttk.Entry(custom_frame, textvariable=diamond_max, width=6)
                ent_dmax.grid(row=2, column=3, sticky="w")
                ToolTip(ent_dmax, "Maximum number of diamonds for filters.")

                # Gifter Level Min
                lbl_lmin = tk.Label(custom_frame, text="Level ≥", bg=BG, fg=FG)
                lbl_lmin.grid(row=3, column=0, sticky="w")
                ent_lmin = ttk.Entry(custom_frame, textvariable=gifter_min, width=6)
                ent_lmin.grid(row=3, column=1, sticky="w", padx=(0, 6))
                ToolTip(ent_lmin, "Minimum gifter level for filters.")

                # Gifter Level Max
                lbl_lmax = tk.Label(custom_frame, text="Level ≤", bg=BG, fg=FG)
                lbl_lmax.grid(row=3, column=2, sticky="w")
                ent_lmax = ttk.Entry(custom_frame, textvariable=gifter_max, width=6)
                ent_lmax.grid(row=3, column=3, sticky="w")
                ToolTip(ent_lmax, "Maximum gifter level for filters.")

                # Subscriber Checkbox
                custom_sub_check = ttk.Checkbutton(custom_frame, text="Subscriber", variable=is_sub_var)
                custom_sub_check.grid(row=4, column=0, columnspan=2, sticky="w")
                ToolTip(custom_sub_check, "Only allow subscribers to trigger (filter).")

                # Moderator Checkbox
                custom_mod_check = ttk.Checkbutton(custom_frame, text="Moderator", variable=is_mod_var)
                custom_mod_check.grid(row=4, column=2, columnspan=2, sticky="w")
                ToolTip(custom_mod_check, "Only allow moderators to trigger (filter).")

        trig_var.trace_add("write", layout_custom)
        layout_custom()

        # ── Subscribe frame ────────────────────────────────────────────────
        subscribe_frame = tk.Frame(left, bg=BG)

        def layout_subscribe(*_):
            # Hide it by default
            subscribe_frame.grid_forget()
            # Show only when the trigger is "subscribe"
            if trig_var.get() == "subscribe":
                log_chain_system("Subscribe trigger selected in chain editor.", level="DEBUG", action_type="editor")
                subscribe_frame.grid(row=9, column=0, columnspan=2, sticky="w", pady=4)

                # Simple explanatory label (customize as needed)
                tk.Label(subscribe_frame,
                         text="This chain will fire whenever a new subscription arrives.",
                         bg=BG, fg=FG, wraplength=300, justify="left"
                ).grid(row=0, column=0, sticky="w")

        # Wire it up
        trig_var.trace_add("write", layout_subscribe)
        layout_subscribe()

        # ────────────────────────────── Integration Toggles ──────────────────────────────
        integration_frame = tk.LabelFrame(
            right,
            text="Integrations",
            bg=BG,
            fg=FG,
            padx=10,
            pady=8
        )
        integration_frame.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        integration_frame.grid_columnconfigure(0, weight=1)

        # ───────────────────────── PiShock ─────────────────────────
        ttk.Checkbutton(
            integration_frame,
            text="⚡ Enable PiShock",
            variable=pishock_enabled
        ).grid(row=0, column=0, sticky="w")

        ttk.Checkbutton(
            integration_frame,
            text="⚡ PiShock Stand-Alone",
            variable=pishock_only
        ).grid(row=1, column=0, sticky="w", padx=(16, 0))

        ttk.Checkbutton(
            integration_frame,
            text="🎲 PiShock Random Devices",
            variable=pishock_random
        ).grid(row=2, column=0, sticky="w", padx=(16, 0))

        # ───────────────────────── Intiface ─────────────────────────
        ttk.Checkbutton(
            integration_frame,
            text="🔗 Enable Intiface",
            variable=intiface_enabled
        ).grid(row=3, column=0, sticky="w", pady=(6, 0))

        ttk.Checkbutton(
            integration_frame,
            text="🔗 Intiface Stand-Alone",
            variable=intiface_only
        ).grid(row=4, column=0, sticky="w", padx=(16, 0))

        ttk.Checkbutton(
            integration_frame,
            text="🎲 Intiface Random Devices",
            variable=intiface_random
        ).grid(row=5, column=0, sticky="w", padx=(16, 0))

        # ───────────────────────── OwO Vest ─────────────────────────
        ttk.Checkbutton(
            integration_frame,
            text="🟣 Enable OwO Vest",
            variable=owo_enabled
        ).grid(row=6, column=0, sticky="w", pady=(6, 0))

        # ────────────────────────────── OSC ──────────────────────────────
        ttk.Checkbutton(
            integration_frame,
            text="📡 Show OSC Parameters",
            variable=osc_enabled
        ).grid(row=7, column=0, sticky="w", pady=(6, 0))
        
        # ─────────────────────────── Prevents Mode Overwrite ──────────────────────────────
        def _sync_integration_flags(*_):
            if intiface_only.get():
                intiface_enabled.set(True)

            if pishock_only.get():
                pishock_enabled.set(True)

        intiface_only.trace_add("write", _sync_integration_flags)
        pishock_only.trace_add("write", _sync_integration_flags)

        # ────────────────────────────── PiShock Container (Visibility Wrapper) ──────────────────────────────
        pishock_container = tk.Frame(right, bg=BG)
        pishock_container.grid(row=1, column=0, sticky="nw", pady=(6, 0), columnspan=2)

        def _toggle_pishock_container(*_):
            if pishock_enabled.get():
                pishock_container.grid()
            else:
                pishock_container.grid_remove()

        _toggle_pishock_container()
        pishock_enabled.trace_add("write", _toggle_pishock_container)

        # PiShock Panel (async)
        tk.Label(pishock_container, text="Devices:", bg=BG, fg=FG).grid(row=1 + 1, column=0, sticky="w")

        device_frame = tk.Frame(pishock_container, bg=BG)
        device_frame.grid(row=2, column=0, sticky="w", pady=4)

        # Placeholder while we fetch
        loading_lbl = tk.Label(device_frame, text="Loading PiShock devices…", bg=BG, fg="lightgrey")
        loading_lbl.pack(anchor="w", padx=4)

        def load_pishock_devices_sync():
            try:
                if not hasattr(self, "pishock") or not hasattr(self.pishock, "devices"):
                    log_chain_system(
                        "PiShock client not initialized or missing devices.",
                        level="WARN",
                        action_type="editor"
                    )
                    return []

                devices = [
                    {
                        "shockerId": str(dev.get("shockerId")),
                        "clientId": str(dev.get("clientId")),
                        "shareId": dev.get("shareId"),
                        "shockerName": dev.get("shockerName") or dev.get("name", "Unnamed"),
                        "name": dev.get("name", "Unnamed"),
                        "code": dev.get("shareCode") or dev.get("code"),
                        "shareCode": dev.get("shareCode"),
                        "maxIntensity": dev.get("maxIntensity"),
                        "canShock": dev.get("canShock"),
                        "canVibrate": dev.get("canVibrate"),
                        "canBeep": dev.get("canBeep"),
                        "canLog": dev.get("canLog"),
                        "canContinuous": dev.get("canContinuous"),
                        "isPaused": dev.get("isPaused"),
                        "fromUser": dev.get("fromUser")
                    }
                    for dev in self.pishock.devices
                    if "shockerId" in dev
                ]

                log_chain_system(
                    f"Loaded {len(devices)} PiShock devices from live state.",
                    level="DEBUG",
                    data={"device_count": len(devices)},
                    action_type="editor"
                )
                return devices

            except Exception as e:
                log_chain_system(
                    "Failed to load PiShock devices.",
                    level="ERROR",
                    action_type="editor",
                    exc=e
                )
                return []

        def render_device_checkboxes(devices):
            for widget in device_frame.winfo_children():
                widget.destroy()

            self._selected_device_vars = {}
            seen_ids = set()

            saved_ids = {
                str(dev.get("shockerId") or dev.get("id"))
                for dev in (cfg.get("pishock_devices") or user_cfg.get("pishock_devices", []))
                if isinstance(dev, dict) and ("shockerId" in dev or "id" in dev)
            }

            for dev in devices:
                dev_id = str(dev.get("shockerId") or dev.get("id") or "")
                dev_name = dev.get("shockerName") or dev.get("name", "Unnamed")
                if not dev_id or dev_id in seen_ids:
                    continue
                seen_ids.add(dev_id)

                var = tk.BooleanVar(value=dev_id in saved_ids)
                self._selected_device_vars[dev_id] = var

                label = f"{dev_name} ({'shared' if dev.get('shareId') else 'owned'})"
                chk = ttk.Checkbutton(device_frame, text=label, variable=var)
                chk.pack(anchor="w", padx=4, pady=2)

                tooltip_lines = [
                    f"Name: {dev_name}",
                    f"ID: {dev_id}",
                    f"ClientId: {dev.get('clientId')}",
                    f"ShareId: {dev.get('shareId')}",
                    f"Code: {dev.get('shareCode') or dev.get('code')}",
                ]
                if dev.get("maxIntensity") is not None:
                    tooltip_lines.append(f"Max Intensity: {dev['maxIntensity']}")
                if dev.get("canShock") is not None:
                    tooltip_lines.append(f"Can Shock: {dev['canShock']}")
                if dev.get("canVibrate") is not None:
                    tooltip_lines.append(f"Can Vibrate: {dev['canVibrate']}")
                if dev.get("canBeep") is not None:
                    tooltip_lines.append(f"Can Beep: {dev.get('canBeep')}")
                if dev.get("canLog") is not None:
                    tooltip_lines.append(f"Can Log: {dev.get('canLog')}")
                if dev.get("canContinuous") is not None:
                    tooltip_lines.append(f"Can Continuous: {dev.get('canContinuous')}")
                if dev.get("isPaused") is not None:
                    tooltip_lines.append(f"Paused: {dev.get('isPaused')}")

                ToolTip(chk, "\n".join(tooltip_lines))

        def async_fetch_and_render():
            devices = load_pishock_devices_sync()
            self._pishock_devices = devices

            def on_ui_thread():
                loading_lbl.destroy()
                render_device_checkboxes(devices)
            win.after(0, on_ui_thread)

        threading.Thread(target=async_fetch_and_render, daemon=True).start()

        # Mode selector
        tk.Label(pishock_container, text="Mode:", bg=BG, fg=FG).grid(row=3, column=0, sticky="w")
        mode_box = ttk.Combobox(
            pishock_container,
            textvariable=mode_var,
            values=[
                "vibrate", "shock", "beep",
                "pulse",
                "randomized_wobble",
                "burst",
                "oscillation",
                "pattern",
                "rage"
            ],
            width=20,
            state="readonly"
        )
        mode_box.grid(row=4, column=0, sticky="w")
        ToolTip(mode_box, "Type of stimulation to apply.")
        log_gui_action("Mode selector rendered", data={"options": ["vibrate", "shock", "beep"]})

        # Intensity + Duration Frame
        dur_frame = tk.Frame(pishock_container, bg=BG)

        mode_config_frame = tk.Frame(pishock_container, bg=BG)
        mode_config_frame.grid(row=6, column=0, sticky="w", pady=(4, 0))

        lbl_int = tk.Label(dur_frame, text="Intensity:", bg=BG, fg=FG)
        ent_int = ttk.Entry(dur_frame, textvariable=intensity_var, width=6)
        ToolTip(lbl_int, "Shock/Vibrate intensity (0-100).")
        ToolTip(ent_int, "Shock/Vibrate intensity (0-100).")

        lbl_dur = tk.Label(dur_frame, text="Duration (s):", bg=BG, fg=FG)
        ent_dur = ttk.Entry(dur_frame, textvariable=duration_var, width=6)
        ToolTip(lbl_dur, "Duration of shock in seconds. max 1800")
        ToolTip(ent_dur, "Duration of shock in seconds. max 1800")

        def render_duration_fields(*_):
            for widget in dur_frame.winfo_children():
                widget.grid_forget()
            for widget in mode_config_frame.winfo_children():
                widget.destroy()

            show_modes = {"vibrate", "shock", "pulse", "burst", "oscillation", "rage"}
            current = mode_var.get()

            if current in show_modes:
                dur_frame.grid(row=5, column=0, sticky="w")
                lbl_int.grid(row=0, column=0)
                ent_int.grid(row=0, column=1, padx=4)
                lbl_dur.grid(row=0, column=2)
                ent_dur.grid(row=0, column=3)

            def locked(msg):
                tk.Label(mode_config_frame, text=msg, bg=BG, fg="white", justify="left")\
                    .grid(row=0, column=0, sticky="w")

            if current == "pulse":
                if LICENSE_TIER in ("bronze", "silver", "gold"):
                    tk.Label(mode_config_frame, text="Delay (ms):", bg=BG, fg=FG).grid(row=0, column=0, sticky="w")
                    ttk.Entry(mode_config_frame, textvariable=pulse_delay_var, width=8).grid(row=0, column=1, padx=(4, 0))
                    tk.Label(mode_config_frame, text="Repeats a fixed pulse with delay between each.", bg=BG, fg="white")\
                        .grid(row=1 + 1, column=0, columnspan=2, sticky="w", pady=(2, 0))
                else:
                    locked("Repeats a fixed pulse with delay between each.\nUpgrade to Bronze to enable this mode.")

            elif current == "burst":
                if LICENSE_TIER in ("bronze", "silver", "gold"):
                    locked("Fires 3 quick shocks in succession.\nDelay is fixed. No configuration required.")
                else:
                    locked("Upgrade to Bronze to enable Burst mode.")

            elif current == "oscillation":
                if LICENSE_TIER in ("bronze", "silver", "gold"):
                    tk.Label(mode_config_frame, text="Step Δ Intensity:", bg=BG, fg=FG).grid(row=0, column=0, sticky="w")
                    ttk.Entry(mode_config_frame, textvariable=osc_step_var, width=6).grid(row=0, column=1, padx=(4, 0))
                    tk.Label(mode_config_frame, text="Increases/decreases intensity in waves (min 5, max 100).", bg=BG, fg="white")\
                        .grid(row=1 + 1, column=0, columnspan=2, sticky="w", pady=(2, 0))
                else:
                    locked("Oscillates intensity in waves over time.\nUpgrade to Bronze to enable Oscillation.")

            elif current == "randomized_wobble":
                if LICENSE_TIER in ("bronze", "silver", "gold"):
                    locked(
                        "🎲 Randomly selects shock, vibrate, beep, or echo each cycle.\n"
                        "Intensity and duration are randomized for unpredictability.\n"
                        "No extra setup required."
                    )
                else:
                    locked("Upgrade to Bronze to enable Randomized Wobble.")

            elif current == "rage":
                if LICENSE_TIER in ("silver", "gold"):
                    tk.Label(mode_config_frame, text="Rapid-fire 100ms shocks with 0.5s delay between hits.", bg=BG, fg="white")\
                        .grid(row=0, column=0, columnspan=2, sticky="w", pady=(2, 0))

                    rage_estimate_lbl = tk.Label(mode_config_frame, text="Est. shocks: -", bg=BG, fg="lightgrey")
                    rage_estimate_lbl.grid(row=1 + 1, column=0, columnspan=2, sticky="w")

                    def update_rage_estimate(*_):
                        try:
                            total_sec = float(duration_var.get())
                            count = int(total_sec / 0.6)
                            rage_estimate_lbl.config(text=f"Est. shocks: {count}")
                        except Exception:
                            rage_estimate_lbl.config(text="Est. shocks: -")

                    duration_var.trace_add("write", update_rage_estimate)
                    update_rage_estimate()
                else:
                    locked("Upgrade to Silver to enable Rage mode.")

            elif current == "pattern":
                if LICENSE_TIER in ("silver", "gold"):
                    ttk.Button(
                        mode_config_frame,
                        text="Edit Pattern",
                        command=lambda: self.open_pattern_editor(cfg)
                    ).grid(row=0, column=0, sticky="w", padx=2)
                    tk.Label(mode_config_frame, text="Define a sequence of mixed shock/vibrate/beep steps.", bg=BG, fg="white")\
                        .grid(row=1 + 1, column=0, sticky="w", pady=(2, 0))
                else:
                    locked("Upgrade to Silver to enable Pattern mode.")

            log_gui_action("Custom mode options rendered", data={"mode": current})

        mode_var.trace_add("write", render_duration_fields)
        render_duration_fields()

        # ─────────────────────────────────────────────────────────────
        # INTIFACE UI CONTAINER (BAKED: capability gating + actuator select + mode tuning + pattern editor)
        # ─────────────────────────────────────────────────────────────
        intiface_container = tk.Frame(right, bg=BG)
        intiface_container.grid(row=10, column=0, sticky="nw", pady=(12, 8), columnspan=2)

        def _toggle_intiface_container(*_):
            intiface_container.grid() if intiface_enabled.get() else intiface_container.grid_remove()

        intiface_enabled.trace_add("write", _toggle_intiface_container)
        _toggle_intiface_container()

        intiface_frame = tk.LabelFrame(
            intiface_container,
            text="Intiface Integration",
            bg=BG,
            fg=FG,
            padx=10,
            pady=8
        )
        intiface_frame.grid(row=0, column=0, sticky="ew")
        intiface_frame.grid_columnconfigure(0, weight=1)

        # ── Helpers: capability model ─────────────────────────────────────────────
        def get_intiface_capabilities():
            """
            Uses self.intiface.devices[*]["features"]["ScalarCmd"] list.
            Returns:
              {
                "per_device": {
                   idx: {
                     "name": str,
                     "supports": {"vibrate": bool, "constrict": bool},
                     "actuators": ["vibrate", "constrict"] (available)
                   }
                },
                "any": {"vibrate": bool, "constrict": bool}
              }
            """
            caps = {"per_device": {}, "any": {"vibrate": False, "constrict": False}}

            for idx, dev in getattr(self.intiface, "devices", {}).items():
                try:
                    did = int(idx)
                except Exception:
                    continue

                name = dev.get("name") or f"Device {did}"
                supports = {"vibrate": False, "constrict": False}

                for f in (dev.get("features", {}) or {}).get("ScalarCmd", []) or []:
                    at = (f.get("ActuatorType") or "").strip().lower()
                    if at == "vibrate":
                        supports["vibrate"] = True
                        caps["any"]["vibrate"] = True
                    elif at == "constrict":
                        supports["constrict"] = True
                        caps["any"]["constrict"] = True

                actuators = []
                if supports["vibrate"]:
                    actuators.append("vibrate")
                if supports["constrict"]:
                    actuators.append("constrict")

                caps["per_device"][did] = {"name": name, "supports": supports, "actuators": actuators}

            return caps

        def _selected_device_indices():
            out = []
            for dev_id, var in (self._selected_intiface_device_vars or {}).items():
                try:
                    if var.get():
                        out.append(int(dev_id))
                except Exception:
                    continue
            return out

        def _effective_caps_for_selection():
            """
            If nothing selected, fall back to global caps.
            If selected, compute caps based on selected + chosen actuator per device.
            """
            caps = get_intiface_capabilities()
            sel = _selected_device_indices()

            eff = {"vibrate": False, "constrict": False}

            if not sel:
                eff["vibrate"] = bool(caps["any"]["vibrate"])
                eff["constrict"] = bool(caps["any"]["constrict"])
                return eff

            # Selected devices: use chosen actuator if present, else device supports
            for did in sel:
                choice_var = (self._intiface_device_actuators or {}).get(did)
                choice = (choice_var.get().strip().lower() if choice_var else "")
                if choice in ("vibrate", "constrict"):
                    eff[choice] = True
                    continue

                dcap = caps["per_device"].get(did, {}).get("supports", {})
                if dcap.get("vibrate"):
                    eff["vibrate"] = True
                if dcap.get("constrict"):
                    eff["constrict"] = True

            return eff

        def refresh_intiface_modes(*_):
            """
            Mode gating logic:
            - Vibrate enables all intensity-based modes
            - Constrict does NOT disable vibrate
            - Pattern allowed if ANY actuator exists
            """
            eff = _effective_caps_for_selection()

            allowed = []

            # Vibrate-based modes
            if eff["vibrate"]:
                allowed += [
                    "vibrate",
                    "pulse",
                    "burst",
                    "oscillation",
                    "randomized_wobble",
                ]

            # Pattern allowed if *any* actuator exists
            if eff["vibrate"] or eff["constrict"]:
                allowed.append("pattern")

            # Failsafe
            if not allowed:
                allowed = ["vibrate"]

            # Deduplicate while preserving order
            allowed = list(dict.fromkeys(allowed))

            try:
                intiface_mode_box["values"] = allowed
            except Exception:
                pass

            if intiface_mode_var.get() not in allowed:
                intiface_mode_var.set(allowed[0])

            render_intiface_mode_controls()

        # ── Device renderer (checkbox + per-device actuator dropdown) ───────────────
        def render_intiface_devices():
            for w in intiface_device_frame.winfo_children():
                w.destroy()

            self._selected_intiface_device_vars = {}
            self._intiface_device_actuators = {}

            caps = get_intiface_capabilities()

            # saved device records (prefer new key "actuator", fall back to "type")
            saved = {}
            for d in (cfg.get("intiface_devices", []) or []):
                if not isinstance(d, dict):
                    continue
                try:
                    k = str(int(d.get("index")))
                except Exception:
                    continue
                act = (d.get("actuator") or d.get("type") or "").strip().lower()
                saved[k] = act

            # stable ordering
            for did in sorted(caps["per_device"].keys()):
                meta = caps["per_device"][did]
                name = meta["name"]
                opts = meta["actuators"] or ["vibrate"]

                row = tk.Frame(intiface_device_frame, bg=BG)
                row.pack(anchor="w", pady=2)

                sel = tk.BooleanVar(value=str(did) in saved)
                self._selected_intiface_device_vars[str(did)] = sel

                ttk.Checkbutton(
                    row,
                    text=f"{name}  (#{did})",
                    variable=sel
                ).pack(side="left", padx=4)

                # default actuator: saved if valid else first available
                default_act = saved.get(str(did), "")
                if default_act not in opts:
                    default_act = opts[0]

                act_var = tk.StringVar(value=default_act)
                self._intiface_device_actuators[did] = act_var

                act_box = ttk.Combobox(
                    row,
                    textvariable=act_var,
                    values=opts,
                    width=12,
                    state="readonly"
                )
                act_box.pack(side="left", padx=6)

                # Any change should refresh gating
                sel.trace_add("write", refresh_intiface_modes)
                act_var.trace_add("write", refresh_intiface_modes)

                try:
                    ToolTip(act_box, "Select which actuator to drive on this device.")
                except Exception:
                    pass

            refresh_intiface_modes()

        def async_fetch_and_render_intiface():
            def on_ui():
                if intiface_loading_lbl.winfo_exists():
                    intiface_loading_lbl.destroy()
                render_intiface_devices()
            win.after(0, on_ui)

        # ── Toggles row ─────────────────────────────────────────────
        ttk.Checkbutton(intiface_frame, text="Enable", variable=intiface_enabled)\
            .grid(row=0, column=0, sticky="w")

        ttk.Checkbutton(intiface_frame, text="Stand-Alone", variable=intiface_only)\
            .grid(row=0, column=1, sticky="w", padx=(10, 0))

        ttk.Checkbutton(intiface_frame, text="Random Devices", variable=intiface_random)\
            .grid(row=0, column=2, sticky="w", padx=(10, 0))

        # ── Devices ─────────────────────────────────────────────
        tk.Label(intiface_frame, text="Devices:", bg=BG, fg=FG)\
            .grid(row=1, column=0, columnspan=3, sticky="w", pady=(6, 2))

        intiface_device_frame = tk.Frame(intiface_frame, bg=BG)
        intiface_device_frame.grid(row=2, column=0, columnspan=3, sticky="w")

        intiface_loading_lbl = tk.Label(intiface_device_frame, text="Loading Intiface devices…", bg=BG, fg="lightgrey")
        intiface_loading_lbl.pack(anchor="w", padx=4)

        # ── Mode ─────────────────────────────────────────────────
        tk.Label(intiface_frame, text="Mode:", bg=BG, fg=FG)\
            .grid(row=3, column=0, sticky="w", pady=(6, 0))

        intiface_mode_box = ttk.Combobox(
            intiface_frame,
            textvariable=intiface_mode_var,
            values=list(VALID_INTIFACE_MODES),
            width=18,
            state="readonly"
        )
        intiface_mode_box.grid(row=3, column=1, sticky="w", padx=(4, 0))
        ToolTip(intiface_mode_box, "Intiface output mode. Modes auto-filter based on selected devices/actuators.")

        # ── Intensity + Duration ─────────────────────────────────
        intiface_dur_frame = tk.Frame(intiface_frame, bg=BG)
        intiface_dur_frame.grid(row=4, column=0, columnspan=3, sticky="w", pady=(6, 0))

        tk.Label(intiface_dur_frame, text="Intensity (%):", bg=BG, fg=FG)\
            .grid(row=0, column=0, sticky="w")
        ttk.Entry(intiface_dur_frame, textvariable=intiface_intensity_var, width=6)\
            .grid(row=0, column=1, padx=(4, 12))

        tk.Label(intiface_dur_frame, text="Duration (s):", bg=BG, fg=FG)\
            .grid(row=0, column=2, sticky="w")
        ttk.Entry(intiface_dur_frame, textvariable=intiface_duration_var, width=6)\
            .grid(row=0, column=3, padx=(4, 0))

        # ── Advanced mode controls (dynamic) ─────────────────────────────────
        intiface_mode_cfg = tk.Frame(intiface_frame, bg=BG)
        intiface_mode_cfg.grid(row=5, column=0, columnspan=3, sticky="w", pady=(6, 0))

        def _clear_mode_cfg():
            for w in intiface_mode_cfg.winfo_children():
                try:
                    w.destroy()
                except Exception:
                    pass

        def render_intiface_mode_controls(*_):
            _clear_mode_cfg()
            mode = (intiface_mode_var.get() or "vibrate").strip().lower()

            def locked(msg):
                tk.Label(intiface_mode_cfg, text=msg, bg=BG, fg="white", justify="left")\
                    .grid(row=0, column=0, sticky="w")

            if mode == "oscillation":
                # step tuning is the missing piece
                tk.Label(intiface_mode_cfg, text="Step Δ Intensity (%):", bg=BG, fg=FG)\
                    .grid(row=0, column=0, sticky="w")
                ttk.Entry(intiface_mode_cfg, textvariable=intiface_step_var, width=6)\
                    .grid(row=0, column=1, padx=(4, 0))
                tk.Label(intiface_mode_cfg, text="Higher = faster ramp up/down (1–50).", bg=BG, fg="white")\
                    .grid(row=1, column=0, columnspan=2, sticky="w", pady=(2, 0))

            elif mode == "randomized_wobble":
                tk.Label(intiface_mode_cfg, text="Min (%):", bg=BG, fg=FG)\
                    .grid(row=0, column=0, sticky="w")
                ttk.Entry(intiface_mode_cfg, textvariable=intiface_random_min_var, width=6)\
                    .grid(row=0, column=1, padx=(4, 12))

                tk.Label(intiface_mode_cfg, text="Max (%):", bg=BG, fg=FG)\
                    .grid(row=0, column=2, sticky="w")
                ttk.Entry(intiface_mode_cfg, textvariable=intiface_random_max_var, width=6)\
                    .grid(row=0, column=3, padx=(4, 0))

                tk.Label(intiface_mode_cfg, text="Random strength between Min/Max each tick.", bg=BG, fg="white")\
                    .grid(row=1, column=0, columnspan=4, sticky="w", pady=(2, 0))

            elif mode == "pattern":
                # ── Pattern selector ─────────────────────────────
                tk.Label(intiface_mode_cfg, text="Pattern:", bg=BG, fg=FG)\
                    .grid(row=0, column=0, sticky="w")

                patterns = cfg.get("intiface_patterns", {})
                keys = sorted(k for k in patterns.keys() if isinstance(k, str))

                if "Default" not in keys:
                    keys.insert(0, "Default")

                pat_box = ttk.Combobox(
                    intiface_mode_cfg,
                    textvariable=self._intiface_pattern_key_var,
                    values=keys,
                    width=18,
                    state="readonly"
                )
                pat_box.grid(row=0, column=1, sticky="w", padx=(4, 8))

                ToolTip(pat_box, "Select which saved pattern to run.")

                # ── Pattern Editor ──────────────────────────────
                ttk.Button(
                    intiface_mode_cfg,
                    text="Edit Pattern",
                    command=lambda: self.open_intiface_pattern_editor(cfg)
                ).grid(row=0, column=2, sticky="w")

                tk.Label(
                    intiface_mode_cfg,
                    text="Define a sequence of vibrate and/or constrict steps.\nBoth may be used together if supported by the device.",
                    bg=BG,
                    fg="white",
                    justify="left"
                ).grid(row=1, column=0, columnspan=3, sticky="w", pady=(2, 0))

            else:
                # nothing special; keep frame empty
                pass

        intiface_mode_var.trace_add("write", render_intiface_mode_controls)
        render_intiface_mode_controls()

        # kick async render
        threading.Thread(target=async_fetch_and_render_intiface, daemon=True).start()

        # Ensure mode gating runs after devices load too (safe)
        refresh_intiface_modes()

        # ────────────────────────────── OwO Container (Visibility Wrapper) ──────────────────────────────
        owo_container = tk.Frame(right, bg=BG)
        owo_container.grid(row=7, column=0, sticky="nw", pady=(12, 0), columnspan=2)

        def _toggle_owo_container(*_):
            if owo_enabled.get():
                owo_container.grid()
            else:
                owo_container.grid_remove()

        _toggle_owo_container()
        owo_enabled.trace_add("write", _toggle_owo_container)

        # ────────────────────────────── OwO Panel ──────────────────────────────
        owo_frame = tk.LabelFrame(
            owo_container,
            text="OWO Vest",
            bg=BG,
            fg=FG,
            padx=6,
            pady=4
        )
        owo_frame.grid(row=0, column=0, sticky="ew")


        # ────────────────────────────── Section Rows (so we can grid_remove groups) ──────────────────────────────
        row_template = tk.Frame(owo_frame, bg=BG)
        row_mode     = tk.Frame(owo_frame, bg=BG)
        row_edit     = tk.Frame(owo_frame, bg=BG)
        row_queue    = tk.Frame(owo_frame, bg=BG)

        row_template.grid(row=0, column=0, sticky="w", pady=(0, 2))
        row_mode.grid(row=1, column=0, sticky="w", pady=(2, 2))
        row_edit.grid(row=2, column=0, sticky="w", pady=(6, 2))
        row_queue.grid(row=3, column=0, sticky="w", pady=(2, 0))


        # ────────────────────────────── Template Selection (ROW) ──────────────────────────────
        tk.Label(row_template, text="Template:", bg=BG, fg=FG).grid(row=0, column=0, sticky="w")

        owo_files = self.owo.list_available_files() if hasattr(self, "owo") else []
        owo_file_var = tk.StringVar(value=cfg.get("owo_file", owo_files[0] if owo_files else ""))

        owo_file_dropdown = ttk.Combobox(
            row_template,
            textvariable=owo_file_var,
            values=owo_files,
            width=24,
            state="readonly"
        )
        owo_file_dropdown.grid(row=0, column=1, sticky="w", padx=(6, 0))


        # ────────────────────────────── Mode Selector (ROW) ──────────────────────────────
        tk.Label(row_mode, text="Mode:", bg=BG, fg=FG).grid(row=0, column=0, sticky="w")

        owo_pattern_mode = tk.StringVar(value=str(cfg.get("owo_pattern_mode", "template")).lower())

        owo_pattern_box = ttk.Combobox(
            row_mode,
            textvariable=owo_pattern_mode,
            values=["template", "pattern"],
            width=18,
            state="readonly"
        )
        owo_pattern_box.grid(row=0, column=1, sticky="w", padx=(6, 0))


        # ────────────────────────────── Pattern Editor (ROW) ──────────────────────────────
        def _open_owo_pattern_editor():
            try:
                self.open_owo_pattern_editor(cfg)
                log_gui_action("Opened OwO pattern editor")
            except Exception as e:
                log_gui_action("Failed to open OwO pattern editor", level="ERROR", exc=e)

        edit_btn = ttk.Button(row_edit, text="Edit Pattern", command=_open_owo_pattern_editor)
        edit_btn.grid(row=0, column=0, sticky="w")

        owo_pattern_help = tk.Label(
            row_edit,
            text="Define a sequence of mixed sensations/steps.",
            bg=BG,
            fg="white",
            justify="left"
        )
        owo_pattern_help.grid(row=1, column=0, sticky="w", pady=(4, 0))


        # ────────────────────────────── Queue Behavior (ROW) ──────────────────────────────
        tk.Label(row_queue, text="Queue Behavior:", bg=BG, fg=FG).grid(row=0, column=0, sticky="w")

        owo_queue_mode = tk.BooleanVar(value=bool(cfg.get("owo_queue", True)))
        ttk.Checkbutton(row_queue, text="Queue with chain", variable=owo_queue_mode)\
            .grid(row=0, column=1, sticky="w", padx=(6, 0))


        # ────────────────────────────── PiShock-style Hide/Show Behavior ──────────────────────────────
        def _sync_owo_mode_ui(*_):
            mode = (owo_pattern_mode.get() or "template").strip().lower()

            if mode == "pattern":
                # ✅ PiShock behavior: hide everything irrelevant, don’t just disable it
                row_template.grid_remove()
                row_queue.grid_remove()

                # show only the pattern controls
                row_edit.grid()
                edit_btn.configure(state="normal")

                # clear template selection like before
                owo_file_var.set("")
            else:
                # ✅ Template mode: show template + queue, hide pattern controls
                row_template.grid()
                row_queue.grid()

                row_edit.grid_remove()
                edit_btn.configure(state="disabled")

                # restore dropdown usability
                owo_file_dropdown.configure(state="readonly")

                # seed template if empty
                if not owo_file_var.get().strip() and owo_files:
                    owo_file_var.set(owo_files[0])

        owo_pattern_mode.trace_add("write", _sync_owo_mode_ui)
        _sync_owo_mode_ui()

        # ────────────────────────────── Save Logic Control ──────────────────────────────
        # Save button logic
        def on_save():
            try:
                cfg["name"] = name_var.get().strip() or "Untitled"
                cfg["delay"] = float(delay_var.get())
                cfg["reset_before"] = rb.get()
                cfg["reset_after"] = ra.get()
                cfg["trigger"] = trig_var.get()
                cfg["queue_mode"] = qm.get()
                cfg["priority"] = int(priority_var.get())
                cfg["osc_enabled"] = False
                cfg["pishock_enabled"] = pishock_enabled.get()
                cfg["pishock_only"] = pishock_only.get()
                cfg["pishock_random_devices"] = pishock_random.get()
                cfg["pishock_devices"] = []

                devices = getattr(self, "_pishock_devices", [])

                for dev_id, var in self._selected_device_vars.items():
                    if not var.get():
                        continue

                    match = next(
                        (d for d in devices if str(d.get("shockerId")) == str(dev_id)),
                        None
                    )

                    if not match:
                        log_chain_system(
                            "Selected PiShock device not found in live list",
                            level="ERROR",
                            data={"dev_id": dev_id},
                            action_type="editor"
                        )
                        continue

                    dev_name = match.get("shockerName") or match.get("name") or "Unknown Device"

                    cfg["pishock_devices"].append({
                        "id": str(match.get("shockerId")),
                        "name": dev_name,
                        "shared": bool(match.get("shareId"))
                    })
                cfg["pishock_mode"] = mode_var.get()
                
                # ───── Advanced mode support ─────
                mode = cfg["pishock_mode"]

                if mode in {"pulse", "burst"}:
                    try:
                        cfg["pishock_delay"] = int(pulse_delay_var.get())
                    except Exception as e:
                        log_chain_system("Invalid delay input", level="WARN", exc=e)

                elif mode == "oscillation":
                    try:
                        cfg["pishock_step"] = int(osc_step_var.get())
                    except Exception as e:
                        log_chain_system("Invalid step input", level="WARN", exc=e)

                elif mode == "pattern":
                    cfg["pishock_pattern"] = cfg.get("pishock_pattern", [])
                    # UI support stubbed, no validation needed here if PATTERN_EDITOR_ENABLED is already enforced in UI

                elif mode == "randomized_wobble":
                    if LICENSE_TIER not in ("bronze", "silver", "gold"):
                        messagebox.showerror(
                            "Unsupported Feature",
                            "Randomized Wobble requires a Bronze supporter tier or higher.\nPlease consider upgrading your license."
                        )
                        log_chain_system("Blocked saving randomized_wobble due to insufficient license", level="WARN", data=cfg)
                        return

                cfg["pishock_intensity"] = int(intensity_var.get())
                cfg["pishock_duration"] = int(float(duration_var.get()) * 1000)

                # ─────────────────────────────────────────────
                # OwO Vest Settings (Authoritative Save Layer)
                # ─────────────────────────────────────────────

                # Master enable
                cfg["owo_enabled"] = bool(owo_enabled.get())

                # Mode: "template" or "pattern"
                mode = (
                    owo_pattern_mode.get().strip().lower()
                    if "owo_pattern_mode" in locals()
                    else "template"
                )

                if mode not in ("template", "pattern"):
                    mode = "template"

                cfg["owo_pattern_mode"] = mode

                # Queue behavior
                cfg["owo_queue"] = bool(
                    owo_queue_mode.get()
                    if "owo_queue_mode" in locals()
                    else True
                )

                # ─────────────────────────────────────────────
                # TEMPLATE MODE
                # ─────────────────────────────────────────────
                if mode == "template":

                    # Template name
                    cfg["owo_file"] = (owo_file_var.get() or "").strip()

                    # Pattern must never exist in template mode
                    cfg.pop("owo_pattern", None)

                # ─────────────────────────────────────────────
                # PATTERN MODE
                # ─────────────────────────────────────────────
                elif mode == "pattern":

                    # Template must never exist in pattern mode
                    cfg.pop("owo_file", None)

                    raw_pattern = cfg.get("owo_pattern", [])
                    if not isinstance(raw_pattern, list):
                        raw_pattern = []

                    normalized = []

                    for step in raw_pattern:
                        try:
                            normalized.append({
                                "step": int(step.get("step", len(normalized) + 1)),

                                "frequency": max(1, min(200, int(step.get("frequency", 60)))),
                                "duration": max(0.05, min(60.0, float(step.get("duration", 1.0)))),
                                "intensity": max(0, min(100, int(step.get("intensity", 70)))),

                                "fade_in_ms": max(0, min(5000, int(step.get("fade_in_ms", 0)))),
                                "fade_out_ms": max(0, min(5000, int(step.get("fade_out_ms", 0)))),
                                "delay_s": max(0.0, float(step.get("delay_s", 0.0))),

                                "label": (step.get("label") or "").strip(),

                                "muscles": [
                                    {
                                        "id": str(m.get("id")),
                                        "pct": max(0, min(100, int(m.get("pct", 0))))
                                    }
                                    for m in (step.get("muscles") or [])
                                    if isinstance(m, dict) and "id" in m
                                ]
                            })
                        except Exception:
                            # Never allow a malformed step to crash save
                            continue

                    cfg["owo_pattern"] = normalized

                # ─────────────────────────────────────────────
                # FINAL SAFETY NORMALIZATION
                # ─────────────────────────────────────────────

                # Always ensure correct types exist
                if not isinstance(cfg.get("owo_pattern"), list):
                    cfg["owo_pattern"] = []

                if not isinstance(cfg.get("owo_queue"), bool):
                    cfg["owo_queue"] = True
                
                # ─────────────────────────────────────────────
                # INTIFACE SAVE LOGIC (Authoritative, Pattern-Safe)
                # ─────────────────────────────────────────────

                cfg["intiface_enabled"] = bool(intiface_enabled.get())
                cfg["intiface_only"] = bool(intiface_only.get())
                cfg["intiface_random_devices"] = bool(intiface_random.get())

                # ─────────────────────────────
                # Mode (UI-selected)
                # ─────────────────────────────
                mode = (intiface_mode_var.get() or "vibrate").strip().lower()
                cfg["intiface_mode"] = mode


                # ─────────────────────────────
                # Devices (preserve actuator intent)
                # ─────────────────────────────
                cfg["intiface_devices"] = []

                live_devices = getattr(self.intiface, "devices", {}) or {}

                for dev_id, sel_var in (self._selected_intiface_device_vars or {}).items():
                    if not sel_var.get():
                        continue

                    try:
                        did = int(dev_id)
                    except Exception:
                        continue

                    dev = live_devices.get(did)
                    if not dev:
                        continue

                    act_var = (self._intiface_device_actuators or {}).get(did)
                    actuator = (act_var.get().strip().lower() if act_var else "vibrate")

                    if actuator not in ("vibrate", "constrict"):
                        actuator = "vibrate"

                    cfg["intiface_devices"].append({
                        "index": did,
                        "name": dev.get("name", f"Device {did}"),
                        "actuator": actuator,
                        "step_count": int(
                            (dev.get("features", {}).get("ScalarCmd") or [{}])[0].get("StepCount", 100)
                        ),
                    })


                # ─────────────────────────────
                # Intensity (normalized 0–1)
                # ─────────────────────────────
                try:
                    pct = float(intiface_intensity_var.get())
                    cfg["intiface_intensity"] = round(max(0.0, min(100.0, pct)) / 100.0, 4)
                except Exception:
                    cfg["intiface_intensity"] = 0.75


                # ─────────────────────────────
                # Duration
                # ─────────────────────────────
                try:
                    dur = float(intiface_duration_var.get())
                    cfg["intiface_duration"] = round(max(0.05, min(dur, 1800.0)), 3)
                except Exception:
                    cfg["intiface_duration"] = 1.0


                # ─────────────────────────────
                # Oscillation
                # ─────────────────────────────
                if mode == "oscillation":
                    try:
                        step = int(intiface_step_var.get())
                        step = max(1, min(50, step))

                        cfg["intiface_step"] = step

                        # 🔥 CRITICAL: lock oscillation ceiling to intensity
                        try:
                            base = float(cfg.get("intiface_intensity", 1.0))
                        except Exception:
                            base = 1.0

                        cfg["intiface_osc_max"] = max(0.05, min(1.0, base))

                    except Exception:
                        cfg["intiface_step"] = 10
                        cfg["intiface_osc_max"] = 1.0
                else:
                    cfg.pop("intiface_step", None)
                    cfg.pop("intiface_osc_max", None)


                # ─────────────────────────────
                # Randomized wobble
                # ─────────────────────────────
                if mode == "randomized_wobble":
                    try:
                        mn = float(intiface_random_min_var.get())
                        mx = float(intiface_random_max_var.get())
                        cfg["intiface_random_min"] = max(0.0, min(mn, mx))
                        cfg["intiface_random_max"] = max(cfg["intiface_random_min"], min(mx, 1.0))
                    except Exception:
                        cfg["intiface_random_min"] = 0.2
                        cfg["intiface_random_max"] = 1.0
                else:
                    cfg.pop("intiface_random_min", None)
                    cfg.pop("intiface_random_max", None)


                # ─────────────────────────────
                # Pattern Mode (Authoritative)
                # ─────────────────────────────
                if mode == "pattern":

                    key = (self._intiface_pattern_key_var.get() or "Default").strip()
                    cfg["intiface_pattern_key"] = key

                    if not isinstance(cfg.get("intiface_patterns"), dict):
                        cfg["intiface_patterns"] = {}

                    # DO NOT reinterpret steps — editor is source of truth
                    normalized = []

                    for step in (cfg.get("intiface_pattern") or []):
                        try:
                            normalized.append({
                                "actuator": (step.get("actuator") or "vibrate").lower(),
                                "intensity": float(step.get("intensity", 1.0)),
                                "duration": float(step.get("duration", 1.0)),
                                "delay": float(step.get("delay", 0.0)),
                                "osc_step": int(step.get("osc_step", 10)),
                            })
                        except Exception:
                            continue

                    cfg["intiface_patterns"][key] = normalized
                    cfg["intiface_pattern"] = normalized

                else:
                    # Non-pattern modes should not erase the library
                    cfg.pop("intiface_pattern", None)
                    cfg.pop("intiface_pattern_key", None)

                # ─── Trigger Variation Settings ───
                if cfg["trigger"] == "gift":
                    cfg["giftName"] = gift_name.get().strip()

                    try:
                        count_val = gift_count.get().strip()
                        cfg["giftCount"] = int(count_val) if count_val else None
                    except ValueError:
                        log_chain_system("Invalid gift count input", level="WARN")
                        cfg["giftCount"] = None

                    cfg["allowRepeat"] = repeat_var.get()

                    # Optional filter fields
                    try:
                        if diamond_min.get().strip():
                            cfg["diamondCountMin"] = int(diamond_min.get())
                        if diamond_max.get().strip():
                            cfg["diamondCountMax"] = int(diamond_max.get())
                        if gifter_min.get().strip():
                            cfg["gifterLevelMin"] = int(gifter_min.get())
                        if gifter_max.get().strip():
                            cfg["gifterLevelMax"] = int(gifter_max.get())
                    except Exception as e:
                        log_chain_system("Invalid numeric input in gift filters", level="WARN", exc=e)

                    cfg["isSubscriber"] = is_sub_var.get()
                    cfg["isModerator"] = is_mod_var.get()

                elif cfg["trigger"] == "custom_diamonds":
                    try:
                        trigger_val = custom_diamond_trigger.get().strip()
                        if trigger_val:
                            cfg["diamondCountTrigger"] = int(trigger_val)
                    except ValueError as e:
                        messagebox.showerror("Invalid Input", "Diamond count must be a number.")
                        log_chain_system("Invalid custom diamond count input", level="WARN", exc=e)

                    # Optional filters (same as gift)
                    try:
                        if diamond_min.get().strip():
                            cfg["diamondCountMin"] = int(diamond_min.get())
                        if diamond_max.get().strip():
                            cfg["diamondCountMax"] = int(diamond_max.get())
                        if gifter_min.get().strip():
                            cfg["gifterLevelMin"] = int(gifter_min.get())
                        if gifter_max.get().strip():
                            cfg["gifterLevelMax"] = int(gifter_max.get())
                    except Exception as e:
                        log_chain_system("Invalid numeric input in custom filters", level="WARN", exc=e)

                    cfg["isSubscriber"] = is_sub_var.get()
                    cfg["isModerator"] = is_mod_var.get()
        
                elif cfg["trigger"] == "random":
                    cfg["delay_min"] = float(delay_min_var.get())
                    cfg["delay_max"] = float(delay_max_var.get())
                    cfg["step_limit"] = int(step_limit_var.get()) if step_limit_var.get().strip() else None
                    cfg["repeat"] = repeat_var.get()

                log_chain_system("Chain config saved", level="INFO", data={
                    **cfg,
                    "selected_pishock_devices": {
                        dev_id: var.get()
                        for dev_id, var in self._selected_device_vars.items()
                    }
                }, action_type="editor")
                log_gui_action("User saved chain config", data=cfg)
                win.destroy()

            except Exception as e:
                log_chain_system("Invalid configuration during chain save", level="ERROR", exc=e, action_type="editor")
                log_gui_action("GUI error during chain save", level="ERROR", exc=e)
                messagebox.showerror("Error", f"Invalid configuration: {e}")

        ttk.Button(content, text="Save", command=on_save).pack(pady=10)
        log_gui_action("Save button initialized")

        # ────────────────────────────── OSC Container (Visibility Wrapper) ──────────────────────────────

        osc_container = tk.Frame(content, bg=BG)
        osc_container.pack(fill="x", expand=True, padx=12, pady=4)

        def _toggle_osc_container(*_):
            if osc_enabled.get():
                osc_container.pack(fill="both", expand=True, padx=12, pady=4)
            else:
                osc_container.pack_forget()

        _toggle_osc_container()
        osc_enabled.trace_add("write", _toggle_osc_container)

        # ────────────────────────────── OSC UI  ──────────────────────────────
        tk.Label(osc_container, text="Search:", bg=BG, fg=FG)\
            .pack(anchor="w", padx=12, pady=(8, 0))
        search_var = tk.StringVar()

        ttk.Entry(osc_container, textvariable=search_var)\
            .pack(fill="x", padx=12, pady=4)
        log_gui_action("Search bar initialized for OSC control list")

        # scroll-ready container
        osc_wrap = tk.Frame(osc_container, bg=BG)
        osc_wrap.pack(fill="both", expand=True, padx=12, pady=4)

        canvas = tk.Canvas(osc_wrap, bg=BG, highlightthickness=0)
        vbar   = ttk.Scrollbar(osc_wrap, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=vbar.set)

        vbar.pack(side="right",  fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        host    = tk.Frame(canvas, bg=BG)
        host_id = canvas.create_window((0, 0), window=host, anchor="nw")

        # keep scroll-region & width synced
        def _sync_inner(event):
            canvas.configure(scrollregion=canvas.bbox("all"))
            canvas.itemconfigure(host_id, width=canvas.winfo_width())
        host.bind("<Configure>", _sync_inner)
        canvas.bind("<Configure>", lambda e: canvas.itemconfigure(host_id, width=e.width))

        # mouse-wheel scrolling (Windows / Linux)
        canvas.bind_all("<MouseWheel>", lambda e: canvas.yview_scroll(int(-1 * e.delta / 120), "units"))
        # macOS
        canvas.bind_all("<Button-4>",   lambda e: canvas.yview_scroll(-1, "units"))
        canvas.bind_all("<Button-5>",   lambda e: canvas.yview_scroll( 1, "units"))

        log_gui_action("OSC scrollable UI initialized")

        # helpers -----------------------------------------------------------
        def _clear_host():
            for w in host.winfo_children():
                w.destroy()
            log_gui_action("OSC UI cleared")

        def _populate(addr, parent):
            prev      = selected.get(addr, {})
            cfg_step  = steps_map.get(addr)
            is_checked= cfg_step is not None

            var = prev.get("var", tk.BooleanVar(value=is_checked))
            val = prev.get("value", tk.StringVar())
            tim = prev.get("timer", tk.StringVar())

            if not prev:
                val.set(str(cfg_step.get("value", self.rows[addr]["value_var"].get())) if is_checked
                        else str(self.rows[addr]["value_var"].get()))
                tim.set(str(cfg_step.get("timer", self.rows[addr]["timer_var"].get())) if is_checked
                        else str(self.rows[addr]["timer_var"].get()))

            selected[addr] = {"var": var, "value": val, "timer": tim}

            tk.Checkbutton(parent, variable=var, bg=BG, selectcolor=ACCENT)\
                .grid(row=0, column=0, sticky="w", padx=4)
            tk.Label(parent, text=addr, bg=BG, fg=FG, anchor="w")\
                .grid(row=0, column=1, sticky="ew", padx=4)
            ttk.Entry(parent, textvariable=val, width=10)\
                .grid(row=0, column=2, sticky="e", padx=4)
            ttk.Entry(parent, textvariable=tim, width=10)\
                .grid(row=0, column=3, sticky="e", padx=4)

            parent.grid_columnconfigure(1, weight=1)
            log_gui_action("OSC control row populated", data={
                "address": addr,
                "default_value": val.get(),
                "default_timer": tim.get(),
                "checked": var.get()
            })

        def render_list(filter_txt=""):
            def worker():
                matches = [a for a in sorted(available) if filter_txt.lower() in a.lower()]
                log_gui_action("Filtering OSC controls", data={"filter": filter_txt, "match_count": len(matches)})

                def ui():
                    _clear_host()
                    header = tk.Frame(host, bg=BG)
                    header.grid(row=0, column=0, sticky="ew", pady=(0, 6))
                    header.grid_columnconfigure(1, weight=1)
                    header_use_label = tk.Label(header, text="Use", bg=BG, fg=FG)
                    header_use_label.grid(row=0, column=0, padx=4, sticky="w")
                    ToolTip(header_use_label, "Check to enable this OSC control in the chain.")

                    header_addr_label = tk.Label(header, text="Address", bg=BG, fg=FG)
                    header_addr_label.grid(row=0, column=1, padx=4, sticky="w")
                    ToolTip(header_addr_label, "OSC address to target for this control.")

                    header_val_label = tk.Label(header, text="Value", bg=BG, fg=FG)
                    header_val_label.grid(row=0, column=2, padx=4, sticky="e")
                    ToolTip(header_val_label, "OSC value to send when triggered.")

                    header_tim_label = tk.Label(header, text="Timer", bg=BG, fg=FG)
                    header_tim_label.grid(row=0, column=3, padx=4, sticky="e")
                    ToolTip(header_tim_label, "Delay in seconds before this control fires.")

                    for r, addr in enumerate(matches, start=1):
                        row = tk.Frame(host, bg=BG)
                        row.grid(row=r, column=0, sticky="ew", pady=2)
                        row.grid_columnconfigure(1, weight=1)
                        _populate(addr, row)

                win.after(0, ui)
            threading.Thread(target=worker, daemon=True).start()

        search_var.trace_add("write", lambda *_: render_list(search_var.get()))
        render_list()
        win.wait_window()

        steps = []
        for addr, info in selected.items():
            if info["var"].get():
                try:
                    v = self._normalize(info["value"].get())
                    t = float(info["timer"].get() or 0.0)
                except Exception:
                    t = 0.0
                steps.append({"addr": addr, "value": v, "timer": t})


        log_chain_system("Chain configuration finalized", action_type="editor", data={
            "step_count": len(steps),
            "trigger": cfg.get("trigger"),
            "delay": cfg.get("delay"),
            "priority": cfg.get("priority"),
            "pishock_enabled": cfg.get("pishock_enabled"),
            "gift_trigger": cfg.get("giftName") if cfg.get("trigger") == "gift" else None
        })

        log_gui_action("User completed chain configuration", data={
            "name": cfg.get("name"),
            "steps": len(steps),
            "config": cfg
        })

        print("[DEBUG] Selected device vars:", {k: v.get() for k, v in self._selected_device_vars.items()})
        return steps, cfg

# ─────────────────────────────────────────────────────────────
# Chain Upgrader (Schema Migration)
# ─────────────────────────────────────────────────────────────
    def _upgrade_chain_data(self, chains: list[dict]) -> list[dict]:
        upgraded = []
        any_changed = False

        for idx, ch in enumerate(chains):
            if not isinstance(ch, dict):
                log_controls_action(
                    "Skipping invalid chain entry",
                    data={"index": idx, "entry": repr(ch)},
                    level="WARNING"
                )
                continue

            dirty = False

            # ─────────────────────────────────────────────
            # Legacy unpacking
            # ─────────────────────────────────────────────
            steps_field = ch.get("steps")

            if (
                isinstance(steps_field, list)
                and len(steps_field) == 2
                and isinstance(steps_field[0], list)
                and isinstance(steps_field[1], dict)
            ):
                ch["steps"] = steps_field[0]
                dirty = True

            if not isinstance(ch.get("steps"), list):
                ch["steps"] = []
                dirty = True

            # ─────────────────────────────────────────────
            # Helper
            # ─────────────────────────────────────────────
            def patch(key, default):
                nonlocal dirty
                if key not in ch:
                    ch[key] = default
                    dirty = True

            # ─────────────────────────────────────────────
            # Core chain fields
            # ─────────────────────────────────────────────
            patch("name", f"Chain {idx + 1}")
            patch("delay", 0.025)
            patch("reset_before", False)
            patch("reset_after", False)
            patch("trigger", "manual")
            patch("queue_mode", True)
            patch("priority", 100)
            patch("layout_index", idx)
            patch("avatar_id", self.avatar_id)
            patch("column", None)

            # ─────────────────────────────────────────────
            # PiShock defaults
            # ─────────────────────────────────────────────
            patch("pishock_enabled", False)
            patch("pishock_only", False)
            patch("pishock_random_devices", False)
            patch("pishock_devices", [])
            patch("pishock_mode", "vibrate")
            patch("pishock_intensity", 50)
            patch("pishock_duration", 1000)
            patch("pishock_delay", 750)
            patch("pishock_step", 10)
            patch("pishock_pattern", [])

            # ─────────────────────────────────────────────
            # Intiface defaults
            # ─────────────────────────────────────────────
            patch("intiface_enabled", False)
            patch("intiface_only", False)
            patch("intiface_random_devices", False)

            mode = ch.get("intiface_mode", "vibrate")
            if mode not in {
                "vibrate",
                "pulse",
                "burst",
                "oscillation",
                "randomized_wobble",
                "pattern",
            }:
                mode = "vibrate"
                dirty = True

            ch["intiface_mode"] = mode


            # ─────────────────────────────────────────────
            # Devices (preserve actuator intent)
            # ─────────────────────────────────────────────
            if not isinstance(ch.get("intiface_devices"), list):
                ch["intiface_devices"] = []
                dirty = True

            normalized_devices = []
            for d in ch.get("intiface_devices", []):
                if not isinstance(d, dict):
                    continue

                actuator = (d.get("actuator") or "vibrate").strip().lower()
                if actuator not in ("vibrate", "constrict"):
                    actuator = "vibrate"

                normalized_devices.append({
                    "index": int(d.get("index", 0)),
                    "name": d.get("name", "Device"),
                    "actuator": actuator,
                    "step_count": d.get("step_count"),
                })

            ch["intiface_devices"] = normalized_devices


            # ─────────────────────────────────────────────
            # Intensity
            # ─────────────────────────────────────────────
            try:
                val = float(ch.get("intiface_intensity", 0.75))
                ch["intiface_intensity"] = max(0.0, min(1.0, val))
            except Exception:
                ch["intiface_intensity"] = 0.75
                dirty = True


            # ─────────────────────────────────────────────
            # Duration
            # ─────────────────────────────────────────────
            try:
                dur = float(ch.get("intiface_duration", 1.0))
                ch["intiface_duration"] = max(0.05, min(1800.0, dur))
            except Exception:
                ch["intiface_duration"] = 1.0
                dirty = True


            # ─────────────────────────────────────────────
            # Oscillation
            # ─────────────────────────────────────────────
            if mode == "oscillation":
                try:
                    step = int(ch.get("intiface_step", 10))
                    step = max(1, min(50, step))
                    ch["intiface_step"] = step

                    # 🔒 LOCK OSCILLATION CEILING
                    # This is REQUIRED or oscillation decays
                    try:
                        base = float(ch.get("intiface_intensity", 1.0))
                    except Exception:
                        base = 1.0

                    ch["intiface_osc_max"] = max(0.05, min(1.0, base))

                except Exception:
                    ch["intiface_step"] = 10
                    ch["intiface_osc_max"] = 1.0
                    dirty = True
            else:
                ch.pop("intiface_step", None)
                ch.pop("intiface_osc_max", None)


            # ─────────────────────────────────────────────
            # Randomized wobble
            # ─────────────────────────────────────────────
            if mode == "randomized_wobble":
                try:
                    mn = float(ch.get("intiface_random_min", 0.2))
                    mx = float(ch.get("intiface_random_max", 1.0))
                    mn = max(0.0, min(1.0, mn))
                    mx = max(mn, min(1.0, mx))
                except Exception:
                    mn, mx = 0.2, 1.0

                ch["intiface_random_min"] = mn
                ch["intiface_random_max"] = mx
            else:
                ch.pop("intiface_random_min", None)
                ch.pop("intiface_random_max", None)


            # ─────────────────────────────────────────────
            # Pattern migration (authoritative, safe)
            # ─────────────────────────────────────────────
            if not isinstance(ch.get("intiface_patterns"), dict):
                ch["intiface_patterns"] = {}
                dirty = True

            key = ch.get("intiface_pattern_key")
            if not isinstance(key, str) or not key:
                key = "Default"
                ch["intiface_pattern_key"] = key
                dirty = True

            legacy = ch.get("intiface_pattern")

            if isinstance(legacy, list) and legacy:
                normalized = []
                for step in legacy:
                    if not isinstance(step, dict):
                        continue

                    normalized.append({
                        "actuator": (step.get("actuator") or "vibrate").lower(),
                        "intensity": float(step.get("intensity", 1.0)),
                        "duration": float(step.get("duration", 1.0)),
                        "delay": float(step.get("delay", 0.0)),
                        "osc_step": int(step.get("osc_step", 10)),
                    })

                ch["intiface_patterns"][key] = normalized
                dirty = True

            if key not in ch["intiface_patterns"]:
                ch["intiface_patterns"][key] = []

            ch["intiface_pattern"] = ch["intiface_patterns"][key]

            # ─────────────────────────────────────────────
            # OwO (Authoritative)
            # ─────────────────────────────────────────────
            patch("owo_enabled", False)
            patch("owo_queue", True)

            mode = ch.get("owo_pattern_mode", "template")
            if mode not in ("template", "pattern"):
                mode = "template"

            ch["owo_pattern_mode"] = mode

            if mode == "template":
                ch["owo_file"] = ch.get("owo_file", "")
                ch.pop("owo_pattern", None)
            else:
                ch.pop("owo_file", None)
                if not isinstance(ch.get("owo_pattern"), list):
                    ch["owo_pattern"] = []

            # ─────────────────────────────────────────────
            # Filters
            # ─────────────────────────────────────────────
            for k, v in [
                ("diamondCountMin", None),
                ("diamondCountMax", None),
                ("gifterLevelMin", None),
                ("gifterLevelMax", None),
                ("isSubscriber", False),
                ("isModerator", False),
            ]:
                patch(k, v)

            upgraded.append(ch)
            if dirty:
                any_changed = True

        if any_changed:
            log_chain_system(
                "Chain data upgraded to latest schema",
                data={"chains": len(upgraded)},
                action_type="editor",
            )

        return upgraded

    # --- Chain Create
    def create_chain(self) -> None:
        result = self._chain_selector()
        if not result or result[0] is None:
            msg = "No steps selected."
            log_controls_action(msg, level="ERROR")
            messagebox.showerror("Error", msg)
            return

        steps, cfg = result

        # -------------------------------------------------------
        # Stand-alone detection (authoritative)
        # -------------------------------------------------------
        is_pishock_only = bool(
            cfg.get("pishock_enabled") and cfg.get("pishock_only")
        )

        is_owo_only = bool(
            cfg.get("owo_enabled")
            and not cfg.get("pishock_enabled")
        )

        is_intiface_only = bool(
            cfg.get("intiface_enabled")
            and cfg.get("intiface_only")
        )

        is_standalone = is_pishock_only or is_owo_only or is_intiface_only

        # -------------------------------------------------------
        # Validation gate
        # -------------------------------------------------------
        if not steps and not is_standalone:
            msg = (
                "Cannot create a chain with no steps.\n\n"
                "Enable one of the following to continue:\n"
                "• PiShock stand-alone\n"
                "• OwO Vest stand-alone\n"
                "• Intiface stand-alone"
            )
            log_controls_action(
                msg,
                data={
                    "steps": steps,
                    "pishock_only": is_pishock_only,
                    "owo_only": is_owo_only,
                    "intiface_only": is_intiface_only,
                },
                level="ERROR",
            )
            messagebox.showerror("Error", msg)
            return

        # -------------------------------------------------------
        # Finalize chain
        # -------------------------------------------------------
        chain = dict(cfg)
        chain["steps"] = steps

        # Default repeat behavior
        chain["allowRepeat"] = bool(cfg.get("allowRepeat", False))

        # Fallback name
        if not chain.get("name"):
            chain["name"] = f"Chain {len(load_chains()) + 1}"
            log_controls_action(
                "Assigned fallback name to new chain",
                data={"name": chain["name"]},
            )

        data = load_chains()
        data.append(chain)
        save_chains(data)

        self._register_chain(chain)

        log_chain_system(
            f"Created new chain '{chain['name']}'",
            data={
                "steps": len(steps),
                "standalone": is_standalone,
                "intiface": is_intiface_only,
                "pishock": is_pishock_only,
                "owo": is_owo_only,
            },
            action_type="runner",
        )

        messagebox.showinfo(
            "Chain created",
            f"Chain “{chain['name']}” saved and registered."
        )

    # --- Chain Edit
    def edit_chain(self, name: str) -> None:
        import copy

        chains = load_chains()
        idx = next((i for i, c in enumerate(chains) if c["name"] == name), None)
        if idx is None:
            log_controls_action(f"Chain '{name}' not found", level="ERROR")
            messagebox.showerror("Error", "Chain not found.")
            return

        chain = chains[idx]

        # Deep copy for comparison
        original_chain = copy.deepcopy(chain)

        # ─────────────────────────────────────────────
        # Normalize legacy nested step formats
        # ─────────────────────────────────────────────
        def _fix_nested_steps(steps):
            if (
                isinstance(steps, list)
                and len(steps) == 2
                and isinstance(steps[1], dict)
                and all(isinstance(s, dict) for s in steps[0])
            ):
                return steps[0]
            return steps

        chain["steps"] = _fix_nested_steps(chain.get("steps", []))

        # ─────────────────────────────────────────────
        # Launch editor
        # ─────────────────────────────────────────────
        result = self._chain_selector(
            initial_steps=chain["steps"],
            initial_config=chain,
        )

        if not result or result[0] is None:
            log_controls_action(
                f"Chain edit canceled for '{name}'",
                level="INFO"
            )
            return

        new_steps, new_cfg = result

        # ─────────────────────────────────────────────
        # Stand-alone detection (authoritative)
        # ─────────────────────────────────────────────
        is_pishock_only = bool(
            new_cfg.get("pishock_enabled") and new_cfg.get("pishock_only")
        )

        is_owo_only = bool(
            new_cfg.get("owo_enabled")
            and not new_cfg.get("pishock_enabled")
        )

        is_intiface_only = bool(
            new_cfg.get("intiface_enabled")
            and new_cfg.get("intiface_only")
        )

        is_standalone = is_pishock_only or is_owo_only or is_intiface_only

        # ─────────────────────────────────────────────
        # Validation gate
        # ─────────────────────────────────────────────
        if not new_steps and not is_standalone:
            msg = (
                "Chain must contain OSC steps unless one of the following is enabled:\n\n"
                "• PiShock stand-alone\n"
                "• OwO Vest stand-alone\n"
                "• Intiface stand-alone"
            )
            log_controls_action(
                msg,
                data={
                    "chain": name,
                    "pishock_only": is_pishock_only,
                    "owo_only": is_owo_only,
                    "intiface_only": is_intiface_only,
                },
                level="ERROR"
            )
            messagebox.showerror("Error", msg)
            return

        # ─────────────────────────────────────────────
        # Apply updates
        # ─────────────────────────────────────────────
        updated_chain = dict(new_cfg)
        updated_chain["steps"] = new_steps

        # No-op if nothing changed
        if original_chain == updated_chain:
            return

        # Unregister old chain if identity changed
        if (
            chain.get("name") != new_cfg.get("name")
            or chain.get("trigger") != new_cfg.get("trigger")
        ):
            unregister_action(chain["name"], "chains")
            self.tikfinity_registered.discard(chain["name"])

        # Apply changes
        chain.update(updated_chain)
        save_chains(chains)

        # ─────────────────────────────────────────────
        # Refresh UI
        # ─────────────────────────────────────────────
        for fr in self.chain_frames:
            fr.destroy()
        self.chain_frames.clear()

        for ch in chains:
            self._register_chain(ch)

        log_chain_system(
            f"Updated chain '{chain['name']}'",
            data={
                "old": original_chain,
                "new": updated_chain,
                "standalone": is_standalone,
            },
            action_type="editor",
        )

        messagebox.showinfo(
            "Chain Updated",
            f"Chain “{chain['name']}” updated successfully."
        )

    # --- Chain Regististration
    def _register_chain(self, chain: Dict[str, Any]) -> None:
        name = chain.get("name", "").strip()
        steps = chain.get("steps", [])
        trigger = str(chain.get("trigger", "manual")).lower()
        queue_mode = chain.get("queue_mode", True)
        reset_after = chain.get("reset_after", False)

        # ─────────────────────────────────────────────
        # Stand-alone detection
        # ─────────────────────────────────────────────
        is_pishock_only = bool(chain.get("pishock_enabled") and chain.get("pishock_only"))
        is_owo_only     = bool(chain.get("owo_enabled") and not is_pishock_only)
        is_intiface_only = bool(chain.get("intiface_enabled") and chain.get("intiface_only"))

        is_standalone = is_pishock_only or is_owo_only or is_intiface_only

        # ─────────────────────────────────────────────
        # Validation
        # ─────────────────────────────────────────────
        if not name or (not steps and not is_standalone):
            log_controls_action(
                "Attempted to register invalid chain",
                data=chain,
                level="ERROR"
            )
            return

        # ─────────────────────────────────────────────
        # Runner wrapper
        # ─────────────────────────────────────────────
        def make_chain_runner(cfg):
            def runner(_ctx=None):
                self._run_chain(
                    cfg.get("steps", []),
                    cfg.get("delay", 0.025),
                    cfg.get("reset_before", False),
                    cfg.get("reset_after", False),
                    cfg.get("queue_mode", True),
                    cfg=cfg,
                )
            return runner

        # ─────────────────────────────────────────────
        # Register execution hook
        # ─────────────────────────────────────────────
        if trigger == "gift":
            self.pending_gift_chains.append(chain)
            if queue_mode:
                register_action("chains", "Chains", name, name, make_chain_runner(chain))

        elif trigger == "random":
            dmin = float(chain.get("delay_min", 0.1))
            dmax = float(chain.get("delay_max", 0.5))
            step_limit = int(chain["step_limit"]) if chain.get("step_limit") else None
            repeat = chain.get("repeat", False)

            register_action(
                "chains",
                "Chains",
                name,
                name,
                lambda _c,
                       s=steps,
                       dmin=dmin,
                       dmax=dmax,
                       ra=reset_after:
                    self._run_random_chain(
                        s, dmin, dmax, ra,
                        step_limit,
                        repeat,
                        cfg=chain
                    )
            )
        else:
            register_action("chains", "Chains", name, name, make_chain_runner(chain))

        self.tikfinity_registered.add(name)

        # ─────────────────────────────────────────────
        # UI Construction
        # ─────────────────────────────────────────────
        glow_border = "#a020f0"
        panel_bg = "#140022"
        MAX_CHAIN_WIDTH = 500

        fr = tk.Frame(self.table, bg=BG)
        self.chain_frames.append(fr)
        fr._chain_meta = {"name": name, "avatar_id": self.avatar_id}
        fr.grid_columnconfigure(0, weight=1)

        container = tk.Frame(fr, bg=BG, width=MAX_CHAIN_WIDTH, height=50)
        container.pack(anchor="center", pady=4)
        container.pack_propagate(False)

        glow_frame = tk.Frame(container, bg=glow_border, width=MAX_CHAIN_WIDTH, height=44)
        glow_frame.pack(fill="both", expand=True, padx=2)
        glow_frame.pack_propagate(False)

        row = tk.Frame(glow_frame, bg=panel_bg, width=MAX_CHAIN_WIDTH - 4, height=40)
        row.pack(fill="both", expand=True, padx=1, pady=1)
        row.pack_propagate(False)

        accent = tk.Frame(row, bg=ACCENT, width=4)
        accent.pack(side="left", fill="y", padx=(0, 6))

        content = tk.Frame(row, bg=panel_bg)
        content.pack(side="left", fill="both", expand=True)

        # ─────────────────────────────────────────────
        # Display label + tooltip
        # ─────────────────────────────────────────────
        play_text = name
        tooltip_txt = "Run this chain"

        if trigger == "gift":
            gift = self._gift_mapping.get(chain.get("giftName"), {})
            cost = gift.get("coinCost", "?")
            play_text = f"🎁 {name} ({cost}🪙)"
            tooltip_txt = f"Triggered by gift: {chain.get('giftName')}"

        elif trigger == "custom_diamonds":
            play_text = f"💎 {name}"
            tooltip_txt = "Triggered by diamond threshold"

        if is_pishock_only:
            play_text = f"⚡ {play_text}"
            tooltip_txt = "PiShock stand-alone chain"

        elif is_owo_only:
            play_text = f"🟣 {play_text}"
            tooltip_txt = "OwO Vest stand-alone chain"

        elif is_intiface_only:
            play_text = f"🔗 {play_text}"
            tooltip_txt = (
                "Intiface stand-alone chain\n"
                f"Mode: {chain.get('intiface_mode', 'vibrate')}"
            )

        # ─────────────────────────────────────────────
        # Button
        # ─────────────────────────────────────────────
        btn_holder = tk.Frame(content, bg=panel_bg)
        btn_holder.pack(side="left", fill="both", expand=True)

        play_btn = GlowButton(
            btn_holder,
            text=play_text[:36] + "…" if len(play_text) > 38 else play_text,
            command=make_chain_runner(chain),
            glow="#a020f0",
            bg="#1a002a" if is_standalone else "#0e0020",
            fg="#ffffff",
            anchor="w",
            width=min(MAX_CHAIN_WIDTH - 100, 250)
        )
        play_btn.pack(fill="y", pady=4)
        ToolTip(play_btn, tooltip_txt)

        # ─────────────────────────────────────────────
        # Controls
        # ─────────────────────────────────────────────
        tools = tk.Frame(content, bg=panel_bg)
        tools.pack(side="right", padx=6)

        ttk.Button(tools, text="✏️", command=lambda: self.edit_chain(name), width=2).pack(side="left")
        ttk.Button(tools, text="🗑", command=lambda: self._delete_chain(name, fr), width=2).pack(side="left")

        drag = tk.Label(tools, text="🖐️", bg=panel_bg, fg=FG, cursor="fleur")
        drag.pack(side="left", padx=(2, 0))

        drag.bind("<ButtonPress-1>", lambda e, f=fr: setattr(self, "_drag_start", f))
        drag.bind("<ButtonRelease-1>", lambda e: self._on_drop_chain())

        self.chain_frames.sort(key=lambda f: f._chain_meta.get("layout_index", 0))
        self._regrid_chains()

    def _on_drop_chain(self):
        if not hasattr(self, "_drag_start"):
            return

        drag_frame = getattr(self, "_drag_start")
        y = self.winfo_pointery()
        new_index = None

        for idx, frame in enumerate(self.chain_frames):
            fy = frame.winfo_rooty()
            fh = frame.winfo_height()
            if fy <= y <= fy + fh:
                new_index = idx
                break

        if new_index is not None:
            # Move the dragged frame to the new position
            self.chain_frames.remove(drag_frame)
            self.chain_frames.insert(new_index, drag_frame)

            # Update layout_index in chain_meta for every frame
            for idx, frame in enumerate(self.chain_frames):
                frame._chain_meta["layout_index"] = idx

            self._regrid_chains()

            log_gui_action(
                "Chain UI reordered via drag and drop",
                data={"from": drag_frame._chain_meta["name"], "to_index": new_index},
                level="INFO"
            )

            # Update the layout_index in actual saved chain data
            all_chains = load_chains()
            index_map = {
                frame._chain_meta["name"]: frame._chain_meta["layout_index"]
                for frame in self.chain_frames
                if frame._chain_meta.get("avatar_id") == self.avatar_id
            }

            for ch in all_chains:
                name = ch.get("name")
                if name in index_map and ch.get("avatar_id") == self.avatar_id:
                    ch["layout_index"] = index_map[name]

            # Force save no matter what
            save_chains_force(all_chains, layout_map=index_map)

            log_chain_system(
                "Updated chain layout indexes after drag-and-drop",
                data=index_map,
                action_type="editor"
            )

        delattr(self, "_drag_start")

    # --- Chain Delete
    def _delete_chain(self, name: str, frame: tk.Frame) -> None:
        # Confirm deletion
        if not messagebox.askyesno("Confirm Delete", f"Delete chain “{name}”?"):
            log_gui_action(
                f"User canceled chain delete for “{name}”.",
                level="INFO"
            )
            return

        log_gui_action(
            f"User confirmed chain delete for “{name}”.",
            level="INFO"
        )

        # Remove from storage
        chains = load_chains()
        chains = [c for c in chains if c.get("name") != name]
        save_chains(chains)

        # Remove from UI
        frame.destroy()
        self.chain_frames = [f for f in self.chain_frames if f != frame]

        # Unregister from TikFinity if needed
        if name in self.tikfinity_registered:
            unregister_action(name, "chains")
            self.tikfinity_registered.remove(name)

            log_controls_action(
                f"Unregistered deleted chain action “{name}” from TikFinity.",
                level="INFO"
            )

        # Log system update
        log_chain_system(
            f"Deleted chain “{name}” from storage and UI.",
            data={"chain_name": name},
            action_type="editor"
        )

        self._regrid_chains()
        messagebox.showinfo("Chain Deleted", f"Chain “{name}” has been removed.")

# ───────────────────────────────────────────────────
# PiShock Random Payload Helper
# ───────────────────────────────────────────────────
    def _select_pishock_devices(self, cfg: dict, raw_devices: List[Dict]) -> List[Dict]:
        """
        Returns the list of PiShock devices to execute for this run.

        FEATURES:
        - Honors pishock_random_devices flag
        - Supports weighted random selection (device['weight'] or device['random_weight'])
        - Supports pishock_random_min / pishock_random_max
        - Optional no-repeat window (cfg['pishock_random_no_repeat_s'])
        - Hard dedupe + stable ID normalization
        - Thread-safe last-used memory with TTL cleanup
        - Stable pool ordering for deterministic behavior
        """

        import random
        import time
        import math
        import threading
        from typing import Dict, List, Tuple

        if not raw_devices:
            return []

        # ─────────────────────────────────────────────────────────
        # Ensure selector state (thread-safe)
        # ─────────────────────────────────────────────────────────
        if not hasattr(self, "_pishock_last_used_devices"):
            self._pishock_last_used_devices = {}  # dev_id -> last_ts
        if not hasattr(self, "_pishock_last_used_lock"):
            self._pishock_last_used_lock = threading.Lock()

        # ─────────────────────────────────────────────────────────
        # Normalize + dedupe device pool (stable)
        # ─────────────────────────────────────────────────────────
        pool: List[Dict] = []
        seen = set()

        for d in raw_devices:
            if not isinstance(d, dict):
                continue

            dev_id = str(d.get("id") or d.get("shockerId") or "").strip()
            if not dev_id:
                continue

            if dev_id in seen:
                continue

            seen.add(dev_id)

            # Normalize ID fields so downstream always has both
            if "id" not in d and "shockerId" in d:
                d["id"] = d.get("shockerId")
            if "shockerId" not in d and "id" in d:
                d["shockerId"] = d.get("id")

            pool.append(d)

        if not pool:
            return []

        # Stable order (deterministic selection inputs)
        pool.sort(key=lambda x: str(x.get("id") or x.get("shockerId") or ""))

        # ─────────────────────────────────────────────────────────
        # If not randomized → return all (stable order)
        # ─────────────────────────────────────────────────────────
        if not cfg.get("pishock_random_devices"):
            return pool

        # ─────────────────────────────────────────────────────────
        # Randomization parameters
        # ─────────────────────────────────────────────────────────
        try:
            rand_min = int(cfg.get("pishock_random_min", 1))
        except Exception:
            rand_min = 1

        try:
            rand_max = int(cfg.get("pishock_random_max", len(pool)))
        except Exception:
            rand_max = len(pool)

        rand_min = max(1, min(rand_min, len(pool)))
        rand_max = max(rand_min, min(rand_max, len(pool)))

        count = random.randint(rand_min, rand_max)

        try:
            no_repeat_window = float(cfg.get("pishock_random_no_repeat_s", 0.0))
        except Exception:
            no_repeat_window = 0.0

        now = time.time()

        # ─────────────────────────────────────────────────────────
        # Load + cleanup last-used memory (thread-safe)
        # ─────────────────────────────────────────────────────────
        with self._pishock_last_used_lock:
            last_used = dict(self._pishock_last_used_devices)

            # TTL cleanup: keep memory bounded
            ttl = max(30.0, no_repeat_window * 4.0) if no_repeat_window > 0 else 300.0
            cutoff = now - ttl
            if last_used:
                last_used = {k: v for k, v in last_used.items() if isinstance(v, (int, float)) and v >= cutoff}

            self._pishock_last_used_devices = last_used

        # ─────────────────────────────────────────────────────────
        # Build weighted list (with no-repeat dampening)
        # ─────────────────────────────────────────────────────────
        weighted: List[Tuple[Dict, float]] = []

        for d in pool:
            dev_id = str(d.get("id") or d.get("shockerId") or "").strip()
            if not dev_id:
                continue

            # Default weight = 1.0 (safe)
            w = 1.0
            try:
                w = float(d.get("weight") or d.get("random_weight") or 1.0)
            except Exception:
                w = 1.0

            # Sanitize weight
            if not math.isfinite(w):
                w = 1.0
            if w <= 0:
                w = 0.01

            # No-repeat dampening (soft block)
            if no_repeat_window > 0:
                last_ts = last_used.get(dev_id)
                if isinstance(last_ts, (int, float)) and (now - last_ts) < no_repeat_window:
                    # stronger penalty to actually avoid repeats in small pools
                    w *= 0.02

            w = max(0.01, min(w, 1e6))
            weighted.append((d, w))

        if not weighted:
            return []

        # ─────────────────────────────────────────────────────────
        # Weighted selection without replacement
        # ─────────────────────────────────────────────────────────
        selected: List[Dict] = []
        working = weighted[:]  # (dev, weight)

        for _ in range(count):
            if not working:
                break

            total = 0.0
            for _, w in working:
                total += w

            if total <= 0:
                # Degenerate fallback: pick first remaining (stable)
                dev, _ = working.pop(0)
                selected.append(dev)
                continue

            r = random.uniform(0.0, total)
            upto = 0.0

            pick_idx = 0
            for i, (dev, w) in enumerate(working):
                upto += w
                if upto >= r:
                    pick_idx = i
                    break

            dev, _ = working.pop(pick_idx)
            selected.append(dev)

            dev_id = str(dev.get("id") or dev.get("shockerId") or "").strip()
            if dev_id:
                with self._pishock_last_used_lock:
                    self._pishock_last_used_devices[dev_id] = now

        return selected

# ───────────────────────────────────────────────────
# Core Execution Payload Helper
# ───────────────────────────────────────────────────
    def _execute_chain_payload(self, steps, delay, reset_before, reset_after, cfg):
        """
        Runs exactly one chain end-to-end.

        IMPORTANT:
        - Per-parameter reset logic is handled ONLY by osc.send()
        - This function must NEVER reset parameters implicitly
        - Hybrid mode must run integrations + OSC in parallel
        """

        import time
        import asyncio

        start_ts = time.time()
        cfg = cfg or {}
        name = cfg.get("name", "<unnamed>")

        try:
            log_chain_system(
                f"Executor starting chain {name!r}",
                data={"trigger": cfg.get("trigger")},
                action_type="runner",
                level="INFO",
            )

            # ─────────────────────────────────────────────
            # Reset-before (explicit only)
            # ─────────────────────────────────────────────
            if reset_before:
                log_chain_system("Executor reset-before", action_type="runner", level="DEBUG")
                self._reset_avatar_parameters()

            has_steps = bool(steps)

            has_osc_activity = (
                has_steps
                or bool(cfg.get("osc_enabled", True))
            )

            is_pishock_only = (
                cfg.get("pishock_enabled")
                and cfg.get("pishock_only")
                and not cfg.get("owo_enabled")
                and not cfg.get("intiface_enabled")
                and not has_osc_activity
            )

            is_owo_only = (
                cfg.get("owo_enabled")
                and not cfg.get("pishock_enabled")
                and not cfg.get("intiface_enabled")
                and not has_osc_activity
            )

            is_intiface_only = (
                cfg.get("intiface_enabled")
                and cfg.get("intiface_only")
                and not cfg.get("pishock_enabled")
                and not cfg.get("owo_enabled")
                and not has_osc_activity
            )

            # ─────────────────────────────────────────────
            # Dispatch helpers
            # ─────────────────────────────────────────────
            def _dispatch_pishock(block: bool):
                raw_devices = cfg.get("pishock_devices", [])
                devices = self._select_pishock_devices(cfg, raw_devices)

                threads = self._run_pishock_parallel(
                    devices,
                    cfg.get("pishock_mode", "vibrate"),
                    int(cfg.get("pishock_intensity", 50)),
                    int(cfg.get("pishock_duration", 1000)),
                    delay_ms=int(cfg.get("pishock_delay", 1000)),
                    step=int(cfg.get("pishock_step", 10)),
                    pattern=cfg.get("pishock_pattern") if cfg.get("pishock_mode") == "pattern" else None,
                )

                if block:
                    for t in threads:
                        t.join()

            def _dispatch_owo(block: bool = False):
                try:
                    mode = (cfg.get("owo_pattern_mode") or "template").strip().lower()

                    # ─────────────────────────────────────────────
                    # PATTERN MODE (LIVE EXECUTION)
                    # ─────────────────────────────────────────────
                    if mode == "pattern":
                        pattern = cfg.get("owo_pattern") or []

                        if not isinstance(pattern, list) or not pattern:
                            log_chain_system(
                                "OwO pattern selected but empty",
                                level="WARNING",
                                action_type="runner",
                            )
                            return

                        # 🚀 THIS is the critical line:
                        self.owo.run_pattern(pattern)

                        # Pattern execution is already queued + timed internally
                        # Never block here — timing handled by OwO worker
                        return

                    # ─────────────────────────────────────────────
                    # TEMPLATE MODE (SDK)
                    # ─────────────────────────────────────────────
                    owo_file = (cfg.get("owo_file") or "").strip()
                    if not owo_file:
                        log_chain_system(
                            "OwO template missing",
                            level="WARNING",
                            action_type="runner",
                        )
                        return

                    self.owo.send_file(owo_file)

                except Exception as exc:
                    log_chain_system(
                        "OwO dispatch failed",
                        level="ERROR",
                        exc=exc,
                        action_type="runner",
                    )

            def _dispatch_intiface(block: bool):
                if not getattr(self, "intiface", None):
                    return

                devices = cfg.get("intiface_devices") or []
                device_ids = [
                    int(d.get("index"))
                    for d in devices
                    if isinstance(d, dict) and d.get("index") is not None
                ]

                mode = (cfg.get("intiface_mode") or "vibrate").strip().lower()

                # ─────────────────────────────────────────────
                # PATTERN MODE (AUTHORITATIVE)
                # ─────────────────────────────────────────────
                if mode == "pattern":

                    pat_key = (cfg.get("intiface_pattern_key") or "Default").strip()
                    patterns = cfg.get("intiface_patterns") or {}
                    steps = patterns.get(pat_key) or []

                    if not isinstance(steps, list) or not steps:
                        log_chain_system(
                            "Intiface pattern selected but empty",
                            level="WARN",
                            action_type="runner",
                            data={"pattern": pat_key},
                        )
                        return

                    normalized = []
                    total_dur = 0.0

                    for s in steps:
                        if not isinstance(s, dict):
                            continue

                        actuator = (s.get("actuator") or "vibrate").strip().lower()
                        if actuator not in ("vibrate", "constrict"):
                            actuator = "vibrate"

                        try:
                            inten = float(s.get("intensity", 1.0))
                        except Exception:
                            inten = 1.0
                        inten = max(0.0, min(1.0, inten))

                        try:
                            dur = float(s.get("duration", 1.0))
                        except Exception:
                            dur = 1.0
                        dur = max(0.05, min(1800.0, dur))

                        try:
                            osc = int(s.get("osc_step", 10))
                        except Exception:
                            osc = 10
                        osc = max(1, min(50, osc))

                        total_dur += dur

                        normalized.append({
                            "actuator": actuator,
                            "intensity": inten,
                            "duration": dur,
                            "osc_step": osc,
                        })

                    # Mark busy
                    try:
                        now = time.time()
                        self._intiface_busy_until = max(
                            float(getattr(self, "_intiface_busy_until", 0) or 0),
                            now + total_dur
                        )
                    except Exception:
                        pass

                    fut = asyncio.run_coroutine_threadsafe(
                        self.intiface.execute_pattern(
                            device_ids=device_ids,
                            steps=normalized,
                        ),
                        self.intiface.loop,
                    )

                    if block:
                        try:
                            fut.result(timeout=total_dur + 2.0)
                        except Exception:
                            pass

                    return
                # ─────────────────────────────────────────────
                # NON-PATTERN MODES
                # ─────────────────────────────────────────────
                try:
                    dur = float(cfg.get("intiface_duration", 1.0))
                except Exception:
                    dur = 1.0

                dur = max(0.05, min(1800.0, dur))

                # ✅ Pull oscillation parameters explicitly
                step = int(cfg.get("intiface_step", 10))
                step = max(1, min(50, step))

                osc_max = float(cfg.get("intiface_osc_max", cfg.get("intiface_intensity", 1.0)))
                osc_max = max(0.05, min(1.0, osc_max))

                # Mark busy window correctly
                try:
                    now = time.time()
                    self._intiface_busy_until = max(
                        float(getattr(self, "_intiface_busy_until", 0) or 0),
                        now + dur
                    )
                except Exception:
                    pass

                # 🔥 THIS WAS THE BUG: step + osc_max were NEVER PASSED
                fut = asyncio.run_coroutine_threadsafe(
                    self.intiface.execute_mode(
                        mode=mode,
                        device_ids=device_ids,
                        intensity=osc_max,     # ← THIS IS THE CEILING
                        duration=dur,
                        step=step,             # ← THIS CONTROLS OSCILLATION
                    ),
                    self.intiface.loop,
                )

                if block:
                    try:
                        fut.result(timeout=dur + 2.0)
                    except Exception:
                        pass

            # ─────────────────────────────────────────────
            # Standalone execution
            # ─────────────────────────────────────────────
            if is_pishock_only:
                _dispatch_pishock(block=True)
                return

            if is_owo_only:
                _dispatch_owo()
                return

            if is_intiface_only:
                _dispatch_intiface(block=True)
                return

            # ─────────────────────────────────────────────
            # Hybrid execution (correct ordering)
            # ─────────────────────────────────────────────
            if cfg.get("owo_enabled"):
                _dispatch_owo()

            if cfg.get("pishock_enabled"):
                _dispatch_pishock(block=False)

            if cfg.get("intiface_enabled"):
                _dispatch_intiface(block=False)

            # ─────────────────────────────────────────────
            # OSC execution (per-parameter logic respected)
            # ─────────────────────────────────────────────
            if has_steps:
                self._run_chain_core(
                    steps,
                    delay,
                    reset_after,
                    cfg=cfg,
                )

            # ─────────────────────────────────────────────
            # Done
            # ─────────────────────────────────────────────
            log_chain_system(
                f"Executor chain complete: {name!r}",
                {"duration_s": round(time.time() - start_ts, 3)},
                action_type="runner",
                level="INFO",
            )

        except Exception as exc:
            log_chain_system(
                "Executor exception",
                level="ERROR",
                exc=exc,
                action_type="runner",
                data={"name": name},
            )

# ───────────────────────────────────────────────────
# Core Runner for Chained Events (OSC steps + filter gate)
# ───────────────────────────────────────────────────
    def _run_chain_core(
        self,
        steps: list,
        delay: float,
        reset_after: bool,
        initial_index_offset: int = 0,
        cfg: dict | None = None,
    ) -> None:
        """
        OSC execution core.

        NOTE:
        - Integrations (PiShock / Intiface / OwO) are executed by _execute_chain_payload()
        - This function ONLY handles OSC + filtering + timers
        - Safe for hybrid + standalone modes
        """
        import time

        cfg = cfg or {}

        # ───────────────────────────────────────────────
        # Mode resolution
        # ───────────────────────────────────────────────
        is_pishock_only  = bool(cfg.get("pishock_enabled") and cfg.get("pishock_only"))
        is_intiface_only = bool(cfg.get("intiface_enabled") and cfg.get("intiface_only"))
        is_owo_only      = bool(
            cfg.get("owo_enabled")
            and not cfg.get("pishock_enabled")
            and not cfg.get("intiface_enabled")
        )

        has_any_integration = bool(
            cfg.get("pishock_enabled")
            or cfg.get("intiface_enabled")
            or cfg.get("owo_enabled")
        )

        # ───────────────────────────────────────────────
        # Filter enforcement (gift / diamond triggers)
        # ───────────────────────────────────────────────
        trigger = str(cfg.get("trigger", "")).lower()
        event_data = cfg.get("event_data", {})

        if trigger in ("gift", "custom_diamonds") and isinstance(event_data, dict):
            failed = []

            def _int(v, d=0):
                try:
                    return int(v)
                except Exception:
                    return int(d)

            ev_diamonds = _int(event_data.get("diamondCount", 0))
            ev_level    = _int(event_data.get("gifterLevel", 0))
            ev_sub      = bool(event_data.get("isSubscriber", False))
            ev_mod      = bool(event_data.get("isModerator", False))

            try:
                if cfg.get("diamondCountMin") is not None and ev_diamonds < int(cfg["diamondCountMin"]):
                    failed.append("diamondCountMin")
            except Exception:
                failed.append("diamondCountMin")

            try:
                if cfg.get("diamondCountMax") is not None and ev_diamonds > int(cfg["diamondCountMax"]):
                    failed.append("diamondCountMax")
            except Exception:
                failed.append("diamondCountMax")

            try:
                if cfg.get("gifterLevelMin") is not None and ev_level < int(cfg["gifterLevelMin"]):
                    failed.append("gifterLevelMin")
            except Exception:
                failed.append("gifterLevelMin")

            try:
                if cfg.get("gifterLevelMax") is not None and ev_level > int(cfg["gifterLevelMax"]):
                    failed.append("gifterLevelMax")
            except Exception:
                failed.append("gifterLevelMax")

            try:
                if cfg.get("isSubscriber") and not ev_sub:
                    failed.append("isSubscriber")
            except Exception:
                failed.append("isSubscriber")

            try:
                if cfg.get("isModerator") and not ev_mod:
                    failed.append("isModerator")
            except Exception:
                failed.append("isModerator")

            if failed:
                log_chain_system(
                    "Chain blocked by filters",
                    data={"failed": failed, "trigger": trigger},
                    level="INFO",
                    action_type="runner",
                )
                return

        # ───────────────────────────────────────────────
        # No OSC steps case
        # ───────────────────────────────────────────────
        if not steps:
            if is_pishock_only or is_intiface_only or is_owo_only or has_any_integration:
                log_chain_system(
                    "No OSC steps to run (integrations handled by executor)",
                    data={
                        "pishock": bool(cfg.get("pishock_enabled")),
                        "intiface": bool(cfg.get("intiface_enabled")),
                        "owo": bool(cfg.get("owo_enabled")),
                    },
                    level="DEBUG",
                    action_type="runner",
                )
            else:
                log_chain_system(
                    "[ABORT] No steps to run",
                    level="WARN",
                    action_type="runner",
                )
            return

        # ───────────────────────────────────────────────
        # Execution loop (OSCBridge is authoritative)
        # ───────────────────────────────────────────────
        for idx, step in enumerate(steps, start=1 + initial_index_offset):

            if isinstance(step, dict):
                raw_addr = step.get("addr", "")
                step_val = step.get("value", 0)
                timer_str = step.get("timer", 0)
            else:
                raw_addr, step_val, timer_str = (step + [0, 0, 0])[:3]

            raw_addr = str(raw_addr).strip()
            if not raw_addr:
                continue

            # INT-series handling
            if "::#" in raw_addr:
                base_addr, forced = raw_addr.split("::#", 1)
                try:
                    value = int(forced)
                except Exception:
                    value = 0
            else:
                base_addr = raw_addr
                try:
                    value = self._normalize(step_val)
                except Exception:
                    value = step_val

            log_chain_system(
                "Executing step",
                {
                    "addr": base_addr,
                    "value": value,
                },
                action_type="runner"
            )

            # 🔥 SINGLE SOURCE OF TRUTH 🔥
            ok = self.osc_bridge.send(
                path=base_addr,
                value=value,
                timer_secs=float(timer_str or 0),
                reset_to=None,
                pulse=False
            )

            if not ok:
                continue

            time.sleep(delay)

        log_chain_system("Chain execution complete", data=cfg, action_type="runner")

# ─────── Main Chain Runner FIFO Type (Priority + FIFO) ──────────────────────────
    def _run_chain(
        self,
        steps: list,
        delay: float,
        reset_before: bool,
        reset_after: bool,
        queue_mode: bool = True,
        cfg: dict | None = None,
    ) -> None:
        import threading, time

        cfg = cfg or {}
        trigger    = str(cfg.get("trigger", "manual")).lower()
        priority   = int(cfg.get("priority", 100) or 100)
        chain_name = str(cfg.get("name", "<unnamed>") or "<unnamed>")

        # Standalone detection
        is_pishock_only  = bool(cfg.get("pishock_enabled") and cfg.get("pishock_only"))
        is_intiface_only = bool(cfg.get("intiface_enabled") and cfg.get("intiface_only"))
        is_owo_only      = bool(
            cfg.get("owo_enabled")
            and not cfg.get("pishock_enabled")
            and not cfg.get("intiface_enabled")
        )

        is_standalone_any = bool(is_pishock_only or is_intiface_only or is_owo_only)

        # ─────────────────────────────────────────────
        # 1) RANDOM TRIGGER (bypasses queue)
        # ─────────────────────────────────────────────
        if trigger == "random":
            log_chain_system(
                "Running random-trigger chain",
                data={"name": chain_name},
                action_type="runner"
            )

            def _random_call():
                self._run_random_chain(
                    steps,
                    float(cfg.get("delay_min", 0.1)),
                    float(cfg.get("delay_max", 0.5)),
                    reset_after,
                    int(cfg["step_limit"]) if cfg.get("step_limit") else None,
                    bool(cfg.get("repeat", False)),
                    cfg=cfg,
                )

            if threading.current_thread() is getattr(self, "_chain_worker_thread", None):
                _random_call()
            else:
                threading.Thread(target=_random_call, daemon=True).start()
            return

        # ─────────────────────────────────────────────
        # 2) QUEUE MODE (Priority + FIFO)
        # ─────────────────────────────────────────────
        if queue_mode:
            log_chain_system(
                "Enqueuing chain",
                data={
                    "name": chain_name,
                    "priority": priority,
                    "standalone": is_standalone_any,
                },
                action_type="runner"
            )

            payload = (
                steps,
                float(delay or 0.0),
                bool(reset_before),
                bool(reset_after),
                cfg,
            )

            self.chain_queue.put((priority, time.time(), payload))

            try:
                self.queue_len_var.set(f"Queue: {self.chain_queue.qsize()}")
            except Exception:
                pass

            self._try_next_chain()
            return

        # ─────────────────────────────────────────────
        # 3) IMMEDIATE EXECUTION (non-queued)
        # ─────────────────────────────────────────────
        def _immediate_worker():
            try:
                # Wait for avatar capture to finish
                timeout = time.time() + 10
                while getattr(self, "_avatar_capturing", False) and time.time() < timeout:
                    time.sleep(0.05)

                try:
                    self.chain_status_var.set(f"Running: {chain_name}")
                except Exception:
                    pass

                # Reset-before happens here (mirrors queue worker)
                if reset_before:
                    try:
                        self._reset_avatar_parameters()
                    except Exception as exc:
                        log_chain_system(
                            "Reset-before failed",
                            level="WARN",
                            exc=exc,
                            action_type="runner",
                            data={"name": chain_name},
                        )

                # Single authoritative executor
                self._execute_chain_payload(
                    steps=steps,
                    delay=float(delay or 0.0),
                    reset_before=False,  # already applied
                    reset_after=bool(reset_after),
                    cfg=cfg,
                )

            finally:
                try:
                    self.chain_status_var.set("Idle")
                except Exception:
                    pass

                try:
                    self.queue_len_var.set(f"Queue: {self.chain_queue.qsize()}")
                except Exception:
                    pass

        log_chain_system(
            "Running chain immediately",
            data={
                "name": chain_name,
                "standalone": is_standalone_any,
                "queue_mode": False,
            },
            action_type="runner",
        )

        if threading.current_thread() is getattr(self, "_chain_worker_thread", None):
            _immediate_worker()
        else:
            threading.Thread(target=_immediate_worker, daemon=True).start()

# ────── Random Mode Runner For Chain Trigger ─────────────────────────────
    def _run_random_chain(
        self,
        steps: list[dict],
        delay_min: float,
        delay_max: float,
        reset_after: bool,
        step_limit: int | None = None,
        repeat: bool = False,
        cfg: dict | None = None,
    ) -> None:
        import random
        import time
        import threading
        import inspect
        import asyncio

        cfg = cfg or {}
        chain_name = cfg.get("name", "<unnamed>")

        delay_min = float(delay_min or 0.0)
        delay_max = float(delay_max or delay_min)
        delay_min = max(0.0, delay_min)
        delay_max = max(delay_min, delay_max)

        # ─────────────────────────────────────────────
        # Helpers
        # ─────────────────────────────────────────────
        def _sleep_jitter():
            if delay_max > 0:
                time.sleep(random.uniform(delay_min, delay_max))

        def _norm_intiface_pattern_steps(cfg: dict) -> tuple[list[dict], float]:
            pat_key = (cfg.get("intiface_pattern_key") or "Default").strip()
            patterns = cfg.get("intiface_patterns") or {}
            raw = patterns.get(pat_key) or cfg.get("intiface_pattern") or []

            if not isinstance(raw, list):
                raw = []

            normalized = []
            total = 0.0

            for s in raw:
                if not isinstance(s, dict):
                    continue

                actuator = (s.get("actuator") or "vibrate").strip().lower()
                if actuator not in ("vibrate", "constrict"):
                    actuator = "vibrate"

                try:
                    inten = float(s.get("intensity", 1.0))
                except Exception:
                    inten = 1.0
                inten = inten / 100.0 if inten > 1.0 else inten
                inten = max(0.0, min(1.0, inten))

                try:
                    dur = float(s.get("duration", 1.0))
                except Exception:
                    dur = 1.0
                dur = max(0.05, min(1800.0, dur))
                total += dur

                try:
                    delay = float(s.get("delay", 0.0))
                except Exception:
                    delay = 0.0
                delay = max(0.0, min(60.0, delay))

                try:
                    osc = int(s.get("osc_step", 10))
                except Exception:
                    osc = 10
                osc = max(1, min(50, osc))

                normalized.append({
                    "actuator": actuator,
                    "intensity": inten,
                    "duration": dur,
                    "delay": delay,
                    "osc_step": osc,
                })

            return normalized, total

        # ─────────────────────────────────────────────
        # Mode detection
        # ─────────────────────────────────────────────
        has_steps = bool(steps)

        is_pishock_only = (
            cfg.get("pishock_enabled")
            and cfg.get("pishock_only")
            and not cfg.get("owo_enabled")
            and not cfg.get("intiface_enabled")
            and not has_steps
        )

        is_owo_only = (
            cfg.get("owo_enabled")
            and not cfg.get("pishock_enabled")
            and not cfg.get("intiface_enabled")
            and not has_steps
        )

        is_intiface_only = (
            cfg.get("intiface_enabled")
            and cfg.get("intiface_only")
            and not cfg.get("pishock_enabled")
            and not cfg.get("owo_enabled")
            and not has_steps
        )

        if not steps and not (is_pishock_only or is_intiface_only or is_owo_only):
            log_chain_system(
                "Random chain skipped — nothing to execute",
                level="WARN",
                action_type="runner",
            )
            return

        # ─────────────────────────────────────────────
        # Runner
        # ─────────────────────────────────────────────
        def _run_body():
            log_chain_system(
                "Starting random chain",
                data={
                    "name": chain_name,
                    "repeat": repeat,
                    "step_limit": step_limit,
                    "mode": (
                        "pishock-only" if is_pishock_only else
                        "intiface-only" if is_intiface_only else
                        "owo-only" if is_owo_only else
                        "hybrid"
                    ),
                },
                action_type="runner",
            )

            # ─────────────────────────────
            # INTIFACE ONLY
            # ─────────────────────────────
            if is_intiface_only:
                if not getattr(self, "intiface", None):
                    return

                devices = cfg.get("intiface_devices") or []
                device_ids = [
                    int(d.get("index"))
                    for d in devices
                    if isinstance(d, dict) and d.get("index") is not None
                ]

                while True:
                    mode = (cfg.get("intiface_mode") or "vibrate").strip().lower()

                    if mode == "pattern":
                        steps_norm, total = _norm_intiface_pattern_steps(cfg)

                        if not steps_norm:
                            log_chain_system(
                                "Random Intiface pattern empty",
                                level="WARN",
                                action_type="runner",
                            )
                            return

                        try:
                            now = time.time()
                            self._intiface_busy_until = max(
                                float(getattr(self, "_intiface_busy_until", 0) or 0),
                                now + total,
                            )
                        except Exception:
                            pass

                        fut = asyncio.run_coroutine_threadsafe(
                            self.intiface.execute_pattern(
                                device_ids=device_ids,
                                steps=steps_norm,
                            ),
                            self.intiface.loop,
                        )

                        try:
                            fut.result(timeout=total + 2.0)
                        except Exception as exc:
                            log_chain_system(
                                "Random Intiface pattern failed",
                                level="ERROR",
                                exc=exc,
                                action_type="runner",
                            )

                    else:
                        # ───── FIXED PART ─────
                        try:
                            dur = float(cfg.get("intiface_duration", 1.0))
                        except Exception:
                            dur = 1.0

                        dur = max(0.05, min(1800.0, dur))

                        step = int(cfg.get("intiface_step", 10))
                        step = max(1, min(50, step))

                        osc_max = float(cfg.get("intiface_intensity", 0.75))
                        osc_max = max(0.05, min(1.0, osc_max))

                        try:
                            now = time.time()
                            self._intiface_busy_until = max(
                                float(getattr(self, "_intiface_busy_until", 0) or 0),
                                now + dur
                            )
                        except Exception:
                            pass

                        fut = asyncio.run_coroutine_threadsafe(
                            self.intiface.execute_mode(
                                mode=mode,
                                device_ids=device_ids,
                                intensity=osc_max,   # ceiling
                                duration=dur,
                                step=step,           # ← REQUIRED FOR OSCILLATION
                            ),
                            self.intiface.loop,
                        )

                        try:
                            fut.result(timeout=dur + 2.0)
                        except Exception as exc:
                            log_chain_system(
                                "Random Intiface execution failed",
                                level="ERROR",
                                exc=exc,
                                action_type="runner",
                            )

                    if not repeat:
                        break

                    _sleep_jitter()

                return

            # ─────────────────────────────
            # OWO ONLY
            # ─────────────────────────────
            if is_owo_only:
                while True:
                    mode = (cfg.get("owo_pattern_mode") or "template").strip().lower()

                    if mode == "pattern":
                        pattern = cfg.get("owo_pattern") or []
                        try:
                            self.owo.send_pattern(pattern)
                        except Exception:
                            pass
                    else:
                        owo_file = (cfg.get("owo_file") or "").strip()
                        if owo_file:
                            try:
                                self.owo.send_file(owo_file)
                            except Exception:
                                pass

                    if not repeat:
                        break
                    _sleep_jitter()

                return

            # ─────────────────────────────
            # HYBRID RANDOM MODE
            # ─────────────────────────────
            while True:
                shuffled = list(steps)
                random.shuffle(shuffled)

                if step_limit is not None:
                    try:
                        shuffled = shuffled[: int(step_limit)]
                    except Exception:
                        pass

                for step in shuffled:
                    try:
                        self._execute_chain_payload(
                            steps=[step],
                            delay=0.0,
                            reset_before=False,
                            reset_after=False,
                            cfg=cfg,
                        )
                    except Exception as exc:
                        log_chain_system(
                            "Random hybrid step failed",
                            level="ERROR",
                            exc=exc,
                            action_type="runner",
                        )

                    _sleep_jitter()

                if reset_after:
                    try:
                        self._reset_avatar_parameters()
                    except Exception:
                        pass

                if not repeat:
                    break

        # ─────────────────────────────────────────────
        # Thread dispatch
        # ─────────────────────────────────────────────
        if inspect.current_thread() is getattr(self, "_chain_worker_thread", None):
            _run_body()
        else:
            threading.Thread(target=_run_body, daemon=True).start()

# ───── Queue Mode Daemon Threaded Worker (DEV BUILD: Priority + FIFO + Tandem Integrations) ─────────────────
    def _try_next_chain(self) -> None:
        import threading
        import time

        log_chain_system(
            "Queue worker setup: initializing",
            action_type="queue",
            level="INFO"
        )

        # Prevent duplicate workers
        if getattr(self, "_chain_worker_thread", None) and self._chain_worker_thread.is_alive():
            log_chain_system(
                "Queue worker already running - skipping spawn",
                action_type="queue",
                level="DEBUG"
            )
            return

        # ─────────────────────────────────────────────
        # Unified busy gate (PiShock + Intiface + OwO)
        # ─────────────────────────────────────────────
        def _get_busy_until() -> float:
            bu = 0.0

            # PiShock
            try:
                bu = max(bu, float(getattr(getattr(self, "pishock", None), "_busy_global", 0) or 0))
            except Exception:
                pass

            # Intiface
            try:
                bu = max(bu, float(getattr(self, "_intiface_busy_until", 0) or 0))
            except Exception:
                pass

            # OwO
            try:
                owo = getattr(self, "owo", None)
                if owo:
                    bu = max(bu, float(getattr(owo, "_busy_until", 0) or 0))
                    bu = max(bu, float(getattr(owo, "_owo_busy_until", 0) or 0))
            except Exception:
                pass

            return float(bu or 0.0)

        def _sleep_until(ts: float, reason: str):
            now = time.time()
            if ts <= now:
                return
            wait = ts - now
            log_chain_system(
                f"⏳ Queue delay: {reason}",
                {"wait_s": round(wait, 3)},
                action_type="queue",
                level="DEBUG"
            )
            time.sleep(wait)

        # ─────────────────────────────────────────────
        # Worker thread
        # ─────────────────────────────────────────────
        def _worker():
            log_chain_system("Chain worker thread started", action_type="queue", level="INFO")
            self.chain_running = True

            while True:
                # Global busy gate
                try:
                    _sleep_until(_get_busy_until(), "integrations busy")
                except Exception:
                    pass

                # Pause support
                if getattr(self, "_chain_worker_paused", False):
                    time.sleep(0.05)
                    continue

                # Dequeue
                try:
                    priority, ts, payload = self.chain_queue.get()
                except Exception:
                    time.sleep(0.05)
                    continue

                try:
                    steps, delay, reset_before, reset_after, cfg = payload
                except Exception:
                    self.chain_queue.task_done()
                    continue

                cfg = cfg or {}
                name = cfg.get("name", "<unnamed>")
                start_ts = time.time()

                try:
                    self.chain_status_var.set(f"Running: {name}")
                except Exception:
                    pass

                try:
                    # ────────── Pre-run gate ──────────
                    _sleep_until(_get_busy_until(), "pre-run sync")

                    # Reset-before
                    if reset_before:
                        try:
                            self._reset_avatar_parameters()
                        except Exception:
                            pass

                    # ────────── EXECUTION ──────────
                    self._execute_chain_payload(
                        steps=steps or [],
                        delay=float(delay or 0.0),
                        reset_before=False,
                        reset_after=bool(reset_after),
                        cfg=cfg,
                    )

                except Exception as exc:
                    log_chain_system(
                        "Chain execution error",
                        level="ERROR",
                        exc=exc,
                        action_type="runner",
                        data={"name": name},
                    )

                finally:
                    try:
                        self.chain_queue.task_done()
                    except Exception:
                        pass

                    # Update UI
                    try:
                        q = self.chain_queue.qsize()
                        self.chain_status_var.set("Idle" if q == 0 else "Waiting…")
                        self.queue_len_var.set(f"Queue: {q}")
                    except Exception:
                        pass

                    # ────────── Tail gating (CRITICAL) ──────────
                    try:
                        longest_timer = 0.0
                        if steps:
                            for s in steps:
                                if isinstance(s, dict):
                                    longest_timer = max(
                                        longest_timer,
                                        float(s.get("timer", 0) or 0)
                                    )

                        now = time.time()
                        busy = _get_busy_until()

                        if longest_timer:
                            busy = max(busy, now + longest_timer)

                        # micro jitter
                        _sleep_until(busy + 0.02, "post-run tail")
                    except Exception:
                        pass

            self.chain_running = False

        # ─────────────────────────────────────────────
        # Spawn worker
        # ─────────────────────────────────────────────
        self._chain_worker_thread = threading.Thread(
            target=_worker,
            daemon=True
        )
        self._chain_worker_thread.start()

        log_chain_system(
            "🔨 Chain worker thread spawned",
            action_type="queue",
            level="INFO"
        )

# ─────────────────────────────────────────────────────────────

    def _reset_param(self, addr: str, value: Any):
        try:
            self.osc_bridge.send(addr, value)
            log_osc_core(
                f"Reset parameter {addr} ← {value} (post-chain delay)",
                level="INFO",
                action_type="send"
            )
        except Exception as e:
            log_osc_core(
                f"Reset failed: {addr} ← {value}",
                level="WARNING",
                action_type="send",
                exc=e
            )

    def _repack_table(self) -> None:
        for idx, (addr, row) in enumerate(self.rows.items(), start=1):
            row["widgets"]["checkbox"].grid(row=idx, column=0, sticky="w", padx=2)
            row["widgets"]["label"].grid(row=idx, column=1, sticky="w", padx=2)
            row["widgets"]["default"].grid(row=idx, column=2, padx=2)
            row["widgets"]["value"].grid(row=idx, column=3, padx=2)
            row["widgets"]["timer"].grid(row=idx, column=4, padx=2)
            row["widgets"]["send_btn"].grid(row=idx, column=5, padx=2)

        self._regrid_chains()

# ───────────────────────────────────────────────────────── 8. heartbeat / queue
    def _heartbeat(self) -> None:
        now = time.time()
        delta = now - getattr(self, "last_osc", 0)

        if delta < 2.5:
            style = "StatusConnected.TLabel"
            text = "Connected"

        elif getattr(self, "_osc_advertised", False):
            style = "StatusWaiting.TLabel"
            text = "Waiting"

        else:
            style = "StatusDisconnected.TLabel"
            text = "Idle"

        self.osc_lbl.config(text=text, style=style)
        self.after(1000, self._heartbeat)

# ---------------------------------------------------------------- helper callbacks
    def _pump_q(self) -> None:
        try:
            while True:
                item = q.get_nowait()

                # Ensure it's a 3-tuple (svc, ev, data)
                if not isinstance(item, tuple) or len(item) != 3:
                    log_osc_core(
                        "Invalid queue item structure",
                        level="warning",
                        data={"item": item}
                    )
                    continue

                svc, ev, data = item
                now = time.time()

                # ───────────────────────────────────────── Track last OSC activity
                if svc == "osc" and ev == "msg":
                    self.last_osc = now

                # ───────────────────────────────────────── Service-specific handlers
                try:
                    if svc == "intiface":
                        self._handle_intiface_event(ev, data)

                    elif svc == "pishock":
                        self._handle_pishock_event(ev, data)

                    elif svc == "tikfinity":
                        self._handle_tikfinity_event(ev, data)

                    elif svc == "tiktok":
                        self._handle_tiktok_event(ev, data)

                    elif svc == "osc":
                        self._handle_osc_event(ev, data)

                except Exception as exc:
                    log_gui_action(
                        "Service handler failed",
                        data={"service": svc, "event": ev, "data": data},
                        level="ERROR",
                        exc=exc
                    )

                # ───────────────────────────────────────── Log panel output
                try:
                    self.log.configure(state="normal")
                    self.log.insert(
                        "1.0",
                        f"[{svc.upper()}] {ev} → {json.dumps(data, default=str)}\n"
                    )
                    self.log.configure(state="disabled")
                except Exception:
                    pass

                # ───────────────────────────────────────── OSC avatar change handler
                if svc == "osc" and ev == "msg":
                    addr = data.get("addr", "")
                    args = data.get("args", [])

                    if addr == "/avatar/change":
                        avatar_id = str(args[0]).strip() if args else ""
                        if not avatar_id:
                            continue

                        last_sent = str(getattr(self, "_last_avatar_change_sent", "")).strip()
                        last_time = getattr(self, "_last_avatar_change_time", 0)

                        # Echo suppression
                        if avatar_id == last_sent and (now - last_time) < 2.0:
                            avatar_log_verbose(
                                "Echo suppression: ignoring self-sent avatar change",
                                data={
                                    "avatar_id": avatar_id,
                                    "last_sent": last_sent,
                                    "time_diff": now - last_time
                                }
                            )
                            continue

                        if (
                            avatar_id != getattr(self, "avatar_id", None)
                            or (now - getattr(self, "_last_avatar_evt", 0)) > 1
                        ):
                            self._last_avatar_evt = now
                            avatar_log_verbose(
                                "Avatar changed",
                                data={"avatar_id": avatar_id}
                            )
                            self._on_avatar_change(avatar_id)

                # ───────────────────────────────────────── TikFinity connect handling (HALF-LIVE)
                # No registry cache. No re-register. Rows are authoritative.
                if svc == "tikfinity" and ev == "connected":
                    avatar_log_verbose("TikFinity connected (half-live mode).")

        except queue.Empty:
            pass

        # schedule next pump
        self.after(20, self._pump_q)

# ───────────────────────────────────────────────── Clear Queue ────────────
    def _clear_queue(self):
        """Forcefully clear all pending items in both queues immediately."""
        cleared_gui = 0
        cleared_chain = 0

        # Pause GUI pump
        self._pump_enabled = False

        # Pause chain worker
        self._chain_worker_paused = True

        # Clear GUI queue
        try:
            while True:
                self.q.get_nowait()
                cleared_gui += 1
        except queue.Empty:
            pass

        # Clear chain queue safely
        with self.chain_queue.mutex:
            cleared_chain = len(self.chain_queue.queue)
            self.chain_queue.queue.clear()

        # Resume queues
        self._pump_enabled = True
        self._chain_worker_paused = False

        # Log results
        log_gui_action("Queues cleared", data={
            "gui_items_removed": cleared_gui,
            "chain_items_removed": cleared_chain
        })
        self.log.configure(state="normal")
        self.log.insert(
            "1.0",
            f"[SYSTEM] Cleared {cleared_gui} GUI events and {cleared_chain} chain events\n"
        )
        self.log.configure(state="disabled")
        
# ───────────────────────────────────────────────── avatar snapshot ────────────
    def _on_avatar_change(self, avatar_id: str) -> None:
        if not _avatar_change_lock.acquire(blocking=False):
            avatar_log_verbose(
                "Avatar change already in progress - skipping duplicate",
                data={"avatar_id": avatar_id},
            )
            return

        try:
            avatar_id = str(avatar_id).strip()

            # ── Echo suppression (self-triggered changes) ─────────────────────
            if avatar_id == str(getattr(self, "_last_avatar_change_sent", "")).strip():
                avatar_log_verbose(
                    "Echo suppression: ignoring self-sent avatar change",
                    data={"avatar_id": avatar_id},
                )
                self._last_avatar_change_sent = None
                return

            avatar_log_verbose(
                "Avatar change received",
                data={"avatar_id": avatar_id},
            )

            # ── Reset avatar state ────────────────────────────────────────────
            self.avatar_id = avatar_id
            self._avatar_ready = False
            self.avatar_live_params.clear()

            user_cfg["current_avatar_id"] = avatar_id
            save_user_cfg(user_cfg)

            # ── Resolve avatar JSON path (known → auto → scan) ─────────────────
            avatar_json_path = None
            avatar_entry = user_cfg.get("avatars", {}).get(avatar_id)

            if isinstance(avatar_entry, dict):
                avatar_json_path = avatar_entry.get("path")
            elif isinstance(avatar_entry, str):
                avatar_json_path = avatar_entry

            if avatar_json_path and os.path.exists(avatar_json_path):
                avatar_log_verbose(
                    "Auto-loading avatar from saved path",
                    data={"path": avatar_json_path},
                )
            else:
                avatar_json_path = find_avatar_json(avatar_id)
                if avatar_json_path:
                    avatar_log_verbose(
                        "Auto-importing avatar from detected path",
                        data={"path": avatar_json_path},
                    )

            if not avatar_json_path or not os.path.exists(avatar_json_path):
                avatar_log_verbose(
                    "Avatar change aborted - no avatar JSON found",
                    level="WARN",
                    data={"avatar_id": avatar_id},
                )
                return

            # ── Clear existing rows (avatar scope) ────────────────────────────
            self.rows.clear()

            # ── Build rows from avatar JSON (ROWS ARE TRUTH) ──────────────────
            build_rows_from_avatar_json(self, avatar_json_path)

            # ── Fetch live OSC parameters (half-live overlay) ─────────────────
            if getattr(self, "osc_bridge", None):
                raw_params = self.osc_bridge.fetch_parameters()
                flat_params = {
                    addr: val
                    for addr, val in flatten_params(raw_params)
                    if addr not in NOISY_PARAMETERS
                }

                if not flat_params:
                    avatar_log_verbose(
                        "OSC bridge returned 0 parameters",
                        level="WARN",
                        data={"avatar_id": avatar_id},
                    )
                else:
                    self.avatar_live_params.update(flat_params)
                    log_osc_core(
                        "Fetched parameters from OSC bridge",
                        data={"count": len(self.avatar_live_params)},
                    )

            # ── ONE global sync (authoritative) ───────────────────────────────
            sync_tikfinity_registry(self)

            # ── Finalize avatar state ─────────────────────────────────────────
            self._schedule_snapshot_commit()
            self._avatar_ready = True

            avatar_log_verbose(
                "Avatar change fully applied",
                level="INFO",
                data={
                    "avatar_id": avatar_id,
                    "rows": len(self.rows),
                },
            )

        finally:
            _avatar_change_lock.release()

# ───────────────────────────────────────────────── LEGACY avatar snapshot ────────────
    def _save_avatar_snapshot(self) -> None:
        try:
            aid = getattr(self, "avatar_id", None)
            if not aid:
                avatar_log_verbose("Attempted to save snapshot without avatar_id", level="WARN")
                return

            # --- Fetch current live parameters via bridge --------------------------
            if hasattr(self, "osc_bridge") and self.osc_bridge:
                raw_params = self.osc_bridge.fetch_parameters()
                flat_params = {
                    addr: val for addr, val in flatten_params(raw_params)
                    if addr not in NOISY_PARAMETERS
                }
                self.avatar_live_params = flat_params

            # --- assemble payload ------------------------------------------------
            snapshot = {
                "id": aid,
                "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "parameters": dict(self.avatar_live_params)  # shallow copy
            }

            # --- ensure dir & write per-avatar file -----------------------------
            os.makedirs(AVATAR_STATE_DIR, exist_ok=True)
            archive_path = os.path.join(AVATAR_STATE_DIR, f"{aid}.json")
            with open(archive_path, "w", encoding="utf-8") as fh:
                json.dump(snapshot, fh, indent=2)

            # --- write “current avatar” pointer ---------------------------------
            with open(AVATAR_STATE_FILE, "w", encoding="utf-8") as fh:
                json.dump({"id": aid, "timestamp": snapshot["timestamp"]}, fh, indent=2)

            # --- record JSON path in user_cfg for future auto-load --------------
            json_path = find_avatar_json(aid)
            if json_path:
                user_cfg.setdefault("avatars", {})[aid] = json_path
                os.makedirs(CFG_DIR, exist_ok=True)
                with open(USER_CFG_FILE, "w", encoding="utf-8") as fh:
                    json.dump(user_cfg, fh, indent=2)
                avatar_log_verbose("Avatar path registered", data={"avatar_id": aid, "path": json_path})

            avatar_log_verbose("Avatar snapshot saved",
                               data={"avatar_id": aid, "param_count": len(snapshot["parameters"])})

            self._auto_register_assets()

        except Exception as exc:  # noqa: BLE001
            avatar_log_verbose("Saving avatar snapshot failed", level="ERROR", data={"error": str(exc)})

# ───────────────────────────────────────────────── NEW avatar snapshot ────────────
    def _schedule_snapshot_commit(self) -> None:
        """
        Debounced snapshot writer.
        Only runs if major dirty flag is set.
        """
        if not self._avatar_tx_major_dirty:
            avatar_log_verbose("Snapshot commit skipped – no major changes", level="DEBUG")
            return

        if self._snapshot_debounce_job:
            try:
                self.after_cancel(self._snapshot_debounce_job)
            except Exception:
                pass

        def _commit():
            try:
                avatar_log_verbose(
                    "Committing avatar snapshot (major change detected)",
                    data={
                        "avatar_id": self.avatar_id,
                        "rows": len(self.rows),
                        "chains": len(self.chains),
                        "params": len(self.avatar_live_params)
                    }
                )
                self._save_avatar_snapshot()
                self._avatar_tx_major_dirty = False
                avatar_log_verbose("Avatar snapshot committed successfully", level="INFO")
            except Exception as e:
                avatar_log_verbose("Avatar snapshot commit failed", level="ERROR", exc=e)

        # 1s debounce to avoid storms
        self._snapshot_debounce_job = self.after(1000, _commit)

#──────────────────────────────── auto INT / gift registration ──────────────────────
    def _auto_register_assets(self) -> None:
        import os

        def flatten_params(params: dict, prefix: str = "") -> list[tuple[str, any]]:
            flat = []
            if not isinstance(params, dict):
                return flat
            full = params.get("FULL_PATH", prefix)
            if "VALUE" in params and isinstance(params["VALUE"], list):
                flat.append((full, params["VALUE"][0]))
            for sub in params.get("CONTENTS", {}).values():
                flat.extend(flatten_params(sub, full))
            return flat

        # ── Load gift mapping if missing ─────────────────────────────
        if not getattr(self, "_gift_mapping", None):
            self._gift_mapping = self._load_gift_mapping()

        # ── Ensure chains file exists ────────────────────────────────
        if not os.path.isfile(CHAINS_FILE):
            save_chains([])
            log_chain_system("Created empty chain file", data={"file": CHAINS_FILE})

        # ── Flatten OSC params ───────────────────────────────────────
        if hasattr(self, "osc_bridge") and self.osc_bridge:
            raw_params = self.osc_bridge.fetch_parameters() or {}
            live_params = {addr: val for addr, val in flatten_params(raw_params)}
            log_osc_core("Fetched parameters from OSC bridge", data={"count": len(live_params)})
        else:
            live_params = {}

        # ── INT series auto-injection ────────────────────────────────
        int_series_injected = False
        for path in live_params:
            if path.startswith("/avatar/parameters/twitch"):
                self.create_int_series_rows(path)
                log_controls_action("Injected INT-series rows", data={"root_path": path})
                int_series_injected = True
                break
        if not int_series_injected:
            log_controls_action("No INT-series root found in parameters", level="INFO")

        # ── Save current control layout ──────────────────────────────
        control_list = []
        for addr, row in self.rows.items():
            control_list.append({
                "addr": addr,
                "default": [row["default"]],
                "value": [self._normalize(row["value_var"].get())],
                "timer": float(row["timer_var"].get() or 0)
            })

        if control_list:
            save_controls(control_list)
            log_controls_action("Saved control layout", data={"control_count": len(control_list)})
        else:
            log_controls_action("Refused to save controls - empty list", level="WARN")

        # ── Auto-generate gift chains if matching params exist ───────
        chains = load_chains()
        chain_names = {c["name"] for c in chains}
        already_registered_paths = set(self.rows.keys())
        added_chain = False

        for full_path in live_params:
            if not full_path.startswith("/avatar/parameters/Gifts/"):
                continue

            gift_key = full_path.rsplit("/", 1)[-1]
            if gift_key in {"Ghost", "Pumpkin"}:
                continue

            delay = HYROE_GIFTS.get(full_path, 2.5)
            gift_name = gift_key.replace("_", " ")

            mapping_key = next(
                (k for k in self._gift_mapping if k.lower() == gift_name.lower()),
                gift_name
            )

            if mapping_key in chain_names:
                log_chain_system("Skipped duplicate gift chain", data={"name": mapping_key})
                continue

            info = self._gift_mapping.get(mapping_key, {})
            icon_url = info.get("image", "")
            cost = info.get("coinCost", 0)

            chain = {
                "name": mapping_key,
                "icon": icon_url,
                "coinCost": cost,
                "trigger": "gift",
                "giftName": mapping_key,
                "giftCount": 1,
                "queue_mode": True,
                "delay": 0.025,
                "reset_after": True,
                "allowRepeat": True,
                "steps": [
                    {"addr": full_path, "value": 1, "timer": delay}
                ],
            }

            chains.append(chain)
            self._register_chain(chain)
            log_chain_system("Auto-registered gift chain", data={"name": mapping_key, "address": full_path})
            added_chain = True

        if added_chain:
            save_chains(chains)
            log_chain_system("Saved updated chains file", data={"count": len(chains), "file": CHAINS_FILE})




# ─────────────────────────────────────────────
# LICENSE CHECK (ALWAYS TRUE)
# ─────────────────────────────────────────────

def _check_license_valid() -> bool:
    """
    MIT build.
    Always valid.
    """
    return True


# ─────────────────────────────────────────────
# LEGACY STUBS (NO-OP)
# ─────────────────────────────────────────────

def show_license_gate(reason: str) -> bool:
    return True


def show_license_entry_screen() -> bool:
    return True


def _show_trial_active():
    return


# ─────────────────────────────────────────────
# APPLICATION ENTRY (UNCHANGED FLOW)
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import os
    import signal
    import threading
    import tkinter as tk
    from tkinter import messagebox

    # ─────────────────────────────────────────────
    # 1) LICENSE CHECK (NO-OP)
    # ─────────────────────────────────────────────
    _check_license_valid()

    # ─────────────────────────────────────────────
    # 2) OPTIONAL INFO BANNER (DISABLED)
    # ─────────────────────────────────────────────
    # Intentionally disabled in MIT build

    # ─────────────────────────────────────────────
    # 3) NORMAL BOOT
    # ─────────────────────────────────────────────

    migrate_avatar_controls()
    migrate_chain_files()

    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=run_external_flask, daemon=True).start()
    start_streamerbot_ws_listener(8080)
    start_docs_server()
    threading.Thread(target=osc_thread, daemon=True).start()

    app = Dash()
    set_ui_dispatcher(lambda cb: app.after_idle(cb))

    threading.Thread(
        target=lambda: tikfinity_listener_thread(app),
        daemon=True
    ).start()

    app.after_idle(app.start_pishock)

    # ─────────────────────────────────────────────
    # FINALIZE CONTROLS
    # ─────────────────────────────────────────────

    def finalize_controls():
        auto_register_chains_as_external_hooks(app)
        app._ready = True

        for addr, row in app.rows.items():
            send_addr = row["send_addr"]
            value_var = row["value_var"]
            timer_var = row["timer_var"]

            register_action(
                row["category_id"],
                row["category_name"],
                row["action_id"],
                row["action_name"],
                lambda _c,
                       a=send_addr,
                       v=value_var,
                       t=timer_var:
                    app.send_osc(
                        a,
                        app._normalize(v.get()),
                        t.get()
                    )
            )

        print("[INIT] Controls registered (MIT mode)")

    app.after(1000, finalize_controls)
    sync_tikfinity_registry(app)

    # ─────────────────────────────────────────────
    # CLEAN SHUTDOWN
    # ─────────────────────────────────────────────

    def force_exit(*_):
        try:
            app._on_close()
        except Exception:
            pass
        os._exit(0)

    signal.signal(signal.SIGINT, force_exit)
    signal.signal(signal.SIGTERM, force_exit)
    app.protocol("WM_DELETE_WINDOW", force_exit)

    app.mainloop()