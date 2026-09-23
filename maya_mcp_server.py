# -*- coding: utf-8 -*-
"""MCP Socket for Maya — TCP bridge running inside Maya (mcp-socket family).

Replaces the old HTTP bridge on the same port; wire protocol is the shared
mcp-socket / blender-mcp 1.6.x one, so the stdio server (maya_mcp.py) from
the Blender side of the family talks to it unchanged:

  client sends   {"type": "<command>", "params": {...}}
  server replies {"status": "success", "result": ...}
                 {"status": "error",   "message": "..."}

No framing byte — both sides accumulate bytes and try json.loads until the
document parses whole.

Commands
    ping               versions, pid, scene, counts, actual port
    get_scene_info     name, units, up-axis, frame range, counts, top nodes
    get_hierarchy      DAG tree (paths, types, visibility; capped)
    get_screenshot     PHYSICAL screen capture (QScreen.grabWindow) —
                       mode "window" crops the Maya window, "screen" keeps
                       the full screen. This is the CopyFromScreen class of
                       capture; QWidget.grab() would lie on scaled monitors.
    get_console_log    ring buffer of stdout/stderr captured per execution
    clear_console_log
    execute_maya_code  eval→exec in a fresh namespace with cmds/om/mel/omui
                       preinjected; captures stdout+stderr, returns the
                       optional ``result`` variable; optional undo chunk

Threading: the socket lives on a daemon thread, but cmds and Qt are
main-thread-only — every command is marshalled through
``maya.utils.executeInMainThreadWithResult``.

Autostart chain is unchanged: userSetup.py → mcp_startup.py → start().
"""

from __future__ import annotations

import io
import json
import os
import socket
import tempfile
import threading
import time
import traceback
from collections import deque
from typing import Any, Dict, Optional

import maya.api.OpenMaya as om
import maya.api.OpenMayaUI as omui
import maya.cmds as cmds
import maya.utils as mutils

_TAG = "[MCP_Socket_Maya]"
VERSION = "1.0.0"

_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 7777
_PORT_OFFSETS = 10          # busy port → try 7778, 7779, ... (second Maya)
_ACCEPT_TIMEOUT = 1.0
_RECV_CHUNK = 8192
_CLIENT_IDLE_TIMEOUT = 60.0
_HANDLER_WARN_SECONDS = 30.0

_server = None              # MCPSocketServer
_thread: Optional[threading.Thread] = None

# ── console ring (per-execution stdout/stderr) ────────────────────────────

_LOG_LOCK = threading.Lock()
_LOG_RING: deque = deque(maxlen=500)   # entries: {"ts", "stream", "text"}


def _log_append(stream: str, text: str) -> None:
    if not text:
        return
    with _LOG_LOCK:
        _LOG_RING.append({"ts": time.strftime("%H:%M:%S"),
                          "stream": stream, "text": text[:4000]})


# ── helpers ───────────────────────────────────────────────────────────────

def _jsonify(value: Any) -> Any:
    """Best-effort JSON-safe conversion of an execution result."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return repr(value)


def _node_visible(path: str) -> Optional[bool]:
    try:
        return bool(cmds.getAttr(path + ".visibility"))
    except Exception:  # noqa: BLE001 — intermediate/shape quirks: report None
        return None


def _is_shape(path: str) -> bool:
    try:
        return (cmds.ls(path, long=True, shapes=True) or [None])[0] == path
    except Exception:  # noqa: BLE001
        return False


# ── handlers (all run on Maya's main thread) ──────────────────────────────

def _h_ping(params: dict) -> dict:
    server = _server
    return {
        "app": "maya",
        "bridge_version": VERSION,
        "maya_version": cmds.about(version=True),
        "cutIdentifier": cmds.about(cutIdentifier=True),
        "pid": os.getpid(),
        "port": server.port if server else None,
        "scene": cmds.file(query=True, sceneName=True) or "",
        "scene_modified": bool(cmds.file(query=True, modified=True)),
        "linear_unit": cmds.currentUnit(query=True, fullName=True, linear=True),
        "up_axis": cmds.upAxis(query=True, axis=True),
        "frames": [cmds.playbackOptions(query=True, minTime=True),
                   cmds.playbackOptions(query=True, maxTime=True)],
        "transforms": len(cmds.ls(transforms=True) or []),
        "meshes": len(cmds.ls(type="mesh") or []),
        "eval_idle": True,
    }


def _h_get_scene_info(params: dict) -> dict:
    tops = [t for t in (cmds.ls(assemblies=True, long=True) or [])]
    counts = {}
    for tname in ("mesh", "nurbsCurve", "nurbsSurface", "camera", "light",
                  "joint", "transform"):
        counts[tname] = len(cmds.ls(type=tname) or [])
    return {
        "scene": cmds.file(query=True, sceneName=True) or "(untitled)",
        "modified": bool(cmds.file(query=True, modified=True)),
        "linear_unit": cmds.currentUnit(query=True, fullName=True, linear=True),
        "angular_unit": cmds.currentUnit(query=True, angle=True),
        "up_axis": cmds.upAxis(query=True, axis=True),
        "frame_range": [cmds.playbackOptions(query=True, minTime=True),
                        cmds.playbackOptions(query=True, maxTime=True)],
        "current_frame": cmds.currentTime(query=True),
        "counts": counts,
        "top_objects": tops[:40],
        "top_count": len(tops),
    }


def _h_get_hierarchy(params: dict) -> dict:
    include_shapes = bool(params.get("include_shapes", False))
    max_nodes = int(params.get("max_nodes", 800))
    dag = cmds.ls(long=True, dag=True) or []
    nodes, truncated = [], False
    for path in dag:
        leaf = path.split("|")[-1]
        if not include_shapes and _is_shape(path):
            continue
        if len(nodes) >= max_nodes:
            truncated = True
            break
        try:
            ntype = cmds.nodeType(path)
        except Exception:  # noqa: BLE001 — node died mid-query
            continue
        nodes.append({
            "path": path,
            "name": leaf.split(".")[0],
            "type": ntype,
            "depth": path.count("|") - 1,
            "visible": _node_visible(path),
        })
    return {"total_dag": len(dag), "returned": len(nodes),
            "truncated": truncated, "nodes": nodes}


def _h_get_screenshot(params: dict) -> dict:
    """Physical capture of the screen/window — the CopyFromScreen class.

    grabWindow(0) grabs the QScreen's own buffer (physical pixels), unlike
    QWidget.grab() which re-renders offscreen in logical pixels and hides
    DPI artifacts. Mode "window" crops the Maya window out of that grab.
    """
    from PySide6 import QtGui, QtWidgets
    try:
        import shiboken6
    except ImportError:  # Maya 2022-2024
        from PySide2 import QtGui, QtWidgets  # type: ignore
        import shiboken2 as shiboken6  # type: ignore

    mode = params.get("mode", "window")
    if mode not in ("window", "screen"):
        raise ValueError("mode must be 'window' or 'screen'")
    filepath = params.get("filepath") or os.path.join(
        tempfile.gettempdir(), "mcp_socket_maya",
        "shot_%d.png" % int(time.time() * 1000))
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    ptr = __import__("maya.OpenMayaUI", fromlist=["MQtUtil"]).MQtUtil.mainWindow()
    if not ptr:
        raise RuntimeError("no Maya main window")
    main = shiboken6.wrapInstance(int(ptr), QtWidgets.QMainWindow)
    if params.get("focus"):
        # Raise Maya before grabbing — otherwise the window rect shows
        # whatever is actually on top (another DCC, the desktop).
        main.raise_()
        main.activateWindow()
        time.sleep(0.4)
    screen = main.screen() or QtWidgets.QApplication.primaryScreen()
    full = screen.grabWindow(0)          # physical pixels of that screen
    dpr = full.devicePixelRatio() or 1.0

    if mode == "window":
        geo = main.frameGeometry()
        origin = screen.geometry().topLeft()
        x = int((geo.x() - origin.x()) * dpr)
        y = int((geo.y() - origin.y()) * dpr)
        w = int(geo.width() * dpr)
        h = int(geo.height() * dpr)
        x, y = max(0, x), max(0, y)
        w = min(w, full.width() - x)
        h = min(h, full.height() - y)
        pix = full.copy(x, y, w, h)
    else:
        pix = full

    max_size = params.get("max_size")
    if max_size and max(pix.width(), pix.height()) > int(max_size):
        pix = pix.scaledToHeight(
            int(pix.height() * int(max_size) / max(pix.width(), pix.height())),
            QtGui.Qt.SmoothTransformation)

    ok = pix.save(filepath, "PNG")
    if not ok:
        raise RuntimeError("failed to save %s" % filepath)
    return {"filepath": filepath, "mode": mode,
            "width": pix.width(), "height": pix.height(), "dpr": dpr}


def _h_get_console_log(params: dict) -> dict:
    last_n = int(params.get("last_n", 50))
    text_filter = str(params.get("filter", "") or "")
    stream = str(params.get("stream", "") or "")
    with _LOG_LOCK:
        entries = list(_LOG_RING)
    if stream:
        entries = [e for e in entries if e["stream"] == stream]
    if text_filter:
        entries = [e for e in entries if text_filter.lower() in e["text"].lower()]
    return {"total": len(entries), "entries": entries[-last_n:]}


def _h_clear_console_log(params: dict) -> str:
    with _LOG_LOCK:
        _LOG_RING.clear()
    return "console log cleared"


def _h_execute_maya_code(params: dict) -> dict:
    """eval→exec with cmds/om/mel/omui preinjected; print is the primary
    channel, an optional ``result`` variable is returned JSON-safely."""
    code = params.get("code")
    if not code:
        raise ValueError("code is required")
    undo_chunk = params.get("undo_chunk", True)

    out_buf, err_buf = io.StringIO(), io.StringIO()
    namespace = {
        "__builtins__": __builtins__,
        "cmds": cmds,
        "om": om,
        "omui": omui,
        "mel": __import__("maya.mel", fromlist=["eval"]).eval,
        "mutils": mutils,
    }
    chunk_open = False
    ret = None

    # eval-then-exec mirrors the old bridge; exec fills ``result`` if the
    # snippet sets it.
    def _run_full():
        nonlocal ret
        try:
            ret = eval(compile(code, "<mcp_socket>", "eval"), namespace)
            return
        except SyntaxError:
            pass
        exec(compile(code, "<mcp_socket>", "exec"), namespace)
        ret = namespace.get("result")

    if undo_chunk:
        try:
            cmds.undoInfo(openChunk=True)
            chunk_open = True
        except Exception:  # noqa: BLE001 — undo disabled: run without chunk
            pass
    try:
        import contextlib
        import sys as _sys
        with contextlib.redirect_stdout(out_buf), \
             contextlib.redirect_stderr(err_buf):
            _run_full()
        error_text = ""
    except Exception:  # noqa: BLE001 — traceback is the result
        error_text = traceback.format_exc()
        err_buf.write(error_text)
    finally:
        if chunk_open:
            try:
                cmds.undoInfo(closeChunk=True)
            except Exception:  # noqa: BLE001
                pass

    _log_append("stdout", out_buf.getvalue())
    _log_append("stderr", err_buf.getvalue())

    return {
        "returned": _jsonify(ret),
        "stdout": out_buf.getvalue(),
        "stderr": err_buf.getvalue(),
        "error": bool(error_text),
    }


COMMANDS = {
    "ping": _h_ping,
    "get_scene_info": _h_get_scene_info,
    "get_hierarchy": _h_get_hierarchy,
    "get_screenshot": _h_get_screenshot,
    "get_console_log": _h_get_console_log,
    "clear_console_log": _h_clear_console_log,
    "execute_maya_code": _h_execute_maya_code,
}


# ── TCP server ────────────────────────────────────────────────────────────

def _try_parse(buffer: bytes):
    """(command, consumed) or (None, 0) — longest JSON prefix wins."""
    try:
        return json.loads(buffer.decode("utf-8")), len(buffer)
    except (json.JSONDecodeError, UnicodeDecodeError):
        pass
    for i in range(1, len(buffer)):
        try:
            return json.loads(buffer[:i].decode("utf-8")), i
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
    return None, 0


def _execute_command(command: dict) -> dict:
    cmd_type = command.get("type")
    params = command.get("params") or {}
    if not isinstance(cmd_type, str):
        return {"status": "error", "message": "Missing 'type' in command"}
    handler = COMMANDS.get(cmd_type)
    if handler is None:
        return {"status": "error",
                "message": "Unknown command type: %r. Known: %s"
                           % (cmd_type, sorted(COMMANDS))}
    started = time.monotonic()
    try:
        result = handler(params) if isinstance(params, dict) else handler()
    except TypeError as exc:
        return {"status": "error",
                "message": "Bad params for %r: %s" % (cmd_type, exc)}
    except Exception as exc:  # noqa: BLE001 — surface as protocol error
        return {"status": "error", "message": "%s: %s" % (type(exc).__name__, exc)}
    elapsed = time.monotonic() - started
    if elapsed > _HANDLER_WARN_SECONDS:
        print("%s slow command %r: %.1fs" % (_TAG, cmd_type, elapsed))
    return {"status": "success", "result": result}


class MCPSocketServer:
    """Single-port TCP server marshalling commands to Maya's main thread."""

    def __init__(self, host: str = _DEFAULT_HOST, port: int = _DEFAULT_PORT):
        self.host = host
        self.port = port
        self.preferred_port = port
        self.running = False
        self.socket: Optional[socket.socket] = None
        self._active_clients = 0
        self._total_commands = 0
        self._lock = threading.Lock()

    def start(self) -> bool:
        last_error: Optional[OSError] = None
        for offset in range(_PORT_OFFSETS + 1):
            candidate = self.preferred_port + offset
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((self.host, candidate))
            except OSError as exc:
                sock.close()
                last_error = exc
                continue
            self.port = candidate
            break
        else:
            print("%s failed to bind %s:%d-%d: %s"
                  % (_TAG, self.host, self.preferred_port,
                     self.preferred_port + _PORT_OFFSETS, last_error))
            return False
        sock.listen(1)
        sock.settimeout(_ACCEPT_TIMEOUT)
        self.socket = sock
        self.running = True
        print("%s server started on %s:%d (v%s)"
              % (_TAG, self.host, self.port, VERSION))
        return True

    def stop(self) -> None:
        self.running = False
        if self.socket is not None:
            try:
                self.socket.close()
            except OSError:
                pass
            self.socket = None
        print("%s server stopped" % _TAG)

    def accept_loop(self) -> None:
        while self.running:
            try:
                client, address = self.socket.accept()
            except socket.timeout:
                continue
            except OSError:
                if not self.running:
                    break
                time.sleep(0.25)
                continue
            with self._lock:
                self._active_clients += 1
            threading.Thread(target=self._client_loop, args=(client,),
                             daemon=True).start()

    def _client_loop(self, client: socket.socket) -> None:
        client.settimeout(_CLIENT_IDLE_TIMEOUT)
        buffer = b""
        try:
            while self.running:
                try:
                    data = client.recv(_RECV_CHUNK)
                except socket.timeout:
                    print("%s client idle, closing" % _TAG)
                    break
                if not data:
                    break
                buffer += data
                command, consumed = _try_parse(buffer)
                if command is None:
                    continue
                buffer = buffer[consumed:]
                with self._lock:
                    self._total_commands += 1
                self._dispatch(client, command)
        except (ConnectionError, OSError) as exc:
            print("%s connection error: %s" % (_TAG, exc))
        finally:
            with self._lock:
                self._active_clients = max(0, self._active_clients - 1)
            try:
                client.close()
            except OSError:
                pass

    def _dispatch(self, client: socket.socket, command: dict) -> None:
        """Run the command on Maya's main thread, reply on the client socket."""
        def runner():
            try:
                response = _execute_command(command)
            except Exception as exc:  # noqa: BLE001 — main-thread marshalling
                response = {"status": "error",
                            "message": "main thread: %s" % exc}
            try:
                client.sendall(json.dumps(response).encode("utf-8"))
            except OSError:
                print("%s failed to send reply — client gone" % _TAG)

        try:
            mutils.executeInMainThreadWithResult(runner)
        except Exception as exc:  # noqa: BLE001 — Maya busy/shutting down
            try:
                client.sendall(json.dumps(
                    {"status": "error",
                     "message": "could not reach main thread: %s" % exc}
                ).encode("utf-8"))
            except OSError:
                pass


# ── start/stop (called from mcp_startup.py — signature preserved) ─────────

def start(port: int = _DEFAULT_PORT) -> bool:
    global _server, _thread
    if _server is not None and _server.running:
        print("%s already running on %s:%d" % (_TAG, _server.host, _server.port))
        return True
    _server = MCPSocketServer(port=port)
    if not _server.start():
        _server = None
        return False
    _thread = threading.Thread(target=_server.accept_loop, daemon=True)
    _thread.start()
    return True


def stop() -> None:
    global _server, _thread
    if _server:
        _server.stop()
        _server = None
    _thread = None
    print("%s stopped" % _TAG)
