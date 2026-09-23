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
    ping                  versions, pid, port, scene, counts, actual port
    get_scene_info        name, units, up-axis, frame range, counts, top nodes
    get_hierarchy         DAG tree (paths, types, visibility; capped)
    get_screenshot        PHYSICAL screen capture (QScreen.grabWindow) —
                          mode "window" crops the Maya window, "screen" keeps
                          the full screen. This is the CopyFromScreen class of
                          capture; QWidget.grab() would lie on scaled monitors.
    get_console_log       ring buffer of stdout/stderr (global tee + per-exec)
    clear_console_log
    execute_maya_code     eval→exec in a fresh namespace with cmds/om/mel/omui
                          preinjected; captures stdout+stderr, returns the
                          optional ``result`` variable; joins the agent-session
                          undo chunk (gap-gated, see below)
    undo_agent_session    one Maya undo step rolls back the whole last agent
                          session — only when the session chunk is still the
                          top of the undo queue (honest refusal otherwise)
    list_instances        live Maya instances from the %TEMP% registry
    export_fbx            PROKLADKA neutral export (meters, Y-up, binary) with
                          a per-receiver note; scope selected|scene
    import_fbx            import under a receiver container (t=0 r=0 s=1),
                          report bbox in meters, flag oversize roots (no
                          magic multipliers — ever)
    replay_last_session   re-run the modifying commands of the last recorded
                          session; read-only steps are skipped, a failed step
                          does not stop the rest
    get_session_log_path  path of the newest JSONL session log

Agent-session undo: commands arriving after a >10 s gap open a named undo
chunk ("MCP Socket: agent session"); subsequent commands within the gap join
it. A QTimer watchdog closes the chunk once the agent has been quiet for the
gap, so the user's own manual edits never land inside it. One
``undo_agent_session`` (or one Ctrl+Z while the chunk is still on top) undoes
the whole session.

Session log: every recorded command appends a JSON line to
``%TEMP%\\mcp_socket_maya\\sessions\\session_<stamp>.jsonl``; a >10 s gap
starts a new file, the 30 newest files are kept.

Threading: the socket lives on a daemon thread, but cmds and Qt are
main-thread-only — every command is marshalled through
``maya.utils.executeInMainThreadWithResult``.

Autostart chain is unchanged: userSetup.py → mcp_startup.py → start().
"""

from __future__ import annotations

import base64
import glob
import io
import json
import os
import re
import socket
import sys
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
from maya.mel import eval as mel_eval   # module-level; snippets get ``mel``

_TAG = "[MCP_Socket_Maya]"
VERSION = "0.2.0"

_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 7777
_PORT_OFFSETS = 10          # busy port → try 7778, 7779, ... (second Maya)
_ACCEPT_TIMEOUT = 1.0
_RECV_CHUNK = 8192
_CLIENT_IDLE_TIMEOUT = 60.0
_HANDLER_WARN_SECONDS = 30.0

_SESSION_GAP = 10.0         # agent-session border, seconds (same as Blender)
_CHUNK_NAME = "MCP Socket: agent session"
_SESSIONS_KEEP = 30         # newest JSONL files kept
_HEARTBEAT_MS = 10000       # instance-registry heartbeat
_STALE_SECONDS = 25.0       # registry entry older than this = dead instance

_server = None              # MCPSocketServer
_thread: Optional[threading.Thread] = None
_window = None              # BridgeWindow singleton
_heartbeat_timer = None
_watchdog_timer = None
_tees: Dict[str, Any] = {}

# ── console ring (global tee + per-execution captures) ───────────────────

_LOG_LOCK = threading.Lock()
_LOG_RING: deque = deque(maxlen=500)   # entries: {"ts", "stream", "text"}


def _log_append(stream: str, text: str) -> None:
    if not text:
        return
    with _LOG_LOCK:
        _LOG_RING.append({"ts": time.strftime("%H:%M:%S"),
                          "stream": stream, "text": text[:4000]})


class _Tee:
    """Write-through stream wrapper feeding the console ring.

    Snippet executions bypass it (redirect_stdout inside the handler), so a
    snippet's prints land in the ring exactly once; system messages and
    prints from other tools flow through the tee.
    """

    def __init__(self, original, stream: str):
        self._original = original
        self._stream = stream
        self._partial = ""

    def write(self, text):
        try:
            self._original.write(text)
        except Exception:  # noqa: BLE001 — never break the real stream
            pass
        self._partial += text
        while "\n" in self._partial:
            line, self._partial = self._partial.split("\n", 1)
            _log_append(self._stream, line)
        return len(text)

    def flush(self):
        try:
            self._original.flush()
        except Exception:  # noqa: BLE001
            pass

    def __getattr__(self, name):
        return getattr(self._original, name)


def _install_log_tee() -> None:
    """Idempotent; called on startup and re-checked by the heartbeat
    (self-heal — a reinstall may leave sys.stdout on a stale wrapper)."""
    for name in ("stdout", "stderr"):
        current = getattr(sys, name, None)
        if isinstance(current, _Tee) and current._stream == name:
            continue
        tee = _Tee(current, name)
        _tees[name] = tee
        setattr(sys, name, tee)


def _restore_streams() -> None:
    for name, tee in _tees.items():
        try:
            if getattr(sys, name, None) is tee:
                setattr(sys, name, tee._original)
        except Exception:  # noqa: BLE001
            pass
    _tees.clear()


# ── shelf icon (embedded base64, written next to this module) ────────────

_ICON_B64 = (
    "iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAYAAACqaXHeAAAEBklEQVR4nOWbS0vrQBTH/zm3YsEHSBEfxUep2AriA9FudO975TfxE3iX9wv4Kdz4QEUXCkVFXRR0oSKKuvKJKCqibS8TSG8yndSJJtfG+YHSnGYmOf8558wkOoDiaLIn9vf3Z+EjksmklG+arOP7+/vwE+3t7VJCaD/NcadCaHbO+91xkRAiEUgF5xnMJ1EdIxWcLyQCQXFIldG3iwKC4pBKoy+KAoLiEBSHoDgExSEoDkFxCIpDUByC4hAUh6A4BMUhKA5BcQJePW6Wl5fnji8uLjAwMCDVtq6uDhsbG9C0f+8vZ2ZmMDk56Y8ICIfDFucZDQ0N6OzslGo/OjpqcZ4Ri8XgFeR2h/F4XGgfGRmRaj82NpZna2lpAZE32Upud2g3WjICNDY2oqOjI88eDAbR1NQEX0dAOBxGd3e349E3aG1tha8jwMjvzwrgVR0gNzsLBAKIRqO23w8PD+cVOAPWzi56fBMB0WhUF6HQFNfT0+N49H0TATHBTWazWaliyAvAt4tEIigpKUFRCxDnQjidTmNpaenDNGhra8tLnYWFBUfpVZQRcHZ2pq/izNTU1KC3t9diGx8ftxw/Pz9jenr6v9SBgJcRcHBwgPX1dTw8PKCystIyG2xvb9umxerqqt729fUVpaWlOfufRAK/9/aE1+46Pf3eCCgrK0N9fb3Fdnh4iLe3N6ysrFjsQ0NDuZUdWyKzBZCZubk5PX2Oj48t9l+RiO31U5GI/vNtAsRisbzcZqNoOGSmuroafX19wuLHomVtbS0noKwABk5FILiEaA43HEgmk7i/v7d8xxxngvHhv7y8rEcNY+j62nqztbXQgkFXRSB4VABfXl5wfn6uf35/f9cdMzM4OKhHAVsbmJmdnc05keHzWtNAzc1S9yMrAsGjCDg6OkImk8kdz8/PW74PhUKYmpqy2O7u7rC5uZm7+bSgsMmkgRMRCC7BT1FG/huwlxzMwUKiLS4u6tFikLm6Qvbp6dMCyEBudMLm9qqqKouNL2CiRREPC39+1PgocCrAR1FAcAHRAoWPAFEamLm8vMTOzk6ena8DVIwREBfMACIBtra2cHNzI+yDLX3NNcMuAigUglZR8aX7tfQHDwRgTvL5zmAOsjwXYVR/nvTJSZ7NzTpAXkyBotE34BdFxlvjVCrlTwGISH9pWagAmtnd3dXzXbY2ZB8fkbm9tV7TRQECX+2AhXWhNzmi8xOJhKNrPExMQJm/DHV98qnus/0RFIdQhLgVBTL9EIqUr4og257YL7aTwtha8hNE+KidefcIochxKoLT8zXzQbH/13ihBxtZx/m9QwH4CLenyLwUKNZa4OXOMeJP+qki2G2b0+waKL1x8icI8eWts6psnobq/AUnoqkAtbuDMAAAAABJRU5ErkJggg=="
)


def _icon_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(here, "mcp_socket_maya_icon.png")
    try:
        if os.path.exists(path):
            with open(path, "rb") as fh:
                if base64.b64encode(fh.read()).decode("ascii") == _ICON_B64:
                    return path
        with open(path, "wb") as fh:
            fh.write(base64.b64decode(_ICON_B64))
    except Exception as exc:  # noqa: BLE001 — button still works, ugly icon
        print("%s icon write failed: %s" % (_TAG, exc))
    return path


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


def _qt():
    try:
        from PySide6 import QtCore, QtGui, QtWidgets
        import shiboken6
        return QtCore, QtGui, QtWidgets, shiboken6
    except ImportError:  # Maya 2022-2024
        from PySide2 import QtCore, QtGui, QtWidgets  # type: ignore
        import shiboken2 as shiboken6  # type: ignore
        return QtCore, QtGui, QtWidgets, shiboken6


# ── agent-session undo chunks ─────────────────────────────────────────────

_undo_session = {"last_exec": 0.0, "chunk_open": False}


def _close_session_chunk(reason: str) -> None:
    if not _undo_session["chunk_open"]:
        return
    try:
        cmds.undoInfo(closeChunk=True)
        _log_append("stderr",
                    "%s agent session chunk closed (%s)" % (_TAG, reason))
    except Exception:  # noqa: BLE001 — queue flushed (scene change etc.)
        pass
    _undo_session["chunk_open"] = False


def _open_session_chunk_if_gap(now: float) -> None:
    gap = now - _undo_session["last_exec"]
    if gap <= _SESSION_GAP and _undo_session["last_exec"] > 0:
        return                      # within the session: keep the chunk open
    _close_session_chunk("superseded")
    try:
        cmds.undoInfo(openChunk=True, chunkName=_CHUNK_NAME)
        _undo_session["chunk_open"] = True
    except Exception:  # noqa: BLE001 — undo disabled: run without chunk
        _undo_session["chunk_open"] = False


def _arm_watchdog() -> None:
    """Close the chunk once the agent has been quiet for the gap, so the
    user's manual edits afterwards never join it."""
    global _watchdog_timer
    if cmds.about(batch=True):
        return
    try:
        QTimer = _qt()[0].QTimer
        if _watchdog_timer is None:
            _watchdog_timer = QTimer()
            _watchdog_timer.setSingleShot(True)
            _watchdog_timer.timeout.connect(_on_watchdog)
        _watchdog_timer.start(int(_SESSION_GAP * 1000) + 1000)
    except Exception as exc:  # noqa: BLE001
        print("%s watchdog unavailable: %s" % (_TAG, exc))


def _on_watchdog() -> None:
    if _undo_session["chunk_open"] and \
            (time.monotonic() - _undo_session["last_exec"]) > _SESSION_GAP:
        _close_session_chunk("gap")


# ── session log (JSONL, Blender v2.6.0 architecture) ──────────────────────

_RECORD_SKIP = {"ping", "get_console_log", "clear_console_log",
                "list_instances", "get_session_log_path",
                "replay_last_session"}
_REPLAY_READONLY = {"ping", "get_scene_info", "get_hierarchy",
                    "get_screenshot", "get_console_log", "clear_console_log",
                    "list_instances", "get_session_log_path",
                    "replay_last_session", "undo_agent_session"}

_record_state = {"file": None, "last_ts": 0.0}


def _sessions_dir() -> str:
    return os.path.join(tempfile.gettempdir(), "mcp_socket_maya", "sessions")


def _prune_sessions(keep: int) -> None:
    files = glob.glob(os.path.join(_sessions_dir(), "session_*.jsonl"))
    if len(files) <= keep:
        return
    files.sort(key=os.path.getmtime)
    for path in files[:-keep]:
        try:
            os.remove(path)
        except OSError:
            pass


def _record(command: dict, response: dict, replay: bool = False) -> None:
    ctype = command.get("type")
    if not isinstance(ctype, str) or ctype in _RECORD_SKIP:
        return
    now = time.time()
    st = _record_state
    if st["file"] is None or (now - st["last_ts"]) > _SESSION_GAP:
        os.makedirs(_sessions_dir(), exist_ok=True)
        _prune_sessions(_SESSIONS_KEEP)
        base = os.path.join(_sessions_dir(),
                            "session_%s.jsonl" % time.strftime("%Y%m%d_%H%M%S"))
        path, n = base, 1
        while os.path.exists(path):     # same-second collision guard
            n += 1
            path = base.replace(".jsonl", "_%d.jsonl" % n)
        st["file"] = path
    st["last_ts"] = now
    entry = {"ts": time.strftime("%H:%M:%S"), "type": ctype,
             "params": command.get("params") or {},
             "status": response.get("status")}
    if response.get("status") != "success":
        entry["error"] = response.get("message", "")
    if replay:
        entry["replay"] = True
    try:
        with open(st["file"], "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except OSError as exc:
        print("%s session log write failed: %s" % (_TAG, exc))


def _last_session_file() -> Optional[str]:
    files = glob.glob(os.path.join(_sessions_dir(), "session_*.jsonl"))
    if not files:
        return None
    return max(files, key=os.path.getmtime)


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
        "agent_chunk_open": _undo_session["chunk_open"],
        "sessions_dir": _sessions_dir(),
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
    _QtCore, QtGui, QtWidgets, shiboken = _qt()

    mode = params.get("mode", "window")
    if mode not in ("window", "screen"):
        raise ValueError("mode must be 'window' or 'screen'")
    filepath = params.get("filepath") or os.path.join(
        tempfile.gettempdir(), "mcp_socket_maya",
        "shot_%d.png" % int(time.time() * 1000))
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # MQtUtil lives in the 1.0 API module (maya.OpenMayaUI), NOT maya.api
    mqt = __import__("maya.OpenMayaUI", fromlist=["MQtUtil"]).MQtUtil
    ptr = mqt.mainWindow()
    if not ptr:
        raise RuntimeError("no Maya main window")
    main = shiboken.wrapInstance(int(ptr), QtWidgets.QMainWindow)
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
    channel, an optional ``result`` variable is returned JSON-safely.

    Note: the preinjected ``mel`` IS maya.mel.eval — call ``mel("cmd;")``,
    not ``mel.eval(...)``.

    Undo: by default the call joins the agent-session chunk (opened when the
    call arrives after a >10 s gap, closed by the watchdog after the agent
    leaves). ``undo_chunk=False`` closes an open session first and runs
    unchunked."""
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
        _open_session_chunk_if_gap(time.monotonic())
    else:
        _close_session_chunk("undo_chunk=false request")
    try:
        import contextlib
        with contextlib.redirect_stdout(out_buf), \
             contextlib.redirect_stderr(err_buf):
            _run_full()
        error_text = ""
    except Exception:  # noqa: BLE001 — traceback is the result
        error_text = traceback.format_exc()
        err_buf.write(error_text)

    _undo_session["last_exec"] = time.monotonic()
    _arm_watchdog()

    _log_append("stdout", out_buf.getvalue())
    _log_append("stderr", err_buf.getvalue())

    return {
        "returned": _jsonify(ret),
        "stdout": out_buf.getvalue(),
        "stderr": err_buf.getvalue(),
        "error": bool(error_text),
    }


def _h_undo_agent_session(params: dict) -> dict:
    """One undo step rolls back the whole agent session — only when the
    session chunk is still the top of the undo queue; otherwise refuses
    honestly instead of eating the user's own work."""
    _close_session_chunk("before undo")
    top = ""
    try:
        top = cmds.undoInfo(query=True, undoName=True) or ""
    except Exception:  # noqa: BLE001
        pass
    if top != _CHUNK_NAME:
        return {"undone": False, "top_undo": top,
                "message": ("agent session is not the top undo step; "
                            "undo manually (Ctrl+Z) if needed")}
    cmds.undo()
    _undo_session["last_exec"] = 0.0
    return {"undone": True, "undid": _CHUNK_NAME}


# ── FBX pipeline (PROKLADKA: exporter neutral, receiver converts) ─────────

_FBX_PRESETS = {
    "neutral": ("meters, Y-up, binary; the receiver converts to its own "
                "conventions"),
    "maya": ("receiver Maya: units m (Maya shows cm after import), naming "
             "lowercase + _geo/_grp, UV map1"),
    "houdini": ("receiver Houdini: meters, node suffixes _geo/_vdb/_abc, "
                "attributes on primitives"),
    "ue": ("receiver Unreal: cm (x100 of meters), PascalCase with SM_/SK_/"
           "T_/MI_ prefixes, UV channels"),
}


def _load_fbx_plugin() -> None:
    try:
        if not cmds.pluginInfo("fbxmaya", query=True, loaded=True):
            cmds.loadPlugin("fbxmaya", quiet=True)
    except Exception:  # noqa: BLE001 — already loaded or autoloaded later
        pass


def _h_export_fbx(params: dict) -> dict:
    path = params.get("path")
    if not path:
        raise ValueError("path is required")
    path = os.path.abspath(path)
    if '"' in path:
        raise ValueError("path must not contain quotes")
    preset = params.get("preset", "neutral")
    if preset not in _FBX_PRESETS:
        raise ValueError("unknown preset %r; known: %s"
                         % (preset, sorted(_FBX_PRESETS)))
    scope = params.get("scope", "selected")
    if scope not in ("selected", "scene"):
        raise ValueError("scope must be 'selected' or 'scene'")

    _load_fbx_plugin()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fpath = path.replace("\\", "/")

    mel_eval("FBXResetExport;")
    mel_eval('FBXExportConvertUnitString -v "m";')   # neutral = meters
    mel_eval("FBXExportUpAxis y;")                   # neutral = Y-up (bare value!)
    mel_eval("FBXExportInAscii -v 0;")               # neutral = binary

    sel = cmds.ls(selection=True, long=True) or []
    if scope == "selected":
        if not sel:
            raise ValueError("scope='selected' but selection is empty")
        mel_eval('FBXExport -f "%s" -s;' % fpath)
    else:
        mel_eval('FBXExport -f "%s";' % fpath)

    if not os.path.exists(path):
        raise RuntimeError("FBX export produced no file (see Script Editor)")
    return {"file": path, "size_bytes": os.path.getsize(path),
            "preset": preset, "scope": scope,
            "selection_count": len(sel), "note": _FBX_PRESETS[preset]}


def _h_import_fbx(params: dict) -> dict:
    path = params.get("path")
    if not path or not os.path.exists(path):
        raise ValueError("path does not exist: %r" % path)
    with_container = bool(params.get("container", True))

    _load_fbx_plugin()
    fpath = path.replace("\\", "/")
    before = set(cmds.ls(long=True) or [])
    new = [n for n in (cmds.file(fpath, i=True, type="FBX", ignoreVersion=True,
                                 mergeNamespacesOnClash=False, options="mo=1",
                                 pr=True, returnNewNodes=True) or [])]
    new = [n for n in (cmds.ls(long=True) or []) if n not in before] or new
    if not new:
        raise RuntimeError(
            "import produced no nodes. The file may still be valid (a "
            "fresh Maya session imports it — verify via mayapy); this "
            "session's FBX importer silently returned nothing. A Maya "
            "restart usually clears it. Scene untouched.")
    roots = [n for n in new
             if n.startswith("|") and n.count("|") == 1
             and cmds.nodeType(n) == "transform"]

    bbox_m, oversize = None, []
    if roots:
        # bbox BEFORE grouping — parenting changes full child paths
        # (exactWorldBoundingBox then fails on stale paths)
        bb = cmds.exactWorldBoundingBox(roots)
        dims_m = [(bb[i + 3] - bb[i]) / 100.0 for i in range(3)]  # cm → m
        bbox_m = [round(d, 4) for d in dims_m]
        for root in roots:
            rb = cmds.exactWorldBoundingBox([root])
            if any((rb[i + 3] - rb[i]) / 100.0 > 50.0 for i in range(3)):
                oversize.append(root)

    container = None
    if with_container:
        base = os.path.splitext(os.path.basename(path))[0]
        safe = re.sub(r"[^A-Za-z0-9_]", "_", base) or "imported"
        if safe[0].isdigit():
            safe = "_" + safe
        container = cmds.group(empty=True, name=safe + "_grp")
        if roots:
            # identity parent: world transforms preserved; correct dims are
            # baked in the vertices, never compensated on the transform
            cmds.parent(roots, container)

    return {"file": path, "nodes": len(new), "roots": roots,
            "container": container, "bbox_m": bbox_m,
            "oversize_roots": oversize,
            "note": ("receiver rule: container t=0 r=0 s=1; reported in "
                     "meters; no auto-rescale (check source units if sizes "
                     "are off by x100)")}


# ── instance registry (multi-Maya, Blender v1.3.0 architecture) ───────────

def _registry_dir() -> str:
    return os.path.join(tempfile.gettempdir(), "mcp_socket_maya_instances")


def _write_registry() -> None:
    try:
        os.makedirs(_registry_dir(), exist_ok=True)
        data = {"pid": os.getpid(),
                "port": _server.port if _server else None,
                "version": VERSION,
                "scene": cmds.file(query=True, sceneName=True) or "",
                "ts": time.time()}
        path = os.path.join(_registry_dir(), "pid_%d.json" % os.getpid())
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh)
    except OSError as exc:
        print("%s registry write failed: %s" % (_TAG, exc))


def _remove_registry() -> None:
    try:
        os.remove(os.path.join(_registry_dir(), "pid_%d.json" % os.getpid()))
    except OSError:
        pass


def _h_list_instances(params: dict) -> dict:
    fresh, stale = [], 0
    for path in glob.glob(os.path.join(_registry_dir(), "pid_*.json")):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, ValueError):
            continue
        entry = {"pid": data.get("pid"), "port": data.get("port"),
                 "version": data.get("version"), "scene": data.get("scene"),
                 "age_seconds": round(time.time() - data.get("ts", 0), 1)}
        if entry["age_seconds"] <= _STALE_SECONDS:
            fresh.append(entry)
        else:
            stale += 1
    fresh.sort(key=lambda e: (e["port"] or 0))
    return {"instances": fresh, "stale_seen": stale}


# ── session replay ────────────────────────────────────────────────────────

def _h_replay_last_session(params: dict) -> dict:
    path = _last_session_file()
    if not path:
        return {"replayed": 0, "message": "no session log found"}
    steps = []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    steps.append(json.loads(line))
    except (OSError, ValueError) as exc:
        raise RuntimeError("cannot read session log: %s" % exc)

    summary = {"file": path, "total": len(steps), "replayed": 0,
               "skipped_readonly": 0, "skipped_replayed": 0, "failed": []}
    for entry in steps:
        ctype = entry.get("type")
        if ctype in _REPLAY_READONLY:
            summary["skipped_readonly"] += 1
            continue
        if entry.get("replay"):
            summary["skipped_replayed"] += 1
            continue
        command = {"type": ctype, "params": entry.get("params") or {}}
        response = _execute_command(command, replay=True)
        if response.get("status") == "success":
            summary["replayed"] += 1
        else:
            summary["failed"].append({"type": ctype,
                                      "error": response.get("message", "")})
    return summary


def _h_get_session_log_path(params: dict) -> dict:
    return {"dir": _sessions_dir(), "last_file": _last_session_file()}


COMMANDS = {
    "ping": _h_ping,
    "get_scene_info": _h_get_scene_info,
    "get_hierarchy": _h_get_hierarchy,
    "get_screenshot": _h_get_screenshot,
    "get_console_log": _h_get_console_log,
    "clear_console_log": _h_clear_console_log,
    "execute_maya_code": _h_execute_maya_code,
    "undo_agent_session": _h_undo_agent_session,
    "list_instances": _h_list_instances,
    "export_fbx": _h_export_fbx,
    "import_fbx": _h_import_fbx,
    "replay_last_session": _h_replay_last_session,
    "get_session_log_path": _h_get_session_log_path,
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


def _execute_command(command: dict, replay: bool = False) -> dict:
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
    _record(command, {"status": "success"}, replay)
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


# ── UI: window + shelf button (Blender-panel parity) ─────────────────────

def _show_window() -> None:
    global _window
    _QtCore, QtGui, QtWidgets, shiboken = _qt()
    if _window is not None:
        try:
            _window.close()
            _window.deleteLater()
        except RuntimeError:
            pass
        _window = None

    mqt = __import__("maya.OpenMayaUI", fromlist=["MQtUtil"]).MQtUtil
    ptr = mqt.mainWindow()
    parent = shiboken.wrapInstance(int(ptr), QtWidgets.QMainWindow) if ptr else None
    win = QtWidgets.QWidget(parent,
                            _QtCore.Qt.Window) if parent else QtWidgets.QWidget()
    win.setWindowTitle("MCP Socket Maya %s" % VERSION)
    win.resize(380, 560)

    lay = QtWidgets.QVBoxLayout(win)

    header = QtWidgets.QLabel("<b>MCP Socket Maya %s</b>" % VERSION)
    lay.addWidget(header)
    status = QtWidgets.QLabel()
    status.setWordWrap(True)
    lay.addWidget(status)

    def refresh():
        scene = cmds.file(query=True, sceneName=True) or "(untitled)"
        port = _server.port if _server and _server.running else None
        state = "running" if port else "STOPPED"
        status.setText("port %s | pid %d | %s<br>scene: %s"
                       % (port or "-", os.getpid(), state, os.path.basename(str(scene))))

    def run_handler(fn):
        """Run a bridge handler in-process and surface the result in the log."""
        try:
            response = _execute_command({"type": fn[0],
                                         "params": fn[1] or {}})
        except Exception as exc:  # noqa: BLE001
            response = {"status": "error", "message": repr(exc)}
        if response.get("status") == "success":
            res = response.get("result")
            text = res if isinstance(res, str) else json.dumps(
                res, ensure_ascii=False)
            _log_append("stdout", "%s %s" % (fn[0], text or "ok"))
        else:
            _log_append("stderr", "%s FAILED: %s"
                        % (fn[0], response.get("message", "")))
        refresh_console()
        refresh()

    undo_btn = QtWidgets.QPushButton("Undo Agent Work")
    undo_btn.clicked.connect(lambda: run_handler(("undo_agent_session", {})))
    lay.addWidget(undo_btn)

    box = QtWidgets.QGroupBox("Agent Sessions")
    v = QtWidgets.QVBoxLayout(box)
    row = QtWidgets.QHBoxLayout()
    replay_btn = QtWidgets.QPushButton("Replay Last Session")
    replay_btn.clicked.connect(lambda: run_handler(("replay_last_session", {})))
    logpath_btn = QtWidgets.QPushButton("Copy Log Path")
    logpath_btn.clicked.connect(lambda: _copy_session_path(QtGui, status))
    row.addWidget(replay_btn)
    row.addWidget(logpath_btn)
    v.addLayout(row)
    lay.addWidget(box)

    def refresh_console():
        with _LOG_LOCK:
            entries = list(_LOG_RING)[-12:]
        console.setPlainText("\n".join(
            "%s [%s] %s" % (e["ts"], e["stream"], e["text"].replace("\n", " | ")[:220])
            for e in entries))
        bar = console.verticalScrollBar()
        bar.setValue(bar.maximum())

    box = QtWidgets.QGroupBox("Console Log")
    v = QtWidgets.QVBoxLayout(box)
    console = QtWidgets.QPlainTextEdit()
    console.setReadOnly(True)
    console.setMaximumBlockCount(200)
    f = QtGui.QFont("Consolas")
    f.setStyleHint(QtGui.QFont.StyleHint.Monospace)
    console.setFont(f)
    v.addWidget(console)
    row = QtWidgets.QHBoxLayout()
    save_btn = QtWidgets.QPushButton("Save to File")
    save_btn.clicked.connect(lambda: _save_console(QtGui, status))
    clear_btn = QtWidgets.QPushButton("Clear")
    clear_btn.clicked.connect(lambda: run_handler(("clear_console_log", {})))
    refresh_btn = QtWidgets.QPushButton("Refresh")
    refresh_btn.clicked.connect(refresh_console)
    for b in (save_btn, clear_btn, refresh_btn):
        row.addWidget(b)
    v.addLayout(row)
    lay.addWidget(box)

    box = QtWidgets.QGroupBox("Pipeline FBX (PROKLADKA)")
    v = QtWidgets.QVBoxLayout(box)
    row = QtWidgets.QHBoxLayout()
    preset_cb = QtWidgets.QComboBox()
    preset_cb.addItems(sorted(_FBX_PRESETS))
    scope_cb = QtWidgets.QComboBox()
    scope_cb.addItems(["selected", "scene"])
    row.addWidget(QtWidgets.QLabel("preset"))
    row.addWidget(preset_cb)
    row.addWidget(QtWidgets.QLabel("scope"))
    row.addWidget(scope_cb)
    v.addLayout(row)
    export_row = QtWidgets.QHBoxLayout()
    export_path = QtWidgets.QLineEdit()
    export_btn = QtWidgets.QPushButton("Export...")
    export_row.addWidget(export_path)
    export_row.addWidget(export_btn)
    v.addLayout(export_row)
    import_row = QtWidgets.QHBoxLayout()
    import_path = QtWidgets.QLineEdit()
    import_btn = QtWidgets.QPushButton("Import...")
    import_row.addWidget(import_path)
    import_row.addWidget(import_btn)
    v.addLayout(import_row)

    def do_export():
        path = export_path.text().strip()
        if not path:
            start_dir = os.path.dirname(cmds.file(query=True, sceneName=True) or "") \
                or os.path.expanduser("~")
            path, _ = QtWidgets.QFileDialog.getSaveFileName(
                win, "Export FBX", start_dir + "/untitled.fbx", "FBX (*.fbx)")
            if not path:
                return
            export_path.setText(path)
        run_handler(("export_fbx", {"path": path,
                                    "preset": preset_cb.currentText(),
                                    "scope": scope_cb.currentText()}))

    def do_import():
        path = import_path.text().strip()
        if not path:
            start_dir = os.path.dirname(cmds.file(query=True, sceneName=True) or "") \
                or os.path.expanduser("~")
            path, _ = QtWidgets.QFileDialog.getOpenFileName(
                win, "Import FBX", start_dir, "FBX (*.fbx)")
            if not path:
                return
            import_path.setText(path)
        run_handler(("import_fbx", {"path": path}))

    export_btn.clicked.connect(do_export)
    import_btn.clicked.connect(do_import)
    lay.addWidget(box)

    lay.addStretch(1)
    refresh()
    refresh_console()
    win.show()
    _window = win


def _copy_session_path(QtGui, status) -> None:
    path = _last_session_file()
    if path:
        QtGui.QGuiApplication.clipboard().setText(path)
        _log_append("stdout", "session log path copied: %s" % path)
    else:
        _log_append("stderr", "no session log file yet")
    if _window:
        _window.update()


def _save_console(QtGui, status) -> None:
    with _LOG_LOCK:
        entries = list(_LOG_RING)
    os.makedirs(os.path.join(tempfile.gettempdir(), "mcp_socket_maya"),
                exist_ok=True)
    path = os.path.join(tempfile.gettempdir(), "mcp_socket_maya",
                        "console_%s.log" % time.strftime("%Y%m%d_%H%M%S"))
    with open(path, "w", encoding="utf-8") as fh:
        for e in entries:
            fh.write("%s [%s] %s\n" % (e["ts"], e["stream"], e["text"]))
    QtGui.QGuiApplication.clipboard().setText(path)
    _log_append("stdout", "console saved: %s (path in clipboard)" % path)


_SHELF_CMD = ("import maya_mcp_server as _mcp_socket_maya\n"
              "_mcp_socket_maya._show_window()")


def _install_shelf_button() -> None:
    if cmds.about(batch=True):
        return
    try:
        icon = _icon_path()
        top = mel_eval("$tmp=$gShelfTopLevel")
        tabs = cmds.tabLayout(top, query=True, childArray=True) or []
        custom = next((t for t in tabs if t.split("|")[-1] == "Custom"), None)
        if custom is None:
            custom = cmds.shelfLayout("MCP_Socket_Shelf", parent=top)
        mine = []
        for child in cmds.layout(custom, query=True, childArray=True) or []:
            try:
                ann = cmds.shelfButton(child, query=True, annotation=True) or ""
            except Exception:  # noqa: BLE001 — not a shelf button
                continue
            if ann.startswith("MCP Socket for Maya"):
                mine.append(child)
        ann = ("MCP Socket for Maya %s — open bridge window" % VERSION)
        if mine:
            btn = mine[0]
            for extra in mine[1:]:      # duplicates die (label-anchored, not name)
                cmds.deleteUI(extra)
            cmds.shelfButton(btn, edit=True, image1=icon, label="MCP Socket",
                             annotation=ann, sourceType="python",
                             command=_SHELF_CMD)
        else:
            cmds.shelfButton("mcpSocketShelfBtn", parent=custom,
                             image1=icon, label="MCP Socket", annotation=ann,
                             sourceType="python", command=_SHELF_CMD,
                             width=34, height=34, style="iconOnly")
        print("%s shelf button installed" % _TAG)
    except Exception as exc:  # noqa: BLE001 — never break startup over UI
        print("%s shelf button install failed: %s" % (_TAG, exc))


# ── heartbeat ─────────────────────────────────────────────────────────────

def _heartbeat() -> None:
    _install_log_tee()      # self-heal: reinstall may leave stale wrappers
    _write_registry()


def _start_heartbeat() -> None:
    global _heartbeat_timer
    if _heartbeat_timer is not None:
        return
    _heartbeat()
    try:
        QTimer = _qt()[0].QTimer
        _heartbeat_timer = QTimer()
        _heartbeat_timer.timeout.connect(_heartbeat)
        _heartbeat_timer.start(_HEARTBEAT_MS)
    except Exception as exc:  # noqa: BLE001 — batch mode etc.
        print("%s heartbeat unavailable: %s" % (_TAG, exc))


def _stop_heartbeat() -> None:
    global _heartbeat_timer
    if _heartbeat_timer is not None:
        try:
            _heartbeat_timer.stop()
        except Exception:  # noqa: BLE001
            pass
        _heartbeat_timer = None
    global _watchdog_timer
    if _watchdog_timer is not None:
        try:
            _watchdog_timer.stop()
        except Exception:  # noqa: BLE001
            pass
        _watchdog_timer = None


# ── start/stop (called from mcp_startup.py — signature preserved) ─────────

def _deferred_startup() -> None:
    """Runs when Maya is idle: UI and timers are safe here."""
    _install_log_tee()
    _start_heartbeat()
    _install_shelf_button()


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
    try:
        mutils.executeDeferred(_deferred_startup)
    except Exception as exc:  # noqa: BLE001 — no event loop (rare)
        print("%s deferred startup skipped: %s" % (_TAG, exc))
    return True


def stop() -> None:
    global _server, _thread
    if _server:
        _server.stop()
        _server = None
    _thread = None
    _stop_heartbeat()
    _close_session_chunk("bridge stopped")
    _remove_registry()
    _restore_streams()
    print("%s stopped" % _TAG)
