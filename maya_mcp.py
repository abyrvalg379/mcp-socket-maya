#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MCP Socket for Maya — stdio MCP server (mcp-socket family).

Chain: MCP client → this process → TCP 127.0.0.1:7777 → the TCP listener
inside Maya (maya_mcp_server.py, autostarted via userSetup.py → mcp_startup.py).
Wire protocol is the shared mcp-socket / blender-mcp 1.6.x one.

Replaces the previous HTTP-proxy version of this file under the SAME path,
so the ZCode config entry (mcp.servers.maya) needs no changes.

Run (registered in ZCode config → mcp.servers.maya):
    python.exe maya_mcp.py [--port 7777]

Hand-rolled MCP JSON-RPC (newline-delimited over stdio), stdlib only —
same proven pattern as the Blender mcp_server/server.py.
"""

from __future__ import annotations

import json
import os
import socket
import sys

_DEFAULT_PORT = 7777
_CALL_TIMEOUT = 180.0  # seconds; generous for heavy scene queries


def _bridge_port() -> int:
    for i, part in enumerate(sys.argv):
        if part == "--port" and i + 1 < len(sys.argv):
            return int(sys.argv[i + 1])
    env = os.environ.get("MAYA_MCP_SOCKET_PORT")
    if env:
        return int(env)
    return _DEFAULT_PORT


def _bridge_call(cmd_type: str, params: dict, port: int) -> dict:
    """One bridge command: fresh socket, JSON in, JSON out (no framing —
    accumulate bytes until they parse whole, matching the mcp-socket wire)."""
    with socket.create_connection(("127.0.0.1", port), timeout=_CALL_TIMEOUT) as sock:
        sock.settimeout(_CALL_TIMEOUT)
        sock.sendall(json.dumps({"type": cmd_type, "params": params}).encode("utf-8"))
        buf = b""
        while True:
            chunk = sock.recv(8192)
            if not chunk:
                break
            buf += chunk
            try:
                return json.loads(buf.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
    raise RuntimeError("Bridge closed the connection before replying")


TOOLS = [
    {"name": "ping_maya", "bridge": "ping", "params": [],
     "description": ("Check whether Maya MCP bridge is running and reachable. "
                     "Returns Maya version, pid, port, scene, units, counts.")},
    {"name": "execute_maya_code", "bridge": "execute_maya_code",
     "params": ["code", "undo_chunk"],
     "description": ("Execute a Python code inside the running Maya instance. "
                     "Returns stdout output and/or the expression value. "
                     "Full maya.cmds, OpenMaya API, and all Maya modules are available.")},
    {"name": "get_scene_info", "bridge": "get_scene_info", "params": [],
     "description": ("Scene overview: filepath, units, up-axis, frame range, "
                     "object counts by type, top-level objects.")},
    {"name": "get_hierarchy", "bridge": "get_hierarchy",
     "params": ["include_shapes", "max_nodes"],
     "description": ("DAG tree of the scene: full paths, node types, depth, "
                     "visibility. Capped at max_nodes (default 800) — "
                     "truncated flag tells when it fired.")},
    {"name": "get_screenshot", "bridge": "get_screenshot",
     "params": ["filepath", "mode", "max_size", "focus"],
     "description": ("PHYSICAL screen capture (QScreen.grabWindow — the "
                     "CopyFromScreen class, correct on scaled monitors; do not "
                     "confuse with QWidget.grab which lies about DPI). "
                     "mode 'window' = cropped Maya window, 'screen' = full "
                     "screen. focus=true raises Maya first (steals focus — "
                     "use only when acceptable). Returns the filepath.")},
    {"name": "get_console_log", "bridge": "get_console_log",
     "params": ["last_n", "filter", "stream"],
     "description": ("Ring buffer of stdout/stderr captured per executed code "
                     "snippet (last_n default 50; filter substring; stream "
                     "'stdout'/'stderr'/both).")},
    {"name": "clear_console_log", "bridge": "clear_console_log", "params": [],
     "description": "Clear the console ring buffer."},
]


def _tool_schema(tool: dict) -> dict:
    props: dict = {}
    required = []
    for param in tool["params"]:
        if param == "code":
            props[param] = {"type": "string",
                            "description": "Python code to execute in Maya"}
            required.append(param)
        elif param == "undo_chunk":
            props[param] = {"type": "boolean",
                            "description": "wrap the snippet in one undo chunk "
                                           "(default true)"}
        elif param == "mode":
            props[param] = {"type": "string", "enum": ["window", "screen"],
                            "description": "capture target (default window)"}
        elif param == "stream":
            props[param] = {"type": "string",
                            "description": '"stdout" | "stderr" | "" for both'}
        elif param in ("last_n", "max_nodes", "max_size"):
            props[param] = {"type": "integer"}
        elif param == "include_shapes":
            props[param] = {"type": "boolean"}
        elif param == "focus":
            props[param] = {"type": "boolean",
                            "description": "raise Maya before the grab"}
        else:  # filepath
            props[param] = {"type": "string"}
    return {"type": "object", "properties": props, "required": required}


def _tool_definitions() -> list:
    return [{"name": t["name"], "description": t["description"],
             "inputSchema": _tool_schema(t)} for t in TOOLS]


def _call_tool(name: str, arguments: dict, port: int) -> tuple:
    tool = next((t for t in TOOLS if t["name"] == name), None)
    if tool is None:
        raise ValueError(f"Unknown tool: {name}")
    params = {k: v for k, v in (arguments or {}).items() if v is not None}
    reply = _bridge_call(tool["bridge"], params, port)
    if reply.get("status") == "success":
        result = reply.get("result", "")
        text = result if isinstance(result, str) else json.dumps(
            result, indent=2, ensure_ascii=False)
        return {"content": [{"type": "text", "text": text or "(no output)"}]}, False
    return {"content": [{"type": "text",
                         "text": f"Bridge error: {reply.get('message', 'unknown')}"}],
            "isError": True}, True


# ── MCP JSON-RPC loop (newline-delimited over stdio) ───────────────────────

def _send(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def _result(req_id, data: dict) -> None:
    _send({"jsonrpc": "2.0", "id": req_id, "result": data})


def _error(req_id, code: int, message: str) -> None:
    _send({"jsonrpc": "2.0", "id": req_id,
           "error": {"code": code, "message": message}})


def _handle(msg: dict, port: int) -> None:
    method = msg.get("method", "")
    req_id = msg.get("id")

    if method == "initialize":
        _result(req_id, {
            "protocolVersion": msg.get("params", {}).get("protocolVersion",
                                                         "2025-11-25"),
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "mcp-socket-maya", "version": "1.0.0"},
        })
    elif method == "notifications/initialized":
        pass
    elif method == "tools/list":
        _result(req_id, {"tools": _tool_definitions()})
    elif method == "tools/call":
        try:
            name = msg["params"].get("name", "")
            content, is_error = _call_tool(name,
                                           msg["params"].get("arguments", {}),
                                           port)
            result = dict(content)
            if is_error:
                result["isError"] = True
            _result(req_id, result)
        except Exception as exc:  # noqa: BLE001 — report as tool error
            _result(req_id, {"content": [{"type": "text",
                                          "text": f"MCP Socket Maya error: {exc}"}],
                             "isError": True})
    elif req_id is not None:
        _error(req_id, -32601, f"Method not found: {method}")


def main() -> None:
    port = _bridge_port()
    print(f"[MCP_Socket_Maya_server] started, bridge port {port}",
          file=sys.stderr, flush=True)
    for raw_line in sys.stdin:
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            msg = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            print(f"[MCP_Socket_Maya_server] JSON parse error: {exc}",
                  file=sys.stderr, flush=True)
            continue
        try:
            _handle(msg, port)
        except Exception:  # noqa: BLE001 — keep the server alive no matter what
            import traceback
            print(f"[MCP_Socket_Maya_server] handler error:\n"
                  f"{traceback.format_exc()}", file=sys.stderr, flush=True)
            if msg.get("id") is not None:
                _error(msg["id"], -32603, "internal error")


if __name__ == "__main__":
    main()
