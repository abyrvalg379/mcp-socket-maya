# -*- coding: utf-8 -*-
"""Read-only smoke test for the three MCP Socket bridges (Blender/Maya/Houdini).

Talks raw TCP to LIVE bridges — same wire protocol the stdio servers use:
send {"type": ..., "params": {...}}, read one JSON reply.

STRICTLY READ-ONLY: only ping / scene-info / listing commands are sent.
Never mutates the user's scene (lesson of 2026-09-25: mutations through the
bridge go into the user's live session).

Usage:
    python socket_smoke.py            # probe all three bridges
    python socket_smoke.py blender    # one bridge: blender|maya|houdini

Exit code: 0 if every reachable bridge passed its checks AND at least one
bridge was reachable; 1 otherwise. Unreachable bridges are reported, not failed
(the DCC may simply not be running).
"""

import json
import socket
import sys

BRIDGES = {
    "blender": {
        "port": 9876,
        # Blender bridge has no ping handler — get_bridge_info is the liveness probe.
        "checks": [
            ("get_bridge_info", {}, "bridge_version"),
            ("get_scene_info", {}, "name"),
            ("get_hierarchy", {}, "tree"),
            ("list_presets", {}, "presets"),
            ("get_pipeline_conventions", {}, None),
        ],
    },
    "maya": {
        "port": 7777,
        "checks": [
            ("ping", {}, "bridge_version"),
            ("get_scene_info", {}, None),
            ("get_hierarchy", {"max_nodes": 50}, None),
            ("list_instances", {}, None),
            ("get_session_log_path", {}, None),
        ],
    },
    "houdini": {
        "port": 9877,
        "checks": [
            ("ping", {}, "bridge_version"),
            ("get_scene_info", {}, None),
            ("get_hierarchy", {"max_nodes": 50}, None),
            ("list_instances", {}, None),
            ("get_session_log_path", {}, None),
        ],
    },
}

TIMEOUT = 15


def call(port, ctype, params=None, timeout=TIMEOUT):
    s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    try:
        s.sendall(json.dumps({"type": ctype, "params": params or {}}).encode("utf-8"))
        buf = b""
        while True:
            chunk = s.recv(65536)
            if not chunk:
                raise RuntimeError("bridge closed the connection before replying")
            buf += chunk
            try:
                return json.loads(buf.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
    finally:
        s.close()


def run_bridge(name, cfg):
    print(f"\n=== {name.upper()} :{cfg['port']} ===")
    passed, failed = 0, 0
    try:
        reply = call(cfg["port"], cfg["checks"][0][0], cfg["checks"][0][1])
    except (OSError, RuntimeError) as exc:
        print(f"UNREACHABLE ({exc.__class__.__name__}: {exc}) — DCC not running?")
        return None  # bridge down is not a failure

    for ctype, params, key in cfg["checks"]:
        try:
            reply = call(cfg["port"], ctype, params)
            if reply.get("status") != "success":
                print(f"FAIL {ctype} | {str(reply.get('message', reply))[:120]}")
                failed += 1
                continue
            result = reply.get("result")
            detail = ""
            ok = True
            if key is not None:
                ok = isinstance(result, dict) and key in result
                detail = f"key '{key}' present" if ok else f"key '{key}' missing"
            if ctype == "ping" and isinstance(result, dict) and "port" in result:
                ok = ok and result["port"] == cfg["port"]
                detail += f" (port {result['port']})"
            print(("PASS " if ok else "FAIL ") + ctype + (" | " + detail if detail else ""))
            passed += ok
            failed += not ok
        except (OSError, RuntimeError, json.JSONDecodeError) as exc:
            print(f"FAIL {ctype} | {exc.__class__.__name__}: {exc}")
            failed += 1
    return failed == 0


def main():
    targets = sys.argv[1:] or list(BRIDGES)
    results = {}
    for name in targets:
        if name not in BRIDGES:
            print(f"unknown bridge: {name} (known: {', '.join(BRIDGES)})")
            return 1
        results[name] = run_bridge(name, BRIDGES[name])

    reachable = {k: v for k, v in results.items() if v is not None}
    print("\n=== SUMMARY ===")
    for name, ok in results.items():
        state = "DOWN (skipped)" if ok is None else ("PASS" if ok else "FAIL")
        print(f"{name:8s} {state}")
    if not reachable:
        print("no bridges reachable — nothing to smoke")
        return 1
    return 0 if all(reachable.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
