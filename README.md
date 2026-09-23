# MCP Socket for Maya

Local MCP bridge for Autodesk Maya — the Maya branch of the [mcp-socket](https://github.com/abyrvalg379/mcp-socket) family. A TCP listener runs inside Maya and speaks the same wire protocol as the Blender bridge (blender-mcp 1.6.x compatible), so any MCP client can drive Maya through typed tools.

Part of the **STUKACH — Pipeline Asset Validation System** toolset.

**Author:** Maksim Kovalev · **Version:** 0.1.0 · **License:** GPL-3.0

*Документация на русском: [README.ru.md](README.ru.md)*

## How it works

```
MCP client → maya_mcp.py (stdio) → TCP 127.0.0.1:7777 → maya_mcp_server.py (inside Maya)
```

Every command is marshalled to Maya's main thread (`maya.utils.executeInMainThreadWithResult`); the socket itself lives on a daemon thread. Wire protocol — one JSON document per request, no framing:

```
→ {"type": "ping", "params": {}}
← {"status": "success", "result": {...}}
```

## Tools (7)

| Tool | Purpose |
|------|---------|
| `ping_maya` | Maya version, pid, port, scene, units, object counts |
| `execute_maya_code` | Python inside Maya — `cmds` / `om` / `omui` / `mel` / `mutils` preinjected, stdout/stderr captured, optional `result` variable returned JSON-safely, `undo_chunk=True` wraps each call into one undo step |
| `get_scene_info` | filepath, units, up-axis, frame range, counts by type, top-level objects |
| `get_hierarchy` | DAG tree (full paths, types, depth, visibility), capped at `max_nodes` with a `truncated` flag |
| `get_screenshot` | **physical** screen capture via `QScreen.grabWindow` (the CopyFromScreen class — honest on scaled monitors, unlike `QWidget.grab`); `mode="window"` crops the Maya window, `"screen"` keeps everything; `focus=true` raises Maya first |
| `get_console_log` | ring buffer of stdout/stderr captured per executed snippet (`last_n` / `filter` / `stream`) |
| `clear_console_log` | clear the ring |

The screenshot tool exists because UI verification on a scaled monitor can only be trusted from a physical grab — this is the same lesson as PowerShell `CopyFromScreen` vs offscreen widget rendering.

## Install

1. Grab `mcp_socket_maya_v*.zip` from the [latest release](https://github.com/abyrvalg379/mcp-socket-maya/releases/latest) and unpack (or take the two `.py` files from the repo).
2. Copy `maya_mcp_server.py` and `maya_mcp.py` into `Documents/maya/<version>/scripts/`.
3. Restart Maya — the listener auto-starts (see below), bound to `127.0.0.1:7777`. If the port is busy (a second Maya), it walks 7778, 7779, …

### Auto-start

Add one deferred call to `userSetup.py` (keep it minimal):

```python
import maya.utils as mutils

def _start_mcp():
    import sys
    scripts = r"C:\Users\<you>\Documents\maya\2025\scripts"
    if scripts not in sys.path:
        sys.path.insert(0, scripts)
    try:
        import maya_mcp_server
        maya_mcp_server.start()
    except Exception as e:
        print(f"[mcp_startup] {e}")

mutils.executeDeferred(_start_mcp)
```

### Connect from any MCP client

Zero dependencies beyond the Python standard library. Example for a JSON-configured client:

```json
{
  "mcpServers": {
    "maya": {
      "command": "python",
      "args": ["path/to/maya_mcp.py"]
    }
  }
}
```

`--port 7778` or the `MAYA_MCP_SOCKET_PORT` env var selects a second Maya instance.

## Security

localhost-only, no authentication, and `execute_maya_code` runs arbitrary Python in your Maya — this is a single-workstation tool for artist+agent workflows, not a service. Do not expose the port.

## Related tools

- [mcp-socket](https://github.com/abyrvalg379/mcp-socket) — the Blender branch of the family (undo checkpoints, console log ring, pipeline FBX presets, offscreen render)
- [PROKLADKA](https://github.com/abyrvalg379/prokladka) — FBX bridge Blender ↔ Maya ↔ Houdini ↔ UE
- [STUKACH](https://github.com/abyrvalg379/STUKACH) / [STUKACH for Maya](https://github.com/abyrvalg379/STUKACH_Maya) — pipeline asset validators
