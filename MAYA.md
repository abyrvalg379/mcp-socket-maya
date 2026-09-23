# MCP Socket for Maya — v1.0.0

Maya-ветка семейства mcp-socket. Заменяет старый HTTP-мост (maya_mcp + HTTPServer
на :7777) TCP-листенером с общим протоколом mcp-socket / blender-mcp 1.6.x.

## Архитектура

```
MCP-клиент (ZCode) → maya_mcp.py (stdio) → TCP 127.0.0.1:7777 → maya_mcp_server.py (внутри Maya)
```

- **maya_mcp_server.py** — TCP-листенер внутри Maya. Каждый запрос исполняется в
  главном потоке через `maya.utils.executeInMainThreadWithResult`. Сокет — в
  демоне-потоке.
- **maya_mcp.py** — stdio MCP-сервер (newline-delimited JSON-RPC, только stdlib).
- Протокол: `{"type": ..., "params": {...}}` → `{"status": "success", "result": ...}` /
  `{"status": "error", "message": "..."}`, без фрейминга (JSON-накопление).

## Установка / автостарт

Файлы живут в `Documents\maya\2025\scripts\` под ТЕМИ ЖЕ именами, что и старый
мост — конфиг ZCode (`mcp.servers.maya → maya_mcp.py`) и цепочка автостарта
(`userSetup.py → mcp_startup.py → maya_mcp_server.start()`) не менялись вовсе.

- Перезапуск Maya автоматически поднимает TCP-листенер.
- Перезапуск ZCode подхватывает новый stdio-сервер (тулы появятся со следующей сессии).
- Порт занят (вторая Maya) → авто-оффсет 7778, 7779, ... Клиент задаёт порт
  через `--port` или `MAYA_MCP_SOCKET_PORT`.

## Тулы (7)

| Тул | Что делает |
|-----|------------|
| `ping_maya` | версии, pid, порт, сцена, юниты, счётчики |
| `execute_maya_code` | Python в Maya: `cmds`/`om`/`omui`/`mel`/`mutils` прединжектированы, stdout/stderr ловятся, переменная `result` возвращается (JSON-safe), `undo_chunk=True` по умолчанию — один вызов = один undo-шаг |
| `get_scene_info` | файл, юниты, up-axis, фреймы, счётчики по типам, топ-объекты |
| `get_hierarchy` | DAG-дерево (пути/типы/глубина/visibility), кап `max_nodes` (800) + флаг `truncated` |
| `get_screenshot` | **физический** захват экрана (`QScreen.grabWindow`): `mode="window"` кропит окно Maya, `"screen"` весь экран; `focus=true` поднимает Maya перед захватом (крадёт фокус!); `max_size` даунскейл |
| `get_console_log` | ринг stdout/stderr по исполнениям (last_n/filter/stream) |
| `clear_console_log` | очистка ринга |

## Скрытые грабли (уже выловлены)

1. `MQtUtil` живёт в `maya.OpenMayaUI` (1.0 API), НЕ в `maya.api.OpenMayaUI`.
2. `cmds.ls(world=True)` не существует — весь DAG: `ls(long=True, dag=True)`,
   верхний уровень: `ls(assemblies=True)`.
3. `cmds.currentUnit(query=True, angle=True)` — флаг `angle`, не `angular`.
4. Шейп-фильтр: `ls(path, shapes=True)` возвращает КОРОТКОЕ имя — сравнивать
   только с `long=True`.
5. Скриншот без `focus=true` ловит то, что реально на экране (может быть
   Blender поверх Maya) — это честное поведение физического захвата
   (см. [[measure-display-space]]: CopyFromScreen = ground truth).
6. Ошибка сниппета возвращается In-Band (`error: true` + traceback в stderr),
   status остаётся success — исключение хендлера и исключение кода агента
   различаются.

## Пенсия старого моста

Старый HTTP-код заменён в тех же файлах; `mcp_startup.py` и конфиг нетронуты.
Горячая замена в живой Maya: остановить старый (`maya_mcp_server.stop()`) —
строго через `executeDeferred`, иначе `shutdown()` изнутри своего HTTP-хендлера
мертволжится, затем purge `sys.modules["maya_mcp_server"]` → import → `start()`.

## Фаза 2 (кандидаты, не делать без ТЗ)

- Undo-сессии как в Blender v2.1 (чекпоинт по гэпу, а не чанк на вызов).
- Логирование/реплей агентских сессий (v2.6-архитектура, JSONL).
- FBX export/import по пресетам PROKLADKA (receiver rule).
- Реестр инстансов (list_instances), системный лог script-editor.
