# MCP Socket for Maya

Локальный MCP-мост для Autodesk Maya — майская ветка семейства [mcp-socket](https://github.com/abyrvalg379/mcp-socket). TCP-листенер живёт внутри Maya и говорит на том же wire-протоколе, что и Blender-мост (blender-mcp 1.6.x совместимый) — любой MCP-клиент управляет Maya через типизированные тулы.

**Автор:** Maksim Kovalev · **Версия:** 0.2.0 · **Лицензия:** GPL-3.0

*English documentation: [README.md](README.md)*

## Как это работает

```
MCP-клиент → maya_mcp.py (stdio) → TCP 127.0.0.1:7777 → maya_mcp_server.py (внутри Maya)
```

Каждая команда исполняется в главном потоке Maya (`maya.utils.executeInMainThreadWithResult`); сокет живёт в демоне-потоке. Протокол — один JSON-документ на запрос, без фрейминга:

```
→ {"type": "ping", "params": {}}
← {"status": "success", "result": {...}}
```

## Тулы (13)

| Тул | Назначение |
|-----|------------|
| `ping_maya` | версия Maya, pid, порт, сцена, юниты, счётчики объектов |
| `execute_maya_code` | Python внутри Maya — `cmds` / `om` / `omui` / `mel` / `mutils` прединжектированы, stdout/stderr ловятся, опциональная переменная `result` возвращается (JSON-safe); undo — посессионный (см. ниже) |
| `undo_agent_session` | откат всей агентской сессии одним undo-шагом |
| `get_scene_info` | файл, юниты, up-axis, диапазон кадров, счётчики по типам, топ-объекты |
| `get_hierarchy` | DAG-дерево (полные пути, типы, глубина, visibility), кап `max_nodes` + флаг `truncated` |
| `get_screenshot` | **физический** захват экрана через `QScreen.grabWindow` (класс CopyFromScreen — честный на масштабированных мониторах, в отличие от `QWidget.grab`); `mode="window"` кропит окно Maya, `"screen"` весь экран; `focus=true` поднимает Maya перед захватом |
| `get_console_log` | кольцевой буфер stdout/stderr всей сессии Maya — глобальный tee плюс пер-снипетные захваты (`last_n` / `filter` / `stream`) |
| `clear_console_log` | очистка кольца |
| `list_instances` | живые инстансы Maya из реестра в `%TEMP%` (pid, порт, версия, сцена) |
| `export_fbx` | PROKLADKA-нейтральный экспорт — метры, Y-up, binary; `preset` `neutral`/`maya`/`houdini`/`ue` (настройки одни, отличается note-контракт приёмника), `scope` `selected`/`scene` |
| `import_fbx` | импорт под контейнер-приёмник (t=0 r=0 s=1), bbox в отчёте в метрах, корни >50 м помечаются; никаких магических множителей; честная ошибка, если импортёр сессии вернул ноль нод |
| `replay_last_session` | повтор модифицирующих команд последней записанной сессии из JSONL-лога |
| `get_session_log_path` | путь свежайшего JSONL-лога сессий |

Тул скриншота существует потому, что верификация UI на масштабированном мониторе доверяет только физическому захвату — тот же урок, что PowerShell `CopyFromScreen` против оффскрин-рендера виджета.

## Эргономика агента

- **Undo Agent Work** — команда, пришедшая после паузы >10 с, открывает один
  именованный undo-чанк; следующие вызовы в пределах гэпа вливаются в него;
  QTimer-сторож закрывает чанк, когда агент замолчал, — ручные правки юзера
  в чанк не попадают. `undo_agent_session` (или один Ctrl+Z, пока чанк на
  верху очереди) откатывает сессию целиком — и честно отказывается, если
  сессия уже не верх undo-очереди.
- **Лог и реплей сессий** — каждая записанная команда дописывает JSON-строку
  в `%TEMP%\mcp_socket_maya\sessions\`; гэп >10 с = новый файл, хранятся 30
  свежайших. `replay_last_session` перезапускает модифицирующие команды,
  скипает read-only и помечает реплеиные шаги `replay: true`.
- **Шельф-кнопка + окно** — идемпотентная кнопка **MCP Socket** встаёт во
  вкладку Custom полки (иконка вшита в модуль) и открывает окно с теми же
  секциями, что N-панель Blender-версии: шапка со статусом, Undo Agent Work,
  Agent Sessions (реплей / копировать путь лога), просмотр лога консоли,
  Pipeline FBX (пресет + скоуп + экспорт / импорт).

## Установка

1. Скачайте `mcp_socket_maya_v*.zip` из [последнего релиза](https://github.com/abyrvalg379/mcp-socket-maya/releases/latest) (или возьмите два `.py`-файла из репо).
2. Скопируйте `maya_mcp_server.py` и `maya_mcp.py` в `Documents/maya/<версия>/scripts/`.
3. Перезапустите Maya — листенер поднимется сам на `127.0.0.1:7777`. Порт занят (вторая Maya) — авто-переход на 7778, 7779, …

### Автостарт

Один отложенный вызов в `userSetup.py` (держим его минимальным):

```python
import maya.utils as mutils

def _start_mcp():
    import sys
    scripts = r"C:\Users\<вы>\Documents\maya\2025\scripts"
    if scripts not in sys.path:
        sys.path.insert(0, scripts)
    try:
        import maya_mcp_server
        maya_mcp_server.start()
    except Exception as e:
        print(f"[mcp_startup] {e}")

mutils.executeDeferred(_start_mcp)
```

### Подключение любого MCP-клиента

Ноль зависимостей сверх стандартной библиотеки Python. Пример JSON-конфига:

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

`--port 7778` или переменная `MAYA_MCP_SOCKET_PORT` выбирают второй инстанс Maya.

## Безопасность

Только localhost, без аутентификации; `execute_maya_code` исполняет произвольный Python в вашей Maya — это инструмент одной рабочей станции для связки «художник + агент», а не сервис. Порт наружу не выставлять.

## Смежные туры

- [mcp-socket](https://github.com/abyrvalg379/mcp-socket) — Blender-ветка семейства (undo-чекпоинты, лог-кольцо, FBX-пресеты PROKLADKA, оффскрин-рендер)
- [PROKLADKA](https://github.com/abyrvalg379/prokladka) — FBX-мост Blender ↔ Maya ↔ Houdini ↔ UE
- [STUKACH](https://github.com/abyrvalg379/STUKACH) / [STUKACH for Maya](https://github.com/abyrvalg379/STUKACH_Maya) — пайплайн-валидаторы ассетов
