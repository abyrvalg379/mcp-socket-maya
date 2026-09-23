# MCP Socket for Maya — v0.2.0

Maya-ветка семейства mcp-socket. TCP-листенер внутри Maya с общим протоколом
mcp-socket / blender-mcp 1.6.x. v0.1.0 заменил старый HTTP-мост; v0.2.0 доводит
функциональность до паритета с Blender-версией v2.6.0.

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

- Перезапуск Maya автоматически поднимает TCP-листенер; deferred-старт ставит
  шельф-кнопку, хартбит реестра и сторож undo-чанков.
- Перезапуск ZCode подхватывает новый stdio-сервер (тулы появятся со следующей сессии).
- Порт занят (вторая Maya) → авто-оффсет 7778, 7779, ... Клиент задаёт порт
  через `--port` или `MAYA_MCP_SOCKET_PORT`.

## Тулы (13)

| Тул | Что делает |
|-----|------------|
| `ping_maya` | версии, pid, порт, сцена, юниты, счётчики + `agent_chunk_open` |
| `execute_maya_code` | Python в Maya: `cmds`/`om`/`omui`/`mel`/`mutils` прединжектированы, stdout/stderr ловятся, переменная `result` возвращается (JSON-safe). Undo — по сессиям (см. ниже) |
| `undo_agent_session` | откат всей агентской сессии одним undo-шагом |
| `get_scene_info` | файл, юниты, up-axis, фреймы, счётчики по типам, топ-объекты |
| `get_hierarchy` | DAG-дерево (пути/типы/глубина/visibility), кап `max_nodes` (800) + флаг `truncated` |
| `get_screenshot` | **физический** захват экрана (`QScreen.grabWindow`): `mode="window"` кропит окно Maya, `"screen"` весь экран; `focus=true` поднимает Maya (крадёт фокус — если юзер работает в другом DCC, фокус вернётся к нему за долю секунды); `max_size` даунскейл |
| `get_console_log` | ринг stdout/stderr ВСЕЙ сессии Maya (глобальный tee + пер-снипетные захваты; last_n/filter/stream) |
| `clear_console_log` | очистка ринга |
| `list_instances` | живые инстансы Maya из реестра в %TEMP% (pid/порт/версия/сцена) |
| `export_fbx` | PROKLADKA-нейтральный экспорт: метры, Y-up, binary; `preset` neutral/maya/houdini/ue (настройки одни, отличается note-контракт приёмника), `scope` selected/scene |
| `import_fbx` | импорт под контейнер-приёмник (t=0 r=0 s=1), отчёт bbox в метрах, флаг корней >50 м; никаких магических множителей; честная ошибка, если импортёр сессии вернул ноль нод |
| `replay_last_session` | повтор модифицирующих команд последней сессии из JSONL-лога; read-only скипаются, отказ шага не стопит остальные |
| `get_session_log_path` | путь свежайшего JSONL-лога сессий |

## Undo-сессии (аналог Blender v2.1.0)

- Команда, пришедшая после паузы >10 с, открывает именованный undo-чанк
  **«MCP Socket: agent session»**; следующие вызовы в пределах гэпа вливаются в него.
- QTimer-сторож закрывает чанк, когда агент замолчал на гэп, — ручные правки
  юзера после агентской сессии в чанк не попадают.
- `undo_agent_session` (или один Ctrl+Z) откатывает сессию целиком — ТОЛЬКО
  если чанк всё ещё верх undo-очереди; иначе честный отказ (чтобы не съесть
  работу юзера поверх сессии).
- `undo_chunk=false` в execute_maya_code закрывает открытый чанк и пишет вызов
  без чанка.

## Лог и реплей сессий (аналог Blender v2.6.0)

- Каждая записанная команда дописывает JSON-строку в
  `%TEMP%\mcp_socket_maya\sessions\session_<штамп>.jsonl`; гэп >10 с = новый
  файл, хранятся 30 свежайших; ping/консоль/реестр/пути не пишутся.
- `replay_last_session` перезапускает модифицирующие команды (execute_maya_code,
  export_fbx, import_fbx); read-only и уже-реплеиные скипаются; шаги пишутся в
  текущий лог с `replay: true`; реплей целиком попадает в один undo-чанк.
- Кнопки в окне: Replay Last Session, Copy Log Path.

## Реестр инстансов (аналог v1.3.0 Blender)

`%TEMP%\mcp_socket_maya_instances\pid_<pid>.json` (порт/версия/сцена/ts),
хартбит 10 с, stale 25 с; `stop()` снимает свой файл. Глобальный tee на
sys.stdout/sys.stderr кормит ринг консоли системными печатями Maya
(самолечение в хартбите — переустановка может оставить стрим на старой обёртке).

## UI: шельф-кнопка + окно

- Кнопка **MCP Socket** ставится во вкладку Custom полки (идемпотентно, дубли
  по annotation чистятся), иконка вшита base64 и пишется рядом с модулем.
- Кнопка открывает окно = аналог N-панели Blender-версии: шапка с версией и
  статусом (порт/pid/сцена), Undo Agent Work, Agent Sessions, Console Log
  (12 строк + Save to File с путём в клипборд + Clear + Refresh), Pipeline FBX
  (пресет/скоуп/Export, путь/Import). Кнопки окна зовут хендлеры в процессе
  (тот же главный поток, аудируются в JSONL как обычные команды).

## FBX-пресеты (аналог v2.2.0 Blender)

- Экспорт: `FBXResetExport` + `FBXExportConvertUnitString -v "m"` +
  `FBXExportUpAxis y` + `FBXExportInAscii -v 0` + `FBXExport -f path [-s]`.
- Импорт: `cmds.file(path, i=True, type="FBX", ignoreVersion=True,
  mergeNamespacesOnClash=False, options="mo=1", pr=True, returnNewNodes=True)`
  — MEL-путь (`FBXImport`) теряет меши, не использовать. Новые корни парятся
  диффом `ls(long=True)`; bbox до группировки (после группировки пути детей
  меняются — `exactWorldBoundingBox` по старым путям падает).

## Скрытые грабли (выловлено)

1. `MQtUtil` живёт в `maya.OpenMayaUI` (1.0 API), НЕ в `maya.api.OpenMayaUI`.
2. Прединжектированный `mel` — это САМА функция `maya.mel.eval`: вызывать
   `mel("cmd;")`, не `mel.eval(...)`. На уровне модуля —
   `from maya.mel import eval as mel_eval`.
3. `cmds.ls(world=True)` не существует — весь DAG: `ls(long=True, dag=True)`,
   верхний уровень: `ls(assemblies=True)`.
4. `cmds.currentUnit(query=True, angle=True)` — флаг `angle`, не `angular`.
5. Шейп-фильтр: `ls(path, shapes=True)` возвращает КОРОТКОЕ имя — сравнивать
   только с `long=True`.
6. Ошибка сниппета возвращается In-Band (`error: true` + traceback в stderr),
   status остаётся success — исключение хендлера и исключение кода агента
   различаются.
7. `cmds.undoInfo` в Maya 2025 НЕ имеет флага `undoQueue` (списка чанков нет);
   имя верхнего чанка: `cmds.undoInfo(query=True, undoName=True)`.
8. MEL FBX: у `FBXExportUpAxis` значение голое (`FBXExportUpAxis y`), у прочих
   (`ConvertUnitString`, `InAscii`, `SmoothMesh`) — через `-v`.
   `FBXExportApplyUnitScale` в 2025 НЕ существует.
9. **FBX-импорт в долгоживущей GUI-сессии может молча возвращать ноль нод**
   (файл валиден — свежий mayapy его ест, чужие файлы та же сессия ест;
   перезагрузка fbxmaya не лечит). Поэтому import_fbx орёт RuntimeError при
   нуле нод, а не рапортует пустой успех. Лечится рестартом Maya.
10. Скриншот без `focus=true` ловит то, что реально на экране; если юзер
    активно работает в другом DCC, он вернёт фокус себе быстрее sleep(0.4) —
    для проверки РАСКЛАДКИ окна берите `widget.grab()` окна моста, для
    проверки экрана — физический захват (см. [[measure-display-space]]).

## Пенсия старого моста / горячая замена

Старый HTTP-код заменён в тех же файлах; `mcp_startup.py` и конфиг нетронуты.
Горячая замена в живой Maya (проверено 5 раз за сессию): в execute_maya_code
через `mutils.executeDeferred(_do)` — stop старого, purge
`sys.modules["maya_mcp_server"]`, import, start(). Соединение не рвётся,
ответ «scheduled» возвращается до свапа. Строго deferred: shutdown изнутри
своего хендлера мертволжится.

## Что НЕ портировано (из Blender-версии, по мере нужды)

Undo по кнопке из чужого процесса — есть; offscreen-рендер (у Maya свой
playblast); реестр в общем виде совмещён с Blender-версией только идеей, не
форматом файла.
