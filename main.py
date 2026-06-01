"""
飞书 × Claude Code Bot
通过飞书 WebSocket 长连接接收私聊/群聊消息，调用本机 claude CLI 回复，支持流式卡片输出。

启动：python main.py
"""

import asyncio
import json
import re
import sys
import os
import threading
import time
import traceback
from http.server import HTTPServer, BaseHTTPRequestHandler

# 确保项目目录在 sys.path 最前面
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import lark_oapi as lark
from lark_oapi.api.im.v1.model import P2ImMessageReceiveV1
from lark_oapi.event.callback.model.p2_card_action_trigger import (
    P2CardActionTrigger, P2CardActionTriggerResponse, CallBackToast,
)
from lark_oapi.api.application.v6.model.p2_application_bot_menu_v6 import (
    P2ApplicationBotMenuV6,
)

import bot_config as config
from feishu_client import FeishuClient
from session_store import SessionStore, generate_summary, _write_custom_title
from commands import parse_command, handle_command
from claude_runner import run_claude
from run_control import ActiveRun, ActiveRunRegistry, stop_run
import demo_mode

# ── 演示模式状态（录屏用）────────────────────────────────────
_BOT_DIR = os.path.dirname(os.path.abspath(__file__))
_DEMO_ON: set[str] = set()       # 开启演示模式的 user_id
_DEMO_IDX: dict[str, int] = {}   # 每个 user 下一步要播的 step 序号（0-based）

# ── 看门狗：定时重启防止 WebSocket 假死 ──────────────────────

MAX_UPTIME = 0   # 0 = 禁用「定时自杀重启」，依赖飞书 SDK 自带的 ws 自动重连+心跳保活。
                 # 历史上的 4*3600 硬重启在 Windows 上派生新进程极易失败（文件句柄被子进程
                 # 继承占用 / 派生链静默断裂），反而是 bot 长时间掉线的主因（2026-06-01 03:39）。
_start_time = time.time()
_last_event = time.time()


def _spawn_relauncher() -> bool:
    """直接派生一个完全独立的新 bot 进程并确认其存活；存活才返回 True（调用方可安全退出）。

    旧实现走「WMI → wscript → 静默重启.vbs →(sleep 5s)→ python」四跳，且不校验这条链
    是否真把新 python 拉起来，只要那条 PowerShell 命令本身没报错就当成功 → 老进程 os._exit
    自杀。任一跳静默失败（2026-06-01 03:39 即如此）就「自杀不复活」，bot 长时间掉线。

    现改为：老进程用 DETACHED_PROCESS 直接启动 sys.executable main.py（同一套 .venv 解释器），
    新进程脱离本进程树、不被本进程 os._exit 连带端掉；启动后确认它没有秒崩再返回 True。
    新进程从启动到连上飞书 ws 约需 8s，而老进程确认存活(约 4s)后才退出，两者连飞书的时间窗
    不重叠，因此不会「双开重复回复」。返回 False = 没拉起来，调用方绝不能退出（保持在线）。
    """
    import subprocess

    bot_dir = os.path.dirname(os.path.abspath(__file__))
    main_py = os.path.join(bot_dir, "main.py")
    log_path = os.path.join(bot_dir, "bot.log")
    DETACHED_PROCESS = 0x00000008
    CREATE_NEW_PROCESS_GROUP = 0x00000200

    # 让子进程经 cmd 自己用 `>>` 追加写 bot.log（与首次启动 / vbs 同款的宽松共享模式）。
    # 父进程绝不直接 open(bot.log)：当前进程的 stdout 已独占该文件，Python 再 open 会触发
    # Windows 文件共享冲突 [Errno 13] Permission denied。cmd 的 `>>` 用 FILE_SHARE_WRITE，
    # 可与现有写者并存，老进程退出后句柄自然释放。
    # 关键时序（规避 Windows 两道墙：bot.log 文件独占 + 飞书 ws 双连重复回复）：
    # cmd 先 ping 空转约 7s，期间老进程会 os._exit 退出，释放对 bot.log 的占用并断开飞书 ws；
    # 之后 cmd 才起 python 追加写 bot.log（此时文件已空闲，不再 [Errno13]/「文件被占用」），
    # 新 python 再连飞书（此时老连接已断，不会双开）。派生用 Popen+DETACHED 直连，
    # 不经 WMI/wscript/vbs 三跳——那正是 2026-06-01 03:39「派生链静默断裂」的病根。
    # 整条命令必须作为单一字符串传 Popen（用列表会被 list2cmdline 转义破坏）。
    # 用 ping 当延时而非 timeout：timeout 在无控制台的 DETACHED 进程里会因无 stdin 失败。
    cmdline = (
        'cmd /c ping -n 8 127.0.0.1 >nul & '
        'set "PYTHONUTF8=1" & set "PYTHONIOENCODING=utf-8" & '
        f'"{sys.executable}" -u "{main_py}" >> "{log_path}" 2>&1'
    )
    try:
        proc = subprocess.Popen(
            cmdline,
            cwd=bot_dir,
            stdin=subprocess.DEVNULL,
            close_fds=True,
            creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP,
        )
    except Exception as e:
        print(f"[watchdog] 派生新进程失败，放弃本次重启、保持在线: {e}", flush=True)
        return False

    # 确认重启器 cmd 已创建且没有秒崩（Popen 返回即已脱离本进程树，比 WMI 异步派生更可信）。
    time.sleep(1.5)
    if proc.poll() is not None:
        print(f"[watchdog] 重启器cmd启动后立即退出(code={proc.returncode})，放弃本次重启、保持在线", flush=True)
        return False

    print(f"[watchdog] 已派生独立重启器 (cmd pid={proc.pid})，将在其 ~7s 等待窗口内退出本进程", flush=True)
    return True


def _watchdog():
    """后台线程，定期检查进程健康。满 MAX_UPTIME 主动重启以刷新 WebSocket 长连接。"""
    global _start_time
    while True:
        time.sleep(300)  # 每 5 分钟记录一次健康状态（uptime/idle），不再触发自杀重启
        uptime = time.time() - _start_time
        idle = time.time() - _last_event

        if MAX_UPTIME and uptime > MAX_UPTIME:
            print(f"[watchdog] 运行 {uptime/3600:.1f}h，定时重启刷新连接", flush=True)
            # 关键修复：Windows 没有 launchctl 自动拉起，必须先派生替身再退出，
            # 否则就是「定时自杀且不复活」。派生失败就不退出，重置计时下周期再试。
            if _spawn_relauncher():
                time.sleep(1.5)  # 给替身进程一点启动余量后再退出
                os._exit(0)
            print("[watchdog] 派生失败，放弃本次重启、保持在线，下周期重试", flush=True)
            _start_time = time.time()
            continue

        print(f"[watchdog] uptime={uptime/3600:.1f}h idle={idle/60:.0f}min", flush=True)


# ── 全局单例 ──────────────────────────────────────────────────

# 独立的 asyncio 事件循环，启动时即就绪，不依赖 lark SDK 的首条消息
_bot_loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()


def _start_bot_loop():
    asyncio.set_event_loop(_bot_loop)
    _bot_loop.run_forever()


threading.Thread(target=_start_bot_loop, daemon=True, name="bot-loop").start()

lark_client = lark.Client.builder() \
    .app_id(config.FEISHU_APP_ID) \
    .app_secret(config.FEISHU_APP_SECRET) \
    .log_level(lark.LogLevel.INFO) \
    .build()

feishu = FeishuClient(lark_client, app_id=config.FEISHU_APP_ID, app_secret=config.FEISHU_APP_SECRET)
store = SessionStore()
_active_runs = ActiveRunRegistry()

# per-chat 消息队列锁，保证同一群组的消息串行处理，允许不同群组并发处理
_chat_locks: dict[str, asyncio.Lock] = {}
_MAX_CHAT_LOCKS = 200  # 防止无界增长


# ── /stop 命令处理 ───────────────────────────────────────────

async def _announce_stopped_run(active_run: ActiveRun):
    try:
        await feishu.update_card(active_run.card_msg_id, "⏹ 已停止当前任务")
    except Exception as exc:
        print(f"[warn] update stopped card failed: {exc}", flush=True)


async def _announce_interrupted(active_run: ActiveRun):
    try:
        await feishu.update_card(active_run.card_msg_id, "⏹ 已被新消息打断")
    except Exception:
        pass


async def _handle_stop_command(sender_open_id: str) -> str:
    active_run = _active_runs.get_run(sender_open_id)
    if active_run is None:
        return "当前没有正在运行的任务"
    if active_run.stop_requested:
        return "正在停止当前任务，请稍候"
    stopped = await stop_run(
        _active_runs,
        sender_open_id,
        on_stopped=_announce_stopped_run,
    )
    if not stopped:
        return "当前没有正在运行的任务"
    return "已发送停止请求"


async def _handle_restart_command(user_id: str) -> None:
    """重启 bot：派生一个独立的延时重启窗口 → 关闭当前窗口 → 进程退出。

    用于改完代码后一键重启（Python 启动时即把代码读进内存，必须重启才生效）。
    重启脚本内置几秒延时，保证旧进程先退出，避免「同一时间两个 bot」导致重复回复。
    """
    import subprocess

    try:
        await feishu.send_card_to_user(
            user_id,
            content="🔁 正在重启 bot，约 10 秒后恢复。\n若 1 分钟内无响应，去电脑双击「静默启动.vbs」。",
            loading=False,
        )
    except Exception:
        pass

    bot_dir = os.path.dirname(os.path.abspath(__file__))
    relauncher = os.path.join(bot_dir, "静默重启.vbs")

    if not os.path.exists(relauncher):
        print(f"[重启] 找不到重启脚本: {relauncher}", flush=True)
        try:
            await feishu.send_card_to_user(
                user_id, content=f"❌ 重启失败：找不到 {relauncher}", loading=False
            )
        except Exception:
            pass
        return

    # 1) 通过 WMI(Win32_Process.Create) 派生重启脚本。
    #    关键修复：这样新进程的父进程是 WmiPrvSE.exe，脱离了下面 taskkill /T
    #    要杀的进程树，不会被一起端掉。
    #    旧实现用 subprocess.Popen 直接派生 wscript，它是本进程的子进程，
    #    taskkill /T 会把它连根杀掉 → 5 秒延时还没走完就死了 → 永远拉不起新实例。
    #    这里用阻塞 subprocess.run，确保独立重启进程已经存在后，再去关自己（消除竞态）。
    ps_create = (
        "Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments "
        "@{CommandLine='wscript.exe //B \"" + relauncher + "\"';"
        "CurrentDirectory='" + bot_dir + "'} | Out-Null"
    )
    try:
        subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", ps_create],
            cwd=bot_dir,
            timeout=20,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        print(f"[重启] 已通过 WMI 派生独立重启进程（脱离进程树）: {relauncher}", flush=True)
    except Exception as e:
        print(f"[重启] 派生重启脚本失败: {e}", flush=True)
        return  # 派生失败就保持现状，不退出

    # 2) 重启进程已脱离本进程树，现在安全关闭当前窗口 + 旧 python
    await asyncio.sleep(0.3)
    try:
        ppid = os.getppid()
        subprocess.Popen(["taskkill", "/PID", str(ppid), "/T", "/F"])
        print(f"[重启] 关闭当前窗口 ppid={ppid}", flush=True)
    except Exception as e:
        print(f"[重启] 关闭旧窗口失败（忽略）: {e}", flush=True)

    # 3) 兜底：自我退出
    await asyncio.sleep(0.5)
    os._exit(0)


# ── 命令菜单（锁外即时响应）──────────────────────────────────

_COMMAND_MENU_GROUPS = [
    ("**会话**", [
        {"text": "🆕 新会话",      "value": {"action": "run_cmd", "cmd": "/new"}},
        {"text": "📋 新会话(规划)", "value": {"action": "run_cmd", "cmd": "/new plan"}},
        {"text": "📂 恢复会话",    "value": {"action": "run_cmd", "cmd": "/resume"}},
        {"text": "⏹ 停止任务",     "value": {"action": "run_cmd", "cmd": "/stop"}},
    ]),
    ("**配置**", [
        {"text": "🔄 切模型",      "value": {"action": "run_cmd", "cmd": "/model"}},
        {"text": "⚙️ 切模式",      "value": {"action": "run_cmd", "cmd": "/mode"}},
        {"text": "📁 工作空间",    "value": {"action": "run_cmd", "cmd": "/ws"}},
    ]),
    ("**查看**", [
        {"text": "📊 状态",        "value": {"action": "run_cmd", "cmd": "/status"}},
        {"text": "📈 用量",        "value": {"action": "run_cmd", "cmd": "/usage"}},
        {"text": "🛠 Skills",      "value": {"action": "run_cmd", "cmd": "/skills"}},
        {"text": "🔌 MCP",         "value": {"action": "run_cmd", "cmd": "/mcp"}},
        {"text": "📄 目录",        "value": {"action": "run_cmd", "cmd": "/ls"}},
        {"text": "❓ 帮助",        "value": {"action": "run_cmd", "cmd": "/help"}},
    ]),
    ("**热点**", [
        {"text": "🗞️ 拉新闻",      "value": {"action": "news", "cmd": "/news"}},
    ]),
    ("**系统**", [
        {"text": "🔁 重启bot",     "value": {"action": "run_cmd", "cmd": "/restart"}},
    ]),
]


async def _show_command_menu(user_id: str, chat_id: str, is_group: bool, msg_id: str):
    """显示分组命令菜单（markdown 标题 + 按钮混排），不走队列锁"""
    elements = []
    for title, buttons in _COMMAND_MENU_GROUPS:
        elements.append({"tag": "markdown", "content": title})
        columns = []
        for btn in buttons:
            value = {**btn["value"], "cid": chat_id}
            columns.append({
                "tag": "column",
                "width": "auto",
                "elements": [{
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": btn["text"]},
                    "type": "default",
                    "size": "small",
                    "name": f"menu_{btn['value']['cmd'].replace('/', '').replace(' ', '_')}",
                    "value": value,
                    "behaviors": [{"type": "callback", "value": value}],
                }],
            })
        elements.append({"tag": "column_set", "flex_mode": "flow", "columns": columns})
    try:
        if is_group:
            card_id = await feishu.reply_card(msg_id, content="⚡ 快捷命令", loading=False)
        else:
            card_id = await feishu.send_card_to_user(user_id, content="⚡ 快捷命令", loading=False)
        await feishu.update_card_elements(card_id, elements)
    except Exception as e:
        print(f"[error] 命令菜单发送失败: {e}", flush=True)


# ── 核心消息处理（async）─────────────────────────────────────

def extract_chat_info(event: P2ImMessageReceiveV1) -> tuple[str, str, bool]:
    """
    Extract user_id, chat_id, and is_group from message event.

    Returns:
        (user_id, chat_id, is_group)
        - For private chat: chat_id = user_id
        - For group chat: chat_id = group's chat_id
    """
    sender = event.event.sender
    user_id = sender.sender_id.open_id

    message = event.event.message
    chat_type = message.chat_type
    chat_id_raw = message.chat_id

    is_group = (chat_type == "group")

    if is_group:
        chat_id = chat_id_raw
    else:
        chat_id = user_id

    return user_id, chat_id, is_group


async def handle_message_async(event: P2ImMessageReceiveV1):
    """异步处理一条飞书消息"""
    msg = event.event.message
    print(f"[收到消息] type={msg.message_type} chat={msg.chat_type}", flush=True)

    # Extract chat info (supports both private and group chats)
    user_id, chat_id, is_group = extract_chat_info(event)
    print(f"[Chat Info] user={user_id[:8]}... chat={chat_id[:8]}... is_group={is_group}", flush=True)

    # /stop 和 / 在锁外处理（不需要排队等 Claude）
    if msg.message_type == "text":
        try:
            _text = json.loads(msg.content).get("text", "").strip()
        except Exception:
            _text = ""
        # 群聊去掉 @mention
        if is_group:
            for m in (getattr(msg, 'mentions', None) or []):
                k = getattr(m, 'key', '')
                if k:
                    _text = _text.replace(k, '').strip()

        if _text.lower() in ("/stop", "/stop") or _text.strip().endswith("/stop"):
            reply = await _handle_stop_command(user_id)
            if is_group:
                await feishu.reply_card(msg.message_id, content=reply, loading=False)
            else:
                await feishu.send_card_to_user(user_id, content=reply, loading=False)
            return

        # /restart → 重启 bot（锁外即时处理，进程会自我退出并拉起新实例）
        if _text.lower() == "/restart":
            await _handle_restart_command(user_id)
            return

        # 单独输入 / → 显示命令菜单（按钮）
        if _text == "/":
            await _show_command_menu(user_id, chat_id, is_group, msg.message_id)
            return

    # 群聊只响应 @机器人 的消息
    if is_group:
        mentions = getattr(msg, 'mentions', None) or []
        if not mentions:
            return  # 没有 @mention，忽略

    # 自动打断：新消息到达时，停止该用户的活跃任务（模拟终端 Escape）
    active = _active_runs.get_run(user_id)
    if active and not active.stop_requested:
        print(f"[打断] 新消息到达，自动停止当前任务", flush=True)
        await stop_run(_active_runs, user_id, on_stopped=_announce_interrupted)

    # 获取该群组的队列锁，保证同一群组消息串行处理，不同群组可并发
    if chat_id not in _chat_locks:
        # 简单的 LRU 清理：超出上限时清掉所有锁（已释放的锁丢弃无害）
        if len(_chat_locks) >= _MAX_CHAT_LOCKS:
            # 只清理未持有的锁，避免误杀正在使用的锁导致并发串行保护失效
            idle = [k for k, v in _chat_locks.items() if not v.locked()]
            for k in idle[:len(idle) // 2]:
                del _chat_locks[k]
        _chat_locks[chat_id] = asyncio.Lock()
    lock = _chat_locks[chat_id]

    async with lock:
        try:
            await _process_message(user_id, chat_id, is_group, msg)
        except Exception as e:
            print(f"[error] 消息处理异常: {type(e).__name__}: {e}", flush=True)
            traceback.print_exc(file=sys.stdout)
            sys.stdout.flush()


async def _run_and_display(
    user_id: str, chat_id: str, is_group: bool,
    text: str, card_msg_id: str, session, notify_msg_id: str,
):
    """调用 Claude 并流式展示结果，检测选项时附加按钮。消息处理和按钮回复共用此函数。"""
    active_run = _active_runs.start_run(user_id, card_msg_id)

    accumulated = ""
    tool_history: list[str] = []
    ask_options: list[tuple[str, str]] = []  # AskUserQuestion 解析出的选项
    plan_exited = False  # Claude 调了 ExitPlanMode
    last_push_time = 0.0
    push_failures = 0
    _PUSH_INTERVAL = 0.4
    _MAX_STREAM_DISPLAY = 2500

    async def push(content: str):
        nonlocal push_failures
        if push_failures >= 3:
            return
        try:
            await feishu.update_card(card_msg_id, content)
            push_failures = 0
        except Exception as push_err:
            push_failures += 1
            print(f"[warn] push 失败 ({push_failures}/3): {push_err}", flush=True)

    def _build_display() -> str:
        parts = []
        if tool_history:
            parts.append("\n".join(tool_history[-5:]))
        if accumulated:
            if parts:
                parts.append("")
            d = accumulated
            if len(d) > _MAX_STREAM_DISPLAY:
                d = "...\n\n" + d[-_MAX_STREAM_DISPLAY:]
            parts.append(d)
        return "\n".join(parts) if parts else "⏳ 思考中..."

    async def on_tool_use(name: str, inp: dict):
        nonlocal accumulated, last_push_time, plan_exited
        if name.lower() == "exitplanmode":
            plan_exited = True
            return
        if name.lower() == "enterplanmode":
            if session.permission_mode != "plan":
                print(f"[Plan] EnterPlanMode 检测到，切换为 plan", flush=True)
                await store.set_permission_mode(user_id, chat_id, "plan")
            return
        if name.lower() == "enterworktree" and inp:
            wt_name = inp.get("name", "")
            if wt_name:
                print(f"[Worktree] 进入 worktree: {wt_name}", flush=True)
            return
        if name.lower() == "exitworktree":
            print(f"[Worktree] 退出 worktree", flush=True)
            return
        if name.lower() == "askuserquestion":
            question = inp.get("question", inp.get("text", ""))
            if question:
                accumulated += f"\n\n❓ **等待回复：**\n{question}"
                detected = _extract_options(question)
                if detected:
                    ask_options.clear()
                    ask_options.extend(detected)
                await push(_build_display())
                return
        tool_line = _format_tool(name, inp)
        if inp and tool_history:
            tool_history[-1] = tool_line
        else:
            tool_history.append(tool_line)
        await push(_build_display())
        last_push_time = time.time()

    async def on_text_chunk(chunk: str):
        nonlocal accumulated, last_push_time
        accumulated += chunk
        now = time.time()
        if now - last_push_time >= _PUSH_INTERVAL:
            await push(_build_display())
            last_push_time = now

    claude_msg = text
    try:
        print(f"[run_claude] 开始调用...", flush=True)
        full_text, new_session_id, used_fresh_session_fallback = await run_claude(
            message=claude_msg,
            session_id=session.session_id,
            model=session.model,
            cwd=session.cwd,
            permission_mode=session.permission_mode,
            on_text_chunk=on_text_chunk,
            on_tool_use=on_tool_use,
            on_process_start=lambda proc: _active_runs.attach_process(user_id, proc),
        )
        print(f"[run_claude] 完成, session={new_session_id}", flush=True)
    except Exception as e:
        if active_run.stop_requested:
            return
        print(f"[error] Claude 运行失败: {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        try:
            await feishu.update_card(card_msg_id, f"❌ Claude 执行出错：{type(e).__name__}: {e}")
        except Exception:
            pass
        return
    finally:
        _active_runs.clear_run(user_id, active_run)

    # 最终更新卡片，检测选项时附加按钮
    # AskUserQuestion 的内容在 accumulated 里，full_text 可能不含，需要兜底
    final = full_text or accumulated or "（无输出）"
    if used_fresh_session_fallback:
        final = (
            "⚠️ 检测到工作目录已变化，旧会话无法继续。"
            "本次已自动切换到新 session。\n\n" + final
        )
    options = _extract_options(final) or ask_options
    card_patched = False
    try:
        if options:
            buttons = [
                {"text": display, "value": {"reply": value, "cid": chat_id}}
                for display, value in options
            ]
            # 短选项(Y/N等)横排，长选项竖排
            short = all(len(b["text"]) <= 10 for b in buttons)
            await feishu.update_card_with_buttons(card_msg_id, final, buttons, flow=short)
        else:
            await feishu.update_card(card_msg_id, final)
        card_patched = True
    except Exception as e:
        print(f"[error] 卡片更新失败，回退发文本: {e}", flush=True)
        try:
            if is_group and notify_msg_id:
                await feishu.reply_card(notify_msg_id, content=final, loading=False)
            else:
                await feishu.send_text_to_user(user_id, final)
        except Exception as fallback_err:
            print(f"[error] 文本回退也失败: {fallback_err}", flush=True)

    if card_patched:
        try:
            if is_group and notify_msg_id:
                await feishu.reply_text(notify_msg_id, "✅")
            else:
                await feishu.send_text_to_user(user_id, "✅")
        except Exception:
            pass

    if new_session_id:
        await store.on_claude_response(user_id, chat_id, new_session_id, text)

    # ExitPlanMode: Claude 批准方案后要切到执行模式
    if plan_exited and session.permission_mode == "plan":
        print(f"[Plan] ExitPlanMode 检测到，切换为 bypassPermissions", flush=True)
        await store.set_permission_mode(user_id, chat_id, "bypassPermissions")
        try:
            notice = "🚀 已退出规划模式，发送任意消息开始执行。"
            if is_group and notify_msg_id:
                await feishu.reply_text(notify_msg_id, notice)
            else:
                await feishu.send_text_to_user(user_id, notice)
        except Exception:
            pass


def _handle_demo_cmd(user_id: str, t: str) -> str:
    """处理 /demo 系列命令，返回要回复的文本。"""
    parts = t.split()
    sub = parts[1] if len(parts) > 1 else ""
    n = demo_mode.step_count(_BOT_DIR)
    if sub in ("", "on"):
        _DEMO_ON.add(user_id)
        _DEMO_IDX[user_id] = 0
        return (f"🎬 演示模式已开启（共 {n} 步）。发任意消息播放下一步。\n"
                "/demo off 关闭 ｜ /demo reset 重置 ｜ /demo status 查看 ｜ /demo N 跳到第 N 步")
    if sub == "off":
        _DEMO_ON.discard(user_id)
        return "⏹️ 演示模式已关闭，恢复正常对话。"
    if sub == "reset":
        _DEMO_ON.add(user_id)
        _DEMO_IDX[user_id] = 0
        return f"🔄 演示已重置到第 1 步（共 {n} 步），发任意消息开始。"
    if sub == "status":
        on = user_id in _DEMO_ON
        idx = _DEMO_IDX.get(user_id, 0)
        return f"演示模式：{'开启' if on else '关闭'}，下一步 = 第 {idx + 1} 步 / 共 {n} 步。"
    if sub.isdigit():
        idx = max(1, int(sub)) - 1
        _DEMO_ON.add(user_id)
        _DEMO_IDX[user_id] = idx
        return f"⏭️ 已跳到第 {idx + 1} 步（共 {n} 步），发任意消息播放。"
    return "用法：/demo on｜off｜reset｜status｜N"


async def _play_demo(user_id: str, chat_id: str, is_group: bool, msg):
    """演示模式下，播放下一步预写内容。"""
    idx = _DEMO_IDX.get(user_id, 0)
    n = demo_mode.step_count(_BOT_DIR)
    if idx >= n:
        txt = "✅ 演示已播放到最后一步。/demo reset 重新开始，或 /demo off 关闭。"
        if is_group:
            await feishu.reply_card(msg.message_id, content=txt, loading=False)
        else:
            await feishu.send_text_to_user(user_id, txt)
        return

    try:
        if is_group:
            card_msg_id = await feishu.reply_card(msg.message_id, loading=True)
        else:
            card_msg_id = await feishu.send_card_to_user(user_id, loading=True)
    except Exception as e:
        print(f"[demo] 发送占位卡片失败: {e}", flush=True)
        return

    try:
        await demo_mode.play_step(feishu, card_msg_id, _BOT_DIR, idx)
        _DEMO_IDX[user_id] = idx + 1
    except Exception as e:
        print(f"[demo] 播放第 {idx + 1} 步出错: {e}", flush=True)
        traceback.print_exc()
        try:
            await feishu.update_card(card_msg_id, f"❌ 演示播放出错：{e}")
        except Exception:
            pass


async def _process_message(user_id: str, chat_id: str, is_group: bool, msg):
    """实际处理消息的逻辑，在 per-chat lock 保护下执行"""
    print(f"[处理消息] user={user_id[:8]}... chat={chat_id[:8]}... is_group={is_group}", flush=True)
    text = ""
    img_path = None

    if msg.message_type == "text":
        try:
            text = json.loads(msg.content).get("text", "").strip()
        except Exception:
            return
        if not text:
            return

        # 群聊：去掉 @mention 占位符
        if is_group:
            mentions = getattr(msg, 'mentions', None) or []
            for mention in mentions:
                key = getattr(mention, 'key', '')
                if key:
                    text = text.replace(key, '').strip()
            if not text:
                return

        print(f"[文本] {text[:50]}", flush=True)

    elif msg.message_type == "image":
        try:
            image_key = json.loads(msg.content).get("image_key", "")
            if not image_key:
                return
            img_path = await feishu.download_image(msg.message_id, image_key)
            text = f"[用户发送了一张图片，路径：{img_path}，请读取并分析这张图片，直接回复用中文]"
        except Exception as e:
            print(f"[error] 下载图片失败: {e}")
            if is_group:
                try:
                    await feishu.reply_card(msg.message_id, content=f"❌ 下载图片失败：{e}", loading=False)
                except Exception:
                    pass
            else:
                await feishu.send_text_to_user(user_id, f"❌ 下载图片失败：{e}")
            return

    elif msg.message_type == "post":
        # 富文本：图文混发（飞书把「文字 + 图片」一起发的消息归为 post）
        try:
            post = json.loads(msg.content)
        except Exception:
            return

        # 解析正文：按段落拼接 text/a 元素，收集 img 元素的 image_key
        image_keys: list[str] = []
        para_texts: list[str] = []
        for paragraph in post.get("content", []) or []:
            line = ""
            for el in (paragraph or []):
                tag = el.get("tag")
                if tag in ("text", "a"):
                    line += el.get("text", "")
                elif tag == "img":
                    ik = el.get("image_key", "")
                    if ik:
                        image_keys.append(ik)
            if line:
                para_texts.append(line)
        title = (post.get("title") or "").strip()
        text = "\n".join(([title] if title else []) + para_texts).strip()

        # 群聊去掉 @mention 占位符
        if is_group:
            mentions = getattr(msg, 'mentions', None) or []
            for mention in mentions:
                key = getattr(mention, 'key', '')
                if key:
                    text = text.replace(key, '').strip()

        # 下载所有内嵌图片
        img_paths: list[str] = []
        for ik in image_keys:
            try:
                img_paths.append(await feishu.download_image(msg.message_id, ik))
            except Exception as e:
                print(f"[error] 下载富文本图片失败: {e}", flush=True)

        if img_paths:
            paths_str = "、".join(img_paths)
            n = len(img_paths)
            note = (f"\n\n[用户同时发送了 {n} 张图片，路径：{paths_str}，"
                    f"请读取并结合上面的文字一起分析，直接回复用中文]")
            text = (text + note).strip()

        if not text:
            return
        print(f"[富文本] text={text[:50]} imgs={len(img_paths)}", flush=True)

    else:
        return  # 不支持的消息类型

    # ── 演示模式（录屏用）────────────────────────────────────
    t_low = text.strip().lower()
    if t_low.startswith("/demo"):
        reply = _handle_demo_cmd(user_id, t_low)
        if is_group:
            await feishu.reply_card(msg.message_id, content=reply, loading=False)
        else:
            await feishu.send_card_to_user(user_id, content=reply, loading=False)
        return
    if user_id in _DEMO_ON:
        await _play_demo(user_id, chat_id, is_group, msg)
        return

    # ── /news：轻量快讯，替换成 prompt 走正常 Claude 对话路径 ──
    if text.strip().lower() == "/news":
        text = NEWS_QUICK_PROMPT

    # ── 斜杠命令 ──────────────────────────────────────────────
    parsed = parse_command(text)
    if parsed:
        cmd, args = parsed
        print(f"[cmd] 执行命令 {cmd}", flush=True)
        reply = await handle_command(cmd, args, user_id, chat_id, store)
        print(f"[cmd] 命令返回 type={type(reply).__name__}", flush=True)
        if reply is not None:
            if isinstance(reply, dict):
                reply_text, reply_buttons = reply["text"], reply.get("buttons", [])
            else:
                reply_text, reply_buttons = reply, []

            if reply_buttons:
                if is_group:
                    card_id = await feishu.reply_card(msg.message_id, content=reply_text, loading=False)
                else:
                    card_id = await feishu.send_card_to_user(user_id, content=reply_text, loading=False)
                print(f"[按钮] 卡片已发送 card_id={card_id}, 准备添加 {len(reply_buttons)} 个按钮", flush=True)
                try:
                    short = all(len(b["text"]) <= 12 for b in reply_buttons)
                    await feishu.update_card_with_buttons(card_id, reply_text, reply_buttons, flow=short)
                    print(f"[按钮] 按钮添加成功", flush=True)
                except Exception as btn_err:
                    print(f"[按钮] 按钮添加失败: {btn_err}", flush=True)
            else:
                if is_group:
                    await feishu.reply_card(msg.message_id, content=reply_text, loading=False)
                else:
                    await feishu.send_card_to_user(user_id, content=reply_text, loading=False)
            return
        # reply is None → 不是 bot 命令，当作普通消息（含 /xxx）转发给 Claude

    # ── 普通消息 → 调用 Claude ──────────────────────────────
    session = await store.get_current(user_id, chat_id)
    print(f"[Claude] session={session.session_id} model={session.model}", flush=True)

    # 1. 发送"思考中"占位卡片，拿到 message_id
    try:
        if is_group:
            card_msg_id = await feishu.reply_card(msg.message_id, loading=True)
        else:
            card_msg_id = await feishu.send_card_to_user(user_id, loading=True)
        print(f"[卡片] card_msg_id={card_msg_id}", flush=True)
    except Exception as e:
        print(f"[error] 发送占位卡片失败: {e}", flush=True)
        if is_group:
            try:
                await feishu.reply_card(msg.message_id, content=f"❌ 发送消息失败：{e}", loading=False)
            except Exception:
                pass
        else:
            await feishu.send_text_to_user(user_id, f"❌ 发送消息失败：{e}")
        return

    await _run_and_display(user_id, chat_id, is_group, text, card_msg_id, session, msg.message_id)


def _extract_options(text: str) -> list[tuple[str, str]]:
    """从文本中提取选项，适配 Claude Code 原生输出格式。返回 [(按钮文字, 回复值), ...]"""
    lines = text.strip().split('\n')

    # 从末尾向上扫描连续的编号选项
    option_lines = []
    for line in reversed(lines):
        line = line.strip()
        if not line:
            if option_lines:
                break
            continue
        # 匹配: 1. xxx / 1) xxx / 1、xxx / a) xxx / A) xxx
        m = re.match(r'^(\d+|[a-zA-Z])[.）\)、]\s*(.+)', line)
        if m:
            option_lines.append((m.group(1), m.group(2).strip()))
        elif option_lines:
            break
        else:
            break
    option_lines.reverse()
    if len(option_lines) >= 2:
        return [
            (f"{key}. {desc}" if len(desc) <= 18 else f"{key}. {desc[:16]}..", key)
            for key, desc in option_lines
        ]

    # Y/N 及变体
    tail = "\n".join(lines[-3:]) if len(lines) >= 3 else text
    if re.search(r'\by\b.*\bn\b|Y/N|yes.*no|是/否|确认/取消', tail, re.IGNORECASE):
        return [("Yes", "yes"), ("No", "no")]

    return []


def _format_tool(name: str, inp: dict) -> str:
    """格式化工具调用的进度提示"""
    n = name.lower()
    if n == "bash":
        cmd = inp.get("command", "")
        if len(cmd) > 80:
            cmd = cmd[:77] + "..."
        return f"🔧 **执行命令：** `{cmd}`" if cmd else f"🔧 **执行命令...**"
    elif n in ("read_file", "read"):
        return f"📄 **读取：** `{inp.get('file_path', inp.get('path', ''))}`"
    elif n in ("write_file", "write"):
        return f"✏️ **写入：** `{inp.get('file_path', inp.get('path', ''))}`"
    elif n in ("edit_file", "edit"):
        return f"✂️ **编辑：** `{inp.get('file_path', inp.get('path', ''))}`"
    elif n in ("glob",):
        return f"🔍 **搜索文件：** `{inp.get('pattern', '')}`"
    elif n in ("grep",):
        return f"🔎 **搜索内容：** `{inp.get('pattern', '')}`"
    elif n == "task":
        return f"🤖 **子任务：** {inp.get('description', inp.get('prompt', '')[:40])}"
    elif n == "webfetch":
        return f"🌐 **抓取网页...**"
    elif n == "websearch":
        return f"🔍 **搜索：** {inp.get('query', '')}"
    else:
        return f"⚙️ **{name}**"


# ── 飞书事件回调（同步）→ 调度异步任务 ───────────────────────

# ── 卡片按钮点击处理（选项选择）──────────────────────────────

def on_card_action(data: P2CardActionTrigger) -> P2CardActionTriggerResponse:
    """用户点击卡片按钮：选项回复 or 模式切换"""
    global _last_event
    _last_event = time.time()

    event = data.event
    user_id = event.operator.open_id
    value = event.action.value or {}
    action_type = value.get("action", "")
    chat_id = value.get("cid", user_id)
    clicked_msg_id = event.context.open_message_id if event.context else None

    # 模式切换按钮
    if action_type == "set_mode":
        mode = value.get("mode", "")
        if mode:
            asyncio.run_coroutine_threadsafe(_handle_set_mode(user_id, chat_id, mode, clicked_msg_id), _bot_loop)
        resp = P2CardActionTriggerResponse()
        toast = CallBackToast()
        toast.type = "success"
        toast.content = f"已切换: {mode}"
        resp.toast = toast
        return resp

    # 命令菜单按钮 → 当作用户发了一条命令消息
    if action_type == "run_cmd":
        cmd_text = value.get("cmd", "")
        if cmd_text:
            asyncio.run_coroutine_threadsafe(_handle_menu_command(user_id, chat_id, cmd_text, clicked_msg_id), _bot_loop)
        resp = P2CardActionTriggerResponse()
        toast = CallBackToast()
        toast.type = "info"
        toast.content = cmd_text
        resp.toast = toast
        return resp

    # 拉新闻按钮 → 轻量快讯注入当前对话
    if action_type == "news":
        asyncio.run_coroutine_threadsafe(_run_news_in_chat(user_id, chat_id, clicked_msg_id), _bot_loop)
        resp = P2CardActionTriggerResponse()
        toast = CallBackToast()
        toast.type = "info"
        toast.content = "正在拉新闻…"
        resp.toast = toast
        return resp

    # 恢复会话按钮
    if action_type == "resume_session":
        sid = value.get("sid", "")
        if sid:
            asyncio.run_coroutine_threadsafe(_handle_resume_session(user_id, chat_id, sid, clicked_msg_id), _bot_loop)
        resp = P2CardActionTriggerResponse()
        toast = CallBackToast()
        toast.type = "info"
        toast.content = "正在恢复..."
        resp.toast = toast
        return resp

    # 选项回复按钮（发给 Claude）
    reply_text = value.get("reply", "")
    if reply_text:
        print(f"[按钮] user={user_id[:8]}... reply={reply_text}", flush=True)
        asyncio.run_coroutine_threadsafe(_handle_button_reply(user_id, chat_id, reply_text, clicked_msg_id), _bot_loop)

    resp = P2CardActionTriggerResponse()
    toast = CallBackToast()
    toast.type = "info"
    toast.content = f"已发送: {reply_text}"
    resp.toast = toast
    return resp


async def _handle_menu_command(user_id: str, chat_id: str, cmd_text: str, card_msg_id: str):
    """命令菜单按钮点击 → 执行命令并更新卡片"""
    is_group = (chat_id != user_id)
    parsed = parse_command(cmd_text)
    if not parsed:
        return
    cmd, args = parsed

    # /stop 特殊处理
    if cmd == "stop":
        reply_text = await _handle_stop_command(user_id)
        if card_msg_id:
            try:
                await feishu.update_card(card_msg_id, reply_text)
            except Exception:
                pass
        return

    # /restart 特殊处理（进程自我退出并拉起新实例）
    if cmd == "restart":
        await _handle_restart_command(user_id)
        return

    reply = await handle_command(cmd, args, user_id, chat_id, store)
    if reply is None:
        return

    if isinstance(reply, dict):
        reply_text, reply_buttons = reply["text"], reply.get("buttons", [])
    else:
        reply_text, reply_buttons = reply, []

    if card_msg_id:
        try:
            if reply_buttons:
                short = all(len(b["text"]) <= 12 for b in reply_buttons)
                await feishu.update_card_with_buttons(card_msg_id, reply_text, reply_buttons, flow=short)
            else:
                await feishu.update_card(card_msg_id, reply_text)
        except Exception as e:
            print(f"[error] 菜单命令卡片更新失败: {e}", flush=True)


async def _handle_resume_session(user_id: str, chat_id: str, session_id: str, card_msg_id: str):
    """卡片按钮恢复历史会话"""
    sid, old_title = await store.resume_session(user_id, chat_id, session_id)
    if not sid:
        print(f"[resume] 未找到 session: {session_id[:8]}", flush=True)
        return
    print(f"[resume] 已恢复 session: {sid[:8]}", flush=True)
    if card_msg_id:
        try:
            name = store.get_summary(user_id, sid) or f"#{sid[:8]}"
            text = f"✅ 已恢复会话「{name}」，继续对话吧。"
            if old_title:
                text += f"\n上个会话：「{old_title}」"
            await feishu.update_card(card_msg_id, text)
        except Exception:
            pass


async def _handle_set_mode(user_id: str, chat_id: str, mode: str, card_msg_id: str):
    """卡片按钮切换权限模式"""
    from commands import VALID_MODES
    await store.set_permission_mode(user_id, chat_id, mode)
    desc = VALID_MODES.get(mode, "")
    print(f"[模式切换] user={user_id[:8]}... mode={mode}", flush=True)
    if card_msg_id:
        try:
            await feishu.update_card(card_msg_id, f"✅ 已切换为 **{mode}**\n{desc}")
        except Exception:
            pass


async def _handle_button_reply(user_id: str, chat_id: str, text: str, clicked_msg_id: str):
    """按钮点击 → 走正常的 lock + Claude 流程"""
    is_group = (chat_id != user_id)

    # 自动打断活跃任务
    active = _active_runs.get_run(user_id)
    if active and not active.stop_requested:
        await stop_run(_active_runs, user_id, on_stopped=_announce_interrupted)

    if chat_id not in _chat_locks:
        if len(_chat_locks) >= _MAX_CHAT_LOCKS:
            idle = [k for k, v in _chat_locks.items() if not v.locked()]
            for k in idle[:len(idle) // 2]:
                del _chat_locks[k]
        _chat_locks[chat_id] = asyncio.Lock()
    lock = _chat_locks[chat_id]

    async with lock:
        try:
            session = await store.get_current(user_id, chat_id)
            try:
                if is_group and clicked_msg_id:
                    card_msg_id = await feishu.reply_card(clicked_msg_id, loading=True)
                else:
                    card_msg_id = await feishu.send_card_to_user(user_id, loading=True)
            except Exception as e:
                print(f"[error] 按钮回复占位卡片失败: {e}", flush=True)
                return
            await _run_and_display(
                user_id, chat_id, is_group, text,
                card_msg_id, session, clicked_msg_id or "",
            )
        except Exception as e:
            print(f"[error] 按钮回复处理异常: {type(e).__name__}: {e}", flush=True)
            traceback.print_exc(file=sys.stdout)


# ── /news 轻量快讯（发到当前对话，不写存档、不推群）─────────────
# 复用按钮回复链路：把这段 prompt 当成一次 Claude 对话注入当前会话，
# 流式卡片直接出在聊天里。和每天 7:30 的重型存档任务是两回事。
NEWS_QUICK_PROMPT = """请帮我快速拉一份「今日 AI 热点快讯」，直接在当前对话里回复，要求如下：

1. 先调一次 AIHOT 精选基线拿全景：
   curl -sH "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36" "https://aihot.virxact.com/api/public/items?mode=selected&take=30"
2. 从中挑出最近 24-48 小时、最值得知道的 5 到 8 条，必要时用 WebSearch 快速核一下关键事实（AIHOT 标题可能脑补，别照抄）。
3. 输出格式：每条一行，「标题（MM/DD）+ 一句话说清是什么 + 来源」，中文简体，不堆术语。
4. 这是轻量快讯：不要写存档文件、不要推任何飞书群、不要跑完整 8 段日报流程，就在这个对话里给我一段能扫一眼就懂的简报，控制在 1-2 分钟内。
"""


async def _run_news_in_chat(user_id: str, chat_id: str, clicked_msg_id: str):
    """🗞️ 拉新闻按钮 / 输入 /news：把轻量快讯当成一次对话注入当前会话。"""
    await _handle_button_reply(user_id, chat_id, NEWS_QUICK_PROMPT, clicked_msg_id)


# ── 飞书事件回调（同步）→ 调度异步任务 ───────────────────────

def on_message_receive(data: P2ImMessageReceiveV1) -> None:
    """飞书 SDK 同步回调，调度异步任务到 _bot_loop。"""
    global _last_event
    _last_event = time.time()
    asyncio.run_coroutine_threadsafe(handle_message_async(data), _bot_loop)


# ── 机器人自定义菜单 ─────────────────────────────────────────────
# 飞书后台「机器人自定义菜单」配置的每个菜单项绑定一个 event_key（推送事件类型），
# 点击后通过长连接下发 application.bot.menu_v6 事件。下表把 event_key 映射成命令。
# 配置后台时，菜单项的 event_key 必须与下表的键一致。
MENU_KEY_COMMANDS = {
    # ── 高频（建议放悬浮菜单顶层）──
    "resume": "/resume",      # 历史会话列表
    "new": "/new",            # 新建会话
    "model": "/model",        # 模型切换菜单
    "menu": "/",              # 打开完整命令卡片（所有命令的抽屉）
    "restart": "/restart",    # 重启 bot（改完代码一键重启）
    # ── 低频（建议塞子菜单「更多」）──
    "new_plan": "/new plan",  # 新建会话（规划模式）
    "status": "/status",      # 当前状态
    "stop": "/stop",          # 停止当前任务
    "mode": "/mode",          # 切权限模式
    "ws": "/ws",              # 工作空间
    "usage": "/usage",        # 用量
    "skills": "/skills",      # 技能列表
    "mcp": "/mcp",            # MCP 列表
    "ls": "/ls",              # 当前目录
    "help": "/help",          # 帮助
}


async def _handle_menu_event(user_id: str, cmd_text: str):
    """自定义菜单点击 → 发一张卡片承载结果（复用 _handle_menu_command 的执行+更新逻辑）"""
    chat_id = user_id  # 自定义菜单只在单聊场景
    if cmd_text == "/":
        await _show_command_menu(user_id, chat_id, False, "")
        return
    # 先发一张 loading 占位卡片拿到 msg_id，再交给已有的菜单命令处理器更新它
    try:
        card_msg_id = await feishu.send_card_to_user(user_id, loading=True)
    except Exception as e:
        print(f"[菜单事件] 占位卡片发送失败: {e}", flush=True)
        return
    await _handle_menu_command(user_id, chat_id, cmd_text, card_msg_id)


def on_bot_menu(data: P2ApplicationBotMenuV6) -> None:
    """飞书 SDK 同步回调：机器人自定义菜单点击事件。"""
    global _last_event
    _last_event = time.time()
    event_key = ""
    user_id = ""
    try:
        event_key = data.event.event_key or ""
    except Exception:
        pass
    try:
        user_id = data.event.operator.operator_id.open_id or ""
    except Exception:
        pass
    cmd_text = MENU_KEY_COMMANDS.get(event_key, "")
    print(f"[菜单] user={user_id[:8] if user_id else '?'} key={event_key} "
          f"→ {cmd_text or '(未映射)'}", flush=True)
    if not user_id or not cmd_text:
        return
    asyncio.run_coroutine_threadsafe(_handle_menu_event(user_id, cmd_text), _bot_loop)


def on_message_read(data) -> None:
    """飞书「消息已读」回执事件：空处理器。

    后台订阅了 im.message.message_read_v1，但 bot 不需要处理它。
    注册一个空处理器纯粹是为了避免 SDK 反复打印
    `processor not found, type: im.message.message_read_v1` 把日志刷满。
    """
    return


# ── CLI Handover ─────────────────────────────────────────────

async def _handle_handover(session_id: str, cwd: str, model: str,
                           target_user: str = "", target_chat: str = "") -> dict:
    """处理来自 CLI 的 handover 请求：切换飞书当前会话并推送通知"""
    user_id = target_user or store.find_primary_user()
    if not user_id:
        return {"ok": False, "error": "no user found in sessions, pass user_id param"}

    chat_id = target_chat or user_id  # 默认私聊

    result = await store.handover_session(user_id, chat_id, session_id, cwd=cwd, model=model)

    # 构造通知文本
    cur = await store.get_current_raw(user_id, chat_id)
    display_cwd = cur.get("cwd", "~")
    display_model = cur.get("model", "unknown")
    display_mode = cur.get("permission_mode", "bypassPermissions")
    old_summary = result.get("old_summary", "")
    old_note = f"\n上个会话：「{old_summary}」" if old_summary else ""

    notify_text = (
        f"**CLI 会话已接入**\n"
        f"Session: `{session_id[:12]}...`\n"
        f"目录: `{display_cwd}`\n"
        f"模型: `{display_model}`\n"
        f"模式: `{display_mode}`{old_note}\n\n"
        f"直接发消息即可继续对话。"
    )

    try:
        await feishu.send_card_to_user(user_id, content=notify_text, loading=False)
    except Exception as e:
        print(f"[handover] 推送通知失败: {e}", flush=True)

    print(f"[handover] session={session_id[:8]}... cwd={display_cwd}", flush=True)
    return {"ok": True, "user_id": user_id, "session_id": session_id}


# ── 卡片回调 HTTP 服务（配合 ngrok 暴露给飞书）────────────────

class _CardCallbackHandler(BaseHTTPRequestHandler):
    """处理飞书卡片按钮点击的 HTTP 回调"""

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)
        try:
            data = json.loads(body)
        except Exception:
            self._respond(400, {"error": "bad json"})
            return

        # 飞书 URL 验证
        if data.get("type") == "url_verification":
            self._respond(200, {"challenge": data.get("challenge", "")})
            return

        event = data.get("event", {})
        operator = event.get("operator", {})
        user_id = operator.get("open_id", "")
        action = event.get("action", {})
        value = action.get("value", {})
        context = event.get("context", {})

        action_type = value.get("action", "")
        chat_id = value.get("cid", user_id)
        clicked_msg_id = context.get("open_message_id", "")

        print(f"[HTTP回调] user={user_id[:8]}... action={action_type or 'reply'}", flush=True)

        if action_type == "set_mode":
            mode = value.get("mode", "")
            if mode:
                asyncio.run_coroutine_threadsafe(
                    _handle_set_mode(user_id, chat_id, mode, clicked_msg_id),
                    _bot_loop,
                )
            self._respond(200, {"toast": {"type": "success", "content": f"已切换: {mode}"}})
        elif action_type == "run_cmd":
            cmd_text = value.get("cmd", "")
            if cmd_text:
                asyncio.run_coroutine_threadsafe(
                    _handle_menu_command(user_id, chat_id, cmd_text, clicked_msg_id),
                    _bot_loop,
                )
            self._respond(200, {"toast": {"type": "info", "content": cmd_text}})
        elif action_type == "news":
            asyncio.run_coroutine_threadsafe(
                _run_news_in_chat(user_id, chat_id, clicked_msg_id),
                _bot_loop,
            )
            self._respond(200, {"toast": {"type": "info", "content": "正在拉新闻…"}})
        elif action_type == "resume_session":
            sid = value.get("sid", "")
            if sid:
                asyncio.run_coroutine_threadsafe(
                    _handle_resume_session(user_id, chat_id, sid, clicked_msg_id),
                    _bot_loop,
                )
            self._respond(200, {"toast": {"type": "info", "content": "正在恢复..."}})
        else:
            reply_text = value.get("reply", "")
            if reply_text:
                asyncio.run_coroutine_threadsafe(
                    _handle_button_reply(user_id, chat_id, reply_text, clicked_msg_id),
                    _bot_loop,
                )
            self._respond(200, {"toast": {"type": "info", "content": f"已发送: {reply_text}"}})

    def do_GET(self):
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(self.path)

        if parsed.path == "/handover":
            params = parse_qs(parsed.query)
            session_id = params.get("session_id", [""])[0]
            cwd = params.get("cwd", [""])[0]
            model = params.get("model", [""])[0]
            target_user = params.get("user_id", [""])[0]
            target_chat = params.get("chat_id", [""])[0]

            if not session_id:
                self._respond(400, {"error": "session_id required"})
                return

            try:
                future = asyncio.run_coroutine_threadsafe(
                    _handle_handover(session_id, cwd, model, target_user, target_chat),
                    _bot_loop,
                )
                result = future.result(timeout=15)
                self._respond(200, result)
            except Exception as e:
                self._respond(500, {"error": str(e)})
        else:
            self._respond(404, {"error": "not found"})

    def _respond(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # 静默 HTTP 日志


# ── 后台定时摘要生成 ─────────────────────────────────────────

def _bg_summary_thread():
    """后台线程: 每 10 分钟扫描未摘要的会话，逐个生成摘要"""
    time.sleep(60)  # 启动后等 1 分钟再开始
    while True:
        try:
            unsummarized = store.get_all_unsummarized()
            if unsummarized:
                print(f"[摘要] 发现 {len(unsummarized)} 个未摘要会话", flush=True)
                count = 0
                for user_id, sid in unsummarized[:5]:
                    try:
                        summary = generate_summary(sid)
                        if summary:
                            store._data.setdefault(user_id, {}).setdefault("summaries", {})[sid] = summary
                            _write_custom_title(sid, summary)
                            count += 1
                            print(f"[摘要] #{sid[:8]} → {summary}", flush=True)
                    except Exception as e:
                        print(f"[摘要] #{sid[:8]} 失败: {e}", flush=True)
                    time.sleep(5)  # 每个请求间隔 5 秒，避免 429
                if count:
                    store._save()  # 同步原子写入
                    print(f"[摘要] 本轮完成 {count}/{len(unsummarized)} 个", flush=True)
        except Exception as e:
            print(f"[摘要] 定时任务异常: {e}", flush=True)
        time.sleep(600)  # 10 分钟


def _start_callback_server(port):
    server = HTTPServer(('0.0.0.0', port), _CardCallbackHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()


def _start_ngrok(port):
    """启动 ngrok 隧道，返回公网 URL"""
    import subprocess
    import urllib.request

    # 先检查已有的 ngrok 隧道
    try:
        with urllib.request.urlopen("http://127.0.0.1:4040/api/tunnels", timeout=2) as r:
            tunnels = json.loads(r.read())
            for t in tunnels.get("tunnels", []):
                if t.get("proto") == "https":
                    return t["public_url"]
    except Exception:
        pass

    # 启动新 ngrok（有固定域名就用，保证重启后 URL 不变）
    try:
        ngrok_domain = os.environ.get("NGROK_DOMAIN", "")
        ngrok_cmd = ["ngrok", "http", "--url", ngrok_domain, str(port)] if ngrok_domain else ["ngrok", "http", str(port)]
        subprocess.Popen(
            ngrok_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        time.sleep(3)
        with urllib.request.urlopen("http://127.0.0.1:4040/api/tunnels", timeout=5) as r:
            tunnels = json.loads(r.read())
            for t in tunnels.get("tunnels", []):
                if t.get("proto") == "https":
                    return t["public_url"]
    except Exception as e:
        print(f"   [warn] ngrok 启动失败: {e}", flush=True)
    return None


# ── 启动 ──────────────────────────────────────────────────────

def main():
    print("🚀 飞书 Claude Bot 启动中...")
    print(f"   App ID      : {config.FEISHU_APP_ID}")
    print(f"   默认模型    : {config.DEFAULT_MODEL}")
    print(f"   默认工作目录: {config.DEFAULT_CWD}")
    print(f"   权限模式    : {config.PERMISSION_MODE}")

    # 卡片回调 HTTP 服务 + ngrok 隧道
    cb_port = config.CALLBACK_PORT
    _start_callback_server(cb_port)
    ngrok_url = _start_ngrok(cb_port)
    if ngrok_url:
        print(f"   卡片回调    : {ngrok_url}/callback")
    else:
        print(f"   卡片回调    : http://localhost:{cb_port}/callback (需启动 ngrok)")

    handler = lark.EventDispatcherHandler.builder("", "") \
        .register_p2_im_message_receive_v1(on_message_receive) \
        .register_p2_card_action_trigger(on_card_action) \
        .register_p2_application_bot_menu_v6(on_bot_menu) \
        .register_p2_im_message_message_read_v1(on_message_read) \
        .build()

    ws_client = lark.ws.Client(
        config.FEISHU_APP_ID,
        config.FEISHU_APP_SECRET,
        event_handler=handler,
        log_level=lark.LogLevel.INFO,
    )

    # 启动后台线程
    threading.Thread(target=_watchdog, daemon=True).start()
    threading.Thread(target=_bg_summary_thread, daemon=True).start()

    print("✅ 连接飞书 WebSocket 长连接（自动重连）...")
    ws_client.start()  # 阻塞，内部运行 asyncio loop


if __name__ == "__main__":
    main()
