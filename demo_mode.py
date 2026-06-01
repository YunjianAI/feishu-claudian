"""
演示模式播放器 —— 录屏时按既定路线播放预写内容，不调用 Claude。

设计：
  - demo/script.json 定义若干 step（有序播放列表）。
  - 演示模式开启后，每条普通消息播放「下一步」：先逐条吐工具进度行（模拟搜索/抓网页/子任务），
    再把 step 对应的 md 正文逐字流式打进飞书卡片。
  - 带 push 字段的 step，正文播完后调 push-one.js 把预写文章推上飞书云盘，
    解析 PUSH_RESULT 拿到可点开链接，追加到卡片末尾。

不调 Claude、不动 session、确定性可复录。
"""

import asyncio
import json
import os
import time

_MAX_STREAM_DISPLAY = 2500  # 与 main.py 流式卡片显示口径一致：只显示尾部 2500 字


def _demo_dir(bot_dir: str) -> str:
    return os.path.join(bot_dir, "demo")


def load_script(bot_dir: str) -> dict:
    path = os.path.join(_demo_dir(bot_dir), "script.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def step_count(bot_dir: str) -> int:
    try:
        return len(load_script(bot_dir).get("steps", []))
    except Exception:
        return 0


def _read_file(bot_dir: str, name: str) -> str:
    path = os.path.join(_demo_dir(bot_dir), name)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _build_display(tool_history, accumulated) -> str:
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


async def run_push_one(node: str, push_one_js: str, file_path: str, folder: str) -> dict:
    """异步调 push-one.js，解析最后一行 PUSH_RESULT JSON。"""
    cmd = [node, push_one_js, file_path]
    if folder:
        cmd.append(folder)
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    text = out.decode("utf-8", errors="replace")
    result = {"ok": False, "error": "未捕获到 PUSH_RESULT 输出"}
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("PUSH_RESULT "):
            try:
                result = json.loads(line[len("PUSH_RESULT "):])
            except Exception as e:
                result = {"ok": False, "error": f"解析 PUSH_RESULT 失败：{e}"}
    return result


async def play_step(feishu, card_msg_id, bot_dir, idx) -> bool:
    """播放第 idx 步（0-based）。返回 True 表示成功播放。"""
    script = load_script(bot_dir)
    steps = script.get("steps", [])
    if idx < 0 or idx >= len(steps):
        return False
    step = steps[idx]

    tool_history: list[str] = []
    accumulated = ""

    async def push():
        try:
            await feishu.update_card(card_msg_id, _build_display(tool_history, accumulated))
        except Exception:
            pass

    # 1. 逐条播放工具进度行
    for tool in step.get("tools", []):
        line = tool.get("line", "")
        delay = float(tool.get("delay", 0.8))
        if tool.get("replace") and tool_history:
            tool_history[-1] = line
        else:
            tool_history.append(line)
        await push()
        await asyncio.sleep(delay)

    # 2. 流式播放正文 md
    body = ""
    fname = step.get("file")
    if fname:
        try:
            body = _read_file(bot_dir, fname)
        except Exception as e:
            body = f"（读取 {fname} 失败：{e}）"

    chunk_size = int(step.get("chunk", 6))
    interval = float(step.get("interval", 0.05))
    i = 0
    last = 0.0
    while i < len(body):
        accumulated += body[i:i + chunk_size]
        i += chunk_size
        now = time.time()
        if now - last >= 0.4:
            await push()
            last = now
        await asyncio.sleep(interval)
    await push()  # 收尾全量刷新

    # 3. 推送飞书（仅带 push 字段的 step）
    push_file = step.get("push")
    if push_file:
        node = script.get("node", "node")
        push_one_js = script.get("push_one_js", "")
        folder = script.get("push_folder", "")
        article_path = push_file
        if not os.path.isabs(article_path):
            article_path = os.path.join(_demo_dir(bot_dir), article_path)

        tool_history.append("📤 **上传到飞书文档...**")
        await push()

        result = await run_push_one(node, push_one_js, article_path, folder)
        if result.get("ok"):
            url = result.get("url", "")
            name = result.get("name", "")
            accumulated += (
                f"\n\n---\n\n✅ 成稿已上传到飞书文档：**{name}**\n\n"
                f"点开直接看 👉 {url}"
            )
        else:
            accumulated += f"\n\n---\n\n❌ 上传失败：{result.get('error', '未知错误')}"
        await push()

    return True
