"""
独立重启守护进程：由 bot（main.py）在 /restart 手动重启或定时重启时派生。

【为什么需要这个独立脚本】
bot 在 Windows 上自我重启的真正难点是 stdout 句柄继承：bot 由 `cmd ... >> bot.log`
启动，主进程的 stdout 指向 bot.log；它的子孙进程（一个会继承 stdout 的同名 main.py
子进程、以及 run_claude 起的 claude CLI 子进程）也继承了这个句柄。只要还有任意一个
子孙进程没退出，新实例就无法再 `>> bot.log`（[Errno 13] / 「另一个程序正在使用此文件」），
于是「老进程退了、新进程起不来」=长时间掉线。2026-06-01 多次掉线全是此因。

旧方案（WMI 派生 vbs + 固定 sleep 5s + 老进程 os._exit）的三个致命缺陷：
  1. WMI Create 异步且不校验是否真拉起 → 派生链静默断裂（03:39 掉线 4 小时）
  2. 固定 sleep 5s 不够：claude 子进程可能十几秒才退，甚至「无法终止」
  3. 老进程 os._exit 后，子孙进程变孤儿，taskkill 老 pid 反而追不到它们去杀

【本守护的可靠流程】
  0. 先 sleep 2s，等派生它的 powershell 退出 → 本进程脱离老 bot 进程树（成为孤儿），
     这样下一步 taskkill /T 老进程树绝不会误杀自己
  1. 重试 taskkill 老 bot 整树（含所有子孙），直到老主进程从进程表消失（最多 ~15s）
  2. 轮询等待 bot.log 变为可独占写入（= 所有继承该句柄的子孙都已释放），最多 30s
  3. 才起新的 .venv python main.py（DETACHED + >> bot.log）

由 main.py 的 _trigger_relaunch() 用 `powershell Start-Process` 派生本脚本，并把老
bot 主进程 pid 作为参数传入。
"""
import os
import sys
import time
import subprocess

BOT_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_PY = os.path.join(BOT_DIR, "main.py")
LOG_PATH = os.path.join(BOT_DIR, "bot.log")
VENV_PY = os.path.join(BOT_DIR, ".venv", "Scripts", "python.exe")

CREATE_NO_WINDOW = 0x08000000
DETACHED_PROCESS = 0x00000008
CREATE_NEW_PROCESS_GROUP = 0x00000200


def _log(msg: str) -> None:
    """写一行带 [relaunch] 前缀的日志；bot.log 还被占时静默跳过（等可写后的步骤会补记）。"""
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[relaunch] {msg}\n")
    except Exception:
        pass


def _pid_alive(pid: int) -> bool:
    try:
        r = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True, text=True, timeout=10,
            creationflags=CREATE_NO_WINDOW,
        )
        return str(pid) in (r.stdout or "")
    except Exception:
        return False


def _log_writable() -> bool:
    """能独占追加打开 bot.log = 没有别的进程占着它的 stdout 句柄。"""
    try:
        f = open(LOG_PATH, "a", encoding="utf-8")
        f.close()
        return True
    except Exception:
        return False


def main() -> None:
    old_pid = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 0

    # 0) 等派生本进程的 powershell 退出，使本进程脱离老 bot 进程树（孤儿），
    #    避免下一步 taskkill /T 老树时把自己也端掉。
    time.sleep(2)

    # 1) 杀老 bot 整树（主进程 + 继承 stdout 的子 main.py + claude 子进程），
    #    重试到老主进程彻底消失（应对「暂时无法终止」），最多 ~15s。
    killed = False
    if old_pid:
        for _ in range(15):
            subprocess.run(
                ["taskkill", "/PID", str(old_pid), "/T", "/F"],
                capture_output=True, creationflags=CREATE_NO_WINDOW,
            )
            time.sleep(1)
            if not _pid_alive(old_pid):
                killed = True
                break
    else:
        killed = True  # 没传 pid，跳过杀树（异常兜底）

    # 2) 轮询等 bot.log 可写：所有继承句柄的子孙进程退出后文件才会释放，最多 30s。
    writable = False
    for _ in range(30):
        if _log_writable():
            writable = True
            break
        time.sleep(1)

    # 3) 起新 bot 实例：复用云间原本的「静默重启.vbs」(WScript.Shell.Run 方式)。
    #    为什么不用 DETACHED Popen 直起：实测那样起的 bot，其真正干活的子 python 工作
    #    进程（连飞书 + 监听回调的那个）stdout 不会落到 bot.log，日志全丢、无法监控
    #    （2026-06-01 18:04 验证）。vbs 的 WScript.Shell.Run 起法子进程日志正常。
    #    vbs 内含 5s 延时（这里老进程已被杀，纯空等无害）+ `>> bot.log` 追加写。
    relaunch_vbs = os.path.join(BOT_DIR, "静默重启.vbs")
    _log(
        f"old_pid={old_pid} killed={killed} log_writable={writable} "
        f"→ 用 静默重启.vbs 拉起新实例"
    )
    if os.path.exists(relaunch_vbs):
        subprocess.Popen(
            ["wscript", relaunch_vbs],
            cwd=BOT_DIR,
            creationflags=CREATE_NO_WINDOW,
        )
    else:
        # 兜底：vbs 不在就用 cmd 直起（实际工作子进程日志可能不全，但至少 bot 能活）
        python_exe = VENV_PY if os.path.exists(VENV_PY) else sys.executable
        subprocess.Popen(
            f'cmd /c "{python_exe}" -u "{MAIN_PY}" >> "{LOG_PATH}" 2>&1',
            cwd=BOT_DIR, stdin=subprocess.DEVNULL, close_fds=True,
            creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP,
        )


if __name__ == "__main__":
    main()
