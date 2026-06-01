' Silent restart helper for the Feishu bot.
' Waits a few seconds so the old process exits first, then relaunches the bot
' hidden (no console window). Output -> bot.log. Spawned by /restart from chat.
Dim botDir
botDir = "D:\feishu-claude-code"

' --- log rotation: if bot.log > 5MB, move it to bot.log.old (keep one history) ---
Dim fso, logPath, oldPath
Set fso = CreateObject("Scripting.FileSystemObject")
logPath = botDir & "\bot.log"
oldPath = botDir & "\bot.log.old"
If fso.FileExists(logPath) Then
    If fso.GetFile(logPath).Size > 5242880 Then
        If fso.FileExists(oldPath) Then fso.DeleteFile oldPath, True
        fso.MoveFile logPath, oldPath
    End If
End If

Set sh = CreateObject("WScript.Shell")
sh.CurrentDirectory = botDir
WScript.Sleep 5000
sh.Run "cmd /c set ""PYTHONUTF8=1"" && set ""PYTHONIOENCODING=utf-8"" && "".venv\Scripts\python.exe"" -u main.py >> bot.log 2>&1", 0, False
