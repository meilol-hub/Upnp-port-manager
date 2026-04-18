#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UPnP Port Manager  v2.1.0
GitHub: https://github.com/meilol-hub/Upnp-port-manager
"""

# ════════════════════════════════════════════════════════════════════
#  Standard library
# ════════════════════════════════════════════════════════════════════
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import threading, json, os, sys, queue, math, time
import shutil, subprocess, tempfile, warnings
from pathlib import Path
from datetime import datetime
from urllib import request as urlreq
from urllib.error import URLError
from packaging.version import Version          # pip install packaging

warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

# ════════════════════════════════════════════════════════════════════
#  Optional deps
# ════════════════════════════════════════════════════════════════════
try:
    import winreg;      IS_WINDOWS = True
except ImportError:     IS_WINDOWS = False

try:
    import pystray
    from pystray import MenuItem as Item, Menu as TrayMenu
    HAS_PYSTRAY = True
except ImportError:     HAS_PYSTRAY = False

try:
    from PIL import Image, ImageDraw, ImageTk
    HAS_PIL = True
except ImportError:     HAS_PIL = False

try:
    import miniupnpc;   HAS_UPNPC = True
except ImportError:     HAS_UPNPC = False

try:
    from win10toast import ToastNotifier
    _toaster   = ToastNotifier()
    HAS_TOAST  = True
except Exception:       HAS_TOAST = False

try:
    from packaging.version import Version
    HAS_PKG = True
except ImportError:     HAS_PKG = False

# ════════════════════════════════════════════════════════════════════
#  App meta
# ════════════════════════════════════════════════════════════════════
APP_NAME    = "Upnp port manager"
APP_VER     = "1.1.0"
APP_DESC    = "Universal Plug & Play  ·  Gateway Controller"
GITHUB_RAW  = "https://raw.githubusercontent.com/meilol-hub/Upnp-port-manager/main"
VERSION_URL = f"{GITHUB_RAW}/app/version.json"

# ════════════════════════════════════════════════════════════════════
#  Paths
# ════════════════════════════════════════════════════════════════════
APP_DIR     = Path(__file__).parent
CONFIG_DIR  = Path.home() / ".upnp_manager"
CONFIG_FILE = CONFIG_DIR / "config.json"

DEFAULT_CFG = {
    "icon_path":       "",
    "bg_image":        "",
    "ports":           [],
    "autostart":       False,
    "start_minimized": False,
    "window_geometry": "760x700+120+80",
    "log_visible":     True,
    "check_updates":   True,
}

def load_cfg():
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    if CONFIG_FILE.exists():
        try:
            return {**DEFAULT_CFG,
                    **json.loads(CONFIG_FILE.read_text(encoding="utf-8"))}
        except Exception: pass
    return DEFAULT_CFG.copy()

def save_cfg(c):
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(
        json.dumps(c, ensure_ascii=False, indent=2), encoding="utf-8")

# ════════════════════════════════════════════════════════════════════
#  Palette
# ════════════════════════════════════════════════════════════════════
C = {
    "bg":      "#080814", "bg2":     "#0d0d22",
    "card":    "#111128", "card2":   "#161638",
    "border":  "#222250", "border2": "#2e2e72",
    "accent":  "#5c4fff", "accent2": "#8b7fff",
    "cyan":    "#00d4ff", "cyan2":   "#80e8ff",
    "green":   "#00f5a0", "green2":  "#80ffcc",
    "red":     "#ff4466", "red2":    "#ff8899",
    "yellow":  "#ffd166", "orange":  "#ff9a3c",
    "text":    "#dde4f0", "text2":   "#7a85a0",
    "text3":   "#3d4460",
}
FM = "Consolas";  FU = "Segoe UI"

# ════════════════════════════════════════════════════════════════════
#  UPnP Engine
# ════════════════════════════════════════════════════════════════════
class UPnPEngine:
    def __init__(self):
        self.upnp = None; self.local_ip = "—"; self.extern_ip = "—"
        self._lock = threading.Lock()

    def discover(self, retries=3):
        if not HAS_UPNPC:
            return False, "miniupnpc がインストールされていません"
        # 無線LAN はルーターの応答が遅いため delay を長めにして複数回試みる
        delays = [1000, 2000, 3000]   # ms  (有線300ms, 無線は1〜3秒必要な場合あり)
        last_err = "UPnP デバイスが見つかりません"
        for i in range(retries):
            try:
                u = miniupnpc.UPnP()
                u.discoverdelay = delays[min(i, len(delays)-1)]
                found = u.discover()
                if found == 0:
                    last_err = "UPnP デバイスが見つかりません"
                    continue
                u.selectigd()
                self.upnp      = u
                self.local_ip  = u.lanaddr
                self.extern_ip = u.externalipaddress()
                return True, "OK"
            except Exception as e:
                last_err = str(e)
        return False, f"{last_err}（{retries}回試行）"

    def add(self, port, proto, desc=APP_NAME):
        if not self.upnp:
            ok, msg = self.discover()
            if not ok: return False, msg
        try:
            with self._lock:
                r = self.upnp.addportmapping(
                    port, proto.upper(), self.upnp.lanaddr, port, desc, "")
            return (True, f"{port}/{proto} → opened") if r \
                   else (False, "addportmapping returned False")
        except Exception as e: return False, str(e)

    def remove(self, port, proto):
        if not self.upnp: return False, "未接続"
        try:
            with self._lock: self.upnp.deleteportmapping(port, proto.upper())
            return True, f"{port}/{proto} → closed"
        except Exception as e: return False, str(e)

    def remove_all(self, ports):
        if not self.upnp:
            ok, msg = self.discover()
            if not ok: return [], [f"UPnP: {msg}"]
        ok_list, err_list = [], []
        for entry in ports:
            ok, msg = self.remove(entry["port"], entry["proto"])
            (ok_list if ok else err_list).append(msg)
        return ok_list, err_list

upnp = UPnPEngine()

# ════════════════════════════════════════════════════════════════════
#  Update checker
# ════════════════════════════════════════════════════════════════════
class UpdateChecker:
    TIMEOUT = 6

    @staticmethod
    def fetch_remote_version():
        """Returns (version_str, release_notes, download_url) or raises."""
        with urlreq.urlopen(VERSION_URL, timeout=UpdateChecker.TIMEOUT) as r:
            data = json.loads(r.read().decode())
        return data["version"], data.get("release_notes",""), \
               data.get("download_url",""), data.get("files",[])

    @staticmethod
    def is_newer(remote: str, local: str = APP_VER) -> bool:
        try:
            return Version(remote) > Version(local)
        except Exception:
            return remote != local

    @staticmethod
    def download_update(files: list, dest: Path,
                        progress_cb=None):
        """Download each file to dest folder."""
        for i, f in enumerate(files):
            url  = f["url"]
            name = f["name"]
            if progress_cb: progress_cb(i, len(files), name)
            with urlreq.urlopen(url, timeout=30) as r:
                (dest / name).write_bytes(r.read())

# ════════════════════════════════════════════════════════════════════
#  Auto-start / Toast
# ════════════════════════════════════════════════════════════════════
def set_autostart(enabled):
    if not IS_WINDOWS: return
    key_path = r"Software\Microsoft\Windows\CurrentVersion\Run"
    exe = sys.executable.replace("python.exe", "pythonw.exe")
    script = str(APP_DIR / "upnp_manager.py")
    try:
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER,
                             key_path, 0, winreg.KEY_SET_VALUE)
        if enabled:
            winreg.SetValueEx(key, APP_NAME, 0, winreg.REG_SZ,
                              f'"{exe}" "{script}"')
        else:
            try: winreg.DeleteValue(key, APP_NAME)
            except FileNotFoundError: pass
        winreg.CloseKey(key)
    except Exception as e: print(f"[autostart] {e}")

def show_toast(title, msg, duration=3):
    if not HAS_TOAST: return
    def _do():
        try: _toaster.show_toast(title, msg, duration=duration, threaded=False)
        except Exception: pass
    threading.Thread(target=_do, daemon=True).start()

# ════════════════════════════════════════════════════════════════════
#  Icon helpers
# ════════════════════════════════════════════════════════════════════
def _make_default_icon(size=64):
    img = Image.new("RGBA", (size, size), (0,0,0,0))
    d = ImageDraw.Draw(img)
    r = size//2-2; cx = cy = size//2
    d.ellipse([cx-r,cy-r,cx+r,cy+r], fill="#5c4fff")
    aw = size//5; ah = size//3
    d.polygon([(cx,cy-ah),(cx+aw,cy),(cx+aw//2,cy),
               (cx+aw//2,cy+ah),(cx-aw//2,cy+ah),
               (cx-aw//2,cy),(cx-aw,cy)], fill="white")
    return img

def load_icon(path, size):
    if HAS_PIL:
        if path and os.path.isfile(path):
            try: return Image.open(path).resize((size,size), Image.LANCZOS)
            except Exception: pass
        return _make_default_icon(size)
    return None

def _ensure_icon_file():
    if not HAS_PIL: return
    ico_path = APP_DIR / "icon.ico"
    if ico_path.exists(): return
    try:
        imgs = [_make_default_icon(s) for s in (256,128,64,48,32,16)]
        imgs[0].save(str(ico_path), format="ICO",
                     sizes=[(i.width,i.height) for i in imgs],
                     append_images=imgs[1:])
    except Exception: pass

# ════════════════════════════════════════════════════════════════════
#  Widget helpers
# ════════════════════════════════════════════════════════════════════
def make_btn(parent, text, cmd, accent=False, danger=False,
             warn=False, small=False):
    bg  = C["accent"] if accent else (C["red"] if danger else
          C["orange"] if warn else C["card2"])
    abg = C["accent2"] if accent else (C["red2"] if danger else
          "#ffb347" if warn else C["border2"])
    return tk.Button(parent, text=text, command=cmd,
                     bg=bg, fg="white",
                     activebackground=abg, activeforeground="white",
                     relief="flat", cursor="hand2",
                     font=(FM, 8 if small else 9,
                           "bold" if accent else "normal"),
                     padx=8 if small else 12, pady=3 if small else 6)

def make_entry(parent, var):
    return tk.Entry(parent, textvariable=var,
                    bg=C["card2"], fg=C["text"],
                    insertbackground=C["cyan"], relief="flat",
                    font=(FM,10), highlightthickness=1,
                    highlightbackground=C["border2"],
                    highlightcolor=C["cyan"])

def hsep(parent, color=None):
    return tk.Frame(parent, bg=color or C["border"], height=1)

def vsep(parent):
    return tk.Frame(parent, bg=C["border2"], width=1)

# ════════════════════════════════════════════════════════════════════
#  Tooltip
# ════════════════════════════════════════════════════════════════════
class Tooltip:
    def __init__(self, widget, text, delay=500):
        self.widget=widget; self.text=text; self.delay=delay
        self._id=None; self._win=None
        widget.bind("<Enter>",       self._sched)
        widget.bind("<Leave>",       self._cancel)
        widget.bind("<ButtonPress>", self._cancel)

    def _sched(self, e=None):
        self._cancel()
        self._id = self.widget.after(self.delay, self._show)

    def _cancel(self, e=None):
        if self._id: self.widget.after_cancel(self._id); self._id=None
        if self._win: self._win.destroy(); self._win=None

    def _show(self):
        if self._win: return
        x = self.widget.winfo_rootx()+10
        y = self.widget.winfo_rooty()+self.widget.winfo_height()+4
        self._win = tk.Toplevel(self.widget)
        self._win.overrideredirect(True)
        self._win.geometry(f"+{x}+{y}")
        self._win.configure(bg=C["border2"])
        inner = tk.Frame(self._win, bg=C["card2"], padx=9, pady=5)
        inner.pack(padx=1, pady=1)
        tk.Label(inner, text=self.text, bg=C["card2"], fg=C["text"],
                 font=(FU,8), justify="left", wraplength=300).pack()

# ════════════════════════════════════════════════════════════════════
#  Splash
# ════════════════════════════════════════════════════════════════════
class SplashScreen(tk.Toplevel):
    W, H = 440, 240

    def __init__(self, parent):
        super().__init__(parent)
        self.overrideredirect(True); self.configure(bg=C["bg2"])
        sw=self.winfo_screenwidth(); sh=self.winfo_screenheight()
        self.geometry(f"{self.W}x{self.H}+"
                      f"{(sw-self.W)//2}+{(sh-self.H)//2}")
        self._prog=0.0; self._phase=0
        self._msgs=["Initializing…","Loading config…",
                    "Discovering UPnP…","Ready."]
        self._build(); self.lift(); self.focus_force(); self._tick()

    def _build(self):
        border=tk.Frame(self,bg=C["accent"],padx=1,pady=1)
        border.pack(fill="both",expand=True)
        body=tk.Frame(border,bg=C["bg2"])
        body.pack(fill="both",expand=True)
        top=tk.Frame(body,bg=C["bg2"]); top.pack(pady=(28,6))
        if HAS_PIL:
            img=load_icon("",50); self._ico=ImageTk.PhotoImage(img)
            tk.Label(top,image=self._ico,bg=C["bg2"]).pack(side="left",padx=(0,14))
        tf=tk.Frame(top,bg=C["bg2"]); tf.pack(side="left")
        tk.Label(tf,text=APP_NAME,bg=C["bg2"],fg=C["text"],
                 font=(FM,15,"bold")).pack(anchor="w")
        tk.Label(tf,text=f"v{APP_VER}  ·  {APP_DESC}",
                 bg=C["bg2"],fg=C["text3"],font=(FM,8)).pack(anchor="w",pady=(2,0))
        hsep(body,C["border"]).pack(fill="x",padx=24,pady=(16,10))
        self._sv=tk.StringVar(master=self,value=self._msgs[0])
        tk.Label(body,textvariable=self._sv,bg=C["bg2"],fg=C["text2"],
                 font=(FM,8)).pack()
        track=tk.Frame(body,bg=C["border"],height=3)
        track.pack(fill="x",padx=24,pady=(8,0)); track.pack_propagate(False)
        self._bar=tk.Frame(track,bg=C["accent"],height=3)
        self._bar.place(x=0,y=0,relheight=1,relwidth=0)

    def _tick(self):
        self._prog=min(1.0,self._prog+0.018)
        self._bar.place_configure(relwidth=self._prog)
        phase=min(3,int(self._prog*4))
        if phase!=self._phase:
            self._phase=phase; self._sv.set(self._msgs[phase])
        if self._prog<1.0: self.after(20,self._tick)
        else: self.after(300,self._close)

    def _close(self):
        try: self.destroy()
        except Exception: pass

# ════════════════════════════════════════════════════════════════════
#  Update Dialog
# ════════════════════════════════════════════════════════════════════
class UpdateDialog(tk.Toplevel):
    def __init__(self, parent, remote_ver, notes, dl_url, files):
        super().__init__(parent)
        self.configure(bg=C["bg2"]); self.title("アップデート")
        self.geometry("480x360"); self.resizable(False,False)
        self.grab_set(); self.transient(parent)
        self._files=files; self._dl_url=dl_url
        self._remote=remote_ver; self._parent_app=parent
        px=parent.winfo_rootx()+parent.winfo_width()//2-240
        py=parent.winfo_rooty()+parent.winfo_height()//2-180
        self.geometry(f"+{max(0,px)}+{max(0,py)}")
        self.bind("<Escape>", lambda e: self.destroy())
        self._build(remote_ver, notes, dl_url)

    def _build(self, ver, notes, dl_url):
        tk.Frame(self,bg=C["green"],height=3).pack(fill="x")
        hdr=tk.Frame(self,bg=C["card"],height=48); hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr,text="⬆  NEW VERSION AVAILABLE",
                 bg=C["card"],fg=C["green"],
                 font=(FM,11,"bold")).pack(side="left",padx=16,pady=12)

        body=tk.Frame(self,bg=C["bg2"]); body.pack(fill="both",expand=True,padx=22,pady=12)

        # Version comparison
        vf=tk.Frame(body,bg=C["card2"],highlightthickness=1,
                    highlightbackground=C["border2"])
        vf.pack(fill="x",pady=(0,12))
        vf_in=tk.Frame(vf,bg=C["card2"]); vf_in.pack(padx=14,pady=10)
        tk.Label(vf_in,text=f"現在:  v{APP_VER}",bg=C["card2"],fg=C["text2"],
                 font=(FM,9)).pack(anchor="w")
        tk.Label(vf_in,text=f"最新:  v{ver}",bg=C["card2"],fg=C["green"],
                 font=(FM,9,"bold")).pack(anchor="w")

        # Release notes
        tk.Label(body,text="リリースノート",bg=C["bg2"],fg=C["text3"],
                 font=(FM,7,"bold")).pack(anchor="w")
        hsep(body,C["border2"]).pack(fill="x",pady=(2,6))
        nt=tk.Text(body,bg=C["card"],fg=C["text2"],font=(FM,8),
                   relief="flat",height=4,wrap="word",
                   highlightthickness=0,state="normal")
        nt.insert("1.0", notes or "詳細は GitHub をご確認ください。")
        nt.configure(state="disabled")
        nt.pack(fill="x")

        # Progress
        self._prog_v=tk.StringVar(master=self,value="")
        self._prog_lbl=tk.Label(body,textvariable=self._prog_v,
                                 bg=C["bg2"],fg=C["yellow"],font=(FM,8),anchor="w")
        self._prog_lbl.pack(fill="x",pady=(8,0))
        track=tk.Frame(body,bg=C["border"],height=3)
        track.pack(fill="x",pady=(4,0)); track.pack_propagate(False)
        self._bar=tk.Frame(track,bg=C["green"],height=3)
        self._bar.place(x=0,y=0,relheight=1,relwidth=0)

        hsep(self).pack(fill="x",pady=(8,0))
        foot=tk.Frame(self,bg=C["bg2"]); foot.pack(fill="x",padx=22,pady=10)
        make_btn(foot,"後で",self.destroy).pack(side="left")
        self._upd_btn=make_btn(foot,"今すぐ更新  ⬆",
                                self._do_update,accent=True)
        self._upd_btn.pack(side="right")

    def _do_update(self):
        self._upd_btn.configure(state="disabled",text="ダウンロード中…")
        def run():
            dest=APP_DIR
            def prog(i,total,name):
                self._prog_v.set(f"ダウンロード中: {name}  ({i+1}/{total})")
                self._bar.place_configure(relwidth=(i+1)/total)
                self.update_idletasks()
            try:
                UpdateChecker.download_update(self._files, dest, prog)
                self._prog_v.set("✓  更新完了。再起動してください。")
                self._bar.place_configure(relwidth=1.0)
                self._prog_lbl.configure(fg=C["green"])
                self.after(0, lambda: messagebox.showinfo(
                    "更新完了",
                    "ファイルを更新しました。\nアプリを再起動してください。",
                    parent=self))
                self.after(0, self.destroy)
            except Exception as e:
                self._prog_v.set(f"✗  エラー: {e}")
                self._prog_lbl.configure(fg=C["red"])
                self._upd_btn.configure(state="normal",text="今すぐ更新  ⬆")
        threading.Thread(target=run,daemon=True).start()

# ════════════════════════════════════════════════════════════════════
#  Bulk Release Dialog
# ════════════════════════════════════════════════════════════════════
class BulkReleaseDialog(tk.Toplevel):
    def __init__(self, parent, ports, on_done):
        super().__init__(parent)
        self.configure(bg=C["bg2"]); self.title("一括開放 / 解放")
        self.geometry("520x480"); self.resizable(False,False)
        self.grab_set(); self.transient(parent)
        self._ports=ports; self._on_done=on_done; self._running=False
        px=parent.winfo_rootx()+parent.winfo_width()//2-260
        py=parent.winfo_rooty()+parent.winfo_height()//2-240
        self.geometry(f"+{max(0,px)}+{max(0,py)}")
        self.bind("<Escape>", lambda e: (None if self._running else self.destroy()))
        self._build()

    def _build(self):
        tk.Frame(self,bg=C["orange"],height=3).pack(fill="x")
        hdr=tk.Frame(self,bg=C["card"],height=48); hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr,text="⚡  一括開放 / 解放",
                 bg=C["card"],fg=C["orange"],
                 font=(FM,11,"bold")).pack(side="left",padx=16,pady=12)

        body=tk.Frame(self,bg=C["bg2"]); body.pack(fill="both",expand=True,padx=18,pady=10)

        # Mode selector
        tk.Label(body,text="操作モード",bg=C["bg2"],fg=C["text3"],
                 font=(FM,7,"bold")).pack(anchor="w")
        hsep(body,C["border2"]).pack(fill="x",pady=(2,8))

        self._mode=tk.StringVar(master=self,value="open")
        mf=tk.Frame(body,bg=C["bg2"]); mf.pack(fill="x",pady=(0,10))
        self._mbts={}
        for val,lbl,tip in [
            ("open",  "⬆ 全ポートを開放",   "登録済みの全ポートをルーターに開放する"),
            ("close", "⬇ 全ポートを閉じる", "登録済みの全ポートをルーターから削除する（リストには残る）"),
        ]:
            b=tk.Button(mf,text=lbl,
                        command=lambda v=val: self._sel_mode(v),
                        relief="flat",cursor="hand2",
                        font=(FM,9),padx=14,pady=7)
            b.pack(side="left",padx=(0,6))
            self._mbts[val]=b
            Tooltip(b,tip)
        self._sel_mode("open")

        # Port list with checkboxes
        tk.Label(body,text="対象ポート",bg=C["bg2"],fg=C["text3"],
                 font=(FM,7,"bold")).pack(anchor="w")
        hsep(body,C["border2"]).pack(fill="x",pady=(2,6))

        lf=tk.Frame(body,bg=C["card2"]); lf.pack(fill="both",expand=True)
        self._checks={}
        for entry in self._ports:
            key=(entry["port"],entry["proto"])
            var=tk.BooleanVar(master=self,value=True)
            row=tk.Frame(lf,bg=C["card2"]); row.pack(fill="x",padx=8,pady=3)
            proto_col={"TCP":C["accent"],"UDP":C["yellow"],"BOTH":C["green"]}.get(entry["proto"],C["text2"])
            tk.Checkbutton(row,variable=var,bg=C["card2"],
                           selectcolor=C["card"],
                           activebackground=C["card2"],
                           cursor="hand2").pack(side="left")
            tk.Label(row,text=f"{entry['port']:>5}",fg=C["cyan"],
                     bg=C["card2"],font=(FM,11,"bold")).pack(side="left",padx=(0,6))
            tk.Label(row,text=entry["proto"],bg=proto_col,fg="white",
                     font=(FM,8,"bold"),padx=5,pady=1).pack(side="left",padx=(0,8))
            tk.Label(row,text=entry.get("desc",""),fg=C["text2"],
                     bg=C["card2"],font=(FM,9)).pack(side="left")
            self._checks[key]=var

        # Sel all / none
        sf=tk.Frame(body,bg=C["bg2"]); sf.pack(fill="x",pady=(6,0))
        tk.Button(sf,text="全選択",command=lambda:[v.set(True) for v in self._checks.values()],
                  bg=C["card2"],fg=C["text2"],relief="flat",cursor="hand2",
                  font=(FM,8),padx=8,pady=3).pack(side="left",padx=(0,4))
        tk.Button(sf,text="全解除",command=lambda:[v.set(False) for v in self._checks.values()],
                  bg=C["card2"],fg=C["text2"],relief="flat",cursor="hand2",
                  font=(FM,8),padx=8,pady=3).pack(side="left")

        # Progress
        self._prog_v=tk.StringVar(master=self,value="")
        self._prog_lbl=tk.Label(body,textvariable=self._prog_v,
                                 bg=C["bg2"],fg=C["yellow"],font=(FM,8),anchor="w")
        self._prog_lbl.pack(fill="x",pady=(8,0))
        track=tk.Frame(body,bg=C["border"],height=3)
        track.pack(fill="x",pady=(4,0)); track.pack_propagate(False)
        self._bar=tk.Frame(track,bg=C["orange"],height=3)
        self._bar.place(x=0,y=0,relheight=1,relwidth=0)

        hsep(self).pack(fill="x",pady=(8,0))
        foot=tk.Frame(self,bg=C["bg2"]); foot.pack(fill="x",padx=18,pady=10)
        make_btn(foot,"キャンセル",self.destroy).pack(side="left")
        self._exec_btn=make_btn(foot,"実行  ▶",self._execute,accent=True)
        self._exec_btn.pack(side="right")

    def _sel_mode(self,mode):
        self._mode.set(mode)
        for k,b in self._mbts.items():
            b.configure(bg=C["accent"] if k==mode else C["card2"],fg="white")

    def _execute(self):
        if self._running: return
        selected=[e for e in self._ports
                  if self._checks.get((e["port"],e["proto"]),
                                      tk.BooleanVar(value=False)).get()]
        if not selected:
            messagebox.showinfo("Info","対象ポートを選択してください。",parent=self)
            return
        self._running=True
        self._exec_btn.configure(state="disabled",text="実行中…")
        mode=self._mode.get()

        def run():
            total=len(selected); results=[]
            ok2,_=upnp.discover()
            if ok2:
                self.after(0,lambda: None)
            for i,entry in enumerate(selected):
                if mode=="open":
                    ok,msg=upnp.add(entry["port"],entry["proto"],
                                    entry.get("desc",""))
                    if ok: entry["status"]="ok"
                    else:  entry["status"]="error"
                else:
                    ok,msg=upnp.remove(entry["port"],entry["proto"])
                    if ok: entry["status"]="idle"
                results.append((entry["port"],entry["proto"],ok,msg))
                pct=(i+1)/total
                label=f"{'開放' if mode=='open' else '解放'}中: {entry['port']}/{entry['proto']}  {msg}"
                self.after(0,lambda l=label,p=pct: (
                    self._prog_v.set(l),
                    self._bar.place_configure(relwidth=p),
                    self.update_idletasks()
                ))
            n_ok=sum(1 for *_,o,_ in results if o)
            final=f"✓  {n_ok}/{total} 件 完了"
            col=C["green"] if n_ok==total else C["yellow"]
            self.after(0,lambda: (
                self._prog_v.set(final),
                self._prog_lbl.configure(fg=col),
                self._bar.place_configure(relwidth=1.0)
            ))
            self.after(0,lambda: self._on_done(results,mode))
            self.after(1200, self.destroy)

        threading.Thread(target=run,daemon=True).start()

# ════════════════════════════════════════════════════════════════════
#  Add Port Dialog
# ════════════════════════════════════════════════════════════════════
class AddPortDialog(tk.Toplevel):
    def __init__(self, parent, callback):
        super().__init__(parent)
        self.callback=callback
        self.configure(bg=C["bg2"]); self.title("Add Port Mapping")
        self.geometry("430x370"); self.resizable(False,False)
        self.grab_set(); self.transient(parent)
        px=parent.winfo_rootx()+parent.winfo_width()//2-215
        py=parent.winfo_rooty()+parent.winfo_height()//2-185
        self.geometry(f"+{max(0,px)}+{max(0,py)}")
        self.bind("<Return>",lambda e:self._submit())
        self.bind("<Escape>",lambda e:self.destroy())
        self._build()

    def _build(self):
        tk.Frame(self,bg=C["cyan"],height=3).pack(fill="x")
        hdr=tk.Frame(self,bg=C["card"],height=48); hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr,text="⊕  ADD PORT MAPPING",bg=C["card"],fg=C["cyan"],
                 font=(FM,11,"bold")).pack(side="left",padx=16,pady=12)
        body=tk.Frame(self,bg=C["bg2"]); body.pack(fill="both",expand=True,padx=20,pady=10)

        def lbl(t):
            tk.Label(body,text=t,bg=C["bg2"],fg=C["text3"],
                     font=(FM,7,"bold")).pack(anchor="w",pady=(10,2))

        lbl("PORT NUMBER  (1 – 65535)")
        self.port_v=tk.StringVar(master=self)
        pe=make_entry(body,self.port_v); pe.pack(fill="x",ipady=6); pe.focus_set()
        Tooltip(pe,"例: 25565 (Minecraft)  27015 (Steam)  8080 (HTTP)")

        lbl("PROTOCOL")
        self.proto_v=tk.StringVar(master=self,value="TCP")
        pf=tk.Frame(body,bg=C["bg2"]); pf.pack(fill="x",pady=4)
        self._pbts={}
        for p,tip in [("TCP","信頼性重視 (HTTP/FTP)"),
                      ("UDP","速度重視 (ゲーム/配信)"),
                      ("BOTH","TCP + UDP 両方")]:
            b=tk.Button(pf,text=p,command=lambda x=p:self._sel_proto(x),
                        relief="flat",cursor="hand2",
                        font=(FM,9,"bold"),padx=14,pady=5)
            b.pack(side="left",padx=(0,4))
            self._pbts[p]=b; Tooltip(b,tip)
        self._sel_proto("TCP")

        lbl("DESCRIPTION")
        self.desc_v=tk.StringVar(master=self,value=APP_NAME)
        make_entry(body,self.desc_v).pack(fill="x",ipady=6)

        hsep(self).pack(fill="x",pady=(14,0))
        foot=tk.Frame(self,bg=C["bg2"]); foot.pack(fill="x",padx=20,pady=10)
        make_btn(foot,"CANCEL",self.destroy).pack(side="left")
        make_btn(foot,"ADD  ▶",self._submit,accent=True).pack(side="right")

    def _sel_proto(self,p):
        self.proto_v.set(p)
        for k,b in self._pbts.items():
            b.configure(bg=C["accent"] if k==p else C["card2"],fg="white")

    def _submit(self):
        try:
            port=int(self.port_v.get())
            if not (1<=port<=65535): raise ValueError()
        except ValueError:
            messagebox.showerror("Error","ポート番号は 1〜65535 で入力してください。",parent=self)
            return
        self.callback(port,self.proto_v.get(),self.desc_v.get() or APP_NAME)
        self.destroy()

# ════════════════════════════════════════════════════════════════════
#  Settings Dialog
# ════════════════════════════════════════════════════════════════════
class SettingsDialog(tk.Toplevel):
    def __init__(self, parent, cfg, callback):
        super().__init__(parent)
        self.cfg=cfg.copy(); self.callback=callback
        self.configure(bg=C["bg2"]); self.title("Settings")
        self.geometry("490x530"); self.resizable(False,False)
        self.grab_set(); self.transient(parent)
        px=parent.winfo_rootx()+parent.winfo_width()//2-245
        py=parent.winfo_rooty()+parent.winfo_height()//2-265
        self.geometry(f"+{max(0,px)}+{max(0,py)}")
        self.bind("<Escape>",lambda e:self.destroy())
        self._build()

    def _build(self):
        tk.Frame(self,bg=C["accent2"],height=3).pack(fill="x")
        hdr=tk.Frame(self,bg=C["card"],height=48); hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr,text="⚙  SETTINGS",bg=C["card"],fg=C["accent2"],
                 font=(FM,11,"bold")).pack(side="left",padx=16,pady=12)

        body=tk.Frame(self,bg=C["bg2"]); body.pack(fill="both",expand=True,padx=22,pady=6)

        def section(t):
            tk.Frame(body,bg=C["bg2"],height=8).pack()
            tk.Label(body,text=t,bg=C["bg2"],fg=C["text3"],
                     font=(FM,7,"bold")).pack(anchor="w")
            hsep(body,C["border2"]).pack(fill="x",pady=(2,8))

        def file_row(lbl,var,ftypes,tip=""):
            tk.Label(body,text=lbl,bg=C["bg2"],fg=C["text2"],
                     font=(FM,8)).pack(anchor="w",pady=(0,2))
            rf=tk.Frame(body,bg=C["bg2"]); rf.pack(fill="x",pady=(0,6))
            e=make_entry(rf,var); e.pack(side="left",fill="x",expand=True,ipady=5)
            if tip: Tooltip(e,tip)
            bb=make_btn(rf,"…",lambda v=var,ft=ftypes:
                v.set(p) if (p:=filedialog.askopenfilename(filetypes=ft,parent=self)) else None,
                small=True)
            bb.pack(side="left",padx=(4,2))
            xb=make_btn(rf,"✕",lambda v=var:v.set(""),small=True)
            xb.pack(side="left")

        def chk(var,text,tip=""):
            f=tk.Frame(body,bg=C["bg2"]); f.pack(anchor="w",pady=3)
            cb=tk.Checkbutton(f,text=text,variable=var,
                              bg=C["bg2"],fg=C["text"],
                              selectcolor=C["card2"],
                              activebackground=C["bg2"],
                              activeforeground=C["text"],
                              font=(FM,9),cursor="hand2")
            cb.pack(side="left")
            if tip: Tooltip(cb,tip)

        section("APPEARANCE")
        self.icon_v=tk.StringVar(master=self,value=self.cfg.get("icon_path",""))
        file_row("カスタムアイコン (PNG/ICO/JPG)",self.icon_v,
                 [("Image","*.png *.ico *.jpg *.jpeg *.bmp")],
                 "トレイ・ウィンドウアイコン")
        self.bg_v=tk.StringVar(master=self,value=self.cfg.get("bg_image",""))
        file_row("背景画像 (PNG/JPG/BMP/GIF)",self.bg_v,
                 [("Image","*.png *.jpg *.jpeg *.bmp *.gif *.webp")],
                 "ウィンドウ背景（自動で暗くなります）")

        section("STARTUP")
        self.auto_v=tk.BooleanVar(master=self,value=self.cfg.get("autostart",False))
        self.mini_v=tk.BooleanVar(master=self,value=self.cfg.get("start_minimized",False))
        chk(self.auto_v,"PC 起動時に自動起動",
            "Windows レジストリに登録 (HKCU→Run)")
        chk(self.mini_v,"起動時にトレイへ最小化")

        section("UPDATE")
        self.upd_v=tk.BooleanVar(master=self,value=self.cfg.get("check_updates",True))
        chk(self.upd_v,"起動時に自動で更新を確認",
            f"GitHub ({VERSION_URL}) から最新バージョンを確認します")
        upd_btn=make_btn(body,"今すぐ確認  ↺",
                          lambda:self.winfo_toplevel()._check_update_manual()
                          if hasattr(self.winfo_toplevel(),"_check_update_manual")
                          else None)
        upd_btn.pack(anchor="w")
        Tooltip(upd_btn,"手動でアップデートを確認")

        hsep(self).pack(fill="x",pady=(12,0))
        foot=tk.Frame(self,bg=C["bg2"]); foot.pack(fill="x",padx=22,pady=12)
        make_btn(foot,"CANCEL",self.destroy).pack(side="left")
        make_btn(foot,"SAVE  ✓",self._save,accent=True).pack(side="right")

    def _save(self):
        self.cfg.update({
            "icon_path":self.icon_v.get(),"bg_image":self.bg_v.get(),
            "autostart":self.auto_v.get(),"start_minimized":self.mini_v.get(),
            "check_updates":self.upd_v.get(),
        })
        self.callback(self.cfg); self.destroy()

# ════════════════════════════════════════════════════════════════════
#  About Dialog
# ════════════════════════════════════════════════════════════════════
class AboutDialog(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.configure(bg=C["bg2"]); self.title(f"About {APP_NAME}")
        self.geometry("400x360"); self.resizable(False,False)
        self.grab_set(); self.transient(parent)
        px=parent.winfo_rootx()+parent.winfo_width()//2-200
        py=parent.winfo_rooty()+parent.winfo_height()//2-180
        self.geometry(f"+{max(0,px)}+{max(0,py)}")
        self.bind("<Escape>",lambda e:self.destroy())
        self._build()

    def _build(self):
        tk.Frame(self,bg=C["accent"],height=3).pack(fill="x")
        body=tk.Frame(self,bg=C["bg2"]); body.pack(fill="both",expand=True,padx=32,pady=18)
        if HAS_PIL:
            img=load_icon("",60); self._img=ImageTk.PhotoImage(img)
            tk.Label(body,image=self._img,bg=C["bg2"]).pack(pady=(0,10))
        tk.Label(body,text=APP_NAME,bg=C["bg2"],fg=C["text"],
                 font=(FM,14,"bold")).pack()
        tk.Label(body,text=f"Version {APP_VER}",bg=C["bg2"],fg=C["accent2"],
                 font=(FM,9)).pack(pady=(2,0))
        tk.Label(body,text="Universal Plug & Play Gateway Controller",
                 bg=C["bg2"],fg=C["text3"],font=(FU,8)).pack(pady=(2,14))
        hsep(body).pack(fill="x",pady=(0,10))
        for k,v in [
            ("GitHub", "github.com/YOUR_USERNAME/upnp-port-manager"),
            ("Runtime",f"Python {sys.version.split()[0]}"),
            ("UPnP",   "miniupnpc" if HAS_UPNPC else "not installed"),
            ("Config", str(CONFIG_FILE)),
        ]:
            row=tk.Frame(body,bg=C["bg2"]); row.pack(fill="x",pady=1)
            tk.Label(row,text=f"{k:<10}",bg=C["bg2"],fg=C["text3"],
                     font=(FM,8)).pack(side="left")
            tk.Label(row,text=v,bg=C["bg2"],fg=C["text2"],
                     font=(FM,8)).pack(side="left")
        hsep(self).pack(fill="x",pady=(8,0))
        foot=tk.Frame(self,bg=C["bg2"]); foot.pack(pady=10)
        make_btn(foot,"CLOSE",self.destroy,accent=True).pack()

# ════════════════════════════════════════════════════════════════════
#  Uninstall Dialog
# ════════════════════════════════════════════════════════════════════
class UninstallDialog(tk.Toplevel):
    def __init__(self, parent, cfg, on_done):
        super().__init__(parent)
        self.cfg=cfg; self.on_done=on_done
        self.configure(bg=C["bg2"]); self.title("Uninstall")
        self.geometry("490x400"); self.resizable(False,False)
        self.grab_set(); self.transient(parent)
        px=parent.winfo_rootx()+parent.winfo_width()//2-245
        py=parent.winfo_rooty()+parent.winfo_height()//2-200
        self.geometry(f"+{max(0,px)}+{max(0,py)}")
        self._running=False; self._prog_v=tk.StringVar(master=self,value="")
        self.bind("<Escape>",lambda e:None if self._running else self.destroy())
        self._build()

    def _build(self):
        tk.Frame(self,bg=C["red"],height=3).pack(fill="x")
        hdr=tk.Frame(self,bg="#1a0a0a",height=48); hdr.pack(fill="x")
        hdr.pack_propagate(False)
        tk.Label(hdr,text="⚠  UNINSTALL",bg="#1a0a0a",fg=C["red"],
                 font=(FM,11,"bold")).pack(side="left",padx=16,pady=12)
        body=tk.Frame(self,bg=C["bg2"]); body.pack(fill="both",expand=True,padx=22,pady=12)
        warn=tk.Frame(body,bg="#1e0d0d",highlightthickness=1,
                      highlightbackground=C["red"])
        warn.pack(fill="x",pady=(0,12))
        tk.Label(warn,text="以下の操作は取り消せません。",
                 bg="#1e0d0d",fg=C["red2"],font=(FM,9,"bold"),
                 pady=8,padx=14).pack(anchor="w")
        tk.Label(body,text="UNINSTALL OPTIONS",bg=C["bg2"],fg=C["text3"],
                 font=(FM,7,"bold")).pack(anchor="w")
        hsep(body,C["border2"]).pack(fill="x",pady=(2,8))
        self._o_ports=tk.BooleanVar(master=self,value=True)
        self._o_auto=tk.BooleanVar(master=self,value=True)
        self._o_config=tk.BooleanVar(master=self,value=True)
        self._o_files=tk.BooleanVar(master=self,value=False)
        for var,text in [
            (self._o_ports, "全ての UPnP ポートマッピングを削除"),
            (self._o_auto,  "自動起動の登録を解除 (レジストリ)"),
            (self._o_config,"設定ファイルを削除  (~/.upnp_manager)"),
            (self._o_files, "アプリファイル自体を削除  ⚠ 要注意"),
        ]:
            f=tk.Frame(body,bg=C["bg2"]); f.pack(anchor="w",pady=3)
            tk.Checkbutton(f,text=text,variable=var,
                           bg=C["bg2"],fg=C["text"],
                           selectcolor=C["card2"],
                           activebackground=C["bg2"],
                           activeforeground=C["text"],
                           font=(FM,9),cursor="hand2").pack(side="left")
        hsep(body,C["border"]).pack(fill="x",pady=(12,6))
        self._prog_lbl=tk.Label(body,textvariable=self._prog_v,
                                 bg=C["bg2"],fg=C["yellow"],font=(FM,8),anchor="w")
        self._prog_lbl.pack(fill="x")
        hsep(self).pack(fill="x",pady=(8,0))
        foot=tk.Frame(self,bg=C["bg2"]); foot.pack(fill="x",padx=22,pady=12)
        make_btn(foot,"CANCEL",self.destroy).pack(side="left")
        self._exec_btn=make_btn(foot,"UNINSTALL  ▶",self._execute,danger=True)
        self._exec_btn.pack(side="right")

    def _log(self,msg,color=None):
        self._prog_v.set(msg)
        if color: self._prog_lbl.configure(fg=color)
        self.update_idletasks()

    def _execute(self):
        if self._running: return
        self._running=True
        self._exec_btn.configure(state="disabled",text="実行中…")
        do_p=self._o_ports.get(); do_a=self._o_auto.get()
        do_c=self._o_config.get(); do_f=self._o_files.get()
        def run():
            errors=[]
            if do_p and self.cfg.get("ports"):
                self._log("UPnP ポートを削除中…",C["yellow"])
                ok,_=upnp.discover()
                if ok:
                    for e in self.cfg["ports"]:
                        try: upnp.remove(e["port"],e["proto"])
                        except Exception as ex: errors.append(str(ex))
                    self._log(f"✓  {len(self.cfg['ports'])} 件を削除",C["green"])
                else: self._log("⚠  UPnP 接続失敗",C["yellow"])
            if do_a:
                self._log("自動起動を解除中…",C["yellow"])
                try: set_autostart(False); self._log("✓  解除完了",C["green"])
                except Exception as ex: errors.append(str(ex))
            if do_c:
                self._log("設定ファイルを削除中…",C["yellow"])
                try:
                    if CONFIG_DIR.exists(): shutil.rmtree(CONFIG_DIR)
                    self._log("✓  削除完了",C["green"])
                except Exception as ex: errors.append(str(ex))
            if do_f:
                self._log("アプリファイルを削除予定…",C["yellow"])
                try:
                    bat=Path(os.environ.get("TEMP",str(Path.home())))/"_upnp_cleanup.bat"
                    lines=["@echo off","timeout /t 2 /nobreak > nul"]
                    for f in APP_DIR.rglob("*"):
                        lines.append(f'del /f /q "{f}"')
                    lines+=[f'rd /s /q "{APP_DIR}"','del /f /q "%~f0"']
                    bat.write_text("\n".join(lines),encoding="cp932",errors="replace")
                    if IS_WINDOWS:
                        subprocess.Popen(["cmd","/c","start","/min",str(bat)],
                                         creationflags=getattr(subprocess,"CREATE_NO_WINDOW",0))
                    self._log("✓  終了後に削除されます",C["green"])
                except Exception as ex: errors.append(str(ex))
            self._log("⚠  エラーあり" if errors else "✓  完了",
                      C["red"] if errors else C["green"])
            self.after(800,self._finish)
        threading.Thread(target=run,daemon=True).start()

    def _finish(self):
        self.destroy(); self.on_done()

# ════════════════════════════════════════════════════════════════════
#  Port Row
# ════════════════════════════════════════════════════════════════════
STATUS_CFG = {
    "ok":      (C["green"],  "●  ACTIVE"),
    "error":   (C["red"],    "●  FAILED"),
    "pending": (C["yellow"], "●  PENDING"),
    "idle":    (C["text3"],  "●  IDLE"),
}

class PortRow(tk.Frame):
    def __init__(self,parent,info,on_remove=None,
                 on_reapply=None,on_close_port=None,**kw):
        super().__init__(parent,bg=C["card"],
                         highlightthickness=1,
                         highlightbackground=C["border"],**kw)
        self.info=info; self._on_remove=on_remove
        self._on_reapply=on_reapply; self._on_close_p=on_close_port
        self.selected=False; self._build(); self._bind_hover()

    def _build(self):
        inner=tk.Frame(self,bg=self["bg"])
        inner.pack(fill="x",padx=12,pady=9); self._inner=inner
        pl=tk.Label(inner,text=f"{self.info['port']:>5}",
                    bg=inner["bg"],fg=C["cyan"],
                    font=(FM,14,"bold"),width=6,anchor="e")
        pl.pack(side="left"); Tooltip(pl,f"Port {self.info['port']}")
        vsep(inner).pack(side="left",fill="y",padx=10,pady=2)
        proto=self.info["proto"]
        pc={"TCP":C["accent"],"UDP":C["yellow"],"BOTH":C["green"]}.get(proto,C["text2"])
        prl=tk.Label(inner,text=proto,bg=pc,fg="white",
                      font=(FM,8,"bold"),padx=6,pady=1)
        prl.pack(side="left",padx=(0,10))
        tk.Label(inner,text=self.info.get("desc",""),
                 bg=inner["bg"],fg=C["text2"],
                 font=(FM,9),anchor="w").pack(side="left",fill="x",expand=True)
        st=self.info.get("status","idle")
        col,stxt=STATUS_CFG.get(st,STATUS_CFG["idle"])
        self._st_lbl=tk.Label(inner,text=stxt,bg=inner["bg"],fg=col,
                               font=(FM,8),width=11,anchor="e")
        self._st_lbl.pack(side="right",padx=(4,0))
        rb=make_btn(inner,"✕",self._do_remove,danger=True,small=True)
        rb.pack(side="right",padx=(4,0))
        Tooltip(rb,"削除")
        self._bg_widgets=[inner,pl,self._st_lbl,rb]
        for w in (self,inner,pl,self._st_lbl,prl):
            w.bind("<Button-1>",self._click)
            w.bind("<Button-3>",self._ctx_menu)

    def _click(self,e):
        self.selected=not self.selected; self._refresh_colors()

    def _do_remove(self):
        if self._on_remove: self._on_remove(self.info)

    def _ctx_menu(self,e):
        m=tk.Menu(self,tearoff=0,bg=C["card2"],fg=C["text"],
                  activebackground=C["accent"],activeforeground="white",
                  font=(FU,9),relief="flat",bd=0)
        m.add_command(label=f"  ↻  再適用  ({self.info['port']}/{self.info['proto']})",
                      command=lambda:self._on_reapply and self._on_reapply(self.info))
        m.add_command(label="  ⬇  ポートを閉じる",
                      command=lambda:self._on_close_p and self._on_close_p(self.info))
        m.add_separator()
        m.add_command(label="  🗑  削除",command=self._do_remove)
        m.tk_popup(e.x_root,e.y_root)

    def _bind_hover(self):
        def enter(e):
            if not self.selected:
                self._set_bg(C["card2"])
                self.configure(highlightbackground=C["border2"])
        def leave(e):
            if not self.selected:
                self._set_bg(C["card"])
                self.configure(highlightbackground=C["border"])
        self.bind("<Enter>",enter); self.bind("<Leave>",leave)

    def _refresh_colors(self):
        if self.selected:
            self._set_bg(C["card2"]); self.configure(highlightbackground=C["accent"])
        else:
            self._set_bg(C["card"]); self.configure(highlightbackground=C["border"])

    def _set_bg(self,color):
        self.configure(bg=color)
        for w in self._bg_widgets:
            try: w.configure(bg=color)
            except Exception: pass

    def set_status(self,status):
        self.info["status"]=status
        col,txt=STATUS_CFG.get(status,STATUS_CFG["idle"])
        self._st_lbl.configure(text=txt,fg=col)

# ════════════════════════════════════════════════════════════════════
#  Log Panel
# ════════════════════════════════════════════════════════════════════
class LogPanel(tk.Frame):
    MAX=300
    def __init__(self,parent,**kw):
        super().__init__(parent,bg=C["bg"],**kw)
        self._visible=True; self._build()

    def _build(self):
        hdr=tk.Frame(self,bg=C["card2"],height=22)
        hdr.pack(fill="x"); hdr.pack_propagate(False)
        self._arr=tk.Label(hdr,text="▾  LOG",bg=C["card2"],fg=C["text3"],
                            font=(FM,7,"bold"),cursor="hand2")
        self._arr.pack(side="left",padx=10)
        clr=tk.Button(hdr,text="CLR",bg=C["card2"],fg=C["text3"],
                       activebackground=C["border2"],activeforeground=C["text"],
                       relief="flat",font=(FM,6),cursor="hand2",command=self.clear)
        clr.pack(side="right",padx=(0,4)); Tooltip(clr,"ログをクリア")
        tog=tk.Button(hdr,text="▾",bg=C["card2"],fg=C["text3"],
                       activebackground=C["card2"],activeforeground=C["text"],
                       relief="flat",font=(FM,7),cursor="hand2",command=self.toggle)
        tog.pack(side="right"); self._tog_btn=tog
        Tooltip(tog,"ログパネルを開閉  [Ctrl+L]")
        hdr.bind("<Button-1>",lambda e:self.toggle())
        self._arr.bind("<Button-1>",lambda e:self.toggle())
        self._body=tk.Frame(self,bg=C["bg2"]); self._body.pack(fill="both",expand=True)
        style=ttk.Style()
        style.configure("Log.Vertical.TScrollbar",
                         troughcolor=C["bg2"],background=C["border2"],arrowcolor=C["text3"])
        self._text=tk.Text(self._body,bg=C["bg2"],fg=C["text2"],
                            font=(FM,8),relief="flat",state="disabled",
                            height=5,wrap="word",
                            selectbackground=C["border2"],highlightthickness=0)
        sb=ttk.Scrollbar(self._body,orient="vertical",command=self._text.yview,
                          style="Log.Vertical.TScrollbar")
        self._text.configure(yscrollcommand=sb.set)
        sb.pack(side="right",fill="y"); self._text.pack(fill="both",expand=True,padx=2,pady=2)
        for tag,col in [("ok",C["green"]),("error",C["red"]),("warn",C["yellow"]),
                         ("info",C["text2"]),("special",C["cyan"])]:
            self._text.tag_configure(tag,foreground=col)

    def append(self,msg,level="info"):
        ts=datetime.now().strftime("%H:%M:%S")
        self._text.configure(state="normal")
        lines=int(self._text.index("end-1c").split(".")[0])
        if lines>self.MAX: self._text.delete("1.0",f"{lines-self.MAX}.0")
        self._text.insert("end",f"[{ts}] ","time" if False else "info")
        self._text.insert("end",msg+"\n",level)
        self._text.configure(state="disabled"); self._text.see("end")

    def clear(self):
        self._text.configure(state="normal"); self._text.delete("1.0","end")
        self._text.configure(state="disabled")

    def toggle(self):
        self._visible=not self._visible
        if self._visible:
            self._body.pack(fill="both",expand=True)
            self._arr.configure(text="▾  LOG"); self._tog_btn.configure(text="▾")
        else:
            self._body.pack_forget()
            self._arr.configure(text="▸  LOG"); self._tog_btn.configure(text="▸")

    def set_visible(self,v):
        if v!=self._visible: self.toggle()

# ════════════════════════════════════════════════════════════════════
#  Main App
# ════════════════════════════════════════════════════════════════════
class App(tk.Tk):
    SHORTCUTS={
        "<Control-n>":"_add_port","<Control-N>":"_add_port",
        "<F5>":"_refresh_all","<Delete>":"_remove_selected",
        "<Control-l>":"_toggle_log","<Control-L>":"_toggle_log",
        "<Control-comma>":"_open_settings",
        "<Escape>":"_hide","<F1>":"_open_about",
    }

    def __init__(self,cfg):
        super().__init__()
        self.cfg=cfg; self.tray=None
        self._q=queue.Queue(); self._rows={}
        self._bg_photo=None; self._anim_id=None; self._bg_afid=None
        self._sort_col=None; self._sort_rev=False

        self._setup_window()
        self._build()
        self.update_idletasks()
        self._register_shortcuts()

        self.withdraw()
        self._splash=SplashScreen(self)
        self.after(1650,self._after_splash)

    def _after_splash(self):
        try: self._splash.destroy()
        except Exception: pass
        if not self.cfg.get("start_minimized"):
            self.deiconify(); self.update_idletasks()
        self._reload_rows()
        self._start_tray(); self._poll(); self._anim_pulse()
        if not self.cfg.get("start_minimized"):
            self.after(400,self._auto_refresh)
        if self.cfg.get("check_updates",True):
            self.after(3000,self._check_update_bg)

    def _setup_window(self):
        self.geometry(self.cfg.get("window_geometry","760x700+120+80"))
        self.minsize(520,480); self.configure(bg=C["bg"])
        self.title(APP_NAME)
        self.protocol("WM_DELETE_WINDOW",self._hide)
        self._refresh_app_icon()

    def _refresh_app_icon(self):
        if not HAS_PIL: return
        img=load_icon(self.cfg.get("icon_path",""),32)
        if img:
            self._tk_icon=ImageTk.PhotoImage(img)
            try: self.iconphoto(True,self._tk_icon)
            except Exception: pass

    # ── build ─────────────────────────────────────
    def _build(self):
        self._bg_canvas=tk.Canvas(self,highlightthickness=0,bd=0,bg=C["bg"])
        self._bg_canvas.place(x=0,y=0,relwidth=1,relheight=1)
        self._content=tk.Frame(self,bg=C["bg"])
        self._content.pack(fill="both",expand=True)
        self._build_header()
        hsep(self._content).pack(fill="x")
        self._build_toolbar()
        self._build_filter_bar()
        self._build_list()
        self._build_log()
        self._build_statusbar()
        self.bind("<Configure>",lambda e:self._debounce_bg())
        self._draw_bg()

    def _build_header(self):
        hdr=tk.Frame(self._content,bg=C["bg2"],height=66)
        hdr.pack(fill="x"); hdr.pack_propagate(False)
        if HAS_PIL:
            img=load_icon(self.cfg.get("icon_path",""),42)
            if img:
                self._hdr_img=ImageTk.PhotoImage(img)
                tk.Label(hdr,image=self._hdr_img,bg=C["bg2"]).pack(side="left",padx=14)
        tf=tk.Frame(hdr,bg=C["bg2"]); tf.pack(side="left",fill="y",pady=10)
        tk.Label(tf,text="UPnP PORT MANAGER",bg=C["bg2"],fg=C["text"],
                 font=(FM,13,"bold")).pack(anchor="w")
        tk.Label(tf,text=f"{APP_DESC}  ·  v{APP_VER}",bg=C["bg2"],fg=C["text3"],
                 font=(FM,8)).pack(anchor="w")
        rc=tk.Frame(hdr,bg=C["bg2"]); rc.pack(side="right",padx=10)
        for txt,cmd,tip in [("ⓘ",self._open_about,"バージョン情報  [F1]"),
                              ("⚙",self._open_settings,"設定  [Ctrl+,]")]:
            b=tk.Button(rc,text=txt,command=cmd,bg=C["bg2"],fg=C["text2"],
                         activebackground=C["card2"],activeforeground=C["text"],
                         relief="flat",font=(FU,13),cursor="hand2")
            b.pack(side="left",padx=(0,4)); Tooltip(b,tip)
        self._pulse_dot=tk.Label(hdr,text="●",bg=C["bg2"],fg=C["accent"],
                                  font=(FM,10))
        self._pulse_dot.pack(side="right",padx=(0,6))
        Tooltip(self._pulse_dot,"UPnP 接続インジケーター")

    def _build_toolbar(self):
        bar=tk.Frame(self._content,bg=C["bg"],pady=7)
        bar.pack(fill="x",padx=12)
        btns=[
            ("＋  ADD",    self._add_port,       True,  False, False,
             "ポートを追加  [Ctrl+N]"),
            ("↻  REFRESH",self._refresh_all,      False, False, False,
             "全ポートを再適用  [F5]"),
            ("⚡ 一括操作",self._bulk_release,     False, False, True,
             "全ポートを一括で開放 / 解放"),
            ("✕  REMOVE",  self._remove_selected,  False, True,  False,
             "選択中を削除  [Delete]"),
            ("↺  UPDATE",  self._check_update_manual, False, False, False,
             "アップデートを確認"),
            ("🗑 UNINSTALL",self._open_uninstall,  False, False, False,
             "アンインストール"),
        ]
        for txt,cmd,acc,dng,wrn,tip in btns:
            b=make_btn(bar,txt,cmd,accent=acc,danger=dng,warn=wrn)
            b.pack(side="left",padx=(0,5)); Tooltip(b,tip)
        self._status_lbl=tk.Label(bar,text="Initializing…",
                                   bg=C["bg"],fg=C["text3"],font=(FM,8))
        self._status_lbl.pack(side="right")

    def _build_filter_bar(self):
        fb=tk.Frame(self._content,bg=C["card2"],height=32)
        fb.pack(fill="x",padx=12,pady=(4,0)); fb.pack_propagate(False)
        tk.Label(fb,text="🔍",bg=C["card2"],fg=C["text3"],
                 font=(FU,9)).pack(side="left",padx=(8,4))
        self._filter_v=tk.StringVar(master=self)
        self._filter_v.trace_add("write",lambda *_:self._apply_filter())
        fe=tk.Entry(fb,textvariable=self._filter_v,
                    bg=C["card2"],fg=C["text"],
                    insertbackground=C["cyan"],relief="flat",
                    font=(FM,9),highlightthickness=0)
        fe.pack(side="left",fill="x",expand=True,ipady=3)
        Tooltip(fe,"ポート番号・プロトコル・説明でフィルター")
        clr=tk.Button(fb,text="✕",command=lambda:self._filter_v.set(""),
                       bg=C["card2"],fg=C["text3"],
                       activebackground=C["card2"],activeforeground=C["text"],
                       relief="flat",cursor="hand2",font=(FM,8))
        clr.pack(side="right",padx=4); Tooltip(clr,"クリア")

    def _build_list(self):
        ch=tk.Frame(self._content,bg=C["card2"],height=26)
        ch.pack(fill="x",padx=12,pady=(0,0)); ch.pack_propagate(False)
        self._col_btns={}
        for col,txt,w in [("port","PORT",8),("proto","PROTO",7),
                            ("desc","DESC",26),("status","STATUS",12)]:
            b=tk.Button(ch,text=txt,command=lambda c=col:self._sort_by(c),
                         bg=C["card2"],fg=C["text3"],
                         activebackground=C["card2"],activeforeground=C["text2"],
                         relief="flat",cursor="hand2",
                         font=(FM,7,"bold"),width=w,anchor="w")
            b.pack(side="left",padx=10); Tooltip(b,f"{txt} でソート")
            self._col_btns[col]=b
        wrap=tk.Frame(self._content,bg=C["bg"])
        wrap.pack(fill="both",expand=True,padx=12,pady=(0,2))
        style=ttk.Style(); style.theme_use("clam")
        style.configure("D.Vertical.TScrollbar",troughcolor=C["bg2"],
                         background=C["border2"],arrowcolor=C["text3"])
        self._canvas=tk.Canvas(wrap,bg=C["bg"],highlightthickness=0)
        sb=ttk.Scrollbar(wrap,orient="vertical",command=self._canvas.yview,
                          style="D.Vertical.TScrollbar")
        self._list_frame=tk.Frame(self._canvas,bg=C["bg"])
        self._list_win=self._canvas.create_window((0,0),window=self._list_frame,anchor="nw")
        self._canvas.configure(yscrollcommand=sb.set)
        self._list_frame.bind("<Configure>",
            lambda e:self._canvas.configure(scrollregion=self._canvas.bbox("all")))
        self._canvas.bind("<Configure>",
            lambda e:self._canvas.itemconfig(self._list_win,width=e.width))
        self._canvas.bind("<MouseWheel>",
            lambda e:self._canvas.yview_scroll(-1*(e.delta//120),"units"))
        self._list_frame.bind("<MouseWheel>",
            lambda e:self._canvas.yview_scroll(-1*(e.delta//120),"units"))
        sb.pack(side="right",fill="y"); self._canvas.pack(side="left",fill="both",expand=True)
        self._empty=tk.Label(self._list_frame,
                              text="ポートマッピングがありません\n＋ ADD  または  Ctrl+N  で追加",
                              bg=C["bg"],fg=C["text3"],
                              font=(FM,10),justify="center",pady=50)

    def _build_log(self):
        self._log_panel=LogPanel(self._content)
        self._log_panel.pack(fill="x",padx=12,pady=(0,2))
        self._log_panel.set_visible(self.cfg.get("log_visible",True))

    def _build_statusbar(self):
        sb=tk.Frame(self._content,bg=C["card"],height=24)
        sb.pack(fill="x",side="bottom"); sb.pack_propagate(False)
        self._ext_lbl=tk.Label(sb,text="External: —",bg=C["card"],fg=C["text3"],font=(FM,7))
        self._ext_lbl.pack(side="left",padx=10); Tooltip(self._ext_lbl,"外部 IP")
        self._loc_lbl=tk.Label(sb,text="Local: —",bg=C["card"],fg=C["text3"],font=(FM,7))
        self._loc_lbl.pack(side="left",padx=6); Tooltip(self._loc_lbl,"ローカル IP")
        tk.Label(sb,text=f"v{APP_VER}  ·  Ctrl+N: 追加  F5: 更新  Del: 削除  Ctrl+L: ログ",
                 bg=C["card"],fg=C["text3"],font=(FM,7)).pack(side="right",padx=8)

    # ── background ────────────────────────────────
    def _debounce_bg(self):
        if self._bg_afid: self.after_cancel(self._bg_afid)
        self._bg_afid=self.after(120,self._draw_bg)

    def _draw_bg(self):
        c=self._bg_canvas; c.delete("all")
        w=self.winfo_width() or 760; h=self.winfo_height() or 700
        path=self.cfg.get("bg_image","")
        if path and os.path.isfile(path) and HAS_PIL:
            try:
                img=Image.open(path).convert("RGBA").resize((w,h),Image.LANCZOS)
                dark=Image.new("RGBA",(w,h),(6,6,22,215))
                img=Image.alpha_composite(img,dark)
                self._bg_photo=ImageTk.PhotoImage(img)
                c.create_image(0,0,anchor="nw",image=self._bg_photo); return
            except Exception: pass
        c.configure(bg=C["bg"])

    # ── pulse ─────────────────────────────────────
    def _anim_pulse(self):
        v=0.55+0.45*math.sin(time.time()*1.43)
        r=int(v*180); g=int(v*80); b=int(v*255)
        try: self._pulse_dot.configure(fg=f"#{r:02x}{g:02x}{b:02x}")
        except Exception: pass
        self._anim_id=self.after(100,self._anim_pulse)

    # ── row management ────────────────────────────
    def _reload_rows(self,ports=None):
        self.update_idletasks()
        for w in self._list_frame.winfo_children():
            try: w.destroy()
            except Exception: pass
        self._rows.clear()
        ports=(ports if ports is not None else self.cfg.get("ports",[]))
        if not ports:
            try: self._empty.pack(expand=True)
            except Exception: pass
            return
        try: self._empty.pack_forget()
        except Exception: pass
        for info in ports:
            self._make_row(info)

    def _make_row(self,info):
        key=(info["port"],info["proto"])
        row=PortRow(self._list_frame,info,
                    on_remove=self._on_row_remove,
                    on_reapply=self._on_row_reapply,
                    on_close_port=self._on_row_close_port)
        row.pack(fill="x",pady=3)
        self._rows[key]=row; return row

    def _on_row_remove(self,info):
        if not messagebox.askyesno("Confirm",
                f"ポート {info['port']}/{info['proto']} を削除しますか？",parent=self): return
        def do():
            ok,msg=upnp.remove(info["port"],info["proto"])
            self.cfg["ports"]=[p for p in self.cfg["ports"]
                                if not (p["port"]==info["port"] and p["proto"]==info["proto"])]
            save_cfg(self.cfg); self._q.put(("reload",))
            self._q.put(("log",f"削除: {info['port']}/{info['proto']}  {msg}",
                          "ok" if ok else "warn"))
        threading.Thread(target=do,daemon=True).start()

    def _on_row_reapply(self,info):
        def do():
            ok,msg=upnp.add(info["port"],info["proto"],info.get("desc",""))
            info["status"]="ok" if ok else "error"; save_cfg(self.cfg)
            self._q.put(("row_st",info["port"],info["proto"],info["status"]))
            self._q.put(("log",f"再適用: {info['port']}/{info['proto']}  {msg}",
                          "ok" if ok else "error"))
            if ok: show_toast(APP_NAME,f"Port {info['port']}/{info['proto']} re-opened")
        threading.Thread(target=do,daemon=True).start()

    def _on_row_close_port(self,info):
        def do():
            ok,msg=upnp.remove(info["port"],info["proto"])
            if ok: info["status"]="idle"; save_cfg(self.cfg)
            self._q.put(("row_st",info["port"],info["proto"],
                          "idle" if ok else "error"))
            self._q.put(("log",f"閉鎖: {info['port']}/{info['proto']}  {msg}",
                          "ok" if ok else "error"))
        threading.Thread(target=do,daemon=True).start()

    def _update_row_status(self,port,proto,status):
        key=(port,proto)
        if key in self._rows: self._rows[key].set_status(status)

    # ── sort & filter ─────────────────────────────
    def _sort_by(self,col):
        self._sort_rev=(not self._sort_rev if self._sort_col==col else False)
        self._sort_col=col
        for k,b in self._col_btns.items():
            base=b.cget("text").rstrip(" ▲▼")
            b.configure(text=base+(" ▼" if self._sort_rev else " ▲") if k==col else base,
                         fg=C["cyan"] if k==col else C["text3"])
        ports=self.cfg.get("ports",[])[:]
        fn={"port":lambda p:p["port"],"proto":lambda p:p["proto"],
            "desc":lambda p:p.get("desc","").lower(),
            "status":lambda p:p.get("status","idle")}.get(col,lambda p:p["port"])
        ports.sort(key=fn,reverse=self._sort_rev)
        self._apply_filter(ports=ports)

    def _apply_filter(self,ports=None):
        q=self._filter_v.get().lower().strip()
        src=ports if ports is not None else self.cfg.get("ports",[])
        if q:
            src=[p for p in src if q in str(p["port"])
                 or q in p.get("proto","").lower()
                 or q in p.get("desc","").lower()]
        self._reload_rows(ports=src)

    # ── actions ───────────────────────────────────
    def _add_port(self,e=None):
        def on_add(port,proto,desc):
            protos=["TCP","UDP"] if proto=="BOTH" else [proto]
            existing={(p["port"],p["proto"]) for p in self.cfg["ports"]}
            added=[]
            for pr in protos:
                if (port,pr) not in existing:
                    entry={"port":port,"proto":pr,"desc":desc,"status":"pending"}
                    self.cfg["ports"].append(entry); added.append(pr)
            if not added:
                self._log_panel.append(f"ポート {port} は既に登録済みです","warn"); return
            save_cfg(self.cfg); self._reload_rows()
            self._log_panel.append(f"追加: {port}/{'/'.join(added)}  「{desc}」","info")
            def apply():
                ok2,_=upnp.discover()
                if ok2: self._q.put(("ips",upnp.extern_ip,upnp.local_ip))
                for pr in added:
                    ok3,m3=upnp.add(port,pr,desc)
                    st="ok" if ok3 else "error"
                    for e2 in self.cfg["ports"]:
                        if e2["port"]==port and e2["proto"]==pr: e2["status"]=st
                    save_cfg(self.cfg)
                    self._q.put(("row_st",port,pr,st))
                    self._q.put(("status",f"{'✓' if ok3 else '✗'}  {port}/{pr}  {m3}",
                                  C["green"] if ok3 else C["red"]))
                    self._q.put(("log",f"{'開放' if ok3 else '失敗'}: {port}/{pr}  {m3}",
                                  "ok" if ok3 else "error"))
                    if ok3: show_toast(APP_NAME,f"Port {port}/{pr} opened")
            threading.Thread(target=apply,daemon=True).start()
        AddPortDialog(self,on_add)

    def _remove_selected(self,e=None):
        sel=[r for r in self._rows.values() if r.selected]
        if not sel:
            messagebox.showinfo("Info","削除したいポートを選択してください。",parent=self); return
        if not messagebox.askyesno("Confirm",f"{len(sel)} 件を削除しますか？",parent=self): return
        def do():
            removed=[]
            for row in sel:
                upnp.remove(row.info["port"],row.info["proto"])
                removed.append(f"{row.info['port']}/{row.info['proto']}")
            keys={(r.info["port"],r.info["proto"]) for r in sel}
            self.cfg["ports"]=[p for p in self.cfg["ports"]
                                if (p["port"],p["proto"]) not in keys]
            save_cfg(self.cfg); self._q.put(("reload",))
            self._q.put(("log",f"削除: {', '.join(removed)}","warn"))
        threading.Thread(target=do,daemon=True).start()

    def _refresh_all(self,e=None):
        self._set_status("Refreshing…",C["yellow"])
        self._log_panel.append("全ポートを更新中…","info")
        def do():
            ok,msg=upnp.discover()
            if not ok:
                self._q.put(("status",f"✗  {msg}",C["red"]))
                self._q.put(("log",f"UPnP 検出失敗: {msg}","error")); return
            self._q.put(("ips",upnp.extern_ip,upnp.local_ip))
            results=[]
            for entry in self.cfg["ports"]:
                ok2,m=upnp.add(entry["port"],entry["proto"],entry.get("desc",""))
                entry["status"]="ok" if ok2 else "error"
                self._q.put(("row_st",entry["port"],entry["proto"],entry["status"]))
                results.append((entry["port"],entry["proto"],ok2,m))
            save_cfg(self.cfg)
            n_ok=sum(1 for *_,o,_ in results if o); n_all=len(results)
            self._q.put(("status",f"✓  {n_ok}/{n_all} active  ·  {upnp.extern_ip}",
                          C["green"] if n_ok==n_all else C["yellow"]))
            for port,proto,ok3,m in results:
                self._q.put(("log",f"{'✓' if ok3 else '✗'}  {port}/{proto}  {m}",
                              "ok" if ok3 else "error"))
        threading.Thread(target=do,daemon=True).start()

    def _bulk_release(self,e=None):
        ports=self.cfg.get("ports",[])
        if not ports:
            messagebox.showinfo("Info","登録済みのポートがありません。",parent=self); return
        def on_done(results,mode):
            save_cfg(self.cfg); self._q.put(("reload",))
            n_ok=sum(1 for *_,o,_ in results if o)
            action="開放" if mode=="open" else "解放"
            self._q.put(("log",f"一括{action}: {n_ok}/{len(results)} 件 完了",
                          "ok" if n_ok==len(results) else "warn"))
            self._q.put(("status",f"✓  一括{action} {n_ok}/{len(results)} 件",
                          C["green"] if n_ok==len(results) else C["yellow"]))
        BulkReleaseDialog(self,ports,on_done)

    def _auto_refresh(self):
        if self.cfg.get("ports"): self._refresh_all()
        else:
            def probe():
                ok,msg=upnp.discover()
                if ok:
                    self._q.put(("ips",upnp.extern_ip,upnp.local_ip))
                    self._q.put(("status",f"Ready  ·  {upnp.extern_ip}",C["green"]))
                    self._q.put(("log",f"UPnP 検出成功  External: {upnp.extern_ip}","ok"))
                else:
                    self._q.put(("status","UPnP デバイスが見つかりません",C["red"]))
                    self._q.put(("log",f"UPnP: {msg}","error"))
            threading.Thread(target=probe,daemon=True).start()

    def _set_status(self,txt,color=None):
        self._status_lbl.configure(text=txt,fg=color or C["text2"])

    def _toggle_log(self,e=None):
        self._log_panel.toggle()
        self.cfg["log_visible"]=self._log_panel._visible; save_cfg(self.cfg)

    # ── update ────────────────────────────────────
    def _check_update_bg(self):
        def check():
            try:
                ver,notes,dl_url,files=UpdateChecker.fetch_remote_version()
                if UpdateChecker.is_newer(ver):
                    self._q.put(("update",ver,notes,dl_url,files))
                else:
                    self._q.put(("log",f"最新バージョンです (v{APP_VER})","special"))
            except Exception as e:
                self._q.put(("log",f"更新確認失敗: {e}","warn"))
        threading.Thread(target=check,daemon=True).start()

    def _check_update_manual(self,e=None):
        self._set_status("更新を確認中…",C["yellow"])
        self._log_panel.append("GitHub から更新を確認中…","info")
        def check():
            try:
                ver,notes,dl_url,files=UpdateChecker.fetch_remote_version()
                if UpdateChecker.is_newer(ver):
                    self._q.put(("update",ver,notes,dl_url,files))
                else:
                    self._q.put(("status",f"✓  最新バージョンです (v{APP_VER})",C["green"]))
                    self._q.put(("log",f"最新バージョンです (v{APP_VER})","special"))
            except Exception as e:
                self._q.put(("status",f"✗  確認失敗: {e}",C["red"]))
                self._q.put(("log",f"更新確認失敗: {e}","error"))
        threading.Thread(target=check,daemon=True).start()

    # ── queue poll ────────────────────────────────
    def _poll(self):
        try:
            while True:
                msg=self._q.get_nowait(); cmd=msg[0]
                if   cmd=="status":  self._set_status(msg[1],msg[2])
                elif cmd=="ips":
                    self._ext_lbl.configure(text=f"External: {msg[1]}")
                    self._loc_lbl.configure(text=f"Local: {msg[2]}")
                elif cmd=="row_st":  self._update_row_status(msg[1],msg[2],msg[3])
                elif cmd=="reload":  self._reload_rows()
                elif cmd=="log":     self._log_panel.append(msg[1],msg[2])
                elif cmd=="update":  UpdateDialog(self,msg[1],msg[2],msg[3],msg[4])
        except queue.Empty: pass
        self.after(80,self._poll)

    def _register_shortcuts(self):
        for key,method in self.SHORTCUTS.items():
            self.bind(key,getattr(self,method))

    # ── tray ──────────────────────────────────────
    def _start_tray(self):
        if not (HAS_PYSTRAY and HAS_PIL): return
        tray_img=load_icon(self.cfg.get("icon_path",""),64)
        menu=TrayMenu(
            Item("表示 / 非表示",   self._tray_toggle,default=True),
            TrayMenu.SEPARATOR,
            Item("ポートを追加",    lambda i,m:self.after(0,self._add_port)),
            Item("全て更新",        lambda i,m:self.after(0,self._refresh_all)),
            Item("一括操作",        lambda i,m:self.after(0,self._bulk_release)),
            TrayMenu.SEPARATOR,
            Item("更新を確認",      lambda i,m:self.after(0,self._check_update_manual)),
            Item("設定",            lambda i,m:self.after(0,self._open_settings)),
            Item("About",           lambda i,m:self.after(0,self._open_about)),
            Item("アンインストール",lambda i,m:self.after(0,self._open_uninstall)),
            TrayMenu.SEPARATOR,
            Item("終了",            self._quit),
        )
        self.tray=pystray.Icon("upnp_manager",tray_img,APP_NAME,menu)
        threading.Thread(target=self.tray.run,daemon=True).start()

    def _tray_toggle(self,icon,item):
        if self.winfo_viewable(): self.after(0,self._hide)
        else: self.after(0,self._show)

    def _hide(self,e=None):
        self._save_geometry(); self.withdraw()

    def _show(self):
        self.deiconify(); self.lift(); self.focus_force()

    def _save_geometry(self):
        self.cfg["window_geometry"]=self.geometry()
        self.cfg["log_visible"]=self._log_panel._visible
        save_cfg(self.cfg)

    def _quit(self,*_):
        self._save_geometry()
        if self.tray: self.tray.stop()
        if self._anim_id: self.after_cancel(self._anim_id)
        self.after(0,self.destroy)

    def _open_settings(self,e=None):
        SettingsDialog(self,self.cfg,self._apply_settings)

    def _apply_settings(self,new_cfg):
        self.cfg.update(new_cfg); save_cfg(self.cfg)
        set_autostart(self.cfg.get("autostart",False))
        self._refresh_app_icon(); self._draw_bg()
        if self.tray: self.tray.stop(); self.tray=None
        self.after(600,self._start_tray)
        self._log_panel.append("設定を保存しました","special")

    def _open_uninstall(self,e=None):
        self._show(); UninstallDialog(self,self.cfg,self._quit)

    def _open_about(self,e=None):
        AboutDialog(self)

# ════════════════════════════════════════════════════════════════════
#  Entry point
# ════════════════════════════════════════════════════════════════════
def main():
    _ensure_icon_file()
    cfg=load_cfg()
    for p in cfg.get("ports",[]): p["status"]="pending"
    app=App(cfg); app.mainloop()

if __name__=="__main__":
    try:
        main()
    except Exception:
        import traceback
        log=Path(tempfile.gettempdir())/"upnp_manager_error.log"
        try: log.write_text(traceback.format_exc(),encoding="utf-8")
        except Exception: pass
        raise
