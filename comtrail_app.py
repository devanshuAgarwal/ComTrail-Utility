import os, time, json, threading, csv, shlex, webbrowser, re, random, sys
import tkinter as tk

# Cross-platform UI font: "Segoe UI" on Windows, Helvetica Neue on macOS
_UI_FONT = "Segoe UI" if sys.platform == "win32" else ("Helvetica Neue" if sys.platform == "darwin" else "DejaVu Sans")

def _resource_path(filename):
    """Return absolute path to a bundled resource (works for both script and PyInstaller exe)."""
    base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, filename)

# ── Auto Call-ID generator ───────────────────────────────────────────────────
_used_call_ids: set = set()
_call_id_counter: int = 10_000_000

def _next_call_id() -> int:
    """Return a unique Call ID >= 10000000, randomly incrementing each time."""
    global _call_id_counter
    while True:
        _call_id_counter += random.randint(1, 999)
        if _call_id_counter not in _used_call_ids:
            _used_call_ids.add(_call_id_counter)
            return _call_id_counter
# ─────────────────────────────────────────────────────────────────────────────
from tkinter import ttk, filedialog
import paramiko

# Make every tk.Button show a hand cursor by default
_btn_init_orig = tk.Button.__init__
def _btn_init_patched(self, master=None, **kw):
    kw.setdefault("cursor", "hand2")
    _btn_init_orig(self, master, **kw)
tk.Button.__init__ = _btn_init_patched

# ================================================================
# CONFIGURATION
# ================================================================
# Always store config next to the script — avoids OneDrive/permission issues
CONFIG_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "comtrail_config.json"
)
SCHEDULE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "comtrail_schedule.json"
)
DEFAULT_CFG = {
    "pcap":  {"ip": "", "pwd": "", "path": ""},
    "ludr":  {"ip": "", "pwd": "", "path": ""},
    "voice": {"ip": "", "pwd": "", "path": ""},
    "ip":    {"ip": "", "pwd": "", "path": ""},
    "comtrail_url": "http://klera.comtrail-demo.clear-trail.com/clearinsight/",
    "kafka": {
        "base_url":    "http://kafka-ui.comtrail-demo.clear-trail.com",
        "cluster":     "kafka-cluster-1",
        "topic_voice": "targetVoiceEnrichOutputTopic",
        "topic_ip":    "targetEnrichOutputTopic",
    },
    "solr": {
        "base_url":   "http://solr-s1.comtrail-demo.clear-trail.com",
        "collection": "target_transaction_ct",
    },
}

# ── Solr configuration ───────────────────────────────────────────────────────
SOLR_BASE       = "http://solr-s1.comtrail-demo.clear-trail.com"
SOLR_COLLECTION = "target_transaction_ct"
SOLR_SELECT_URL = f"{SOLR_BASE}/solr/{SOLR_COLLECTION}/select"
SOLR_UI_URL     = (f"{SOLR_BASE}/solr/#/{SOLR_COLLECTION}/query"
                   f"?q=_7:5001&q.op=OR&indent=true&useParams=")
# Field mappings
SOLR_TYPE_FIELD  = "_7"
SOLR_TYPES       = {
    "All":   "ALL",   # no type filter — query everything
    "Voice": "5001",
    "SMS":   "5002",
    "LUDR":  "5003",
    "IP":    "IP",    # IP records: _7 is NOT 5001/5002/5003
}
SOLR_FIELDS      = {
    "_7":   "Type",
    "_116": "Description",
    "_12":  "Activity Time",
    "_15":  "Call Start",
    "_16":  "Call End",
    "_68":  "Called Number",
    "_182": "Target Number",
    "_198": "Target Name",
}

# ── Kafka configuration ───────────────────────────────────────────────────────
KAFKA_UI_BASE    = "http://kafka-ui.comtrail-demo.clear-trail.com"
KAFKA_CLUSTER    = "kafka-cluster-1"
KAFKA_TOPICS     = {
    "Voice": "targetVoiceEnrichOutputTopic",
    "IP":    "targetEnrichOutputTopic",
}
# Default topic (backward-compat)
KAFKA_TOPIC      = "targetVoiceEnrichOutputTopic"

def _apply_service_config(cfg):
    """Push kafka/solr values from config dict into module-level globals."""
    global KAFKA_UI_BASE, KAFKA_CLUSTER, KAFKA_TOPICS, KAFKA_TOPIC
    global SOLR_BASE, SOLR_COLLECTION
    global SOLR_SELECT_URL, SOLR_UI_URL, KAFKA_TOPIC_URL, KAFKA_API_URL
    k = cfg.get("kafka", {})
    s = cfg.get("solr",  {})
    if k.get("base_url"):    KAFKA_UI_BASE = k["base_url"].rstrip("/")
    if k.get("cluster"):     KAFKA_CLUSTER = k["cluster"]
    if k.get("topic_voice"): KAFKA_TOPICS["Voice"] = k["topic_voice"]
    if k.get("topic_ip"):    KAFKA_TOPICS["IP"]    = k["topic_ip"]
    KAFKA_TOPIC = KAFKA_TOPICS.get("Voice", KAFKA_TOPIC)
    if s.get("base_url"):    SOLR_BASE       = s["base_url"].rstrip("/")
    if s.get("collection"):  SOLR_COLLECTION = s["collection"]
    SOLR_SELECT_URL = f"{SOLR_BASE}/solr/{SOLR_COLLECTION}/select"
    SOLR_UI_URL     = (f"{SOLR_BASE}/solr/#/{SOLR_COLLECTION}/query"
                       f"?q=_7:5001&q.op=OR&indent=true&useParams=")
    KAFKA_TOPIC_URL = _kafka_ui_url_raw(KAFKA_TOPIC)
    KAFKA_API_URL   = _kafka_api_url_raw(KAFKA_TOPIC)

def _kafka_api_url_raw(topic):
    return f"{KAFKA_UI_BASE}/api/clusters/{KAFKA_CLUSTER}/topics/{topic}/messages"

def _kafka_ui_url_raw(topic):
    return f"{KAFKA_UI_BASE}/ui/clusters/{KAFKA_CLUSTER}/all-topics/{topic}"

def _kafka_api_url(topic):
    return (f"{KAFKA_UI_BASE}/api/clusters/{KAFKA_CLUSTER}"
            f"/topics/{topic}/messages")

def _kafka_ui_url(topic):
    return (f"{KAFKA_UI_BASE}/ui/clusters/{KAFKA_CLUSTER}"
            f"/all-topics/{topic}")

KAFKA_TOPIC_URL  = _kafka_ui_url(KAFKA_TOPIC)
KAFKA_API_URL    = _kafka_api_url(KAFKA_TOPIC)


def _parse_sse_or_json(raw, _json):
    """
    kafka-ui v0.5+ streams messages as SSE (text/event-stream).
    Each line is:  data: <json-object>
    Supports both flat and nested {"type":"MESSAGE","message":{...}} formats.
    Older versions returned a plain JSON object/array.
    Returns a flat list of message dicts.
    """
    _MSG_KEYS = {"topic", "offset", "content", "partition", "timestamp",
                 "value", "key", "contentSize"}

    # SSE: at least one line starts with "data:"
    if any(line.strip().startswith("data:") for line in raw.splitlines()):
        msgs = []
        for line in raw.splitlines():
            line = line.strip()
            if not line.startswith("data:"):
                continue
            payload = line[len("data:"):].strip()
            if not payload:
                continue
            try:
                obj = _json.loads(payload)
            except _json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                if isinstance(obj, list):
                    msgs.extend(obj)
                continue

            # Format A: {"type":"MESSAGE", "message": {...}}
            if obj.get("type") == "MESSAGE" and isinstance(obj.get("message"), dict):
                msgs.append(obj["message"])
            # Format B: flat message — has offset/content/topic at top level
            elif _MSG_KEYS & set(obj.keys()):
                msgs.append(obj)
            # Format C: {"messages": [...]}
            elif "messages" in obj:
                msgs.extend(obj["messages"])
        return msgs

    # Older plain-JSON response
    try:
        data = _json.loads(raw)
    except _json.JSONDecodeError:
        preview = raw[:300]
        raise ValueError(
            f"Kafka UI did not return JSON or SSE.\nResponse preview:\n{preview}"
        )
    if isinstance(data, dict):
        return data.get("messages", data.get("data", []))
    if isinstance(data, list):
        return data
    return []




SAMPLE_LOCATIONS = {
    "PCAP":     "/data5/sample/pcap",
    "LUDR":     "/data5/sample/ludr",
    "SMS":      "/data5/sample/sms",
    "Voice":    "/data5/sample/voice",
    "CellID":   "/data5/sample/cellid",
    "SDR":      "/data5/sample/sdr",
    "CDR-IPDR": "/data5/sample/cdr_ipdr",
}


# ================================================================
# UTILITIES
# ================================================================
def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return DEFAULT_CFG.copy()

def save_config(cfg):
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(cfg, f, indent=4)
    except Exception as e:
        LOG.log("Config", f"Failed to save config: {e}", "ERROR")

def mkdirs_sftp(sftp, path):
    p = ""
    for d in path.strip("/").split("/"):
        p += "/" + d
        try:
            sftp.listdir(p)
        except IOError:
            sftp.mkdir(p)

def _add_tooltip(widget, text):
    """Show a small tooltip popup when the mouse hovers over widget."""
    tip = [None]

    def _show(event):
        if tip[0] and tip[0].winfo_exists():
            return
        tw = tk.Toplevel(widget)
        tw.wm_overrideredirect(True)
        tw.wm_attributes("-topmost", True)
        tk.Label(tw, text=text, background="#1e293b", foreground="#f1f5f9",
                 font=(_UI_FONT, 9), padx=8, pady=5,
                 relief="flat").pack()
        x = event.x_root + 12
        y = event.y_root + 16
        tw.wm_geometry(f"+{x}+{y}")
        tip[0] = tw

    def _hide(event):
        if tip[0] and tip[0].winfo_exists():
            tip[0].destroy()
        tip[0] = None

    widget.bind("<Enter>", _show, add="+")
    widget.bind("<Leave>", _hide, add="+")
    widget.bind("<ButtonPress>", _hide, add="+")


def _bind_mousewheel(widget, canvas):
    """Bind mouse wheel on widget so it scrolls the given canvas."""
    def _on_wheel(event):
        if not canvas.winfo_exists():
            return
        # Windows/macOS give delta; Linux gives Button-4/5
        if event.num == 4:
            canvas.yview_scroll(-1, "units")
        elif event.num == 5:
            canvas.yview_scroll(1, "units")
        else:
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
    widget.bind("<MouseWheel>", _on_wheel)
    widget.bind("<Button-4>",   _on_wheel)
    widget.bind("<Button-5>",   _on_wheel)


# ================================================================
# GLOBAL CONNECTION MANAGER
# ================================================================
class ConnectionManager:
    """
    Keeps one persistent SSH+SFTP session per server key.

    Key design decisions that prevent false disconnects
    ───────────────────────────────────────────────────
    • SSH-level keepalives (transport.set_keepalive) are sent every 20 s
      at the protocol layer — this keeps NAT/firewall sessions alive even
      when no data is flowing and is far more reliable than our own ping.
    • The liveness check is done OUTSIDE the lock so a slow network round-
      trip never blocks other threads or triggers a spurious failure.
    • exec_command stdout is always drained so paramiko's internal buffer
      never stalls future commands.
    • A "connecting" flag prevents multiple reconnect threads racing each
      other when the keepalive fires while a reconnect is already in flight.
    • Connect timeout is 20 s — generous enough for slow/busy servers.
    """

    _PING_INTERVAL  = 60    # seconds between liveness checks
    _CONNECT_TIMEOUT = 20   # seconds for initial SSH handshake
    _SSH_KEEPALIVE   = 20   # seconds between SSH-level keepalive packets

    def __init__(self):
        self._lock       = threading.Lock()
        self._conns      = {}   # key → {ip, pwd, ssh, sftp, state, connecting}
        self._next_retry = {}   # key → float timestamp of next retry attempt
        self._running    = True
        self._start_keepalive()

    # ── public API ───────────────────────────────────────────────
    def register(self, key, ip, pwd):
        """Register or re-register credentials and trigger a connection."""
        with self._lock:
            existing = self._conns.get(key)
            # Already connected with same creds — nothing to do
            if (existing
                    and existing["state"] == "connected"
                    and existing["ip"]  == ip
                    and existing["pwd"] == pwd):
                return
            # Close any old session
            if existing:
                self._close_entry(existing)
            self._conns[key] = {
                "ip": ip, "pwd": pwd,
                "ssh": None, "sftp": None,
                "state": "pending", "connecting": False,
            }
        self._spawn_connect(key)

    def state(self, key):
        with self._lock:
            return self._conns.get(key, {}).get("state", "pending")

    def retry_countdown(self, key):
        """Seconds until next retry attempt, or None if not applicable."""
        nr = self._next_retry.get(key)
        if nr is None:
            return None
        remaining = int(nr - time.time())
        return max(0, remaining)

    def get_sftp(self, key):
        with self._lock:
            e = self._conns.get(key)
            return e["sftp"] if e and e["state"] == "connected" else None

    def get_ssh(self, key):
        with self._lock:
            e = self._conns.get(key)
            return e["ssh"] if e and e["state"] == "connected" else None

    def disconnect_all(self):
        self._running = False
        with self._lock:
            for e in self._conns.values():
                self._close_entry(e)

    # ── internal ─────────────────────────────────────────────────
    def _spawn_connect(self, key):
        """Start a connect thread only if one is not already running."""
        with self._lock:
            e = self._conns.get(key)
            if not e or e.get("connecting"):
                return
            e["connecting"] = True
        threading.Thread(target=self._connect, args=(key,), daemon=True).start()

    def _connect(self, key):
        with self._lock:
            e = self._conns.get(key)
            if not e:
                return
            ip, pwd = e["ip"], e["pwd"]
        ssh = sftp = None
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                ip, username="root", password=pwd,
                timeout=self._CONNECT_TIMEOUT,
                banner_timeout=self._CONNECT_TIMEOUT,
                auth_timeout=self._CONNECT_TIMEOUT,
            )
            # Enable SSH-level keepalives so the server never idles us out
            transport = ssh.get_transport()
            if transport:
                transport.set_keepalive(self._SSH_KEEPALIVE)
            sftp = ssh.open_sftp()
            with self._lock:
                e = self._conns.get(key)
                if e:
                    e["ssh"]        = ssh
                    e["sftp"]       = sftp
                    e["state"]      = "connected"
                    e["connecting"] = False
        except Exception:
            # Clean up any partially-opened session
            for obj in (sftp, ssh):
                try:
                    if obj: obj.close()
                except Exception:
                    pass
            with self._lock:
                e = self._conns.get(key)
                if e:
                    e["state"]      = "failed"
                    e["connecting"] = False
                    self._next_retry[key] = time.time() + self._PING_INTERVAL

    def _is_alive(self, ssh):
        """
        Check liveness WITHOUT holding the lock.
        Returns True if the connection is still up.
        Drains stdout so paramiko's buffer never stalls.
        """
        try:
            transport = ssh.get_transport()
            if transport is None or not transport.is_active():
                return False
            # Send a cheap command and fully drain its output
            _, stdout, _ = ssh.exec_command("echo ok", timeout=10)
            stdout.read()   # drain — critical, prevents buffer stall
            return True
        except Exception:
            return False

    def _keepalive_check(self, key):
        """Run a liveness check for one key (called from keepalive thread)."""
        # Grab a snapshot of the ssh object without holding the lock
        with self._lock:
            e = self._conns.get(key)
            if not e or e["state"] != "connected":
                return
            ssh = e["ssh"]

        # Liveness check outside the lock — slow round-trips won't block anything
        alive = self._is_alive(ssh)

        if alive:
            return  # All good — SSH-level keepalives will maintain the session

        # Connection really did drop — mark and reconnect
        with self._lock:
            e = self._conns.get(key)
            if not e:
                return
            self._close_entry(e)
            e["state"] = "pending"
        self._spawn_connect(key)

    def _close_entry(self, e):
        for obj in (e.get("sftp"), e.get("ssh")):
            try:
                if obj: obj.close()
            except Exception:
                pass
        e["ssh"] = e["sftp"] = None

    def _start_keepalive(self):
        def _loop():
            while self._running:
                time.sleep(self._PING_INTERVAL)
                if not self._running:
                    break
                with self._lock:
                    keys = list(self._conns.keys())
                for key in keys:
                    try:
                        self._keepalive_check(key)
                    except Exception:
                        pass
        threading.Thread(target=_loop, daemon=True).start()


CONN = ConnectionManager()


# ================================================================
# LOGGER
# ================================================================
class Logger:
    def __init__(self):
        self._entries   = []
        self._callbacks = []

    def log(self, action, detail="", level="INFO"):
        entry = {"ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                 "level": level, "action": action, "detail": detail}
        self._entries.append(entry)
        for cb in self._callbacks:
            try:
                cb(entry)
            except Exception:
                pass

    def subscribe(self, cb):
        self._callbacks.append(cb)

    def all(self):
        return list(self._entries)


LOG = Logger()




# ================================================================
# CUSTOM DIALOG
# ================================================================
class Dialog(tk.Toplevel):
    ICONS = {"info": "ℹ️", "success": "✅", "error": "❌",
             "warning": "⚠️", "confirm": "❓"}
    BG      = "#161b22"
    BORDER  = "#30363d"
    PRIMARY = "#0891b2"
    SUCCESS = "#67e8f9"
    ERROR   = "#f87171"
    TEXT    = "#e6edf3"
    MUTED   = "#8b949e"

    def __init__(self, parent, title, message, kind="info",
                 on_confirm=None, on_cancel=None):
        super().__init__(parent)
        self.result = False
        self.overrideredirect(True)
        self.configure(bg=self.BORDER)
        self.resizable(False, False)
        self.grab_set()

        outer = tk.Frame(self, bg=self.BORDER, padx=2, pady=2)
        outer.pack(fill="both", expand=True)
        inner = tk.Frame(outer, bg=self.BG)
        inner.pack(fill="both", expand=True)

        title_bar = tk.Frame(inner, bg=self.PRIMARY, height=36)
        title_bar.pack(fill="x")
        title_bar.pack_propagate(False)
        icon = self.ICONS.get(kind, "ℹ️")
        tk.Label(title_bar, text=f"  {icon}  {title}", bg=self.PRIMARY, fg=self.TEXT,
                 font=(_UI_FONT, 11, "bold")).pack(side="left", padx=10)

        msg_frame = tk.Frame(inner, bg=self.BG, padx=30, pady=20)
        msg_frame.pack(fill="both", expand=True)
        tk.Label(msg_frame, text=message, bg=self.BG, fg=self.TEXT,
                 font=(_UI_FONT, 11), wraplength=380, justify="left").pack()

        btn_row = tk.Frame(inner, bg=self.BG, pady=15)
        btn_row.pack()

        if kind == "confirm":
            tk.Button(btn_row, text="  Cancel  ", bg="#333", fg=self.TEXT,
                      relief="flat", font=(_UI_FONT, 10, "bold"),
                      activebackground="#444", padx=10, pady=6,
                      command=self._cancel).pack(side="left", padx=8)
            tk.Button(btn_row, text="  Confirm  ", bg=self.PRIMARY, fg=self.TEXT,
                      relief="flat", font=(_UI_FONT, 10, "bold"),
                      activebackground=self.BORDER, padx=10, pady=6,
                      command=self._confirm).pack(side="left", padx=8)
        else:
            btn_bg = self.SUCCESS if kind == "success" else \
                     self.ERROR   if kind == "error"   else self.PRIMARY
            tk.Button(btn_row, text="    OK    ", bg=btn_bg, fg=self.TEXT,
                      relief="flat", font=(_UI_FONT, 10, "bold"),
                      activebackground=self.BORDER, padx=10, pady=6,
                      command=self._ok).pack()

        self._on_confirm = on_confirm
        self._on_cancel  = on_cancel

        self.update_idletasks()
        pw, ph = parent.winfo_width(), parent.winfo_height()
        px, py = parent.winfo_rootx(), parent.winfo_rooty()
        dw, dh = self.winfo_reqwidth(), self.winfo_reqheight()
        self.geometry(f"+{px + (pw - dw)//2}+{py + (ph - dh)//2}")

    def _ok(self):      self.result = True;  self.destroy()
    def _confirm(self):
        self.result = True
        if self._on_confirm: self._on_confirm()
        self.destroy()
    def _cancel(self):
        self.result = False
        if self._on_cancel: self._on_cancel()
        self.destroy()


def show_dialog(parent, title, msg, kind="info"):
    d = Dialog(parent, title, msg, kind)
    parent.wait_window(d)
    return d.result


# ================================================================
# STATUS DOT
# ================================================================
class StatusDot(tk.Canvas):
    PENDING   = "#6e7681"
    CONNECTED = "#67e8f9"
    FAILED    = "#f87171"

    def __init__(self, parent, bg, **kw):
        super().__init__(parent, width=12, height=12, bg=bg,
                         highlightthickness=0, **kw)
        self._oval = self.create_oval(2, 2, 10, 10,
                                      fill=self.PENDING, outline=self.PENDING)

    def set_state(self, state):
        color = {"pending": self.PENDING, "connected": self.CONNECTED,
                 "failed": self.FAILED}.get(state, self.PENDING)
        self.itemconfig(self._oval, fill=color, outline=color)



# ================================================================
# UTILITIES — MAP SERVER
# ================================================================
def _find_free_port():
    """Return a free TCP port on localhost."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ================================================================
# SMS PDU ENGINE  (embedded — no external dependencies)
# ================================================================
def _sms_encode_number_to_da(number: str):
    """
    Encode a phone number into PDU Destination Address bytes.
    Returns (da_len_byte, da_bytes) where da_bytes includes Type-of-Address.
    Handles international (+xx), 11-digit national, and short sender names.
    """
    digits = number.lstrip('+')
    digits = ''.join(c for c in digits if c.isdigit())

    if not digits:
        # Non-numeric sender (alphanumeric) — fall back to hardcoded DA
        return b'\x10', b'\xD0\x48\xA2\x71\x08\x12\x86\xDD\x6B'

    # Type-of-Address: 0x91 = international, 0x81 = national/unknown
    toa = b'\x91' if (number.startswith('+') or len(digits) >= 11) else b'\x81'

    # Semi-octet encoding: pad to even length with 'F', swap nibbles
    padded = digits if len(digits) % 2 == 0 else digits + 'F'
    semi   = bytes(
        int(padded[i+1], 16) << 4 | int(padded[i], 16)
        for i in range(0, len(padded), 2)
    )
    return bytes([len(digits)]), toa + semi


def _sms_text_to_pdu(text: str, ref: int = 0, to_number: str = None):
    """
    Encode text → PDU format (UTF-16-BE, multi-part UDH).
    If to_number is provided, builds a proper DA from the phone number.
    Returns list of (hex_pdu_str, byte_length).
    """
    SCA = b'\x00'
    FO  = b'\x51'
    MR  = b'\x00'
    PID = b'\x00'
    DCS = b'\x08'   # UTF-16-BE
    VP  = b'\x0B'

    if to_number:
        DA_LEN, DA = _sms_encode_number_to_da(to_number)
    else:
        DA_LEN = b'\x10'
        DA     = b'\xD0\x48\xA2\x71\x08\x12\x86\xDD\x6B'

    encoded = text.encode('utf-16-be')
    # 132 bytes = 66 UTF-16 chars per part
    chunks = [encoded[i:i+132] for i in range(0, len(encoded), 132)]
    total  = len(chunks)
    result = []

    for seq, chunk in enumerate(chunks, 1):
        udh    = b'\x05\x00\x03' + bytes([ref & 0xFF, total, seq])
        ud     = udh + chunk
        header = SCA + FO + MR + DA_LEN + DA + PID + DCS + VP + bytes([len(ud)])
        pdu    = header + ud
        result.append((pdu.hex().upper(), len(pdu)))

    return result


def _sms_create_iri_file(output_dir: str, sender: str, sms_text: str,
                          pdu_hex: str, call_id: int, seq_num: int,
                          liid: str, target_number: str,
                          imei: str, imsi: str, msisdn: str,
                          timestamp: str, network_id: str,
                          call_direction: str,
                          cell_id: str = "") -> str:
    """
    Write a single IRI .txt file.
    LIID is always equal to MSISDN — enforced by caller.
    Returns the full file path.
    """
    # Strip SCA length byte (first 2 hex chars)
    sms_sub = pdu_hex[2:]

    # Truncate display text at 80 chars (not bytes)
    display = sms_text[:80] + ("…" if len(sms_text) > 80 else "")

    # Directionality sets From / To
    if call_direction.lower() == "incoming":
        from_num = sender
        to_num   = target_number
    else:
        from_num = target_number
        to_num   = sender

    content = (
        f"CDR Start\n"
        f"CDRType : N\n"
        f"IRIRecordType : REPORT-RECORD\n"
        f"RecordType : SMS Message\n"
        f"IRIVersion : 31\n"
        f"LIID : {liid}\n"
        f"CallId : {call_id}\n"
        f"TimeStamp : {timestamp}\n"
        f"CallDirection : {call_direction}\n"
        f"NetworkId : {network_id}\n"
        f"CellId : {cell_id}\n"
        f"IMEI : {imei}\n"
        f"IMSI : {imsi}\n"
        f"MSISDN : {msisdn}\n"
        f"Target Number : {target_number}\n"
        f"From : {from_num}\n"
        f"To : {to_num}\n"
        f"Type : SMS\n"
        f"SMSSubMessage : {sms_sub}\n"
        f"SMS :In: {display}\n"
        f"CDR End\n"
    )

    filename = f"testcase_{call_id - 20320639}_pdu_{seq_num}.txt"
    filepath = os.path.join(output_dir, filename)
    os.makedirs(output_dir, exist_ok=True)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return filepath


# ================================================================
# LBS / LUDR CDR ENGINE  (embedded — no external dependencies)
# ================================================================
def _lbs_create_cdr_file(output_dir: str, call_id: int, seq_num: int,
                          timestamp: str, last_activity: str,
                          imei: str, imsi: str, msisdn: str,
                          target_number: str, event: str,
                          latitude: str, longitude: str) -> str:
    """
    Write a single LBS/LUDR CDR .txt file.
    Mirrors the format:
        CDR Start
        CDRType : Z
        IRIRecordType : Report-Record
        CallId : <call_id>
        TimeStamp : <timestamp>
        LastActivity : <last_activity>
        IMEI : <imei>
        IMSI : <imsi>
        MSISDN : <msisdn>
        Target Number : <target_number>
        CFEvent : location-Lat./Long.
        Event : <event>
        Latitude : <latitude>
        Longitude : <longitude>
        CDR End
    """
    content = (
        f"CDR Start\n"
        f"CDRType : Z\n"
        f"IRIRecordType : Report-Record\n"
        f"CallId : {call_id}\n"
        f"TimeStamp : {timestamp}\n"
        f"LastActivity : {last_activity}\n"
        f"IMEI : {imei}\n"
        f"IMSI : {imsi}\n"
        f"MSISDN : {msisdn}\n"
        f"Target Number : {target_number}\n"
        f"CFEvent : location-Lat./Long.\n"
        f"Event : {event}\n"
        f"Latitude : {latitude}\n"
        f"Longitude : {longitude}\n"
        f"CDR End\n"
    )

    filename = f"lbs_{call_id}_{seq_num}.txt"
    filepath = os.path.join(output_dir, filename)
    os.makedirs(output_dir, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    return filepath


# ================================================================
# VOICE CALL CDR ENGINE  (embedded — no external dependencies)
# ================================================================

def _voice_make_hi2_begin(liid, call_id, timestamp, call_direction,
                           network_id, imei, imsi, msisdn,
                           target_number, calling_number, called_number,
                           nat_ip, access_type):
    """Generate HI2 BEGIN-RECORD (INVITE) content."""
    return (
        f"CDR Start\n"
        f"CDRType : N\n"
        f"IRIRecordType : BEGIN-RECORD\n"
        f"RecordType : INVITE\n"
        f"IRIVersion : 31\n"
        f"LIID : {liid}\n"
        f"CallId : {call_id}\n"
        f"TimeStamp : {timestamp}\n"
        f"CallDirection : {call_direction}\n"
        f"NetworkId : {network_id}\n"
        f"IMEI : {imei}\n"
        f"IMSI : {imsi}\n"
        f"MSISDN : {msisdn}\n"
        f"Target Number : {target_number}\n"
        f"CallingNumber : {calling_number}\n"
        f"CalledNumber : {called_number}\n"
        f"From : {calling_number}\n"
        f"To : {called_number}\n"
        f"NAT IP : {nat_ip}\n"
        f"Access Type : {access_type}\n"
        f"Type : Voice\n"
        f"CDR End\n"
    )


def _voice_make_hi2_end(liid, call_id, timestamp_end, duration,
                         network_id, imei, imsi, msisdn,
                         target_number, calling_number, called_number,
                         nat_ip, access_type,
                         release_reason="Normal call clearing"):
    """Generate HI2 END-RECORD (BYE) content."""
    return (
        f"CDR Start\n"
        f"CDRType : N\n"
        f"IRIRecordType : END-RECORD\n"
        f"RecordType : BYE\n"
        f"IRIVersion : 31\n"
        f"LIID : {liid}\n"
        f"CallId : {call_id}\n"
        f"TimeStamp : {timestamp_end}\n"
        f"Duration : {duration}\n"
        f"NetworkId : {network_id}\n"
        f"IMEI : {imei}\n"
        f"IMSI : {imsi}\n"
        f"MSISDN : {msisdn}\n"
        f"Target Number : {target_number}\n"
        f"From : {calling_number}\n"
        f"To : {called_number}\n"
        f"NAT IP : {nat_ip}\n"
        f"Access Type : {access_type}\n"
        f"Type : Voice\n"
        f"Release Reason : {release_reason}\n"
        f"CDR End\n"
    )


def _voice_make_hi2_conf_begin(liid, call_id, timestamp, call_direction,
                               network_id, imei, imsi, msisdn,
                               target_number, calling_number,
                               cell_id, target_ip, access_type,
                               conf_numbers):
    """Generate HI2 BEGIN-RECORD for a Conference call."""
    return (
        f"CDR Start\n"
        f"CDRType : A\n"
        f"IRIRecordType : BEGIN-RECORD\n"
        f"RecordType : INVITE\n"
        f"IRIVersion : 1\n"
        f"LIID : {liid}\n"
        f"CallId : {call_id}\n"
        f"TimeStamp : {timestamp}\n"
        f"Duration : 00:00:00\n"
        f"CallDirection : {call_direction}\n"
        f"CellId : {cell_id}\n"
        f"IMEI : {imei}\n"
        f"IMSI : {imsi}\n"
        f"MSISDN : {msisdn}\n"
        f"Target Number : {target_number}\n"
        f"CallingNumber : {calling_number}\n"
        f"CalledNumber : CONF\n"
        f"From : {calling_number}\n"
        f"To : CONF\n"
        f"Conference Numbers : {conf_numbers}\n"
        f"Target IP : {target_ip}\n"
        f"NetworkID : {network_id}\n"
        f"Access Type : {access_type}\n"
        f"Type : Conference\n"
        f"CDR End\n"
    )


def _voice_make_hi2_conf_continue(liid, call_id, timestamp, duration,
                                   network_id, imei, imsi, msisdn,
                                   target_number, calling_number,
                                   cell_id, target_ip, conf_numbers):
    """Generate HI2 CONTINUE-RECORD (SIP 200 OK) for a Conference call."""
    return (
        f"CDR Start\n"
        f"CDRType : A\n"
        f"IRIRecordType : CONTINUE-RECORD\n"
        f"RecordType : SIP/2.0 200 OK\n"
        f"IRIVersion : 1\n"
        f"LIID : {liid}\n"
        f"CallId : {call_id}\n"
        f"TimeStamp : {timestamp}\n"
        f"Duration : {duration}\n"
        f"CellId : {cell_id}\n"
        f"IMEI : {imei}\n"
        f"IMSI : {imsi}\n"
        f"MSISDN : {msisdn}\n"
        f"Target Number : {target_number}\n"
        f"From : {calling_number}\n"
        f"To : CONF\n"
        f"Conference Numbers : {conf_numbers}\n"
        f"Target IP : {target_ip}\n"
        f"NetworkID : {network_id}\n"
        f"Type : Conference\n"
        f"CDR End\n"
    )


def _voice_make_hi2_conf_end(liid, call_id, timestamp_end, duration,
                              network_id, imei, imsi, msisdn,
                              target_number, calling_number,
                              cell_id, target_ip, access_type,
                              conf_numbers):
    """Generate HI2 END-RECORD (BYE) for a Conference call."""
    return (
        f"CDR Start\n"
        f"CDRType : A\n"
        f"IRIRecordType : END-RECORD\n"
        f"RecordType : BYE\n"
        f"IRIVersion : 1\n"
        f"LIID : {liid}\n"
        f"CallId : {call_id}\n"
        f"TimeStamp : {timestamp_end}\n"
        f"Duration : {duration}\n"
        f"CallDirection : Outgoing\n"
        f"CellId : {cell_id}\n"
        f"IMEI : {imei}\n"
        f"IMSI : {imsi}\n"
        f"MSISDN : {msisdn}\n"
        f"Target Number : {target_number}\n"
        f"From : {calling_number}\n"
        f"To : CONF\n"
        f"Conference Numbers : {conf_numbers}\n"
        f"Target IP : {target_ip}\n"
        f"NetworkID : {network_id}\n"
        f"Access Type : {access_type}\n"
        f"Type : Conference\n"
        f"CDR End\n"
    )


def _voice_generate_conf_call(output_dir, call_id, liid, msisdn,
                               target_number, calling_number,
                               conf_numbers, timestamp_start,
                               duration_secs, duration_fmt,
                               network_id, imei, imsi,
                               cell_id, target_ip, access_type,
                               call_direction,
                               wav_source_path="",
                               call_name_prefix="ConferenceCall"):
    """
    Create a conference call folder:
        output_dir/
          Hi2/
            Z_..._Begin.txt     (BEGIN-RECORD)
            Z_..._Continue.txt  (CONTINUE-RECORD)
            Z_..._End.txt       (END-RECORD)
          Hi3/
            <prefix>_a.wav
            <prefix>_b.wav
            uae_cs_..._call_data_record.txt
    """
    import shutil
    from datetime import datetime, timedelta

    hi2_dir = os.path.join(output_dir, "Hi2")
    hi3_dir = os.path.join(output_dir, "Hi3")
    os.makedirs(hi2_dir, exist_ok=True)
    os.makedirs(hi3_dir, exist_ok=True)

    timestamp_end = _voice_calc_end_time(timestamp_start, duration_secs)

    # Continue timestamp = ~halfway through the call
    ts_cont = _voice_calc_end_time(
        timestamp_start, max(1, duration_secs - 20))

    warnings = []

    # ── Begin ──────────────────────────────────────────────────
    begin_fname = _voice_make_hi2_filename(
        timestamp_start, calling_number, call_id, "O", "Begin")
    begin_content = _voice_make_hi2_conf_begin(
        liid=liid, call_id=call_id, timestamp=timestamp_start,
        call_direction=call_direction, network_id=network_id,
        imei=imei, imsi=imsi, msisdn=msisdn,
        target_number=target_number, calling_number=calling_number,
        cell_id=cell_id, target_ip=target_ip, access_type=access_type,
        conf_numbers=conf_numbers)
    with open(os.path.join(hi2_dir, begin_fname), "w",
              encoding="utf-8") as f:
        f.write(begin_content)

    # ── Continue ───────────────────────────────────────────────
    cont_fname = _voice_make_hi2_filename(
        timestamp_start, calling_number, call_id, "C", "Begin")
    # Use _C_ in filename for continue (matches sample)
    cont_fname = cont_fname.replace("_O_", "_C_").replace(
        "_I_", "_C_").replace("_Begin.txt", "_Begin.txt")
    cont_content = _voice_make_hi2_conf_continue(
        liid=liid, call_id=call_id, timestamp=ts_cont,
        duration=duration_fmt, network_id=network_id,
        imei=imei, imsi=imsi, msisdn=msisdn,
        target_number=target_number, calling_number=calling_number,
        cell_id=cell_id, target_ip=target_ip,
        conf_numbers=conf_numbers)
    with open(os.path.join(hi2_dir, cont_fname), "w",
              encoding="utf-8") as f:
        f.write(cont_content)

    # ── End ────────────────────────────────────────────────────
    end_fname = _voice_make_hi2_filename(
        timestamp_start, calling_number, call_id, "C", "End")
    end_fname = end_fname.replace("_I_", "_C_")
    end_content = _voice_make_hi2_conf_end(
        liid=liid, call_id=call_id, timestamp_end=timestamp_end,
        duration=duration_fmt, network_id=network_id,
        imei=imei, imsi=imsi, msisdn=msisdn,
        target_number=target_number, calling_number=calling_number,
        cell_id=cell_id, target_ip=target_ip, access_type=access_type,
        conf_numbers=conf_numbers)
    with open(os.path.join(hi2_dir, end_fname), "w",
              encoding="utf-8") as f:
        f.write(end_content)

    # ── Hi3 WAV ────────────────────────────────────────────────
    wav_a = os.path.join(hi3_dir, f"{call_name_prefix}_a.wav")
    wav_b = os.path.join(hi3_dir, f"{call_name_prefix}_b.wav")
    if wav_source_path and os.path.isfile(wav_source_path):
        shutil.copy2(wav_source_path, wav_a)
        shutil.copy2(wav_source_path, wav_b)
    else:
        warnings.append(
            "⚠️  No WAV file provided — silent placeholder created.")
        n = 8000 * max(1, duration_secs)
        import struct as _s
        hdr = _s.pack("<4sI4s4sIHHIIHH4sI",
            b"RIFF", 36+n, b"WAVE", b"fmt ", 16,
            6, 1, 8000, 8000, 1, 8, b"data", n)
        silent = hdr + b"\xd5" * n
        for p in (wav_a, wav_b):
            with open(p, "wb") as f: f.write(silent)

    # ── Hi3 CRI ────────────────────────────────────────────────
    cri_fname = _voice_make_hi3_cri_filename(timestamp_start)
    cri_content = _voice_make_hi3_cri(
        timestamp_start, timestamp_end, calling_number,
        call_id, liid)
    with open(os.path.join(hi3_dir, cri_fname), "w",
              encoding="utf-8") as f:
        f.write(cri_content)

    return {
        "begin_name":    begin_fname,
        "cont_name":     cont_fname,
        "end_name":      end_fname,
        "cri_name":      cri_fname,
        "warnings":      warnings,
    }


def _voice_make_hi3_cri(call_start, call_end, calling_party, call_id, liid):
    """Generate HI3 CRI (Call Record Information) content."""
    return (
        f"Call Record\n"
        f"Call Start: {call_start}\n"
        f"Call End: {call_end}\n"
        f"Calling Party: {calling_party}\n"
        f"CIN: {call_id}\n"
        f"LIID: {liid}\n"
    )


def _voice_make_wav(duration_secs: int) -> bytes:
    """
    Generate a minimal silent WAV file for the given duration.
    PCM 16-bit, 8000 Hz, mono — no external libraries needed.
    Returns raw bytes ready to write to a .wav file.
    """
    import struct
    sample_rate   = 8000
    num_channels  = 1
    bits_per_sample = 16
    num_samples   = sample_rate * max(1, duration_secs)
    data_size     = num_samples * num_channels * (bits_per_sample // 8)
    byte_rate     = sample_rate * num_channels * (bits_per_sample // 8)
    block_align   = num_channels * (bits_per_sample // 8)

    # RIFF header
    header = struct.pack(
        "<4sI4s"            # RIFF, chunk size, WAVE
        "4sIHHIIHH"         # fmt subchunk
        "4sI",              # data subchunk header
        b"RIFF",
        36 + data_size,
        b"WAVE",
        b"fmt ",
        16,                 # fmt chunk size
        1,                  # PCM = 1
        num_channels,
        sample_rate,
        byte_rate,
        block_align,
        bits_per_sample,
        b"data",
        data_size,
    )
    # Silent PCM samples (all zeros)
    samples = b"\x00" * data_size
    return header + samples


def _voice_parse_duration(duration_str: str):
    """
    Parse duration string HH:MM:SS or MM:SS or integer seconds.
    Returns (total_seconds, 'HH:MM:SS' formatted string).
    """
    duration_str = duration_str.strip()
    try:
        parts = list(map(int, duration_str.split(":")))
        if len(parts) == 3:
            total = parts[0] * 3600 + parts[1] * 60 + parts[2]
        elif len(parts) == 2:
            total = parts[0] * 60 + parts[1]
        else:
            total = int(duration_str)
    except Exception:
        total = 60   # default 1 minute
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    return total, f"{h:02d}:{m:02d}:{s:02d}"


def _voice_calc_end_time(start_str: str, duration_secs: int) -> str:
    """
    Add duration_secs to start_str timestamp.
    Supports formats: DD-MM-YYYY HH:MM:SS and YYYY-MM-DD HH:MM:SS
    Returns same format as input.
    """
    from datetime import datetime, timedelta
    for fmt in ("%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                "%d/%m/%Y %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            dt = datetime.strptime(start_str.strip(), fmt)
            end_dt = dt + timedelta(seconds=duration_secs)
            return end_dt.strftime(fmt)
        except ValueError:
            continue
    # Fallback — return as-is
    return start_str


def _voice_convert_to_alaw(input_path: str) -> tuple:
    """
    Convert ANY WAV to A-law encoded WAV (8 kHz, Mono, CCITT A-law / G.711).
    Pure-Python — no external libraries needed.
    Supports:
      - PCM 8/16/24/32-bit  (fmt=1)
      - A-law already        (fmt=6) — re-encodes to ensure 8kHz Mono
      - μ-law                (fmt=7) — decodes then re-encodes
      - Any sample rate, mono/stereo/multi-channel
    Uses struct to read the header so wave.open() is never called on input
    (wave.open rejects non-PCM formats and raises 'unknown format: 6').
    Returns (alaw_wav_bytes: bytes, info_str: str)
    """
    import struct as _struct, array as _array, io as _io, wave as _wave

    # ── Read WAV header and raw audio data with struct ─────────────
    with open(input_path, "rb") as f:
        header = f.read(44)
        rest   = f.read()

    if len(header) < 44 or header[:4] != b"RIFF" or header[8:12] != b"WAVE":
        raise ValueError("Not a valid WAV file")

    fmt_code    = _struct.unpack_from("<H", header, 20)[0]
    ch          = _struct.unpack_from("<H", header, 22)[0]
    fr          = _struct.unpack_from("<I", header, 24)[0]
    sw          = _struct.unpack_from("<H", header, 34)[0] // 8  # bytes per sample
    data_size   = _struct.unpack_from("<I", header, 40)[0]
    raw         = rest[:data_size]   # audio data only, strip any trailing chunks

    # ── Check if already A-law 8kHz Mono — skip conversion ───────
    if fmt_code == 6 and fr == 8000 and ch == 1:
        # Already the exact target format — read full file and return as-is
        with open(input_path, "rb") as f:
            original = f.read()
        info = (f"Input: A-law  1ch  8-bit  8000 Hz  ({len(raw)} bytes)  "
                f"— already A-law 8kHz Mono, copied without re-encoding")
        return original, info

    info = (f"Input: fmt={fmt_code}  {ch}ch  {sw*8}-bit  {fr} Hz  "
            f"({len(raw)} bytes)  — converting to A-law 8kHz Mono")
    _t = bytearray(65536)
    for _i in range(65536):
        _s = _i - 32768
        if _s < 0:
            _s, _sg = -_s, 0x00
        else:
            _sg = 0x80
        if _s > 32767: _s = 32767
        if _s < 256:
            _e, _m = 7, _s >> 4
        else:
            _e = 0
            for _b in range(7, 0, -1):
                if _s & (1 << (_b + 3)):
                    _e = _b; break
            _m = (_s >> (_e + 3)) & 0x0F
        _t[_i] = (_e << 4 | _m) ^ (_sg ^ 0x55)
    _AT = bytes(_t)

    # ── A-law decode table (for fmt=6 input) ──────────────────────
    def _alaw_decode_byte(b):
        b ^= 0x55
        sg = 1 if (b & 0x80) else -1
        b &= 0x7F
        e = (b >> 4) & 0x07
        m = b & 0x0F
        if e == 0:
            v = (m << 1) | 1
        else:
            v = ((m << 1) | 1 | 0x20) << (e - 1)
        return sg * v * 8   # scale to ~16-bit range

    # ── μ-law decode table (for fmt=7 input) ──────────────────────
    def _ulaw_decode_byte(b):
        b = ~b & 0xFF
        sg = 1 if (b & 0x80) == 0 else -1
        e = (b >> 4) & 0x07
        m = b & 0x0F
        v = ((m << 3) | 0x84) << e
        return sg * (v - 132)

    # ── Step 1: decode to 16-bit signed PCM ───────────────────────
    def _to16(data, fmt, sampwidth):
        if fmt == 6:   # A-law → 16-bit PCM
            a = _array.array("h",
                [max(-32768, min(32767, _alaw_decode_byte(b))) for b in data])
            return a.tobytes()
        if fmt == 7:   # μ-law → 16-bit PCM
            a = _array.array("h",
                [max(-32768, min(32767, _ulaw_decode_byte(b))) for b in data])
            return a.tobytes()
        # PCM
        if sampwidth == 1:
            a = _array.array("B", data)
            o = _array.array("h", [(s - 128) * 256 for s in a])
            return o.tobytes()
        if sampwidth == 2:
            return data
        if sampwidth == 3:
            n = len(data) // 3
            o = _array.array("h", [0] * n)
            for i in range(n):
                v = _struct.unpack_from("<i", data[i*3:i*3+3] + b"\x00")[0] >> 8
                o[i] = max(-32768, min(32767, v))
            return o.tobytes()
        if sampwidth == 4:
            a = _array.array("i", data)
            o = _array.array("h", [max(-32768, min(32767, s >> 16)) for s in a])
            return o.tobytes()
        return data

    # ── Step 2: multi-channel → mono ──────────────────────────────
    def _mono(pcm, channels):
        if channels == 1: return pcm
        a = _array.array("h", pcm)
        n = len(a) // channels
        o = _array.array("h",
            [sum(a[i*channels+c] for c in range(channels)) // channels
             for i in range(n)])
        return o.tobytes()

    # ── Step 3: resample to 8000 Hz (linear interpolation) ────────
    def _resamp(pcm, src, dst=8000):
        if src == dst: return pcm
        s  = _array.array("h", pcm)
        ni = len(s)
        no = int(round(ni * dst / src))
        o  = _array.array("h", [0] * no)
        r  = (ni - 1) / max(no - 1, 1)
        for i in range(no):
            p = i * r; lo = int(p); hi = min(lo + 1, ni - 1); f = p - lo
            o[i] = int(s[lo] * (1.0 - f) + s[hi] * f)
        return o.tobytes()

    # ── Step 4: encode 16-bit PCM → A-law ─────────────────────────
    def _enc(pcm):
        n = len(pcm) // 2
        o = bytearray(n)
        for i in range(n):
            o[i] = _AT[_struct.unpack_from("<H", pcm, i*2)[0]]
        return bytes(o)

    # ── Run pipeline ──────────────────────────────────────────────
    pcm = _to16(raw, fmt_code, sw)
    pcm = _mono(pcm, ch)
    pcm = _resamp(pcm, fr)
    aw  = _enc(pcm)

    # ── Write output WAV container, patch fmt_code to 6 (A-law) ───
    buf = _io.BytesIO()
    with _wave.open(buf, "wb") as w:
        w.setnchannels(1); w.setsampwidth(1); w.setframerate(8000)
        w.writeframes(aw)
    out = bytearray(buf.getvalue())
    _struct.pack_into("<H", out, 20, 6)   # patch PCM(1) → A-law(6)

    return bytes(out), f"{info}  →  A-law 8kHz Mono  ({len(out)} bytes)"



def _voice_make_hi2_filename(timestamp_start: str, calling: str,
                              call_id: int, called: str, suffix: str) -> str:
    """
    Build HI2 filename:
    Z_YYYYMMDD.HHMMSSmmm.<calling>_<callid>_I_<called>_<suffix>.txt
    e.g. Z_20250412.161005158.420603871245_13092979_I_5319405_Begin.txt
    """
    from datetime import datetime
    ts = timestamp_start.strip()
    dt = None
    for fmt in ("%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                "%d/%m/%Y %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            dt = datetime.strptime(ts, fmt)
            break
        except ValueError:
            continue
    if dt:
        date_part = dt.strftime("%Y%m%d")
        time_part = dt.strftime("%H%M%S") + "000"   # ms = 000 placeholder
    else:
        date_part = "00000000"
        time_part = "000000000"
    return f"Z_{date_part}.{time_part}.{calling}_{call_id}_I_{called}_{suffix}.txt"


def _voice_make_hi3_cri_filename(timestamp_start: str) -> str:
    """
    Build HI3 CRI filename:
    uae_cs_YYYYMMDD_HHMMSS_call_data_record.txt
    """
    from datetime import datetime
    ts = timestamp_start.strip()
    for fmt in ("%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                "%d/%m/%Y %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            dt = datetime.strptime(ts, fmt)
            return f"uae_cs_{dt.strftime('%Y%m%d_%H%M%S')}_call_data_record.txt"
        except ValueError:
            continue
    return "uae_cs_call_data_record.txt"


def _voice_generate_call(output_dir: str, call_id: int,
                          liid: str, msisdn: str, target_number: str,
                          calling_number: str, called_number: str,
                          timestamp_start: str, duration_secs: int,
                          duration_fmt: str,
                          network_id: str, imei: str, imsi: str,
                          nat_ip: str, access_type: str,
                          call_direction: str, release_reason: str,
                          wav_source_path: str = "",
                          call_name_prefix: str = "BanglaCall") -> dict:
    """
    Create one voice call folder:
        output_dir/
          Hi2/
            Z_<date>.<time>.<calling>_<callid>_I_<called>_Begin.txt
            Z_<date>.<time>.<calling>_<callid>_I_<called>_End.txt
          Hi3/
            <prefix>_a.wav          ← copy of uploaded A-law WAV
            <prefix>_b.wav          ← second copy
            uae_cs_<ts>_call_data_record.txt
    Returns dict with all created file paths + warnings.
    """
    hi2_dir = os.path.join(output_dir, "Hi2")
    hi3_dir = os.path.join(output_dir, "Hi3")
    os.makedirs(hi2_dir, exist_ok=True)
    os.makedirs(hi3_dir, exist_ok=True)

    timestamp_end = _voice_calc_end_time(timestamp_start, duration_secs)

    # Format timestamps for HI3 CRI (YYYY-MM-DD HH:MM:SS)
    from datetime import datetime
    hi3_start = timestamp_start
    hi3_end   = timestamp_end
    for fmt in ("%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            dt_s = datetime.strptime(timestamp_start.strip(), fmt)
            dt_e = datetime.strptime(timestamp_end.strip(), fmt)
            hi3_start = dt_s.strftime("%Y-%m-%d %H:%M:%S")
            hi3_end   = dt_e.strftime("%Y-%m-%d %H:%M:%S")
            break
        except ValueError:
            continue

    warnings = []

    # ── HI2 Begin ──────────────────────────────────────────────
    begin_fname = _voice_make_hi2_filename(
        timestamp_start, calling_number, call_id, called_number, "Begin")
    begin_content = _voice_make_hi2_begin(
        liid=liid, call_id=call_id, timestamp=timestamp_start,
        call_direction=call_direction, network_id=network_id,
        imei=imei, imsi=imsi, msisdn=msisdn, target_number=target_number,
        calling_number=calling_number, called_number=called_number,
        nat_ip=nat_ip, access_type=access_type,
    )
    begin_path = os.path.join(hi2_dir, begin_fname)
    with open(begin_path, "w", encoding="utf-8") as f:
        f.write(begin_content)

    # ── HI2 End ────────────────────────────────────────────────
    end_fname = _voice_make_hi2_filename(
        timestamp_start, calling_number, call_id, called_number, "End")
    end_content = _voice_make_hi2_end(
        liid=liid, call_id=call_id, timestamp_end=timestamp_end,
        duration=duration_fmt, network_id=network_id,
        imei=imei, imsi=imsi, msisdn=msisdn, target_number=target_number,
        calling_number=calling_number, called_number=called_number,
        nat_ip=nat_ip, access_type=access_type, release_reason=release_reason,
    )
    end_path = os.path.join(hi2_dir, end_fname)
    with open(end_path, "w", encoding="utf-8") as f:
        f.write(end_content)

    # ── HI3 WAV — copy source WAV as-is, two copies ───────────────
    import shutil
    wav_a = os.path.join(hi3_dir, f"{call_name_prefix}_a.wav")
    wav_b = os.path.join(hi3_dir, f"{call_name_prefix}_b.wav")

    if wav_source_path and os.path.isfile(wav_source_path):
        shutil.copy2(wav_source_path, wav_a)
        shutil.copy2(wav_source_path, wav_b)
        LOG.log("Upload", f"Voice: copied {os.path.basename(wav_source_path)} → {call_name_prefix}_a.wav + _b.wav")
    else:
        # No WAV supplied — write silent placeholder
        warnings.append(
            "⚠️  No WAV file provided — silent placeholder created.\n"
            "Select a WAV file and regenerate for real audio."
        )
        import struct as _struct
        n         = 8000 * max(1, duration_secs)
        data_size = n
        hdr = _struct.pack(
            "<4sI4s4sIHHIIHH4sI",
            b"RIFF", 36 + data_size, b"WAVE",
            b"fmt ", 16,
            6, 1, 8000, 8000, 1, 8,
            b"data", data_size,
        )
        silent_wav = hdr + b"\xd5" * data_size
        with open(wav_a, "wb") as f: f.write(silent_wav)
        with open(wav_b, "wb") as f: f.write(silent_wav)

    # ── HI3 CRI text ──────────────────────────────────────────
    cri_fname   = _voice_make_hi3_cri_filename(timestamp_start)
    cri_content = _voice_make_hi3_cri(
        call_start    = hi3_start,
        call_end      = hi3_end,
        calling_party = calling_number,
        call_id       = call_id,
        liid          = liid,
    )
    cri_path = os.path.join(hi3_dir, cri_fname)
    with open(cri_path, "w", encoding="utf-8") as f:
        f.write(cri_content)

    return {
        "folder":    output_dir,
        "begin":     begin_path,
        "begin_name":begin_fname,
        "end":       end_path,
        "end_name":  end_fname,
        "wav_a":     wav_a,
        "wav_b":     wav_b,
        "cri":       cri_path,
        "cri_name":  cri_fname,
        "ts_end":    timestamp_end,
        "dur_fmt":   duration_fmt,
        "warnings":  warnings,
    }


# ================================================================
# PCAP ENGINE  (pure Python — no scapy/dpkt/libpcap needed)
# ================================================================
import struct as _pstruct
import socket as _psocket

# ── PCAP global header (little-endian, linktype=1 = Ethernet) ──
_PCAP_GLOBAL_HDR = _pstruct.pack(
    "<IHHiIII",
    0xA1B2C3D4,   # magic
    2, 4,          # version
    0,             # UTC offset
    0,             # timestamp accuracy
    65535,         # snaplen
    1,             # linktype: Ethernet
)

def _pcap_ts(dt_str: str):
    """Parse timestamp string → (sec, usec)."""
    from datetime import datetime
    for fmt in ("%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S",
                "%d/%m/%Y %H:%M:%S"):
        try:
            dt  = datetime.strptime(dt_str.strip(), fmt)
            sec = int(dt.timestamp())
            return sec, 0
        except ValueError:
            continue
    import time as _t
    return int(_t.time()), 0


def _pcap_packet(data: bytes, sec: int, usec: int) -> bytes:
    """Wrap raw bytes in a pcap packet record."""
    n = len(data)
    return _pstruct.pack("<IIII", sec, usec, n, n) + data


def _eth_ip(src_ip, dst_ip, proto, payload):
    """
    Build Ethernet (fake MACs) + IPv4 header + payload.
    proto: 6=TCP, 17=UDP, 1=ICMP
    """
    # Ethernet header (14 bytes): dst_mac, src_mac, ethertype
    eth = b"\x00\x11\x22\x33\x44\x55" + b"\x66\x77\x88\x99\xaa\xbb" + b"\x08\x00"
    # IPv4 header (20 bytes)
    total_len = 20 + len(payload)
    src = _psocket.inet_aton(src_ip)
    dst = _psocket.inet_aton(dst_ip)
    ip = _pstruct.pack(
        "!BBHHHBBH4s4s",
        0x45, 0,           # version+IHL, DSCP
        total_len,
        0x1234,            # ID
        0x4000,            # flags+fragment offset (DF)
        64,                # TTL
        proto,
        0,                 # checksum (0 = skip)
        src, dst,
    )
    return eth + ip + payload


def _udp(sport, dport, payload):
    length = 8 + len(payload)
    hdr = _pstruct.pack("!HHHH", sport, dport, length, 0)
    return hdr + payload


def _tcp(sport, dport, seq, payload, flags=0x018):
    hdr = _pstruct.pack("!HHIIBBHHH",
        sport, dport, seq, 0,
        0x50,     # data offset = 5 * 4 = 20 bytes
        flags,    # PSH+ACK
        65535, 0, 0)
    return hdr + payload


def _icmp(icmp_type=8, code=0, payload=b"Hello"):
    hdr = _pstruct.pack("!BBHH", icmp_type, code, 0, 1)
    return hdr + payload


# ── SIP + RTP builder ──────────────────────────────────────────
def _pcap_voip(src_ip, dst_ip, src_port, dst_port,
               calling, called, call_id_str,
               ts_sec, duration_secs, imei="", msisdn="") -> bytes:
    """
    Generate a minimal SIP+RTP call PCAP:
      INVITE → 100 Trying → 200 OK → ACK
      RTP packets (1 per second for duration)
      BYE → 200 OK
    """
    pkts = []
    t    = ts_sec
    seq  = 1000
    rtp_seq = 0

    # Helper: add one UDP packet
    def add_udp(sip, dip, sp, dp, data, offset=0):
        nonlocal seq
        raw = _eth_ip(sip, dip, 17, _udp(sp, dp, data))
        pkts.append(_pcap_packet(raw, t + offset, 0))
        seq += 1

    via_branch = f"z9hG4bK-{call_id_str}"
    meta = f"X-IMEI: {imei}\r\nX-MSISDN: {msisdn}\r\n" if (imei or msisdn) else ""

    # INVITE
    invite = (
        f"INVITE sip:{called}@{dst_ip} SIP/2.0\r\n"
        f"Via: SIP/2.0/UDP {src_ip}:{src_port};branch={via_branch}\r\n"
        f"From: <sip:{calling}@{src_ip}>;tag=orig\r\n"
        f"To: <sip:{called}@{dst_ip}>\r\n"
        f"Call-ID: {call_id_str}@{src_ip}\r\n"
        f"CSeq: 1 INVITE\r\nContact: <sip:{calling}@{src_ip}:{src_port}>\r\n"
        f"Content-Type: application/sdp\r\nContent-Length: 0\r\n"
        f"{meta}\r\n"
    ).encode()
    add_udp(src_ip, dst_ip, src_port, dst_port, invite, 0)

    # 100 Trying
    trying = (
        f"SIP/2.0 100 Trying\r\n"
        f"Via: SIP/2.0/UDP {src_ip}:{src_port};branch={via_branch}\r\n"
        f"From: <sip:{calling}@{src_ip}>;tag=orig\r\n"
        f"To: <sip:{called}@{dst_ip}>\r\n"
        f"Call-ID: {call_id_str}@{src_ip}\r\nCSeq: 1 INVITE\r\n\r\n"
    ).encode()
    add_udp(dst_ip, src_ip, dst_port, src_port, trying, 0)

    # 200 OK
    ok = (
        f"SIP/2.0 200 OK\r\n"
        f"Via: SIP/2.0/UDP {src_ip}:{src_port};branch={via_branch}\r\n"
        f"From: <sip:{calling}@{src_ip}>;tag=orig\r\n"
        f"To: <sip:{called}@{dst_ip}>;tag=resp\r\n"
        f"Call-ID: {call_id_str}@{src_ip}\r\nCSeq: 1 INVITE\r\n\r\n"
    ).encode()
    add_udp(dst_ip, src_ip, dst_port, src_port, ok, 1)

    # ACK
    ack = (
        f"ACK sip:{called}@{dst_ip} SIP/2.0\r\n"
        f"Via: SIP/2.0/UDP {src_ip}:{src_port};branch={via_branch}ack\r\n"
        f"From: <sip:{calling}@{src_ip}>;tag=orig\r\n"
        f"To: <sip:{called}@{dst_ip}>;tag=resp\r\n"
        f"Call-ID: {call_id_str}@{src_ip}\r\nCSeq: 2 ACK\r\n\r\n"
    ).encode()
    add_udp(src_ip, dst_ip, src_port, dst_port, ack, 1)

    # RTP packets — one per second of duration (silence payload)
    rtp_port_s = src_port + 2
    rtp_port_d = dst_port + 2
    for s in range(2, 2 + max(1, duration_secs)):
        rtp_hdr = _pstruct.pack("!BBHII",
            0x80,            # V=2, P=0, X=0, CC=0
            0x00,            # M=0, PT=0 (μ-law)
            rtp_seq & 0xFFFF,
            s * 160,         # timestamp (8000 Hz, 20ms = 160 samples)
            0xCAFEBABE,      # SSRC
        )
        rtp_payload = b"\xd5" * 160   # silence
        raw = _eth_ip(src_ip, dst_ip, 17,
                      _udp(rtp_port_s, rtp_port_d, rtp_hdr + rtp_payload))
        pkts.append(_pcap_packet(raw, t + s, 0))
        rtp_seq += 1

    # BYE
    bye_offset = 2 + duration_secs
    bye = (
        f"BYE sip:{called}@{dst_ip} SIP/2.0\r\n"
        f"Via: SIP/2.0/UDP {src_ip}:{src_port};branch={via_branch}bye\r\n"
        f"From: <sip:{calling}@{src_ip}>;tag=orig\r\n"
        f"To: <sip:{called}@{dst_ip}>;tag=resp\r\n"
        f"Call-ID: {call_id_str}@{src_ip}\r\nCSeq: 3 BYE\r\n\r\n"
    ).encode()
    add_udp(src_ip, dst_ip, src_port, dst_port, bye, bye_offset)

    # 200 OK for BYE
    ok_bye = (
        f"SIP/2.0 200 OK\r\n"
        f"Via: SIP/2.0/UDP {src_ip}:{src_port};branch={via_branch}bye\r\n"
        f"From: <sip:{calling}@{src_ip}>;tag=orig\r\n"
        f"To: <sip:{called}@{dst_ip}>;tag=resp\r\n"
        f"Call-ID: {call_id_str}@{src_ip}\r\nCSeq: 3 BYE\r\n\r\n"
    ).encode()
    add_udp(dst_ip, src_ip, dst_port, src_port, ok_bye, bye_offset)

    return _PCAP_GLOBAL_HDR + b"".join(pkts)


def _pcap_http(src_ip, dst_ip, src_port, dst_port,
               method, url, payload_str, ts_sec,
               imei="", msisdn="") -> bytes:
    """Generate HTTP request + response PCAP."""
    pkts = []
    seq_c = 0x10000; seq_s = 0x20000

    def tcp_pkt(sip, dip, sp, dp, sq, data, flags=0x018):
        raw = _eth_ip(sip, dip, 6, _tcp(sp, dp, sq, data, flags))
        pkts.append(_pcap_packet(raw, ts_sec, 0))
        return sq + len(data)

    meta_hdr = f"X-IMEI: {imei}\r\nX-MSISDN: {msisdn}\r\n" if (imei or msisdn) else ""
    body = payload_str.encode() if payload_str else b""

    # SYN
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, b"", 0x002)
    # SYN-ACK
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s, b"", 0x012)
    # ACK
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, b"", 0x010)

    # HTTP Request
    req = (
        f"{method} {url} HTTP/1.1\r\n"
        f"Host: {dst_ip}\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n"
        f"{meta_hdr}\r\n"
    ).encode() + body
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, req)

    # HTTP Response
    resp_body = b"<html><body>OK</body></html>"
    resp = (
        f"HTTP/1.1 200 OK\r\n"
        f"Content-Length: {len(resp_body)}\r\nConnection: close\r\n\r\n"
    ).encode() + resp_body
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s, resp)

    # FIN
    tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, b"", 0x011)

    return _PCAP_GLOBAL_HDR + b"".join(pkts)


def _pcap_dns(src_ip, dst_ip, src_port, ts_sec,
              domain="example.com", imei="", msisdn="") -> bytes:
    """Generate DNS query + response PCAP."""
    pkts = []

    def dns_query(name):
        txid = 0x1234
        flags = 0x0100   # standard query
        qd = b""
        for part in name.encode().split(b"."):
            qd += bytes([len(part)]) + part
        qd += b"\x00\x00\x01\x00\x01"   # null, QTYPE=A, QCLASS=IN
        return _pstruct.pack("!HHHHHH", txid, flags, 1, 0, 0, 0) + qd

    def dns_response(name, ip):
        txid = 0x1234
        flags = 0x8180   # response, recursion available
        qd = b""
        for part in name.encode().split(b"."):
            qd += bytes([len(part)]) + part
        qd += b"\x00\x00\x01\x00\x01"
        ans = b"\xc0\x0c\x00\x01\x00\x01\x00\x00\x00\x3c\x00\x04"
        ans += _psocket.inet_aton(ip)
        return _pstruct.pack("!HHHHHH", txid, flags, 1, 1, 0, 0) + qd + ans

    q_data = dns_query(domain)
    raw_q  = _eth_ip(src_ip, dst_ip, 17, _udp(src_port, 53, q_data))
    pkts.append(_pcap_packet(raw_q, ts_sec, 0))

    r_data = dns_response(domain, dst_ip)
    raw_r  = _eth_ip(dst_ip, src_ip, 17, _udp(53, src_port, r_data))
    pkts.append(_pcap_packet(raw_r, ts_sec, 100000))

    return _PCAP_GLOBAL_HDR + b"".join(pkts)


def _pcap_raw(src_ip, dst_ip, src_port, dst_port,
              proto_str, payload_str, ts_sec,
              imei="", msisdn="") -> bytes:
    """Generate raw TCP/UDP/ICMP packet with custom payload."""
    payload = payload_str.encode("utf-8", errors="replace") if payload_str else b""
    if imei or msisdn:
        meta = f"IMEI={imei};MSISDN={msisdn};".encode()
        payload = meta + payload

    proto_map = {"TCP": 6, "UDP": 17, "ICMP": 1}
    proto_num = proto_map.get(proto_str.upper(), 17)

    if proto_num == 6:
        seg = _tcp(src_port, dst_port, 0x10000, payload)
    elif proto_num == 17:
        seg = _udp(src_port, dst_port, payload)
    else:
        seg = _icmp(payload=payload[:56] if payload else b"ping")

    raw = _eth_ip(src_ip, dst_ip, proto_num, seg)
    return _PCAP_GLOBAL_HDR + _pcap_packet(raw, ts_sec, 0)


def _pcap_write(path: str, data: bytes):
    """Write PCAP bytes to file."""
    with open(path, "wb") as f:
        f.write(data)


def _pcap_ftp(src_ip, dst_ip, src_port, dst_port,
              ftp_user, ftp_pass, ftp_file, payload_str, ts_sec,
              imei="", msisdn="") -> bytes:
    """Generate FTP control + data session PCAP (PORT mode)."""
    pkts = []; seq_c = 0x10000; seq_s = 0x20000

    def tcp_pkt(sip, dip, sp, dp, sq, data, flags=0x018):
        raw = _eth_ip(sip, dip, 6, _tcp(sp, dp, sq, data, flags))
        pkts.append(_pcap_packet(raw, ts_sec, len(pkts) * 1000))
        return sq + max(1, len(data))

    # Handshake
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, b"", 0x002)
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s, b"", 0x012)
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, b"", 0x010)
    # FTP banner
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s,
                    b"220 FTP Server ready\r\n")
    # USER
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c,
                    f"USER {ftp_user or 'anonymous'}\r\n".encode())
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s,
                    b"331 Password required\r\n")
    # PASS
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c,
                    f"PASS {ftp_pass or 'pass'}\r\n".encode())
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s,
                    b"230 Login successful\r\n")
    # RETR
    fn = ftp_file or "data.bin"
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c,
                    f"RETR {fn}\r\n".encode())
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s,
                    b"150 Opening data connection\r\n")
    # Data (simulate on same port for simplicity)
    body = (payload_str.encode() if payload_str else b"FTP-DATA:" + fn.encode()) + b"\r\n"
    if imei or msisdn:
        body = f"IMEI={imei};MSISDN={msisdn};".encode() + body
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s, body)
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s,
                    b"226 Transfer complete\r\n")
    # QUIT
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, b"QUIT\r\n")
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s, b"221 Goodbye\r\n")
    return _PCAP_GLOBAL_HDR + b"".join(pkts)


def _pcap_stun(src_ip, dst_ip, src_port, ts_sec,
               imei="", msisdn="") -> bytes:
    """Generate STUN Binding Request + Response PCAP."""
    pkts = []
    # STUN Binding Request (RFC 5389)
    txid = b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c"
    req  = _pstruct.pack("!HHI", 0x0001, 0, 0x2112A442) + txid
    if imei or msisdn:
        attr = f"IMEI={imei};MSISDN={msisdn}".encode()
        attr_hdr = _pstruct.pack("!HH", 0x8022, len(attr))
        pad = (4 - len(attr) % 4) % 4
        req = _pstruct.pack("!HHI", 0x0001, len(attr_hdr + attr) + pad,
                            0x2112A442) + txid + attr_hdr + attr + b"\x00" * pad

    raw_req = _eth_ip(src_ip, dst_ip, 17, _udp(src_port, 3478, req))
    pkts.append(_pcap_packet(raw_req, ts_sec, 0))

    # STUN Binding Success Response with XOR-MAPPED-ADDRESS
    ip_parts = list(map(int, dst_ip.split(".")))
    xor_ip   = bytes([p ^ ((0x2112A442 >> (8 * (3 - i))) & 0xFF)
                     for i, p in enumerate(ip_parts)])
    xor_port = (src_port ^ 0x2112) & 0xFFFF
    mapped   = _pstruct.pack("!HHH", 0x0020, 8, 0) + \
               _pstruct.pack("!HH", 0x0001, xor_port) + xor_ip
    resp = _pstruct.pack("!HHI", 0x0101, len(mapped), 0x2112A442) + txid + mapped
    raw_resp = _eth_ip(dst_ip, src_ip, 17, _udp(3478, src_port, resp))
    pkts.append(_pcap_packet(raw_resp, ts_sec, 50000))
    return _PCAP_GLOBAL_HDR + b"".join(pkts)


def _pcap_rtsp(src_ip, dst_ip, src_port, dst_port,
               stream_url, ts_sec, duration_secs, imei="", msisdn="") -> bytes:
    """Generate RTSP DESCRIBE→SETUP→PLAY→TEARDOWN + RTP packets."""
    pkts = []; seq_c = 0x10000; seq_s = 0x20000
    url = stream_url or f"rtsp://{dst_ip}:{dst_port}/stream"
    meta = f"X-IMEI: {imei}\r\nX-MSISDN: {msisdn}\r\n" if (imei or msisdn) else ""

    def tcp_pkt(sip, dip, sp, dp, sq, data, flags=0x018):
        raw = _eth_ip(sip, dip, 6, _tcp(sp, dp, sq, data, flags))
        pkts.append(_pcap_packet(raw, ts_sec, len(pkts) * 500))
        return sq + max(1, len(data))

    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, b"", 0x002)
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s, b"", 0x012)
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, b"", 0x010)

    for cseq, method, extra in [
        (1, "DESCRIBE",  "Accept: application/sdp\r\n"),
        (2, "SETUP",     f"Transport: RTP/AVP;unicast;client_port={src_port+2}-{src_port+3}\r\n"),
        (3, "PLAY",      "Range: npt=0.000-\r\n"),
        (4, "TEARDOWN",  ""),
    ]:
        req = (f"{method} {url} RTSP/1.0\r\n"
               f"CSeq: {cseq}\r\nSession: 12345678\r\n"
               f"{extra}{meta}\r\n").encode()
        seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, req)
        resp = (f"RTSP/1.0 200 OK\r\nCSeq: {cseq}\r\n"
                f"Session: 12345678\r\n\r\n").encode()
        seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s, resp)

    # RTP packets during PLAY
    for s in range(max(1, duration_secs)):
        rtp = _pstruct.pack("!BBHII", 0x80, 0x60, s, s * 90000, 0xDEADBEEF)
        raw = _eth_ip(src_ip, dst_ip, 17,
                      _udp(src_port + 2, dst_port + 2, rtp + b"\x00" * 160))
        pkts.append(_pcap_packet(raw, ts_sec + s, 0))
    return _PCAP_GLOBAL_HDR + b"".join(pkts)


def _pcap_smtp(src_ip, dst_ip, src_port, dst_port,
               mail_from, mail_to, subject, body_str, ts_sec,
               imei="", msisdn="") -> bytes:
    """Generate SMTP email send session PCAP."""
    pkts = []; seq_c = 0x10000; seq_s = 0x20000

    def tcp_pkt(sip, dip, sp, dp, sq, data, flags=0x018):
        raw = _eth_ip(sip, dip, 6, _tcp(sp, dp, sq, data, flags))
        pkts.append(_pcap_packet(raw, ts_sec, len(pkts) * 1000))
        return sq + max(1, len(data))

    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, b"", 0x002)
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s, b"", 0x012)
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, b"", 0x010)

    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s,
                    b"220 mail.example.com ESMTP\r\n")
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c,
                    f"EHLO {src_ip}\r\n".encode())
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s,
                    b"250-mail.example.com Hello\r\n250 OK\r\n")
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c,
                    f"MAIL FROM:<{mail_from or 'sender@example.com'}>\r\n".encode())
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s, b"250 OK\r\n")
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c,
                    f"RCPT TO:<{mail_to or 'recipient@example.com'}>\r\n".encode())
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s, b"250 OK\r\n")
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, b"DATA\r\n")
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s,
                    b"354 Start mail input\r\n")
    body  = body_str or "Test email body"
    if imei or msisdn:
        body = f"X-IMEI: {imei}\r\nX-MSISDN: {msisdn}\r\n" + body
    msg   = (f"Subject: {subject or 'Test'}\r\n"
             f"From: {mail_from or 'sender@example.com'}\r\n"
             f"To: {mail_to or 'recipient@example.com'}\r\n\r\n"
             f"{body}\r\n.\r\n").encode()
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, msg)
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s, b"250 OK\r\n")
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, b"QUIT\r\n")
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s, b"221 Bye\r\n")
    return _PCAP_GLOBAL_HDR + b"".join(pkts)


def _pcap_tls(src_ip, dst_ip, src_port, dst_port,
              sni, payload_str, ts_sec, imei="", msisdn="") -> bytes:
    """Generate TLS/HTTPS ClientHello + ServerHello + encrypted data PCAP."""
    pkts = []; seq_c = 0x10000; seq_s = 0x20000

    def tcp_pkt(sip, dip, sp, dp, sq, data, flags=0x018):
        raw = _eth_ip(sip, dip, 6, _tcp(sp, dp, sq, data, flags))
        pkts.append(_pcap_packet(raw, ts_sec, len(pkts) * 500))
        return sq + max(1, len(data))

    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, b"", 0x002)
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s, b"", 0x012)
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, b"", 0x010)

    # TLS ClientHello (simplified)
    sni_name   = (sni or "example.com").encode()
    sni_ext    = (_pstruct.pack("!HHH", 0, len(sni_name) + 3, len(sni_name)) +
                  b"\x00" + sni_name)
    sni_len    = _pstruct.pack("!HH", 0, len(sni_ext))
    random_    = b"\x00" * 32
    hello_body = b"\x03\x03" + random_ + b"\x00" + \
                 b"\x00\x04\xc0\x2b\xc0\x2f" + b"\x01\x00" + \
                 sni_len + sni_ext
    client_hello = (b"\x16\x03\x01" +
                    _pstruct.pack("!H", len(hello_body) + 4) +
                    b"\x01\x00" + _pstruct.pack("!H", len(hello_body)) +
                    hello_body)
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, client_hello)

    # TLS ServerHello (simplified)
    server_hello = b"\x16\x03\x03\x00\x31" + b"\x02\x00\x00\x2d" + b"\x03\x03" + \
                   b"\xff" * 32 + b"\x00\xc0\x2b\x00"
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s, server_hello)

    # Simulate encrypted app data (TLS record type 0x17)
    app_data = payload_str.encode() if payload_str else b"GET / HTTP/1.1\r\nHost: " + sni_name
    if imei or msisdn:
        app_data = f"IMEI={imei};MSISDN={msisdn};".encode() + app_data
    tls_app = b"\x17\x03\x03" + _pstruct.pack("!H", len(app_data)) + app_data
    seq_c = tcp_pkt(src_ip, dst_ip, src_port, dst_port, seq_c, tls_app)

    # Server response (encrypted)
    tls_resp = b"\x17\x03\x03\x00\x1f" + b"\xAB" * 31
    seq_s = tcp_pkt(dst_ip, src_ip, dst_port, src_port, seq_s, tls_resp)
    return _PCAP_GLOBAL_HDR + b"".join(pkts)


def _pcap_sip_only(src_ip, dst_ip, src_port, dst_port,
                   calling, called, call_id_str,
                   ts_sec, imei="", msisdn="") -> bytes:
    """Generate SIP-only signalling PCAP (no RTP)."""
    pkts = []; t = ts_sec
    via  = f"z9hG4bK-{call_id_str}"
    meta = f"X-IMEI: {imei}\r\nX-MSISDN: {msisdn}\r\n" if (imei or msisdn) else ""

    def u(sip, dip, sp, dp, msg, off=0):
        raw = _eth_ip(sip, dip, 17, _udp(sp, dp, msg.encode()))
        pkts.append(_pcap_packet(raw, t + off, 0))

    u(src_ip, dst_ip, src_port, dst_port,
      f"REGISTER sip:{dst_ip} SIP/2.0\r\n"
      f"Via: SIP/2.0/UDP {src_ip}:{src_port};branch={via}\r\n"
      f"From: <sip:{calling}@{src_ip}>;tag=reg1\r\n"
      f"To: <sip:{calling}@{dst_ip}>\r\n"
      f"Call-ID: reg-{call_id_str}@{src_ip}\r\nCSeq: 1 REGISTER\r\n"
      f"Expires: 3600\r\n{meta}\r\n", 0)
    u(dst_ip, src_ip, dst_port, src_port,
      f"SIP/2.0 200 OK\r\nVia: SIP/2.0/UDP {src_ip}:{src_port};branch={via}\r\n"
      f"From: <sip:{calling}@{src_ip}>;tag=reg1\r\n"
      f"To: <sip:{calling}@{dst_ip}>;tag=srv1\r\n"
      f"Call-ID: reg-{call_id_str}@{src_ip}\r\nCSeq: 1 REGISTER\r\n\r\n", 1)
    u(src_ip, dst_ip, src_port, dst_port,
      f"INVITE sip:{called}@{dst_ip} SIP/2.0\r\n"
      f"Via: SIP/2.0/UDP {src_ip}:{src_port};branch={via}inv\r\n"
      f"From: <sip:{calling}@{src_ip}>;tag=inv1\r\n"
      f"To: <sip:{called}@{dst_ip}>\r\n"
      f"Call-ID: {call_id_str}@{src_ip}\r\nCSeq: 2 INVITE\r\n"
      f"Content-Length: 0\r\n{meta}\r\n", 2)
    u(dst_ip, src_ip, dst_port, src_port,
      f"SIP/2.0 100 Trying\r\nVia: SIP/2.0/UDP {src_ip}:{src_port};branch={via}inv\r\n"
      f"Call-ID: {call_id_str}@{src_ip}\r\nCSeq: 2 INVITE\r\n\r\n", 2)
    u(dst_ip, src_ip, dst_port, src_port,
      f"SIP/2.0 180 Ringing\r\nVia: SIP/2.0/UDP {src_ip}:{src_port};branch={via}inv\r\n"
      f"Call-ID: {call_id_str}@{src_ip}\r\nCSeq: 2 INVITE\r\n\r\n", 3)
    u(dst_ip, src_ip, dst_port, src_port,
      f"SIP/2.0 200 OK\r\nVia: SIP/2.0/UDP {src_ip}:{src_port};branch={via}inv\r\n"
      f"Call-ID: {call_id_str}@{src_ip}\r\nCSeq: 2 INVITE\r\n\r\n", 4)
    return _PCAP_GLOBAL_HDR + b"".join(pkts)


def _pcap_sample_csv() -> list:
    """Return sample CSV rows for all supported PCAP types."""
    return [
        ["Type", "SrcIP", "DstIP", "SrcPort", "DstPort",
         "Timestamp", "Duration", "Payload",
         "IMEI", "MSISDN",
         "Calling", "Called", "CallID",
         "Method", "URL", "Domain",
         "FTPUser", "FTPPass", "FTPFile",
         "SNI", "MailFrom", "MailTo", "Subject",
         "StreamURL"],
        ["VoIP",  "192.168.1.10","10.0.0.1",  "5060","5060",
         "24-03-2026 19:00:00","58","",
         "354076826454420","919456622889",
         "919456622889","916253957648","13092840",
         "","","","","","","","","","",""],
        ["SIP",   "192.168.1.10","10.0.0.1",  "5060","5060",
         "24-03-2026 19:02:00","0","",
         "354076826454420","919456622889",
         "919456622889","916253957648","13092841",
         "","","","","","","","","","",""],
        ["HTTP",  "192.168.1.10","203.0.113.5","50000","80",
         "24-03-2026 19:03:00","1","Hello world",
         "354076826454420","919456622889",
         "","","","GET","/index.html","","","","","","","","",""],
        ["HTTPS", "192.168.1.10","203.0.113.5","50001","443",
         "24-03-2026 19:04:00","1","",
         "354076826454420","919456622889",
         "","","","","","","","","","example.com","","","",""],
        ["DNS",   "192.168.1.10","8.8.8.8",   "54321","53",
         "24-03-2026 19:05:00","0","",
         "","","","","","","","example.com","","","","","","","",""],
        ["FTP",   "192.168.1.10","172.16.0.1","21000","21",
         "24-03-2026 19:06:00","0","file content here",
         "354076826454421","919456622890",
         "","","","","","","ftpuser","ftppass","data.bin","","","","",""],
        ["SMTP",  "192.168.1.10","172.16.0.2","25000","25",
         "24-03-2026 19:07:00","0","Email body text",
         "","","","","","","","",
         "","","","","sender@test.com","rcpt@test.com","Test Subject",""],
        ["STUN",  "192.168.1.10","8.8.8.8",  "54400","3478",
         "24-03-2026 19:08:00","0","",
         "354076826454422","919456622891",
         "","","","","","","","","","","","","",""],
        ["RTSP",  "192.168.1.10","10.0.0.2", "55000","554",
         "24-03-2026 19:09:00","5","",
         "","","","","","","","","","","",
         "","","","","rtsp://10.0.0.2:554/live"],
        ["TCP",   "192.168.1.10","172.16.0.3","44000","8080",
         "24-03-2026 19:10:00","0","custom tcp payload",
         "354076826454421","919456622890",
         "","","","","","","","","","","","","",""],
        ["UDP",   "192.168.1.10","172.16.0.4","44001","9999",
         "24-03-2026 19:11:00","0","custom udp payload",
         "","","","","","","","","","","","","","","",""],
        ["ICMP",  "192.168.1.10","8.8.8.8",  "0","0",
         "24-03-2026 19:12:00","0","ping",
         "","","","","","","","","","","","","","","",""],
    ]


def _pcap_dispatch(pt, src_ip, dst_ip, sport, dport,
                   ts_sec, dur, imei, msisdn, g) -> bytes:
    """Route to the correct PCAP engine based on protocol type."""
    pt = pt.upper()
    if pt == "VOIP":
        return _pcap_voip(src_ip, dst_ip, sport, dport,
                          g("Calling") or "919456622889",
                          g("Called")  or "916253957648",
                          g("Call ID") or "1",
                          ts_sec, dur, imei, msisdn)
    elif pt == "SIP":
        return _pcap_sip_only(src_ip, dst_ip, sport, dport,
                              g("Calling") or "919000000000",
                              g("Called")  or "916253957648",
                              g("Call ID") or "1",
                              ts_sec, imei, msisdn)
    elif pt == "HTTP":
        return _pcap_http(src_ip, dst_ip, sport, dport,
                          g("Method") or "GET",
                          g("URL")    or "/",
                          g("Payload"), ts_sec, imei, msisdn)
    elif pt == "HTTPS":
        return _pcap_tls(src_ip, dst_ip, sport, dport,
                         g("SNI") or "example.com",
                         g("Payload"), ts_sec, imei, msisdn)
    elif pt == "DNS":
        return _pcap_dns(src_ip, dst_ip, sport, ts_sec,
                         g("Domain") or "example.com", imei, msisdn)
    elif pt == "FTP":
        return _pcap_ftp(src_ip, dst_ip, sport, dport,
                         g("FTP User") or "anonymous",
                         g("FTP Pass") or "pass",
                         g("FTP File") or "data.bin",
                         g("Payload"), ts_sec, imei, msisdn)
    elif pt == "SMTP":
        return _pcap_smtp(src_ip, dst_ip, sport, dport,
                          g("Mail From") or "sender@example.com",
                          g("Mail To")   or "rcpt@example.com",
                          g("Subject")   or "Test",
                          g("Payload")   or "Email body",
                          ts_sec, imei, msisdn)
    elif pt == "STUN":
        return _pcap_stun(src_ip, dst_ip, sport, ts_sec, imei, msisdn)
    elif pt == "RTSP":
        return _pcap_rtsp(src_ip, dst_ip, sport, dport,
                          g("Stream URL") or f"rtsp://{dst_ip}:{dport}/live",
                          ts_sec, dur, imei, msisdn)
    else:
        return _pcap_raw(src_ip, dst_ip, sport, dport,
                         pt, g("Payload"), ts_sec, imei, msisdn)


# ================================================================
# MAIN APPLICATION
# ================================================================
class ComTrailApp(tk.Tk):

    THEMES = {
        "dark": {
            # Core teal
            "primary":    "#0891b2",   # crystal teal — buttons, accents
            "success":    "#22c55e",   # green — success / connected / valid
            "error":      "#f87171",   # soft red — errors
            # Backgrounds
            "bg":         "#0f172a",   # deep dark
            "panel":      "#1e293b",   # card background
            "input_bg":   "#0f172a",   # input fields
            "topbar":     "#0f172a",   # top navigation bar
            # Text
            "text":       "#e6edf3",   # primary text — bright white-blue
            "card_title": "#e6edf3",   # headings on cards
            "muted":      "#adb5bd",   # secondary text — clearly visible
            "subtle":     "#e6edf3",   # body text — bright white
            "dim":        "#8b949e",   # placeholder / disabled text
            # Borders & dividers
            "border":     "#30363d",   # card borders — subtle
            "separator":  "#0891b2",   # accent separator line
            # Nav
            "topbtn":     "#21262d",   # nav hover background
            "topbtn_h":   "#30363d",   # nav active hover
            "nav_active": "#67e8f9",   # nav label color
            # Misc
            "warn":       "#f0a84a",   # warnings
        },
        "light": {
            # Core teal
            "primary":    "#0891b2",   # crystal teal — buttons, accents
            "success":    "#16a34a",   # green — success / connected / valid
            "error":      "#dc2626",   # red — errors
            # Backgrounds
            "bg":         "#f8fafc",   # clean off-white
            "panel":      "#ffffff",   # pure white cards
            "input_bg":   "#f1f5f9",   # very light grey inputs
            "topbar":     "#164e63",   # dark navy topbar
            # Text
            "text":       "#0f172a",   # near-black — primary text
            "card_title": "#0f172a",   # headings
            "muted":      "#475569",   # secondary text — clearly readable
            "subtle":     "#1e293b",   # body text — dark for contrast
            "dim":        "#64748b",   # placeholder text
            # Borders & dividers
            "border":     "#cbd5e1",   # light grey borders
            "separator":  "#06b6d4",   # blue separator
            # Nav
            "topbtn":     "#155e75",   # nav hover
            "topbtn_h":   "#164e63",   # nav active hover
            "nav_active": "#ffffff",   # nav labels — white on dark navy
            # Misc
            "warn":       "#d97706",   # warnings
        },
    }

    def __init__(self):
        super().__init__()
        self.title("ComTrail Data Upload & Generate Utility")
        # Set window / taskbar icon to logo1.png
        try:
            _ico_path = _resource_path("logo1.png")
            if os.path.isfile(_ico_path):
                _ico_img = tk.PhotoImage(file=_ico_path)
                self.iconphoto(True, _ico_img)
                self._taskbar_icon = _ico_img  # keep reference so GC doesn't collect it
            else:
                self.iconbitmap("")
        except Exception:
            try: self.iconbitmap("")
            except Exception: pass
        if sys.platform == "win32":
            self.state("zoomed")
        elif sys.platform == "darwin":
            self.attributes("-zoomed", True)
        else:
            self.attributes("-zoomed", True)
        self._theme = "dark"
        self.COLORS = dict(self.THEMES["dark"])
        self.config(bg=self.COLORS["bg"])
        self.cfg = load_config()
        _apply_service_config(self.cfg)
        self._font_scale     = float(self.cfg.get("font_scale", 1.0))
        self._orig_font_sizes = {}   # name → original size (populated on first scale)
        self.active_frame = None
        self._topbar_frame  = None
        self._topbar_extras = []   # separator + dt_bar packed on self, destroyed before rebuild

        self._status_dots   = {}
        self._status_labels = {}
        self._target_cache     = {}
        self._target_file_list = []

        # ── Session tracking ───────────────────────────────────────
        self._session_history  = []
        self._session_counts   = {"pcap": 0, "ludr": 0, "voice": 0,
                                   "generated": 0, "failed": 0}
        self._active_uploads   = 0   # incremented while an upload thread runs
        self._history_lock     = threading.Lock()  # guards CSV append from concurrent threads
        self._schedule_job     = None  # after() id for scheduled upload
        self._schedule_fn      = None  # function to call on next tick
        self._schedule_label   = None  # StringVar for countdown label
        self._schedules          = {}
        self._sched_jobs         = []
        self._sched_ticker       = None
        self._sched_page_refresh = None
        self._sched_lock         = threading.Lock()   # guards _sched_save from concurrent threads

        # ── Notification history (#19) ─────────────────────────────
        self._notif_history    = []   # list of (ts, msg, kind)
        self._notif_unread     = [0]  # mutable counter
        self._notif_badge_var  = None # set in build_topbar
        self._topbar_health    = {}   # srv_key → canvas oval tag tuple

        # ── Kafka dashboard persistent filter state ─────────────────
        self._kafka_filter_var   = None  # initialised lazily on first open
        self._kafka_search_var   = None
        self._kafka_date_var     = None  # kept for legacy compat
        self._kafka_date_from_var= None
        self._kafka_date_to_var  = None
        self._kafka_limit_var    = None
        self._kafka_topic_var    = None
        self._kafka_messages    = []    # last fetched message cache
        self._kafka_trace_search = None
        self._kafka_trace_date   = None
        self._kafka_trace_topic  = None


        # ── Generate-page field persistence ───────────────────────
        self._sms_state        = {}
        self._lbs_state        = {}
        self._voice_state      = {}
        self._bulk_voice_state = {}

        # ── Home page live state ───────────────────────────────────
        self._home_clock_job    = None   # after-id for clock/server-time tick
        self._last_upload       = {"pcap": None, "ludr": None, "voice": None}
        self._kafka_last_fetch  = None   # time.time() of last Kafka fetch
        self._target_last_load  = None   # time.time() of last Target load
        self._home_stat_labels  = {}     # key → Label widget for live stats
        self._server_time_label = None   # Label showing server time
        self._home_frame_ref    = [None] # mutable ref so clock can check alive

        # ── Activity log badge tracking ────────────────────────────
        self._log_unseen_errors  = 0   # errors since Logs page last opened
        self._log_seen_count     = 0   # total entries seen on last Logs visit
        self._log_badge_labels   = []  # weak list of badge widgets to update

        self._sched_load()
        self._register_all_connections()
        LOG.subscribe(self._on_log_entry)
        self.build_topbar()
        self.show_first_page()
        self._poll_dots()
        self._setup_global_hover()
        self.after(3000, self._sched_startup_check)
        if self._sched_jobs:
            self._sched_ensure_ticker()

        # ── Exit handler (#20) ─────────────────────────────────────
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        # ── Keyboard shortcut: ? = shortcuts overlay (#17) ─────────
        self.bind_all("<question>", lambda e: self._show_shortcuts_overlay())

        # ── Alt+1..5 = nav shortcuts ────────────────────────────────
        _nav_fns = [
            self.show_first_page,
            self.show_sample_data,
            self.show_settings,
            self.show_tag_validation,
            self.show_help,
        ]
        for _i, _fn in enumerate(_nav_fns, 1):
            self.bind_all(f"<Alt-Key-{_i}>", lambda e, f=_fn: f())

        # Apply saved font scale on startup (skip if default 1.0)
        if abs(self._font_scale - 1.0) > 0.01:
            self.after(200, lambda: self._apply_font_scale(self._font_scale))

    def _apply_font_scale(self, scale):
        """Scale all tkinter named fonts proportionally and persist the setting."""
        import tkinter.font as _tkf
        self._font_scale = float(scale)
        self.cfg["font_scale"] = self._font_scale
        save_config(self.cfg)
        for name in _tkf.names(self):
            try:
                f = _tkf.nametofont(name)
                # Snapshot original sizes on first call
                if name not in self._orig_font_sizes:
                    self._orig_font_sizes[name] = abs(f.cget("size")) or 9
                new_size = max(6, int(self._orig_font_sizes[name] * self._font_scale))
                f.configure(size=new_size)
            except Exception:
                pass
        # Refresh topbar and current page
        if self._topbar_frame:
            self._topbar_frame.destroy()
        for _w in self._topbar_extras:
            try: _w.destroy()
            except Exception: pass
        self._topbar_extras = []
        self.build_topbar()
        self.show_settings()

    def _setup_global_hover(self):
        """Highlight any widget under the cursor on mouse-enter; restore on leave."""

        def _lighten(color, amount=28):
            try:
                c = color.lstrip("#")
                if len(c) == 3:
                    c = c[0]*2 + c[1]*2 + c[2]*2
                r = min(255, int(c[0:2], 16) + amount)
                g = min(255, int(c[2:4], 16) + amount)
                b = min(255, int(c[4:6], 16) + amount)
                return f"#{r:02x}{g:02x}{b:02x}"
            except Exception:
                return None

        def _on_enter(event):
            w = event.widget
            if isinstance(w, str) or not w.winfo_exists():
                return
            try:
                wclass = w.winfo_class()
                if wclass == "Button":
                    if not hasattr(w, "_h_orig"):
                        orig = w.cget("bg")
                        lighter = _lighten(orig)
                        if lighter:
                            w._h_orig = orig
                            w.config(bg=lighter)
                elif wclass in ("Label", "Frame"):
                    cursor = ""
                    try:
                        cursor = w.cget("cursor")
                    except Exception:
                        pass
                    if cursor in ("hand2", "hand"):
                        if not hasattr(w, "_h_orig"):
                            orig = w.cget("bg")
                            lighter = _lighten(orig)
                            if lighter:
                                w._h_orig = orig
                                w.config(bg=lighter)
            except Exception:
                pass

        def _on_leave(event):
            w = event.widget
            try:
                if hasattr(w, "_h_orig"):
                    w.config(bg=w._h_orig)
                    del w._h_orig
            except Exception:
                pass

        self.bind_all("<Enter>", _on_enter, add="+")
        self.bind_all("<Leave>", _on_leave, add="+")

    def switch_theme(self, theme_name):
        """Switch between dark and light theme and redraw everything."""
        if theme_name not in self.THEMES:
            return
        self._theme = theme_name
        self.COLORS = dict(self.THEMES[theme_name])
        self.config(bg=self.COLORS["bg"])
        # Rebuild topbar with new colours
        if self._topbar_frame:
            self._topbar_frame.destroy()
        for _w in self._topbar_extras:
            try: _w.destroy()
            except Exception: pass
        self._topbar_extras = []
        self.build_topbar()
        # Redraw current page
        if self.active_frame:
            # Re-invoke whichever show_* was last shown by re-showing Home
            self.show_first_page()

    # ──────────────────────────────────────────────────────────────
    # STARTUP
    # ──────────────────────────────────────────────────────────────
    def _register_all_connections(self):
        for key in ("pcap", "ludr", "voice"):
            ip  = self.cfg.get(key, {}).get("ip",  "")
            pwd = self.cfg.get(key, {}).get("pwd", "")
            if ip and pwd:
                LOG.log("Connection", f"Initiating {key.upper()} connection to {ip}")
                CONN.register(key, ip, pwd)

    # ──────────────────────────────────────────────────────────────
    # DOT POLLING
    # ──────────────────────────────────────────────────────────────
    def _poll_dots(self):
        for key, dot in list(self._status_dots.items()):
            conn_key = key.split("_")[0]
            state = CONN.state(conn_key)
            lbl   = self._status_labels.get(key)
            try:
                if not dot.winfo_exists():
                    continue
                dot.set_state(state)
                if lbl:
                    if state == "failed":
                        cd = CONN.retry_countdown(conn_key)
                        if cd and cd > 0:
                            text  = f"Retry in {cd}s"
                            color = self.C("error")
                        else:
                            text  = "Reconnecting…"
                            color = self.C("warn")
                    else:
                        text, color = {
                            "connected": ("Connected",   self.C("success")),
                            "pending":   ("Connecting…", self.C("muted")),
                        }.get(state, ("Unknown", self.C("muted")))
                    lbl.config(text=text, fg=color)
            except Exception:
                pass

        # (#1) Update topbar health strip canvases
        _health_colors = {
            "connected": "#22c55e",
            "failed":    "#ef4444",
            "pending":   "#f59e0b",
        }
        for srv_key, (canvas, oval_id) in list(self._topbar_health.items()):
            try:
                if not canvas.winfo_exists():
                    continue
                state = CONN.state(srv_key)
                canvas.itemconfig(oval_id,
                                  fill=_health_colors.get(state, "#6b7280"))
                # Update tooltip with countdown when failed
                if state == "failed":
                    cd = CONN.retry_countdown(srv_key)
                    tip = (f"Retrying in {cd}s…" if cd and cd > 0
                           else "Retrying now…")
                    _add_tooltip(canvas, tip)
                elif state == "connected":
                    _add_tooltip(canvas, "Connected")
                else:
                    _add_tooltip(canvas, "Connecting…")
            except Exception:
                pass

        self.after(2000, self._poll_dots)

    # ──────────────────────────────────────────────────────────────
    # LOG CALLBACK
    # ──────────────────────────────────────────────────────────────
    def _on_log_entry(self, entry):
        # Track unseen errors for the badge
        if entry["level"] == "ERROR":
            self._log_unseen_errors += 1
            self._update_log_badge()

        # Live refresh handles tree updates — no direct insert needed

    def _refresh_home_stats(self):
        """Update live session stat labels on the Home page if they exist."""
        c = self._session_counts
        for key, val in [
            ("pcap",      c["pcap"]),
            ("ludr",      c["ludr"]),
            ("voice",     c["voice"]),
            ("generated", c["generated"]),
            ("failed",    c["failed"]),
        ]:
            lbl = self._home_stat_labels.get(key)
            if lbl:
                try:
                    if lbl.winfo_exists():
                        lbl.config(text=str(val))
                except Exception:
                    pass

    def _update_log_badge(self):
        """Refresh all registered badge labels with the current unseen error count."""
        dead = []
        for lbl in self._log_badge_labels:
            try:
                if not lbl.winfo_exists():
                    dead.append(lbl)
                    continue
                n = self._log_unseen_errors
                if n > 0:
                    lbl.config(text=f" {n} ", bg="#ef4444", fg="white")
                    lbl.pack(side="left", padx=(4, 0))
                else:
                    lbl.pack_forget()
            except Exception:
                dead.append(lbl)
        for d in dead:
            self._log_badge_labels.remove(d)

    # ──────────────────────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────────────────────
    def _show_schedule_dialog(self, upload_fn, upload_label):
        """Schedule dialog — pick file/folder once, then upload on a timer."""
        # Determine if this is a folder-picker (PCAP/Voice) or file-picker (LUDR)
        is_ludr = "LUDR" in upload_label or "LBS" in upload_label

        dlg = tk.Toplevel(self)
        dlg.title(f"Schedule — {upload_label}")
        dlg.configure(bg=self.C("panel"))
        dlg.resizable(False, False)
        dlg.grab_set()
        W, H = 480, 360
        x = self.winfo_rootx() + (self.winfo_width()  - W) // 2
        y = self.winfo_rooty() + (self.winfo_height() - H) // 2
        dlg.geometry(f"{W}x{H}+{x}+{y}")
        tk.Frame(dlg, bg=self.C("primary"), height=4).pack(fill="x")

        body = tk.Frame(dlg, bg=self.C("panel"))
        body.pack(padx=24, pady=14, fill="x")

        tk.Label(body, text=f"🕐  Schedule  —  {upload_label}",
                 bg=self.C("panel"), fg=self.C("card_title"),
                 font=(_UI_FONT, 12, "bold")).pack(anchor="w", pady=(0, 6))
        tk.Frame(body, bg=self.C("border"), height=1).pack(fill="x", pady=(0, 12))

        # ── File / Folder picker ──────────────────────────────────
        pick_lbl_text = "Files (CSV/TXT)" if is_ludr else "Folder"
        tk.Label(body, text=f"📂  {pick_lbl_text}:",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(anchor="w")

        path_var = tk.StringVar()
        # Pre-fill with last used path
        if is_ludr:
            path_var.set(self.cfg.get("last_folder", {}).get("ludr", ""))
        else:
            key = "pcap" if "PCAP" in upload_label else "voice"
            path_var.set(self.cfg.get("last_folder", {}).get(key, ""))

        path_row = tk.Frame(body, bg=self.C("panel"))
        path_row.pack(fill="x", pady=(4, 0))
        path_entry = tk.Entry(path_row, textvariable=path_var,
                              bg=self.C("input_bg"), fg=self.C("text"),
                              insertbackground=self.C("text"),
                              relief="flat", font=(_UI_FONT, 9), width=38)
        path_entry.pack(side="left", ipady=5, padx=(0, 6))

        # store selected files for LUDR (list), folder for others (str)
        _selected = [None]   # None = not yet picked / use path_var

        def _browse():
            if is_ludr:
                files = filedialog.askopenfilenames(
                    title="Select Files to Upload",
                    filetypes=[("CSV/TXT", "*.csv *.txt")])
                if files:
                    _selected[0] = list(files)
                    path_var.set("; ".join(os.path.basename(f) for f in files))
            else:
                folder = filedialog.askdirectory(title=f"Select {upload_label} Folder")
                if folder:
                    _selected[0] = folder
                    path_var.set(folder)

        tk.Button(path_row, text="Browse…",
                  bg=self.C("primary"), fg="white", relief="flat",
                  font=(_UI_FONT, 9, "bold"), cursor="hand2",
                  command=_browse).pack(side="left", ipady=4, padx=(0, 4))

        # ── Re-use same file checkbox ─────────────────────────────
        reuse_var = tk.BooleanVar(value=True)
        tk.Checkbutton(body, text="Re-use same file/folder each run (no browse popup)",
                       variable=reuse_var,
                       bg=self.C("panel"), fg=self.C("text"),
                       selectcolor=self.C("input_bg"),
                       activebackground=self.C("panel"),
                       font=(_UI_FONT, 9)).pack(anchor="w", pady=(8, 0))

        # ── Interval ──────────────────────────────────────────────
        int_row = tk.Frame(body, bg=self.C("panel"))
        int_row.pack(fill="x", pady=(10, 0))
        tk.Label(int_row, text="Repeat every:", bg=self.C("panel"),
                 fg=self.C("muted"), font=(_UI_FONT, 9)).pack(side="left")
        interval_var = tk.StringVar(value="15")
        tk.Entry(int_row, textvariable=interval_var, width=6,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"), relief="flat",
                 font=(_UI_FONT, 10)).pack(side="left", padx=6, ipady=4)
        tk.Label(int_row, text="minutes", bg=self.C("panel"),
                 fg=self.C("muted"), font=(_UI_FONT, 9)).pack(side="left")

        # Show active countdown if already running
        if self._schedule_label and self._schedule_job:
            tk.Label(body, textvariable=self._schedule_label,
                     bg=self.C("panel"), fg="#f59e0b",
                     font=(_UI_FONT, 8, "italic")).pack(anchor="w", pady=(8, 0))

        info = tk.Label(body, text="", bg=self.C("panel"), fg="#ef4444",
                        font=(_UI_FONT, 8))
        info.pack(anchor="w", pady=(6, 0))

        def _run_upload_with_path():
            """Upload using stored path, bypassing the file picker."""
            sel = _selected[0]
            if is_ludr:
                files = sel if isinstance(sel, list) else None
                if not files:
                    # fall back to normal picker
                    upload_fn()
                    return
                # Run upload_ludr logic directly with known files
                self._upload_ludr_with_files(files)
            else:
                folder = sel if isinstance(sel, str) else None
                if not folder or not os.path.isdir(folder):
                    upload_fn()
                    return
                if "PCAP" in upload_label:
                    self._upload_pcap_with_folder(folder)
                else:
                    self._upload_voice_with_folder(folder)

        def _start():
            try:
                mins = max(1, int(interval_var.get().strip()))
            except ValueError:
                info.config(text="❌  Enter a valid number of minutes.")
                return

            # Must have a path selected
            if _selected[0] is None and not path_var.get().strip():
                info.config(text="❌  Please browse and select a file/folder first.")
                return

            # If user typed a path manually without browsing, store it
            if _selected[0] is None:
                raw = path_var.get().strip()
                if is_ludr:
                    _selected[0] = [f.strip() for f in raw.split(";") if f.strip()]
                else:
                    _selected[0] = raw

            # Cancel existing schedule
            if self._schedule_job:
                try: self.after_cancel(self._schedule_job)
                except Exception: pass

            self._schedule_label = tk.StringVar(value="")
            _next = [time.time() + mins * 60]
            _reuse = reuse_var.get()

            def _tick():
                remaining = int(_next[0] - time.time())
                if remaining <= 0:
                    if _reuse:
                        _run_upload_with_path()
                    else:
                        upload_fn()
                    _next[0] = time.time() + mins * 60
                    remaining = mins * 60
                m, s = divmod(remaining, 60)
                self._schedule_label.set(f"🔁  Next {upload_label} in {m}m {s}s")
                self._schedule_job = self.after(1000, _tick)

            self._schedule_job = self.after(1000, _tick)
            self._toast(f"⏰  {upload_label} scheduled every {mins} min", "info")
            dlg.destroy()

        def _stop():
            if self._schedule_job:
                try: self.after_cancel(self._schedule_job)
                except Exception: pass
            self._schedule_job   = None
            self._schedule_fn    = None
            self._schedule_label = None
            _selected[0]         = None
            self._toast("Schedule cancelled.", "info", duration=2000)
            dlg.destroy()

        br = tk.Frame(dlg, bg=self.C("panel"))
        br.pack(pady=(0, 14))
        tk.Button(br, text="▶  Start Schedule", bg=self.C("primary"), fg="white",
                  relief="flat", font=(_UI_FONT, 9, "bold"), cursor="hand2",
                  padx=12, pady=5, command=_start).pack(side="left", padx=6)
        tk.Button(br, text="⏹  Stop", bg="#374151", fg="white",
                  relief="flat", font=(_UI_FONT, 9, "bold"), cursor="hand2",
                  padx=12, pady=5, command=_stop).pack(side="left", padx=6)
        tk.Button(br, text="Cancel", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9),
                  highlightbackground=self.C("border"), highlightthickness=1,
                  padx=12, pady=5, command=dlg.destroy).pack(side="left", padx=6)

    def _safe_nav(self, fn):
        """Call fn only after confirming it's OK to leave mid-upload."""
        if self._active_uploads > 0:
            n = self._active_uploads
            lbl = f"{n} upload{'s' if n > 1 else ''} still in progress"
            result = [False]
            dlg = tk.Toplevel(self)
            dlg.title("Upload in Progress")
            dlg.configure(bg=self.C("panel"))
            dlg.resizable(False, False)
            dlg.grab_set()
            W, H = 360, 160
            x = self.winfo_rootx() + (self.winfo_width()  - W) // 2
            y = self.winfo_rooty() + (self.winfo_height() - H) // 2
            dlg.geometry(f"{W}x{H}+{x}+{y}")
            tk.Frame(dlg, bg="#f59e0b", height=4).pack(fill="x")
            body = tk.Frame(dlg, bg=self.C("panel"))
            body.pack(padx=24, pady=16)
            tk.Label(body, text=f"⚠️  {lbl}",
                     bg=self.C("panel"), fg=self.C("card_title"),
                     font=(_UI_FONT, 11, "bold")).pack(pady=(0, 6))
            tk.Label(body, text="Navigating away will not cancel the upload —\nit will continue in the background.",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9), justify="center").pack()
            br = tk.Frame(dlg, bg=self.C("panel"))
            br.pack(pady=(0, 14))
            def _go():
                result[0] = True
                dlg.destroy()
            tk.Button(br, text="Continue Anyway", bg=self.C("primary"), fg="white",
                      relief="flat", font=(_UI_FONT, 9, "bold"), cursor="hand2",
                      padx=12, pady=5, command=_go).pack(side="left", padx=6)
            tk.Button(br, text="Stay", bg=self.C("panel"), fg=self.C("muted"),
                      relief="flat", font=(_UI_FONT, 9),
                      highlightbackground=self.C("border"), highlightthickness=1,
                      padx=12, pady=5, command=dlg.destroy).pack(side="left", padx=6)
            dlg.wait_window()
            if result[0]:
                fn()
        else:
            fn()

    def C(self, name):
        return self.COLORS.get(name, "#888888")

    def popup(self, title, msg, kind="info"):
        LOG.log("Dialog", f"{title}: {msg}", "INFO" if kind != "error" else "ERROR")
        return show_dialog(self, title, msg, kind)

    def clear_main(self):
        if self.active_frame:
            self.active_frame.destroy()
        self._status_dots.clear()
        self._status_labels.clear()
        self.active_frame = tk.Frame(self, bg=self.C("bg"))
        self.active_frame.pack(fill="both", expand=True)


    def _toast(self, msg, kind="success", duration=3500):
        """
        Non-blocking right-side toast notification.
        Auto-dismisses after `duration` ms.
        kind: success | info | warning | error
        """
        # (#19) Record to notification history
        self._add_notification(msg, kind)

        COLORS = {
            "success": ("#22c55e", "✅"),
            "info":    ("#67e8f9", "ℹ️"),
            "warning": ("#f0a84a", "⚠️"),
            "error":   ("#f87171", "❌"),
        }
        bar_color, icon = COLORS.get(kind, COLORS["info"])

        top = tk.Toplevel(self)
        top.overrideredirect(True)
        top.attributes("-topmost", True)
        top.configure(bg=bar_color)

        W, H = 340, 72
        sx = self.winfo_screenwidth()
        sy = self.winfo_screenheight()
        x  = sx - W - 18
        # Stack above any existing toasts
        y  = sy - H - 60

        top.geometry(f"{W}x{H}+{x}+{y}")

        inner = tk.Frame(top, bg=self.C("panel"), padx=2, pady=2)
        inner.pack(fill="both", expand=True, padx=2, pady=(0, 2))

        tk.Frame(inner, bg=bar_color, height=3).pack(fill="x")

        body = tk.Frame(inner, bg=self.C("panel"))
        body.pack(fill="both", expand=True, padx=10, pady=8)

        tk.Label(body, text=icon,
                 bg=self.C("panel"),
                 font=(_UI_FONT, 14)).pack(side="left", padx=(0, 8))

        txt = tk.Label(body, text=msg,
                       bg=self.C("panel"),
                       fg=self.C("card_title"),
                       font=(_UI_FONT, 9),
                       wraplength=260, justify="left", anchor="w")
        txt.pack(side="left", fill="x")

        # Click anywhere to dismiss early
        for w in [top, inner, body, txt]:
            w.bind("<Button-1>", lambda e: top.destroy())

        self.after(duration, lambda: (
            top.destroy() if top.winfo_exists() else None))

    def progress_window(self, text):
        """
        Right-side sliding toast progress bar.
        Returns (top, pb) — caller destroys top when done.
        Also exposes top._set_status(msg, pct) for live updates.
        """
        top = tk.Toplevel(self)
        top.title("")
        top.overrideredirect(True)
        top.attributes("-topmost", True)
        top.configure(bg=self.C("border"))

        # Dimensions + right-side position
        W, H = 360, 110
        sx = self.winfo_screenwidth()
        sy = self.winfo_screenheight()
        x  = sx - W - 18
        y  = sy - H - 60
        top.geometry(f"{W}x{H}+{x}+{y}")

        inner = tk.Frame(top, bg=self.C("panel"), padx=2, pady=2)
        inner.pack(fill="both", expand=True, padx=2, pady=2)

        # Accent top bar
        tk.Frame(inner, bg=self.C("primary"), height=3).pack(fill="x")

        body = tk.Frame(inner, bg=self.C("panel"))
        body.pack(fill="both", expand=True, padx=12, pady=(8, 6))

        # Icon + title row
        hrow = tk.Frame(body, bg=self.C("panel"))
        hrow.pack(fill="x")
        tk.Label(hrow, text="⏳", bg=self.C("panel"),
                 font=(_UI_FONT, 13)).pack(side="left", padx=(0, 6))
        lbl_var = tk.StringVar(value=text)
        tk.Label(hrow, textvariable=lbl_var,
                 bg=self.C("panel"), fg=self.C("card_title"),
                 font=(_UI_FONT, 9, "bold"),
                 wraplength=280, anchor="w").pack(side="left", fill="x")

        # Sub-status (count / detail)
        sub_var = tk.StringVar(value="")
        tk.Label(body, textvariable=sub_var,
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 8)).pack(anchor="w", pady=(2, 4))

        # Determinate progress bar
        pct_var = tk.DoubleVar(value=0)
        pb = ttk.Progressbar(body, variable=pct_var,
                             maximum=100, mode="determinate",
                             length=310)
        pb.pack(fill="x")

        def _set_status(msg, pct=None, sub=""):
            try:
                lbl_var.set(msg)
                if sub: sub_var.set(sub)
                if pct is not None:
                    pct_var.set(pct)
                top.update_idletasks()
            except Exception:
                pass

        top._set_status = _set_status
        top._sub_var    = sub_var
        top._pct_var    = pct_var

        top.update_idletasks()
        return top, pb

    # ── Status row: dot + "Connected / Disconnected" label only ──
    def _make_status_row(self, parent, key, ip, pwd):
        """One-line: ● Connected (or Disconnected / Not Configured).
        Uses the parent's actual background so the canvas dot blends in."""
        # Read the parent's bg so the canvas oval background matches exactly
        try:
            parent_bg = parent.cget("bg")
        except Exception:
            parent_bg = self.C("panel")

        frame = tk.Frame(parent, bg=parent_bg)

        # Dot — canvas bg MUST match frame bg or a square shows around the circle
        dot = tk.Canvas(frame, width=14, height=14, bg=parent_bg,
                        highlightthickness=0)
        oval = dot.create_oval(2, 2, 12, 12, fill="#888888", outline="#888888")
        dot._oval = oval
        dot._set  = lambda state, d=dot, o=oval: d.itemconfig(
            o,
            fill  ={"connected": "#67e8f9", "failed": "#f87171", "pending": "#6e7681"}.get(state, "#6e7681"),
            outline={"connected": "#67e8f9", "failed": "#f87171", "pending": "#6e7681"}.get(state, "#6e7681"),
        )
        dot.pack(side="left", padx=(0, 6))

        conn_key = key.split("_")[0]
        state    = CONN.state(conn_key)

        if not ip or not pwd:
            dot._set("failed")
            init_text  = "Not Configured"
            init_color = self.C("error")
        else:
            dot._set(state)
            init_text, init_color = {
                "connected": ("Connected",    self.C("success")),
                "failed":    ("Disconnected", self.C("error")),
                "pending":   ("Connecting…",  self.C("muted")),
            }.get(state, ("Checking…", self.C("muted")))

        lbl = tk.Label(frame, text=init_text, bg=parent_bg,
                       fg=init_color, font=(_UI_FONT, 10, "bold"))
        lbl.pack(side="left")
        frame.pack(anchor="w", pady=(0, 2))

        # Store so _poll_dots can update them
        # We store a thin wrapper that exposes set_state like StatusDot did
        class _DotProxy:
            def __init__(self, canvas_dot): self._d = canvas_dot
            def set_state(self, s): self._d._set(s)
            def winfo_exists(self): return self._d.winfo_exists()

        self._status_dots[key]   = _DotProxy(dot)
        self._status_labels[key] = lbl
        return frame

    def _scrollable(self, parent):
        """
        Returns (canvas, scroll_frame).
        Binds mouse-wheel on the canvas so scrolling works without clicking
        the scrollbar first.
        """
        canvas = tk.Canvas(parent, bg=self.C("bg"), highlightthickness=0)
        vsb    = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        sf     = tk.Frame(canvas, bg=self.C("bg"))
        sf.bind("<Configure>",
                lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=sf, anchor="nw")
        canvas.configure(yscrollcommand=vsb.set)
        canvas.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        # Bind wheel to canvas AND the inner frame so it works anywhere
        _bind_mousewheel(canvas, canvas)
        _bind_mousewheel(sf,     canvas)
        # Also cascade to every child widget added later
        def _on_child_enter(event):
            if hasattr(event.widget, "bind"):
                _bind_mousewheel(event.widget, canvas)
        sf.bind_all("<Enter>", _on_child_enter, add="+")

        return canvas, sf

    # ──────────────────────────────────────────────────────────────
    # TOP BAR
    # ──────────────────────────────────────────────────────────────
    def build_topbar(self):
        # ── Colour constants — always dark navy regardless of theme ──
        TOPBAR_BG    = "#164e63"   # main navy
        TOPBAR_TOP   = "#0e7490"   # slightly lighter top edge (highlight)
        TOPBAR_BTM   = "#0a3040"   # slightly darker bottom edge (shadow)
        TOPBAR_FG    = "#ffffff"
        TOPBAR_SUB   = "#a5f3fc"   # light blue subtitle
        NAV_FG       = "#e2e8f0"   # nav label colour
        NAV_HOV_BG   = "#0e7490"   # hover pill background
        NAV_HOV_FG   = "#ffffff"   # hover text
        NAV_PILL_BG  = "#0c3d4a"   # resting pill (slightly darker than bar)
        NAV_PILL_BD  = "#0e7490"   # pill border
        SEP_BRIGHT   = "#ffffff"   # thin white separator line
        SEP_SHADOW   = "#164e63"   # shadow line below separator

        # ── 3D outer shell: top highlight line ────────────────────
        tk.Frame(self, bg=TOPBAR_TOP, height=2).pack(side="top", fill="x")

        # ── Main topbar body ───────────────────────────────────────
        top = tk.Frame(self, bg=TOPBAR_BG, height=76)
        top.pack(side="top", fill="x")
        top.pack_propagate(False)
        self._topbar_frame = top

        # ── 3D inner top shine (subtle lighter stripe inside) ──────
        shine = tk.Frame(top, bg=TOPBAR_TOP, height=1)
        shine.place(x=0, y=0, relwidth=1)

        # ── 3D inner bottom shadow (darker stripe at bottom) ───────
        shadow_line = tk.Frame(top, bg=TOPBAR_BTM, height=2)
        shadow_line.place(x=0, rely=1.0, y=-2, relwidth=1)

        # ── Left: Logo + App name ──────────────────────────────────
        left = tk.Frame(top, bg=TOPBAR_BG)
        left.pack(side="left", padx=(20, 0))

        _logo_path = _resource_path("logo1.png")
        _logo_placed = False
        try:
            from PIL import Image as _PIL, ImageTk as _PILTk
            _img   = _PIL.open(_logo_path).resize((48, 48), _PIL.LANCZOS)
            _photo = _PILTk.PhotoImage(_img)
            _lbl   = tk.Label(left, image=_photo, bg=TOPBAR_BG, borderwidth=0)
            _lbl.image = _photo
            _lbl.pack(side="left", pady=14, padx=(0, 14))
            _logo_placed = True
        except Exception:
            try:
                _photo = tk.PhotoImage(file=_logo_path)
                _f = max(1, max(_photo.width(), _photo.height()) // 48)
                if _f > 1:
                    _photo = _photo.subsample(_f, _f)
                _lbl   = tk.Label(left, image=_photo, bg=TOPBAR_BG, borderwidth=0)
                _lbl.image = _photo
                _lbl.pack(side="left", pady=14, padx=(0, 14))
                _logo_placed = True
            except Exception:
                pass
        if not _logo_placed:
            self._logo_widget(left, 48, TOPBAR_BG).pack(side="left", pady=14, padx=(0, 14))

        name_col = tk.Frame(left, bg=TOPBAR_BG)
        name_col.pack(side="left", pady=12)
        tk.Label(name_col, text="ComTrail",
                 bg=TOPBAR_BG, fg=TOPBAR_FG,
                 font=(_UI_FONT, 17, "bold")).pack(anchor="w")
        tk.Label(name_col, text="Data Upload & Generate Utility",
                 bg=TOPBAR_BG, fg=TOPBAR_SUB,
                 font=(_UI_FONT, 9)).pack(anchor="w")

        # ── Vertical divider between logo and nav ──────────────────
        divider = tk.Frame(top, bg=NAV_PILL_BD, width=1)
        divider.pack(side="left", fill="y", pady=14, padx=20)

        # ── (#1) Server health strip ────────────────────────────────
        health_bar = tk.Frame(top, bg=TOPBAR_BG)
        health_bar.pack(side="left", padx=(0, 16))
        self._topbar_health.clear()
        for srv_key, srv_label in [("pcap","PCAP"),("ludr","LUDR"),("voice","Voice")]:
            dot_c = tk.Canvas(health_bar, width=10, height=10,
                              bg=TOPBAR_BG, highlightthickness=0)
            oid = dot_c.create_oval(1, 1, 9, 9, fill="#6b7280", outline="")
            dot_c.pack(side="left", padx=(4, 2), pady=30)
            tk.Label(health_bar, text=srv_label,
                     bg=TOPBAR_BG, fg=TOPBAR_SUB,
                     font=(_UI_FONT, 8)).pack(side="left", padx=(0, 8))
            self._topbar_health[srv_key] = (dot_c, oid)

        tk.Frame(top, bg=NAV_PILL_BD, width=1).pack(
            side="left", fill="y", pady=14, padx=(0, 14))

        # ── (#19) Notification bell ─────────────────────────────────
        right_extras = tk.Frame(top, bg=TOPBAR_BG)
        right_extras.pack(side="right", padx=(0, 6))

        bell_outer = tk.Frame(right_extras, bg=NAV_PILL_BD, padx=1, pady=1)
        bell_outer.pack(side="right", padx=4, pady=20)
        bell_pill = tk.Label(bell_outer, text="🔔",
                             bg=NAV_PILL_BG, fg=NAV_FG,
                             font=(_UI_FONT, 12),
                             padx=8, pady=6, cursor="hand2")
        bell_pill.pack()

        self._notif_badge_var = tk.StringVar(value="")
        badge_lbl = tk.Label(bell_outer, textvariable=self._notif_badge_var,
                             bg="#ef4444", fg="white",
                             font=(_UI_FONT, 7, "bold"),
                             padx=3)

        def _update_badge():
            n = self._notif_unread[0]
            if n > 0:
                self._notif_badge_var.set(str(n) if n < 10 else "9+")
                badge_lbl.place(relx=1.0, rely=0.0, anchor="ne")
            else:
                badge_lbl.place_forget()

        self._update_notif_badge = _update_badge

        def _open_notif():
            self._notif_unread[0] = 0
            _update_badge()
            self._show_notif_panel()

        for w in [bell_outer, bell_pill]:
            w.bind("<Enter>", lambda e, p=bell_pill, po=bell_outer:
                   (p.config(bg=NAV_HOV_BG, fg="white"),
                    po.config(bg=NAV_HOV_BG)))
            w.bind("<Leave>", lambda e, p=bell_pill, po=bell_outer:
                   (p.config(bg=NAV_PILL_BG, fg=NAV_FG),
                    po.config(bg=NAV_PILL_BD)))
            w.bind("<Button-1>", lambda e: _open_notif())

        # ── Right: Nav pill buttons ────────────────────────────────
        right = tk.Frame(top, bg=TOPBAR_BG)
        right.pack(side="right", padx=6)

        nav_items = [
            ("🏠", "Home",           self.show_first_page),
            ("📁", "Sample Data",    self.show_sample_data),
            ("⚙️", "Settings",       self.show_settings),
            ("✅", "Validation",     self.show_tag_validation),
            ("❓", "Help",           self.show_help),
        ]

        for icon, label, cmd in nav_items:
            # Outer border frame = pill border effect
            pill_outer = tk.Frame(right,
                                  bg=NAV_PILL_BD,
                                  padx=1, pady=1)
            pill_outer.pack(side="left", padx=5, pady=20)

            pill = tk.Label(pill_outer,
                            text=f"{icon}  {label}",
                            bg=NAV_PILL_BG, fg=NAV_FG,
                            font=(_UI_FONT, 10, "bold"),
                            padx=14, pady=7,
                            cursor="hand2")
            pill.pack()

            # Hover: fill pill with bright blue
            def _enter(e, p=pill, po=pill_outer):
                p.config(bg=NAV_HOV_BG, fg=NAV_HOV_FG)
                po.config(bg=NAV_HOV_BG)
            def _leave(e, p=pill, po=pill_outer):
                p.config(bg=NAV_PILL_BG, fg=NAV_FG)
                po.config(bg=NAV_PILL_BD)

            pill.bind("<Enter>",    _enter)
            pill.bind("<Leave>",    _leave)
            pill.bind("<Button-1>", lambda e, c=cmd: self._safe_nav(c))
            pill_outer.bind("<Button-1>", lambda e, c=cmd: self._safe_nav(c))

        # ── Bottom 3D separator: bright blue + shadow ──────────────
        sep1 = tk.Frame(self, bg=SEP_BRIGHT, height=1)
        sep1.pack(side="top", fill="x")
        sep2 = tk.Frame(self, bg=SEP_SHADOW, height=1)
        sep2.pack(side="top", fill="x")

        # ── Date & Time strip below separator ─────────────────────
        dt_bar = tk.Frame(self, bg="#0f2a4a", height=22)
        dt_bar.pack(side="top", fill="x")
        dt_bar.pack_propagate(False)
        dt_lbl = tk.Label(dt_bar, text="",
                          bg="#0f2a4a", fg="#a5f3fc",
                          font=(_UI_FONT, 8))
        dt_lbl.pack(side="right", padx=16)

        # Track so they can be destroyed on the next build_topbar call
        self._topbar_extras = [sep1, sep2, dt_bar]

        def _update_dt():
            try:
                if not dt_lbl.winfo_exists():
                    return
                dt_lbl.config(text=time.strftime("%A, %d %B %Y   %H:%M:%S"))
                self.after(1000, _update_dt)
            except Exception:
                pass
        _update_dt()

    def _logo_widget(self, parent, size, bg):
        """Return a Label showing logo1.png, or a drawn-logo Canvas as fallback."""
        _path = _resource_path("logo1.png")
        try:
            from PIL import Image as _PIL, ImageTk as _PILTk
            _img   = _PIL.open(_path).resize((size, size), _PIL.LANCZOS)
            _photo = _PILTk.PhotoImage(_img)
            lbl    = tk.Label(parent, image=_photo, bg=bg, borderwidth=0)
            lbl.image = _photo
            return lbl
        except Exception:
            pass
        try:
            _photo = tk.PhotoImage(file=_path)
            _f = max(1, max(_photo.width(), _photo.height()) // size)
            if _f > 1:
                _photo = _photo.subsample(_f, _f)
            lbl = tk.Label(parent, image=_photo, bg=bg, borderwidth=0)
            lbl.image = _photo
            return lbl
        except Exception:
            pass
        # Fallback: drawn chevron canvas
        cv = tk.Canvas(parent, width=size, height=size, bg=bg, highlightthickness=0)
        self._draw_logo(cv, size, size, bg)
        return cv

    def _draw_logo(self, canvas, w, h, bg_color=None):
        """
        ClearTrail logo — exactly matches uploaded image:
        Two right-facing chevrons  >>
          · Left chevron  = dark grey  (#5a5a5a)
          · Right chevron = light grey (#b0b0b0)
          · No background shape — draws directly on canvas bg
          · Bold rounded strokes, angled at ~45°
        """
        bg = bg_color or "#164e63"
        canvas.config(bg=bg)
        canvas.delete("all")

        # Scale from a 40×40 reference
        s  = min(w, h) / 40.0
        cx = w / 2
        cy = h / 2

        lw  = max(3, int(4.5 * s))   # thick stroke — matches the bold look
        h2  = 8.0 * s                 # half-height of each chevron arm
        gap = 5.0 * s                 # horizontal spacing between chevrons

        # ── Left chevron  >  (dark grey) ──────────────────────────
        l_base_x = cx - gap * 0.9     # where the open end (left side) is
        l_apex_x = cx - gap * 0.0     # where the point is

        canvas.create_line(
            l_base_x, cy - h2,
            l_apex_x, cy,
            fill="#5a5a5a", width=lw,
            capstyle="round", joinstyle="round")
        canvas.create_line(
            l_apex_x, cy,
            l_base_x, cy + h2,
            fill="#5a5a5a", width=lw,
            capstyle="round", joinstyle="round")

        # ── Right chevron  >  (light grey) ────────────────────────
        r_base_x = cx + gap * 0.1     # open end (left side of right chevron)
        r_apex_x = cx + gap * 1.0     # point of right chevron

        canvas.create_line(
            r_base_x, cy - h2,
            r_apex_x, cy,
            fill="#b8b8b8", width=lw,
            capstyle="round", joinstyle="round")
        canvas.create_line(
            r_apex_x, cy,
            r_base_x, cy + h2,
            fill="#b8b8b8", width=lw,
            capstyle="round", joinstyle="round")

    # ──────────────────────────────────────────────────────────────
    # CARD / LABEL FACTORY
    # ──────────────────────────────────────────────────────────────
    def _card(self, parent, pady=8):
        f = tk.Frame(parent, bg=self.C("panel"),
                     highlightbackground=self.C("border"), highlightthickness=1)
        f.pack(fill="x", pady=pady, ipady=12, ipadx=15)
        return f

    def _section_label(self, parent, text, pady=(20, 12)):
        tk.Label(parent, text=text, bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 13, "bold")).pack(anchor="w", pady=pady, padx=2)

    def _collapsible_section(self, parent, text, pady=(20, 12), start_open=True):
        """
        Returns (header_row, content_frame).
        Clicking the header toggles the content_frame visibility.
        """
        _open = [start_open]
        hdr = tk.Frame(parent, bg=self.C("bg"), cursor="hand2")
        hdr.pack(fill="x", pady=pady, padx=2)
        arrow_lbl = tk.Label(hdr, text="▾" if start_open else "▸",
                             bg=self.C("bg"), fg=self.C("primary"),
                             font=(_UI_FONT, 12, "bold"))
        arrow_lbl.pack(side="left", padx=(0, 6))
        tk.Label(hdr, text=text, bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 13, "bold")).pack(side="left")
        content = tk.Frame(parent, bg=self.C("bg"))
        if start_open:
            content.pack(fill="x")

        def _toggle(e=None):
            _open[0] = not _open[0]
            if _open[0]:
                content.pack(fill="x")
                arrow_lbl.config(text="▾")
            else:
                content.pack_forget()
                arrow_lbl.config(text="▸")

        hdr.bind("<Button-1>", _toggle)
        arrow_lbl.bind("<Button-1>", _toggle)
        for child in hdr.winfo_children():
            try: child.bind("<Button-1>", _toggle)
            except Exception: pass

        return hdr, content

    # ──────────────────────────────────────────────────────────────
    # FIRST PAGE
    # ──────────────────────────────────────────────────────────────
    def show_first_page(self):
        self.clear_main()
        main = self.active_frame

        # Cancel any running clock job from a previous Home visit
        if self._home_clock_job:
            try: self.after_cancel(self._home_clock_job)
            except Exception: pass
            self._home_clock_job = None
        self._home_frame_ref[0] = main

        # ── Hero ───────────────────────────────────────────────────
        hero = tk.Frame(main, bg=self.C("bg"))
        hero.pack(pady=(40, 20))

        logo_row = tk.Frame(hero, bg=self.C("bg"))
        logo_row.pack()
        self._logo_widget(logo_row, 52, self.C("bg")).pack(side="left", padx=(0, 16))

        title_col = tk.Frame(logo_row, bg=self.C("bg"))
        title_col.pack(side="left")
        tk.Label(title_col, text="ComTrail",
                 bg=self.C("bg"),
                 fg="#ffffff" if self._theme == "dark" else "#000000",
                 font=(_UI_FONT, 28, "bold")).pack(anchor="w")
        tk.Label(title_col, text="Data Upload & Generate Utility  ·  v3.0",
                 bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT, 11)).pack(anchor="w")

        tk.Frame(main, bg=self.C("separator"), height=2).pack(
            fill="x", padx=100, pady=(18, 16))

        # ── Session summary strip ──────────────────────────────────
        c = self._session_counts
        ss_bg = "#164e63" if self._theme == "dark" else "#ecfeff"
        ss = tk.Frame(main, bg=ss_bg,
                      highlightbackground=self.C("primary"),
                      highlightthickness=1)
        ss.pack(fill="x", padx=100, pady=(0, 14), ipady=6)
        self._home_stat_labels.clear()
        for col, (lbl, key, val, col_color) in enumerate([
            ("PCAP Uploads",    "pcap",      c["pcap"],      "#06b6d4"),
            ("LUDR / SMS",      "ludr",      c["ludr"],      "#10b981"),
            ("Voice Uploads",   "voice",     c["voice"],     "#8b5cf6"),
            ("Generated Files", "generated", c["generated"], "#f59e0b"),
            ("Failures",        "failed",    c["failed"],    "#ef4444"),
        ]):
            box = tk.Frame(ss, bg=ss_bg)
            box.pack(side="left", padx=20)
            num_lbl = tk.Label(box, text=str(val), bg=ss_bg, fg=col_color,
                               font=(_UI_FONT, 18, "bold"))
            num_lbl.pack()
            tk.Label(box, text=lbl, bg=ss_bg,
                     fg="#a5f3fc" if self._theme == "dark" else "#475569",
                     font=(_UI_FONT, 8)).pack()
            self._home_stat_labels[key] = num_lbl
        # "This session" label on far right
        tk.Label(ss, text="This session",
                 bg=ss_bg, fg="#a5f3fc" if self._theme == "dark" else "#64748b",
                 font=(_UI_FONT, 7, "bold")).pack(side="right", padx=(0, 16))
        # Open Web UI button between Failures and "This session"
        tk.Button(ss, text="🌐  Open ComTrail Web UI",
                  command=lambda: webbrowser.open(self.cfg.get("comtrail_url", "")),
                  bg=self.C("primary"), fg="white",
                  activebackground="#0e7490",
                  font=(_UI_FONT, 8, "bold"), relief="flat",
                  padx=10, pady=4, cursor="hand2").pack(side="right", padx=(0, 12))

        tk.Label(main, text="Choose an action to get started",
                 bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT, 11)).pack(pady=(0, 12))


        # ── Main action cards ───────────────────────────────────────
        cards_frame = tk.Frame(main, bg=self.C("bg"))
        cards_frame.pack(fill="x", padx=80)

        items = [
            ("📤", "Upload Data",
             "Send PCAP, LBS/LUDR, Voice to server",
             ["PCAP files", "LBS / LUDR / SMS", "Voice Hi2 + Hi3"],
             self.show_home),
            ("⚙️", "Generate Data",
             "Create intercept data files",
             ["SMS IRI files", "LBS / LUDR CDR", "Voice call Hi2/Hi3"],
             self.show_generate_data),
        ]

        for col, (icon, title, subtitle, bullets, cmd) in enumerate(items):
            self._hero_card(cards_frame, col, icon, title, subtitle, bullets, cmd)

        # ── (#2) Recent activity — compact inline table ─────────────
        if self._session_history:
            tk.Frame(main, bg=self.C("separator"), height=1).pack(
                fill="x", padx=100, pady=(22, 10))

            act_hdr = tk.Frame(main, bg=self.C("bg"))
            act_hdr.pack(fill="x", padx=80, pady=(0, 4))
            tk.Label(act_hdr, text="Recent Activity",
                     bg=self.C("bg"), fg=self.C("muted"),
                     font=(_UI_FONT, 9, "bold")).pack(side="left")
            _badge_home = tk.Label(act_hdr, text="",
                                   bg="#ef4444", fg="white",
                                   font=(_UI_FONT, 7, "bold"))
            self._log_badge_labels.append(_badge_home)
            self._update_log_badge()
            tk.Button(act_hdr, text="View all →",
                      bg=self.C("bg"), fg=self.C("primary"),
                      relief="flat", font=(_UI_FONT, 8, "bold"),
                      cursor="hand2",
                      command=self.show_logs).pack(side="right")

            feed = tk.Frame(main,
                            highlightbackground=self.C("border"),
                            highlightthickness=1,
                            bg=self.C("panel"))
            feed.pack(fill="x", padx=80, pady=(0, 12))

            for i, entry in enumerate(self._session_history[:5]):
                if i > 0:
                    tk.Frame(feed, bg=self.C("border"),
                             height=1).pack(fill="x", padx=12)
                row = tk.Frame(feed, bg=self.C("panel"))
                row.pack(fill="x", padx=14, pady=5)
                ok = "✅" in entry["status"]
                tk.Label(row, text="✅" if ok else "❌",
                         bg=self.C("panel"),
                         font=(_UI_FONT, 9)).pack(side="left", padx=(0, 10))
                tk.Label(row, text=entry["action"],
                         bg=self.C("panel"), fg=self.C("card_title"),
                         font=(_UI_FONT, 9, "bold"),
                         width=24, anchor="w").pack(side="left")
                tk.Label(row, text=entry.get("server", ""),
                         bg=self.C("panel"), fg=self.C("muted"),
                         font=(_UI_FONT, 8),
                         width=14, anchor="w").pack(side="left")
                tk.Label(row, text=entry["ts"],
                         bg=self.C("panel"), fg=self.C("dim"),
                         font=(_UI_FONT, 7)).pack(side="right")


    def _hero_card(self, parent, col, icon, title, subtitle, bullets, cmd):
        """Home page hero card — same 3D style as upload cards, full-clickable."""
        parent.columnconfigure(col, weight=1, uniform="hero")

        outer = tk.Frame(parent,
                         highlightbackground=self.C("border"),
                         highlightthickness=1,
                         bg=self.C("border"))
        outer.grid(row=0, column=col, padx=20, pady=4, sticky="nsew")

        card = tk.Frame(outer, bg=self.C("panel"), cursor="hand2")
        card.pack(fill="both", expand=True)

        # Top accent bar
        accent = tk.Frame(card, bg=self.C("primary"), height=5)
        accent.pack(fill="x")

        body = tk.Frame(card, bg=self.C("panel"))
        body.pack(fill="both", expand=True, padx=24, pady=(18, 20))

        # Icon circle
        ic = tk.Canvas(body, width=60, height=60,
                       bg=self.C("panel"), highlightthickness=0)
        ic.pack(anchor="w", pady=(0, 12))
        ic.create_oval(2, 2, 58, 58, fill=self.C("input_bg"), outline="")
        ic.create_text(30, 30, text=icon,
                       font=(_UI_FONT, 24), fill=self.C("card_title"))

        tk.Label(body, text=title,
                 bg=self.C("panel"), fg=self.C("card_title"),
                 font=(_UI_FONT, 16, "bold"),
                 anchor="w", wraplength=260).pack(anchor="w")

        tk.Label(body, text=subtitle,
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 10),
                 anchor="w", wraplength=260).pack(anchor="w", pady=(3, 12))


        def _collect():
            ws = [outer, card, body, ic]
            for w in [card, body]:
                try:
                    for c in w.winfo_children():
                        ws.append(c)
                        for gc in c.winfo_children():
                            ws.append(gc)
                except: pass
            return ws

        def _on(e=None):
            accent.config(bg=self.C("success"))
            outer.config(highlightbackground=self.C("success"))
            for w in _collect():
                try: w.config(bg=self.C("input_bg"))
                except: pass
            ic.config(bg=self.C("input_bg"))

        def _off(e=None):
            accent.config(bg=self.C("primary"))
            outer.config(highlightbackground=self.C("border"))
            for w in _collect():
                try: w.config(bg=self.C("panel"))
                except: pass
            ic.config(bg=self.C("panel"))

        def _click(e=None): cmd()

        for w in _collect():
            try:
                w.bind("<Enter>",    _on)
                w.bind("<Leave>",    _off)
                w.bind("<Button-1>", _click)
            except: pass

    def show_generate_data(self):
        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Generate Data page")

        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(25, 5))
        tk.Label(hdr, text="⚙️  Generate Data",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", padx=10, pady=4,
                  command=self.show_first_page).pack(side="right")
        tk.Frame(main, bg=self.C("border"), height=1).pack(
            fill="x", padx=50, pady=(5, 10))

        # Scrollable so history table is reachable
        canvas, sf = self._scrollable(main)
        container = tk.Frame(sf, bg=self.C("bg"))
        container.pack(fill="x", padx=50, pady=5)

        tk.Label(container, text="Select a generator to get started",
                 bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT, 10)).pack(pady=(0, 14))

        cards_frame = tk.Frame(container, bg=self.C("bg"))
        cards_frame.pack(fill="x")

        gen_items = [
            ("💬", "SMS IRI Generator",
             "PDU-encoded IRI files",
             ["UTF-16 multi-language", "Bulk CSV input", "Auto Call ID"],
             self.show_sms_generator),
            ("📍", "LBS / LUDR Generator",
             "Location-based CDR files",
             ["Interactive map picker", "Search any location", "Export as CSV"],
             self.show_lbs_generator),
            ("📞", "Voice Call Generator",
             "Hi2 + Hi3 call intercept files",
             ["Auto-named HI2 files", "WAV copy to Hi3", "Live file preview"],
             self.show_voice_generator),
            ("📞", "Bulk Voice Generator",
             "Generate many calls at once",
             ["CSV mode — one row per call", "Quick mode — form + count",
              "Normal + Conference support", "Per-call WAV from folder"],
             self.show_bulk_voice_generator),
        ]

        for col, (icon, title, subtitle, bullets, cmd) in enumerate(gen_items):
            self._feature_card(cards_frame, col, icon, title, subtitle, bullets, cmd)

        # ── Generate History — inline table ───────────────────────
        self._section_label(container, "📋  Recent Generate History",
                            pady=(28, 8))

        import csv as _csv, os as _os

        HIST_FILE = _os.path.join(
            _os.path.dirname(_os.path.abspath(__file__)),
            "comtrail_upload_history.csv")

        hist_card = tk.Frame(container,
                             bg=self.C("panel"),
                             highlightbackground=self.C("border"),
                             highlightthickness=1)
        hist_card.pack(fill="x", pady=(0, 16))

        # Card header bar
        top_bar = tk.Frame(hist_card, bg=self.C("primary"), height=36)
        top_bar.pack(fill="x")
        top_bar.pack_propagate(False)
        tk.Frame(top_bar, bg="#06b6d4", height=2).pack(fill="x", side="top")
        top_inner = tk.Frame(top_bar, bg=self.C("primary"))
        top_inner.pack(fill="x", padx=12, expand=True)
        tk.Label(top_inner, text="⚙️  Last 10 Generated Files",
                 bg=self.C("primary"), fg="white",
                 font=(_UI_FONT, 9, "bold")).pack(side="left", pady=7)
        gen_status = tk.StringVar(value="")
        tk.Label(top_inner, textvariable=gen_status,
                 bg=self.C("primary"), fg="#a5f3fc",
                 font=(_UI_FONT, 8)).pack(side="right", pady=7)

        # Column headers
        COL_DEFS = [
            ("Date / Time",  160, "center"),
            ("Generator",    140, "w"),
            ("Files",         50, "center"),
            ("Output",       160, "w"),
            ("Status",        80, "center"),
            ("Note",         190, "w"),
        ]
        _hdr_bg = "#0c4a5a" if self._theme == "dark" else "#0891b2"
        hdr_row = tk.Frame(hist_card, bg=_hdr_bg)
        hdr_row.pack(fill="x")
        for col_name, col_w, col_anchor in COL_DEFS:
            tk.Label(hdr_row, text=col_name,
                     bg=_hdr_bg, fg="white",
                     font=(_UI_FONT, 8, "bold"),
                     width=col_w // 8,
                     anchor=col_anchor).pack(
                side="left", padx=1, pady=5, ipadx=4)

        rows_frame = tk.Frame(hist_card, bg=self.C("panel"))
        rows_frame.pack(fill="x")

        def _load_gen_hist():
            for w in rows_frame.winfo_children():
                w.destroy()
            rows = []
            if _os.path.isfile(HIST_FILE):
                try:
                    with open(HIST_FILE, "r",
                              encoding="utf-8-sig", newline="") as f:
                        all_rows = list(_csv.DictReader(f))
                    # Only generate actions
                    rows = [r for r in reversed(all_rows)
                            if "Generate" in r.get("action", "")][:10]
                except Exception:
                    pass

            if not rows:
                tk.Label(rows_frame,
                         text="No generate history yet — "
                              "generated files will appear here.",
                         bg=self.C("panel"), fg=self.C("dim"),
                         font=(_UI_FONT, 9, "italic")).pack(
                    pady=16, padx=16)
                gen_status.set("No records")
                return

            gen_status.set(f"{len(rows)} recent record(s)")

            for i, row in enumerate(rows):
                status = row.get("status", "")
                if self._theme == "dark":
                    row_bg = "#1a2133" if i % 2 == 0 else "#161b22"
                else:
                    row_bg = "#f8faff" if i % 2 == 0 else "#ffffff"

                if "✅" in status or "OK" in status.upper():
                    st_fg = "#22c55e"
                    st_bg = "#14532d" if self._theme=="dark" else "#dcfce7"
                elif "❌" in status or "FAIL" in status.upper():
                    st_fg = "#ef4444"
                    st_bg = "#7f1d1d" if self._theme=="dark" else "#fee2e2"
                else:
                    st_fg, st_bg = self.C("muted"), row_bg

                fr = tk.Frame(rows_frame, bg=row_bg)
                fr.pack(fill="x")

                # Accent colour by generator type
                action = row.get("action", "")
                accent = ("#8b5cf6" if "Voice" in action
                          else "#06b6d4" if "SMS"   in action
                          else "#10b981" if "LBS"   in action
                          else "#f59e0b")
                tk.Frame(fr, bg=accent, width=3).pack(side="left", fill="y")

                server_val = row.get("server", "-")
                cells = [
                    (row.get("ts",    "-"),    160, "center",
                     self.C("muted"),       ("Consolas", 8)),
                    (action,                  140, "w",
                     self.C("card_title"),  (_UI_FONT, 8, "bold")),
                    (str(row.get("files","-")), 50, "center",
                     self.C("card_title"),  (_UI_FONT, 8)),
                    (server_val[:22],         160, "w",
                     self.C("muted"),       (_UI_FONT, 8)),
                    (status[:12],              80, "center",
                     st_fg,                 (_UI_FONT, 8, "bold")),
                    (row.get("note","")[:32], 190, "w",
                     self.C("dim"),         (_UI_FONT, 8)),
                ]
                for val, w, anchor, fg, fnt in cells:
                    cell_bg = st_bg if val == status[:12] else row_bg
                    tk.Label(fr, text=val, bg=cell_bg, fg=fg,
                             font=fnt, width=w // 8,
                             anchor=anchor).pack(
                        side="left", padx=1, pady=4, ipadx=4)

                sep_col = "#1e293b" if self._theme=="dark" else "#e2e8f0"
                tk.Frame(rows_frame, bg=sep_col,
                         height=1).pack(fill="x")

        _load_gen_hist()

        # Footer
        foot = tk.Frame(hist_card, bg=self.C("input_bg"))
        foot.pack(fill="x")
        tk.Frame(foot, bg=self.C("border"), height=1).pack(fill="x")
        btn_fr = tk.Frame(foot, bg=self.C("input_bg"))
        btn_fr.pack(anchor="e", padx=12, pady=6)
        tk.Button(btn_fr, text="🔄  Refresh",
                  bg=self.C("primary"), fg="white",
                  activebackground="#0e7490", activeforeground="white",
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2",
                  command=_load_gen_hist).pack(
            side="left", ipadx=10, ipady=4, padx=(0, 8))
        tk.Button(btn_fr, text="📊  View Full History",
                  bg="#374151", fg="white",
                  activebackground="#4b5563", activeforeground="white",
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2",
                  command=self.show_upload_history).pack(
            side="left", ipadx=10, ipady=4)

    def _feature_card(self, parent, col, icon, title, subtitle, bullets, cmd):
        """3D generator card — auto-sizes to fill grid column, all text visible."""
        # Make grid columns equal weight so cards fill the row evenly
        parent.columnconfigure(col, weight=1, uniform="gen")

        outer = tk.Frame(parent,
                         highlightbackground=self.C("border"),
                         highlightthickness=1,
                         bg=self.C("border"))
        outer.grid(row=0, column=col, padx=12, pady=4, sticky="nsew")

        card = tk.Frame(outer, bg=self.C("panel"), cursor="hand2")
        card.pack(fill="both", expand=True)

        # Top accent bar
        accent = tk.Frame(card, bg=self.C("primary"), height=4)
        accent.pack(fill="x")

        body = tk.Frame(card, bg=self.C("panel"))
        body.pack(fill="both", expand=True, padx=20, pady=(16, 18))

        # Icon in circle
        ic = tk.Canvas(body, width=52, height=52,
                       bg=self.C("panel"), highlightthickness=0)
        ic.pack(anchor="w", pady=(0, 10))
        ic.create_oval(2, 2, 50, 50, fill=self.C("input_bg"), outline="")
        ic.create_text(26, 26, text=icon, font=(_UI_FONT, 20),
                       fill=self.C("card_title"))

        tk.Label(body, text=title,
                 bg=self.C("panel"), fg=self.C("card_title"),
                 font=(_UI_FONT, 13, "bold"),
                 anchor="w", wraplength=220).pack(anchor="w")

        tk.Label(body, text=subtitle,
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9),
                 anchor="w", wraplength=220).pack(anchor="w", pady=(3, 10))

        for b in bullets:
            br = tk.Frame(body, bg=self.C("panel"))
            br.pack(anchor="w", fill="x", pady=2)
            tk.Label(br, text="›", bg=self.C("panel"),
                     fg=self.C("primary"),
                     font=(_UI_FONT, 11, "bold"),
                     width=2).pack(side="left")
            tk.Label(br, text=b, bg=self.C("panel"),
                     fg=self.C("subtle"),
                     font=(_UI_FONT, 9),
                     anchor="w", wraplength=200,
                     justify="left").pack(side="left", fill="x", expand=True)

        def _collect():
            ws = [outer, card, body, ic]
            for w in [card, body]:
                try:
                    for c in w.winfo_children():
                        ws.append(c)
                        for gc in c.winfo_children():
                            ws.append(gc)
                except: pass
            return ws

        def _on(e=None):
            accent.config(bg=self.C("success"))
            outer.config(highlightbackground=self.C("success"))
            for w in _collect():
                try: w.config(bg=self.C("input_bg"))
                except: pass
            ic.config(bg=self.C("input_bg"))

        def _off(e=None):
            accent.config(bg=self.C("primary"))
            outer.config(highlightbackground=self.C("border"))
            for w in _collect():
                try: w.config(bg=self.C("panel"))
                except: pass
            ic.config(bg=self.C("panel"))

        def _click(e=None): cmd()

        for w in _collect():
            try:
                w.config(cursor="hand2")
            except Exception:
                pass
            try:
                w.bind("<Enter>",    _on)
                w.bind("<Leave>",    _off)
                w.bind("<Button-1>", _click)
            except: pass

    # ──────────────────────────────────────────────────────────────
    # SMS IRI GENERATOR
    # ──────────────────────────────────────────────────────────────
    def show_sms_generator(self):
        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened SMS IRI Generator")

        # ── Header ─────────────────────────────────────────────────
        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(20, 5))
        tk.Label(hdr, text="💬  SMS IRI Generator",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", padx=10, pady=4,
                  command=self.show_generate_data).pack(side="right")
        tk.Frame(main, bg=self.C("border"), height=1).pack(fill="x", padx=50, pady=(5, 10))

        # ── Scrollable body ─────────────────────────────────────────
        canvas, sf = self._scrollable(main)
        body = tk.Frame(sf, bg=self.C("bg"))
        body.pack(fill="x", padx=50, pady=5)

        # ══════════════════════════════════════════════════════════
        # SECTION 1 — Files
        # ══════════════════════════════════════════════════════════
        _, _sec1 = self._collapsible_section(body, "📂  Files", pady=(5, 6))
        file_card = self._card(_sec1)

        _ss = self._sms_state
        csv_var    = tk.StringVar(value=_ss.get("csv", ""))
        output_var = tk.StringVar(value=_ss.get("output", ""))

        def _sms_save(*_):
            self._sms_state["csv"]    = csv_var.get()
            self._sms_state["output"] = output_var.get()
        csv_var.trace_add("write", _sms_save)
        output_var.trace_add("write", _sms_save)

        # (#8) CSV preview frame — declared first so callbacks can reference it
        csv_preview_frame = tk.Frame(file_card, bg=self.C("panel"))

        def _refresh_csv_preview(*_):
            self._build_csv_preview(csv_preview_frame, csv_var.get())

        for lbl, var, cmd in [
            ("Input CSV File", csv_var,
             lambda: (csv_var.set(filedialog.askopenfilename(
                 title="Select CSV",
                 filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])) or
                 _refresh_csv_preview())),
            ("Output Folder",  output_var,
             lambda: output_var.set(filedialog.askdirectory(
                 title="Select Output Folder"))),
        ]:
            r = tk.Frame(file_card, bg=self.C("panel"))
            r.pack(fill="x", pady=4, padx=8)
            tk.Label(r, text=lbl, width=16, anchor="w",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9)).pack(side="left")
            ent = tk.Entry(r, textvariable=var, bg=self.C("input_bg"),
                           fg=self.C("text"),
                           insertbackground=self.C("text"), relief="flat",
                           width=55)
            ent.pack(side="left", ipady=5, padx=(0, 8))
            # (#4) Drag-and-drop
            self._enable_drop(ent, var,
                              callback=_refresh_csv_preview
                              if var is csv_var else None)
            tk.Button(r, text="Browse…", bg=self.C("primary"), fg="white",
                      relief="flat", font=(_UI_FONT, 9, "bold"),
                      activebackground=self.C("border"),
                      command=cmd).pack(side="left", ipady=3)

        csv_preview_frame.pack(fill="x", padx=8, pady=(0, 4))
        csv_var.trace_add("write", _refresh_csv_preview)

        # Sample CSV download for SMS
        sms_sample_row = tk.Frame(file_card, bg=self.C("panel"))
        sms_sample_row.pack(fill="x", pady=(2, 6), padx=8)
        tk.Label(sms_sample_row, text="Need a template?", width=16, anchor="w",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8)).pack(side="left")

        def download_sms_sample():
            path = filedialog.asksaveasfilename(
                title="Save SMS Sample CSV",
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv")],
                initialfile="sms_sample.csv")
            if not path:
                return
            rows = [
                ["Test-TextData", "SenderName", "Direction", "IMEI", "IMSI",
                 "MCC", "MNC", "LAC", "CI", "Timestamp"],
                ["Hello, your OTP is 452891. Valid for 10 minutes.",
                 "911234567890", "incoming", "354076826454420", "405863169296834",
                 "405", "86", "304A", "5A18", "21-07-2025 16:08:02"],
                ["Your account balance is Rs. 5,230.",
                 "HDFC Bank", "incoming", "354076826454421", "405863169296835",
                 "405", "45", "2341", "67891", "21-07-2025 16:09:15"],
                ["Aapka verification code hai: 781234",
                 "912345678901", "incoming", "354076826454422", "405863169296836",
                 "404", "20", "1300", "3344", "21-07-2025 16:10:30"],
                ["Your delivery is out for delivery. Track: xyz.com/track/123",
                 "AMAZON", "incoming", "", "",
                 "", "", "", "", "21-07-2025 16:11:00"],
                ["Meeting reminder: Team sync at 3 PM today.",
                 "913344556677", "outgoing", "354076826454423", "405863169296837",
                 "405", "86", "1500", "7788", ""],
            ]
            try:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerows(rows)
                LOG.log("SMS Generator", f"Sample CSV saved → {path}")
                self.popup("Saved", f"SMS sample CSV saved to:\n{os.path.basename(path)}", "success")
            except Exception as e:
                self.popup("Error", str(e), "error")

        tk.Button(sms_sample_row, text="⬇️  Download Sample CSV",
                  bg=self.C("primary"), fg="white", relief="flat",
                  font=(_UI_FONT, 9, "bold"), activebackground=self.C("border"),
                  command=download_sms_sample).pack(side="left", ipady=3, padx=2)
        tk.Label(sms_sample_row,
                 text="  ← download a pre-filled example to see the expected format",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8)).pack(side="left")

        # ══════════════════════════════════════════════════════════
        # SECTION 2 — Target / LIID / MSISDN (UI inputs)
        # ══════════════════════════════════════════════════════════
        _, _sec2 = self._collapsible_section(body, "📋  Interception Parameters", pady=(18, 6))
        param_card = self._card(_sec2)

        target_var  = tk.StringVar(value=_ss.get("target", "919182556256"))
        target_var.trace_add("write", lambda *_: self._sms_state.update({"target": target_var.get()}))
        # LIID and MSISDN always mirror Target Number
        msisdn_var  = target_var

        param_grid = tk.Frame(param_card, bg=self.C("panel"))
        param_grid.pack(fill="x", padx=10, pady=8)

        def _pfield(parent, label, var, col, hint="", required=False, numeric=False):
            f = tk.Frame(parent, bg=self.C("panel"))
            f.grid(row=0, column=col, padx=15, pady=4, sticky="w")
            lbl_text = f"{'* ' if required else ''}{label}"
            tk.Label(f, text=lbl_text, bg=self.C("panel"),
                     fg="#ef4444" if required else self.C("muted"),
                     font=(_UI_FONT, 9, "bold" if required else "normal")).pack(anchor="w")
            border = tk.Frame(f, bg="#ef4444" if required else self.C("border"),
                              padx=1, pady=1)
            border.pack()
            ent = tk.Entry(border, textvariable=var, bg=self.C("input_bg"),
                           fg=self.C("text"), insertbackground=self.C("text"),
                           relief="flat", width=20)
            ent.pack(ipady=5)
            if numeric:
                def _strip_non_digits(*_, v=var):
                    val = v.get()
                    cleaned = ''.join(c for c in val if c.isdigit())
                    if cleaned != val:
                        v.set(cleaned)
                var.trace_add("write", _strip_non_digits)
            if required:
                def _chk(*_, b=border, v=var):
                    b.config(bg="#ef4444" if not v.get().strip() else self.C("success"))
                var.trace_add("write", _chk)
                _chk()
            if hint:
                tk.Label(f, text=hint, bg=self.C("panel"), fg=self.C("dim"),
                         font=(_UI_FONT, 7)).pack(anchor="w")

        _pfield(param_grid, "Target Number", target_var, col=0,
                required=True, numeric=True,
                hint="MSISDN & LIID are automatically set to this value")

        tk.Label(param_card,
                 text="  MSISDN = LIID = Target Number.  "
                      "Per-row CSV fields (IMEI, IMSI, MCC, MNC, LAC, CI, Timestamp, Direction) still override per row.",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8)).pack(anchor="w", padx=10, pady=(0, 6))

        # ══════════════════════════════════════════════════════════
        # ══════════════════════════════════════════════════════════
        # SECTION 4 — Generate + Results
        # ══════════════════════════════════════════════════════════
        _, _sec4 = self._collapsible_section(body, "🚀  Generate", pady=(18, 6))
        gen_card = self._card(_sec4)

        status_var = tk.StringVar(value="Ready — fill in the fields above and click Generate.")
        status_lbl = tk.Label(gen_card, textvariable=status_var,
                              bg=self.C("panel"), fg=self.C("muted"),
                              font=(_UI_FONT, 9), wraplength=900, justify="left")
        status_lbl.pack(anchor="w", padx=10, pady=(8, 4))

        # Results tree
        rf = tk.Frame(gen_card, bg=self.C("panel"))
        rf.pack(fill="x", padx=10, pady=(0, 6))

        style = ttk.Style()
        style.configure("Gen.Treeview",
                        background=self.C("input_bg"), foreground=self.C("text"),
                        rowheight=22, fieldbackground=self.C("input_bg"), borderwidth=0)
        style.configure("Gen.Treeview.Heading",
                        background=self.C("primary"), foreground="white",
                        font=(_UI_FONT, 9, "bold"), relief="flat")
        style.map("Gen.Treeview", background=[("selected", self.C("primary"))], foreground=[("selected", "#ffffff")])

        tcols = ("File", "CallID", "Parts", "Sender", "Direction", "Preview")
        res_tree = ttk.Treeview(rf, columns=tcols, show="headings",
                                height=10, style="Gen.Treeview")
        for col, w in [("File",160),("CallID",80),("Parts",55),
                       ("Sender",130),("Direction",90),("Preview",350)]:
            res_tree.heading(col, text=col)
            res_tree.column(col, width=w, anchor="w")
        vsb = ttk.Scrollbar(rf, orient="vertical", command=res_tree.yview)
        res_tree.configure(yscrollcommand=vsb.set)
        res_tree.pack(side="left", fill="x", expand=True)
        vsb.pack(side="right", fill="y")
        _bind_mousewheel(res_tree, vsb)

        btn_row = tk.Frame(gen_card, bg=self.C("panel"))
        btn_row.pack(anchor="w", padx=10, pady=(4, 10))

        def open_output():
            p = output_var.get().strip()
            if p and os.path.isdir(p):
                webbrowser.open(f"file:///{p.replace(os.sep, '/')}")

        tk.Button(btn_row, text="📁  Open Output Folder",
                  bg="#444", fg="white", relief="flat",
                  font=(_UI_FONT, 9, "bold"), activebackground="#555",
                  width=22, command=open_output).pack(side="left", padx=(0, 10))

        def do_generate():
            csv_path = csv_var.get().strip()
            out_path = output_var.get().strip()
            target   = target_var.get().strip()
            liid     = target   # LIID always equals Target Number
            msisdn   = msisdn_var.get().strip()

            # Validate
            if not csv_path or not os.path.isfile(csv_path):
                return self.popup("Error", "Please select a valid CSV file.", "error")
            if not out_path:
                return self.popup("Error", "Please select an output folder.", "error")
            if not target:
                return self.popup("Error", "Target Number is required.", "error")
            if not msisdn:
                return self.popup("Error", "MSISDN is required.", "error")

            status_var.set("Generating IRI files…")
            status_lbl.config(fg=self.C("muted"))
            res_tree.delete(*res_tree.get_children())

            def task():
                results = []
                error   = None
                try:
                    os.makedirs(out_path, exist_ok=True)
                    ref     = 0

                    with open(csv_path, "r", encoding="utf-8-sig") as f:
                        rows = list(csv.DictReader(f))

                    LOG.log("SMS Generator",
                            f"Processing {len(rows)} row(s)  "
                            f"Target={target}  LIID={liid}  MSISDN={msisdn}")

                    for row in rows:
                        text   = row.get("Test-TextData", "").strip().strip('"')
                        sender = row.get("SenderName",    "").strip().strip('"') or "Unknown"
                        if not text:
                            continue

                        # Per-row optional fields from CSV
                        direction = row.get("Direction",  "").strip().strip('"') or "incoming"
                        imei      = row.get("IMEI",       "").strip().strip('"') or "000000000000000"
                        imsi      = row.get("IMSI",       "").strip().strip('"') or "000000000000000"
                        timestamp = row.get("Timestamp",  "").strip().strip('"') or \
                                    time.strftime("%d-%m-%Y %H:%M:%S")

                        mcc = row.get("MCC", "").strip().strip('"')
                        mnc = row.get("MNC", "").strip().strip('"')
                        lac = row.get("LAC", "").strip().strip('"')
                        ci  = row.get("CI",  "").strip().strip('"')
                        network_id = (f"{mcc}:{mnc}:{lac}:{ci}"
                                      if mcc and mnc and lac and ci
                                      else "000:00:0000:0000")
                        # CellId = MCC+MNC+LAC+CI concatenated (no separators)
                        cell_id = (f"{mcc}{mnc}{lac}{ci}".upper()
                                   if mcc and mnc and lac and ci
                                   else "")
                        # CellId column override
                        cell_id = row.get("CellId", cell_id).strip().strip('"') or cell_id
                        tower_db = row.get("TowerDB", "").strip().strip('"')

                        # DA in PDU must match the "To" field in the IRI file.
                        # IRI rule (same as _sms_create_iri_file):
                        #   incoming → To = target (MSISDN) → DA = target
                        #   outgoing → To = sender           → DA = sender
                        # CSV "To" column can override this per-row.
                        csv_to   = row.get("To", "").strip().strip('"')
                        if csv_to:
                            # Explicit To from CSV — use as-is
                            pdu_da = csv_to
                        elif direction.lower() == "outgoing":
                            # Outgoing: message goes TO the sender (recipient)
                            pdu_da = sender
                        else:
                            # Incoming: message goes TO the target (MSISDN)
                            pdu_da = msisdn  # MSISDN from UI = subscriber number

                        # PDU encode with correct DA matching IRI To field
                        pdus = _sms_text_to_pdu(text, ref=ref, to_number=pdu_da)
                        call_id = _next_call_id()

                        for seq, (hex_pdu, _) in enumerate(pdus, 1):
                            fp = _sms_create_iri_file(
                                output_dir    = out_path,
                                sender        = sender,
                                sms_text      = text,
                                pdu_hex       = hex_pdu,
                                call_id       = call_id,
                                seq_num       = seq,
                                liid          = liid,       # from UI
                                target_number = target,     # from UI → To field
                                imei          = imei,
                                imsi          = imsi,
                                msisdn        = msisdn,     # from UI
                                timestamp     = timestamp,
                                network_id    = network_id,
                                call_direction= direction,
                                cell_id       = cell_id,
                            )
                            results.append((
                                os.path.basename(fp),
                                str(call_id),
                                f"{seq}/{len(pdus)}",
                                sender,
                                direction,
                                text[:55] + ("…" if len(text) > 55 else ""),
                            ))
                            tdb_note = f" | Tower: {tower_db}" if tower_db else ""
                            LOG.log("SMS Generator",
                                    f"  ✓ {os.path.basename(fp)}" + (f" | CellId: {cell_id}{tdb_note}" if cell_id else ""))

                        ref = (ref + 1) % 256

                    LOG.log("SMS Generator",
                            f"Done — {len(results)} IRI file(s) → {out_path}")

                except Exception as exc:
                    import traceback
                    error = str(exc)
                    LOG.log("SMS Generator",
                            f"FAILED: {exc}\n{traceback.format_exc()}", "ERROR")

                def _ui():
                    res_tree.delete(*res_tree.get_children())
                    for r in results:
                        res_tree.insert("", "end", values=r)
                    if error:
                        status_var.set(f"❌  {error}")
                        status_lbl.config(fg=self.C("error"))
                        self._write_history("SMS Generate", 0, "local",
                                            "❌ Failed", error[:60])
                    else:
                        status_var.set(
                            f"✅  {len(results)} IRI file(s) generated  →  {out_path}")
                        status_lbl.config(fg=self.C("success"))
                        self._write_history("SMS Generate", len(results),
                                            "local", "✅ OK", out_path)
                self.after(0, _ui)

            threading.Thread(target=task, daemon=True).start()

        tk.Button(btn_row, text="🚀  Generate IRI Files",
                  bg=self.C("success"), fg="white", relief="flat",
                  font=(_UI_FONT, 10, "bold"), activebackground=self.C("border"),
                  width=22, command=do_generate).pack(side="left", padx=(0, 10))
        main.bind_all("<Control-Return>", lambda e: do_generate())

        def do_upload_sms():
            out_path = output_var.get().strip()
            if not out_path or not os.path.isdir(out_path):
                return self.popup("Error",
                    "No output folder selected or folder does not exist.\n"
                    "Generate files first, then click Upload.", "error")
            files = [
                os.path.join(out_path, fn)
                for fn in os.listdir(out_path)
                if fn.lower().endswith(".txt")
            ]
            if not files:
                return self.popup("Error",
                    f"No .txt IRI files found in:\n{out_path}", "error")
            ip   = self.cfg["ludr"].get("ip",   "")
            path = self.cfg["ludr"].get("path", "")
            if not ip:
                return self.popup("Error",
                    "LUDR/SMS server not configured. Go to Settings.", "error")
            sftp = CONN.get_sftp("ludr")
            if not sftp:
                return self.popup("Error",
                    "LUDR server is not connected. Please check Settings.", "error")
            total = len(files)
            LOG.log("Upload", f"SMS IRI upload — {total} file(s) → {ip}:{path}")
            top, _ = self.progress_window(f"Uploading SMS IRI — {total} file(s)…")

            def _task():
                try:
                    for idx, f in enumerate(files):
                        fn  = os.path.basename(f)
                        pct = int((idx + 1) * 100 / total)
                        self.after(0, lambda i=idx+1, n=fn, p=pct:
                            top._set_status(
                                f"Uploading file {i} of {total}…",
                                pct=p, sub=n) if top.winfo_exists() else None)
                        sftp.put(f, path.rstrip("/") + "/" + fn)
                        LOG.log("Upload", f"  {fn} → {path}")
                    LOG.log("Upload", f"SMS IRI upload complete — {total} file(s)")
                    self._write_history("LBS Upload", total, ip, "✅ OK",
                                        f"{total} SMS IRI file(s)")
                    self.after(0, lambda: (
                        top.destroy() if top.winfo_exists() else None,
                        self._toast(
                            f"✅  SMS IRI upload complete\n{total} file(s) transferred.",
                            "success")))
                except Exception as e:
                    LOG.log("Upload", f"SMS IRI upload failed: {e}", "ERROR")
                    self._write_history("LBS Upload", 0, ip, "❌ Failed", str(e)[:60])
                    self.after(0, lambda err=e: (
                        top.destroy() if top.winfo_exists() else None,
                        self.popup("Error", str(err), "error")))
            threading.Thread(target=_task, daemon=True).start()

        tk.Button(btn_row, text="📤  Upload to Server",
                  bg=self.C("primary"), fg="white", relief="flat",
                  font=(_UI_FONT, 10, "bold"), activebackground=self.C("border"),
                  width=22, command=do_upload_sms).pack(side="left")

    # ──────────────────────────────────────────────────────────────
    # LBS / LUDR CDR GENERATOR
    # ──────────────────────────────────────────────────────────────
    def show_lbs_generator(self):
        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened LBS/LUDR CDR Generator")

        # ── Header ─────────────────────────────────────────────────
        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(20, 5))
        tk.Label(hdr, text="📍  LBS / LUDR CDR Generator",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", padx=10, pady=4,
                  command=self.show_generate_data).pack(side="right")
        tk.Frame(main, bg=self.C("border"), height=1).pack(fill="x", padx=50, pady=(5, 10))

        canvas, sf = self._scrollable(main)
        body = tk.Frame(sf, bg=self.C("bg"))
        body.pack(fill="x", padx=40, pady=(6, 10))

        # ── Shared vars ────────────────────────────────────────────
        _ls = self._lbs_state
        csv_var    = tk.StringVar(value=_ls.get("csv",    ""))
        output_var = tk.StringVar(value=_ls.get("output", ""))
        target_var = tk.StringVar(value=_ls.get("target", "918989123123"))
        msisdn_var = target_var   # MSISDN always mirrors Target Number

        def _lbs_save(*_):
            self._lbs_state.update({
                "csv":    csv_var.get(),
                "output": output_var.get(),
                "target": target_var.get(),
            })
        csv_var.trace_add("write", _lbs_save)
        output_var.trace_add("write", _lbs_save)
        target_var.trace_add("write", _lbs_save)

        # ── Single unified panel card ──────────────────────────────
        main_panel = tk.Frame(body,
                              highlightbackground=self.C("border"),
                              highlightthickness=1,
                              bg=self.C("panel"))
        main_panel.pack(fill="x")
        tk.Frame(main_panel, bg=self.C("primary"), height=4).pack(fill="x")

        def _sec(title):
            """Compact inline section divider + label."""
            tk.Frame(main_panel, bg=self.C("border"), height=1).pack(fill="x")
            hf = tk.Frame(main_panel, bg=self.C("input_bg"))
            hf.pack(fill="x")
            tk.Label(hf, text=title,
                     bg=self.C("input_bg"), fg=self.C("card_title"),
                     font=(_UI_FONT, 9, "bold"),
                     padx=14, pady=7).pack(side="left")

        # ── SECTION 1: Map Coordinate Picker ──────────────────────
        _sec("🗺️   Map Coordinate Picker")
        map_card = tk.Frame(main_panel, bg=self.C("panel"))
        map_card.pack(fill="x")

        tk.Label(map_card,
                 text="Pick coordinates on the map · enter date/time per point · "
                      "export a complete LBS CSV with IMEI and IMSI included.",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8)).pack(anchor="w", padx=14, pady=(8, 6))

        # ── IMEI / IMSI fields (compact, inline) ──────────────────
        id_row = tk.Frame(map_card, bg=self.C("panel"))
        id_row.pack(fill="x", padx=10, pady=(0, 8))

        for lbl_text, default, width in [
            ("IMEI", "354076826454420", 22),
            ("IMSI", "405863169296834", 22),
        ]:
            tk.Label(id_row, text=lbl_text + ":",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9)).pack(side="left", padx=(0, 4))
            var = tk.StringVar(value=default)
            tk.Entry(id_row, textvariable=var, width=width,
                     bg=self.C("input_bg"), fg=self.C("text"),
                     insertbackground=self.C("text"),
                     relief="flat", font=(_UI_FONT, 9)).pack(
                side="left", ipady=4, padx=(0, 18))
            if lbl_text == "IMEI":
                map_imei_var = var
            else:
                map_imsi_var = var

        tk.Label(id_row,
                 text="← used in exported CSV",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 7)).pack(side="left")

        # ── Coordinate table ──────────────────────────────────────
        tbl_frame = tk.Frame(map_card, bg=self.C("panel"))
        tbl_frame.pack(fill="x", padx=10, pady=(0, 6))

        style = ttk.Style()
        style.configure("Map.Treeview",
                        background=self.C("input_bg"), foreground=self.C("text"),
                        rowheight=22, fieldbackground=self.C("input_bg"), borderwidth=0)
        style.configure("Map.Treeview.Heading",
                        background=self.C("primary"), foreground="white",
                        font=(_UI_FONT, 9, "bold"), relief="flat")
        style.map("Map.Treeview",
                  background=[("selected", self.C("primary"))],
                  foreground=[("selected", "#ffffff")])

        map_cols = ("#", "Latitude", "Longitude", "Event", "Timestamp")
        map_tree = ttk.Treeview(tbl_frame, columns=map_cols, show="headings",
                                height=6, style="Map.Treeview")
        for col, w in [("#", 40), ("Latitude", 130), ("Longitude", 130),
                       ("Event", 100), ("Timestamp", 180)]:
            map_tree.heading(col, text=col)
            map_tree.column(col, width=w, anchor="w")
        map_vsb = ttk.Scrollbar(tbl_frame, orient="vertical", command=map_tree.yview)
        map_tree.configure(yscrollcommand=map_vsb.set)
        map_tree.pack(side="left", fill="x", expand=True)
        map_vsb.pack(side="right", fill="y")
        _bind_mousewheel(map_tree, map_vsb)

        # Storage: [lat, lon, event, timestamp]
        picked_coords = []

        def refresh_table():
            map_tree.delete(*map_tree.get_children())
            for i, (lat, lon, ev, ts) in enumerate(picked_coords, 1):
                map_tree.insert("", "end", values=(i, lat, lon, ev, ts))

        def delete_selected():
            sel = map_tree.selection()
            if not sel:
                return
            idx = int(map_tree.item(sel[0])["values"][0]) - 1
            if 0 <= idx < len(picked_coords):
                picked_coords.pop(idx)
                refresh_table()

        def clear_all():
            picked_coords.clear()
            refresh_table()

        def export_map_csv():
            if not picked_coords:
                return self.popup("Info", "No coordinates picked yet.", "info")
            imei = map_imei_var.get().strip() or "354076826454420"
            imsi = map_imsi_var.get().strip() or "405863169296834"
            path = filedialog.asksaveasfilename(
                title="Save Map Coordinates as CSV",
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv")],
                initialfile="map_coordinates.csv")
            if not path:
                return
            try:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow([
                        "Latitude", "Longitude", "Timestamp", "Event",
                        "IMEI", "IMSI"
                    ])
                    for lat, lon, ev, ts in picked_coords:
                        w.writerow([lat, lon, ts, ev, imei, imsi])
                LOG.log("LBS Map Picker",
                        f"Exported {len(picked_coords)} coordinate(s) → {path}")
                try:
                    csv_var.set(path)
                    self.popup("Saved",
                               f"{len(picked_coords)} coordinate(s) exported to:\n"
                               f"{os.path.basename(path)}\n\n"
                               "✅ Automatically loaded as LBS Input CSV below.",
                               "success")
                except NameError:
                    self.popup("Saved",
                               f"{len(picked_coords)} coordinate(s) exported to:\n"
                               f"{os.path.basename(path)}\n\n"
                               "You can now use this file as the Input CSV for the LBS Generator.",
                               "success")
            except Exception as e:
                self.popup("Error", str(e), "error")

        # ── Controls row ──────────────────────────────────────────
        ctrl_row = tk.Frame(map_card, bg=self.C("panel"))
        ctrl_row.pack(fill="x", padx=10, pady=(4, 8))

        tk.Label(ctrl_row, text="Event for next click:",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(side="left", padx=(0, 6))
        event_pick_var = tk.StringVar(value="BEGIN")
        ttk.Combobox(ctrl_row, textvariable=event_pick_var,
                     values=["BEGIN", "CONTINUE", "END"],
                     state="readonly", width=12).pack(side="left", padx=(0, 15))

        tk.Button(ctrl_row, text="🗑️  Delete Selected",
                  bg="#555", fg="white", relief="flat",
                  font=(_UI_FONT, 9, "bold"), activebackground="#666",
                  command=delete_selected).pack(side="left", padx=(0, 6), ipady=3)
        tk.Button(ctrl_row, text="✖  Clear All",
                  bg=self.C("error"), fg="white", relief="flat",
                  font=(_UI_FONT, 9, "bold"), activebackground="#c0392b",
                  command=clear_all).pack(side="left", padx=(0, 15), ipady=3)
        tk.Button(ctrl_row, text="💾  Export Coords as CSV",
                  bg=self.C("success"), fg="white", relief="flat",
                  font=(_UI_FONT, 9, "bold"), activebackground=self.C("border"),
                  command=export_map_csv).pack(side="left", ipady=3)

        # ── Open Map button ───────────────────────────────────────
        map_btn_row = tk.Frame(map_card, bg=self.C("panel"))
        map_btn_row.pack(fill="x", padx=10, pady=(0, 10))

        map_status_var = tk.StringVar(value="")
        map_status_lbl = tk.Label(map_btn_row, textvariable=map_status_var,
                                  bg=self.C("panel"), fg=self.C("muted"),
                                  font=(_UI_FONT, 9))
        map_status_lbl.pack(side="right", padx=10)

        def open_map():
            """
            Spin up a tiny local HTTP server that:
            - Serves a self-contained Leaflet map page (GET /)
            - Receives coordinate POST from the page (POST /coord)
            - Shuts down after the user closes the browser tab or clicks Done
            The map page itself posts each click back to localhost so the
            app captures coordinates without any external dependency.
            """
            import http.server
            import urllib.parse
            import tempfile

            port = _find_free_port()
            ev   = event_pick_var.get()

            # Build the full self-contained HTML map with search + remove pin
            html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>ComTrail — Map Coordinate Picker</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:#0d1117; color:#e6edf3; font-family:'Segoe UI',sans-serif; overflow:hidden; }}

  /* ── Top header ── */
  #header {{
    background:#0891b2; padding:10px 16px;
    display:flex; align-items:center; gap:12px; flex-wrap:wrap;
  }}
  #header-left {{ min-width:0; }}
  #header-left h2 {{ font-size:15px; white-space:nowrap; color:#fff; }}
  #info {{ font-size:11px; color:#a5f3fc; margin-top:2px; }}

  /* ── Search bar ── */
  #search-wrap {{
    display:flex; align-items:center; gap:6px; flex:1; min-width:280px;
    position:relative;
  }}
  #search-input {{
    flex:1; padding:8px 14px; border-radius:6px;
    border:2px solid #67e8f9; background:#164e63; color:#e6edf3;
    font-size:13px; outline:none; transition:border-color .2s;
  }}
  #search-input::placeholder {{ color:#a5f3fc; }}
  #search-input:focus {{ border-color:#a5f3fc; background:#1e3a8a; }}
  #search-btn {{
    background:#fff; color:#0891b2; border:none;
    padding:8px 16px; border-radius:6px; cursor:pointer;
    font-size:13px; font-weight:bold; white-space:nowrap;
    transition:background .15s;
  }}
  #search-btn:hover {{ background:#e8f0fe; }}

  /* ── Search results dropdown ── */
  #search-results {{
    position:absolute; top:calc(100% + 6px); left:0; right:52px;
    background:#1e293b; border:1px solid #334155;
    border-radius:8px; z-index:9999;
    box-shadow:0 8px 24px rgba(0,0,0,.5); display:none; overflow:hidden;
  }}
  .sr-item {{
    padding:10px 14px; cursor:pointer; font-size:13px;
    border-bottom:1px solid #334155; display:flex; align-items:flex-start;
    gap:10px; transition:background .12s;
  }}
  .sr-item:last-child {{ border-bottom:none; }}
  .sr-item:hover {{ background:#0891b2; }}
  .sr-icon {{ font-size:16px; width:22px; flex-shrink:0; margin-top:1px; }}
  .sr-text {{ flex:1; min-width:0; }}
  .sr-name {{ color:#e6edf3; font-weight:600; font-size:13px;
              white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
  .sr-addr {{ color:#94a3b8; font-size:11px; margin-top:2px;
              white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
  #sr-status {{ padding:12px 14px; color:#94a3b8; font-size:12px; }}

  /* ── Map ── */
  #map {{ height: calc(100vh - 102px); }}

  /* ── Bottom bar ── */
  #coords-bar {{
    background:#161b22; padding:0 16px;
    border-top:1px solid #30363d;
    display:flex; align-items:center; gap:12px;
    height:46px;
  }}
  #last-coord {{ font-size:13px; color:#67e8f9; font-weight:600; flex:1; }}
  #point-count {{ font-size:12px; color:#8b949e; white-space:nowrap; }}

  /* ── Buttons in bottom bar ── */
  .bar-btn {{
    border:none; padding:6px 16px; border-radius:6px;
    cursor:pointer; font-size:12px; font-weight:700;
    white-space:nowrap; transition:opacity .15s;
  }}
  .bar-btn:hover {{ opacity:0.85; }}
  #undo-btn  {{ background:#e6a817; color:#000; }}
  #done-btn  {{ background:#67e8f9; color:#0d1117; }}

  /* ── Marker tooltip ── */
  .marker-label {{
    background:#0891b2; color:#fff; border:2px solid #a5f3fc;
    padding:2px 8px; border-radius:4px; font-size:11px;
    white-space:nowrap; font-weight:600;
    box-shadow:0 2px 6px rgba(0,0,0,.4);
  }}

  /* ── Date/time dialog overlay ── */
  #ts-overlay {{
    display:none; position:fixed; inset:0;
    background:rgba(0,0,0,0.55); z-index:99999;
    align-items:center; justify-content:center;
  }}
  #ts-overlay.show {{ display:flex; }}
  #ts-box {{
    background:#1e293b; border:2px solid #06b6d4;
    border-radius:12px; padding:24px 28px; min-width:340px;
    box-shadow:0 12px 40px rgba(0,0,0,.6);
  }}
  #ts-box h3 {{
    color:#e6edf3; font-size:15px; margin:0 0 6px;
    font-weight:700;
  }}
  #ts-coord-label {{
    color:#67e8f9; font-size:12px; margin-bottom:14px;
  }}
  #ts-box label {{
    color:#94a3b8; font-size:12px; display:block; margin-bottom:4px;
  }}
  #ts-input {{
    width:100%; padding:10px 12px; border-radius:6px;
    border:2px solid #06b6d4; background:#0d1117; color:#e6edf3;
    font-size:14px; font-family:'Segoe UI',sans-serif;
    outline:none; box-sizing:border-box;
  }}
  #ts-input:focus {{ border-color:#a5f3fc; }}
  #ts-hint {{
    color:#6b7280; font-size:11px; margin-top:5px; margin-bottom:16px;
  }}
  #ts-btn-row {{ display:flex; gap:10px; justify-content:flex-end; }}
  .ts-btn {{
    border:none; padding:9px 20px; border-radius:6px;
    cursor:pointer; font-size:13px; font-weight:700;
    transition:opacity .15s;
  }}
  .ts-btn:hover {{ opacity:0.85; }}
  #ts-cancel {{ background:#374151; color:#e6edf3; }}
  #ts-confirm {{ background:#0891b2; color:#fff; }}
  #ts-input {{
    width:100%; padding:10px 12px; border-radius:6px;
    border:2px solid #06b6d4; background:#0d1117; color:#e6edf3;
    font-size:14px; font-family:'Segoe UI',sans-serif;
    outline:none; box-sizing:border-box;
    color-scheme: dark;
  }}
  #ts-input:focus {{ border-color:#a5f3fc; }}
</style>
</head>
<body>

<!-- Date/time dialog — shown on each map click -->
<div id="ts-overlay">
  <div id="ts-box">
    <h3>📅  Set Date &amp; Time for this Point</h3>
    <div id="ts-coord-label"></div>
    <label>Select date and time</label>
    <input id="ts-input" type="datetime-local" step="1"/>
    <div id="ts-hint">Choose from the calendar picker · Esc to cancel</div>
    <div id="ts-btn-row">
      <button class="ts-btn" id="ts-cancel" onclick="cancelPoint()">Cancel</button>
      <button class="ts-btn" id="ts-confirm" onclick="confirmPoint()">Add Point</button>
    </div>
  </div>
</div>

<div id="header">
  <div id="header-left">
    <h2>🗺️  ComTrail — Map Coordinate Picker</h2>
    <div id="info">
      Click map → enter date/time → point added &nbsp;·&nbsp;
      Event: <strong style="color:#fff">{ev}</strong>
      &nbsp;·&nbsp; Right-click a marker to remove it
    </div>
  </div>

  <div id="search-wrap">
    <input id="search-input" type="text"
           placeholder="🔍  Search city, area, address, landmark…"
           autocomplete="off"/>
    <button id="search-btn" onclick="doSearch()">Search</button>
    <div id="search-results"></div>
  </div>
</div>

<div id="map"></div>

<div id="coords-bar">
  <span id="last-coord">Click anywhere on the map to capture a point…</span>
  <span id="point-count"></span>
  <button class="bar-btn" id="undo-btn" onclick="removeLastPin()"
          style="display:none">⬅ Remove Last Pin</button>
  <button class="bar-btn" id="done-btn" onclick="window.close()">✓ Done</button>
</div>

<script>
// ── Map setup ──────────────────────────────────────────────────
var count   = 0;
var markers = [];   // {{marker, lat, lon, ts}}

var map = L.map('map', {{zoomControl:true}}).setView([20.5937, 78.9629], 5);

L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  attribution: '© <a href="https://openstreetmap.org">OpenStreetMap</a>',
  maxZoom: 19
}}).addTo(map);

// ── Pending click state ───────────────────────────────────────
var pendingLat = null;
var pendingLon = null;
var pendingMarker = null;   // ghost marker shown while dialog is open

// Returns YYYY-MM-DDTHH:MM:SS  (value format for datetime-local input)
function _nowInputValue() {{
  var d = new Date();
  var yyyy = d.getFullYear();
  var mm   = String(d.getMonth()+1).padStart(2,'0');
  var dd   = String(d.getDate()).padStart(2,'0');
  var hh   = String(d.getHours()).padStart(2,'0');
  var mi   = String(d.getMinutes()).padStart(2,'0');
  var ss   = String(d.getSeconds()).padStart(2,'0');
  return yyyy+'-'+mm+'-'+dd+'T'+hh+':'+mi+':'+ss;
}}

// Converts YYYY-MM-DDTHH:MM:SS  →  DD/MM/YYYY HH:MM:SS  (for CSV)
function _inputValueToDisplay(val) {{
  if (!val) return _inputValueToDisplay(_nowInputValue());
  // val = "2026-03-24T10:00:00"
  var parts = val.split('T');
  var dateParts = parts[0].split('-');   // [yyyy, mm, dd]
  var timePart  = parts[1] || '00:00:00';
  // Trim seconds if browser omitted them
  if (timePart.length === 5) timePart += ':00';
  return dateParts[2]+'/'+dateParts[1]+'/'+dateParts[0]+' '+timePart;
}}

// ── Click: show date/time dialog ──────────────────────────────
map.on('click', function(e) {{
  pendingLat = parseFloat(e.latlng.lat).toFixed(6);
  pendingLon = parseFloat(e.latlng.lng).toFixed(6);

  // Ghost marker so user sees where they clicked
  if (pendingMarker) {{ map.removeLayer(pendingMarker); }}
  pendingMarker = L.circleMarker([pendingLat, pendingLon], {{
    radius:8, color:'#f59e0b', fillColor:'#fbbf24',
    fillOpacity:0.8, weight:2, dashArray:'4'
  }}).addTo(map);

  document.getElementById('ts-coord-label').textContent =
    'Location: ' + pendingLat + ', ' + pendingLon;
  document.getElementById('ts-input').value = _nowInputValue();
  document.getElementById('ts-overlay').classList.add('show');
  setTimeout(function() {{
    var inp = document.getElementById('ts-input');
    inp.focus(); inp.select();
  }}, 80);
}});

function confirmPoint() {{
  var raw = document.getElementById('ts-input').value.trim();
  var ts  = _inputValueToDisplay(raw);
  document.getElementById('ts-overlay').classList.remove('show');
  if (pendingMarker) {{ map.removeLayer(pendingMarker); pendingMarker = null; }}
  addPin(pendingLat, pendingLon, ts);
  pendingLat = pendingLon = null;
}}

function cancelPoint() {{
  document.getElementById('ts-overlay').classList.remove('show');
  if (pendingMarker) {{ map.removeLayer(pendingMarker); pendingMarker = null; }}
  pendingLat = pendingLon = null;
}}

// Escape = cancel  (Enter is used by the datetime picker itself)
document.getElementById('ts-input').addEventListener('keydown', function(e) {{
  if (e.key === 'Escape') cancelPoint();
}});

function addPin(rawLat, rawLon, ts) {{
  var lat = parseFloat(rawLat).toFixed(6);
  var lon = parseFloat(rawLon).toFixed(6);
  count++;
  var idx = count;

  var marker = L.marker([lat, lon]).addTo(map);
  marker.bindTooltip(idx + ': ' + lat + ', ' + lon + '<br>' + ts,
    {{permanent:true, className:'marker-label', direction:'top'}}).openTooltip();

  // Right-click marker → remove it
  marker.on('contextmenu', function(e) {{
    removePin(markers.findIndex(function(m) {{ return m.marker === marker; }}));
    L.DomEvent.stopPropagation(e);
  }});

  markers.push({{marker:marker, lat:lat, lon:lon, ts:ts, idx:idx}});
  updateBottomBar();

  // POST coord + timestamp to Python
  fetch('http://127.0.0.1:{port}/coord', {{
    method:'POST',
    headers:{{'Content-Type':'application/x-www-form-urlencoded'}},
    body:'lat=' + encodeURIComponent(lat)
       + '&lon=' + encodeURIComponent(lon)
       + '&ts='  + encodeURIComponent(ts)
  }}).catch(function(){{}});
}}

// ── Remove pin by array index ─────────────────────────────────
function removePin(arrIdx) {{
  if (arrIdx < 0 || arrIdx >= markers.length) return;
  map.removeLayer(markers[arrIdx].marker);
  var removed = markers.splice(arrIdx, 1)[0];

  // Renumber remaining tooltips
  markers.forEach(function(m, i) {{
    m.marker.setTooltipContent((i+1) + ': ' + m.lat + ', ' + m.lon);
  }});
  count = markers.length;

  // Notify Python to remove this coord
  fetch('http://127.0.0.1:{port}/remove', {{
    method:'POST',
    headers:{{'Content-Type':'application/x-www-form-urlencoded'}},
    body:'idx=' + arrIdx
  }}).catch(function(){{}});

  updateBottomBar();
}}

function removeLastPin() {{
  if (markers.length > 0) removePin(markers.length - 1);
}}

function updateBottomBar() {{
  var n = markers.length;
  if (n > 0) {{
    var last = markers[n-1];
    document.getElementById('last-coord').textContent =
      '✓ Point ' + n + ' captured: ' + last.lat + ', ' + last.lon;
    document.getElementById('point-count').textContent = n + ' point(s)';
    document.getElementById('undo-btn').style.display = 'inline-block';
  }} else {{
    document.getElementById('last-coord').textContent =
      'Click anywhere on the map to capture a point…';
    document.getElementById('point-count').textContent = '';
    document.getElementById('undo-btn').style.display = 'none';
  }}
}}

// ── Search ────────────────────────────────────────────────────
var searchTimeout  = null;
var currentResults = [];
var searchPin      = null;

var TYPE_ICONS = {{
  'city':'🏙️','town':'🏘️','village':'🏡','suburb':'🏘️',
  'neighbourhood':'🏘️','country':'🌍','state':'📍',
  'county':'📍','region':'📍','district':'📍',
  'road':'🛣️','street':'🛣️','highway':'🛣️',
  'restaurant':'🍽️','hotel':'🏨','hospital':'🏥',
  'school':'🏫','university':'🎓','airport':'✈️',
  'station':'🚉','park':'🌳','mall':'🏬','shop':'🏪',
  'museum':'🏛️','church':'⛪','administrative':'🗺️'
}};

function getIcon(type, cls) {{
  return TYPE_ICONS[(type||'').toLowerCase()]
      || TYPE_ICONS[(cls ||'').toLowerCase()]
      || '📍';
}}

function doSearch() {{
  var q = document.getElementById('search-input').value.trim();
  if (!q) return;

  var box = document.getElementById('search-results');
  box.style.display = 'block';
  box.innerHTML = '<div id="sr-status">🔍 Searching for "<b>' + q + '</b>"…</div>';

  var url = 'https://nominatim.openstreetmap.org/search'
    + '?q='      + encodeURIComponent(q)
    + '&format=json&limit=8&addressdetails=1&extratags=1';

  fetch(url, {{headers:{{'Accept-Language':'en','User-Agent':'ComTrailApp/2.0'}}}})
  .then(function(r) {{ return r.json(); }})
  .then(function(data) {{
    currentResults = data || [];
    if (!currentResults.length) {{
      box.innerHTML = '<div id="sr-status">No results for "<b>' + q + '</b>". Try a different term.</div>';
      return;
    }}
    var html = '';
    currentResults.forEach(function(item, i) {{
      var parts = item.display_name.split(',');
      var name  = parts[0].trim();
      var addr  = parts.slice(1,4).join(',').trim();
      var icon  = getIcon(item.type, item.class);
      html += '<div class="sr-item" onclick="selectResult(' + i + ')">'
        + '<span class="sr-icon">' + icon + '</span>'
        + '<div class="sr-text">'
        + '<div class="sr-name">' + name + '</div>'
        + '<div class="sr-addr">' + addr + '</div>'
        + '</div></div>';
    }});
    box.innerHTML = html;
  }})
  .catch(function() {{
    box.innerHTML = '<div id="sr-status" style="color:#f87171">'
      + '⚠️ Search failed. Check internet connection.</div>';
  }});
}}

var ZOOM_FOR_TYPE = {{
  'country':5,'state':7,'region':7,'county':9,
  'city':12,'town':13,'village':14,'suburb':15,
  'neighbourhood':16,'road':16,'street':16,
  'restaurant':17,'hotel':17,'hospital':16,
  'airport':13,'station':15,'administrative':9
}};

function selectResult(idx) {{
  var item = currentResults[idx];
  if (!item) return;

  closeSearch();

  // Remove old highlight marker (no circle — just fly)
  if (searchPin) {{ map.removeLayer(searchPin); searchPin = null; }}

  var lat = parseFloat(item.lat);
  var lon = parseFloat(item.lon);
  var z   = ZOOM_FOR_TYPE[item.type]
         || ZOOM_FOR_TYPE[item.class]
         || 15;

  map.flyTo([lat, lon], z, {{duration:1.2}});

  // Small blue dot at the searched location — no red circle
  searchPin = L.circleMarker([lat, lon], {{
    radius:6, color:'#0891b2', fillColor:'#a5f3fc',
    fillOpacity:0.9, weight:2
  }}).addTo(map);
  searchPin.bindTooltip('📍 ' + item.display_name.split(',')[0],
    {{direction:'top', offset:[0,-6]}}).openTooltip();

  document.getElementById('search-input').value =
    item.display_name.split(',').slice(0,3).join(',');

  document.getElementById('last-coord').textContent =
    '📍 Navigated to: ' + item.display_name.split(',')[0]
    + '  (' + lat.toFixed(6) + ', ' + lon.toFixed(6) + ')  — click to capture';
}}

function closeSearch() {{
  document.getElementById('search-results').style.display = 'none';
}}

// Live search — debounced 450ms, starts at 2 chars
document.getElementById('search-input').addEventListener('input', function() {{
  clearTimeout(searchTimeout);
  var q = this.value.trim();
  if (q.length < 2) {{ closeSearch(); return; }}
  searchTimeout = setTimeout(doSearch, 450);
}});

document.getElementById('search-input').addEventListener('keydown', function(e) {{
  if (e.key === 'Enter')  {{ clearTimeout(searchTimeout); doSearch(); }}
  if (e.key === 'Escape') {{ closeSearch(); this.value = ''; }}
}});

// Close dropdown on outside click
document.addEventListener('click', function(e) {{
  var wrap = document.getElementById('search-wrap');
  if (!wrap.contains(e.target)) closeSearch();
}});
</script>
</body>
</html>"""

            # Local HTTP request handler
            app_ref   = self
            event_ref = [ev]
            coord_ref = picked_coords
            status_ref = map_status_var
            tree_ref   = map_tree

            class _Handler(http.server.BaseHTTPRequestHandler):
                def log_message(self, *a): pass   # silence access log

                def do_GET(self):
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(html.encode("utf-8"))

                def do_POST(self):
                    if self.path == "/coord":
                        import urllib.parse as _up
                        length = int(self.headers.get("Content-Length", 0))
                        body   = self.rfile.read(length).decode("utf-8")
                        params = _up.parse_qs(body, keep_blank_values=True)
                        lat = params.get("lat", [""])[0]
                        lon = params.get("lon", [""])[0]
                        ts  = params.get("ts",  [""])[0].strip()
                        if not ts:
                            ts = time.strftime("%d/%m/%Y %H:%M:%S")
                        if lat and lon:
                            coord_ref.append([lat, lon, event_ref[0], ts])
                            LOG.log("LBS Map Picker",
                                    f"Point captured: Lat={lat} Lon={lon} "
                                    f"Event={event_ref[0]}")
                            def _ui():
                                refresh_table()
                                status_ref.set(
                                    f"{len(coord_ref)} point(s) captured")
                                map_status_lbl.config(fg=app_ref.C("success"))
                            app_ref.after(0, _ui)
                        self.send_response(200)
                        self.end_headers()
                    elif self.path == "/remove":
                        length = int(self.headers.get("Content-Length", 0))
                        body   = self.rfile.read(length).decode("utf-8")
                        params = dict(p.split("=") for p in body.split("&") if "=" in p)
                        try:
                            arr_idx = int(params.get("idx", -1))
                            if 0 <= arr_idx < len(coord_ref):
                                removed = coord_ref.pop(arr_idx)
                                LOG.log("LBS Map Picker",
                                        f"Pin removed: idx={arr_idx} "
                                        f"Lat={removed[0]} Lon={removed[1]}")
                                def _ui_rm():
                                    refresh_table()
                                    n = len(coord_ref)
                                    status_ref.set(
                                        f"{n} point(s) captured" if n else "")
                                    map_status_lbl.config(
                                        fg=app_ref.C("success") if n
                                        else app_ref.C("muted"))
                                app_ref.after(0, _ui_rm)
                        except (ValueError, IndexError):
                            pass
                        self.send_response(200)
                        self.end_headers()
                    else:
                        self.send_response(404)
                        self.end_headers()

            # Start server in background thread
            server = http.server.HTTPServer(("127.0.0.1", port), _Handler)

            def _serve():
                server.serve_forever()

            t = threading.Thread(target=_serve, daemon=True)
            t.start()

            # Open map in browser
            webbrowser.open(f"http://127.0.0.1:{port}/")
            map_status_var.set("Map opened in browser — click points to capture…")
            map_status_lbl.config(fg=self.C("muted"))
            LOG.log("LBS Map Picker",
                    f"Map server started on port {port}  Event={ev}")

        tk.Button(map_btn_row, text="🗺️  Open Interactive Map",
                  bg=self.C("primary"), fg="white", relief="flat",
                  font=(_UI_FONT, 11, "bold"), activebackground=self.C("border"),
                  width=28, command=open_map).pack(side="left", ipady=6)
        tk.Label(map_btn_row,
                 text="  Opens in your browser · click map to capture lat/long · "
                      "coordinates appear in the table above in real time",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8)).pack(side="left", padx=10)

        # ── SECTION 2: Interception Parameters ────────────────────
        _sec("📋   Interception Parameters")
        ip_body = tk.Frame(main_panel, bg=self.C("panel"))
        ip_body.pack(fill="x", padx=14, pady=10)

        ip_row = tk.Frame(ip_body, bg=self.C("panel"))
        ip_row.pack(fill="x")

        def _ip_field(parent, label, var, width=20, hint="", required=False, numeric=False):
            col = tk.Frame(parent, bg=self.C("panel"))
            col.pack(side="left", padx=(0, 24))
            lbl_text = f"{'* ' if required else ''}{label}"
            tk.Label(col, text=lbl_text, bg=self.C("panel"),
                     fg="#ef4444" if required else self.C("muted"),
                     font=(_UI_FONT, 8, "bold" if required else "normal")).pack(anchor="w")
            border = tk.Frame(col, bg="#ef4444" if required else self.C("border"),
                              padx=1, pady=1)
            border.pack()
            ent = tk.Entry(border, textvariable=var, width=width,
                           bg=self.C("input_bg"), fg=self.C("text"),
                           insertbackground=self.C("text"),
                           relief="flat", font=(_UI_FONT, 9))
            ent.pack(ipady=5)
            if numeric:
                def _strip_non_digits(*_, v=var):
                    val = v.get()
                    cleaned = ''.join(c for c in val if c.isdigit())
                    if cleaned != val:
                        v.set(cleaned)
                var.trace_add("write", _strip_non_digits)
            if required:
                def _chk(*_, b=border, v=var):
                    b.config(bg="#ef4444" if not v.get().strip() else self.C("success"))
                var.trace_add("write", _chk)
                _chk()
            if hint:
                tk.Label(col, text=hint, bg=self.C("panel"), fg=self.C("dim"),
                         font=(_UI_FONT, 7)).pack(anchor="w", pady=(2, 0))

        _ip_field(ip_row, "Target Number", target_var, width=22,
                  required=True, numeric=True,
                  hint="MSISDN set automatically to this value")

        # ── SECTION 3: Files ───────────────────────────────────────
        _sec("📂   Files")
        f_body = tk.Frame(main_panel, bg=self.C("panel"))
        f_body.pack(fill="x", padx=14, pady=10)

        lbs_csv_preview = tk.Frame(f_body, bg=self.C("panel"))

        def _lbs_refresh_preview(*_):
            self._build_csv_preview(lbs_csv_preview, csv_var.get())

        for lbl, var, cmd in [
            ("Input CSV File", csv_var,
             lambda: (csv_var.set(filedialog.askopenfilename(
                 title="Select CSV",
                 filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])) or
                 _lbs_refresh_preview())),
            ("Output Folder",  output_var,
             lambda: output_var.set(filedialog.askdirectory(
                 title="Select Output Folder"))),
        ]:
            r = tk.Frame(f_body, bg=self.C("panel"))
            r.pack(fill="x", pady=3)
            tk.Label(r, text=lbl, width=16, anchor="w",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9)).pack(side="left")
            ent = tk.Entry(r, textvariable=var,
                           bg=self.C("input_bg"), fg=self.C("text"),
                           insertbackground=self.C("text"),
                           relief="flat", font=(_UI_FONT, 9),
                           width=52)
            ent.pack(side="left", ipady=5, padx=(0, 8))
            self._enable_drop(ent, var,
                              callback=_lbs_refresh_preview
                              if var is csv_var else None)
            tk.Button(r, text="Browse…",
                      bg=self.C("primary"), fg="white", relief="flat",
                      font=(_UI_FONT, 8, "bold"), cursor="hand2",
                      activebackground=self.C("border"),
                      command=cmd).pack(side="left", ipady=4, ipadx=6)

        lbs_csv_preview.pack(fill="x", pady=(4, 0))
        csv_var.trace_add("write", _lbs_refresh_preview)

        # Sample CSV download — compact inline
        smp_row = tk.Frame(f_body, bg=self.C("panel"))
        smp_row.pack(fill="x", pady=(6, 0))

        def download_lbs_sample():
            path = filedialog.asksaveasfilename(
                title="Save LBS Sample CSV", defaultextension=".csv",
                filetypes=[("CSV files", "*.csv")],
                initialfile="lbs_sample.csv")
            if not path: return
            rows = [
                ["Latitude", "Longitude", "Timestamp", "Event", "IMEI", "IMSI"],
                ["28.6139","77.2090","24/03/2026 09:16:00","BEGIN",
                 "354792001234567","405912349872561"],
                ["28.6145","77.2095","24/03/2026 09:18:30","CONTINUE",
                 "354792001234567","405912349872561"],
                ["19.0760","72.8777","24/03/2026 09:45:00","BEGIN",
                 "354792001234568","405912349872562"],
                ["19.0765","72.8782","24/03/2026 09:47:15","CONTINUE",
                 "354792001234568","405912349872562"],
                ["19.0770","72.8788","24/03/2026 09:50:00","END",
                 "354792001234568","405912349872562"],
                ["12.9716","77.5946","24/03/2026 10:05:00","BEGIN",
                 "354792001234569","405912349872563"],
            ]
            try:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    csv.writer(f).writerows(rows)
                LOG.log("LBS Generator", f"Sample CSV saved → {path}")
                self.popup("Saved",
                           f"LBS sample CSV saved to:\n{os.path.basename(path)}",
                           "success")
            except Exception as e:
                self.popup("Error", str(e), "error")

        tk.Button(smp_row, text="⬇️  Download Sample CSV",
                  bg=self.C("panel"), fg=self.C("primary"), relief="flat",
                  font=(_UI_FONT, 8, "bold"), cursor="hand2",
                  activebackground=self.C("input_bg"),
                  command=download_lbs_sample).pack(side="left", ipady=3, ipadx=4)
        tk.Label(smp_row,
                 text="  ← pre-filled template showing the expected CSV columns",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 7)).pack(side="left")

        # ── SECTION 4: Generate CDR Files ─────────────────────────
        _sec("🚀   Generate CDR Files")
        gen_body = tk.Frame(main_panel, bg=self.C("panel"))
        gen_body.pack(fill="x", padx=14, pady=12)

        status_var = tk.StringVar(value="Ready — fill in the fields above and click Generate.")
        status_lbl = tk.Label(gen_body, textvariable=status_var,
                              bg=self.C("panel"), fg=self.C("muted"),
                              font=(_UI_FONT, 9), wraplength=860, justify="left")
        status_lbl.pack(anchor="w", pady=(0, 10))

        # Silent tree — keeps generate logic working without showing a table
        res_tree_data = []

        class _SilentTree:
            def delete(self, *a):       res_tree_data.clear()
            def insert(self, *a, **kw): res_tree_data.append(kw.get("values", a))
            def get_children(self):     return []

        res_tree = _SilentTree()

        btn_row = tk.Frame(gen_body, bg=self.C("panel"))
        btn_row.pack(anchor="w")

        def open_output():
            p = output_var.get().strip()
            if p and os.path.isdir(p):
                webbrowser.open(f"file:///{p.replace(os.sep, '/')}")

        _BTN = dict(relief="flat", font=(_UI_FONT, 10, "bold"),
                    fg="white", cursor="hand2", width=20)

        tk.Button(btn_row, text="📁  Open Folder",
                  bg="#444", activebackground="#555",
                  command=open_output,
                  **_BTN).pack(side="left", padx=(0, 8), ipady=7)

        def do_generate():
            csv_path = csv_var.get().strip()
            out_path = output_var.get().strip()
            msisdn   = msisdn_var.get().strip()
            target   = target_var.get().strip()

            if not csv_path or not os.path.isfile(csv_path):
                return self.popup("Error", "Please select a valid CSV file.", "error")
            if not out_path:
                return self.popup("Error", "Please select an output folder.", "error")
            if not msisdn:
                return self.popup("Error", "MSISDN is required.", "error")
            if not target:
                return self.popup("Error", "Target Number is required.", "error")
            status_var.set("Generating CDR files…")
            status_lbl.config(fg=self.C("muted"))
            res_tree.delete(*res_tree.get_children())

            def task():
                results = []
                error   = None
                try:
                    os.makedirs(out_path, exist_ok=True)

                    with open(csv_path, "r", encoding="utf-8-sig") as f:
                        rows = list(csv.DictReader(f))

                    LOG.log("LBS Generator",
                            f"Processing {len(rows)} row(s)  "
                            f"MSISDN={msisdn}  Target={target}")

                    for seq, row in enumerate(rows, 1):
                        # Required per-row fields
                        lat = row.get("Latitude",  "").strip().strip('"')
                        lon = row.get("Longitude", "").strip().strip('"')
                        if not lat or not lon:
                            LOG.log("LBS Generator",
                                    f"  Row {seq}: skipped — missing Latitude/Longitude",
                                    "WARNING")
                            continue

                        # Optional per-row fields — IMEI/IMSI come from CSV only
                        ts    = (row.get("Timestamp", "").strip().strip('"')
                                 or time.strftime("%d/%m/%Y %H:%M:%S"))
                        event = (row.get("Event",     "").strip().strip('"').upper()
                                 or "BEGIN")
                        imei  = row.get("IMEI", "").strip().strip('"') or "000000000000000"
                        imsi  = row.get("IMSI", "").strip().strip('"') or "000000000000000"

                        # Validate event value
                        if event not in ("BEGIN", "END", "CONTINUE"):
                            event = "BEGIN"

                        call_id = _next_call_id()
                        fp = _lbs_create_cdr_file(
                            output_dir    = out_path,
                            call_id       = call_id,
                            seq_num       = seq,
                            timestamp     = ts,
                            last_activity = ts,       # always equal
                            imei          = imei,
                            imsi          = imsi,
                            msisdn        = msisdn,
                            target_number = target,
                            event         = event,
                            latitude      = lat,
                            longitude     = lon,
                        )
                        results.append((
                            os.path.basename(fp),
                            str(call_id),
                            ts,
                            event,
                            lat,
                            lon,
                        ))
                        LOG.log("LBS Generator",
                                f"  ✓ {os.path.basename(fp)}  "
                                f"Lat={lat}  Lon={lon}  Event={event}")

                    LOG.log("LBS Generator",
                            f"Done — {len(results)} CDR file(s) → {out_path}")

                except Exception as exc:
                    import traceback
                    error = str(exc)
                    LOG.log("LBS Generator",
                            f"FAILED: {exc}\n{traceback.format_exc()}", "ERROR")

                def _ui():
                    res_tree.delete(*res_tree.get_children())
                    for r in results:
                        res_tree.insert("", "end", values=r)
                    if error:
                        status_var.set(f"❌  {error}")
                        status_lbl.config(fg=self.C("error"))
                        self._write_history("LBS Generate", 0, "local",
                                            "❌ Failed", error[:60])
                    else:
                        status_var.set(
                            f"✅  {len(results)} CDR file(s) generated  →  {out_path}")
                        status_lbl.config(fg=self.C("success"))
                        self._write_history("LBS Generate", len(results),
                                            "local", "✅ OK", out_path)
                self.after(0, _ui)

            threading.Thread(target=task, daemon=True).start()

        tk.Button(btn_row, text="🚀  Generate CDR Files",
                  bg=self.C("success"), activebackground=self.C("border"),
                  command=do_generate,
                  **_BTN).pack(side="left", padx=(0, 8), ipady=7)

        def upload_cdr_direct():
            """Upload generated CDR files directly from the output folder."""
            out_path = output_var.get().strip()
            if not out_path or not os.path.isdir(out_path):
                return self.popup("Error",
                    "Output folder not set or does not exist.\n"
                    "Generate CDR files first, then click Upload CDR.", "error")
            files = [
                os.path.join(out_path, f)
                for f in os.listdir(out_path)
                if os.path.isfile(os.path.join(out_path, f))
            ]
            if not files:
                return self.popup("Error",
                    "No files found in the output folder.\n"
                    "Generate CDR files first.", "error")
            ip   = self.cfg["ludr"].get("ip", "")
            path = self.cfg["ludr"].get("path", "")
            if not ip:
                return self.popup("Error",
                    "LUDR server not configured. Go to Settings.", "error")
            sftp = CONN.get_sftp("ludr")
            if not sftp:
                return self.popup("Error",
                    "LUDR server is not connected. Check Settings.", "error")
            total = len(files)
            LOG.log("LBS Upload",
                    f"Direct CDR upload — {total} file(s) from {out_path} → {ip}:{path}")
            top, _ = self.progress_window(
                f"Uploading {total} CDR file(s)…")

            def _task():
                try:
                    for idx, f in enumerate(files):
                        fn  = os.path.basename(f)
                        pct = int((idx + 1) * 100 / total)
                        self.after(0, lambda i=idx+1, n=fn, p=pct:
                            top._set_status(
                                f"Uploading file {i} of {total}…",
                                pct=p, sub=n)
                            if top.winfo_exists() else None)
                        sftp.put(f, path.rstrip("/") + "/" + fn)
                        LOG.log("LBS Upload", f"  {fn} → {path}")
                    self._write_history("LBS CDR Upload", total, ip,
                                        "✅ OK", out_path)
                    self.after(0, lambda: (
                        top.destroy() if top.winfo_exists() else None,
                        self._toast(
                            f"✅  CDR upload complete\n{total} file(s) transferred.",
                            "success")))
                except Exception as e:
                    LOG.log("LBS Upload", f"Upload failed: {e}", "ERROR")
                    self.after(0, lambda err=e: (
                        top.destroy() if top.winfo_exists() else None,
                        self.popup("Error", f"Upload failed:\n{err}", "error")))

            threading.Thread(target=_task, daemon=True).start()

        tk.Button(btn_row, text="📤  Upload CDR",
                  bg=self.C("primary"), activebackground=self.C("border"),
                  command=upload_cdr_direct,
                  **_BTN).pack(side="left", ipady=7)

    # ──────────────────────────────────────────────────────────────
    # VOICE CALL GENERATOR
    # ──────────────────────────────────────────────────────────────

    # ════════════════════════════════════════════════════════════════
    # PCAP GENERATION ENGINE — Pure stdlib, no scapy required
    # Produces valid PCAP files readable in Wireshark / tcpdump
    # ════════════════════════════════════════════════════════════════
    def show_pcap_generator(self):
        import struct, random, socket, os, threading, time, json
        from datetime import datetime

        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened PCAP Generator")

        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(25, 5))
        tk.Button(hdr, text="← Back", bg=self.C("panel"),
                  fg=self.C("muted"), relief="flat",
                  font=(_UI_FONT, 9),
                  command=self.show_generate_data).pack(side="left")
        tk.Label(hdr, text="📡  PCAP Generator",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left", padx=15)
        tk.Frame(main, bg=self.C("border"), height=1).pack(
            fill="x", padx=50, pady=(5, 10))

        # ── Scrollable canvas ─────────────────────────────────────
        canvas, sf = self._scrollable(main)
        body = tk.Frame(sf, bg=self.C("bg"))
        body.pack(fill="x", padx=50, pady=5)

        # ── Mode toggle ───────────────────────────────────────────
        mode_var = tk.StringVar(value="Quick")
        self._section_label(body, "🎛️  Generation Mode", pady=(0, 8))
        mode_frame = tk.Frame(body, bg=self.C("panel"),
                              highlightbackground=self.C("border"),
                              highlightthickness=1)
        mode_frame.pack(fill="x", pady=(0, 14))
        for m, desc in [
            ("Quick",    "Template-based — choose a scenario and go"),
            ("Advanced", "Full control — protocols, IPs, ports, timing"),
        ]:
            r = tk.Frame(mode_frame, bg=self.C("panel"))
            r.pack(side="left", expand=True, fill="x", padx=2, pady=8)
            tk.Radiobutton(r, text=f"  {m} Mode",
                           variable=mode_var, value=m,
                           bg=self.C("panel"), fg=self.C("card_title"),
                           selectcolor=self.C("input_bg"),
                           activebackground=self.C("panel"),
                           font=(_UI_FONT, 11, "bold"),
                           command=lambda: _toggle_mode()).pack(side="left")
            tk.Label(r, text=desc, bg=self.C("panel"),
                     fg=self.C("muted"),
                     font=(_UI_FONT, 8)).pack(side="left", padx=6)

        # ── SECTION A — Quick Mode ─────────────────────────────────
        SCENARIOS = {
            "Web Browsing":          {"protocols": ["TCP","HTTP","HTTPS","DNS"],
                                      "hint": "HTTP GET/POST, DNS lookups, TLS handshakes"},
            "DNS Heavy":             {"protocols": ["UDP","DNS"],
                                      "hint": "High-volume DNS queries and responses"},
            "VoIP (SIP + RTP)":      {"protocols": ["UDP","SIP","RTP"],
                                      "hint": "SIP INVITE/BYE, RTP media streams"},
            "Email Traffic":         {"protocols": ["TCP","SMTP","POP3","IMAP"],
                                      "hint": "SMTP send, POP3/IMAP receive sessions"},
            "File Transfer (FTP)":   {"protocols": ["TCP","FTP"],
                                      "hint": "FTP control + data channel transfers"},
            "Mixed Enterprise":      {"protocols": ["TCP","UDP","HTTP","HTTPS",
                                                    "DNS","SMTP","ICMP"],
                                      "hint": "Realistic enterprise mix of all protocols"},
            "ICMP / Ping Sweep":     {"protocols": ["ICMP"],
                                      "hint": "ICMP echo request/reply, TTL variation"},
            "Suspicious Traffic":    {"protocols": ["TCP","UDP","ICMP"],
                                      "hint": "SYN flood, port scan, DNS amplification"},
            "ARP Storm":             {"protocols": ["ARP"],
                                      "hint": "Broadcast ARP requests across subnet"},
            "TLS/HTTPS Only":        {"protocols": ["TCP","TLS"],
                                      "hint": "Full TLS handshake + encrypted data"},
        }

        quick_frame = tk.Frame(body, bg=self.C("bg"))
        quick_frame.pack(fill="x")
        self._section_label(quick_frame, "⚡  Quick Scenario", pady=(0, 8))
        q_card = self._card(quick_frame)

        scenario_var = tk.StringVar(value="Web Browsing")
        hint_var     = tk.StringVar(
            value=SCENARIOS["Web Browsing"]["hint"])

        sq_row = tk.Frame(q_card, bg=self.C("panel"))
        sq_row.pack(fill="x", padx=10, pady=8)
        tk.Label(sq_row, text="Scenario", width=18, anchor="w",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left")
        sc_cb = ttk.Combobox(sq_row, textvariable=scenario_var,
                             values=list(SCENARIOS.keys()),
                             state="readonly", width=30)
        sc_cb.pack(side="left", ipady=5, padx=(0, 12))
        tk.Label(sq_row, textvariable=hint_var,
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8),
                 wraplength=320).pack(side="left")

        def _on_scenario(*_):
            sc = scenario_var.get()
            hint_var.set(SCENARIOS.get(sc, {}).get("hint", ""))
        sc_cb.bind("<<ComboboxSelected>>", _on_scenario)

        # ── SECTION B — Common Parameters ─────────────────────────
        self._section_label(body, "⚙️  Output Parameters", pady=(16, 8))
        param_card = self._card(body)

        size_var     = tk.StringVar(value="10")
        size_unit    = tk.StringVar(value="MB")
        duration_var = tk.StringVar(value="60")
        pps_var      = tk.StringVar(value="1000")
        out_dir_var  = tk.StringVar(value="")
        prefix_var   = tk.StringVar(value="capture")

        for lbl, var, hint, wd in [
            ("PCAP Size",      size_var,     "1 – 10000",     8),
            ("Duration (sec)", duration_var, "optional",      8),
            ("Packet Rate",    pps_var,      "packets/second",8),
            ("File Prefix",    prefix_var,   "output filename base", 18),
        ]:
            r = tk.Frame(param_card, bg=self.C("panel"))
            r.pack(fill="x", padx=10, pady=5)
            tk.Label(r, text=lbl, width=18, anchor="w",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9, "bold")).pack(side="left")
            tk.Entry(r, textvariable=var, bg=self.C("input_bg"),
                     fg=self.C("text"), insertbackground=self.C("text"),
                     relief="flat", width=wd).pack(
                side="left", ipady=5, padx=(0, 8))
            if lbl == "PCAP Size":
                ttk.Combobox(r, textvariable=size_unit,
                             values=["MB","GB"],
                             state="readonly", width=4).pack(
                    side="left", ipady=4, padx=(0, 10))
            tk.Label(r, text=hint, bg=self.C("panel"),
                     fg=self.C("dim"),
                     font=(_UI_FONT, 7)).pack(side="left")

        # Output dir
        od_row = tk.Frame(param_card, bg=self.C("panel"))
        od_row.pack(fill="x", padx=10, pady=5)
        tk.Label(od_row, text="Output Folder", width=18, anchor="w",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left")
        tk.Entry(od_row, textvariable=out_dir_var,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"),
                 relief="flat", width=36).pack(
            side="left", ipady=5, padx=(0, 6))
        def _browse():
            d = filedialog.askdirectory(title="Select output folder")
            if d: out_dir_var.set(d)
        tk.Button(od_row, text="📂 Browse",
                  bg=self.C("primary"), fg="white", relief="flat",
                  font=(_UI_FONT, 9, "bold"),
                  command=_browse).pack(side="left", ipady=4, ipadx=8)

        # ── SECTION B3 — Media Source Folder ─────────────────────
        self._section_label(body, "📁  Media Source Folder", pady=(16, 8))
        media_card = self._card(body)

        media_dir_var   = tk.StringVar(value="")
        media_files_var = tk.StringVar(value="No folder selected")
        media_counts    = {"images": [], "audio": [], "video": [], "other": [], "total": 0}

        # Info banner
        tk.Label(media_card,
                 text="Optional — point to a folder containing images, "
                      "voice WAVs, and/or videos.\n"
                      "The generator will embed real file bytes as "
                      "HTTP/SIP-RTP/RTSP responses in the PCAP.",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8),
                 wraplength=820, justify="left").pack(
            anchor="w", padx=10, pady=(8, 6))

        # Folder row
        mf_row = tk.Frame(media_card, bg=self.C("panel"))
        mf_row.pack(fill="x", padx=10, pady=(0, 6))
        tk.Label(mf_row, text="Media Folder", width=18, anchor="w",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left")
        tk.Entry(mf_row, textvariable=media_dir_var,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"),
                 relief="flat", width=36).pack(
            side="left", ipady=5, padx=(0, 6))

        def _browse_media():
            d = filedialog.askdirectory(title="Select Media Folder")
            if not d: return
            media_dir_var.set(d)
            _scan_media(d)

        def _scan_media(d):
            # Images — real bytes embedded in HTTP
            IMG_EXT   = {'.jpg','.jpeg','.png','.gif','.bmp',
                         '.webp','.ico','.tif','.tiff','.svg'}
            # Audio/Voice — real bytes embedded in SIP+RTP
            AUDIO_EXT = {'.wav','.mp3','.aac','.ogg','.flac',
                         '.m4a','.wma','.opus','.amr','.gsm'}
            # Video — random bytes (too large), filename/metadata used
            VIDEO_EXT = {'.mp4','.avi','.mkv','.mov','.wmv',
                         '.flv','.m4v','.ts','.webm','.3gp'}
            # Generic — any other file type → HTTP download session
            # (real bytes if < 5MB, otherwise random bytes same size)

            images = []
            audio  = []
            video  = []
            other  = []   # any other file type

            for root, _, files in os.walk(d):
                for fn in sorted(files):
                    if fn.startswith('.'): continue
                    ext = os.path.splitext(fn)[1].lower()
                    fp  = os.path.join(root, fn)
                    if ext in IMG_EXT:
                        images.append(fp)
                    elif ext in AUDIO_EXT:
                        audio.append(fp)
                    elif ext in VIDEO_EXT:
                        video.append(fp)
                    elif ext:   # any other extension
                        other.append(fp)

            media_counts["images"] = images
            media_counts["audio"]  = audio
            media_counts["video"]  = video
            media_counts["other"]  = other
            media_counts["total"]  = (len(images) + len(audio)
                                      + len(video) + len(other))

            # Update status label
            parts = []
            if images: parts.append(f"🖼️ {len(images)} image(s)")
            if audio:  parts.append(f"🎵 {len(audio)} audio file(s)")
            if video:  parts.append(f"🎬 {len(video)} video file(s)")
            if other:  parts.append(f"📄 {len(other)} other file(s)")
            if parts:
                media_files_var.set("  ✅  Found: " + "  ·  ".join(parts))
                media_status_lbl.config(fg=self.C("success"))
                LOG.log("PCAP Generator",
                        f"Media scan: {media_counts['total']} file(s) "
                        f"in {d}  "
                        f"(img={len(images)} audio={len(audio)} "
                        f"video={len(video)} other={len(other)})")
            else:
                media_files_var.set(
                    "  ⚠️  No files found in folder.")
                media_status_lbl.config(fg=self.C("warn"))

        tk.Button(mf_row, text="📂 Browse",
                  bg=self.C("primary"), fg="white", relief="flat",
                  font=(_UI_FONT, 9, "bold"),
                  command=_browse_media).pack(
            side="left", ipady=4, ipadx=8, padx=(0, 8))
        tk.Button(mf_row, text="✖ Clear",
                  bg="#374151", fg="white", relief="flat",
                  font=(_UI_FONT, 9),
                  command=lambda: [
                      media_dir_var.set(""),
                      media_files_var.set("No folder selected"),
                      media_status_lbl.config(fg=self.C("muted")),
                      media_counts.update(
                          {"images":[],"audio":[],"video":[],"total":0})
                  ]).pack(side="left", ipady=4, ipadx=8)

        media_status_lbl = tk.Label(media_card,
                                    textvariable=media_files_var,
                                    bg=self.C("panel"),
                                    fg=self.C("muted"),
                                    font=(_UI_FONT, 9, "bold"))
        media_status_lbl.pack(anchor="w", padx=10, pady=(0, 6))

        # How it works legend
        leg = tk.Frame(media_card,
                       bg=self.C("input_bg"),
                       highlightbackground=self.C("border"),
                       highlightthickness=1)
        leg.pack(fill="x", padx=10, pady=(0, 10))
        for icon, label, detail in [
            ("🖼️", "Images  (JPG/PNG/GIF/BMP/WEBP/ICO/SVG/TIFF)",
             "REAL bytes embedded — HTTP GET request + 200 response "
             "with exact file bytes and correct Content-Type header. "
             "Wireshark can extract and preview the actual image."),
            ("🎵", "Voice/Audio  (WAV/MP3/AAC/OGG/FLAC/M4A/AMR/OPUS)",
             "REAL bytes embedded — SIP INVITE + 200 OK + ACK "
             "followed by RTP stream carrying the actual audio bytes "
             "in 160-byte G.711 chunks. Full SIP teardown (BYE) included."),
            ("🎬", "Video  (MP4/AVI/MKV/MOV/WMV/TS/WEBM)",
             "RANDOM bytes used (videos too large for PCAP) — "
             "RTSP DESCRIBE/SETUP/PLAY session + RTP video chunks "
             "with realistic size matching the actual file size."),
            ("📄", "Any Other File  (PDF/ZIP/EXE/CSV/XML/DOC etc.)",
             "REAL bytes if file < 5 MB, otherwise random bytes "
             "same size — HTTP GET + 200 response with "
             "application/octet-stream or matched Content-Type."),
        ]:
            r = tk.Frame(leg, bg=self.C("input_bg"))
            r.pack(fill="x", padx=10, pady=4)
            tk.Label(r, text=icon, bg=self.C("input_bg"),
                     font=(_UI_FONT, 13), width=3).pack(side="left")
            col = tk.Frame(r, bg=self.C("input_bg"))
            col.pack(side="left", fill="x")
            tk.Label(col, text=label,
                     bg=self.C("input_bg"), fg=self.C("card_title"),
                     font=(_UI_FONT, 9, "bold")).pack(anchor="w")
            tk.Label(col, text=detail,
                     bg=self.C("input_bg"), fg=self.C("dim"),
                     font=(_UI_FONT, 8),
                     wraplength=780, justify="left").pack(anchor="w")
        tk.Frame(media_card, bg=self.C("input_bg"),
                 height=4).pack()
        self._section_label(body, "📋  Traffic Parameters", pady=(16, 8))
        tp_card = self._card(body)

        # ── All defaults pre-filled with real values ──────────────
        TP_DEFAULTS = {
            "src_ip":      "192.168.1.10",
            "dst_ip":      "10.0.0.1",
            "src_port":    "50000",
            "dst_port":    "80",
            "imei":        "354077862454420",
            "msisdn":      "919457622889",
            "http_method": "GET",
            "http_urls": (
                "/,/favicon.ico,"
                "/inc/css/homepage.css,/inc/css/general.css,"
                "/inc/css/thumbs.css,/inc/css/main_navigation.css,"
                "/inc/js/tracker.js,/inc/js/dropdown.js,"
                "/inc/js/mootools-trunk-1547-compatible.js,"
                "/inc/js/Autocompleter.js,/inc/js/autosuggest_2.0.js,"
                "/images/navigation/logo.gif,"
                "/images/homepage/mainnav_head.gif,"
                "/images/general/submit.gif,"
                "/images/banners/dreamstime/2012/DreamstimeBanner01.jpg,"
                "/dbase/images/mechanics/thumbs/b19mechanics182.jpg,"
                "/dbase/images/objects/thumbs/b1ammo001.jpg,"
                "/dbase/images/vehicles_land/thumbs/b19vehicles_land115.jpg,"
                "/dbase/images/vehicles_air/thumbs/b2airvehicles013.jpg,"
                "/search.php?search=gun&x=0&y=0,"
                "/search.php?search=missile&x=0&y=0,"
                "/search.php?search=pistol&x=0&y=0,"
                "/image.php?image=b19mechanics182.jpg,"
                "/image.php?image=b1ammo001.jpg,"
                "/inc/functions/function_keyword_suggest_ajax2.php?json=true&"
            ),
            "http_hosts": (
                "www.imageafter.com,www.google.com,www.facebook.com,"
                "www.youtube.com,www.instagram.com,www.twitter.com,"
                "api.whatsapp.com,web.telegram.org,www.amazon.com,"
                "www.netflix.com,accounts.google.com,"
                "mail.google.com,drive.google.com,www.microsoft.com,"
                "www.apple.com,www.reddit.com,www.wikipedia.org,"
                "www.linkedin.com,www.github.com"
            ),
            "https_sni": (
                "www.imageafter.com,www.google.com,www.facebook.com,"
                "www.youtube.com,www.instagram.com,api.whatsapp.com,"
                "web.telegram.org,www.amazon.com,www.netflix.com,"
                "accounts.google.com,ssl.gstatic.com,"
                "cdn.jsdelivr.net,fonts.googleapis.com,"
                "www.gstatic.com,fbcdn.net,"
                "static.cdninstagram.com,abs.twimg.com,"
                "login.microsoftonline.com,s.ytimg.com"
            ),
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/132.0.0.0 Safari/537.36"
            ),
            "dns_server":  "8.8.8.8",
            "dns_domains": (
                "google.com,facebook.com,youtube.com,instagram.com,"
                "whatsapp.com,telegram.org,amazon.com,netflix.com,"
                "twitter.com,linkedin.com,microsoft.com,apple.com,"
                "icloud.com,gmail.com,outlook.com,yahoo.com,"
                "doubleclick.net,googlevideo.com,fbcdn.net,"
                "akamaiedge.net,cloudfront.net,akamai.net,"
                "fastly.net,gstatic.com,ytimg.com,twimg.com,"
                "cdninstagram.com,whatsapp.net,t.me,telegram.me"
            ),
            "calling":     "919457622889",
            "called":      "916254957648",
            "callid":      "13092840",
            "sip_domain":  "sip.vodafone.in",
            "ftp_server":  "ftp.example.com",
            "ftp_user":    "ftpuser",
            "ftp_pass":    "Ftp@2026#Secure",
            "ftp_files": (
                "backup_2026.tar.gz,database_dump.sql,"
                "reports_Q1_2026.zip,config_backup.xml,"
                "server_logs_march.tar,employee_data.csv,"
                "financials_2026.xlsx,project_files.zip,"
                "images_archive.tar.gz,software_v2.1.exe"
            ),
            "smtp_server": "smtp.gmail.com",
            "mail_from":   "mohit.tambe@cleartrail.in",
            "mail_to":     "support@client.com",
            "mail_subjects": (
                "Q1 Report 2026,Meeting Invitation - Project Review,"
                "Invoice #INV-2026-0042,Security Alert - Unusual Login,"
                "Server Backup Complete,Weekly Status Update,"
                "Action Required: Account Verification,"
                "New Message from ClearTrail Support,"
                "Your session has expired,Scheduled Maintenance Notice"
            ),
            "rtsp_urls": (
                "rtsp://stream.example.com:554/live/channel1,"
                "rtsp://192.168.1.200:554/live/main,"
                "rtsp://cctv.company.com:554/cam1/h264,"
                "rtsp://192.168.1.201:554/cam2/h264,"
                "rtsp://192.168.1.202:554/entrance/stream"
            ),
            "yt_cdn": (
                "r5---sn-vgqs7ns7.googlevideo.com,"
                "rr3---sn-vgqskn7k.googlevideo.com,"
                "r1---sn-vgqsknzl.googlevideo.com,"
                "vd.googlevideo.com,redirector.googlevideo.com"
            ),
        }

        hint_row = tk.Frame(tp_card, bg=self.C("panel"))
        hint_row.pack(fill="x", padx=10, pady=(8, 4))
        tk.Label(hint_row,
                 text="All fields below are pre-filled with real values. "
                      "Edit any field or click Reset Defaults to restore.",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8)).pack(side="left")

        # ── Network ───────────────────────────────────────────────
        self._section_label(tp_card, "🌐  Network", pady=(8, 4))
        net_grid = tk.Frame(tp_card, bg=self.C("panel"))
        net_grid.pack(fill="x", padx=10, pady=(0, 6))

        src_ip_p   = tk.StringVar(value=TP_DEFAULTS["src_ip"])
        dst_ip_p   = tk.StringVar(value=TP_DEFAULTS["dst_ip"])
        src_port_p = tk.StringVar(value=TP_DEFAULTS["src_port"])
        dst_port_p = tk.StringVar(value=TP_DEFAULTS["dst_port"])
        imei_var   = tk.StringVar(value=TP_DEFAULTS["imei"])
        msisdn_var = tk.StringVar(value=TP_DEFAULTS["msisdn"])

        for i, (lbl, var, hint) in enumerate([
            ("Source IP",   src_ip_p,   "Source IP address"),
            ("Dest IP",     dst_ip_p,   "Destination IP address"),
            ("Source Port", src_port_p, "Ephemeral source port"),
            ("Dest Port",   dst_port_p,
             "80=HTTP 443=HTTPS 53=DNS 21=FTP 25=SMTP 5060=SIP"),
            ("IMEI",        imei_var,   "15-digit IMEI — no scientific notation"),
            ("MSISDN",      msisdn_var, "Mobile subscriber number"),
        ]):
            col = i % 2; row = i // 2
            fr = tk.Frame(net_grid, bg=self.C("panel"))
            fr.grid(row=row, column=col, sticky="w", padx=(0,20), pady=3)
            tk.Label(fr, text=lbl, width=14, anchor="w",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9, "bold")).pack(side="left")
            tk.Entry(fr, textvariable=var,
                     bg=self.C("input_bg"), fg=self.C("text"),
                     insertbackground=self.C("text"),
                     relief="flat", width=22).pack(
                side="left", ipady=4, padx=(0,6))
            tk.Label(fr, text=hint, bg=self.C("panel"),
                     fg=self.C("dim"), font=(_UI_FONT, 7),
                     wraplength=260).pack(side="left")

        # ── HTTP / HTTPS ──────────────────────────────────────────
        tk.Frame(tp_card, bg=self.C("border"),
                 height=1).pack(fill="x", padx=10, pady=4)
        self._section_label(tp_card, "🌍  HTTP / HTTPS", pady=(4, 4))
        http_grid = tk.Frame(tp_card, bg=self.C("panel"))
        http_grid.pack(fill="x", padx=10, pady=(0, 6))

        http_method_var = tk.StringVar(value=TP_DEFAULTS["http_method"])
        http_urls_var   = tk.StringVar(value=TP_DEFAULTS["http_urls"])
        http_hosts_var  = tk.StringVar(value=TP_DEFAULTS["http_hosts"])
        https_sni_var   = tk.StringVar(value=TP_DEFAULTS["https_sni"])
        user_agent_var  = tk.StringVar(value=TP_DEFAULTS["user_agent"])

        for lbl, var, hint, wd in [
            ("HTTP Method", http_method_var, "HTTP verb", 8),
            ("HTTP URLs",   http_urls_var,
             "20 real URL paths pre-filled", 58),
            ("HTTP Hosts",  http_hosts_var,
             "20 real hostnames pre-filled", 58),
            ("HTTPS SNI",   https_sni_var,
             "20 real TLS SNI hostnames pre-filled", 58),
            ("User-Agent",  user_agent_var,
             "Real Chrome 123 user-agent", 58),
        ]:
            r = tk.Frame(http_grid, bg=self.C("panel"))
            r.pack(fill="x", pady=3)
            tk.Label(r, text=lbl, width=14, anchor="w",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9, "bold")).pack(side="left")
            if lbl == "HTTP Method":
                ttk.Combobox(r, textvariable=var,
                             values=["GET","POST","PUT","DELETE","HEAD"],
                             state="readonly", width=wd).pack(
                    side="left", ipady=4, padx=(0,8))
            else:
                tk.Entry(r, textvariable=var,
                         bg=self.C("input_bg"), fg=self.C("text"),
                         insertbackground=self.C("text"),
                         relief="flat", width=wd).pack(
                    side="left", ipady=4, padx=(0,8))
            tk.Label(r, text=hint, bg=self.C("panel"),
                     fg=self.C("dim"),
                     font=(_UI_FONT, 7)).pack(side="left")

        # ── DNS ───────────────────────────────────────────────────
        tk.Frame(tp_card, bg=self.C("border"),
                 height=1).pack(fill="x", padx=10, pady=4)
        self._section_label(tp_card, "🔍  DNS", pady=(4, 4))
        dns_fr = tk.Frame(tp_card, bg=self.C("panel"))
        dns_fr.pack(fill="x", padx=10, pady=(0, 6))

        dns_server_var  = tk.StringVar(value=TP_DEFAULTS["dns_server"])
        dns_domains_var = tk.StringVar(value=TP_DEFAULTS["dns_domains"])

        for lbl, var, hint, wd in [
            ("DNS Server",  dns_server_var,
             "8.8.8.8=Google  1.1.1.1=Cloudflare  8.8.4.4=Google2", 16),
            ("DNS Domains", dns_domains_var,
             "30 real domains pre-filled", 62),
        ]:
            r = tk.Frame(dns_fr, bg=self.C("panel"))
            r.pack(fill="x", pady=3)
            tk.Label(r, text=lbl, width=14, anchor="w",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9, "bold")).pack(side="left")
            tk.Entry(r, textvariable=var,
                     bg=self.C("input_bg"), fg=self.C("text"),
                     insertbackground=self.C("text"),
                     relief="flat", width=wd).pack(
                side="left", ipady=4, padx=(0,8))
            tk.Label(r, text=hint, bg=self.C("panel"),
                     fg=self.C("dim"),
                     font=(_UI_FONT, 7)).pack(side="left")

        # ── VoIP / SIP ────────────────────────────────────────────
        tk.Frame(tp_card, bg=self.C("border"),
                 height=1).pack(fill="x", padx=10, pady=4)
        self._section_label(tp_card, "📞  VoIP / SIP", pady=(4, 4))
        voip_grid = tk.Frame(tp_card, bg=self.C("panel"))
        voip_grid.pack(fill="x", padx=10, pady=(0, 6))

        calling_var    = tk.StringVar(value=TP_DEFAULTS["calling"])
        called_var     = tk.StringVar(value=TP_DEFAULTS["called"])
        callid_var     = tk.StringVar(value=TP_DEFAULTS["callid"])
        sip_domain_var = tk.StringVar(value=TP_DEFAULTS["sip_domain"])

        for i, (lbl, var, hint) in enumerate([
            ("Calling Number", calling_var,    "Originating number"),
            ("Called Number",  called_var,     "Destination number"),
            ("Call ID",        callid_var,     "Unique call identifier"),
            ("SIP Domain",     sip_domain_var, "SIP proxy domain"),
        ]):
            col = i % 2; row = i // 2
            fr = tk.Frame(voip_grid, bg=self.C("panel"))
            fr.grid(row=row, column=col, sticky="w", padx=(0,20), pady=3)
            tk.Label(fr, text=lbl, width=16, anchor="w",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9, "bold")).pack(side="left")
            tk.Entry(fr, textvariable=var,
                     bg=self.C("input_bg"), fg=self.C("text"),
                     insertbackground=self.C("text"),
                     relief="flat", width=22).pack(
                side="left", ipady=4, padx=(0,6))
            tk.Label(fr, text=hint, bg=self.C("panel"),
                     fg=self.C("dim"),
                     font=(_UI_FONT, 7)).pack(side="left")

        # ── FTP ───────────────────────────────────────────────────
        tk.Frame(tp_card, bg=self.C("border"),
                 height=1).pack(fill="x", padx=10, pady=4)
        self._section_label(tp_card, "📁  FTP", pady=(4, 4))
        ftp_grid = tk.Frame(tp_card, bg=self.C("panel"))
        ftp_grid.pack(fill="x", padx=10, pady=(0, 6))

        ftp_server_var = tk.StringVar(value=TP_DEFAULTS["ftp_server"])
        ftp_user_var   = tk.StringVar(value=TP_DEFAULTS["ftp_user"])
        ftp_pass_var   = tk.StringVar(value=TP_DEFAULTS["ftp_pass"])
        ftp_files_var  = tk.StringVar(value=TP_DEFAULTS["ftp_files"])

        for lbl, var, hint, wd in [
            ("FTP Server",   ftp_server_var, "FTP server hostname",           30),
            ("FTP User",     ftp_user_var,   "Login username",                20),
            ("FTP Password", ftp_pass_var,   "Login password",                20),
            ("FTP Files",    ftp_files_var,  "10 real filenames pre-filled",  55),
        ]:
            r = tk.Frame(ftp_grid, bg=self.C("panel"))
            r.pack(fill="x", pady=3)
            tk.Label(r, text=lbl, width=14, anchor="w",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9, "bold")).pack(side="left")
            tk.Entry(r, textvariable=var,
                     bg=self.C("input_bg"), fg=self.C("text"),
                     insertbackground=self.C("text"),
                     relief="flat", width=wd).pack(
                side="left", ipady=4, padx=(0,8))
            tk.Label(r, text=hint, bg=self.C("panel"),
                     fg=self.C("dim"),
                     font=(_UI_FONT, 7)).pack(side="left")

        # ── SMTP / Email ──────────────────────────────────────────
        tk.Frame(tp_card, bg=self.C("border"),
                 height=1).pack(fill="x", padx=10, pady=4)
        self._section_label(tp_card, "📧  SMTP / Email", pady=(4, 4))
        smtp_grid = tk.Frame(tp_card, bg=self.C("panel"))
        smtp_grid.pack(fill="x", padx=10, pady=(0, 6))

        smtp_server_var  = tk.StringVar(value=TP_DEFAULTS["smtp_server"])
        mail_from_var    = tk.StringVar(value=TP_DEFAULTS["mail_from"])
        mail_to_var      = tk.StringVar(value=TP_DEFAULTS["mail_to"])
        mail_subject_var = tk.StringVar(value=TP_DEFAULTS["mail_subjects"])

        for lbl, var, hint, wd in [
            ("SMTP Server", smtp_server_var,
             "smtp.gmail.com / smtp.office365.com",   32),
            ("Mail From",   mail_from_var,
             "Sender email address",                  30),
            ("Mail To",     mail_to_var,
             "Recipient email address",               30),
            ("Subjects",    mail_subject_var,
             "10 real subject lines pre-filled",      55),
        ]:
            r = tk.Frame(smtp_grid, bg=self.C("panel"))
            r.pack(fill="x", pady=3)
            tk.Label(r, text=lbl, width=14, anchor="w",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9, "bold")).pack(side="left")
            tk.Entry(r, textvariable=var,
                     bg=self.C("input_bg"), fg=self.C("text"),
                     insertbackground=self.C("text"),
                     relief="flat", width=wd).pack(
                side="left", ipady=4, padx=(0,8))
            tk.Label(r, text=hint, bg=self.C("panel"),
                     fg=self.C("dim"),
                     font=(_UI_FONT, 7)).pack(side="left")

        # ── Streaming / RTSP ──────────────────────────────────────
        tk.Frame(tp_card, bg=self.C("border"),
                 height=1).pack(fill="x", padx=10, pady=4)
        self._section_label(tp_card, "🎬  Streaming / RTSP", pady=(4, 4))
        stream_grid = tk.Frame(tp_card, bg=self.C("panel"))
        stream_grid.pack(fill="x", padx=10, pady=(0, 6))

        stream_urls_var = tk.StringVar(value=TP_DEFAULTS["rtsp_urls"])
        yt_cdn_var      = tk.StringVar(value=TP_DEFAULTS["yt_cdn"])

        for lbl, var, hint, wd in [
            ("RTSP URLs",   stream_urls_var,
             "5 real RTSP stream URLs pre-filled",    60),
            ("YouTube CDN", yt_cdn_var,
             "Real Google video CDN hostnames",       55),
        ]:
            r = tk.Frame(stream_grid, bg=self.C("panel"))
            r.pack(fill="x", pady=3)
            tk.Label(r, text=lbl, width=14, anchor="w",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9, "bold")).pack(side="left")
            tk.Entry(r, textvariable=var,
                     bg=self.C("input_bg"), fg=self.C("text"),
                     insertbackground=self.C("text"),
                     relief="flat", width=wd).pack(
                side="left", ipady=4, padx=(0,8))
            tk.Label(r, text=hint, bg=self.C("panel"),
                     fg=self.C("dim"),
                     font=(_UI_FONT, 7)).pack(side="left")

        # ── Reset to Defaults button ──────────────────────────────
        tk.Frame(tp_card, bg=self.C("border"),
                 height=1).pack(fill="x", padx=10, pady=6)
        reset_row = tk.Frame(tp_card, bg=self.C("panel"))
        reset_row.pack(anchor="e", padx=10, pady=(0, 10))

        def _reset_tp():
            src_ip_p.set(TP_DEFAULTS["src_ip"])
            dst_ip_p.set(TP_DEFAULTS["dst_ip"])
            src_port_p.set(TP_DEFAULTS["src_port"])
            dst_port_p.set(TP_DEFAULTS["dst_port"])
            imei_var.set(TP_DEFAULTS["imei"])
            msisdn_var.set(TP_DEFAULTS["msisdn"])
            http_method_var.set(TP_DEFAULTS["http_method"])
            http_urls_var.set(TP_DEFAULTS["http_urls"])
            http_hosts_var.set(TP_DEFAULTS["http_hosts"])
            https_sni_var.set(TP_DEFAULTS["https_sni"])
            user_agent_var.set(TP_DEFAULTS["user_agent"])
            dns_server_var.set(TP_DEFAULTS["dns_server"])
            dns_domains_var.set(TP_DEFAULTS["dns_domains"])
            calling_var.set(TP_DEFAULTS["calling"])
            called_var.set(TP_DEFAULTS["called"])
            callid_var.set(TP_DEFAULTS["callid"])
            sip_domain_var.set(TP_DEFAULTS["sip_domain"])
            ftp_server_var.set(TP_DEFAULTS["ftp_server"])
            ftp_user_var.set(TP_DEFAULTS["ftp_user"])
            ftp_pass_var.set(TP_DEFAULTS["ftp_pass"])
            ftp_files_var.set(TP_DEFAULTS["ftp_files"])
            smtp_server_var.set(TP_DEFAULTS["smtp_server"])
            mail_from_var.set(TP_DEFAULTS["mail_from"])
            mail_to_var.set(TP_DEFAULTS["mail_to"])
            mail_subject_var.set(TP_DEFAULTS["mail_subjects"])
            stream_urls_var.set(TP_DEFAULTS["rtsp_urls"])
            yt_cdn_var.set(TP_DEFAULTS["yt_cdn"])

        tk.Button(reset_row, text="🔄  Reset to Defaults",
                  bg=self.C("input_bg"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  activebackground=self.C("border"),
                  command=_reset_tp).pack(
            side="left", ipadx=12, ipady=5)

        # ── SECTION C — Advanced Mode ──────────────────────────────
        adv_frame = tk.Frame(body, bg=self.C("bg"))

        self._section_label(adv_frame, "🔬  Protocol Selection",
                            pady=(0, 8))
        proto_card = self._card(adv_frame)
        PROTOCOLS = [
            ("Ethernet/ARP", "ARP",   True),
            ("ICMP",         "ICMP",  True),
            ("TCP",          "TCP",   True),
            ("UDP",          "UDP",   True),
            ("DNS",          "DNS",   True),
            ("DHCP",         "DHCP",  False),
            ("HTTP",         "HTTP",  True),
            ("HTTPS/TLS",    "TLS",   True),
            ("FTP",          "FTP",   False),
            ("SMTP",         "SMTP",  False),
            ("POP3",         "POP3",  False),
            ("IMAP",         "IMAP",  False),
            ("SIP",          "SIP",   False),
            ("RTP",          "RTP",   False),
        ]
        proto_vars = {}
        proto_grid = tk.Frame(proto_card, bg=self.C("panel"))
        proto_grid.pack(fill="x", padx=10, pady=8)
        for i, (label, key, default) in enumerate(PROTOCOLS):
            v = tk.BooleanVar(value=default)
            proto_vars[key] = v
            col = i % 4; row = i // 4
            tk.Checkbutton(proto_grid, text=label, variable=v,
                           bg=self.C("panel"),
                           fg=self.C("card_title"),
                           selectcolor=self.C("input_bg"),
                           activebackground=self.C("panel"),
                           font=(_UI_FONT, 9, "bold")).grid(
                row=row, column=col, sticky="w", padx=12, pady=3)

        self._section_label(adv_frame, "🌐  Network Configuration",
                            pady=(14, 8))
        net_card = self._card(adv_frame)
        src_ip_var  = tk.StringVar(value="192.168.1.0/24")
        dst_ip_var  = tk.StringVar(value="10.0.0.0/24")
        src_prt_var = tk.StringVar(value="1024-65535")
        dst_prt_var = tk.StringVar(value="80,443,53,25,110")

        for lbl, var, hint in [
            ("Source IP Range",   src_ip_var,  "e.g. 192.168.1.0/24"),
            ("Dest IP Range",     dst_ip_var,  "e.g. 10.0.0.0/24"),
            ("Source Ports",      src_prt_var, "e.g. 1024-65535"),
            ("Dest Ports",        dst_prt_var, "e.g. 80,443,53"),
        ]:
            r = tk.Frame(net_card, bg=self.C("panel"))
            r.pack(fill="x", padx=10, pady=5)
            tk.Label(r, text=lbl, width=18, anchor="w",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9, "bold")).pack(side="left")
            tk.Entry(r, textvariable=var,
                     bg=self.C("input_bg"), fg=self.C("text"),
                     insertbackground=self.C("text"),
                     relief="flat", width=28).pack(
                side="left", ipady=5, padx=(0, 8))
            tk.Label(r, text=hint, bg=self.C("panel"),
                     fg=self.C("dim"),
                     font=(_UI_FONT, 7)).pack(side="left")

        self._section_label(adv_frame, "📦  Payload & Behavior",
                            pady=(14, 8))
        pay_card = self._card(adv_frame)
        payload_var  = tk.StringVar(value="Realistic")
        pkt_sz_var   = tk.StringVar(value="Mixed")

        for lbl, var, opts in [
            ("Payload Type",  payload_var,
             ["Realistic","Random","Pattern","JSON","XML","Text"]),
            ("Packet Size",   pkt_sz_var,
             ["Mixed","Small (64B)","Medium (512B)","Large (1500B)"]),
        ]:
            r = tk.Frame(pay_card, bg=self.C("panel"))
            r.pack(fill="x", padx=10, pady=5)
            tk.Label(r, text=lbl, width=18, anchor="w",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9, "bold")).pack(side="left")
            ttk.Combobox(r, textvariable=var, values=opts,
                         state="readonly", width=22).pack(
                side="left", ipady=4)

        tcp_var  = tk.BooleanVar(value=True)
        retr_var = tk.BooleanVar(value=True)
        tk.Checkbutton(pay_card,
                       text="  Stateful TCP sessions (SYN/ACK/FIN)",
                       variable=tcp_var, bg=self.C("panel"),
                       fg=self.C("card_title"),
                       selectcolor=self.C("input_bg"),
                       activebackground=self.C("panel"),
                       font=(_UI_FONT, 9, "bold")).pack(
            anchor="w", padx=10, pady=3)
        tk.Checkbutton(pay_card,
                       text="  Simulate retransmissions (~2%)",
                       variable=retr_var, bg=self.C("panel"),
                       fg=self.C("card_title"),
                       selectcolor=self.C("input_bg"),
                       activebackground=self.C("panel"),
                       font=(_UI_FONT, 9, "bold")).pack(
            anchor="w", padx=10, pady=3)

        def _toggle_mode():
            if mode_var.get() == "Advanced":
                adv_frame.pack(fill="x", after=quick_frame)
                quick_frame.pack_forget()
            else:
                quick_frame.pack(fill="x")
                adv_frame.pack_forget()

        # ── Progress & Generate ────────────────────────────────────
        self._section_label(body, "🚀  Generate", pady=(18, 8))
        gen_card = self._card(body)

        prog_var   = tk.DoubleVar(value=0)
        status_var = tk.StringVar(value="Ready to generate.")
        cancel_ev  = threading.Event()

        prog_bar = ttk.Progressbar(
            gen_card, variable=prog_var, maximum=100,
            length=500, mode="determinate")
        prog_bar.pack(fill="x", padx=10, pady=(10, 4))

        tk.Label(gen_card, textvariable=status_var,
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=("Consolas", 9)).pack(anchor="w",
                                            padx=10, pady=(0, 8))

        btn_row = tk.Frame(gen_card, bg=self.C("panel"))
        btn_row.pack(anchor="e", padx=10, pady=(0, 10))

        gen_btn = tk.Button(btn_row, text="📡  Generate PCAP",
                            bg="#0891b2", fg="white", relief="flat",
                            font=(_UI_FONT, 11, "bold"),
                            activebackground="#0e7490",
                            activeforeground="white",
                            cursor="hand2")
        gen_btn.pack(side="left", ipadx=18, ipady=8, padx=(0, 8))

        can_btn = tk.Button(btn_row, text="⛔  Cancel",
                            bg="#374151", fg="white", relief="flat",
                            font=(_UI_FONT, 10, "bold"),
                            activebackground="#4b5563",
                            cursor="hand2",
                            state="disabled")
        can_btn.pack(side="left", ipadx=12, ipady=8)

        # ── PCAP ENGINE (pure stdlib) ──────────────────────────────
        def _pcap_global_hdr():
            """24-byte PCAP global header — little-endian, Ethernet."""
            return struct.pack("<IHHiIII",
                0xA1B2C3D4,  # magic
                2, 4,        # version
                0,           # thiszone
                0,           # sigfigs
                65535,       # snaplen
                1,           # network = Ethernet
            )

        def _pcap_pkt_hdr(ts_sec, ts_usec, caplen):
            return struct.pack("<IIII",
                ts_sec, ts_usec, caplen, caplen)

        def _rand_ip(cidr="192.168.1.0/24"):
            try:
                parts = cidr.split("/")
                base  = parts[0].split(".")
                mask  = int(parts[1]) if len(parts) > 1 else 24
                host_bits = 32 - mask
                base_int  = (int(base[0]) << 24 | int(base[1]) << 16 |
                             int(base[2]) << 8  | int(base[3]))
                rand_host = random.randint(1, (1 << host_bits) - 2)
                ip_int    = (base_int & ~((1 << host_bits) - 1)) | rand_host
                return socket.inet_ntoa(struct.pack(">I", ip_int))
            except Exception:
                return f"192.168.{random.randint(1,254)}.{random.randint(1,254)}"

        def _rand_port(spec="1024-65535"):
            try:
                if "-" in spec:
                    lo, hi = spec.split("-")
                    return random.randint(int(lo), int(hi))
                opts = [int(x) for x in spec.split(",") if x.strip()]
                return random.choice(opts) if opts else random.randint(1024,65535)
            except Exception:
                return random.randint(1024, 65535)

        def _chk(data):
            if len(data) % 2:
                data += b"\x00"
            s = sum(struct.unpack(f">{len(data)//2}H", data))
            s  = (s >> 16) + (s & 0xFFFF)
            s += (s >> 16)
            return (~s) & 0xFFFF

        def _eth(src_mac, dst_mac, etype, payload):
            def _mac(m):
                return bytes(int(x, 16) for x in m.split(":"))
            return _mac(dst_mac) + _mac(src_mac) + struct.pack(">H", etype) + payload

        def _ip_hdr(src, dst, proto, payload_len):
            ihl = 5; ver = 4; tos = 0
            tot = 20 + payload_len
            ident = random.randint(0, 65535)
            ttl = random.randint(32, 128)
            hdr = struct.pack(">BBHHHBBH4s4s",
                (ver << 4) | ihl, tos, tot, ident,
                0x4000, ttl, proto, 0,
                socket.inet_aton(src), socket.inet_aton(dst))
            chk = _chk(hdr)
            return struct.pack(">BBHHHBBH4s4s",
                (ver << 4) | ihl, tos, tot, ident,
                0x4000, ttl, proto, chk,
                socket.inet_aton(src), socket.inet_aton(dst))

        def _tcp_seg(src_p, dst_p, seq, ack, flags, payload=b""):
            # flags: SYN=0x02 ACK=0x10 FIN=0x01 RST=0x04 PSH=0x08
            hdr = struct.pack(">HHIIBBHHH",
                src_p, dst_p, seq, ack,
                0x50, flags, 65535, 0, 0)
            return hdr + payload

        def _udp_dgram(src_p, dst_p, payload):
            length = 8 + len(payload)
            return struct.pack(">HHHH",
                src_p, dst_p, length, 0) + payload

        def _icmp(type_=8, code=0, payload=b"Hello"):
            hdr  = struct.pack(">BBH", type_, code, 0)
            chk  = _chk(hdr + payload)
            return struct.pack(">BBH", type_, code, chk) + payload

        def _arp_pkt(src_ip, dst_ip):
            src_mac = ":".join(f"{random.randint(0,255):02x}" for _ in range(6))
            # ARP request
            return struct.pack(">HHBBH",
                1, 0x0800, 6, 4, 1) + \
                bytes(int(x,16) for x in src_mac.split(":")) + \
                socket.inet_aton(src_ip) + \
                b"\xff\xff\xff\xff\xff\xff" + \
                socket.inet_aton(dst_ip)

        # Realistic payloads
        # ── PAYLOAD FACTORIES ─────────────────────────────────────
        MTU = 1460  # TCP MSS — max payload per segment

        def _jpg_frag(size=65000):
            """Fake JPEG image data — realistic size for image transfer."""
            hdr = (b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01"
                   b"\x00\x01\x00\x00\xff\xdb\x00C\x00")
            body = bytes([random.randint(0,255) for _ in range(size)])
            return hdr + body + b"\xff\xd9"

        def _png_frag(size=48000):
            hdr  = b"\x89PNG\r\n\x1a\n"
            body = bytes([random.randint(0,255) for _ in range(size)])
            return hdr + body

        def _http_image_session(src_ip, dst_ip, sp, dp=80):
            """Returns list of (payload, flags) for a full HTTP image download."""
            # Real URLs extracted from HTTP_Milipol_Updated.pcap
            imgs = [
                "GET /dbase/images/mechanics/thumbs/b19mechanics182.jpg HTTP/1.1\r\nHost: www.imageafter.com\r\nAccept: image/webp,image/apng,*/*\r\nReferer: http://www.imageafter.com/\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36\r\n\r\n",
                "GET /dbase/images/objects/thumbs/b1ammo001.jpg HTTP/1.1\r\nHost: www.imageafter.com\r\nAccept: image/webp,image/apng,*/*\r\nReferer: http://www.imageafter.com/search.php?search=gun\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36\r\n\r\n",
                "GET /dbase/images/vehicles_land/thumbs/b19vehicles_land115.jpg HTTP/1.1\r\nHost: www.imageafter.com\r\nAccept: image/webp,*/*\r\nReferer: http://www.imageafter.com/search.php?search=missile\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36\r\n\r\n",
                "GET /dbase/images/vehicles_air/thumbs/b2airvehicles013.jpg HTTP/1.1\r\nHost: www.imageafter.com\r\nAccept: image/webp,*/*\r\nReferer: http://www.imageafter.com/search.php?search=pistol\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36\r\n\r\n",
                "GET /images/banners/dreamstime/2012/DreamstimeBanner01.jpg HTTP/1.1\r\nHost: www.imageafter.com\r\nAccept: image/webp,*/*\r\nReferer: http://www.imageafter.com/\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36\r\n\r\n",
            ]
            req = random.choice(imgs).encode()
            img_data = _jpg_frag(random.randint(40000, 120000))
            resp_hdr = (
                f"HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\n"
                f"Content-Length: {len(img_data)}\r\n"
                f"Cache-Control: max-age=3600\r\n\r\n"
            ).encode()
            # Break into MTU-sized chunks
            full = resp_hdr + img_data
            chunks = [full[i:i+MTU] for i in range(0, len(full), MTU)]
            return req, chunks

        def _whatsapp_session():
            """WhatsApp-like encrypted HTTPS traffic over port 443/5222."""
            # WhatsApp uses XMPP over TLS on port 5222 and HTTPS on 443
            # TLS record header + encrypted blob mimicking real sessions
            blobs = []
            # TLS ClientHello
            blobs.append(bytes([
                0x16,0x03,0x03,0x01,0x00,   # TLS 1.2, length 256
                0x01,0x00,0x00,0xfc,0x03,0x03,  # ClientHello
            ] + [random.randint(0,255) for _ in range(32)]  # random
            + [0x00,0x20] + [random.randint(0,255) for _ in range(32)]  # session
            + [0x00,0x04,0xc0,0x2c,0xc0,0x2b,  # cipher suites
               0x01,0x00,  # compression
               0x00,0x9e]  # extensions length
            + [random.randint(0,255) for _ in range(158)]))
            # Encrypted application data (chat messages, media metadata)
            for _ in range(random.randint(8, 25)):
                sz = random.randint(200, MTU)
                blob = bytes([0x17,0x03,0x03]) + \
                       struct.pack(">H", sz) + \
                       bytes([random.randint(0,255) for _ in range(sz)])
                blobs.append(blob)
            return blobs

        def _telegram_session():
            """Telegram MTProto-like traffic — port 443/80."""
            blobs = []
            # MTProto2 uses 64-bit auth_key_id + encrypted payload
            for _ in range(random.randint(5, 20)):
                auth_key_id = bytes([random.randint(0,255) for _ in range(8)])
                msg_key     = bytes([random.randint(0,255) for _ in range(16)])
                sz = random.randint(128, MTU)
                encrypted   = bytes([random.randint(0,255) for _ in range(sz)])
                blobs.append(auth_key_id + msg_key + encrypted)
            return blobs

        def _instagram_session():
            """Instagram HTTPS traffic — image uploads, feed API."""
            reqs = [
                b"POST /api/v1/media/upload/ HTTP/1.1\r\nHost: i.instagram.com\r\nContent-Type: image/jpeg\r\nX-Instagram-Rupload-Params: {}\r\n\r\n",
                b"GET /api/v1/feed/timeline/ HTTP/1.1\r\nHost: i.instagram.com\r\nAuthorization: Bearer IGT:2:xxx\r\n\r\n",
                b"POST /api/v1/direct_v2/threads/send/ HTTP/1.1\r\nHost: i.instagram.com\r\nContent-Type: application/x-www-form-urlencoded\r\n\r\n",
            ]
            req = random.choice(reqs)
            # Large media response
            img = _jpg_frag(random.randint(80000, 200000))
            resp_hdr = (
                f"HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\n"
                f"Content-Length: {len(img)}\r\n\r\n"
            ).encode()
            full   = resp_hdr + img
            chunks = [full[i:i+MTU] for i in range(0, len(full), MTU)]
            return req, chunks

        def _facebook_session():
            """Facebook HTTPS — feed, images, reactions."""
            fb_graph = (
                b"GET /v16.0/me/feed?fields=id,message,attachments "
                b"HTTP/1.1\r\nHost: graph.facebook.com\r\n"
                b"Authorization: Bearer EAAG...\r\n\r\n"
            )
            img = _jpg_frag(random.randint(50000, 150000))
            resp_hdr = (
                f"HTTP/1.1 200 OK\r\nContent-Type: image/jpeg\r\n"
                f"Content-Length: {len(img)}\r\n\r\n"
            ).encode()
            full   = resp_hdr + img
            chunks = [full[i:i+MTU] for i in range(0, len(full), MTU)]
            return fb_graph, chunks

        def _youtube_session():
            """YouTube HLS/DASH video chunk streaming."""
            # HTTPS video segment request
            req = (
                b"GET /videoplayback?itag=137&clen=5000000&range=0-524288 "
                b"HTTP/1.1\r\nHost: r5---sn-vgqs7ns7.googlevideo.com\r\n"
                b"Accept: */*\r\nRange: bytes=0-524288\r\n\r\n"
            )
            # 500KB video chunk
            chunk_data = bytes([random.randint(0,255)
                                 for _ in range(random.randint(200000, 500000))])
            resp_hdr = (
                f"HTTP/1.1 206 Partial Content\r\n"
                f"Content-Type: video/mp4\r\nContent-Length: {len(chunk_data)}\r\n\r\n"
            ).encode()
            full   = resp_hdr + chunk_data
            chunks = [full[i:i+MTU] for i in range(0, len(full), MTU)]
            return req, chunks

        def _generic_http_download(host, path, ctype, size):
            """Generic large HTTP download session — mimics imageafter.com style."""
            # Pick from real PCAP URLs when possible
            _real_paths = [
                "/inc/css/homepage.css","/inc/css/general.css",
                "/inc/js/tracker.js","/inc/js/mootools-trunk-1547-compatible.js",
                "/search.php?search=gun&x=0&y=0",
                "/search.php?search=missile&x=0&y=0",
                "/image.php?image=b19mechanics182.jpg",
                "/image.php?image=b1ammo001.jpg",
            ]
            use_path = random.choice(_real_paths) if random.random() < 0.5 else path
            req = (f"GET {use_path} HTTP/1.1\r\nHost: {host}\r\n"
                   f"User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   f"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36\r\n"
                   f"Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n"
                   f"Referer: http://www.imageafter.com/\r\n"
                   f"Connection: keep-alive\r\n\r\n").encode()
            body = bytes([random.randint(0,255) for _ in range(size)])
            resp_hdr = (
                f"HTTP/1.1 200 OK\r\nContent-Type: {ctype}\r\n"
                f"Content-Length: {len(body)}\r\nConnection: keep-alive\r\n\r\n"
            ).encode()
            full   = resp_hdr + body
            chunks = [full[i:i+MTU] for i in range(0, len(full), MTU)]
            return req, chunks

        # ── Existing small payloads ────────────────────────────────
        _DNS_Q    = (b"\xab\xcd\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00"
                     b"\x07example\x03com\x00\x00\x01\x00\x01")
        _DNS_R    = (b"\xab\xcd\x81\x80\x00\x01\x00\x01\x00\x00\x00\x00"
                     b"\x07example\x03com\x00\x00\x01\x00\x01"
                     b"\xc0\x0c\x00\x01\x00\x01\x00\x00\x01\x00\x00\x04"
                     b"\x5d\xb8\xd8\x22")
        _SIP_INV  = (b"INVITE sip:bob@example.com SIP/2.0\r\n"
                     b"Via: SIP/2.0/UDP 192.168.1.10:5060\r\n"
                     b"From: Alice <sip:alice@example.com>\r\n"
                     b"To: Bob <sip:bob@example.com>\r\n"
                     b"Call-ID: 1234567890@192.168.1.10\r\n"
                     b"CSeq: 1 INVITE\r\n\r\n")
        _TLS_HELLO = bytes([
            0x16,0x03,0x01,0x00,0x31,0x01,0x00,0x00,0x2d,
            0x03,0x03] + [random.randint(0,255) for _ in range(32)] + [
            0x00,0x00,0x02,0x00,0x2f,0x01,0x00,0x00,0x02,
            0xff,0x01,0x00,0x01,0x00])

        # ── Core frame builder — returns LIST of frames ────────────
        def _make_frames(protocol, src_ip, dst_ip,
                         src_mac, dst_mac,
                         src_ports, dst_ports, ts,
                         payload_type, tcp_stateful, seq_ctr,
                         params=None):
            """Build a realistic traffic SESSION using real parameter values."""
            p       = params or {}
            frames  = []
            rng     = random.random()
            sp      = _rand_port(src_ports)

            # ── Real value lists from parameters ─────────────────
            _http_hosts = [h.strip() for h in
                           p.get("http_hosts","www.google.com").split(",")
                           if h.strip()]
            _http_urls  = [u.strip() for u in
                           p.get("http_urls","/").split(",") if u.strip()]
            _sni_list   = [s.strip() for s in
                           p.get("https_sni","www.google.com").split(",")
                           if s.strip()]
            _dns_doms   = [d.strip() for d in
                           p.get("dns_domains","google.com").split(",")
                           if d.strip()]
            _ftp_files  = [f.strip() for f in
                           p.get("ftp_files","data.bin").split(",")
                           if f.strip()]
            _subjects   = [s.strip() for s in
                           p.get("mail_subjects","Report").split(",")
                           if s.strip()]
            _rtsp_urls  = [u.strip() for u in
                           p.get("rtsp_urls",
                                 "rtsp://stream.example.com:554/live").split(",")
                           if u.strip()]
            _yt_cdn     = [c.strip() for c in
                           p.get("yt_cdn",
                                 "r5---sn-vgqs7ns7.googlevideo.com").split(",")
                           if c.strip()]
            ua          = p.get("user_agent",
                                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36")
            calling     = p.get("calling", "919457622889")
            called      = p.get("called",  "916254957648")
            call_id     = p.get("callid",  "13092840")
            sip_domain  = p.get("sip_domain", "sip.example.com")
            ftp_user    = p.get("ftp_user",   "ftpuser")
            ftp_pass    = p.get("ftp_pass",   "ftppass")
            mail_from   = p.get("mail_from",  "sender@example.com")
            mail_to     = p.get("mail_to",    "rcpt@example.com")
            http_method = p.get("http_method","GET")
            imei        = p.get("imei",       "354077862454420")
            msisdn      = p.get("msisdn",     "919457622889")

            # ── Media file lists from folder ──────────────────────
            _media_images = p.get("media_images", [])
            _media_audio  = p.get("media_audio",  [])
            _media_video  = p.get("media_video",  [])
            _media_other  = p.get("media_other",  [])

            def _tcp_frames(src, dst, smac, dmac, sport, dport,
                            payload_chunks, base_seq=None):
                """Turn payload chunks into a TCP session with SYN/data/FIN."""
                flist = []
                seq = base_seq or random.randint(10000, 9999999)
                ack = 0

                if tcp_stateful:
                    # SYN
                    seg = _tcp_seg(sport, dport, seq, 0, 0x02)
                    ip  = _ip_hdr(src, dst, 6, len(seg))
                    flist.append(_eth(smac, dmac, 0x0800, ip + seg))
                    seq += 1
                    # SYN-ACK (reverse)
                    ack = random.randint(10000, 9999999)
                    seg = _tcp_seg(dport, sport, ack, seq, 0x12)
                    ip  = _ip_hdr(dst, src, 6, len(seg))
                    flist.append(_eth(dmac, smac, 0x0800, ip + seg))
                    ack += 1
                    # ACK
                    seg = _tcp_seg(sport, dport, seq, ack, 0x10)
                    ip  = _ip_hdr(src, dst, 6, len(seg))
                    flist.append(_eth(smac, dmac, 0x0800, ip + seg))

                # Data segments
                for chunk in payload_chunks:
                    seg  = _tcp_seg(sport, dport, seq, ack, 0x18, chunk)
                    ip   = _ip_hdr(src, dst, 6, len(seg))
                    flist.append(_eth(smac, dmac, 0x0800, ip + seg))
                    seq += len(chunk)

                if tcp_stateful:
                    # FIN
                    seg = _tcp_seg(sport, dport, seq, ack, 0x11)
                    ip  = _ip_hdr(src, dst, 6, len(seg))
                    flist.append(_eth(smac, dmac, 0x0800, ip + seg))

                return flist


            def _embed_image_file(img_path):
                """
                Real image bytes → proper HTTP/1.1 session.
                Correct TCP seq tracking so ComTrail can reassemble stream.
                """
                try:
                    with open(img_path, "rb") as _f:
                        img_bytes = _f.read()
                    fname = os.path.basename(img_path)
                    ext   = os.path.splitext(fname)[1].lower()
                    ctype = {
                        ".jpg":"image/jpeg",".jpeg":"image/jpeg",
                        ".png":"image/png", ".gif":"image/gif",
                        ".bmp":"image/bmp", ".webp":"image/webp",
                        ".ico":"image/x-icon",".svg":"image/svg+xml",
                        ".tif":"image/tiff",".tiff":"image/tiff",
                    }.get(ext, "application/octet-stream")
                    host = (random.choice(_http_hosts)
                            if _http_hosts else "www.imageafter.com")
                    req = (
                        f"GET /images/{fname} HTTP/1.1\r\n"
                        f"Host: {host}\r\n"
                        f"User-Agent: {ua}\r\n"
                        f"Accept: image/webp,image/apng,image/*,*/*;q=0.8\r\n"
                        f"Referer: http://{host}/\r\n"
                        f"Connection: keep-alive\r\n\r\n"
                    ).encode()
                    resp_hdr = (
                        f"HTTP/1.1 200 OK\r\n"
                        f"Content-Type: {ctype}\r\n"
                        f"Content-Length: {len(img_bytes)}\r\n"
                        f"Cache-Control: max-age=86400\r\n"
                        f"Server: Apache/2.4.41\r\n"
                        f"Connection: keep-alive\r\n\r\n"
                    ).encode()
                    full_resp = resp_hdr + img_bytes
                    fl = []
                    # Proper seq numbers tracking across all chunks
                    seq_c = random.randint(100000, 9000000)
                    seq_s = random.randint(100000, 9000000)

                    def _tp(sip, dip, sm, dm, sp2, dp2, sq, aq, fl2, pay=b""):
                        seg = _tcp_seg(sp2, dp2, sq, aq, fl2, pay)
                        ip  = _ip_hdr(sip, dip, 6, len(seg))
                        return _eth(sm, dm, 0x0800, ip + seg)

                    fl.append(_tp(src_ip,dst_ip,src_mac,dst_mac,sp,80,seq_c,0,0x02))
                    fl.append(_tp(dst_ip,src_ip,dst_mac,src_mac,80,sp,seq_s,seq_c+1,0x12))
                    fl.append(_tp(src_ip,dst_ip,src_mac,dst_mac,sp,80,seq_c+1,seq_s+1,0x10))
                    seq_c += 1; seq_s += 1
                    for chunk in [req[i:i+MTU] for i in range(0,len(req),MTU)]:
                        fl.append(_tp(src_ip,dst_ip,src_mac,dst_mac,sp,80,seq_c,seq_s,0x18,chunk))
                        seq_c += len(chunk)
                    for chunk in [full_resp[i:i+MTU] for i in range(0,len(full_resp),MTU)]:
                        fl.append(_tp(dst_ip,src_ip,dst_mac,src_mac,80,sp,seq_s,seq_c,0x18,chunk))
                        seq_s += len(chunk)
                    fl.append(_tp(src_ip,dst_ip,src_mac,dst_mac,sp,80,seq_c,seq_s,0x10))
                    fl.append(_tp(dst_ip,src_ip,dst_mac,src_mac,80,sp,seq_s,seq_c,0x11))
                    fl.append(_tp(src_ip,dst_ip,src_mac,dst_mac,sp,80,seq_c,seq_s+1,0x11))
                    return fl
                except Exception as e:
                    LOG.log("PCAP Generator", f"img embed error: {e}", "WARNING")
                    return []

            def _embed_audio_file(audio_path):
                """
                Real audio bytes → SIP with proper SDP body + RTP.
                SDP has m=audio with correct port and SSRC so ComTrail
                can correlate SIP Call-ID → RTP stream → reconstruct call.
                """
                try:
                    with open(audio_path, "rb") as _f:
                        audio_bytes = _f.read()
                    fname           = os.path.basename(audio_path)
                    fl              = []
                    rtp_port_caller = sp + 2
                    rtp_port_callee = 49152
                    ssrc_c          = random.randint(0x10000000, 0x7FFFFFFF)
                    ssrc_s          = random.randint(0x10000000, 0x7FFFFFFF)
                    branch          = f"z9hG4bK{call_id}"
                    call_id_full    = f"{call_id}@{src_ip}"
                    # SDP offer — port and SSRC must match RTP below
                    sdp_offer = (
                        f"v=0\r\no={calling} {ssrc_c} {ssrc_c} IN IP4 {src_ip}\r\n"
                        f"s=ComTrail Call\r\nc=IN IP4 {src_ip}\r\nt=0 0\r\n"
                        f"m=audio {rtp_port_caller} RTP/AVP 0 8 101\r\n"
                        f"a=rtpmap:0 PCMU/8000\r\na=rtpmap:8 PCMA/8000\r\n"
                        f"a=rtpmap:101 telephone-event/8000\r\n"
                        f"a=sendrecv\r\na=ssrc:{ssrc_c} cname:{calling}@{sip_domain}\r\n"
                        f"a=X-media-file:{fname}\r\n"
                    ).encode()
                    sdp_answer = (
                        f"v=0\r\no={called} {ssrc_s} {ssrc_s} IN IP4 {dst_ip}\r\n"
                        f"s=ComTrail Call\r\nc=IN IP4 {dst_ip}\r\nt=0 0\r\n"
                        f"m=audio {rtp_port_callee} RTP/AVP 0 8 101\r\n"
                        f"a=rtpmap:0 PCMU/8000\r\na=rtpmap:8 PCMA/8000\r\n"
                        f"a=sendrecv\r\na=ssrc:{ssrc_s} cname:{called}@{sip_domain}\r\n"
                    ).encode()
                    # INVITE with SDP
                    inv = (
                        f"INVITE sip:{called}@{sip_domain} SIP/2.0\r\n"
                        f"Via: SIP/2.0/UDP {src_ip}:5060;branch={branch};rport\r\n"
                        f"Max-Forwards: 70\r\n"
                        f"From: \"{calling}\" <sip:{calling}@{sip_domain}>;tag={call_id}\r\n"
                        f"To: <sip:{called}@{sip_domain}>\r\n"
                        f"Call-ID: {call_id_full}\r\n"
                        f"CSeq: 1 INVITE\r\n"
                        f"Contact: <sip:{calling}@{src_ip}:5060>\r\n"
                        f"Content-Type: application/sdp\r\n"
                        f"Content-Length: {len(sdp_offer)}\r\n\r\n"
                    ).encode() + sdp_offer
                    def _u(sip,dip,sm,dm,sp2,dp2,pay):
                        r = _udp_dgram(sp2,dp2,pay)
                        return _eth(sm,dm,0x0800,_ip_hdr(sip,dip,17,len(r))+r)
                    fl.append(_u(src_ip,dst_ip,src_mac,dst_mac,sp,5060,inv))
                    # 100 Trying
                    trying = (
                        f"SIP/2.0 100 Trying\r\nVia: SIP/2.0/UDP {src_ip}:5060;branch={branch}\r\n"
                        f"From: \"{calling}\" <sip:{calling}@{sip_domain}>;tag={call_id}\r\n"
                        f"To: <sip:{called}@{sip_domain}>\r\n"
                        f"Call-ID: {call_id_full}\r\nCSeq: 1 INVITE\r\nContent-Length: 0\r\n\r\n"
                    ).encode()
                    fl.append(_u(dst_ip,src_ip,dst_mac,src_mac,5060,sp,trying))
                    # 180 Ringing
                    ringing = (
                        f"SIP/2.0 180 Ringing\r\nVia: SIP/2.0/UDP {src_ip}:5060;branch={branch}\r\n"
                        f"From: \"{calling}\" <sip:{calling}@{sip_domain}>;tag={call_id}\r\n"
                        f"To: <sip:{called}@{sip_domain}>;tag=resp{call_id}\r\n"
                        f"Call-ID: {call_id_full}\r\nCSeq: 1 INVITE\r\nContent-Length: 0\r\n\r\n"
                    ).encode()
                    fl.append(_u(dst_ip,src_ip,dst_mac,src_mac,5060,sp,ringing))
                    # 200 OK with answer SDP
                    ok = (
                        f"SIP/2.0 200 OK\r\nVia: SIP/2.0/UDP {src_ip}:5060;branch={branch}\r\n"
                        f"From: \"{calling}\" <sip:{calling}@{sip_domain}>;tag={call_id}\r\n"
                        f"To: <sip:{called}@{sip_domain}>;tag=resp{call_id}\r\n"
                        f"Call-ID: {call_id_full}\r\nCSeq: 1 INVITE\r\n"
                        f"Contact: <sip:{called}@{dst_ip}:5060>\r\n"
                        f"Content-Type: application/sdp\r\nContent-Length: {len(sdp_answer)}\r\n\r\n"
                    ).encode() + sdp_answer
                    fl.append(_u(dst_ip,src_ip,dst_mac,src_mac,5060,sp,ok))
                    # ACK
                    ack_msg = (
                        f"ACK sip:{called}@{sip_domain} SIP/2.0\r\n"
                        f"Via: SIP/2.0/UDP {src_ip}:5060;branch={branch}ack\r\n"
                        f"From: \"{calling}\" <sip:{calling}@{sip_domain}>;tag={call_id}\r\n"
                        f"To: <sip:{called}@{sip_domain}>;tag=resp{call_id}\r\n"
                        f"Call-ID: {call_id_full}\r\nCSeq: 2 ACK\r\nContent-Length: 0\r\n\r\n"
                    ).encode()
                    fl.append(_u(src_ip,dst_ip,src_mac,dst_mac,sp,5060,ack_msg))
                    # RTP — PT=0 PCMU, SSRC=ssrc_c matches SDP offer
                    ts_r = 0
                    for i, off in enumerate(range(0, len(audio_bytes), 160)):
                        chunk = audio_bytes[off:off+160]
                        if not chunk: break
                        rtp_h = struct.pack(">BBHII", 0x80, 0x00,
                                            i & 0xFFFF, ts_r, ssrc_c)
                        r2 = _udp_dgram(rtp_port_caller, rtp_port_callee,
                                        rtp_h + chunk)
                        fl.append(_eth(src_mac,dst_mac,0x0800,
                                       _ip_hdr(src_ip,dst_ip,17,len(r2))+r2))
                        ts_r += 160
                    # BYE + 200 OK
                    bye = (
                        f"BYE sip:{called}@{sip_domain} SIP/2.0\r\n"
                        f"Via: SIP/2.0/UDP {src_ip}:5060;branch={branch}bye\r\n"
                        f"From: \"{calling}\" <sip:{calling}@{sip_domain}>;tag={call_id}\r\n"
                        f"To: <sip:{called}@{sip_domain}>;tag=resp{call_id}\r\n"
                        f"Call-ID: {call_id_full}\r\nCSeq: 3 BYE\r\nContent-Length: 0\r\n\r\n"
                    ).encode()
                    fl.append(_u(src_ip,dst_ip,src_mac,dst_mac,sp,5060,bye))
                    ok2 = (
                        f"SIP/2.0 200 OK\r\nCall-ID: {call_id_full}\r\n"
                        f"CSeq: 3 BYE\r\nContent-Length: 0\r\n\r\n"
                    ).encode()
                    fl.append(_u(dst_ip,src_ip,dst_mac,src_mac,5060,sp,ok2))
                    return fl
                except Exception as e:
                    LOG.log("PCAP Generator", f"audio embed error: {e}", "WARNING")
                    return []

            def _embed_video_file(video_path):
                """
                Random bytes matching file size → RTSP + RTP.
                SDP in DESCRIBE response with SSRC, codec, correct ports
                so ComTrail can reconstruct the video session.
                """
                try:
                    real_size  = os.path.getsize(video_path)
                    fname      = os.path.basename(video_path)
                    cap_size   = min(real_size, 10 * 1024 * 1024)
                    vid_bytes  = bytes([random.randint(0,255)
                                        for _ in range(cap_size)])
                    rtsp_url   = f"rtsp://{dst_ip}:554/media/{fname}"
                    fl         = []
                    seq_c      = random.randint(100000, 9000000)
                    seq_s      = random.randint(100000, 9000000)
                    ssrc       = random.randint(0x10000000, 0x7FFFFFFF)
                    session_id = str(random.randint(10000000, 99999999))
                    rtp_c      = sp + 2
                    rtp_s      = 6970

                    def _tp2(sip,dip,sm,dm,sp2,dp2,sq,aq,fl2,pay=b""):
                        seg = _tcp_seg(sp2,dp2,sq,aq,fl2,pay)
                        return _eth(sm,dm,0x0800,_ip_hdr(sip,dip,6,len(seg))+seg)

                    # TCP handshake
                    fl.append(_tp2(src_ip,dst_ip,src_mac,dst_mac,sp,554,seq_c,0,0x02))
                    fl.append(_tp2(dst_ip,src_ip,dst_mac,src_mac,554,sp,seq_s,seq_c+1,0x12))
                    fl.append(_tp2(src_ip,dst_ip,src_mac,dst_mac,sp,554,seq_c+1,seq_s+1,0x10))
                    seq_c += 1; seq_s += 1
                    # SDP for DESCRIBE response — CRITICAL for ComTrail
                    sdp = (
                        f"v=0\r\no=- {ssrc} {ssrc} IN IP4 {dst_ip}\r\n"
                        f"s={fname}\r\nc=IN IP4 {dst_ip}\r\nt=0 0\r\n"
                        f"a=range:npt=0-\r\n"
                        f"m=video 0 RTP/AVP 96\r\n"
                        f"a=control:{rtsp_url}/track1\r\n"
                        f"a=rtpmap:96 H264/90000\r\n"
                        f"a=fmtp:96 packetization-mode=1\r\n"
                        f"a=ssrc:{ssrc} cname:{fname}\r\n"
                    ).encode()
                    for cmd_b, resp_b in [
                        ((f"DESCRIBE {rtsp_url} RTSP/1.0\r\nCSeq: 1\r\n"
                          f"User-Agent: {ua}\r\nAccept: application/sdp\r\n\r\n").encode(),
                         (f"RTSP/1.0 200 OK\r\nCSeq: 1\r\n"
                          f"Content-Base: {rtsp_url}/\r\n"
                          f"Content-Type: application/sdp\r\n"
                          f"Content-Length: {len(sdp)}\r\n\r\n").encode() + sdp),
                        ((f"SETUP {rtsp_url}/track1 RTSP/1.0\r\nCSeq: 2\r\n"
                          f"Transport: RTP/AVP;unicast;client_port={rtp_c}-{rtp_c+1}\r\n\r\n").encode(),
                         (f"RTSP/1.0 200 OK\r\nCSeq: 2\r\n"
                          f"Session: {session_id};timeout=60\r\n"
                          f"Transport: RTP/AVP;unicast;client_port={rtp_c}-{rtp_c+1};"
                          f"server_port={rtp_s}-{rtp_s+1};ssrc={ssrc:08X}\r\n\r\n").encode()),
                        ((f"PLAY {rtsp_url} RTSP/1.0\r\nCSeq: 3\r\n"
                          f"Session: {session_id}\r\nRange: npt=0.000-\r\n\r\n").encode(),
                         (f"RTSP/1.0 200 OK\r\nCSeq: 3\r\n"
                          f"Session: {session_id}\r\nRange: npt=0.000-\r\n\r\n").encode()),
                    ]:
                        fl.append(_tp2(src_ip,dst_ip,src_mac,dst_mac,sp,554,seq_c,seq_s,0x18,cmd_b))
                        seq_c += len(cmd_b)
                        fl.append(_tp2(dst_ip,src_ip,dst_mac,src_mac,554,sp,seq_s,seq_c,0x18,resp_b))
                        seq_s += len(resp_b)
                    # RTP video — PT=96 H264, SSRC matches SDP
                    cs = MTU - 12; ts_r = 0
                    for i, off in enumerate(range(0, len(vid_bytes), cs)):
                        chunk = vid_bytes[off:off+cs]
                        last  = (off+cs) >= len(vid_bytes)
                        rtp_h = struct.pack(">BBHII", 0x80,
                            0x60|(0x80 if last else 0), i & 0xFFFF, ts_r, ssrc)
                        r2 = _udp_dgram(rtp_c, rtp_s, rtp_h + chunk)
                        fl.append(_eth(src_mac,dst_mac,0x0800,
                                       _ip_hdr(src_ip,dst_ip,17,len(r2))+r2))
                        ts_r += 3600
                    # TEARDOWN
                    td  = (f"TEARDOWN {rtsp_url} RTSP/1.0\r\nCSeq: 4\r\n"
                           f"Session: {session_id}\r\n\r\n").encode()
                    fl.append(_tp2(src_ip,dst_ip,src_mac,dst_mac,sp,554,seq_c,seq_s,0x18,td))
                    return fl
                except Exception as e:
                    LOG.log("PCAP Generator", f"video embed error: {e}", "WARNING")
                    return []

            def _embed_other_file(file_path):
                """Any file type → HTTP GET + 200 with real or random bytes."""
                try:
                    fname    = os.path.basename(file_path)
                    ext      = os.path.splitext(fname)[1].lower()
                    fsize    = os.path.getsize(file_path)
                    MAX_REAL = 5 * 1024 * 1024  # 5 MB threshold

                    if fsize <= MAX_REAL:
                        # Embed real bytes
                        with open(file_path, "rb") as _f:
                            file_bytes = _f.read()
                        embed_note = "real"
                    else:
                        # Random bytes, same size (capped at 50MB for PCAP)
                        cap = min(fsize, 50 * 1024 * 1024)
                        file_bytes = bytes([random.randint(0,255)
                                            for _ in range(cap)])
                        embed_note = "random"

                    # Map extension → Content-Type
                    ctype_map = {
                        ".pdf": "application/pdf",
                        ".zip": "application/zip",
                        ".gz":  "application/gzip",
                        ".tar": "application/x-tar",
                        ".exe": "application/octet-stream",
                        ".doc": "application/msword",
                        ".docx":"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                        ".xls": "application/vnd.ms-excel",
                        ".xlsx":"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        ".ppt": "application/vnd.ms-powerpoint",
                        ".csv": "text/csv",
                        ".xml": "application/xml",
                        ".json":"application/json",
                        ".txt": "text/plain",
                        ".html":"text/html",
                        ".sql": "application/sql",
                        ".bin": "application/octet-stream",
                    }
                    ctype = ctype_map.get(ext, "application/octet-stream")
                    host  = (random.choice(_http_hosts)
                             if _http_hosts else "files.example.com")
                    req   = (
                        f"GET /files/{fname} HTTP/1.1\r\n"
                        f"Host: {host}\r\n"
                        f"User-Agent: {ua}\r\n"
                        f"Accept: */*\r\n"
                        f"Connection: keep-alive\r\n\r\n"
                    ).encode()
                    resp_hdr = (
                        f"HTTP/1.1 200 OK\r\n"
                        f"Content-Type: {ctype}\r\n"
                        f"Content-Length: {len(file_bytes)}\r\n"
                        f"Content-Disposition: attachment; filename=\"{fname}\"\r\n"
                        f"Server: Apache/2.4.41\r\n\r\n"
                    ).encode()
                    full   = resp_hdr + file_bytes
                    chunks = [full[i:i+MTU] for i in range(0, len(full), MTU)]
                    fl     = []
                    fl.extend(_tcp_frames(
                        src_ip, dst_ip, src_mac, dst_mac,
                        sp, 80,
                        [req[i:i+MTU] for i in range(0, len(req), MTU)]))
                    fl.extend(_tcp_frames(
                        dst_ip, src_ip, dst_mac, src_mac,
                        80, sp, chunks))
                    LOG.log("PCAP Generator",
                            f"Embedded {embed_note} bytes: {fname} "
                            f"({len(file_bytes):,}B) as HTTP {ctype}")
                    return fl
                except Exception:
                    return []

            # ── Override with real media when folder is provided ──
            # Media folder was selected → ALWAYS use real files for
            # matching protocols. Mix images/other for HTTP (50/50).
            if protocol == "HTTP" and (_media_images or _media_other):
                if _media_images and _media_other:
                    if random.random() < 0.7:
                        return _embed_image_file(random.choice(_media_images))
                    else:
                        return _embed_other_file(random.choice(_media_other))
                elif _media_images:
                    return _embed_image_file(random.choice(_media_images))
                else:
                    return _embed_other_file(random.choice(_media_other))
            if protocol in ("SIP","RTP","VoIP") and _media_audio:
                return _embed_audio_file(random.choice(_media_audio))
            if protocol == "RTSP" and _media_video:
                return _embed_video_file(random.choice(_media_video))


            if protocol == "ARP":
                raw = _arp_pkt(src_ip, dst_ip)
                frames.append(_eth(src_mac, dst_mac, 0x0806, raw))

            elif protocol == "ICMP":
                for _ in range(random.randint(4, 12)):
                    pay  = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnop"
                    raw  = _icmp(8, 0, pay)
                    ip   = _ip_hdr(src_ip, dst_ip, 1, len(raw))
                    frames.append(_eth(src_mac, dst_mac, 0x0800, ip + raw))
                    # Reply
                    raw2 = _icmp(0, 0, pay)
                    ip2  = _ip_hdr(dst_ip, src_ip, 1, len(raw2))
                    frames.append(_eth(dst_mac, src_mac, 0x0800, ip2 + raw2))

            elif protocol == "DNS":
                dns_srv = p.get("dns_server", "8.8.8.8")
                for _ in range(random.randint(3, 8)):
                    dom = random.choice(_dns_doms)
                    # Build real DNS query for domain
                    labels = b"".join(
                        bytes([len(lbl)]) + lbl.encode()
                        for lbl in dom.split("."))
                    q_id = random.randint(0, 65535)
                    q   = (struct.pack(">HHHHHH",
                               q_id,0x0100,1,0,0,0) +
                           labels + b"\x00\x00\x01\x00\x01")
                    raw = _udp_dgram(sp, 53, q)
                    ip  = _ip_hdr(src_ip, dns_srv, 17, len(raw))
                    frames.append(_eth(src_mac, dst_mac, 0x0800, ip + raw))
                    # DNS Response
                    ans_ip = ".".join(str(random.randint(1,254))
                                     for _ in range(4))
                    r = (struct.pack(">HHHHHH",
                             q_id,0x8180,1,1,0,0) +
                         labels + b"\x00\x00\x01\x00\x01" +
                         b"\xc0\x0c\x00\x01\x00\x01"
                         b"\x00\x00\x00\x3c\x00\x04" +
                         socket.inet_aton(ans_ip))
                    raw2= _udp_dgram(53, sp, r)
                    ip2 = _ip_hdr(dns_srv, src_ip, 17, len(raw2))
                    frames.append(_eth(dst_mac, src_mac, 0x0800, ip2 + raw2))

            elif protocol == "UDP":
                for _ in range(random.randint(5, 15)):
                    pay = bytes([random.randint(0,255)
                                 for _ in range(random.randint(200,1400))])
                    raw = _udp_dgram(sp, _rand_port(dst_ports), pay)
                    ip  = _ip_hdr(src_ip, dst_ip, 17, len(raw))
                    frames.append(_eth(src_mac, dst_mac, 0x0800, ip + raw))

            elif protocol == "RTP":
                # Many small RTP audio packets (G.711 = 160 bytes/20ms)
                ssrc = random.randint(0, 2**32-1)
                seq_n = random.randint(0, 65535)
                ts_rtp = random.randint(0, 2**32-1)
                for i in range(random.randint(50, 200)):
                    rtp_hdr = struct.pack(">BBHII",
                        0x80, 0x00,
                        (seq_n + i) & 0xFFFF,
                        (ts_rtp + i*160) & 0xFFFFFFFF,
                        ssrc)
                    audio = bytes([random.randint(0,255) for _ in range(160)])
                    pay   = rtp_hdr + audio
                    raw   = _udp_dgram(sp, 5004, pay)
                    ip    = _ip_hdr(src_ip, dst_ip, 17, len(raw))
                    frames.append(_eth(src_mac, dst_mac, 0x0800, ip + raw))

            elif protocol == "SIP":
                pay = (
                    f"INVITE sip:{called}@{sip_domain} SIP/2.0\r\n"
                    f"Via: SIP/2.0/UDP {src_ip}:5060;branch=z9hG4bK{call_id}\r\n"
                    f"From: <sip:{calling}@{sip_domain}>;tag={call_id}\r\n"
                    f"To: <sip:{called}@{sip_domain}>\r\n"
                    f"Call-ID: {call_id}@{src_ip}\r\n"
                    f"CSeq: 1 INVITE\r\n"
                    f"Contact: <sip:{calling}@{src_ip}:5060>\r\n"
                    f"Content-Type: application/sdp\r\n"
                    f"Content-Length: 0\r\n\r\n"
                ).encode()
                raw = _udp_dgram(sp, 5060, pay)
                ip  = _ip_hdr(src_ip, dst_ip, 17, len(raw))
                frames.append(_eth(src_mac, dst_mac, 0x0800, ip + raw))
                # 200 OK
                ok  = (
                    f"SIP/2.0 200 OK\r\n"
                    f"Via: SIP/2.0/UDP {src_ip}:5060\r\n"
                    f"From: <sip:{calling}@{sip_domain}>;tag={call_id}\r\n"
                    f"To: <sip:{called}@{sip_domain}>\r\n"
                    f"Call-ID: {call_id}@{src_ip}\r\n"
                    f"CSeq: 1 INVITE\r\n\r\n"
                ).encode()
                raw2= _udp_dgram(5060, sp, ok)
                ip2 = _ip_hdr(dst_ip, src_ip, 17, len(raw2))
                frames.append(_eth(dst_mac, src_mac, 0x0800, ip2 + raw2))

            elif protocol == "HTTP":
                # Pick a realistic HTTP session type
                session_type = random.choice([
                    "image","html","api","download",
                    "facebook","instagram","youtube",
                ])
                if session_type == "image":
                    req, chunks = _http_image_session(src_ip, dst_ip, sp)
                elif session_type == "facebook":
                    req, chunks = _facebook_session()
                elif session_type == "instagram":
                    req, chunks = _instagram_session()
                elif session_type == "youtube":
                    req, chunks = _youtube_session()
                elif session_type == "download":
                    req, chunks = _generic_http_download(
                        "dl.example.com", "/files/data.bin",
                        "application/octet-stream",
                        random.randint(100000, 500000))
                elif session_type == "api":
                    body = ('{"status":"ok","data":['
                            + ",".join([f'{{"id":{i},"val":"{random.randint(0,9999)}"}}' for i in range(200)])
                            + ']}').encode()
                    hdr2 = (f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                            f"Content-Length: {len(body)}\r\n\r\n").encode()
                    req    = b"GET /api/v2/data HTTP/1.1\r\nHost: api.example.com\r\n\r\n"
                    chunks = [(hdr2+body)[i:i+MTU] for i in range(0, len(hdr2+body), MTU)]
                else:
                    html = (b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n" +
                            b"<html>" + b"A" * random.randint(5000, 30000) + b"</html>")
                    req    = b"GET / HTTP/1.1\r\nHost: www.example.com\r\n\r\n"
                    chunks = [html[i:i+MTU] for i in range(0, len(html), MTU)]

                # Request frame
                req_segs = [req[i:i+MTU] for i in range(0, len(req), MTU)]
                frames.extend(_tcp_frames(src_ip, dst_ip, src_mac, dst_mac,
                                          sp, 80, req_segs))
                # Response frames
                frames.extend(_tcp_frames(dst_ip, src_ip, dst_mac, src_mac,
                                          80, sp, chunks))

            elif protocol in ("HTTPS","TLS","WhatsApp",
                              "Telegram","Instagram"):
                sni = random.choice(_sni_list).encode()
                if protocol == "WhatsApp":
                    dport = random.choice([443, 5222])
                    blobs = _whatsapp_session()
                elif protocol == "Telegram":
                    dport = 443
                    blobs = _telegram_session()
                elif protocol == "Instagram":
                    dport = 443
                    _, chunks = _instagram_session()
                    blobs = chunks
                else:
                    dport  = 443
                    # TLS ClientHello with real SNI extension
                    sni_ext = (bytes([0x00,0x00]) +
                               struct.pack(">H", len(sni)+5) +
                               struct.pack(">H", len(sni)+3) +
                               b"\x00" +
                               struct.pack(">H", len(sni)) + sni)
                    blobs  = [_TLS_HELLO + sni_ext]
                    # Simulate 200–500KB of encrypted app data
                    total  = random.randint(200000, 500000)
                    done   = 0
                    while done < total:
                        sz   = min(MTU, total - done)
                        rec  = (bytes([0x17, 0x03, 0x03]) +
                                struct.pack(">H", sz) +
                                bytes([random.randint(0,255) for _ in range(sz)]))
                        blobs.append(rec)
                        done += sz

                frames.extend(_tcp_frames(src_ip, dst_ip, src_mac, dst_mac,
                                          sp, dport, blobs))

            elif protocol == "SMTP":
                subj = random.choice(_subjects)
                smtp_domain = p.get("smtp_server","smtp.gmail.com")
                smtp_session = [
                    f"EHLO {smtp_domain}\r\n".encode(),
                    b"AUTH LOGIN\r\n",
                    f"MAIL FROM:<{mail_from}>\r\n".encode(),
                    f"RCPT TO:<{mail_to}>\r\n".encode(),
                    b"DATA\r\n",
                    (f"From: {mail_from}\r\n"
                     f"To: {mail_to}\r\n"
                     f"Subject: {subj}\r\n"
                     f"MIME-Version: 1.0\r\n"
                     f"Content-Type: text/plain\r\n\r\n"
                     + "This is an automated report email.\n" * 200
                     + "\r\n.\r\n").encode(),
                    b"QUIT\r\n",
                ]
                frames.extend(_tcp_frames(src_ip, dst_ip, src_mac, dst_mac,
                                          sp, 25, smtp_session))

            elif protocol == "FTP":
                fname = random.choice(_ftp_files)
                ftp_ctrl = [
                    f"USER {ftp_user}\r\n".encode(),
                    f"PASS {ftp_pass}\r\n".encode(),
                    b"TYPE I\r\n",
                    b"PASV\r\n",
                    f"RETR {fname}\r\n".encode(),
                ]
                frames.extend(_tcp_frames(src_ip, dst_ip, src_mac, dst_mac,
                                          sp, 21, ftp_ctrl))
                # Data channel — large file transfer
                file_data = bytes([random.randint(0,255)
                                   for _ in range(random.randint(200000, 1000000))])
                data_chunks = [file_data[i:i+MTU]
                               for i in range(0, len(file_data), MTU)]
                frames.extend(_tcp_frames(dst_ip, src_ip, dst_mac, src_mac,
                                          20, random.randint(50000,60000),
                                          data_chunks))

            elif protocol in ("POP3","IMAP"):
                dport = 110 if protocol == "POP3" else 143
                mail_body = (b"From: sender@example.com\r\nTo: user@example.com\r\n"
                             b"Subject: Test Email\r\n\r\n" +
                             b"Email body content. " * 200)
                msgs = [
                    b"USER testuser\r\n" if protocol=="POP3"
                    else b"a001 LOGIN testuser pass\r\n",
                    b"PASS testpass\r\n" if protocol=="POP3"
                    else b"a002 SELECT INBOX\r\n",
                    b"RETR 1\r\n" if protocol=="POP3"
                    else b"a003 FETCH 1 BODY[]\r\n",
                    mail_body,
                    b"QUIT\r\n",
                ]
                frames.extend(_tcp_frames(src_ip, dst_ip, src_mac, dst_mac,
                                          sp, dport, msgs))

            elif protocol == "DHCP":
                xid = random.randint(0, 0xFFFFFFFF)
                try:
                    pay = struct.pack(">BBBBIHH",
                        1,1,6,0,xid,0,0x8000) + bytes(240)
                    raw = _udp_dgram(68, 67, pay[:248])
                    ip  = _ip_hdr("0.0.0.0","255.255.255.255",17,len(raw))
                    frames.append(_eth(src_mac,"ff:ff:ff:ff:ff:ff",
                                       0x0800,ip+raw))
                except Exception:
                    pass

            else:  # Generic TCP bulk
                size = random.randint(50000, 300000)
                body = bytes([random.randint(0,255) for _ in range(size)])
                chunks = [body[i:i+MTU] for i in range(0, len(body), MTU)]
                frames.extend(_tcp_frames(src_ip, dst_ip, src_mac, dst_mac,
                                          sp, _rand_port(dst_ports), chunks))

            return frames

        # ── Add WhatsApp/Telegram/Instagram/YouTube to scenario map ─
        # Inject media scenario — appears when media folder is set
        SCENARIOS["Media Files (from folder)"] = {
            "protocols": ["HTTP","SIP","RTSP"],
            "hint": "Embeds your real images (HTTP), audio (SIP+RTP), video (RTSP)"}
        SCENARIOS["WhatsApp + Telegram"] = {
            "protocols": ["WhatsApp","Telegram","DNS","HTTPS"],
            "hint": "Encrypted messaging — WhatsApp XMPP+TLS, Telegram MTProto"}
        SCENARIOS["Social Media"] = {
            "protocols": ["Instagram","HTTP","HTTPS","DNS"],
            "hint": "Instagram feed + image uploads, Facebook feed, image CDN"}
        SCENARIOS["Video Streaming"] = {
            "protocols": ["HTTPS","HTTP","DNS"],
            "hint": "YouTube HLS video chunks, large TCP sessions, CDN traffic"}
        sc_cb["values"] = list(SCENARIOS.keys())
        # Auto-select media scenario if folder has files
        def _auto_select_media_scenario():
            if media_counts.get('total', 0) > 0:
                scenario_var.set('Media Files (from folder)')
                hint_var.set(SCENARIOS['Media Files (from folder)']['hint'])
        media_counts['_on_scan'] = _auto_select_media_scenario

        def _get_protocols_from_scenario(sc):
            return SCENARIOS.get(sc, {}).get("protocols", ["TCP","UDP","DNS"])

        def _do_generate():
            cancel_ev.clear()
            gen_btn.config(state="disabled")
            can_btn.config(state="normal")
            prog_var.set(0)

            try:
                size_mb = float(size_var.get())
                if size_unit.get() == "GB":
                    size_mb *= 1024
                if size_mb <= 0:
                    raise ValueError("Size must be > 0")
                target_bytes = int(size_mb * 1024 * 1024)
            except ValueError as ve:
                self.after(0, lambda: self.popup(
                    "Error", f"Invalid size: {ve}", "error"))
                gen_btn.config(state="normal")
                can_btn.config(state="disabled")
                return

            out_dir  = out_dir_var.get().strip() or os.path.expanduser("~")
            prefix   = prefix_var.get().strip() or "capture"
            ts_str   = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = os.path.join(out_dir, f"{prefix}_{ts_str}.pcap")

            if mode_var.get() == "Quick":
                protos   = _get_protocols_from_scenario(scenario_var.get())
                s_cidr   = "192.168.1.0/24"
                d_cidr   = "10.0.0.0/24"
                s_ports  = "1024-65535"
                d_ports  = "80,443,53,25,110,21,5060,5222"
                pld_type = "Realistic"
                pkt_mode = "Mixed"
                stateful = True
            else:
                protos   = [k for k, v in proto_vars.items() if v.get()]
                s_cidr   = src_ip_var.get().strip()  or "192.168.1.0/24"
                d_cidr   = dst_ip_var.get().strip()  or "10.0.0.0/24"
                s_ports  = src_prt_var.get().strip() or "1024-65535"
                d_ports  = dst_prt_var.get().strip() or "80,443,53"
                pld_type = payload_var.get()
                pkt_mode = pkt_sz_var.get()
                stateful = tcp_var.get()

            if not protos:
                self.after(0, lambda: self.popup(
                    "Error", "Select at least one protocol.", "error"))
                gen_btn.config(state="normal")
                can_btn.config(state="disabled")
                return

            # Collect all traffic parameters from UI
            _tp_params = {
                "http_hosts":    http_hosts_var.get(),
                "http_urls":     http_urls_var.get(),
                "https_sni":     https_sni_var.get(),
                "dns_domains":   dns_domains_var.get(),
                "dns_server":    dns_server_var.get(),
                "calling":       calling_var.get(),
                "called":        called_var.get(),
                "callid":        callid_var.get(),
                "sip_domain":    sip_domain_var.get(),
                "ftp_user":      ftp_user_var.get(),
                "ftp_pass":      ftp_pass_var.get(),
                "ftp_files":     ftp_files_var.get(),
                "smtp_server":   smtp_server_var.get(),
                "mail_from":     mail_from_var.get(),
                "mail_to":       mail_to_var.get(),
                "mail_subjects": mail_subject_var.get(),
                "http_method":   http_method_var.get(),
                "user_agent":    user_agent_var.get(),
                "rtsp_urls":     stream_urls_var.get(),
                "yt_cdn":        yt_cdn_var.get(),
                "imei":          imei_var.get(),
                "msisdn":        msisdn_var.get(),
                # Media folder contents
                "media_images":  list(media_counts.get("images", [])),
                "media_audio":   list(media_counts.get("audio",  [])),
                "media_video":   list(media_counts.get("video",  [])),
                "media_other":   list(media_counts.get("other",  [])),
            }
            def _run():
                try:
                    mac1  = ":".join(f"{random.randint(0,255):02x}"
                                     for _ in range(6))
                    mac2  = ":".join(f"{random.randint(0,255):02x}"
                                     for _ in range(6))
                    seq_ctr   = {}
                    written   = 0
                    pkt_count = 0
                    ts_sec    = int(time.time()) - 3600
                    ts_usec   = 0

                    with open(out_path, "wb") as fh:
                        fh.write(_pcap_global_hdr())
                        written += 24

                        while written < target_bytes:
                            if cancel_ev.is_set():
                                self.after(0, lambda:
                                    status_var.set("⛔  Cancelled."))
                                break

                            proto = random.choice(protos)
                            s_ip  = _rand_ip(s_cidr)
                            d_ip  = _rand_ip(d_cidr)

                            # _make_frames returns a list (whole session)
                            session_frames = _make_frames(
                                proto, s_ip, d_ip,
                                mac1, mac2,
                                s_ports, d_ports,
                                ts_sec, pld_type,
                                stateful, seq_ctr,
                                params=_tp_params)

                            for frame in session_frames:
                                if cancel_ev.is_set():
                                    break
                                if written >= target_bytes:
                                    break
                                cap = frame[:65535]
                                hdr = _pcap_pkt_hdr(
                                    ts_sec, ts_usec, len(cap))
                                fh.write(hdr + cap)
                                written   += 16 + len(cap)
                                pkt_count += 1

                                ts_usec += random.randint(50, 5000)
                                if ts_usec >= 1_000_000:
                                    ts_sec  += 1
                                    ts_usec -= 1_000_000

                            pct = min(99, written * 100 // target_bytes)
                            if pkt_count % 200 == 0:
                                mb_done = written / 1048576
                                self.after(0, lambda p=pct, m=mb_done,
                                           n=pkt_count: (
                                    prog_var.set(p),
                                    status_var.set(
                                        f"Writing… {m:.2f} MB / "
                                        f"{size_mb:.1f} {size_unit.get()}"
                                        f" | {n:,} packets")))

                    if not cancel_ev.is_set():
                        final_mb = os.path.getsize(out_path) / 1048576
                        LOG.log("PCAP Generator",
                                f"✓ {os.path.basename(out_path)}"
                                f" | {pkt_count:,} pkts"
                                f" | {final_mb:.2f} MB")
                        self._write_history(
                            "PCAP Generate", pkt_count, "local", "✅ OK",
                            f"{final_mb:.1f}MB | "
                            f"{scenario_var.get() if mode_var.get()=='Quick' else 'Advanced'}")
                        self.after(0, lambda: (
                            prog_var.set(100),
                            status_var.set(
                                f"✅  Done! {pkt_count:,} packets → "
                                f"{os.path.basename(out_path)}"
                                f" ({final_mb:.2f} MB)"),
                            self.popup("Done",
                                f"PCAP generated:\n"
                                f"  Packets : {pkt_count:,}\n"
                                f"  Size    : {final_mb:.2f} MB\n"
                                f"  File    : {out_path}",
                                "success")))
                except Exception as ex:
                    LOG.log("PCAP Generator", f"Error: {ex}", "ERROR")
                    self.after(0, lambda: (
                        status_var.set(f"❌  {ex}"),
                        self.popup("Error", str(ex), "error")))
                finally:
                    self.after(0, lambda: (
                        gen_btn.config(state="normal"),
                        can_btn.config(state="disabled")))

            threading.Thread(target=_run, daemon=True).start()

        gen_btn.config(command=_do_generate)
        can_btn.config(command=lambda: cancel_ev.set())


        gen_btn.config(command=_do_generate)
        can_btn.config(command=lambda: cancel_ev.set())

    def show_voice_generator(self):
        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Voice Call Generator")

        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(20, 5))
        tk.Label(hdr, text="📞  Voice Call Generator",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", padx=10, pady=4,
                  command=self.show_generate_data).pack(side="right")
        tk.Frame(main, bg=self.C("border"), height=1).pack(
            fill="x", padx=50, pady=(5, 10))

        canvas, sf = self._scrollable(main)
        body = tk.Frame(sf, bg=self.C("bg"))
        body.pack(fill="x", padx=50, pady=5)

        # ── Info banner ────────────────────────────────────────
        banner = tk.Frame(body, bg=self.C("input_bg"),
                          highlightbackground=self.C("primary"),
                          highlightthickness=1)
        banner.pack(fill="x", pady=(5, 10), ipady=8, ipadx=12)

        banner_top = tk.Frame(banner, bg=self.C("input_bg"))
        banner_top.pack(fill="x")
        tk.Label(banner_top,
                 text="ℹ️  Upload a WAV file — it will be copied as-is into both "
                      "Hi3/<prefix>_a.wav  and  Hi3/<prefix>_b.wav.",
                 bg=self.C("input_bg"), fg=self.C("primary"),
                 font=(_UI_FONT, 10, "bold")).pack(side="left", anchor="w")

        tk.Label(banner,
                 text="Ensure your WAV is already in the correct format: "
                      "A-law  ·  8kHz  ·  Mono  ·  8-bit  ·  64kbps  before uploading.",
                 bg=self.C("input_bg"), fg=self.C("subtle"),
                 font=(_UI_FONT, 9)).pack(anchor="w", pady=(3, 0))

        conv_row = tk.Frame(banner, bg=self.C("input_bg"))
        conv_row.pack(anchor="w", pady=(5, 0))
        tk.Label(conv_row, text="Need to convert?  ",
                 bg=self.C("input_bg"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(side="left")
        link = tk.Label(conv_row, text="🌐  Open G711.org WAV Converter",
                        bg=self.C("input_bg"), fg=self.C("success"),
                        font=(_UI_FONT, 9, "bold", "underline"),
                        cursor="hand2")
        link.pack(side="left")
        link.bind("<Button-1>",
                  lambda e: webbrowser.open("https://www.g711.org"))

        # ══════════════════════════════════════════════════════════
        # SECTION 1 — Output & WAV File
        # ══════════════════════════════════════════════════════════
        self._section_label(body, "📂  Output & WAV File", pady=(5, 10))
        file_card = self._card(body)

        _vs = self._voice_state
        output_var     = tk.StringVar(value=_vs.get("output", ""))
        wav_var        = tk.StringVar(value=_vs.get("wav", ""))
        wav_status_var = tk.StringVar(value="")
        wav_dur_var    = tk.StringVar(value=_vs.get("wav_dur", ""))
        prefix_var     = tk.StringVar(value=_vs.get("prefix", "BanglaCall"))

        def _voice_save(*_):
            self._voice_state.update({
                "output":   output_var.get(),
                "wav":      wav_var.get(),
                "wav_dur":  wav_dur_var.get(),
                "prefix":   prefix_var.get(),
                "target":   target_var.get()   if "target_var"  in dir() else self._voice_state.get("target",  ""),
                "calling":  calling_var.get()  if "calling_var" in dir() else self._voice_state.get("calling", ""),
                "called":   called_var.get()   if "called_var"  in dir() else self._voice_state.get("called",  ""),
                "ts":       ts_var.get()        if "ts_var"      in dir() else self._voice_state.get("ts",      ""),
                "dir":      dir_var.get()       if "dir_var"     in dir() else self._voice_state.get("dir",     "Outgoing"),
                "conf":     conf_var.get()      if "conf_var"    in dir() else self._voice_state.get("conf",    ""),
            })
        for _v in (output_var, wav_var, wav_dur_var, prefix_var):
            _v.trace_add("write", _voice_save)

        def _file_row(parent, lbl, var, cmd, w=55):
            r = tk.Frame(parent, bg=self.C("panel"))
            r.pack(fill="x", pady=4, padx=8)
            tk.Label(r, text=lbl, width=18, anchor="w",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9)).pack(side="left")
            tk.Entry(r, textvariable=var, bg=self.C("input_bg"), fg=self.C("text"),
                     insertbackground=self.C("text"), relief="flat",
                     width=w).pack(side="left", ipady=5, padx=(0, 8))
            tk.Button(r, text="Browse…", bg=self.C("primary"), fg="white",
                      relief="flat", font=(_UI_FONT, 9, "bold"),
                      activebackground=self.C("border"),
                      command=cmd).pack(side="left", ipady=3)

        _file_row(file_card, "Output Folder", output_var,
                  lambda: output_var.set(filedialog.askdirectory(
                      title="Select Output Folder")))

        def pick_wav():
            p = filedialog.askopenfilename(
                title="Select WAV File",
                filetypes=[("WAV files", "*.wav"), ("All files", "*.*")])
            if not p:
                return
            wav_var.set(p)
            # Read WAV header directly with struct — works for ALL formats
            # including A-law (fmt=6), μ-law (fmt=7), PCM (fmt=1), etc.
            try:
                import struct as _st
                with open(p, "rb") as f:
                    raw = f.read(44)
                if len(raw) < 44 or raw[:4] != b"RIFF" or raw[8:12] != b"WAVE":
                    raise ValueError("Not a valid WAV file")
                fmt_code    = _st.unpack_from("<H", raw, 20)[0]
                channels    = _st.unpack_from("<H", raw, 22)[0]
                sample_rate = _st.unpack_from("<I", raw, 24)[0]
                byte_rate   = _st.unpack_from("<I", raw, 28)[0]
                bits        = _st.unpack_from("<H", raw, 34)[0]
                file_size   = os.path.getsize(p)
                dur_s       = (file_size - 44) / byte_rate if byte_rate else 0

                # Format duration as MM:SS and total seconds
                dur_mm  = int(dur_s) // 60
                dur_ss  = int(dur_s) % 60
                dur_hh  = dur_mm // 60
                dur_mm  = dur_mm % 60
                if dur_hh > 0:
                    dur_fmt = f"{dur_hh:02d}:{dur_mm:02d}:{dur_ss:02d}"
                else:
                    dur_fmt = f"{dur_mm:02d}:{dur_ss:02d}"

                fmt_names = {1: "PCM", 3: "IEEE Float", 6: "A-law", 7: "μ-law"}
                fmt_str   = fmt_names.get(fmt_code, f"fmt#{fmt_code}")
                ch_str    = "Mono" if channels == 1 else \
                            ("Stereo" if channels == 2 else f"{channels}ch")
                is_alaw   = (fmt_code == 6 and channels == 1
                             and sample_rate == 8000)
                ready_txt = "✅  Compatible — A-law 8kHz Mono" \
                            if is_alaw else \
                            "⚠️  Not A-law 8kHz Mono — please convert before uploading"
                ready_clr = self.C("success") if is_alaw else self.C("warn")

                # Store duration for do_generate to use
                wav_dur_var.set(dur_fmt)

                # Update info banner
                wav_status_var.set(
                    f"{fmt_str}  ·  {ch_str}  ·  {bits}-bit  ·  "
                    f"{sample_rate/1000:.3f} kHz  ·  "
                    f"{dur_fmt}  ({dur_s:.1f}s)  ·  "
                    f"{file_size/1024:.1f} KB")
                wav_status_lbl.config(fg=self.C("card_title"))

                # Update duration highlight label
                dur_lbl.config(
                    text=f"⏱️  Duration:  {dur_fmt}  ({dur_s:.1f} seconds)",
                    fg=self.C("success"))

                # Update ready status label
                ready_lbl.config(text=ready_txt, fg=ready_clr)

            except Exception as e:
                wav_dur_var.set("")
                wav_status_var.set(f"⚠️  Could not read WAV header: {e}")
                wav_status_lbl.config(fg=self.C("warn"))
                dur_lbl.config(text="⏱️  Duration:  —", fg=self.C("muted"))
                ready_lbl.config(text="", fg=self.C("muted"))

        _file_row(file_card, "WAV File", wav_var, pick_wav)

        # ── WAV info banner — codec + duration + status ─────────────
        info_banner = tk.Frame(file_card,
                               bg=self.C("input_bg"),
                               highlightbackground=self.C("border"),
                               highlightthickness=1)
        info_banner.pack(fill="x", padx=8, pady=(2, 6))

        # Row 1: codec details
        wav_status_lbl = tk.Label(info_banner,
                                  textvariable=wav_status_var,
                                  bg=self.C("input_bg"),
                                  fg=self.C("muted"),
                                  font=(_UI_FONT, 9))
        wav_status_lbl.pack(anchor="w", padx=12, pady=(6, 2))

        # Row 2: duration — large and prominent
        dur_lbl = tk.Label(info_banner,
                           text="⏱️  Duration:  —",
                           bg=self.C("input_bg"),
                           fg=self.C("muted"),
                           font=(_UI_FONT, 11, "bold"))
        dur_lbl.pack(anchor="w", padx=12, pady=(0, 2))

        # Row 3: ready / conversion status
        ready_lbl = tk.Label(info_banner,
                             text="",
                             bg=self.C("input_bg"),
                             fg=self.C("muted"),
                             font=(_UI_FONT, 9))
        ready_lbl.pack(anchor="w", padx=12, pady=(0, 6))

        # WAV prefix name row
        pr = tk.Frame(file_card, bg=self.C("panel"))
        pr.pack(fill="x", pady=4, padx=8)
        tk.Label(pr, text="HI3 WAV Prefix", width=18, anchor="w",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(side="left")
        tk.Entry(pr, textvariable=prefix_var, bg=self.C("input_bg"),
                 fg=self.C("text"), insertbackground=self.C("text"), relief="flat",
                 width=22).pack(side="left", ipady=5, padx=(0, 8))
        tk.Label(pr,
                 text="→ files named  <prefix>_a.wav  and  <prefix>_b.wav",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8)).pack(side="left")

        # ══════════════════════════════════════════════════════════
        # SECTION 2 — Interception Parameters
        # ══════════════════════════════════════════════════════════
        self._section_label(body, "📋  Interception Parameters", pady=(18, 10))
        param_card = self._card(body)

        fields_r1 = {}
        fields_r2 = {}

        pg = tk.Frame(param_card, bg=self.C("panel"))
        pg.pack(fill="x", padx=10, pady=8)

        def _pf(grid, label, default, col, row=0, width=20, hint="", required=False, numeric=False):
            f = tk.Frame(grid, bg=self.C("panel"))
            f.grid(row=row, column=col, padx=12, pady=4, sticky="w")
            lbl_text = f"{'* ' if required else ''}{label}"
            tk.Label(f, text=lbl_text, bg=self.C("panel"),
                     fg="#ef4444" if required else self.C("muted"),
                     font=(_UI_FONT, 9, "bold" if required else "normal")).pack(anchor="w")
            var = tk.StringVar(value=default)
            border = tk.Frame(f, bg="#ef4444" if required else self.C("border"),
                              padx=1, pady=1)
            border.pack()
            tk.Entry(border, textvariable=var, bg=self.C("input_bg"), fg=self.C("text"),
                     insertbackground=self.C("text"), relief="flat",
                     width=width).pack(ipady=5)
            if numeric:
                def _strip_non_digits(*_, v=var):
                    val = v.get()
                    cleaned = ''.join(c for c in val if c.isdigit())
                    if cleaned != val:
                        v.set(cleaned)
                var.trace_add("write", _strip_non_digits)
            if required:
                def _chk(*_, b=border, v=var):
                    b.config(bg="#ef4444" if not v.get().strip() else self.C("success"))
                var.trace_add("write", _chk)
                _chk()
            if hint:
                tk.Label(f, text=hint, bg=self.C("panel"), fg=self.C("dim"),
                         font=(_UI_FONT, 7)).pack(anchor="w")
            return var

        # Single row — all interception parameters
        # (LIID/MSISDN/IMEI/IMSI auto-set; Duration from WAV; Network ID/NAT IP/Access/Release auto)
        target_var  = _pf(pg, "Target Number",   _vs.get("target",  "919456622889"),        col=0,
                          hint="LIID & MSISDN auto-set", required=True, numeric=True)
        calling_var = _pf(pg, "Calling Number",  _vs.get("calling", "919456622889"),        col=1,
                          hint="From / CallingNumber", required=True, numeric=True)
        called_var  = _pf(pg, "Called Number",   _vs.get("called",  "916253957648"),        col=2,
                          hint="To / CalledNumber", required=True, numeric=True)
        ts_var      = _pf(pg, "Call Start",      _vs.get("ts", "24-03-2026 19:00:00"),      col=3,
                          width=22, hint="DD-MM-YYYY HH:MM:SS", required=True)
        for _v in (target_var, calling_var, called_var, ts_var):
            _v.trace_add("write", _voice_save)

        dir_var = tk.StringVar(value=_vs.get("dir", "Outgoing"))
        dir_var.trace_add("write", _voice_save)

        def _auto_direction(*_):
            tgt  = target_var.get().strip()
            call = calling_var.get().strip()
            cald = called_var.get().strip()
            if not tgt:
                return
            if call and call == tgt:
                dir_var.set("Outgoing")
            elif cald and cald == tgt:
                dir_var.set("Incoming")

        target_var.trace_add("write",  _auto_direction)
        calling_var.trace_add("write", _auto_direction)
        called_var.trace_add("write",  _auto_direction)

        # ── Call Type selector ─────────────────────────────────
        ct_frame = tk.Frame(param_card, bg=self.C("panel"))
        ct_frame.pack(fill="x", padx=10, pady=(4, 2))
        tk.Label(ct_frame, text="Call Type",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(2, 12))
        call_type_var = tk.StringVar(value=_vs.get("call_type", "Normal"))
        for ct in ["Normal", "Conference"]:
            tk.Radiobutton(
                ct_frame, text=ct,
                variable=call_type_var, value=ct,
                bg=self.C("panel"), fg=self.C("card_title"),
                selectcolor=self.C("input_bg"),
                activebackground=self.C("panel"),
                font=(_UI_FONT, 10, "bold"),
                command=lambda: _toggle_conf()).pack(side="left", padx=8)

        # ── Conference Numbers field (hidden until Conference selected) ─
        conf_outer = tk.Frame(param_card, bg=self.C("panel"))
        conf_var = tk.StringVar(
            value=_vs.get("conf", "Conf:919893586776,07000026454"))
        conf_var.trace_add("write", _voice_save)
        call_type_var.trace_add("write", lambda *_: self._voice_state.update({"call_type": call_type_var.get()}))

        conf_row = tk.Frame(conf_outer, bg=self.C("panel"))
        conf_row.pack(fill="x", padx=8, pady=4)
        tk.Label(conf_row, text="Conference Numbers",
                 width=20, anchor="w",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(side="left")
        tk.Entry(conf_row, textvariable=conf_var,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"),
                 relief="flat", width=50).pack(
            side="left", ipady=5, padx=(0, 8))
        tk.Label(conf_row,
                 text="e.g.  Conf:919893586776,07000026454",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 7)).pack(side="left")


        def _toggle_conf():
            if call_type_var.get() == "Conference":
                conf_outer.pack(fill="x", padx=2, pady=(0, 4))
            else:
                conf_outer.pack_forget()
            if 'update_preview' in dir():
                update_preview()

        # ══════════════════════════════════════════════════════════
        # SECTION 3 — Output Structure Preview
        # ══════════════════════════════════════════════════════════
        self._section_label(body, "📁  Output Structure", pady=(18, 8))
        prev_card = self._card(body)
        preview_var = tk.StringVar(value="Fill in fields above to see preview")
        prev_lbl = tk.Label(prev_card, textvariable=preview_var,
                            bg=self.C("input_bg"), fg=self.C("subtle"),
                            font=("Consolas", 9), justify="left",
                            padx=12, pady=8)
        prev_lbl.pack(anchor="w", padx=10, pady=6, fill="x")

        def update_preview(*_):
            calling = calling_var.get().strip() or "CALLING"
            called  = called_var.get().strip()  or "CALLED"
            cid     = "AUTO"
            ts      = ts_var.get().strip()      or "24-03-2026 19:00:00"
            pfx     = prefix_var.get().strip()  or "BanglaCall"
            is_conf = call_type_var.get() == "Conference"
            b_called = "O" if is_conf else called
            bname   = _voice_make_hi2_filename(ts, calling, cid, b_called, "Begin")
            cname   = _voice_make_hi3_cri_filename(ts)
            if is_conf:
                cfile = _voice_make_hi2_filename(ts, calling, cid, "C", "Begin")
                efile = _voice_make_hi2_filename(ts, calling, cid, "C", "End")
                efile = efile.replace("_I_", "_C_")
                preview_var.set(
                    f"{pfx}/\n"
                    f"  Hi2/\n"
                    f"    {bname}  ← BEGIN\n"
                    f"    {cfile}  ← CONTINUE\n"
                    f"    {efile}  ← END\n"
                    f"  Hi3/\n"
                    f"    {pfx}_a.wav\n"
                    f"    {pfx}_b.wav\n"
                    f"    {cname}"
                )
            else:
                ename   = _voice_make_hi2_filename(ts, calling, cid, called, "End")
                preview_var.set(
                    f"{pfx}/\n"
                    f"  Hi2/\n"
                    f"    {bname}\n"
                    f"    {ename}\n"
                    f"  Hi3/\n"
                    f"    {pfx}_a.wav\n"
                    f"    {pfx}_b.wav\n"
                    f"    {cname}"
                )

        # Bind all fields to live preview
        for v in (calling_var, called_var, ts_var, prefix_var, conf_var):
            v.trace_add("write", update_preview)
        call_type_var.trace_add("write", lambda *_: update_preview())
        # Restore conference panel visibility if previously selected
        _toggle_conf()
        update_preview()

        # ══════════════════════════════════════════════════════════
        # SECTION 4 — Generate
        # ══════════════════════════════════════════════════════════
        self._section_label(body, "🚀  Generate", pady=(18, 8))
        gen_card = self._card(body)

        status_var = tk.StringVar(value="Ready — fill in all fields above.")
        status_lbl = tk.Label(gen_card, textvariable=status_var,
                              bg=self.C("panel"), fg=self.C("muted"),
                              font=(_UI_FONT, 9), wraplength=900, justify="left")
        status_lbl.pack(anchor="w", padx=10, pady=(8, 6))

        btn_row = tk.Frame(gen_card, bg=self.C("panel"))
        btn_row.pack(anchor="w", padx=10, pady=(0, 12))

        def open_output():
            p = output_var.get().strip()
            if p and os.path.isdir(p):
                webbrowser.open(f"file:///{p.replace(os.sep, '/')}")

        tk.Button(btn_row, text="📁  Open Output Folder",
                  bg="#444", fg="white", relief="flat",
                  font=(_UI_FONT, 9, "bold"), activebackground="#555",
                  width=22, command=open_output).pack(side="left", padx=(0, 10))

        def do_generate():
            out_path = output_var.get().strip()
            wav_path = wav_var.get().strip()
            target   = target_var.get().strip()
            liid     = target   # LIID always equals Target Number
            msisdn   = target   # MSISDN always equals Target Number
            calling  = calling_var.get().strip()
            called   = called_var.get().strip()
            ts       = ts_var.get().strip()
            dur_raw  = wav_dur_var.get().strip()   # duration from WAV header
            direction= dir_var.get().strip()
            # Auto-generated / fixed values
            imei     = "35" + "".join(str(random.randint(0, 9)) for _ in range(13))
            imsi     = "405" + "".join(str(random.randint(0, 9)) for _ in range(12))
            net_id   = ":".join(f"{random.randint(0, 0xFFFF):04X}" for _ in range(8))
            nat_ip   = ".".join(str(random.randint(1, 254)) for _ in range(4))
            access   = "EUTRAN"
            rel      = "Normal call clearing"
            pfx      = prefix_var.get().strip() or "BanglaCall"

            # Validate
            missing = []
            for fld, val in [("Output Folder", out_path), ("Target Number", target),
                              ("Calling Number", calling), ("Called Number", called),
                              ("Call Start", ts)]:
                if not val:
                    missing.append(fld)
            if missing:
                return self.popup("Error",
                    f"Required fields missing:\n" + "\n".join(f"  • {m}" for m in missing),
                    "error")
            if not dur_raw:
                return self.popup("Error",
                    "Duration could not be read.\nPlease select a valid WAV file first.",
                    "error")

            call_id = _next_call_id()

            dur_secs, dur_fmt = _voice_parse_duration(dur_raw)

            status_var.set("Generating voice call files…")
            status_lbl.config(fg=self.C("muted"))

            is_conf  = call_type_var.get() == "Conference"
            conf_nums= conf_var.get().strip()
            cell_id  = "404-93-200233-252"
            tgt_ip   = "2401:4900:5269:ba7b:4c81:f052:834b:6884"

            def task():
                error    = None
                warnings = []
                try:
                    os.makedirs(out_path, exist_ok=True)
                    if is_conf:
                        paths = _voice_generate_conf_call(
                            output_dir      = out_path,
                            call_id         = call_id,
                            liid            = liid,
                            msisdn          = msisdn,
                            target_number   = target,
                            calling_number  = calling,
                            conf_numbers    = conf_nums,
                            timestamp_start = ts,
                            duration_secs   = dur_secs,
                            duration_fmt    = dur_fmt,
                            network_id      = net_id or "Amprpnasb18",
                            imei            = imei,
                            imsi            = imsi,
                            cell_id         = cell_id,
                            target_ip       = tgt_ip,
                            access_type     = access or "EUTRAN",
                            call_direction  = direction,
                            wav_source_path = wav_path,
                            call_name_prefix= pfx,
                        )
                        warnings = paths.get("warnings", [])
                        LOG.log("Voice Generator",
                                f"✓ Conference: Begin={paths['begin_name']} "
                                f"Cont={paths['cont_name']} "
                                f"End={paths['end_name']} "
                                f"CRI={paths['cri_name']}")
                    else:
                        paths = _voice_generate_call(
                            output_dir      = out_path,
                            call_id         = call_id,
                            liid            = liid,
                            msisdn          = msisdn,
                            target_number   = target,
                            calling_number  = calling,
                            called_number   = called,
                            timestamp_start = ts,
                            duration_secs   = dur_secs,
                            duration_fmt    = dur_fmt,
                            network_id      = net_id or "000:000:000",
                            imei            = imei,
                            imsi            = imsi,
                            nat_ip          = nat_ip or "0.0.0.0",
                            access_type     = access or "EUTRAN",
                            call_direction  = direction,
                            release_reason  = rel or "Normal call clearing",
                            wav_source_path = wav_path,
                            call_name_prefix= pfx,
                        )
                    warnings = paths.get("warnings", [])
                    LOG.log("Voice Generator",
                            f"✓ Generated  Begin={paths['begin_name']}  "
                            f"End={paths['end_name']}  "
                            f"CRI={paths['cri_name']}")
                    for w in warnings:
                        LOG.log("Voice Generator", w, "WARNING")

                except Exception as exc:
                    import traceback
                    error = str(exc)
                    LOG.log("Voice Generator",
                            f"FAILED: {exc}\n{traceback.format_exc()}", "ERROR")

                def _ui():
                    if error:
                        status_var.set(f"❌  {error}")
                        status_lbl.config(fg=self.C("error"))
                    elif warnings:
                        status_var.set(
                            f"✅  Files generated with warnings — "
                            f"check Logs tab for details  →  {out_path}")
                        status_lbl.config(fg=self.C("warn"))
                        self.popup("Done with Warnings",
                                   "\n\n".join(warnings), "warning")
                    else:
                        conv_note = ""
                        if warnings:
                            infos = [w for w in warnings if w.startswith("ℹ️")]
                            if infos:
                                conv_note = "\n\n" + infos[0]
                        status_var.set(
                            f"✅  Voice call files generated  →  {out_path}")
                        status_lbl.config(fg=self.C("success"))
                        self._write_history("Voice Generate", 3, "local",
                                            "✅ OK", out_path)
                        self.popup("Success",
                                   f"Files generated successfully:\n\n"
                                   f"  Hi2/  {paths['begin_name']}\n"
                                   f"        {paths['end_name']}\n"
                                   f"  Hi3/  {pfx}_a.wav  (A-law converted)\n"
                                   f"        {pfx}_b.wav  (A-law converted)\n"
                                   f"        {paths['cri_name']}"
                                   f"{conv_note}",
                                   "success")
                self.after(0, _ui)

            threading.Thread(target=task, daemon=True).start()

        tk.Button(btn_row, text="🚀  Generate Voice Call Files",
                  bg=self.C("success"), fg="white", relief="flat",
                  font=(_UI_FONT, 11, "bold"), activebackground=self.C("border"),
                  width=26, command=do_generate).pack(side="left")
        main.bind_all("<Control-Return>", lambda e: do_generate())

    # ──────────────────────────────────────────────────────────────
    # BULK VOICE CALL GENERATOR
    # ──────────────────────────────────────────────────────────────
    def show_bulk_voice_generator(self):
        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Bulk Voice Call Generator")

        # ── Header ─────────────────────────────────────────────────
        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(20, 5))
        tk.Label(hdr, text="📞  Bulk Voice Call Generator",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", padx=10, pady=4,
                  command=self.show_generate_data).pack(side="right")
        tk.Frame(main, bg=self.C("border"), height=1).pack(
            fill="x", padx=50, pady=(5, 0))

        # ── Tab bar ────────────────────────────────────────────────
        tab_var = tk.StringVar(value="csv")
        tab_bar = tk.Frame(main, bg=self.C("panel"),
                           highlightbackground=self.C("border"),
                           highlightthickness=1)
        tab_bar.pack(fill="x", padx=50, pady=(0, 0))

        csv_frame   = tk.Frame(main, bg=self.C("bg"))
        quick_frame = tk.Frame(main, bg=self.C("bg"))

        tab_btns = {}
        def _switch_tab(tab):
            tab_var.set(tab)
            for t, b in tab_btns.items():
                b.config(bg=self.C("primary") if t == tab
                         else self.C("panel"),
                         fg="white" if t == tab
                         else self.C("muted"))
            if tab == "csv":
                quick_frame.pack_forget()
                csv_frame.pack(fill="both", expand=True)
            else:
                csv_frame.pack_forget()
                quick_frame.pack(fill="both", expand=True)

            # Rebind mousewheel app-wide to the active tab's canvas.
            # Two _scrollable() calls compete via bind_all("<Enter>"); the
            # last one registered always wins, breaking scroll on the other
            # tab. Overriding bind_all here after each tab switch fixes it.
            active_c = cc if tab == "csv" else qc
            def _scroll(event, c=active_c):
                if not c.winfo_exists():
                    return
                if event.num == 4:
                    c.yview_scroll(-1, "units")
                elif event.num == 5:
                    c.yview_scroll(1, "units")
                else:
                    c.yview_scroll(int(-1 * (event.delta / 120)), "units")
            active_c.bind_all("<MouseWheel>", _scroll)
            active_c.bind_all("<Button-4>",   _scroll)
            active_c.bind_all("<Button-5>",   _scroll)

        for val, lbl in [("csv",   "📋  CSV Mode"),
                          ("quick", "⚡  Quick Mode")]:
            b = tk.Button(tab_bar, text=lbl,
                          bg=self.C("panel"), fg=self.C("muted"),
                          relief="flat",
                          font=(_UI_FONT, 10, "bold"),
                          padx=22, pady=10, cursor="hand2",
                          command=lambda v=val: _switch_tab(v))
            b.pack(side="left")
            tab_btns[val] = b

        # ══════════════════════════════════════════════════════════
        # CSV MODE TAB
        # ══════════════════════════════════════════════════════════
        _bvs = self._bulk_voice_state

        cc, csf = self._scrollable(csv_frame)
        cb = tk.Frame(csf, bg=self.C("bg"))
        cb.pack(fill="x", padx=50, pady=5)

        # ── Section: Files ─────────────────────────────────────────
        self._section_label(cb, "📂  Files", pady=(5, 8))
        fc = self._card(cb)

        csv_var    = tk.StringVar(value=_bvs.get("csv_path", ""))
        out_var    = tk.StringVar(value=_bvs.get("out_path", ""))
        wav_dir_var = tk.StringVar(value=_bvs.get("wav_dir", ""))
        csv_var.trace_add("write",     lambda *_: self._bulk_voice_state.update({"csv_path": csv_var.get()}))
        out_var.trace_add("write",     lambda *_: self._bulk_voice_state.update({"out_path": out_var.get()}))
        wav_dir_var.trace_add("write", lambda *_: self._bulk_voice_state.update({"wav_dir":  wav_dir_var.get()}))

        def _file_row(parent, lbl, var, cmd, w=52):
            r = tk.Frame(parent, bg=self.C("panel"))
            r.pack(fill="x", pady=4, padx=10)
            tk.Label(r, text=lbl, width=20, anchor="w",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9)).pack(side="left")
            tk.Entry(r, textvariable=var,
                     bg=self.C("input_bg"), fg=self.C("text"),
                     insertbackground=self.C("text"),
                     relief="flat", width=w).pack(
                side="left", ipady=5, padx=(0, 8))
            tk.Button(r, text="Browse…",
                      bg=self.C("primary"), fg="white",
                      relief="flat", font=(_UI_FONT, 9, "bold"),
                      command=cmd).pack(side="left", ipady=3)

        _file_row(fc, "Input CSV File", csv_var,
                  lambda: csv_var.set(filedialog.askopenfilename(
                      title="Select CSV",
                      filetypes=[("CSV", "*.csv"), ("All", "*.*")])))
        _file_row(fc, "Output Folder", out_var,
                  lambda: out_var.set(
                      filedialog.askdirectory(title="Select Output Folder")))
        _file_row(fc, "WAV Files Folder", wav_dir_var,
                  lambda: wav_dir_var.set(
                      filedialog.askdirectory(
                          title="Select WAV Files Folder")))

        # WAV folder status
        wav_status_var = tk.StringVar(value="")
        wav_status_lbl = tk.Label(fc, textvariable=wav_status_var,
                                  bg=self.C("panel"), fg=self.C("dim"),
                                  font=(_UI_FONT, 8))
        wav_status_lbl.pack(anchor="w", padx=10, pady=(0, 4))

        def _on_wav_dir(*_):
            d = wav_dir_var.get().strip()
            if not d or not os.path.isdir(d):
                wav_status_var.set("")
                return
            wavs = [f for f in os.listdir(d)
                    if f.lower().endswith(".wav")]
            if wavs:
                wav_status_var.set(
                    f"  ✅  {len(wavs)} WAV file(s) found in folder")
                wav_status_lbl.config(fg=self.C("success"))
            else:
                wav_status_var.set(
                    "  ⚠️  No WAV files found — silent placeholders will be used")
                wav_status_lbl.config(fg=self.C("warn"))
        wav_dir_var.trace_add("write", _on_wav_dir)

        # Sample CSV download
        sample_row = tk.Frame(fc, bg=self.C("panel"))
        sample_row.pack(fill="x", padx=10, pady=(2, 8))
        tk.Label(sample_row, text="Need a template?", width=20,
                 anchor="w", bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8)).pack(side="left")

        def _download_sample():
            path = filedialog.asksaveasfilename(
                title="Save Sample CSV",
                defaultextension=".csv",
                filetypes=[("CSV", "*.csv")],
                initialfile="bulk_voice_sample.csv")
            if not path: return
            rows = [
                # Header
                ["CallType","LIID","MSISDN","TargetNumber",
                 "CallingNumber","CalledNumber","IMEI","IMSI",
                 "CallID","CallStart","Duration",
                 "NetworkID","NATIP","AccessType",
                 "CallDirection","ReleaseReason",
                 "ConfNumbers","CellID","TargetIP",
                 "Prefix","WAVFile"],
                # Normal call example
                ["Normal","919456622889","919456622889","919456622889",
                 "919456622889","916253957648",
                 "354076826454420","405863169296834",
                 "13092840","24-03-2026 10:00:00","00:02:30",
                 "2405:0200:0381:1606:00:00:00:00:00:00:00:087",
                 "182.75.187.91","EUTRAN",
                 "Outgoing","Normal call clearing",
                 "","","",
                 "NormalCall_1","call1.wav"],
                # Conference call example
                ["Conference","919456622890","919456622890","919456622890",
                 "919456622890","CONF",
                 "354076826454421","405863169296835",
                 "13092841","24-03-2026 10:05:00","00:03:00",
                 "Amprpnasb18","","EUTRAN",
                 "Outgoing","Normal call clearing",
                 "Conf:919893586776,07000026454",
                 "404-93-200233-252",
                 "2401:4900:5269:ba7b:4c81:f052:834b:6884",
                 "ConfCall_1","call2.wav"],
                # Another normal call
                ["Normal","919456622891","919456622891","919456622891",
                 "919456622891","916253957649",
                 "354076826454422","405863169296836",
                 "13092842","24-03-2026 10:10:00","00:01:45",
                 "2405:0200:0381:1606:00:00:00:00:00:00:00:088",
                 "182.75.187.92","EUTRAN",
                 "Incoming","Normal call clearing",
                 "","","",
                 "NormalCall_2","call3.wav"],
            ]
            try:
                with open(path, "w", newline="",
                          encoding="utf-8") as f:
                    csv.writer(f).writerows(rows)
                LOG.log("Bulk Voice", f"Sample CSV saved → {path}")
                self.popup("Saved",
                           f"Sample CSV saved:\n{os.path.basename(path)}\n\n"
                           "Includes Normal + Conference call examples.\n"
                           "WAVFile column = filename inside WAV folder.",
                           "success")
            except Exception as e:
                self.popup("Error", str(e), "error")

        tk.Button(sample_row, text="⬇️  Download Sample CSV",
                  bg=self.C("primary"), fg="white", relief="flat",
                  font=(_UI_FONT, 9, "bold"),
                  command=_download_sample).pack(
            side="left", ipady=3, padx=2)
        tk.Label(sample_row,
                 text="  ← includes Normal + Conference call examples "
                      "with all required columns",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8)).pack(side="left")

        # ── Section: CSV Column Reference ──────────────────────────
        self._section_label(cb, "📋  CSV Column Reference",
                            pady=(16, 8))
        ref_card = self._card(cb)
        ref_grid = tk.Frame(ref_card, bg=self.C("panel"))
        ref_grid.pack(fill="x", padx=10, pady=6)

        COL_DEFS = [
            ("CallType",       "Normal or Conference",                    True),
            ("LIID",           "Lawful Interception ID",                  True),
            ("MSISDN",         "Mobile subscriber number",                True),
            ("TargetNumber",   "Intercept target number",                 True),
            ("CallingNumber",  "Calling party number",                    True),
            ("CalledNumber",   "Called party (or CONF)",                  True),
            ("IMEI",           "15-digit device IMEI",                    True),
            ("IMSI",           "15-digit subscriber identity",            True),
            ("CallID",         "Unique call ID (integer)",                True),
            ("CallStart",      "DD-MM-YYYY HH:MM:SS",                     True),
            ("Duration",       "HH:MM:SS or seconds",                     True),
            ("NetworkID",      "Network identifier string",               False),
            ("NATIP",          "NAT IP address",                          False),
            ("AccessType",     "e.g. EUTRAN",                            False),
            ("CallDirection",  "Outgoing or Incoming",                    False),
            ("ReleaseReason",  "e.g. Normal call clearing",               False),
            ("ConfNumbers",    "Conference: Conf:num1,num2 (Conference)", False),
            ("CellID",         "Cell tower ID (Conference only)",         False),
            ("TargetIP",       "Target IP (Conference only)",             False),
            ("Prefix",         "Output folder name prefix",               False),
            ("WAVFile",        "WAV filename inside WAV folder",          False),
        ]
        for i, (col, desc, req) in enumerate(COL_DEFS):
            row_bg = (self.C("input_bg") if i % 2 == 0
                      else self.C("panel"))
            fr = tk.Frame(ref_grid, bg=row_bg)
            fr.pack(fill="x")
            req_lbl = tk.Label(fr, text="*" if req else " ",
                               bg=row_bg,
                               fg=self.C("error") if req
                               else self.C("dim"),
                               font=(_UI_FONT, 9, "bold"),
                               width=2)
            req_lbl.pack(side="left", padx=(6, 0))
            tk.Label(fr, text=col,
                     bg=row_bg, fg=self.C("success"),
                     font=("Consolas", 9, "bold"),
                     width=18, anchor="w").pack(side="left",
                                                padx=(0, 8))
            tk.Label(fr, text=desc,
                     bg=row_bg, fg=self.C("subtle"),
                     font=(_UI_FONT, 9),
                     anchor="w").pack(side="left", fill="x",
                                      expand=True, pady=3)
        tk.Label(ref_card,
                 text="  * = required   |   All others use sensible defaults if blank",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8)).pack(
            anchor="w", padx=10, pady=(4, 8))

        # ── Section: Generate ──────────────────────────────────────
        self._section_label(cb, "🚀  Generate", pady=(16, 8))
        gen_card = self._card(cb)

        prog_var    = tk.DoubleVar(value=0)
        status_var  = tk.StringVar(
            value="Ready — select CSV, output folder, and WAV folder.")
        cancel_ev   = threading.Event()

        prog_bar = ttk.Progressbar(gen_card, variable=prog_var,
                                   maximum=100, length=500,
                                   mode="determinate")
        prog_bar.pack(fill="x", padx=10, pady=(10, 4))

        status_lbl = tk.Label(gen_card, textvariable=status_var,
                              bg=self.C("panel"), fg=self.C("muted"),
                              font=(_UI_FONT, 9), wraplength=900,
                              justify="left")
        status_lbl.pack(anchor="w", padx=10, pady=(0, 6))

        # Results treeview
        rf = tk.Frame(gen_card, bg=self.C("panel"))
        rf.pack(fill="x", padx=10, pady=(0, 6))
        style = ttk.Style()
        style.configure("BV.Treeview",
                        background=self.C("input_bg"),
                        foreground=self.C("text"),
                        rowheight=22,
                        fieldbackground=self.C("input_bg"),
                        borderwidth=0)
        style.configure("BV.Treeview.Heading",
                        background=self.C("primary"),
                        foreground="white",
                        font=(_UI_FONT, 9, "bold"),
                        relief="flat")
        style.map("BV.Treeview",
                  background=[("selected", self.C("primary"))],
                  foreground=[("selected", "#ffffff")])

        tcols = ("#", "Type", "Prefix", "Calling", "Called",
                 "Duration", "WAV", "Status")
        res_tree = ttk.Treeview(rf, columns=tcols,
                                show="headings", height=10,
                                style="BV.Treeview")
        for col, w in [("#", 40), ("Type", 80),
                       ("Prefix", 160), ("Calling", 130),
                       ("Called", 130), ("Duration", 80),
                       ("WAV", 120), ("Status", 120)]:
            res_tree.heading(col, text=col)
            res_tree.column(col, width=w, anchor="w")
        vsb = ttk.Scrollbar(rf, orient="vertical",
                            command=res_tree.yview)
        res_tree.configure(yscrollcommand=vsb.set)
        res_tree.pack(side="left", fill="x", expand=True)
        vsb.pack(side="right", fill="y")
        _bind_mousewheel(res_tree, vsb)

        res_tree.tag_configure("ok",
                               foreground="#22c55e")
        res_tree.tag_configure("err",
                               foreground="#f87171")
        res_tree.tag_configure("warn",
                               foreground="#f0a84a")

        btn_row = tk.Frame(gen_card, bg=self.C("panel"))
        btn_row.pack(anchor="w", padx=10, pady=(4, 12))

        def open_output():
            p = out_var.get().strip()
            if p and os.path.isdir(p):
                webbrowser.open(
                    f"file:///{p.replace(os.sep, '/')}")

        tk.Button(btn_row, text="📁  Open Output Folder",
                  bg="#374151", fg="white", relief="flat",
                  font=(_UI_FONT, 9, "bold"),
                  command=open_output).pack(
            side="left", padx=(0, 10), ipady=4, ipadx=8)

        gen_btn = tk.Button(btn_row,
                            text="🚀  Generate All Calls",
                            bg=self.C("success"), fg="white",
                            relief="flat",
                            font=(_UI_FONT, 11, "bold"),
                            activebackground="#059669")
        gen_btn.pack(side="left", ipady=6, ipadx=16, padx=(0, 8))

        can_btn = tk.Button(btn_row, text="⛔  Cancel",
                            bg="#374151", fg="white",
                            relief="flat",
                            font=(_UI_FONT, 10, "bold"),
                            state="disabled")
        can_btn.pack(side="left", ipady=6, ipadx=12)

        def _do_csv_generate():
            csv_path = csv_var.get().strip()
            out_path = out_var.get().strip()
            wav_dir  = wav_dir_var.get().strip()

            if not csv_path or not os.path.isfile(csv_path):
                return self.popup("Error",
                                  "Please select a valid CSV file.",
                                  "error")
            if not out_path:
                return self.popup("Error",
                                  "Please select an output folder.",
                                  "error")

            cancel_ev.clear()
            gen_btn.config(state="disabled")
            can_btn.config(state="normal")
            prog_var.set(0)
            res_tree.delete(*res_tree.get_children())
            status_var.set("Reading CSV…")
            status_lbl.config(fg=self.C("muted"))

            def _run():
                results = []
                error   = None
                try:
                    with open(csv_path, "r",
                              encoding="utf-8-sig") as f:
                        rows = list(csv.DictReader(f))

                    total = len(rows)
                    LOG.log("Bulk Voice",
                            f"Starting bulk generation: "
                            f"{total} call(s)")

                    # Build WAV lookup from folder
                    wav_lookup = {}
                    if wav_dir and os.path.isdir(wav_dir):
                        for fn in os.listdir(wav_dir):
                            if fn.lower().endswith(".wav"):
                                wav_lookup[fn.lower()] = \
                                    os.path.join(wav_dir, fn)

                    os.makedirs(out_path, exist_ok=True)

                    def _g(row, *keys):
                        for k in keys:
                            v = row.get(k, "").strip().strip('"')
                            if v: return v
                        return ""

                    for idx, row in enumerate(rows, 1):
                        if cancel_ev.is_set():
                            break

                        pct = int((idx - 1) * 100 / total)
                        self.after(0, lambda p=pct, i=idx, t=total:
                                   (prog_var.set(p),
                                    status_var.set(
                                        f"Generating call "
                                        f"{i} / {t}…")))

                        call_type = _g(row, "CallType",
                                       "calltype",
                                       "Type").upper()
                        is_conf   = "CONF" in call_type

                        liid      = _g(row, "LIID")
                        msisdn    = _g(row, "MSISDN")
                        target    = _g(row, "TargetNumber",
                                       "Target")
                        calling   = _g(row, "CallingNumber",
                                       "Calling")
                        called    = _g(row, "CalledNumber",
                                       "Called")
                        imei      = (_g(row, "IMEI")
                                     or "000000000000000")
                        imsi      = (_g(row, "IMSI")
                                     or "000000000000000")
                        call_id   = _g(row, "CallID", "CallId")
                        ts        = (_g(row, "CallStart",
                                        "Timestamp")
                                     or time.strftime(
                                         "%d-%m-%Y %H:%M:%S"))
                        dur_raw   = (_g(row, "Duration")
                                     or "00:01:00")
                        net_id    = (_g(row, "NetworkID",
                                        "NetworkId")
                                     or "000:000:000")
                        nat_ip    = (_g(row, "NATIP",
                                        "NatIP", "NAT_IP")
                                     or "0.0.0.0")
                        access    = (_g(row, "AccessType")
                                     or "EUTRAN")
                        direction = (_g(row, "CallDirection",
                                        "Direction")
                                     or "Outgoing")
                        rel       = (_g(row, "ReleaseReason",
                                        "Release")
                                     or "Normal call clearing")
                        conf_nums = _g(row, "ConfNumbers",
                                       "ConferenceNumbers")
                        cell_id   = (_g(row, "CellID",
                                        "CellId")
                                     or "404-93-200233-252")
                        tgt_ip    = (_g(row, "TargetIP",
                                        "Target_IP")
                                     or "0.0.0.0")
                        prefix    = (_g(row, "Prefix")
                                     or f"Call_{idx:04d}")
                        wav_file  = _g(row, "WAVFile",
                                       "WAV", "wav_file")

                        # Validate required fields
                        missing = []
                        for fld, val in [
                            ("LIID",          liid),
                            ("MSISDN",        msisdn),
                            ("TargetNumber",  target),
                            ("CallingNumber", calling),
                            ("CalledNumber",  called),
                            ("CallID",        call_id),
                        ]:
                            if not val:
                                missing.append(fld)

                        if missing:
                            results.append({
                                "idx":      idx,
                                "type":     "Conference"
                                            if is_conf
                                            else "Normal",
                                "prefix":   prefix,
                                "calling":  calling or "—",
                                "called":   called  or "—",
                                "duration": dur_raw,
                                "wav":      "—",
                                "status":   "❌ Skip",
                                "note":     f"Missing: "
                                            f"{', '.join(missing)}",
                            })
                            LOG.log("Bulk Voice",
                                    f"  Row {idx}: skipped — "
                                    f"missing {missing}",
                                    "WARNING")
                            continue

                        # Resolve WAV path
                        wav_path = ""
                        wav_note = "silent"
                        if wav_file:
                            # Exact match first
                            key = wav_file.lower()
                            if key in wav_lookup:
                                wav_path = wav_lookup[key]
                                wav_note = wav_file
                            elif wav_dir and os.path.isfile(
                                    os.path.join(wav_dir,
                                                 wav_file)):
                                wav_path = os.path.join(
                                    wav_dir, wav_file)
                                wav_note = wav_file
                            else:
                                wav_note = "⚠ not found"
                                LOG.log("Bulk Voice",
                                        f"  Row {idx}: WAV "
                                        f"'{wav_file}' not found"
                                        f" in folder",
                                        "WARNING")

                        try:
                            call_id_int = int(call_id)
                        except ValueError:
                            call_id_int = 20320640 + idx

                        dur_secs, dur_fmt = \
                            _voice_parse_duration(dur_raw)

                        # ── Create call folder ────────────────
                        call_folder = os.path.join(
                            out_path, prefix)
                        os.makedirs(call_folder, exist_ok=True)

                        if is_conf:
                            paths = _voice_generate_conf_call(
                                output_dir      = call_folder,
                                call_id         = call_id_int,
                                liid            = liid,
                                msisdn          = msisdn,
                                target_number   = target,
                                calling_number  = calling,
                                conf_numbers    = conf_nums,
                                timestamp_start = ts,
                                duration_secs   = dur_secs,
                                duration_fmt    = dur_fmt,
                                network_id      = net_id,
                                imei            = imei,
                                imsi            = imsi,
                                cell_id         = cell_id,
                                target_ip       = tgt_ip,
                                access_type     = access,
                                call_direction  = direction,
                                wav_source_path = wav_path,
                                call_name_prefix= prefix,
                            )
                        else:
                            paths = _voice_generate_call(
                                output_dir      = call_folder,
                                call_id         = call_id_int,
                                liid            = liid,
                                msisdn          = msisdn,
                                target_number   = target,
                                calling_number  = calling,
                                called_number   = called,
                                timestamp_start = ts,
                                duration_secs   = dur_secs,
                                duration_fmt    = dur_fmt,
                                network_id      = net_id,
                                imei            = imei,
                                imsi            = imsi,
                                nat_ip          = nat_ip,
                                access_type     = access,
                                call_direction  = direction,
                                release_reason  = rel,
                                wav_source_path = wav_path,
                                call_name_prefix= prefix,
                            )

                        warns = paths.get("warnings", [])
                        st    = ("⚠️ Warn"
                                 if warns else "✅ OK")
                        results.append({
                            "idx":      idx,
                            "type":     "Conference"
                                        if is_conf
                                        else "Normal",
                            "prefix":   prefix,
                            "calling":  calling,
                            "called":   called,
                            "duration": dur_fmt,
                            "wav":      wav_note,
                            "status":   st,
                            "note":     (warns[0][:60]
                                         if warns else
                                         call_folder),
                        })
                        LOG.log("Bulk Voice",
                                f"  ✓ Row {idx}: {prefix}  "
                                f"{'Conf' if is_conf else 'Normal'}  "
                                f"WAV={wav_note}")

                    # Write history
                    ok_count = sum(
                        1 for r in results
                        if "✅" in r["status"])
                    self._write_history(
                        "Voice Generate (Bulk)",
                        ok_count, "local",
                        "✅ OK" if ok_count == len(results)
                        else f"⚠ {ok_count}/{len(results)} OK",
                        out_path)

                except Exception as exc:
                    import traceback
                    error = str(exc)
                    LOG.log("Bulk Voice",
                            f"FAILED: {exc}\n"
                            f"{traceback.format_exc()}",
                            "ERROR")

                def _ui():
                    res_tree.delete(*res_tree.get_children())
                    for r in results:
                        tag = ("ok"   if "✅" in r["status"]
                               else "warn" if "⚠" in r["status"]
                               else "err")
                        res_tree.insert("", "end",
                            values=(r["idx"], r["type"],
                                    r["prefix"],
                                    r["calling"], r["called"],
                                    r["duration"], r["wav"],
                                    r["status"]),
                            tags=(tag,))
                    ok  = sum(1 for r in results
                              if "✅" in r["status"])
                    skp = sum(1 for r in results
                              if "❌" in r["status"])
                    wrn = sum(1 for r in results
                              if "⚠" in r["status"])
                    if error:
                        status_var.set(f"❌  {error}")
                        status_lbl.config(fg=self.C("error"))
                    elif cancel_ev.is_set():
                        status_var.set(
                            f"⛔  Cancelled — "
                            f"{ok} generated before stop.")
                        status_lbl.config(fg=self.C("warn"))
                    else:
                        status_var.set(
                            f"✅  Done — "
                            f"{ok} OK  "
                            f"{wrn} warnings  "
                            f"{skp} skipped  "
                            f"→  {out_path}")
                        status_lbl.config(
                            fg=self.C("success"))
                    prog_var.set(100)
                    gen_btn.config(state="normal")
                    can_btn.config(state="disabled")

                self.after(0, _ui)

            threading.Thread(target=_run, daemon=True).start()

        gen_btn.config(command=_do_csv_generate)
        can_btn.config(command=lambda: cancel_ev.set())

        # ══════════════════════════════════════════════════════════
        # QUICK MODE TAB
        # ══════════════════════════════════════════════════════════
        qc, qsf = self._scrollable(quick_frame)
        qb = tk.Frame(qsf, bg=self.C("bg"))
        qb.pack(fill="x", padx=50, pady=5)

        self._section_label(qb, "⚙️  Common Parameters",
                            pady=(5, 8))
        common_card = self._card(qb)
        cg = tk.Frame(common_card, bg=self.C("panel"))
        cg.pack(fill="x", padx=10, pady=8)

        def _bv_save(*_):
            for key, var in _bv_vars.items():
                self._bulk_voice_state[key] = var.get()
        _bv_vars = {}

        def _qf(parent, label, default, col, row=0,
                width=20, hint="", key=None):
            f = tk.Frame(parent, bg=self.C("panel"))
            f.grid(row=row, column=col,
                   padx=12, pady=4, sticky="w")
            tk.Label(f, text=label,
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9)).pack(anchor="w")
            saved = _bvs.get(key, default) if key else default
            var = tk.StringVar(value=saved)
            tk.Entry(f, textvariable=var,
                     bg=self.C("input_bg"), fg=self.C("text"),
                     insertbackground=self.C("text"),
                     relief="flat", width=width).pack(ipady=5)
            if hint:
                tk.Label(f, text=hint,
                         bg=self.C("panel"), fg=self.C("dim"),
                         font=(_UI_FONT, 7)).pack(anchor="w")
            if key:
                _bv_vars[key] = var
                var.trace_add("write", _bv_save)
            return var

        q_liid    = _qf(cg, "LIID",           "919456622889", 0, 0, key="liid")
        q_msisdn  = _qf(cg, "MSISDN",          "919456622889", 1, 0, key="msisdn")
        q_target  = _qf(cg, "Target Number",   "919456622889", 2, 0, key="target")
        q_calling = _qf(cg, "Calling Number",  "919456622889", 3, 0, key="calling")
        q_called  = _qf(cg, "Called Number",   "916253957648", 4, 0, key="called")

        q_imei    = _qf(cg, "IMEI",            "354076826454420", 0, 1, key="imei")
        q_imsi    = _qf(cg, "IMSI",            "405863169296834", 1, 1, key="imsi")
        q_ts      = _qf(cg, "First Call Start",
                        time.strftime("%d-%m-%Y %H:%M:%S"),
                        2, 1, width=22,
                        hint="DD-MM-YYYY HH:MM:SS", key="ts")
        q_dur     = _qf(cg, "Duration",        "00:01:00",   3, 1,
                        hint="HH:MM:SS — same for all calls", key="dur")

        dir_frame = tk.Frame(cg, bg=self.C("panel"))
        dir_frame.grid(row=1, column=4,
                       padx=12, pady=4, sticky="w")
        tk.Label(dir_frame, text="Call Direction",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(anchor="w")
        q_dir = tk.StringVar(value=_bvs.get("dir", "Outgoing"))
        ttk.Combobox(dir_frame, textvariable=q_dir,
                     values=["Outgoing","Incoming"],
                     state="readonly", width=14).pack(ipady=3)
        _bv_vars["dir"] = q_dir
        q_dir.trace_add("write", _bv_save)

        # Call count + type + prefix
        self._section_label(qb, "📋  Bulk Settings",
                            pady=(16, 8))
        bulk_card = self._card(qb)
        bg2 = tk.Frame(bulk_card, bg=self.C("panel"))
        bg2.pack(fill="x", padx=10, pady=8)

        q_count  = _qf(bg2, "Number of Calls", "5",  0, 0,
                       width=8,
                       hint="Each call gets its own folder", key="count")
        q_prefix = _qf(bg2, "Folder Prefix",
                       "BulkCall", 1, 0, width=16,
                       hint="e.g. BulkCall → BulkCall_001", key="prefix")
        q_gap    = _qf(bg2, "Mins Between Calls",
                       "5", 2, 0, width=6, key="gap",
                       hint="Timestamp gap per call")

        type_frame = tk.Frame(bg2, bg=self.C("panel"))
        type_frame.grid(row=0, column=3,
                        padx=12, pady=4, sticky="w")
        tk.Label(type_frame, text="Call Type",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(anchor="w")
        q_type = tk.StringVar(value="Normal")
        ttk.Combobox(type_frame, textvariable=q_type,
                     values=["Normal","Conference"],
                     state="readonly", width=14).pack(ipady=3)

        # Conference-only fields (shown/hidden)
        conf_outer_q = tk.Frame(bulk_card, bg=self.C("panel"))
        q_conf_nums = tk.StringVar(
            value="Conf:919893586776,07000026454")
        q_cell_id   = tk.StringVar(value="404-93-200233-252")
        q_tgt_ip    = tk.StringVar(
            value="2401:4900:5269:ba7b:4c81:f052:834b:6884")

        for lbl, var, hint in [
            ("Conference Numbers", q_conf_nums,
             "e.g. Conf:num1,num2"),
            ("Cell ID",            q_cell_id,
             "e.g. 404-93-200233-252"),
            ("Target IP",          q_tgt_ip,
             "IPv4 or IPv6"),
        ]:
            er = tk.Frame(conf_outer_q, bg=self.C("panel"))
            er.pack(fill="x", padx=10, pady=3)
            tk.Label(er, text=lbl, width=22, anchor="w",
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9)).pack(side="left")
            tk.Entry(er, textvariable=var,
                     bg=self.C("input_bg"), fg=self.C("text"),
                     insertbackground=self.C("text"),
                     relief="flat", width=40).pack(
                side="left", ipady=4, padx=(0, 8))
            tk.Label(er, text=hint,
                     bg=self.C("panel"), fg=self.C("dim"),
                     font=(_UI_FONT, 7)).pack(side="left")

        def _toggle_conf_q(*_):
            if q_type.get() == "Conference":
                conf_outer_q.pack(fill="x", padx=2,
                                  pady=(0, 6))
            else:
                conf_outer_q.pack_forget()
        q_type.trace_add("write", _toggle_conf_q)

        # WAV folder for quick mode
        self._section_label(qb, "🎵  WAV Files",
                            pady=(16, 8))
        wav_card_q = self._card(qb)
        q_wav_dir  = tk.StringVar()
        q_wav_status = tk.StringVar(value="")

        wr = tk.Frame(wav_card_q, bg=self.C("panel"))
        wr.pack(fill="x", padx=10, pady=6)
        tk.Label(wr, text="WAV Files Folder", width=20,
                 anchor="w", bg=self.C("panel"),
                 fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(side="left")
        tk.Entry(wr, textvariable=q_wav_dir,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"),
                 relief="flat", width=40).pack(
            side="left", ipady=5, padx=(0, 8))

        def _browse_wav_q():
            d = filedialog.askdirectory(
                title="Select WAV Files Folder")
            if d: q_wav_dir.set(d)

        tk.Button(wr, text="Browse…",
                  bg=self.C("primary"), fg="white",
                  relief="flat",
                  font=(_UI_FONT, 9, "bold"),
                  command=_browse_wav_q).pack(
            side="left", ipady=3)

        q_wav_lbl = tk.Label(wav_card_q,
                             textvariable=q_wav_status,
                             bg=self.C("panel"),
                             fg=self.C("dim"),
                             font=(_UI_FONT, 8))
        q_wav_lbl.pack(anchor="w", padx=10, pady=(0, 6))
        tk.Label(wav_card_q,
                 text="  WAV files are assigned to calls "
                      "in order (call 1 → first WAV, "
                      "call 2 → second WAV, etc.). "
                      "If fewer WAVs than calls, "
                      "they cycle around.",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8),
                 wraplength=780).pack(
            anchor="w", padx=10, pady=(0, 8))

        def _on_q_wav_dir(*_):
            d = q_wav_dir.get().strip()
            if not d or not os.path.isdir(d): return
            wavs = sorted([f for f in os.listdir(d)
                           if f.lower().endswith(".wav")])
            if wavs:
                q_wav_status.set(
                    f"  ✅  {len(wavs)} WAV(s): "
                    f"{', '.join(wavs[:4])}"
                    f"{'…' if len(wavs) > 4 else ''}")
                q_wav_lbl.config(fg=self.C("success"))
            else:
                q_wav_status.set(
                    "  ⚠️  No WAVs found — "
                    "silent placeholders will be used")
                q_wav_lbl.config(fg=self.C("warn"))
        q_wav_dir.trace_add("write", _on_q_wav_dir)

        # Output folder for quick mode
        self._section_label(qb, "📂  Output", pady=(16, 8))
        out_card_q = self._card(qb)
        q_out_var  = tk.StringVar(value=_bvs.get("q_out", ""))
        q_out_var.trace_add("write", lambda *_: self._bulk_voice_state.update({"q_out": q_out_var.get()}))
        or2 = tk.Frame(out_card_q, bg=self.C("panel"))
        or2.pack(fill="x", padx=10, pady=6)
        tk.Label(or2, text="Output Folder", width=20,
                 anchor="w", bg=self.C("panel"),
                 fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(side="left")
        tk.Entry(or2, textvariable=q_out_var,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"),
                 relief="flat", width=40).pack(
            side="left", ipady=5, padx=(0, 8))
        tk.Button(or2, text="Browse…",
                  bg=self.C("primary"), fg="white",
                  relief="flat",
                  font=(_UI_FONT, 9, "bold"),
                  command=lambda: q_out_var.set(
                      filedialog.askdirectory())).pack(
            side="left", ipady=3)

        # Quick generate section
        self._section_label(qb, "🚀  Generate",
                            pady=(16, 8))
        qgen_card = self._card(qb)

        q_prog    = tk.DoubleVar(value=0)
        q_status  = tk.StringVar(value="Ready.")
        q_cancel  = threading.Event()

        ttk.Progressbar(qgen_card, variable=q_prog,
                        maximum=100, length=500,
                        mode="determinate").pack(
            fill="x", padx=10, pady=(10, 4))

        q_status_lbl = tk.Label(qgen_card,
                                textvariable=q_status,
                                bg=self.C("panel"),
                                fg=self.C("muted"),
                                font=(_UI_FONT, 9),
                                wraplength=900)
        q_status_lbl.pack(anchor="w", padx=10, pady=(0, 6))

        # Quick results tree (same style)
        qrf = tk.Frame(qgen_card, bg=self.C("panel"))
        qrf.pack(fill="x", padx=10, pady=(0, 6))
        q_tree = ttk.Treeview(qrf, columns=tcols,
                               show="headings", height=8,
                               style="BV.Treeview")
        for col, w in [("#", 40), ("Type", 80),
                       ("Prefix", 160), ("Calling", 130),
                       ("Called", 130), ("Duration", 80),
                       ("WAV", 120), ("Status", 120)]:
            q_tree.heading(col, text=col)
            q_tree.column(col, width=w, anchor="w")
        qvsb = ttk.Scrollbar(qrf, orient="vertical",
                             command=q_tree.yview)
        q_tree.configure(yscrollcommand=qvsb.set)
        q_tree.pack(side="left", fill="x", expand=True)
        qvsb.pack(side="right", fill="y")
        q_tree.tag_configure("ok",   foreground="#22c55e")
        q_tree.tag_configure("err",  foreground="#f87171")
        q_tree.tag_configure("warn", foreground="#f0a84a")

        qbtn_row = tk.Frame(qgen_card, bg=self.C("panel"))
        qbtn_row.pack(anchor="w", padx=10, pady=(4, 12))

        def open_q_output():
            p = q_out_var.get().strip()
            if p and os.path.isdir(p):
                webbrowser.open(
                    f"file:///{p.replace(os.sep, '/')}")

        tk.Button(qbtn_row, text="📁  Open Output Folder",
                  bg="#374151", fg="white", relief="flat",
                  font=(_UI_FONT, 9, "bold"),
                  command=open_q_output).pack(
            side="left", padx=(0, 10), ipady=4, ipadx=8)

        q_gen_btn = tk.Button(
            qbtn_row, text="🚀  Generate Calls",
            bg=self.C("success"), fg="white",
            relief="flat",
            font=(_UI_FONT, 11, "bold"),
            activebackground="#059669")
        q_gen_btn.pack(side="left", ipady=6,
                       ipadx=16, padx=(0, 8))

        q_can_btn = tk.Button(
            qbtn_row, text="⛔  Cancel",
            bg="#374151", fg="white",
            relief="flat",
            font=(_UI_FONT, 10, "bold"),
            state="disabled")
        q_can_btn.pack(side="left", ipady=6, ipadx=12)

        def _do_quick_generate():
            out_path = q_out_var.get().strip()
            wav_dir  = q_wav_dir.get().strip()
            if not out_path:
                return self.popup("Error",
                                  "Please select an output folder.",
                                  "error")
            try:
                count = int(q_count.get().strip())
                if count < 1: raise ValueError
            except ValueError:
                return self.popup("Error",
                                  "Number of calls must be a "
                                  "positive integer.", "error")

            q_cancel.clear()
            q_gen_btn.config(state="disabled")
            q_can_btn.config(state="normal")
            q_prog.set(0)
            q_tree.delete(*q_tree.get_children())
            q_status.set("Generating…")
            q_status_lbl.config(fg=self.C("muted"))

            # Collect WAVs
            wav_list = []
            if wav_dir and os.path.isdir(wav_dir):
                wav_list = sorted([
                    os.path.join(wav_dir, fn)
                    for fn in os.listdir(wav_dir)
                    if fn.lower().endswith(".wav")])

            # Parse gap
            try:
                gap_mins = int(q_gap.get().strip())
            except ValueError:
                gap_mins = 5

            def _qrun():
                from datetime import datetime, timedelta
                results = []
                is_conf       = q_type.get() == "Conference"
                prefix_base   = q_prefix.get().strip() or "BulkCall"
                dur_raw       = q_dur.get().strip() or "00:01:00"
                dur_secs, dur_fmt = _voice_parse_duration(dur_raw)

                # Parse first call start time
                ts_str = q_ts.get().strip()
                try:
                    ts_dt = datetime.strptime(ts_str, "%d-%m-%Y %H:%M:%S")
                except ValueError:
                    ts_dt = datetime.now()

                os.makedirs(out_path, exist_ok=True)

                for i in range(count):
                    if q_cancel.is_set():
                        break

                    pct = int(i * 100 / count)
                    self.after(0, lambda p=pct, n=i+1, t=count:
                               (q_prog.set(p),
                                q_status.set(
                                    f"Generating call "
                                    f"{n} / {t}…")))

                    call_id = _next_call_id()
                    prefix  = f"{prefix_base}_{i+1:03d}"
                    ts_call = (ts_dt + timedelta(
                        minutes=gap_mins * i)
                               ).strftime("%d-%m-%Y %H:%M:%S")

                    # Pick WAV
                    wav_path = ""
                    wav_note = "silent"
                    if wav_list:
                        wp = wav_list[i % len(wav_list)]
                        wav_path = wp
                        wav_note = os.path.basename(wp)

                    call_folder = os.path.join(out_path, prefix)
                    os.makedirs(call_folder, exist_ok=True)

                    try:
                        if is_conf:
                            paths = _voice_generate_conf_call(
                                output_dir      = call_folder,
                                call_id         = call_id,
                                liid            = q_liid.get(),
                                msisdn          = q_msisdn.get(),
                                target_number   = q_target.get(),
                                calling_number  = q_calling.get(),
                                conf_numbers    = q_conf_nums.get(),
                                timestamp_start = ts_call,
                                duration_secs   = dur_secs,
                                duration_fmt    = dur_fmt,
                                network_id      = "2405:0200:0381:1606:00:00:00:00",
                                imei            = q_imei.get(),
                                imsi            = q_imsi.get(),
                                cell_id         = q_cell_id.get(),
                                target_ip       = q_tgt_ip.get(),
                                access_type     = "EUTRAN",
                                call_direction  = q_dir.get(),
                                wav_source_path = wav_path,
                                call_name_prefix= prefix,
                            )
                        else:
                            paths = _voice_generate_call(
                                output_dir      = call_folder,
                                call_id         = call_id,
                                liid            = q_liid.get(),
                                msisdn          = q_msisdn.get(),
                                target_number   = q_target.get(),
                                calling_number  = q_calling.get(),
                                called_number   = q_called.get(),
                                timestamp_start = ts_call,
                                duration_secs   = dur_secs,
                                duration_fmt    = dur_fmt,
                                network_id      = "2405:0200:0381:1606:00:00:00:00",
                                imei            = q_imei.get(),
                                imsi            = q_imsi.get(),
                                nat_ip          = "182.75.187.91",
                                access_type     = "EUTRAN",
                                call_direction  = q_dir.get(),
                                release_reason  = "Normal call clearing",
                                wav_source_path = wav_path,
                                call_name_prefix= prefix,
                            )
                        warns = paths.get("warnings", [])
                        st    = "⚠️ Warn" if warns else "✅ OK"
                        LOG.log("Bulk Voice",
                                f"  ✓ Quick {i+1}/{count}: "
                                f"{prefix}  WAV={wav_note}")
                    except Exception as exc:
                        st    = "❌ Fail"
                        warns = [str(exc)]
                        LOG.log("Bulk Voice",
                                f"  ✗ Quick {i+1}: {exc}",
                                "ERROR")

                    results.append({
                        "idx":      i + 1,
                        "type":     "Conference"
                                    if is_conf else "Normal",
                        "prefix":   prefix,
                        "calling":  q_calling.get(),
                        "called":   (q_called.get()
                                     if not is_conf else "CONF"),
                        "duration": dur_fmt,
                        "wav":      wav_note,
                        "status":   st,
                    })

                ok  = sum(1 for r in results
                          if "✅" in r["status"])
                self._write_history(
                    "Voice Generate (Bulk Quick)",
                    ok, "local",
                    "✅ OK" if ok == len(results)
                    else f"⚠ {ok}/{len(results)} OK",
                    out_path)

                def _qui():
                    q_tree.delete(*q_tree.get_children())
                    for r in results:
                        tag = ("ok"   if "✅" in r["status"]
                               else "warn" if "⚠" in r["status"]
                               else "err")
                        q_tree.insert("", "end",
                            values=(r["idx"], r["type"],
                                    r["prefix"],
                                    r["calling"], r["called"],
                                    r["duration"], r["wav"],
                                    r["status"]),
                            tags=(tag,))
                    ok2 = sum(1 for r in results
                              if "✅" in r["status"])
                    if q_cancel.is_set():
                        q_status.set(
                            f"⛔  Cancelled — "
                            f"{ok2} generated.")
                        q_status_lbl.config(
                            fg=self.C("warn"))
                    else:
                        q_status.set(
                            f"✅  {ok2} call(s) generated "
                            f"→ {out_path}")
                        q_status_lbl.config(
                            fg=self.C("success"))
                    q_prog.set(100)
                    q_gen_btn.config(state="normal")
                    q_can_btn.config(state="disabled")

                self.after(0, _qui)

            threading.Thread(target=_qrun, daemon=True).start()

        q_gen_btn.config(command=_do_quick_generate)
        q_can_btn.config(command=lambda: q_cancel.set())

        # Start on CSV tab
        _switch_tab("csv")

    # ──────────────────────────────────────────────────────────────
    # PCAP GENERATOR
    # ──────────────────────────────────────────────────────────────
    # ──────────────────────────────────────────────────────────────
    # PCAP GENERATOR
    # ──────────────────────────────────────────────────────────────
    def show_help(self):
        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Help page")

        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(20, 5))
        tk.Label(hdr, text="❓  Help  &  Documentation",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")
        _logs_btn_row = tk.Frame(hdr, bg=self.C("bg"))
        _logs_btn_row.pack(side="right")
        _badge_help = tk.Label(_logs_btn_row, text="",
                               bg="#ef4444", fg="white",
                               font=(_UI_FONT, 7, "bold"))
        self._log_badge_labels.append(_badge_help)
        self._update_log_badge()
        tk.Button(_logs_btn_row, text="📋  View Logs",
                  bg=self.C("primary"), fg="white", relief="flat",
                  font=(_UI_FONT, 9, "bold"), cursor="hand2",
                  activebackground=self.C("border"),
                  command=self.show_logs).pack(side="left", ipady=5, ipadx=12)
        tk.Frame(main, bg=self.C("border"), height=1).pack(
            fill="x", padx=50, pady=(5, 15))

        canvas, sf = self._scrollable(main)
        body = tk.Frame(sf, bg=self.C("bg"))
        body.pack(fill="x", padx=50, pady=5)

        # ══════════════════════════════════════════════════════════
        # ABOUT CARD
        # ══════════════════════════════════════════════════════════
        about_card = self._card(body)
        about_card.config(highlightbackground=self.C("success"),
                          highlightthickness=2)

        title_row = tk.Frame(about_card, bg=self.C("panel"))
        title_row.pack(fill="x", padx=18, pady=(14, 12))

        # Logo
        self._logo_widget(title_row, 52, self.C("panel")).pack(side="left", padx=(0, 18))

        info_col = tk.Frame(title_row, bg=self.C("panel"))
        info_col.pack(side="left")

        # App title
        tk.Label(info_col, text="ComTrail  Data Upload & Generate Utility",
                 bg=self.C("panel"), fg=self.C("card_title"),
                 font=(_UI_FONT, 17, "bold")).pack(anchor="w")

        # Version
        tk.Label(info_col, text="Version  3.0",
                 bg=self.C("panel"), fg=self.C("success"),
                 font=(_UI_FONT, 13, "bold")).pack(anchor="w", pady=(4, 0))

        # Divider
        tk.Frame(info_col, bg=self.C("border"), height=1).pack(
            fill="x", pady=(8, 6))

        # Product — larger, prominent
        prod_row = tk.Frame(info_col, bg=self.C("panel"))
        prod_row.pack(anchor="w")
        tk.Label(prod_row, text="Product: ",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 11)).pack(side="left")
        tk.Label(prod_row, text="ClearTrail Technologies",
                 bg=self.C("panel"), fg=self.C("card_title"),
                 font=(_UI_FONT, 11, "bold")).pack(side="left")

        # Author — same size as product
        auth_row = tk.Frame(info_col, bg=self.C("panel"))
        auth_row.pack(anchor="w", pady=(4, 0))
        tk.Label(auth_row, text="Author:  ",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 11)).pack(side="left")
        tk.Label(auth_row, text="Mohit Tambe",
                 bg=self.C("panel"), fg=self.C("card_title"),
                 font=(_UI_FONT, 11, "bold")).pack(side="left")


        # ══════════════════════════════════════════════════════════
        # WHAT'S NEW IN V3.0
        # ══════════════════════════════════════════════════════════
        self._section_label(body, "🆕  What's New in v3.0", pady=(20, 8))

        whats_new_features = [
            ("🆕", "What's New in v3.0  ← Latest",
             "v3.0 is a major release with new generators, smart upload scheduling, "
             "multi-folder HI3 upload, Kafka validation improvements, mandatory field "
             "validation, UX improvements, and production-grade bug fixes.\n\n"
             "🕐  Scheduled Upload\n"
             "  • Schedule PCAP, LUDR/SMS, or Voice uploads to run at set times or intervals.\n"
             "  • Jobs fire automatically without the user being logged in (no manual Connect needed).\n"
             "  • Dedicated SSH connection per job — multiple jobs run concurrently safely.\n"
             "  • Daily reset ensures jobs repeat correctly every day even if the app stays open.\n"
             "  • Retry on failure: configurable retry count and delay.\n"
             "  • Job history (last 10 runs) viewable per job.\n"
             "  • Stop All, Run Now, Pause/Resume, and Delete controls.\n\n"
             "📡  HI3 Only Upload — Multi-Folder\n"
             "  • Select and upload multiple HI3 folders in one operation.\n"
             "  • Each folder is copied as /etc/vsf/input/<folder-name>/ on the Voice server.\n"
             "  • Server selector removed — always uses Voice server from Settings.\n"
             "  • Per-folder and per-file progress shown in the upload log.\n\n"
             "📊  Kafka Validation Dashboard — Fixes\n"
             "  • Date filter now works correctly when both Start and End date are selected.\n"
             "  • Search box works for IP topic messages.\n"
             "  • Status bar shows the correct active topic name after fetch.\n"
             "  • Records with missing timestamps are never incorrectly filtered out.\n\n"
             "📊  Session Summary Strip\n"
             "  • Home page shows live counts: PCAP · LUDR/SMS · Voice · Generated · Failures.\n"
             "  • 🌐 Open ComTrail Web UI button lives inside this strip.\n\n"
             "🔍  Upload History Search & Re-run\n"
             "  • Search bar filters recent history as you type.\n"
             "  • ↩ Re-run button on each history row to re-launch that upload.\n\n"
             "⚡  Upload Speed & ETA\n"
             "  • PCAP and LUDR progress shows real-time speed (MB/s) and ETA.\n\n"
             "📋  Copy IP in Settings\n"
             "  • 📋 icon next to each server IP field — click to copy with a toast.\n\n"
             "⚠️  Nav Warning During Upload\n"
             "  • Navigating away mid-upload shows a confirmation dialog.\n\n"
             "✅  Mandatory Field Validation\n"
             "  • SMS & LBS: Target Number required, digits only.\n"
             "  • Voice: Target Number, Calling Number, Called Number, Call Start — required; "
             "number fields accept digits only. Border is red when empty, green when filled.\n\n"
             "📋  Collapsible Sections\n"
             "  • SMS Generator and Voice Generator sections collapse/expand on click.\n\n"
             "🎨  Crystal Teal Theme & Hover Effects\n"
             "  • Full colour scheme: blue → crystal teal across every page.\n"
             "  • All buttons highlight on hover; hand cursor everywhere.\n\n"
             "💬  SMS IRI Generator\n"
             "  • LIID auto-derived from Target Number.\n"
             "  • 📤 Upload to Server button added.\n\n"
             "📞  Voice & Bulk Voice Generator\n"
             "  • IMEI, IMSI, Network ID, NAT IP all auto-generated.\n"
             "  • Duration read from WAV header automatically.\n\n"
             "📊  Kafka & Solr Dashboards\n"
             "  • Kafka: search match highlighting, match count, ticking 'last fetched'.\n"
             "  • Solr: ticking 'last fetched' timestamp.\n\n"
             "🎯  Target Details\n"
             "  • Live search, sortable columns, CSV export, copy row.\n\n"
             "⚙️  Settings\n"
             "  • Export/Import Config includes Kafka topic names.\n"
             "  • Connected and valid path labels shown in green."),

            ("📤", "PCAP, LBS & Voice Upload",
             "Upload PCAP folders, LBS/LUDR/SMS files, and Voice call data "
             "(Hi2 + Hi3) to ComTrail servers via secure SSH/SFTP.\n"
             "Voice upload enforces the required sequence automatically:\n"
             "  ① Hi3 files → /etc/vsf/input/HI3/\n"
             "  ② 1-second wait\n"
             "  ③ Hi2 Begin file(s) → WatchDir\n"
             "  ④ Hi2 End file → WatchDir\n\n"
             "HI3 Only Upload:\n"
             "  • Add one or more HI3 folders using the ➕ Add Folder button.\n"
             "  • Each folder is copied as /etc/vsf/input/<folder-name>/ on the Voice server.\n"
             "  • Upload runs over a dedicated SSH connection — no manual Connect required."),

            ("💬", "SMS IRI Generator",
             "Generates PDU-encoded IRI intercept files (.txt) from a CSV. "
             "Supports UTF-16 encoding for Arabic, Hindi, Bengali, and all other languages. "
             "LIID is set automatically to the Target Number. "
             "Call ID auto-increments per row. "
             "Long messages are split into multi-part files automatically. "
             "Generated files can be uploaded to the server directly from the page."),

            ("📍", "LBS / LUDR Generator",
             "Generates CDR location files for LBS and LUDR intercepts. "
             "Use the built-in interactive map to pick GPS coordinates — "
             "search any city, click to drop a pin, export as CSV. "
             "Supports BEGIN, CONTINUE, and END event types."),

            ("📞", "Voice Call Generator — Normal & Conference",
             "Generates complete Hi2 and Hi3 folder structures for a single voice call. "
             "Enter Target Number, Calling Number, Called Number, Call ID, Call Start, "
             "and Call Direction — everything else is handled automatically. "
             "Duration is read from the WAV file. "
             "Supports Normal calls (Begin + End) and Conference calls "
             "(Begin + Continue + End with participant numbers, Cell ID, and Target IP)."),

            ("📞", "Bulk Voice Call Generator",
             "Generates multiple voice call folders in a single operation.\n"
             "  📋 CSV Mode — one row per call; supports mixed Normal and Conference calls.\n"
             "  ⚡ Quick Mode — fill one form, set a count, generate N calls with "
             "auto-incrementing Call IDs and configurable time spacing between calls.\n"
             "WAV files are drawn from a folder and cycled across calls. "
             "A live progress bar and Cancel option are included."),

            ("🎯", "Target Details — View Intercept Targets",
             "Connects to the ComTrail server and lists active intercept targets "
             "from the Voice and IP query files.\n"
             "  • Switch between Voice and IP paths using the toggle at the top.\n"
             "  • Search entries live by Filter ID, Mobile Number, or Target Name.\n"
             "  • Click any column heading to sort the table.\n"
             "  • Copy a row or just the mobile number from the right-click menu.\n"
             "  • Export the visible table to CSV with one click."),

            ("📊", "Kafka Validation Dashboard",
             "Reads enriched intercept messages directly from Kafka topics "
             "and presents them in a searchable, filterable table.\n"
             "  • Filter by type: All, SMS, LBS, Voice, IP.\n"
             "  • Filter by date range using the From and To calendar pickers.\n"
             "  • Search across all fields.\n"
             "  • Timestamps display in IST.\n"
             "  • Export visible results to CSV.\n"
             "  • Auto-refresh mode available for live monitoring."),

            ("🔍", "Solr Dashboard",
             "Queries the Solr search index to verify that uploaded records "
             "have been indexed correctly. "
             "Select a collection, enter a search query, and browse the results. "
             "Useful for confirming that SMS, Voice, and LBS data is "
             "visible and searchable in the system."),

            ("🖥️", "Server Log Monitor",
             "Monitors live log files from up to 15 ComTrail backend services "
             "over SSH, directly within the Logs page.\n"
             "  • Tick the checkbox next to a service to enable monitoring.\n"
             "  • Click a service to view its latest log file in the terminal viewer.\n"
             "  • Auto-refresh every 5 seconds; keyword search; save to file.\n"
             "  • Add, edit, or remove services from the panel at any time."),

            ("📋", "Upload & Generate History",
             "Every upload and generate action is recorded automatically.\n"
             "  • The Upload Data page shows the last 10 upload actions inline.\n"
             "  • The Generate Data page shows the last 10 generate actions inline.\n"
             "  • Click  📊 View Full History  for the complete log with filtering, "
             "search, and CSV export."),

            ("🎨", "Light & Dark Theme",
             "Switch between Dark and Light themes from the Help page → Appearance section. "
             "The change takes effect immediately across every page and table."),

            ("⚙️", "Settings — Server Configuration",
             "Configure the three server connections (PCAP, LBS/LUDR/SMS, Voice) "
             "from the Settings page.\n"
             "  • After saving, the utility tests the SSH connection and reports "
             "✅ Connected (green) or ❌ Failed within 4 seconds.\n"
             "  • Remote Path fields include a dropdown of known valid server paths "
             "(shown in green when a known path is selected).\n"
             "  • Kafka section includes Voice Topic Name and IP Topic Name fields.\n"
             "  • Export all settings to CSV for backup (includes Kafka topic names); "
             "import on a new machine."),

            ("🌐", "Open ComTrail Web UI",
             "A  🌐 Open ComTrail Web UI  button is available on both the Home page "
             "and the Upload Data page.\n"
             "  • Click it to open the ComTrail browser interface directly.\n"
             "  • The URL is set in Settings → ComTrail Web UI.\n"
             "  • If no URL is configured, a reminder popup appears directing you to Settings."),
        ]

        # Build as accordion card — same style as guide topics
        wn_acc = tk.Frame(body, bg=self.C("panel"),
                          highlightbackground=self.C("border"),
                          highlightthickness=1)
        wn_acc.pack(fill="x", pady=4)

        wn_open    = [False]
        wn_content = tk.Frame(wn_acc, bg=self.C("panel"))
        wn_arrow   = tk.StringVar(value="▶")

        wn_header = tk.Frame(wn_acc, bg=self.C("panel"), cursor="hand2")
        wn_header.pack(fill="x")
        tk.Label(wn_header, text="🆕", bg=self.C("panel"),
                 font=(_UI_FONT, 14), width=3).pack(
                 side="left", padx=(10, 0), pady=10)
        tk.Label(wn_header, text="Release Notes — v3.0",
                 bg=self.C("panel"), fg=self.C("card_title"),
                 font=(_UI_FONT, 11, "bold")).pack(
                 side="left", padx=8, pady=10)
        wn_arrow_lbl = tk.Label(wn_header, textvariable=wn_arrow,
                                bg=self.C("panel"), fg=self.C("success"),
                                font=(_UI_FONT, 11, "bold"))
        wn_arrow_lbl.pack(side="right", padx=14)

        for i, (icon, title, desc) in enumerate(whats_new_features):
            if i > 0:
                tk.Frame(wn_content, bg=self.C("border"), height=1).pack(
                    fill="x", padx=16)
            row = tk.Frame(wn_content, bg=self.C("panel"))
            row.pack(fill="x", padx=16, pady=(8, 6))
            badge = tk.Label(row, text=icon, bg=self.C("panel"),
                             font=(_UI_FONT, 14), width=3)
            badge.pack(side="left", padx=(0, 10))
            col = tk.Frame(row, bg=self.C("panel"))
            col.pack(side="left", fill="x", expand=True)
            tk.Label(col, text=title, bg=self.C("panel"),
                     fg=self.C("card_title"),
                     font=(_UI_FONT, 10, "bold"),
                     anchor="w").pack(anchor="w")
            tk.Label(col, text=desc, bg=self.C("panel"),
                     fg=self.C("subtle"),
                     font=(_UI_FONT, 9),
                     wraplength=820, justify="left",
                     anchor="w").pack(anchor="w", pady=(2, 0))

        tk.Frame(wn_content, bg=self.C("panel"), height=8).pack()

        def _wn_toggle(event=None):
            if wn_open[0]:
                wn_content.pack_forget()
                wn_arrow.set("▶")
                wn_open[0] = False
            else:
                wn_content.pack(fill="x")
                wn_arrow.set("▼")
                wn_open[0] = True

        wn_header.bind("<Button-1>", _wn_toggle)
        for child in wn_header.winfo_children():
            child.bind("<Button-1>", _wn_toggle)

        # ══════════════════════════════════════════════════════════
        # ACCORDION TOPICS — Detailed step-by-step guides
        # ══════════════════════════════════════════════════════════
        self._section_label(body, "📖  Step-by-Step Guides", pady=(22, 8))

        TOPICS = [
            # ── 1. GETTING STARTED ──────────────────────────────
            ("🚀", "Getting Started — First-Time Setup", [
                ("Step 1 — Open Settings",
                 "Click  ⚙️ Settings  in the top navigation bar at the top of the screen. "
                 "This is where you configure the three server connections the utility needs."),
                ("Step 2 — Configure the PCAP Server",
                 "In the PCAP Server card, enter:\n"
                 "  • IP Address — the IP of the server that receives PCAP files "
                 "(e.g. 192.168.1.100)\n"
                 "  • Password — the root password for SSH access\n"
                 "  • Remote Path — the folder on the server where PCAPs go "
                 "(default is pre-filled, leave as-is unless told otherwise)"),
                ("Step 3 — Configure the LBS / LUDR / SMS Server",
                 "In the LBS/LUDR/SMS card, enter the IP address and password "
                 "for the server that handles Location, LUDR, and SMS data. "
                 "The remote path is pre-filled — do not change it unless instructed."),
                ("Step 4 — Configure the Voice Server",
                 "In the Voice card, enter the IP address and root password "
                 "for the server that receives Hi2 and Hi3 voice call files. "
                 "This is the same server that has /etc/vsf/input and /data5/prism paths."),
                ("Step 5 — Save All Settings",
                 "Click the  💾 Save All Settings  button at the top of the Settings page. "
                 "A connecting popup will appear. After 4 seconds it will show the result:\n"
                 "  ✅ Connected — server is reachable and login was successful\n"
                 "  ❌ Failed — wrong IP or password, server unreachable\n"
                 "The coloured dot on each card also updates automatically every 2 seconds."),
                ("Step 6 — Verify connections (optional)",
                 "Each server card shows a small coloured dot next to the server name:\n"
                 "  🟢 Teal = Connected and ready\n"
                 "  🔴 Red = Cannot connect — check IP and password\n"
                 "  ⚫ Grey = Not configured yet\n"
                 "The  Connected  label also appears in green for clear confirmation. "
                 "If any server shows Red, double-check the IP address and password, "
                 "then click Save again."),
                ("Step 7 — You are ready",
                 "Once all dots are Teal / Connected, you can start uploading and generating data. "
                 "Navigate using the buttons at the top: "
                 "Home (Upload), Generate Data, Sample Data, Targets, Logs, Help.\n\n"
                 "Tip: Use the  🌐 Open ComTrail Web UI  button on the Home page "
                 "to open the browser interface at any time."),
            ]),

            # ── 2. UPLOADING PCAP DATA ──────────────────────────
            ("📡", "Uploading PCAP Data", [
                ("What is PCAP data?",
                 "PCAP (Packet Capture) files contain recorded network traffic. "
                 "They are .pcap files typically generated by tools like Wireshark or tcpdump. "
                 "You upload them to the ComTrail server so it can analyse the network data."),
                ("Step 1 — Go to Upload Data",
                 "Click  🏠 Home  in the top navigation bar. "
                 "You will see three upload cards: PCAP Data, LBS/LUDR/SMS Data, and Voice Call Data."),
                ("Step 2 — Select your PCAP folder",
                 "Click anywhere on the  📡 PCAP Data  card. "
                 "A folder browser will open. Navigate to and select the folder "
                 "that contains your .pcap files. "
                 "The utility will recursively find and upload every file inside that folder, "
                 "including files in sub-folders, preserving the folder structure on the server."),
                ("Step 3 — Wait for completion",
                 "A progress window will appear showing the upload is in progress. "
                 "Do not close the app during upload. "
                 "When finished, a success popup tells you how many files were transferred."),
                ("Step 4 — Check the result",
                 "Click  📋 Logs  in the top nav to see every individual file that was uploaded, "
                 "with the exact remote path and file size confirmed on the server."),

            ]),

            # ── 3. UPLOADING LBS / LUDR / SMS DATA ─────────────
            ("📍", "Uploading LBS / LUDR / SMS Data", [
                ("What is LBS/LUDR/SMS data?",
                 "LBS (Location Based Services) files contain GPS or cell-tower coordinates. "
                 "LUDR files contain usage data records. "
                 "SMS files contain SMS intercept records. "
                 "All three are CSV or TXT files that you upload to the same server path."),
                ("Step 1 — Go to Upload Data",
                 "Click  🏠 Home  in the top navigation bar."),
                ("Step 2 — Select your files",
                 "Click anywhere on the  📍 LBS / LUDR / SMS Data  card. "
                 "A file picker opens — you can select multiple CSV or TXT files at once "
                 "by holding Ctrl and clicking each file."),
                ("Step 3 — Upload completes",
                 "All selected files are uploaded directly to the configured remote path. "
                 "A popup confirms how many files were sent."),
            ]),

            # ── 4. UPLOADING VOICE CALL DATA ────────────────────
            ("📞", "Uploading Voice Call Data", [
                ("What is Voice Call data?",
                 "Voice call interception produces two sets of files:\n"
                 "  Hi2/ folder — contains Begin and End .txt files (call signalling)\n"
                 "  Hi3/ folder — contains WAV audio files and a CRI .txt file\n"
                 "Both folders must be inside the same parent folder."),
                ("Step 1 — Prepare your folder structure",
                 "Your folder should look like this:\n"
                 "  MyCall/\n"
                 "    Hi2/\n"
                 "      Z_20260416.100000000.919456622889_1_I_916253957648_Begin.txt\n"
                 "      Z_20260416.100000000.919456622889_1_I_916253957648_End.txt\n"
                 "    Hi3/\n"
                 "      BanglaCall_a.wav\n"
                 "      BanglaCall_b.wav\n"
                 "      uae_cs_2026-04-16_100000_call_data_record.txt\n"
                 "You can generate this structure using the Voice Call Generator (see below)."),
                ("Step 2 — Click the Voice Call Data card",
                 "On the Home page, click the  📞 Voice Call Data  card. "
                 "A folder browser opens — select the folder that directly contains "
                 "the Hi2/ and Hi3/ sub-folders (e.g. select  MyCall/ )."),
                ("Step 3 — Upload sequence (automatic)",
                 "The utility uploads in the exact required order:\n"
                 "  ① cp -r HI3 /etc/vsf/input/  — copies the entire Hi3 folder\n"
                 "  ② Waits 1 second\n"
                 "  ③ Copies HI2/Begin file → WatchDir\n"
                 "  ④ Copies HI2/End file → WatchDir\n"
                 "This sequence is mandatory — changing it causes H2_MERGE_TIMEOUT errors."),

            ]),

            # ── 5. SCHEDULED UPLOAD ──────────────────────────────
            ("🕐", "Scheduled Upload — Automating Uploads", [
                ("What is Scheduled Upload?",
                 "The Scheduled Upload page lets you create jobs that run automatically "
                 "at a specific time each day (or on a repeat interval), without needing "
                 "to manually connect to the server first. Jobs fire correctly even if the "
                 "app is left running overnight."),
                ("Step 1 — Open the Schedule page",
                 "Click  🏠 Home  →  Schedule Upload  (or  🕐 Schedule  in the top nav)."),
                ("Step 2 — Create a new job",
                 "Click  ➕ New Job. Fill in:\n"
                 "  • Job Name — a label to identify this job\n"
                 "  • Type — PCAP, LUDR / LBS, SMS, or Voice\n"
                 "  • File / Folder — click Browse to pick the folder or files\n"
                 "  • Run Time(s) — the time(s) to fire each day (HH:MM, 24-hour)\n"
                 "  • Days — tick which days of the week it should run\n"
                 "  • Repeat — untick if the job should run only once\n"
                 "  • Retry — set max retry count and delay if the upload fails"),
                ("Step 3 — Save and activate",
                 "Click  💾 Save Job. The job appears in the list with status  Waiting. "
                 "It will fire automatically at the configured time. "
                 "No manual Connect is required — the scheduler uses credentials from Settings."),
                ("Run a job immediately",
                 "Click  ▶ Run Now  on any job to trigger it instantly, "
                 "regardless of the scheduled time. The status changes to  Running  "
                 "and back to  Scheduled  (or  Done  if non-repeating) when finished."),
                ("Pause or resume a job",
                 "Click  ⏸ Pause  to disable a job without deleting it. "
                 "Click  ▶ Resume  to re-enable it. "
                 "Running jobs cannot be paused — wait for the upload to finish first."),
                ("Stop all jobs",
                 "Click  ⏹ Stop All  to disable all non-running jobs at once. "
                 "Jobs already uploading will finish — they cannot be interrupted mid-upload."),
                ("Job history",
                 "Click any job row to expand it and see the last 10 run results "
                 "with timestamps and durations."),
                ("Interval mode",
                 "Toggle  Interval Mode  to run the job every N hours instead of at a fixed time. "
                 "Useful for uploads that need to repeat throughout the day."),
            ]),

            # ── 6. SMS IRI GENERATOR ─────────────────────────────
            ("💬", "Generating SMS IRI Files", [
                ("What does this generate?",
                 "The SMS IRI Generator creates PDU-encoded IRI (Intercept Related Information) "
                 "files in .txt format. Each file represents one SMS message captured "
                 "for lawful interception. These files are then uploaded to the LBS server."),
                ("Step 1 — Open the generator",
                 "Click  🏠 Home  →  Generate Data  →  SMS IRI Generator."),
                ("Step 2 — Download the sample CSV template",
                 "Click  ⬇️ Download Sample CSV  and save the file. "
                 "Open it in Excel or Notepad to see the column format. "
                 "Required columns:  Test-TextData  (the SMS message text) "
                 "and  SenderName  (the sender's phone number, e.g. 919456622889). "
                 "Optional columns: Direction (incoming/outgoing), IMEI, IMSI, "
                 "Timestamp (DD-MM-YYYY HH:MM:SS format), MCC, MNC, LAC, CI."),
                ("Step 3 — Prepare your CSV file",
                 "Fill in one row per SMS message. "
                 "Each row will produce one .txt IRI file (or multiple files if the "
                 "SMS text is very long — long messages are split into parts automatically). "
                 "Save the file as .csv (UTF-8 encoding recommended)."),
                ("Step 4 — Select input CSV",
                 "In the generator, click  Browse…  next to  Input CSV File  "
                 "and select your prepared CSV file."),
                ("Step 5 — Select output folder",
                 "Click  Browse…  next to  Output Folder  and choose where the "
                 "generated .txt IRI files should be saved on your computer."),
                ("Step 6 — Enter interception parameters",
                 "Fill in these fields (they apply to every row in the CSV):\n"
                 "  • Target Number — the mobile number being intercepted\n"
                 "    LIID and MSISDN are automatically set to this value\n"
                 "  • Start Call ID — the first Call ID number (e.g. 1 or 13092840). "
                 "It auto-increments by 1 for each row."),
                ("Step 7 — Generate",
                 "Click  🚀 Generate IRI Files. "
                 "The results table shows each file with its Call ID, part count, "
                 "sender name, direction, and a preview of the message text. "
                 "Click  📁 Open Output Folder  to see the files."),
                ("Step 8 — Upload the generated files",
                 "Option A (quick): Click  📤 Upload to Server  directly on this page. "
                 "It uploads all .txt files from the output folder to the configured "
                 "LBS/LUDR/SMS server path automatically.\n\n"
                 "Option B: Go to  🏠 Home  →  Upload Data  →  click LBS/LUDR/SMS card "
                 "→ select the generated .txt files → upload."),
            ]),

            # ── 6. LBS / LUDR GENERATOR ─────────────────────────
            ("📍", "Generating LBS / LUDR CDR Files", [
                ("What does this generate?",
                 "The LBS/LUDR Generator creates CDR (Call Detail Record) files "
                 "in .txt format containing GPS location data for a mobile number. "
                 "Each file represents one location event (BEGIN, CONTINUE, or END)."),
                ("Step 1 — Open the generator",
                 "Click  🏠 Home  →  Generate Data  →  LBS / LUDR Generator."),
                ("Step 2 — Choose your method: Map Picker or CSV",
                 "Method A (easiest): Use the Map Coordinate Picker — scroll down to "
                 "🗺️ Map Coordinate Picker and click  🗺️ Open Interactive Map.\n"
                 "Method B: Prepare a CSV manually (see Step 3 below)."),
                ("Step 3 — Using the Interactive Map",
                 "The map opens in your browser:\n"
                 "  1. Select the Event type: BEGIN for the first point, "
                 "CONTINUE for middle points, END for the last point\n"
                 "  2. Type a city or address in the search bar (e.g. 'Indore, India')\n"
                 "  3. Click on the map anywhere to drop a pin — the coordinates are "
                 "captured instantly in the table\n"
                 "  4. Right-click any pin to remove it if placed incorrectly\n"
                 "  5. Click  ⬅ Remove Last Pin  to undo the last point\n"
                 "  6. Click  ✓ Done  when finished\n"
                 "  7. Back in the app, click  💾 Export Coords as CSV  to save the file"),
                ("Step 4 — Preparing a CSV manually",
                 "Required columns:  Latitude  and  Longitude  "
                 "(decimal format, e.g. 22.7196 and 75.8577). "
                 "Optional: Timestamp (DD/MM/YYYY HH:MM:SS), "
                 "Event (BEGIN/CONTINUE/END), IMEI, IMSI. "
                 "Click  ⬇️ Download Sample CSV  for a ready-made template."),
                ("Step 5 — Enter interception parameters",
                 "Fill in:\n"
                 "  • MSISDN — the mobile number being tracked\n"
                 "  • Target Number — same as MSISDN usually\n"
                 "  • Start Call ID — first Call ID number (auto-increments per row)\n"
                 "  IMEI and IMSI are read per-row from the CSV if provided."),
                ("Step 6 — Select CSV and output folder",
                 "Click  Browse…  to load your coordinates CSV. "
                 "Click  Browse…  next to Output Folder to choose where files are saved."),
                ("Step 7 — Generate",
                 "Click  🚀 Generate CDR Files. One .txt CDR file is created per row. "
                 "TimeStamp and LastActivity in each file are identical. "
                 "The results table shows filename, coordinates, and event type."),
                ("Step 8 — Upload",
                 "Go to  🏠 Home  →  Upload Data  →  LBS/LUDR/SMS card "
                 "→ select the generated files → upload."),
            ]),

            # ── 7. VOICE CALL GENERATOR ─────────────────────────
            ("📞", "Generating Voice Call Files", [
                ("What does this generate?",
                 "The Voice Call Generator creates the complete Hi2/ and Hi3/ folder "
                 "structure for one voice call interception.\n"
                 "  Hi2/ — HI2 signalling text files (Begin, Continue if conference, End)\n"
                 "  Hi3/ — two WAV audio copies + uae_cs_...call_data_record.txt (CRI)\n\n"
                 "Two call types are supported:\n"
                 "  • Normal Call — Begin + End (CDRType N, IRIVersion 31)\n"
                 "  • Conference Call — Begin + Continue + End (CDRType A, IRIVersion 1)"),
                ("Step 1 — Open the generator",
                 "Click  🏠 Home  →  Generate Data  →  Voice Call Generator."),
                ("Step 2 — Select Call Type",
                 "At the top of the Interception Parameters section, select:\n"
                 "  ● Normal — for a standard two-party call\n"
                 "  ● Conference — for a multi-party conference call\n\n"
                 "If Conference is selected, three additional fields appear:\n"
                 "  • Conference Numbers — participants e.g. Conf:919893586776,07000026454\n"
                 "  • Cell ID — tower ID e.g. 404-93-200233-252\n"
                 "  • Target IP — IPv4 or IPv6 address of target"),
                ("Step 3 — Prepare your WAV file",
                 "You need a WAV audio file in A-law format (8kHz, Mono, 8-bit).\n"
                 "To convert a standard WAV:\n"
                 "  1. Go to https://www.g711.org\n"
                 "  2. Upload your WAV → select A-law, 8000 Hz, Mono → download.\n"
                 "When you browse for the WAV, the utility shows codec, duration, and "
                 "whether it is  ✅ Compatible  or  ⚠️ Not A-law 8kHz Mono."),
                ("Step 4 — Set output folder and WAV prefix",
                 "Select an output folder (Hi2/ and Hi3/ are created inside it).\n"
                 "Set the  HI3 WAV Prefix  — e.g. BanglaCall → creates "
                 "BanglaCall_a.wav and BanglaCall_b.wav."),
                ("Step 5 — Fill in parameters",
                 "All fields are on a single row:\n"
                 "  • Target Number — LIID and MSISDN are automatically set to this value\n"
                 "  • Calling Number — the caller (From)\n"
                 "  • Called Number — the recipient (To)\n"
                 "  • Call ID — unique integer used in filenames\n"
                 "  • Call Start — DD-MM-YYYY HH:MM:SS\n"
                 "  • Call Direction — Outgoing or Incoming\n\n"
                 "The following are handled automatically (no input needed):\n"
                 "  • Duration — read from the WAV file header automatically\n"
                 "  • IMEI / IMSI — randomly generated each time\n"
                 "  • Network ID / NAT IP — randomly generated each time\n"
                 "  • Access Type — always EUTRAN\n"
                 "  • Release Reason — always Normal call clearing"),
                ("Step 6 — Check file preview and generate",
                 "The Output Structure panel shows the exact filenames before you generate. "
                 "Click  🚀 Generate  — Hi2/ and Hi3/ folders are created immediately."),
                ("Step 7 — Upload",
                 "Go to  🏠 Home  →  Upload Data  →  Voice Call Data. "
                 "Select the output folder. The correct upload sequence is enforced automatically."),
            ]),

                        # ── 8. CONFERENCE CALL ───────────────────────────────
            ("📞", "Generating Conference Call Files", [
                ("What is a Conference call in this context?",
                 "A conference call interception file uses a different CDR format:\n"
                 "  • CDRType : A  (not N)\n"
                 "  • IRIVersion : 1  (not 31)\n"
                 "  • CalledNumber : CONF  (fixed)\n"
                 "  • Three HI2 files: Begin + Continue (SIP/2.0 200 OK) + End\n"
                 "  • Conference Numbers lists all participants"),
                ("How to generate",
                 "In the Voice Call Generator, select  ● Conference  under Call Type. "
                 "Fill in Conference Numbers, Cell ID, and Target IP in addition to "
                 "the standard parameters. Generate as normal — three HI2 files are created."),
                ("Uploading conference call files",
                 "Upload works the same way as a normal call — select the folder containing "
                 "Hi2/ and Hi3/ sub-folders. Both Begin files (O and C) are uploaded "
                 "automatically in the correct sequence."),
            ]),

            # ── 9. BULK VOICE CALL GENERATOR ─────────────────────
            ("📞", "Bulk Voice Call Generator — Generate Many Calls at Once", [
                ("What is the Bulk Voice Call Generator?",
                 "The Bulk Voice Call Generator lets you create tens or hundreds of "
                 "voice call folders in a single operation. Each call gets its own "
                 "output folder with Hi2/ and Hi3/ sub-folders, correctly named HI2 "
                 "files, and WAV audio copied in.\n\n"
                 "Two modes are available:\n"
                 "  📋 CSV Mode — define every call precisely in a spreadsheet\n"
                 "  ⚡ Quick Mode — fill one form, set a count, auto-generate N calls"),

                ("CSV Mode — Step 1: Open the generator",
                 "Click  🏠 Home  →  Generate Data  →  📞 Bulk Voice Generator.\n"
                 "The page opens on the CSV tab by default."),

                ("CSV Mode — Step 2: Download the sample CSV",
                 "Click  ⬇️ Download Sample CSV  and open it in Excel.\n"
                 "It has all 21 columns pre-filled with one Normal call and one Conference call.\n\n"
                 "Required columns (marked * in the UI):\n"
                 "  CallType · LIID · MSISDN · TargetNumber · CallingNumber\n"
                 "  CalledNumber · IMEI · IMSI · CallID · CallStart · Duration\n\n"
                 "Optional columns (blank = sensible default applied automatically):\n"
                 "  NetworkID · NATIP · AccessType · CallDirection · ReleaseReason\n"
                 "  ConfNumbers · CellID · TargetIP · Prefix · WAVFile"),

                ("CSV Mode — Step 3: Fill in your CSV",
                 "One row = one complete call folder. Key fields:\n\n"
                 "  • CallType — write  Normal  or  Conference  (any case)\n"
                 "  • CallID — unique integer per row (e.g. 13092840, 13092841…)\n"
                 "  • CallStart — format: DD-MM-YYYY HH:MM:SS\n"
                 "  • Duration — HH:MM:SS or seconds (e.g. 00:02:30 or 150)\n"
                 "  • Prefix — folder name for this call "
                 "(e.g. BanglaCall → creates BanglaCall/Hi2/ and BanglaCall/Hi3/)\n"
                 "  • WAVFile — just the filename (e.g. call1.wav), not the full path\n"
                 "  • ConfNumbers — Conference only: e.g. Conf:919893586776,07000026454\n"
                 "  • CellID and TargetIP — Conference only"),

                ("CSV Mode — Step 4: Select files and WAV folder",
                 "  1. Browse… next to Input CSV File → select your CSV\n"
                 "  2. Browse… next to Output Folder → where all call folders will be created\n"
                 "  3. Browse… next to WAV Files Folder → folder containing your .wav files\n\n"
                 "The WAV folder status shows how many WAV files were found. "
                 "If a WAV filename from the CSV is not found in that folder, "
                 "a silent placeholder WAV is used and the row is marked ⚠️ Warn."),

                ("CSV Mode — Step 5: Generate and read results",
                 "Click  🚀 Generate All Calls.\n\n"
                 "The progress bar shows which call is being processed. "
                 "The results table shows each row with:\n"
                 "  #  ·  Type  ·  Prefix  ·  Calling  ·  Called  ·  Duration  ·  WAV  ·  Status\n\n"
                 "Status colours:\n"
                 "  ✅ OK — all files created successfully\n"
                 "  ⚠️ Warn — created but WAV not found or other warning\n"
                 "  ❌ Skip — row skipped due to missing required fields\n\n"
                 "Click  ⛔ Cancel  to stop at any time. "
                 "Already-generated call folders are kept."),

                ("Quick Mode — Step 1: Switch to Quick Mode tab",
                 "Click the  ⚡ Quick Mode  tab at the top of the Bulk Voice Generator page."),

                ("Quick Mode — Step 2: Fill in Common Parameters",
                 "These fields apply to every call in the batch:\n\n"
                 "  LIID · MSISDN · Target Number · Calling Number · Called Number\n"
                 "  IMEI · IMSI · Network ID · NAT IP · Access Type\n"
                 "  Call Direction (Outgoing / Incoming) · Release Reason\n\n"
                 "  Start Call ID — first Call ID; each next call auto-increments by 1\n"
                 "  First Call Start — timestamp for call #1 (DD-MM-YYYY HH:MM:SS)\n"
                 "  Duration — same duration applied to every call in the batch"),

                ("Quick Mode — Step 3: Bulk Settings",
                 "  Number of Calls — how many folders to create (e.g. 10)\n"
                 "  Folder Prefix — base folder name "
                 "(e.g. BulkCall → BulkCall_001, BulkCall_002…)\n"
                 "  Mins Between Calls — timestamp gap between calls "
                 "(e.g. 5 → call 1 at 10:00, call 2 at 10:05, call 3 at 10:10…)\n"
                 "  Call Type — Normal or Conference for all calls in this batch\n\n"
                 "If Conference is selected, three extra fields appear:\n"
                 "  Conference Numbers · Cell ID · Target IP"),

                ("Quick Mode — Step 4: WAV Files folder",
                 "Browse to a folder containing your .wav files.\n\n"
                 "WAV assignment order (alphabetical):\n"
                 "  Call 001 → 1st WAV file\n"
                 "  Call 002 → 2nd WAV file\n"
                 "  Call 011 → 1st WAV again (cycles when list ends)\n\n"
                 "If the folder is empty or not set, "
                 "silent placeholder WAVs are created for every call."),

                ("Quick Mode — Step 5: Select output folder and generate",
                 "Browse to an Output Folder, then click  🚀 Generate Calls.\n\n"
                 "All N call folders are created with Hi2/ and Hi3/ structure. "
                 "The results table shows each call with its WAV assignment and status. "
                 "Click  📁 Open Output Folder  to see all generated folders immediately."),

                ("Uploading bulk-generated calls to the server",
                 "Go to  🏠 Home  →  Upload Data  →  📞 Voice Call Data.\n"
                 "Select one call folder at a time (the folder containing Hi2/ and Hi3/).\n\n"
                 "The upload enforces the correct sequence automatically for each folder:\n"
                 "  ① cp -r HI3  →  /etc/vsf/input/\n"
                 "  ② 1 second wait\n"
                 "  ③ HI2 Begin file(s)  →  WatchDir\n"
                 "  ④ HI2 End file  →  WatchDir"),
            ]),

            # ── v3.0 NEW FEATURES GUIDE ──────────────────────────
            ("🆕", "What's New in v3.0 — Detailed Guide", [
                ("Scheduled Upload",
                 "Found on the Upload Data page — three buttons: "
                 "🕐 Schedule PCAP, 🕐 Schedule LUDR, 🕐 Schedule Voice.\n\n"
                 "How to use:\n"
                 "  1. Click 🕐 Schedule PCAP (or LUDR / Voice).\n"
                 "  2. Click Browse… and select the file/folder to upload.\n"
                 "  3. Check 'Re-use same file each run' (default ON) — the browse "
                 "window will NOT open again on each scheduled run.\n"
                 "  4. Set the interval in minutes (e.g. 15).\n"
                 "  5. Click ▶ Start Schedule.\n\n"
                 "A live countdown appears on the Upload page: "
                 "'🔁 Next PCAP in 14m 32s'. "
                 "Click ⏹ Stop in the dialog to cancel at any time."),

                ("Session Summary Strip",
                 "The Home page shows a teal strip with live counts for this session:\n\n"
                 "  📡 PCAP Uploads  ·  📍 LUDR/SMS  ·  📞 Voice  ·  "
                 "⚙️ Generated Files  ·  ❌ Failures\n\n"
                 "These update automatically after every upload or generate action. "
                 "The 🌐 Open ComTrail Web UI button is also embedded in this strip "
                 "between Failures and 'This session'."),

                ("Upload History Search & Re-run",
                 "The recent history table on the Upload Data page now has a search bar.\n\n"
                 "  • Type any keyword (file name, status, type) to filter results instantly.\n"
                 "  • Click ✕ to clear the search.\n"
                 "  • Each history row has a ↩ Re-run button — click it to re-launch "
                 "that upload type immediately (opens the file picker for a fresh selection)."),

                ("Upload Speed & ETA",
                 "The PCAP and LUDR progress windows now show real-time transfer metrics.\n\n"
                 "  • Speed: e.g.  2.3 MB/s\n"
                 "  • ETA: e.g.  ~40s left  or  ~2m 15s left\n\n"
                 "These update live as files are transferred."),

                ("Mandatory Field Validation",
                 "Required fields are now visually enforced across all generators:\n\n"
                 "SMS Generator:\n"
                 "  • Target Number — required, digits only.\n\n"
                 "LBS / LUDR Generator:\n"
                 "  • Target Number — required, digits only.\n\n"
                 "Voice Call Generator:\n"
                 "  • Target Number, Calling Number, Called Number, Call Start — all required.\n"
                 "  • Number fields (Target, Calling, Called) accept digits only — "
                 "letters and symbols are stripped instantly.\n\n"
                 "The field border is red when empty and turns green when a valid value is entered. "
                 "Clicking Generate with a missing field shows an error listing what is missing."),

                ("Copy IP in Settings",
                 "A 📋 clipboard icon appears next to each server IP field in Settings.\n\n"
                 "Click it to copy the IP address to the clipboard — "
                 "a toast notification confirms: 'Copied: 192.168.1.10'.\n"
                 "Useful when you need to paste the server IP into another tool quickly."),

                ("Navigation Warning During Upload",
                 "If you click a nav pill while an upload is in progress, a dialog appears:\n\n"
                 "  'X upload(s) still in progress. Leaving now will not cancel them.'\n\n"
                 "Options:\n"
                 "  • Continue Anyway — navigate away; upload continues in the background.\n"
                 "  • Stay — remain on the current page.\n\n"
                 "The upload thread is never killed — it always completes regardless."),

                ("Collapsible Sections",
                 "The SMS IRI Generator and Voice Call Generator pages now have "
                 "collapsible sections marked with ▾ / ▸ arrows.\n\n"
                 "Click any section header to collapse or expand it. "
                 "This keeps the page tidy when you only need to work on one section at a time."),

                ("Crystal Teal Theme & Hover Effects",
                 "The full colour scheme changed from blue to crystal teal:\n"
                 "  • Primary:  #0891b2  ·  Hover:  #0e7490  ·  Accent:  #06b6d4\n\n"
                 "All buttons brighten on mouse-over and show a hand cursor. "
                 "Applied globally — every button in the app is covered."),

                ("Bug Fixes",
                 "v3.0 fixes two pages that were showing blank content:\n\n"
                 "Voice Call Generator:\n"
                 "  • callid_var, cell_id_var, target_ip_var were referenced before being "
                 "defined — NameError crashed the page silently. Fixed by removing the "
                 "stale references from the preview binding loop.\n\n"
                 "  • _toggle_conf() called update_preview() before it was defined — "
                 "NameError on page load. Fixed by moving the initial _toggle_conf() "
                 "call to after update_preview() is defined.\n\n"
                 "Bulk Voice Generator:\n"
                 "  • _bvs = self._bulk_voice_state was placed in the Quick Mode section "
                 "but referenced in CSV Mode — NameError crashed the whole page. "
                 "Fixed by moving the assignment to before the first use."),
            ]),

                        # ── 11. SETTINGS ─────────────────────────────────────
            ("⚙️", "Settings — Configuring Servers", [
                ("Open Settings",
                 "Click  ⚙️ Settings  in the top navigation bar."),
                ("Understanding the three server cards",
                 "There are three server cards — PCAP, LBS/LUDR/SMS, and Voice.\n"
                 "Each card has:\n"
                 "  • IP Address — the server's IP (e.g. 192.168.148.71)\n"
                 "  • Password — the root SSH password\n"
                 "  • Remote Path — where files are placed on the server\n"
                 "  • Status dot — Blue=connected, Red=failed, Grey=not configured"),
                ("Kafka & Solr configuration",
                 "The Kafka & Solr section lets you set:\n"
                 "  • Kafka UI Base URL and Cluster Name\n"
                 "  • Voice Topic Name — the Kafka topic for voice enriched messages\n"
                 "  • IP Topic Name — the Kafka topic for IP enriched messages\n"
                 "  • Solr Base URL and Collection\n"
                 "Click  💾 Save  on each row after editing."),
                ("Saving individual server settings",
                 "After editing a server card, click its  💾 Save  button. "
                 "The utility reconnects and shows the result after 4 seconds:\n"
                 "  ✅ Connected to X.X.X.X  (shown in green) — SSH login successful\n"
                 "  ❌ Could not connect — wrong IP, password, or network issue\n"
                 "The Remote Path hint also turns green when a known valid path is selected."),
                ("Save All Settings at once",
                 "Click  💾 Save All Settings  to save all three servers at once. "
                 "A popup shows the connection result for each server."),
                ("Backing up and restoring settings",
                 "  📥 Export Config  — export all settings including Kafka topic names to a CSV file.\n"
                 "  📤 Import Config  — import from a previously saved CSV file.\n"
                 "The exported CSV includes Voice Topic Name and IP Topic Name "
                 "so they are restored correctly on import.\n"
                 "Useful when setting up the utility on a new machine."),
            ]),

            # ── 12. ACTIVITY LOGS ────────────────────────────────
            ("📋", "Activity Logs", [
                ("Opening Activity Logs",
                 "Click  View all →  on the Home page, or open the Logs page and select "
                 "the  📋 Activity Logs  tab.\n\n"
                 "Every in-app action is recorded here: uploads, file transfers, "
                 "generator runs, settings saves, SSH connections, Kafka fetches, "
                 "and errors. Each entry shows:\n"
                 "  Timestamp  ·  Level  ·  Category  ·  Detail"),

                ("Error badge — know when something went wrong",
                 "A red badge appears on the  View all →  link on the Home page and on "
                 "the  📋 View Logs  button on the Help page whenever new errors have "
                 "been recorded since you last opened the Logs page.\n\n"
                 "The number inside the badge is the count of unseen errors. "
                 "It resets to zero as soon as you open the Logs page."),

                ("Filtering by level",
                 "Use the Level radio buttons at the top of the tab:\n"
                 "  All  — show every entry\n"
                 "  Info  — informational messages only\n"
                 "  Errors  — error entries only (shown in red)\n"
                 "  Warns  — warning entries only (shown in amber)\n\n"
                 "The stats bar above the table always shows live counts for "
                 "Total, Info, Errors, and Warnings regardless of the active filter."),

                ("Filtering by category",
                 "The  Category  dropdown lists every unique action category that "
                 "has been recorded in the current session — for example: "
                 "Navigation, Upload, SMS Generator, Kafka, Target Details.\n\n"
                 "Select a category to show only entries from that area of the app. "
                 "Combine it with the Level filter and the search box to narrow "
                 "results to exactly what you need."),

                ("Text search",
                 "Type in the  🔍  search box to filter entries by any text — "
                 "matched against Timestamp, Level, Category, and Detail simultaneously.\n\n"
                 "Click  ✕  next to the search box to clear it instantly."),

                ("Live mode — entries appear in real time",
                 "The  🟢 Live ON  toggle refreshes the table every 2 seconds. "
                 "New entries added by any action in the app appear automatically "
                 "without navigating away.\n\n"
                 "Click the button to switch to  ⏸ Live OFF  if you want to pause "
                 "the view to inspect a specific set of entries without the list moving."),

                ("Identifying new entries",
                 "Entries that were added after the last time you opened the Logs page "
                 "are displayed in bold. Older entries from previous visits use normal "
                 "weight. This lets you spot fresh activity at a glance, even in a "
                 "long log."),

                ("Viewing the full detail of an entry",
                 "Double-click any row to open a detail popup.\n\n"
                 "The popup shows the complete Detail text in a scrollable box — "
                 "useful for long error messages or multi-line outputs that are "
                 "truncated in the table.\n\n"
                 "Click  📋 Copy Detail  to copy the text to the clipboard, "
                 "or press  Escape  to close the popup."),

                ("Copying a row",
                 "Three ways to copy:\n"
                 "  1. Right-click any row → Copy Row (all columns, tab-separated)\n"
                 "  2. Right-click → Copy Detail Only (just the Detail column)\n"
                 "  3. Select a row and press  Ctrl+C  to copy the full row\n\n"
                 "The detail popup also has its own  📋 Copy Detail  button."),

                ("Exporting and clearing",
                 "  💾 Export  — saves all currently loaded entries to a CSV file. "
                 "The file is named  comtrail_logs_YYYYMMDD_HHMMSS.csv  by default.\n\n"
                 "  🗑️ Clear  — removes all entries from the in-session log and "
                 "reloads the page. This does not affect the upload history CSV."),

                ("Follow mode — keep the latest entry in view",
                 "The  ⬇ Follow ON  button keeps the table scrolled to the most "
                 "recent entry at all times. When Live mode is also on, new entries "
                 "scroll into view automatically.\n\n"
                 "Click  ⬇ Follow OFF  to pin the scroll position so you can "
                 "read earlier entries without the view jumping."),
            ]),

            # ── 13. SERVER LOGS ──────────────────────────────────
            ("🖥️", "Server Logs", [
                ("Opening Server Logs",
                 "Open the Logs page and select the  🖥️ Server Logs  tab.\n\n"
                 "The left panel lists all configured ComTrail backend services. "
                 "The right panel shows the log content for the selected service. "
                 "Up to three services can be open as tabs simultaneously."),

                ("Enabling a service",
                 "All services start disabled by default. "
                 "Tick the checkbox ☑ next to a service name to enable monitoring. "
                 "Your selection is saved permanently and restored on the next launch.\n\n"
                 "Default enabled services: tmsc, prism, signal, RmqToKafka.\n\n"
                 "Disabled services show a grey accent bar and dimmed text. "
                 "Clicking a disabled service shows a reminder to enable it first."),

                ("Opening a service as a tab",
                 "Click any enabled service row to open it in the right panel. "
                 "A tab appears in the tab strip at the top of the right panel "
                 "showing the service name.\n\n"
                 "Click a different service to open it in a new tab alongside the first. "
                 "Switch between open tabs by clicking their names in the tab strip.\n\n"
                 "Close a tab with the  ✕  button on the tab. "
                 "Each tab maintains its own file selection and log content independently."),

                ("Choosing a log file",
                 "When a service is opened, its log directory is listed automatically "
                 "and the latest file is loaded. Use the  Log File  dropdown to select "
                 "an older file from the same directory.\n\n"
                 "Files are listed newest first (sorted by modification time on the server)."),

                ("Tail mode and Grep mode",
                 "Use the  Mode  radio buttons to switch how the log is read:\n\n"
                 "  Tail mode  — reads the last N lines of the log file "
                 "(controlled by the Lines selector: 50 / 100 / 200 / 500 / 1000). "
                 "This is the default and works well for monitoring recent activity.\n\n"
                 "  Grep mode  — searches across the entire log file for the text in "
                 "the Search box using case-insensitive matching. "
                 "The Lines limit is bypassed — all matching lines are returned. "
                 "Use this when you need to find all occurrences of an error or "
                 "keyword across a large log file regardless of how far back it occurred."),

                ("Searching and keyword highlighting",
                 "Type in the  Search  box to filter what is displayed.\n\n"
                 "In Tail mode, the search filters the loaded lines and highlights "
                 "every match in yellow. The viewer scrolls to the first match "
                 "automatically and the status bar shows the total match count "
                 "(e.g.  · 12 matches).\n\n"
                 "In Grep mode, the search term is sent directly to the server as a "
                 "grep command, returning only the matching lines. "
                 "Matches are still highlighted yellow in the viewer.\n\n"
                 "Click  ✕  next to the search box to clear the filter."),

                ("Log level colour-coding",
                 "Each line in the viewer is automatically colour-coded by severity:\n\n"
                 "  Red  — lines containing: error, exception, traceback, fatal, critical, failed\n"
                 "  Amber  — lines containing: warn, warning\n"
                 "  Blue  — lines containing: info, started, connected, success, initialized, ready\n"
                 "  Dimmed  — lines containing: debug, trace, verbose\n"
                 "  Normal  — all other lines\n\n"
                 "Colour-coding is applied in both Tail and Grep modes."),

                ("Auto-refresh",
                 "The  Auto-refresh (5s)  checkbox keeps the current log file "
                 "reloading every 5 seconds in the background. "
                 "Use this to monitor a live service without clicking Refresh manually.\n\n"
                 "The status bar shows the last refresh time after each load "
                 "(e.g.  ✅  200 lines · tmsc.log · 14:32:05)."),

                ("Saving the displayed content",
                 "Click  📥 Save Displayed  to save the lines currently shown in the "
                 "viewer to a local file. Only the lines that are visible are saved — "
                 "in Tail mode this is the last N lines; in Grep mode this is the "
                 "matching lines."),

                ("Downloading the complete log file",
                 "Click  ⬇ Full File  to download the entire log file from the server "
                 "to your local machine via SFTP.\n\n"
                 "A save dialog opens so you can choose the destination. "
                 "The status bar shows the file size after the download completes. "
                 "Use this when you need to inspect or share the full log history "
                 "beyond the tail limit."),

                ("Adding, editing, and removing services",
                 "  + Add  (top of the Services panel) — opens a dialog to add a new "
                 "service. Enter a name and the log directory path on the server.\n\n"
                 "  ✎  on any service row — opens the edit dialog to change the "
                 "service name, path, or enabled state.\n\n"
                 "  ✕  on any service row — removes the service after confirmation. "
                 "All changes are saved immediately to the config file."),

                ("Troubleshooting — service not loading",
                 "Each SSH read has a hard 15-second timeout. "
                 "If the server is unreachable or the path does not exist, "
                 "the status bar shows  ❌  with the error message and the "
                 "UI remains fully responsive.\n\n"
                 "Common causes:\n"
                 "  • Server not connected — check the PCAP server settings\n"
                 "  • Wrong log path — use  ✎  to correct the path for that service\n"
                 "  • Empty directory — the status bar shows  ⚠️ No files in <path>"),
            ]),

            # ── 13. UPLOAD & GENERATE HISTORY ────────────────────
            ("📊", "Upload & Generate History", [
                ("Where to find history",
                 "History is shown in two places:\n"
                 "  • Home (Upload Data) page — last 10 upload actions inline\n"
                 "  • Generate Data page — last 10 generate actions inline\n"
                 "  • Full history: click  📊 View Full History  on either page"),
                ("What is recorded",
                 "Every upload and generate action is saved to  comtrail_upload_history.csv  "
                 "next to the utility EXE. Each record includes:\n"
                 "  • Date / Time, Action, Files count, Server IP, Status, Note"),
                ("Upload History page",
                 "Click  📊 View Full History  to open the full table.\n"
                 "Filter by: PCAP Upload, LBS Upload, Voice Upload, ✅ OK, ❌ Failed.\n"
                 "Search by keyword. Export as CSV. Clear all history."),
                ("Generate History",
                 "The Generate Data page shows only SMS Generate, LBS Generate, "
                 "and Voice Generate records — filtered automatically.\n"
                 "Each row has a colour-coded left stripe: blue=SMS, green=LBS, purple=Voice."),
            ]),

            # ── 14. SAMPLE DATA ──────────────────────────────────
            ("📁", "Sample Data — Testing the System", [
                ("What is Sample Data?",
                 "The Sample Data page browses pre-existing test files on the server "
                 "(e.g. /data5/sample/pcap). Use these to verify the system is working "
                 "before uploading real interception data."),
                ("Step 1 — Open Sample Data",
                 "Click  📁 Sample Data  in the top navigation bar."),
                ("Step 2 — Select data type and refresh",
                 "Choose: 📡 PCAP, 📍 LUDR, 💬 SMS, or 📞 Voice.\n"
                 "Click  🔄 Refresh File List  to list all files in that folder on the server."),
                ("Step 3 — Download templates",
                 "Click  ⬇️ Download CSV  to download a sample CSV template for the generators.\n"
                 "Click  📦 Download All Sample Data  to save all four templates at once."),
            ]),

            # ── 15. TARGET DETAILS ───────────────────────────────
            ("🎯", "Target Details — Viewing Intercept Targets", [
                ("Open Target Details",
                 "Click  🎯 Targets  in the top navigation bar."),
                ("What it shows",
                 "Lists all configured intercept targets fetched live from the server:\n"
                 "  • Filter ID — the system's internal identifier\n"
                 "  • Mobile Number — the intercepted number\n"
                 "  • Target Name — the label for this target"),
                ("Refreshing",
                 "Click  🔄 Refresh  to fetch the latest list. "
                 "The list is cached — navigating away and back does not re-fetch."),
                ("Opening the ComTrail Web Interface",
                 "Click  🌐 Open ComTrail Web UI  to open the ClearInsight web interface "
                 "in your default browser. Requires the ComTrail URL in Settings."),
            ]),
        ]

        # ── Accordion builder ──────────────────────────────────────
        for icon, topic_title, steps in TOPICS:
            # Outer accordion frame
            acc = tk.Frame(body, bg=self.C("panel"),
                           highlightbackground=self.C("border"),
                           highlightthickness=1)
            acc.pack(fill="x", pady=4)

            # Header row (always visible — click to expand)
            is_open   = [False]
            content   = tk.Frame(acc, bg=self.C("panel"))
            arrow_var = tk.StringVar(value="▶")

            header = tk.Frame(acc, bg=self.C("panel"), cursor="hand2")
            header.pack(fill="x")

            tk.Label(header, text=icon, bg=self.C("panel"),
                     font=(_UI_FONT, 14), width=3).pack(
                     side="left", padx=(10, 0), pady=10)
            tk.Label(header, text=topic_title,
                     bg=self.C("panel"), fg=self.C("card_title"),
                     font=(_UI_FONT, 11, "bold")).pack(
                     side="left", padx=8, pady=10)
            arrow_lbl = tk.Label(header, textvariable=arrow_var,
                                 bg=self.C("panel"), fg=self.C("success"),
                                 font=(_UI_FONT, 11, "bold"))
            arrow_lbl.pack(side="right", padx=14)

            # Content (hidden by default)
            for i, (step_title, step_body) in enumerate(steps):
                # Separator before each step except first
                if i > 0:
                    tk.Frame(content, bg=self.C("border"), height=1).pack(
                        fill="x", padx=16)
                row = tk.Frame(content, bg=self.C("panel"))
                row.pack(fill="x", padx=16, pady=(8, 6))
                badge = tk.Label(row, text=f" {i+1} ",
                                 bg=self.C("primary"), fg="white",
                                 font=(_UI_FONT, 9, "bold"))
                badge.pack(side="left", padx=(0, 10), ipady=2, ipadx=2)
                col = tk.Frame(row, bg=self.C("panel"))
                col.pack(side="left", fill="x", expand=True)
                tk.Label(col, text=step_title,
                         bg=self.C("panel"), fg=self.C("card_title"),
                         font=(_UI_FONT, 10, "bold"),
                         anchor="w").pack(anchor="w")
                tk.Label(col, text=step_body,
                         bg=self.C("panel"), fg=self.C("subtle"),
                         font=(_UI_FONT, 9),
                         wraplength=820, justify="left",
                         anchor="w").pack(anchor="w", pady=(2, 0))

            # Bottom padding in content
            tk.Frame(content, bg=self.C("panel"), height=8).pack()

            def _toggle(event=None, c=content, o=is_open, a=arrow_var):
                if o[0]:
                    c.pack_forget()
                    a.set("▶")
                    o[0] = False
                else:
                    c.pack(fill="x")
                    a.set("▼")
                    o[0] = True

            header.bind("<Button-1>", _toggle)
            for child in header.winfo_children():
                child.bind("<Button-1>", _toggle)

        # ── Footer ─────────────────────────────────────────────────
        footer = tk.Frame(body, bg=self.C("bg"))
        footer.pack(fill="x", pady=(24, 12))
        tk.Label(footer,
                 text="ComTrail Data Upload & Generate Utility  ·  v3.0  ·  "
                      "Author: Mohit Tambe  ·  © ClearTrail Technologies",
                 bg=self.C("bg"), fg=self.C("dim"),
                 font=(_UI_FONT, 9)).pack()

    # ──────────────────────────────────────────────────────────────
    # SCHEDULE — persistent engine (survives page navigation)
    # ──────────────────────────────────────────────────────────────
    def _sched_save(self):
        with getattr(self, "_sched_lock", threading.Lock()):
            jobs_out = []
            for j in getattr(self, "_sched_jobs", []):
                rec = {k: v for k, v in j.items() if k not in ("_fired_times", "_fired_date")}
                if "id" not in rec:
                    import uuid as _uuid
                    rec["id"] = str(_uuid.uuid4())
                jobs_out.append(rec)
            try:
                with open(SCHEDULE_FILE, "w", encoding="utf-8") as f:
                    json.dump({"jobs": jobs_out}, f, indent=2)
            except Exception as e:
                LOG.log("Schedule", f"Save error: {e}", "ERROR")

    def _sched_load(self):
        import uuid as _uuid
        if not os.path.exists(SCHEDULE_FILE):
            return
        try:
            with open(SCHEDULE_FILE, encoding="utf-8") as f:
                data = json.load(f)
            loaded = data.get("jobs", [])
            for j in loaded:
                j.setdefault("id",              str(_uuid.uuid4()))
                j.setdefault("name",            "")
                j.setdefault("run_times",       [j.get("run_at", "09:00")])
                j.setdefault("interval_mode",   False)
                j.setdefault("interval_hours",  1)
                j.setdefault("weekdays",        [])
                j.setdefault("repeat",          True)
                j.setdefault("retry_max",       0)
                j.setdefault("retry_delay_min", 5)
                j.setdefault("status",          "Waiting")
                j.setdefault("last_run",        None)
                j.setdefault("last_run_ts",     "")
                j.setdefault("last_result",     "")
                j.setdefault("history",         [])
                j["_fired_times"] = []
                j["_fired_date"]  = ""
                if j["status"] in ("Running", "Retrying"):
                    j["status"] = "Waiting"
            self._sched_jobs = loaded
            LOG.log("Schedule", f"Loaded {len(loaded)} job(s) from disk")
        except Exception as e:
            LOG.log("Schedule", f"Load error: {e}", "ERROR")

    def _sched_startup_check(self):
        from datetime import datetime as _DT2
        jobs = getattr(self, "_sched_jobs", [])
        if not jobs:
            return
        now      = _DT2.now()
        today    = now.strftime("%Y-%m-%d")
        now_hhmm = now.strftime("%H:%M")
        missed = []
        for j in jobs:
            if j.get("status") in ("Disabled", "Done"):
                continue
            if j.get("last_run") == today:
                continue
            for rt in j.get("run_times", []):
                if rt < now_hhmm:
                    missed.append(f"{j.get('name') or j['type']} at {rt}")
                    break
        if missed:
            self.after(0, lambda: self._toast(
                f"⏰  {len(missed)} missed job(s) — open Schedule to run now", "warn"))
            LOG.log("Schedule", f"Missed jobs on startup: {', '.join(missed)}")

    def _sched_notify(self, title, msg, kind="info"):
        self._toast(f"{title}: {msg}", kind)
        LOG.log("Schedule", f"{title} — {msg}")
        try:
            import ctypes
            ctypes.windll.user32.MessageBeep(0xFFFFFFFF)
        except Exception:
            pass

    def _sched_execute_job(self, job, _retry_attempt=0):
        from datetime import datetime as _DT2
        import time as _time

        t    = job["type"]
        path = job.get("path", "")
        ts   = _DT2.now().strftime("%H:%M:%S")
        t0   = _time.time()

        # Set Running here too (covers _run_now path which bypasses _sched_run_tick)
        from datetime import datetime as _DT2b
        job["status"]      = "Running"
        job["last_run"]    = _DT2b.now().strftime("%Y-%m-%d")
        job["last_run_ts"] = ts
        job["last_result"] = "Running…"

        def _notify_refresh():
            live_cb = getattr(self, "_sched_page_refresh", None)
            if live_cb:
                try: self.after(0, live_cb)
                except Exception: pass

        _notify_refresh()
        self.after(0, lambda: self._sched_notify(
            "🕐 Schedule Started",
            f"{job.get('name') or t} fired at {ts}", "info"))
        self._sched_save()

        # Build file/folder argument
        if t in ("LUDR / LBS", "SMS"):
            raw_files = path.split(";") if ";" in path else [path]
            files = [f.strip() for f in raw_files if f.strip() and os.path.isfile(f.strip())]
            if not files:
                self._sched_job_failed(job, "No valid files found", t0, _retry_attempt); return
        else:
            folder = path.strip()
            if not folder or not os.path.isdir(folder):
                self._sched_job_failed(job, f"Folder not found: {folder}", t0, _retry_attempt); return

        # Dispatch upload using per-job events — eliminates shared-counter race condition
        _started = threading.Event()
        _done    = threading.Event()

        if t in ("LUDR / LBS", "SMS"):
            self.after(0, lambda ff=files: self._upload_ludr_with_files(ff, _done, _started))
        elif t == "PCAP":
            self.after(0, lambda fd=folder: self._upload_pcap_with_folder(fd, _done, _started))
        else:
            self.after(0, lambda fd=folder: self._upload_voice_with_folder(fd, _done, _started))

        def _poll():
            import time as _t2
            # Wait up to 15s for the upload task thread to actually start
            started = _started.wait(timeout=15)
            if not started:
                # Helper may have returned early (config/connection error) and set _done directly
                if _done.is_set():
                    self._sched_job_failed(
                        job, "Upload could not start — check config/connection", t0, _retry_attempt)
                else:
                    self._sched_job_failed(
                        job, "Upload did not start within 15s — check server connection", t0, _retry_attempt)
                return
            # Upload has started — wait up to 2 hours for completion
            finished = _done.wait(timeout=7200)
            if not finished:
                self._sched_job_failed(job, "Timeout after 2 hours", t0, _retry_attempt)
                return
            finish_ts  = _DT2.now().strftime("%H:%M:%S")
            duration   = f"{int(_time.time()-t0)}s"
            job["status"]      = "Scheduled" if job.get("repeat") else "Done"
            job["last_result"] = f"✅ {finish_ts}"
            job["last_run_ts"] = finish_ts
            hist_entry = {"ts": f"{_DT2.now().strftime('%Y-%m-%d')} {finish_ts}",
                          "result": "✅ OK", "duration": duration}
            job.setdefault("history", []).insert(0, hist_entry)
            job["history"] = job["history"][:10]
            _notify_refresh()
            self._sched_save()
            self.after(0, lambda: self._sched_notify(
                "✅ Schedule Done",
                f"{job.get('name') or t} done at {finish_ts} ({duration})",
                "success"))

        threading.Thread(target=_poll, daemon=True).start()

    def _sched_job_failed(self, job, err_msg, t0=None, retry_attempt=0):
        import time as _time
        from datetime import datetime as _DT2
        finish_ts = _DT2.now().strftime("%H:%M:%S")
        duration  = f"{int(_time.time()-t0)}s" if t0 else "—"
        retry_max = job.get("retry_max", 0)

        if retry_attempt < retry_max:
            delay = job.get("retry_delay_min", 5) * 60
            job["status"]      = "Retrying"
            job["last_result"] = f"⚠ Retry {retry_attempt+1}/{retry_max}"
            live_cb = getattr(self, "_sched_page_refresh", None)
            if live_cb:
                try: self.after(0, live_cb)
                except Exception: pass
            LOG.log("Schedule", f"Retrying in {delay}s (attempt {retry_attempt+1}/{retry_max})")
            def _retry():
                import time as _t2
                _t2.sleep(delay)
                job["status"] = "Running"   # set before re-entering execute so tick skips it
                self._sched_execute_job(job, retry_attempt + 1)
            threading.Thread(target=_retry, daemon=True).start()
            return

        job["status"]      = "Error"
        job["last_result"] = f"❌ {err_msg[:25]}"
        job["last_run_ts"] = finish_ts
        hist_entry = {"ts": f"{_DT2.now().strftime('%Y-%m-%d')} {finish_ts}",
                      "result": f"❌ {err_msg[:40]}", "duration": duration}
        job.setdefault("history", []).insert(0, hist_entry)
        job["history"] = job["history"][:10]
        live_cb = getattr(self, "_sched_page_refresh", None)
        if live_cb:
            try: self.after(0, live_cb)
            except Exception: pass
        self._sched_save()
        self.after(0, lambda: self._sched_notify(
            "❌ Schedule Failed",
            f"{job.get('name') or job['type']}: {err_msg}", "error"))

    def _sched_run_tick(self, *_):
        from datetime import datetime as _DT2
        now      = _DT2.now()
        now_str  = now.strftime("%H:%M")
        today    = now.strftime("%Y-%m-%d")
        weekday  = now.weekday()

        for job in list(getattr(self, "_sched_jobs", [])):
            if job.get("status") in ("Disabled", "Done", "Running", "Error", "Retrying"):
                continue
            wd = job.get("weekdays", [])
            if wd and weekday not in wd:
                continue
            if job.get("last_run") == today and not job.get("repeat"):
                continue

            # Reset fired-times at the start of each new day BEFORE any checks
            if job.get("_fired_date") != today:
                job["_fired_times"] = []
                job["_fired_date"]  = today

            fired_today = job["_fired_times"]

            if job.get("interval_mode"):
                ih       = max(1, int(job.get("interval_hours", 1)))
                last_ts  = job.get("last_run_ts", "")
                should_fire = False
                if not last_ts or job.get("last_run") != today:
                    should_fire = True
                else:
                    try:
                        from datetime import datetime as _DT3
                        last_dt = _DT3.strptime(f"{today} {last_ts[:8]}", "%Y-%m-%d %H:%M:%S")
                        should_fire = (now - last_dt).total_seconds() >= ih * 3600
                    except Exception:
                        should_fire = False
                if should_fire:
                    fired_today.append(now_str)
                    # Set Running on main thread before dispatch so next tick skips it
                    job["status"]   = "Running"
                    job["last_run"] = today
                    threading.Thread(target=self._sched_execute_job,
                                     args=(job,), daemon=True).start()
            else:
                for rt in job.get("run_times", []):
                    if rt == now_str and rt not in fired_today:
                        fired_today.append(rt)
                        # Set Running on main thread before dispatch so next tick skips it
                        job["status"]   = "Running"
                        job["last_run"] = today
                        threading.Thread(target=self._sched_execute_job,
                                         args=(job,), daemon=True).start()
                        break

        self._sched_ticker = self.after(10000, lambda: self._sched_run_tick())

    def _sched_ensure_ticker(self):
        if not getattr(self, "_sched_ticker", None):
            self._sched_ticker = self.after(10000, self._sched_run_tick)

    # ──────────────────────────────────────────────────────────────
    # SCHEDULE UPLOAD PAGE
    # ──────────────────────────────────────────────────────────────
    def show_schedule_page(self):
        import tkinter.filedialog as _fd_sched
        import uuid as _uuid
        from datetime import datetime as _DT

        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Schedule Upload page")

        _edit_id  = [None]
        _path_sel = [None]

        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(25, 5))
        tk.Label(hdr, text="🕐  Schedule Upload",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", padx=10, pady=4,
                  command=self.show_first_page).pack(side="right")
        tk.Frame(main, bg=self.C("border"), height=1).pack(fill="x", padx=50, pady=(4, 10))

        form_card = tk.Frame(main, bg=self.C("panel"),
                             highlightbackground=self.C("border"), highlightthickness=1)
        form_card.pack(fill="x", padx=50, pady=(0, 10))
        tk.Frame(form_card, bg=self.C("primary"), height=3).pack(fill="x")
        form = tk.Frame(form_card, bg=self.C("panel"))
        form.pack(fill="x", padx=16, pady=12)

        _form_title_var = tk.StringVar(value="➕  Add New Scheduled Job")
        tk.Label(form, textvariable=_form_title_var, bg=self.C("panel"),
                 fg=self.C("card_title"), font=(_UI_FONT, 11, "bold")).pack(anchor="w", pady=(0, 8))

        # Row 1: Name + Type
        r1 = tk.Frame(form, bg=self.C("panel"))
        r1.pack(fill="x", pady=(0, 6))
        tk.Label(r1, text="Job Name:", bg=self.C("panel"),
                 fg=self.C("muted"), font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(0, 4))
        _name_var = tk.StringVar()
        tk.Entry(r1, textvariable=_name_var, width=22,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"),
                 relief="flat", font=(_UI_FONT, 9)).pack(side="left", ipady=5, padx=(0, 16))
        tk.Label(r1, text="Type:", bg=self.C("panel"),
                 fg=self.C("muted"), font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(0, 4))
        _type_var = tk.StringVar(value="PCAP")
        ttk.Combobox(r1, textvariable=_type_var,
                     values=["PCAP", "LUDR / LBS", "SMS", "Voice"],
                     state="readonly", width=12).pack(side="left")

        # Row 2: Folder/Files
        r2 = tk.Frame(form, bg=self.C("panel"))
        r2.pack(fill="x", pady=(0, 6))
        tk.Label(r2, text="Folder / Files:", bg=self.C("panel"),
                 fg=self.C("muted"), font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(0, 4))
        _path_var = tk.StringVar()
        tk.Entry(r2, textvariable=_path_var, width=44,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"),
                 relief="flat", font=(_UI_FONT, 9)).pack(side="left", ipady=5, padx=(0, 8))

        def _browse():
            t = _type_var.get()
            if t in ("LUDR / LBS", "SMS"):
                files = _fd_sched.askopenfilenames(
                    title="Select Files",
                    filetypes=[("CSV/TXT", "*.csv *.txt"), ("All", "*.*")])
                if files:
                    _path_sel[0] = list(files)
                    _path_var.set("; ".join(files))
            else:
                folder = _fd_sched.askdirectory(title=f"Select {t} Folder")
                if folder:
                    _path_sel[0] = folder
                    _path_var.set(folder)

        tk.Button(r2, text="Browse…", bg=self.C("primary"), fg="white",
                  relief="flat", font=(_UI_FONT, 9, "bold"), cursor="hand2",
                  padx=8, pady=3, command=_browse).pack(side="left")

        # Row 3: Schedule mode
        r3 = tk.Frame(form, bg=self.C("panel"))
        r3.pack(fill="x", pady=(0, 6))
        _interval_mode  = tk.BooleanVar(value=False)
        _fixed_frame    = tk.Frame(r3, bg=self.C("panel"))
        _interval_frame = tk.Frame(r3, bg=self.C("panel"))

        tk.Label(_fixed_frame, text="Run at (HH:MM, comma-sep):",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(0, 4))
        _times_var = tk.StringVar(value="09:00")
        tk.Entry(_fixed_frame, textvariable=_times_var, width=22,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"),
                 relief="flat", font=(_UI_FONT, 10)).pack(side="left", ipady=5, padx=(0, 16))

        tk.Label(_interval_frame, text="Every:",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(0, 4))
        _interval_h = tk.StringVar(value="2")
        tk.Entry(_interval_frame, textvariable=_interval_h, width=5,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"),
                 relief="flat", font=(_UI_FONT, 10)).pack(side="left", ipady=5, padx=(0, 4))
        tk.Label(_interval_frame, text="hours",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(side="left", padx=(0, 16))

        def _toggle_mode(*_):
            if _interval_mode.get():
                _fixed_frame.pack_forget()
                _interval_frame.pack(side="left")
            else:
                _interval_frame.pack_forget()
                _fixed_frame.pack(side="left")

        tk.Checkbutton(r3, text="Interval mode (every N hours)",
                       variable=_interval_mode, command=_toggle_mode,
                       bg=self.C("panel"), fg=self.C("text"),
                       selectcolor=self.C("input_bg"),
                       activebackground=self.C("panel"),
                       font=(_UI_FONT, 9)).pack(side="left", padx=(0, 12))
        _fixed_frame.pack(side="left")

        # Row 4: Weekdays + Repeat
        r4 = tk.Frame(form, bg=self.C("panel"))
        r4.pack(fill="x", pady=(0, 6))
        tk.Label(r4, text="Days:", bg=self.C("panel"),
                 fg=self.C("muted"), font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(0, 6))
        _day_vars = {}
        for i, d in enumerate(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]):
            v = tk.BooleanVar(value=True)
            _day_vars[i] = v
            tk.Checkbutton(r4, text=d, variable=v,
                           bg=self.C("panel"), fg=self.C("text"),
                           selectcolor=self.C("input_bg"),
                           activebackground=self.C("panel"),
                           font=(_UI_FONT, 8)).pack(side="left", padx=1)
        tk.Frame(r4, bg=self.C("border"), width=1).pack(side="left", fill="y", padx=8)
        _repeat_var = tk.BooleanVar(value=True)
        tk.Checkbutton(r4, text="Repeat daily", variable=_repeat_var,
                       bg=self.C("panel"), fg=self.C("text"),
                       selectcolor=self.C("input_bg"),
                       activebackground=self.C("panel"),
                       font=(_UI_FONT, 9)).pack(side="left", padx=(0, 12))

        # Row 5: Retry
        r5 = tk.Frame(form, bg=self.C("panel"))
        r5.pack(fill="x", pady=(0, 6))
        tk.Label(r5, text="Retry on failure:", bg=self.C("panel"),
                 fg=self.C("muted"), font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(0, 4))
        _retry_max = tk.StringVar(value="2")
        tk.Entry(r5, textvariable=_retry_max, width=3,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"),
                 relief="flat", font=(_UI_FONT, 9)).pack(side="left", ipady=4, padx=(0, 4))
        tk.Label(r5, text="times, delay:", bg=self.C("panel"),
                 fg=self.C("muted"), font=(_UI_FONT, 9)).pack(side="left", padx=(0, 4))
        _retry_delay = tk.StringVar(value="5")
        tk.Entry(r5, textvariable=_retry_delay, width=4,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"),
                 relief="flat", font=(_UI_FONT, 9)).pack(side="left", ipady=4, padx=(0, 4))
        tk.Label(r5, text="min", bg=self.C("panel"),
                 fg=self.C("muted"), font=(_UI_FONT, 9)).pack(side="left")

        _form_err = tk.Label(form, text="", bg=self.C("panel"),
                             fg="#ef4444", font=(_UI_FONT, 8))
        _form_err.pack(anchor="w")

        def _reset_form():
            _edit_id[0] = None
            _name_var.set(""); _type_var.set("PCAP"); _path_var.set(""); _path_sel[0] = None
            _times_var.set("09:00"); _interval_mode.set(False); _interval_h.set("2")
            _repeat_var.set(True); _retry_max.set("2"); _retry_delay.set("5")
            for v in _day_vars.values(): v.set(True)
            _toggle_mode()
            _form_title_var.set("➕  Add New Scheduled Job"); _form_err.config(text="")

        def _populate_form(job):
            _edit_id[0] = job["id"]
            _name_var.set(job.get("name", "")); _type_var.set(job.get("type", "PCAP"))
            _path_var.set(job.get("path", "")); _path_sel[0] = None
            _times_var.set(", ".join(job.get("run_times", ["09:00"])))
            _interval_mode.set(job.get("interval_mode", False))
            _interval_h.set(str(job.get("interval_hours", 2)))
            _repeat_var.set(job.get("repeat", True))
            _retry_max.set(str(job.get("retry_max", 0)))
            _retry_delay.set(str(job.get("retry_delay_min", 5)))
            wd = job.get("weekdays", [])
            for i, v in _day_vars.items(): v.set(not wd or i in wd)
            _toggle_mode()
            _form_title_var.set(f"✏️  Editing: {job.get('name') or job['type']}")
            _form_err.config(text="")

        def _save_job():
            name = _name_var.get().strip()
            t    = _type_var.get()
            p    = _path_var.get().strip()
            if not p:
                _form_err.config(text="❌  Please select a folder or files."); return
            raw_times = [x.strip() for x in _times_var.get().split(",") if x.strip()]
            validated_times = []
            for rt in raw_times:
                try:
                    _DT.strptime(rt, "%H:%M"); validated_times.append(rt)
                except ValueError:
                    _form_err.config(text=f"❌  Invalid time: {rt} — use HH:MM"); return
            if not _interval_mode.get() and not validated_times:
                _form_err.config(text="❌  Enter at least one run time."); return
            try:
                i_hours = max(1, int(_interval_h.get()))
                r_max   = max(0, int(_retry_max.get()))
                r_delay = max(1, int(_retry_delay.get()))
            except ValueError:
                _form_err.config(text="❌  Invalid number in retry or interval."); return
            wd = [i for i, v in _day_vars.items() if v.get()]
            if not wd:
                _form_err.config(text="❌  Select at least one day."); return
            if len(wd) == 7: wd = []
            _form_err.config(text="")

            if _edit_id[0]:
                existing = next((j for j in self._sched_jobs if j["id"] == _edit_id[0]), None)
                if existing:
                    existing.update({
                        "name": name, "type": t, "path": p,
                        "run_times": validated_times, "interval_mode": _interval_mode.get(),
                        "interval_hours": i_hours, "weekdays": wd, "repeat": _repeat_var.get(),
                        "retry_max": r_max, "retry_delay_min": r_delay,
                        "status": "Waiting" if existing["status"] in ("Done","Error") else existing["status"],
                        "_fired_times": [],
                    })
                _reset_form(); _refresh_jobs(); self._sched_save()
                self._toast("✅  Job updated.", "success")
            else:
                job = {
                    "id": str(_uuid.uuid4()), "name": name, "type": t, "path": p,
                    "run_times": validated_times, "interval_mode": _interval_mode.get(),
                    "interval_hours": i_hours, "weekdays": wd, "repeat": _repeat_var.get(),
                    "retry_max": r_max, "retry_delay_min": r_delay,
                    "status": "Waiting", "last_run": None, "last_run_ts": "",
                    "last_result": "", "_fired_times": [], "_fired_date": "", "history": [],
                }
                self._sched_jobs.append(job)
                _reset_form(); _refresh_jobs(); self._sched_save(); self._sched_ensure_ticker()
                self._toast(f"✅  Job added — {name or t}", "success")

        btn_row = tk.Frame(form, bg=self.C("panel"))
        btn_row.pack(anchor="w", pady=(4, 0))
        tk.Button(btn_row, text="💾  Save Job", bg=self.C("success"), fg="white",
                  relief="flat", font=(_UI_FONT, 10, "bold"), cursor="hand2",
                  padx=14, pady=5, command=_save_job).pack(side="left", padx=(0, 8))
        tk.Button(btn_row, text="✕  Cancel Edit", bg="#374151", fg="white",
                  relief="flat", font=(_UI_FONT, 9), cursor="hand2",
                  padx=10, pady=5, command=_reset_form).pack(side="left")

        # ── Job list ───────────────────────────────────────────────
        canvas_outer, sf = self._scrollable(main)
        jc = tk.Frame(sf, bg=self.C("bg"))
        jc.pack(fill="x", padx=50, pady=(0, 20))

        jtb = tk.Frame(jc, bg=self.C("bg"))
        jtb.pack(fill="x", pady=(0, 6))
        tk.Label(jtb, text="Scheduled Jobs", bg=self.C("bg"),
                 fg=self.C("muted"), font=(_UI_FONT, 10, "bold")).pack(side="left")

        def _export_schedule():
            import csv as _csv
            import tkinter.filedialog as _fd2
            out = _fd2.asksaveasfilename(defaultextension=".csv",
                                          filetypes=[("CSV","*.csv")], title="Export Schedule")
            if not out: return
            try:
                with open(out, "w", newline="", encoding="utf-8") as f:
                    w = _csv.writer(f)
                    w.writerow(["Name","Type","Path","Run Times","Interval Mode",
                                "Interval Hours","Weekdays","Repeat","Retry Max",
                                "Retry Delay","Status","Last Run","Last Result"])
                    wd_names = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
                    for j in self._sched_jobs:
                        wdn = ",".join(wd_names[i] for i in j.get("weekdays",[])) or "All"
                        w.writerow([j.get("name",""), j["type"], j["path"],
                                    ",".join(j.get("run_times",[])), j.get("interval_mode",False),
                                    j.get("interval_hours",1), wdn, j.get("repeat",True),
                                    j.get("retry_max",0), j.get("retry_delay_min",5),
                                    j.get("status",""), j.get("last_run",""), j.get("last_result","")])
                    w.writerow([]); w.writerow(["--- Execution History ---"])
                    w.writerow(["Job Name","Timestamp","Result","Duration"])
                    for j in self._sched_jobs:
                        for h in j.get("history",[]):
                            w.writerow([j.get("name") or j["type"],
                                        h.get("ts",""), h.get("result",""), h.get("duration","")])
                self._toast(f"✅  Exported to {os.path.basename(out)}", "success")
            except Exception as ex:
                self._toast(f"❌  Export failed: {ex}", "error")

        def _stop_all():
            running_count = 0
            for j in self._sched_jobs:
                if j.get("status") == "Running":
                    running_count += 1  # can't stop mid-upload, skip
                elif j.get("status") not in ("Disabled", "Done"):
                    j["status"] = "Disabled"
            if getattr(self, "_sched_ticker", None):
                try: self.after_cancel(self._sched_ticker)
                except Exception: pass
                self._sched_ticker = None
            _refresh_jobs(); self._sched_save()
            if running_count:
                self._toast(f"⏹  Jobs disabled. {running_count} still uploading — will finish.", "warn")
            else:
                self._toast("⏹  All jobs disabled.", "info")

        def _clear_done():
            self._sched_jobs[:] = [j for j in self._sched_jobs
                                   if j.get("status") not in ("Done", "Error")]
            _refresh_jobs(); self._sched_save(); self._toast("🗑  Cleared completed/failed jobs.", "info")

        for lbl, cmd, bg in [
            ("⬇  Export", _export_schedule, "#0891b2"),
            ("⏹  Stop All", _stop_all, "#ef4444"),
            ("🗑  Clear Done", _clear_done, "#374151"),
        ]:
            tk.Button(jtb, text=lbl, bg=bg, fg="white", relief="flat",
                      font=(_UI_FONT, 8, "bold"), cursor="hand2",
                      padx=8, pady=3, command=cmd).pack(side="right", padx=(4,0))

        _jobs_frame = tk.Frame(jc, bg=self.C("bg"))
        _jobs_frame.pack(fill="x")

        ST_COLOR = {
            "Waiting":   "#f59e0b", "Scheduled": "#22c55e", "Running": "#3b82f6",
            "Retrying":  "#f59e0b", "Done":      "#94a3b8", "Error":   "#ef4444",
            "Disabled":  "#475569",
        }

        def _sched_tooltip(widget, text):
            _tip = [None]
            def _show(e):
                _tip[0] = tk.Toplevel(widget)
                _tip[0].wm_overrideredirect(True)
                _tip[0].wm_geometry(f"+{e.x_root+8}+{e.y_root+28}")
                tk.Label(_tip[0], text=text, bg="#1e293b", fg="white",
                         font=(_UI_FONT, 8), relief="solid",
                         padx=6, pady=3, borderwidth=1).pack()
            def _hide(e):
                if _tip[0]:
                    try: _tip[0].destroy()
                    except Exception: pass
                    _tip[0] = None
            widget.bind("<Enter>", _show)
            widget.bind("<Leave>", _hide)

        def _refresh_jobs():
            for w in _jobs_frame.winfo_children():
                w.destroy()
            if not self._sched_jobs:
                tk.Label(_jobs_frame, text="No jobs yet. Add one above.",
                         bg=self.C("bg"), fg=self.C("dim"),
                         font=(_UI_FONT, 9, "italic")).pack(anchor="w")
                return

            from datetime import datetime as _DT3
            now2 = _DT3.now()

            for idx, job in enumerate(self._sched_jobs):
                st     = job.get("status", "Waiting")
                row_bg = self.C("input_bg") if idx % 2 == 0 else self.C("panel")
                rf = tk.Frame(_jobs_frame, bg=row_bg,
                              highlightbackground=self.C("border"), highlightthickness=1)
                rf.pack(fill="x", pady=1)
                mr = tk.Frame(rf, bg=row_bg)
                mr.pack(fill="x", padx=8, pady=5)

                sc = ST_COLOR.get(st, self.C("muted"))
                tk.Label(mr, text="●", bg=row_bg, fg=sc,
                         font=(_UI_FONT, 10, "bold")).pack(side="left", padx=(0, 6))
                lbl_txt = job.get("name") or job["type"]
                tk.Label(mr, text=lbl_txt, bg=row_bg, fg=self.C("card_title"),
                         font=(_UI_FONT, 9, "bold"), width=18, anchor="w").pack(side="left")
                tk.Label(mr, text=job["type"], bg=row_bg, fg=self.C("muted"),
                         font=(_UI_FONT, 8), width=10, anchor="w").pack(side="left")
                times_txt = (f"Every {job.get('interval_hours',1)}h"
                             if job.get("interval_mode")
                             else ", ".join(job.get("run_times", [])))
                tk.Label(mr, text=times_txt, bg=row_bg, fg=self.C("text"),
                         font=(_UI_FONT, 9), width=14, anchor="w").pack(side="left")
                wd = job.get("weekdays", [])
                wd_names = ["Mo","Tu","We","Th","Fr","Sa","Su"]
                days_txt = "All" if not wd else "".join(wd_names[i] for i in sorted(wd))
                tk.Label(mr, text=days_txt, bg=row_bg, fg=self.C("muted"),
                         font=(_UI_FONT, 8), width=8, anchor="w").pack(side="left")
                tk.Label(mr, text=st, bg=row_bg, fg=sc,
                         font=(_UI_FONT, 9, "bold"), width=10, anchor="w").pack(side="left")
                res = job.get("last_result", "—")
                res_c = "#22c55e" if "✅" in res else ("#ef4444" if "❌" in res else self.C("dim"))
                tk.Label(mr, text=res, bg=row_bg, fg=res_c,
                         font=(_UI_FONT, 8), width=14, anchor="w").pack(side="left")

                # Countdown
                if st in ("Waiting", "Scheduled", "Retrying") and not job.get("interval_mode"):
                    try:
                        from datetime import timedelta as _td2
                        best = None
                        for rt in job.get("run_times", []):
                            h2, m2 = map(int, rt.split(":"))
                            target = now2.replace(hour=h2, minute=m2, second=0, microsecond=0)
                            if target <= now2: target += _td2(days=1)
                            diff = int((target - now2).total_seconds())
                            if best is None or diff < best[0]: best = (diff, rt)
                        if best:
                            d2, rt = best
                            hh, rm = divmod(d2, 3600); mm2, ss = divmod(rm, 60)
                            cd_txt = f"next {rt} in {hh}h{mm2}m" if hh else f"next {rt} in {mm2}m{ss}s"
                            tk.Label(mr, text=cd_txt, bg=row_bg, fg="#f59e0b",
                                     font=(_UI_FONT, 8, "italic")).pack(side="left", padx=(6,0))
                    except Exception:
                        pass

                ab = tk.Frame(mr, bg=row_bg)
                ab.pack(side="right")

                def _run_now(j=job):
                    if j.get("status") == "Running":
                        self._toast("Job is already running.", "warn"); return
                    j["status"] = "Running"
                    _refresh_jobs()
                    threading.Thread(target=self._sched_execute_job, args=(j,), daemon=True).start()

                def _edit(j=job):
                    _populate_form(j)

                def _duplicate(j=job):
                    import copy as _copy, uuid as _uuid3
                    nj = _copy.deepcopy(j)
                    nj["id"] = str(_uuid3.uuid4())
                    nj["name"] = (j.get("name") or j["type"]) + " (copy)"
                    nj["status"] = "Waiting"; nj["last_run"] = None
                    nj["last_run_ts"] = ""; nj["last_result"] = ""
                    nj["history"] = []; nj["_fired_times"] = []
                    self._sched_jobs.append(nj)
                    _refresh_jobs(); self._sched_save()
                    self._toast("📋  Job duplicated.", "success")

                def _toggle(j=job):
                    if j.get("status") == "Running":
                        self._toast("Cannot pause a running job — wait for it to finish.", "warn"); return
                    j["status"] = "Waiting" if j.get("status") == "Disabled" else "Disabled"
                    if j["status"] == "Waiting":
                        self._sched_ensure_ticker()  # restart ticker if Stop All had cancelled it
                    _refresh_jobs(); self._sched_save()

                def _delete(j=job):
                    if j.get("status") == "Running":
                        self._toast("Cannot delete a running job — wait for it to finish.", "warn"); return
                    if j in self._sched_jobs: self._sched_jobs.remove(j)
                    _refresh_jobs(); self._sched_save()

                pause_lbl     = "▶▶" if st == "Disabled" else "⏸"
                pause_tooltip  = "Enable Job — resume scheduling" if st == "Disabled" else "Pause Job — suspend scheduling"
                for btxt, bcmd, bbg, bttip in [
                    ("▶",      _run_now,   "#0891b2", "Run Now — trigger this job immediately"),
                    ("✏",      _edit,      "#7c3aed", "Edit Job — modify settings"),
                    ("⧉",      _duplicate, "#374151", "Duplicate — create a copy of this job"),
                    (pause_lbl, _toggle,   "#64748b", pause_tooltip),
                    ("✕",      _delete,    "#ef4444", "Delete Job — remove permanently"),
                ]:
                    btn = tk.Button(ab, text=btxt, bg=bbg, fg="white", relief="flat",
                                    font=(_UI_FONT, 9, "bold"), cursor="hand2",
                                    padx=5, pady=2, command=bcmd)
                    btn.pack(side="left", padx=1)
                    _sched_tooltip(btn, bttip)

                # History expander
                hist = job.get("history", [])
                if hist:
                    _expanded = [False]
                    hist_frame = tk.Frame(rf, bg=row_bg)

                    def _toggle_hist(hf=hist_frame, j=job, ex=_expanded):
                        ex[0] = not ex[0]
                        if ex[0]:
                            hf.pack(fill="x", padx=24, pady=(0, 6))
                            for w2 in hf.winfo_children(): w2.destroy()
                            for h in j.get("history", []):
                                hr = tk.Frame(hf, bg=row_bg)
                                hr.pack(fill="x")
                                rc2 = "#22c55e" if "✅" in h.get("result","") else "#ef4444"
                                tk.Label(hr, text=h.get("ts",""), bg=row_bg,
                                         fg=self.C("dim"), font=("Consolas", 8),
                                         width=20, anchor="w").pack(side="left")
                                tk.Label(hr, text=h.get("result",""), bg=row_bg,
                                         fg=rc2, font=(_UI_FONT, 8, "bold"),
                                         width=10, anchor="w").pack(side="left")
                                tk.Label(hr, text=h.get("duration",""), bg=row_bg,
                                         fg=self.C("muted"), font=(_UI_FONT, 8),
                                         width=8, anchor="w").pack(side="left")
                        else:
                            hf.pack_forget()

                    tk.Button(rf, text=f"  📋 {len(hist)} run(s)  ",
                              bg=row_bg, fg=self.C("dim"), relief="flat",
                              cursor="hand2", font=(_UI_FONT, 7),
                              command=_toggle_hist).pack(anchor="w", padx=32, pady=(0, 4))

        _refresh_jobs()
        self._sched_page_refresh = _refresh_jobs

        _alive = [True]
        def _auto_refresh():
            if not _alive[0]: return
            try:
                if _jobs_frame.winfo_exists():
                    _refresh_jobs()
                    _jobs_frame.after(5000, _auto_refresh)
                else:
                    _alive[0] = False
                    self._sched_page_refresh = None
            except Exception:
                _alive[0] = False

        _jobs_frame.after(5000, _auto_refresh)
        self._sched_ensure_ticker()

    # ──────────────────────────────────────────────────────────────
    # HOME — UPLOAD
    # ──────────────────────────────────────────────────────────────
    def show_home(self):
        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Upload Data page")
        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(25, 5))
        tk.Label(hdr, text="📤  Upload Data", bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", padx=10, pady=4,
                  command=self.show_first_page).pack(side="right")

        # Schedule buttons row
        sched_row = tk.Frame(main, bg=self.C("bg"))
        sched_row.pack(fill="x", padx=50, pady=(0, 4))
        tk.Button(sched_row, text="🕐  Schedule Upload",
                  bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 8, "bold"),
                  highlightbackground=self.C("border"), highlightthickness=1,
                  cursor="hand2", padx=10, pady=3,
                  command=self.show_schedule_page).pack(side="left", padx=(0, 6))
        tk.Frame(main, bg=self.C("border"), height=1).pack(fill="x", padx=50, pady=(5, 10))

        # ── Scrollable so Advanced Tools are always reachable ──────
        canvas, sf = self._scrollable(main)
        container = tk.Frame(sf, bg=self.C("bg"))
        container.pack(fill="x", padx=50, pady=(5, 20))

        # ── Standard upload cards ──────────────────────────────────
        self._section_label(container, "📤  Standard Upload", pady=(0, 8))
        upload_items = [
            ("📡", "PCAP Data",
             "Network capture files",
             ["Select a folder", "Uploads recursively", "All .pcap files"],
             self.upload_pcap),
            ("📍", "LBS / LUDR / SMS Data",
             "Location, LUDR & SMS records",
             ["CSV or TXT files", "Multi-file select", "Direct to server"],
             self.upload_ludr),
            ("📞", "Voice Call Data",
             "Single call — Hi2 & Hi3",
             ["Folder with Hi2/ & Hi3/", "Auto upload sequence", "Hi3 → wait → Hi2"],
             self.upload_voice),
            ("📞", "Bulk Voice Upload",
             "Multiple calls sequentially",
             ["Parent folder of calls", "One-by-one upload", "Backend ACK wait"],
             self._bulk_voice_upload_picker),
        ]
        upload_cards_frame = tk.Frame(container, bg=self.C("bg"))
        upload_cards_frame.pack(fill="x")
        for col, (icon, title, subtitle, bullets, cmd) in enumerate(upload_items):
            self._feature_card(upload_cards_frame, col, icon, title, subtitle, bullets, cmd)

        # ── HI3 row (full-width single card) ──────────────────────
        hi3_row = tk.Frame(container, bg=self.C("bg"))
        hi3_row.pack(fill="x", pady=(8, 0))
        hi3_row.columnconfigure(0, weight=1)
        hi3_outer = tk.Frame(hi3_row, highlightbackground=self.C("border"),
                             highlightthickness=1, bg=self.C("border"))
        hi3_outer.grid(row=0, column=0, padx=12, pady=4, sticky="nsew")
        hi3_card = tk.Frame(hi3_outer, bg=self.C("panel"), cursor="hand2")
        hi3_card.pack(fill="both", expand=True)
        tk.Frame(hi3_card, bg=self.C("primary"), height=4).pack(fill="x")
        hi3_body = tk.Frame(hi3_card, bg=self.C("panel"))
        hi3_body.pack(fill="x", padx=20, pady=(12, 14))
        left = tk.Frame(hi3_body, bg=self.C("panel"))
        left.pack(side="left", fill="x", expand=True)
        ic2 = tk.Canvas(left, width=40, height=40, bg=self.C("panel"), highlightthickness=0)
        ic2.pack(side="left", padx=(0, 14))
        ic2.create_oval(2, 2, 38, 38, fill=self.C("input_bg"), outline="")
        ic2.create_text(20, 20, text="📡", font=(_UI_FONT, 16), fill=self.C("card_title"))
        txt = tk.Frame(left, bg=self.C("panel"))
        txt.pack(side="left", fill="x", expand=True)
        tk.Label(txt, text="HI3 Only Upload", bg=self.C("panel"),
                 fg=self.C("card_title"), font=(_UI_FONT, 12, "bold"), anchor="w").pack(anchor="w")
        tk.Label(txt, text="Upload HI3 folder to /etc/vsf/input/<folder-name>/ on server",
                 bg=self.C("panel"), fg=self.C("muted"), font=(_UI_FONT, 9), anchor="w").pack(anchor="w")
        tk.Button(hi3_body, text="Open →", bg=self.C("primary"), fg="white",
                  relief="flat", font=(_UI_FONT, 9, "bold"), cursor="hand2",
                  padx=14, pady=6, command=self.upload_hi3_only).pack(side="right", padx=(12, 0))
        for w in (hi3_card, hi3_body, left, txt):
            w.bind("<Button-1>", lambda e: self.upload_hi3_only())

        # ── Upload History — inline table ──────────────────────────
        self._section_label(container, "📋  Recent Upload History", pady=(24, 8))

        import csv as _csv, os as _os

        HIST_FILE = _os.path.join(
            _os.path.dirname(_os.path.abspath(__file__)),
            "comtrail_upload_history.csv")

        # ── History search bar ─────────────────────────────────────
        _hist_search_var = tk.StringVar()
        _hsbar = tk.Frame(container, bg=self.C("bg"))
        _hsbar.pack(fill="x", pady=(0, 4))
        tk.Label(_hsbar, text="🔍", bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT, 10)).pack(side="left")
        _hse = tk.Entry(_hsbar, textvariable=_hist_search_var, width=32,
                        bg=self.C("input_bg"), fg=self.C("text"),
                        insertbackground=self.C("text"),
                        relief="flat", font=(_UI_FONT, 9))
        _hse.pack(side="left", ipady=4, padx=(4, 0))
        tk.Button(_hsbar, text="✕", bg=self.C("bg"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 8), cursor="hand2",
                  command=lambda: _hist_search_var.set("")).pack(side="left", padx=2)
        _hist_search_var.trace_add("write", lambda *_: _load_hist())

        hist_card = tk.Frame(container,
                             bg=self.C("panel"),
                             highlightbackground=self.C("border"),
                             highlightthickness=1)
        hist_card.pack(fill="x", pady=(0, 10))

        # ── Card top bar ───────────────────────────────────────────
        top_bar = tk.Frame(hist_card, bg=self.C("primary"), height=36)
        top_bar.pack(fill="x")
        top_bar.pack_propagate(False)
        tk.Frame(top_bar, bg="#06b6d4", height=2).pack(fill="x", side="top")

        top_inner = tk.Frame(top_bar, bg=self.C("primary"))
        top_inner.pack(fill="x", padx=12, expand=True)
        tk.Label(top_inner, text="📋  Last 10 Uploads & Generates",
                 bg=self.C("primary"), fg="white",
                 font=(_UI_FONT, 9, "bold")).pack(side="left", pady=7)

        hist_status = tk.StringVar(value="")
        tk.Label(top_inner, textvariable=hist_status,
                 bg=self.C("primary"), fg="#a5f3fc",
                 font=(_UI_FONT, 8)).pack(side="right", pady=7)

        # ── Column headers ─────────────────────────────────────────
        COL_DEFS = [
            ("Date / Time",  160, "center"),
            ("Action",       130, "w"),
            ("Files",         50, "center"),
            ("Server",       130, "w"),
            ("Status",        80, "center"),
            ("Note",         220, "w"),
        ]
        _hdr_bg = "#0c4a5a" if self._theme == "dark" else "#0891b2"
        hdr_row = tk.Frame(hist_card, bg=_hdr_bg)
        hdr_row.pack(fill="x")
        for col_name, col_w, col_anchor in COL_DEFS:
            tk.Label(hdr_row, text=col_name,
                     bg=_hdr_bg, fg="white",
                     font=(_UI_FONT, 8, "bold"),
                     width=col_w // 8,
                     anchor=col_anchor).pack(
                side="left", padx=1, pady=5, ipadx=4)

        # ── Table rows container ───────────────────────────────────
        rows_frame = tk.Frame(hist_card, bg=self.C("panel"))
        rows_frame.pack(fill="x")

        def _load_hist():
            for w in rows_frame.winfo_children():
                w.destroy()
            rows = []
            if _os.path.isfile(HIST_FILE):
                try:
                    with open(HIST_FILE, "r",
                              encoding="utf-8-sig", newline="") as f:
                        rows = list(_csv.DictReader(f))
                    rows = list(reversed(rows))
                except Exception:
                    pass
            # Apply search filter
            _sq = _hist_search_var.get().strip().lower()
            if _sq:
                rows = [r for r in rows if any(
                    _sq in str(v).lower() for v in r.values())]
            rows = rows[:10]

            if not rows:
                tk.Label(rows_frame,
                         text="No history yet — uploads and generates will appear here.",
                         bg=self.C("panel"),
                         fg=self.C("dim"),
                         font=(_UI_FONT, 9, "italic")).pack(
                    pady=16, padx=16)
                hist_status.set("No records")
                return

            hist_status.set(f"{len(rows)} recent record(s)")

            for i, row in enumerate(rows):
                status = row.get("status", "")
                # Row alternating colours
                if self._theme == "dark":
                    row_bg = "#1a2133" if i % 2 == 0 else "#161b22"
                else:
                    row_bg = "#f8faff" if i % 2 == 0 else "#ffffff"

                # Status colours
                if "✅" in status or "OK" in status.upper():
                    st_fg, st_bg = "#22c55e", \
                        ("#14532d" if self._theme=="dark" else "#dcfce7")
                elif "❌" in status or "FAIL" in status.upper():
                    st_fg, st_bg = "#ef4444", \
                        ("#7f1d1d" if self._theme=="dark" else "#fee2e2")
                else:
                    st_fg, st_bg = self.C("muted"), row_bg

                fr = tk.Frame(rows_frame, bg=row_bg)
                fr.pack(fill="x")

                # Left accent stripe by action type
                action = row.get("action", "")
                accent = ("#06b6d4" if "PCAP" in action
                          else "#10b981" if "LBS" in action
                          or "SMS" in action
                          else "#8b5cf6" if "Voice" in action
                          else "#f59e0b" if "Generate" in action
                          else "#64748b")
                tk.Frame(fr, bg=accent, width=3).pack(
                    side="left", fill="y")

                cells = [
                    (row.get("ts",     "-"), 160, "center",
                     self.C("muted"),  ("Consolas", 8)),
                    (action,             130, "w",
                     self.C("card_title"), (_UI_FONT, 8, "bold")),
                    (str(row.get("files", "-")), 50, "center",
                     self.C("card_title"), (_UI_FONT, 8)),
                    (row.get("server", "-"), 130, "w",
                     self.C("muted"),  (_UI_FONT, 8)),
                    (status[:12],         80, "center",
                     st_fg, (_UI_FONT, 8, "bold")),
                    (row.get("note", "")[:35], 220, "w",
                     self.C("dim"),    (_UI_FONT, 8)),
                ]
                for val, w, anchor, fg, fnt in cells:
                    cell_bg = st_bg if val == status[:12] else row_bg
                    tk.Label(fr, text=val,
                             bg=cell_bg, fg=fg,
                             font=fnt,
                             width=w // 8,
                             anchor=anchor).pack(
                        side="left", padx=1, pady=4, ipadx=4)

                # Re-run button — map action → upload function
                _rerun_map = {
                    "PCAP":     self.upload_pcap,
                    "LBS":      self.upload_ludr,
                    "Voice":    self.upload_voice,
                    "Generate": self.show_generate_data,
                }
                _rerun_fn = next(
                    (fn for key, fn in _rerun_map.items() if key in action),
                    None)
                if _rerun_fn:
                    tk.Button(fr, text="↩ Re-run",
                              bg="#0891b2" if self._theme=="dark" else "#0e7490",
                              fg="white", relief="flat",
                              font=(_UI_FONT, 7, "bold"), cursor="hand2",
                              padx=6, pady=1,
                              command=_rerun_fn).pack(
                        side="right", padx=6, pady=3)

                # Bottom separator
                sep_col = "#1e293b" if self._theme=="dark" else "#e2e8f0"
                tk.Frame(rows_frame, bg=sep_col,
                         height=1).pack(fill="x")

        _load_hist()

        # ── Footer: refresh + open full history ───────────────────
        foot = tk.Frame(hist_card, bg=self.C("input_bg"))
        foot.pack(fill="x")
        tk.Frame(foot, bg=self.C("border"), height=1).pack(fill="x")
        btn_fr = tk.Frame(foot, bg=self.C("input_bg"))
        btn_fr.pack(anchor="e", padx=12, pady=6)
        tk.Button(btn_fr, text="🔄  Refresh",
                  bg=self.C("primary"), fg="white",
                  activebackground="#0e7490", activeforeground="white",
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2",
                  command=_load_hist).pack(
            side="left", ipadx=10, ipady=4, padx=(0, 8))
        tk.Button(btn_fr, text="📊  View Full History",
                  bg="#374151", fg="white",
                  activebackground="#4b5563", activeforeground="white",
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2",
                  command=self.show_upload_history).pack(
            side="left", ipadx=10, ipady=4)



    def _upload_card(self, parent, s):
        """Upload card — full width, 3D accent bar, entire card clickable."""
        cfg = self.cfg.get(s["key"], {})
        ip  = cfg.get("ip", "")
        pwd = cfg.get("pwd", "")

        # Outer frame — border gives the raised 3D look
        outer = tk.Frame(parent,
                         highlightbackground=self.C("border"),
                         highlightthickness=1,
                         bg=self.C("border"))
        outer.pack(fill="x", pady=5)

        card = tk.Frame(outer, bg=self.C("panel"), cursor="hand2")
        card.pack(fill="x")

        # Top accent bar (4px blue stripe = depth/3D feel)
        accent = tk.Frame(card, bg=self.C("primary"), height=4)
        accent.pack(fill="x")

        body = tk.Frame(card, bg=self.C("panel"))
        body.pack(fill="x", padx=22, pady=16)

        # ── Left: icon circle ──────────────────────────────────
        left = tk.Frame(body, bg=self.C("panel"))
        left.pack(side="left", padx=(0, 20))

        ic = tk.Canvas(left, width=56, height=56,
                       bg=self.C("panel"), highlightthickness=0)
        ic.pack()
        ic.create_oval(2, 2, 54, 54, fill=self.C("input_bg"), outline="")
        ic.create_text(28, 28, text=s["icon"],
                       font=(_UI_FONT, 24), fill=self.C("card_title"))

        # ── Middle: title / desc / hint ────────────────────────
        mid = tk.Frame(body, bg=self.C("panel"))
        mid.pack(side="left", fill="both", expand=True)

        tk.Label(mid, text=s["title"],
                 bg=self.C("panel"), fg=self.C("card_title"),
                 font=(_UI_FONT, 15, "bold"),
                 anchor="w").pack(anchor="w")

        tk.Label(mid, text=s["desc"],
                 bg=self.C("panel"), fg=self.C("subtle"),
                 font=(_UI_FONT, 10)).pack(anchor="w", pady=(3, 6))

        # Pill badge for file hint
        hint_lbl = tk.Label(mid,
                            text=f"  📂  {s['file_hint']}  ",
                            bg=self.C("input_bg"), fg=self.C("muted"),
                            font=(_UI_FONT, 9), padx=6, pady=3)
        hint_lbl.pack(anchor="w")

        # ── Right: status + IP ─────────────────────────────────
        right = tk.Frame(body, bg=self.C("panel"))
        right.pack(side="right", padx=(24, 0), anchor="center")

        dot_row = tk.Frame(right, bg=self.C("panel"))
        dot_row.pack(anchor="e")
        self._make_status_row(dot_row, s["key"] + "_home", ip, pwd)

        tk.Label(right, text=ip or "Not configured",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 9)).pack(anchor="e", pady=(4, 0))

        # ── Hover & click ──────────────────────────────────────
        def _collect():
            ws = [outer, card, body, left, mid, right, dot_row]
            for w in ws:
                try:
                    for c in w.winfo_children():
                        ws.append(c)
                except: pass
            return ws

        def _on(e=None):
            accent.config(bg=self.C("success"))
            outer.config(highlightbackground=self.C("success"))
            for w in _collect():
                try: w.config(bg=self.C("input_bg"))
                except: pass
            ic.config(bg=self.C("input_bg"))
            hint_lbl.config(bg=self.C("border"))

        def _off(e=None):
            accent.config(bg=self.C("primary"))
            outer.config(highlightbackground=self.C("border"))
            for w in _collect():
                try: w.config(bg=self.C("panel"))
                except: pass
            ic.config(bg=self.C("panel"))
            hint_lbl.config(bg=self.C("input_bg"))

        def _click(e=None): s["cmd"]()

        for w in _collect():
            try:
                w.bind("<Enter>",    _on)
                w.bind("<Leave>",    _off)
                w.bind("<Button-1>", _click)
            except: pass

    # ──────────────────────────────────────────────────────────────
    # UPLOAD LOGIC
    # ──────────────────────────────────────────────────────────────
    def upload_hi3_only(self):
        """Upload HI3 folder (WAV + CRI) to /etc/vsf/input/<folder-name>/ on server."""
        import tkinter.filedialog as _fd

        self.clear_main()
        main = self.active_frame
        LOG.log("Upload", "Opened HI3 Only Upload page")

        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(25, 5))
        tk.Label(hdr, text="📡  HI3 Only Upload",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", padx=10, pady=4,
                  command=self.show_home).pack(side="right")
        tk.Frame(main, bg=self.C("border"), height=1).pack(fill="x", padx=50, pady=(4, 14))

        canvas, sf = self._scrollable(main)
        container = tk.Frame(sf, bg=self.C("bg"))
        container.pack(fill="x", padx=50, pady=(5, 20))

        # ── Info card ──────────────────────────────────────────────────
        info = tk.Frame(container, bg=self.C("panel"),
                        highlightbackground=self.C("border"), highlightthickness=1)
        info.pack(fill="x", pady=(0, 14))
        tk.Frame(info, bg=self.C("primary"), height=3).pack(fill="x")
        ib = tk.Frame(info, bg=self.C("panel"))
        ib.pack(fill="x", padx=16, pady=10)
        tk.Label(ib, text="Upload Path on Server:", bg=self.C("panel"),
                 fg=self.C("muted"), font=(_UI_FONT, 8, "bold")).pack(anchor="w")
        tk.Label(ib, text="/etc/vsf/input/<folder-name>/", bg=self.C("panel"),
                 fg=self.C("card_title"), font=("Consolas", 11, "bold")).pack(anchor="w")
        tk.Label(ib, text="Add one or more HI3 folders. Each folder is copied as "
                          "/etc/vsf/input/<folder-name>/ on the Voice server, "
                          "preserving the folder name and all files inside it.",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 9), wraplength=600).pack(anchor="w", pady=(4, 0))

        # ── Multi-folder list ──────────────────────────────────────────
        pick_frame = tk.Frame(container, bg=self.C("panel"),
                              highlightbackground=self.C("border"), highlightthickness=1)
        pick_frame.pack(fill="x", pady=(0, 14))
        tk.Frame(pick_frame, bg="#06b6d4", height=2).pack(fill="x")
        pf2 = tk.Frame(pick_frame, bg=self.C("panel"))
        pf2.pack(fill="x", padx=16, pady=10)

        tk.Label(pf2, text="HI3 Folders:", bg=self.C("panel"),
                 fg=self.C("muted"), font=(_UI_FONT, 9, "bold")).pack(anchor="w", pady=(0, 4))

        list_frame = tk.Frame(pf2, bg=self.C("input_bg"),
                              highlightbackground=self.C("border"), highlightthickness=1)
        list_frame.pack(fill="x", pady=(0, 6))
        folder_listbox = tk.Listbox(list_frame, height=5,
                                    bg=self.C("input_bg"), fg=self.C("text"),
                                    selectbackground=self.C("primary"),
                                    font=(_UI_FONT, 9), relief="flat",
                                    activestyle="none", selectmode="extended")
        list_vsb = ttk.Scrollbar(list_frame, orient="vertical",   command=folder_listbox.yview)
        list_hsb = ttk.Scrollbar(list_frame, orient="horizontal", command=folder_listbox.xview)
        folder_listbox.configure(yscrollcommand=list_vsb.set, xscrollcommand=list_hsb.set)
        list_vsb.pack(side="right",  fill="y")
        list_hsb.pack(side="bottom", fill="x")
        folder_listbox.pack(fill="both", expand=True)

        _folders = []  # list of absolute folder paths

        def _add_folder():
            d = _fd.askdirectory(title="Select HI3 folder")
            if d and d not in _folders:
                _folders.append(d)
                folder_listbox.insert("end", d)

        def _remove_selected():
            for i in reversed(folder_listbox.curselection()):
                folder_listbox.delete(i)
                _folders.pop(i)

        btn_row = tk.Frame(pf2, bg=self.C("panel"))
        btn_row.pack(anchor="w")
        tk.Button(btn_row, text="➕  Add Folder", bg=self.C("primary"), fg="white",
                  relief="flat", font=(_UI_FONT, 9, "bold"), cursor="hand2",
                  padx=10, pady=4, command=_add_folder).pack(side="left", padx=(0, 8))
        tk.Button(btn_row, text="🗑  Remove Selected", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9), cursor="hand2",
                  padx=10, pady=4, command=_remove_selected).pack(side="left")

        # ── Progress / log area ────────────────────────────────────────
        log_card = tk.Frame(container, bg=self.C("panel"),
                            highlightbackground=self.C("border"), highlightthickness=1)
        log_card.pack(fill="x", pady=(0, 14))
        tk.Frame(log_card, bg=self.C("primary"), height=2).pack(fill="x")
        log_inner = tk.Frame(log_card, bg=self.C("panel"))
        log_inner.pack(fill="x", padx=12, pady=8)
        tk.Label(log_inner, text="Upload Log", bg=self.C("panel"),
                 fg=self.C("muted"), font=(_UI_FONT, 9, "bold")).pack(anchor="w")
        log_txt = tk.Text(log_inner, height=12, bg=self.C("input_bg"),
                          fg=self.C("text"), font=("Consolas", 9),
                          relief="flat", state="disabled",
                          insertbackground=self.C("text"))
        log_txt.pack(fill="x", pady=(4, 0))

        status_var = tk.StringVar(value="Ready.")
        tk.Label(container, textvariable=status_var, bg=self.C("bg"),
                 fg=self.C("muted"), font=(_UI_FONT, 9)).pack(anchor="w", pady=(0, 8))

        def _log(msg):
            log_txt.config(state="normal")
            log_txt.insert("end", msg + "\n")
            log_txt.see("end")
            log_txt.config(state="disabled")

        def _do_upload():
            if not _folders:
                self.popup("Error", "Please add at least one HI3 folder.", "error")
                return
            ip  = self.cfg.get("voice", {}).get("ip",  "")
            pwd = self.cfg.get("voice", {}).get("pwd", "")
            if not ip or not pwd:
                self.popup("Error", "Voice server not configured.\nGo to ⚙️ Settings.", "error")
                return
            upload_btn.config(state="disabled")
            total_folders = len(_folders)
            status_var.set(f"Uploading {total_folders} folder(s) to Voice server…")
            LOG.log("HI3 Upload", f"Starting upload of {total_folders} folder(s) to {ip}")

            def _run():
                ssh_t = sftp_t = None
                total_ok = total_fail = 0
                try:
                    ssh_t = paramiko.SSHClient()
                    ssh_t.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    ssh_t.connect(ip, username="root", password=pwd,
                                  timeout=20, banner_timeout=20, auth_timeout=20)
                    sftp_t = ssh_t.open_sftp()

                    for fidx, local_dir in enumerate(_folders, 1):
                        folder_name = os.path.basename(local_dir.rstrip("/\\"))
                        remote_base = f"/etc/vsf/input/{folder_name}"
                        self.after(0, lambda fn=folder_name, fi=fidx, rb=remote_base:
                                   _log(f"\n[{fi}/{total_folders}] Uploading folder: {fn} → {rb}"))
                        LOG.log("HI3 Upload", f"[{fidx}/{total_folders}] {local_dir} → {remote_base}")

                        all_files = [os.path.join(_r, _f)
                                     for _r, _, _fs in os.walk(local_dir) for _f in _fs]
                        try:
                            mkdirs_sftp(sftp_t, remote_base)
                        except Exception as e:
                            self.after(0, lambda err=e: _log(f"  ❌  Cannot create remote dir: {err}"))

                        ok = fail = 0
                        for local_fp in all_files:
                            rel       = os.path.relpath(local_fp, local_dir).replace(os.sep, "/")
                            remote_fp = f"{remote_base}/{rel}"
                            rdir      = remote_fp.rsplit("/", 1)[0]
                            try:
                                mkdirs_sftp(sftp_t, rdir)
                            except Exception:
                                pass
                            try:
                                sftp_t.put(local_fp, remote_fp)
                                self.after(0, lambda f=rel: _log(f"  ✅  {f}"))
                                ok += 1
                            except Exception as e:
                                self.after(0, lambda f=rel, err=e: _log(f"  ❌  {f} — {err}"))
                                fail += 1
                        total_ok   += ok
                        total_fail += fail
                        self.after(0, lambda fn=folder_name, o=ok, f=fail:
                                   _log(f"  → {fn}: {o} uploaded, {f} failed"))
                        LOG.log("HI3 Upload", f"  {folder_name}: {ok} ok, {fail} fail")

                except Exception as e:
                    self.after(0, lambda err=e: _log(f"\n❌  Connection error: {err}"))
                    LOG.log("HI3 Upload", f"Connection error: {e}", "ERROR")
                    total_fail += 1
                finally:
                    for obj in (sftp_t, ssh_t):
                        try:
                            if obj: obj.close()
                        except Exception:
                            pass

                def _done():
                    upload_btn.config(state="normal")
                    msg = f"Done — {total_ok} file(s) uploaded, {total_fail} failed across {total_folders} folder(s)."
                    status_var.set(msg)
                    _log(f"\n{msg}")
                    LOG.log("HI3 Upload", msg)
                    if total_fail == 0:
                        self.popup("Success",
                                   f"{total_folders} folder(s) uploaded to /etc/vsf/input/ on Voice server.",
                                   "success")
                    else:
                        self.popup("Warning", f"{total_ok} uploaded, {total_fail} failed. Check log.", "warn")
                self.after(0, _done)
            threading.Thread(target=_run, daemon=True).start()

        upload_btn = tk.Button(container, text="📤  Upload HI3 Files",
                               bg=self.C("success"), fg="white",
                               relief="flat", font=(_UI_FONT, 11, "bold"),
                               cursor="hand2", padx=20, pady=8,
                               command=_do_upload)
        upload_btn.pack(anchor="w", pady=(0, 10))

    def upload_pcap(self):
        _last = self.cfg.get("last_folder", {}).get("pcap", "")
        folder = filedialog.askdirectory(title="Select PCAP Folder",
                                         initialdir=_last if _last else None)
        if not folder: return
        self.cfg.setdefault("last_folder", {})["pcap"] = folder
        save_config(self.cfg)
        ip   = self.cfg["pcap"].get("ip", "")
        path = self.cfg["pcap"].get("path", "")
        if not ip:
            return self.popup("Error", "PCAP server not configured. Go to Settings.", "error")
        sftp = CONN.get_sftp("pcap")
        if not sftp:
            return self.popup("Error", "PCAP server is not connected. Please check Settings.", "error")
        remote = path.rstrip("/") + "/" + os.path.basename(folder)

        # Pre-count total files and size so the progress bar can be deterministic
        total_files = 0
        total_bytes = 0
        for _r, _, _fs in os.walk(folder):
            for _f in _fs:
                total_files += 1
                try: total_bytes += os.path.getsize(os.path.join(_r, _f))
                except Exception: pass
        _sz = f"{total_bytes/1024/1024:.1f} MB" if total_bytes >= 1024*1024 else f"{total_bytes//1024} KB"

        LOG.log("Upload", f"Starting PCAP upload: {folder} → {ip}:{remote} ({total_files} files, {_sz})")
        top, _ = self.progress_window(f"Uploading PCAP — {total_files} file(s) · {_sz}")

        def task():
            try:
                count = 0
                bytes_done = 0
                t_start = time.time()
                for rootdir, _, files in os.walk(folder):
                    rel  = os.path.relpath(rootdir, folder)
                    rdir = os.path.join(remote, rel).replace("\\", "/")
                    mkdirs_sftp(sftp, rdir)
                    for f in files:
                        fpath = os.path.join(rootdir, f)
                        fsize = 0
                        try: fsize = os.path.getsize(fpath)
                        except Exception: pass
                        sftp.put(fpath, f"{rdir}/{f}")
                        count += 1
                        bytes_done += fsize
                        pct = int(count * 100 / total_files) if total_files else 100
                        elapsed = time.time() - t_start
                        speed = bytes_done / elapsed if elapsed > 0 else 0
                        remaining_bytes = total_bytes - bytes_done
                        eta = int(remaining_bytes / speed) if speed > 0 else 0
                        speed_str = (f"{speed/1024/1024:.1f} MB/s" if speed >= 1024*1024
                                     else f"{speed/1024:.0f} KB/s")
                        eta_str = (f"~{eta}s" if eta < 60 else f"~{eta//60}m {eta%60}s")
                        self.after(0, lambda c=count, fn=f, p=pct, sp=speed_str, et=eta_str:
                            top._set_status(
                                f"Uploading PCAP…  ({c} / {total_files})  {sp} · {et} left",
                                pct=p,
                                sub=fn) if top.winfo_exists() else None)
                LOG.log("Upload", f"PCAP complete — {count} file(s) → {ip}:{remote}")
                self._write_history("PCAP Upload", count, ip, "✅ OK",
                                    os.path.basename(folder))
                self.after(0, lambda: (
                    top.destroy() if top.winfo_exists() else None,
                    self._toast(f"✅  PCAP upload complete\n{count} file(s) transferred.",
                                "success")))
            except Exception as e:
                LOG.log("Upload", f"PCAP failed: {e}", "ERROR")
                self._write_history("PCAP Upload", 0, ip, "❌ Failed", str(e)[:60])
                self.after(0, lambda err=e: (
                    top.destroy() if top.winfo_exists() else None,
                    self.popup("Error", str(err), "error")))
        self._active_uploads += 1
        _orig_task = task
        def task():
            try:
                _orig_task()
            finally:
                self._active_uploads = max(0, self._active_uploads - 1)
        threading.Thread(target=task, daemon=True).start()

    def upload_ludr(self):
        _last = self.cfg.get("last_folder", {}).get("ludr", "")
        files = filedialog.askopenfilenames(
            filetypes=[("CSV/TXT", "*.csv *.txt")],
            initialdir=_last if _last else None)
        if not files: return
        self.cfg.setdefault("last_folder", {})["ludr"] = os.path.dirname(files[0])
        save_config(self.cfg)
        ip    = self.cfg["ludr"].get("ip", "")
        path  = self.cfg["ludr"].get("path", "")
        total = len(files)
        if not ip:
            return self.popup("Error", "LUDR server not configured. Go to Settings.", "error")
        sftp = CONN.get_sftp("ludr")
        if not sftp:
            return self.popup("Error", "LUDR server is not connected. Please check Settings.", "error")
        _ludr_bytes = sum(os.path.getsize(f) for f in files if os.path.isfile(f))
        _ludr_sz = f"{_ludr_bytes/1024/1024:.1f} MB" if _ludr_bytes >= 1024*1024 else f"{_ludr_bytes//1024} KB"
        LOG.log("Upload", f"Starting LBS/LUDR/SMS upload — {total} file(s), {_ludr_sz} → {ip}:{path}")
        top, _ = self.progress_window(f"Uploading LBS / LUDR / SMS — {total} file(s) · {_ludr_sz}")

        def task():
            try:
                bytes_done = 0
                t_start = time.time()
                for idx, f in enumerate(files):
                    fn    = os.path.basename(f)
                    fsize = 0
                    try: fsize = os.path.getsize(f)
                    except Exception: pass
                    sftp.put(f, path.rstrip("/") + "/" + fn)
                    bytes_done += fsize
                    LOG.log("Upload", f"  {fn} → {path}")
                    elapsed = time.time() - t_start
                    speed   = bytes_done / elapsed if elapsed > 0 else 0
                    rem_b   = _ludr_bytes - bytes_done
                    eta     = int(rem_b / speed) if speed > 0 else 0
                    speed_str = (f"{speed/1024/1024:.1f} MB/s" if speed >= 1024*1024
                                 else f"{speed/1024:.0f} KB/s")
                    eta_str = (f"~{eta}s" if eta < 60 else f"~{eta//60}m {eta%60}s")
                    pct = int((idx + 1) * 100 / total)
                    self.after(0, lambda i=idx+1, n=fn, p=pct, sp=speed_str, et=eta_str:
                        top._set_status(
                            f"Uploading file {i} of {total}…  {sp} · {et} left",
                            pct=p,
                            sub=n) if top.winfo_exists() else None)
                LOG.log("Upload", f"LBS/LUDR/SMS complete — {total} file(s) uploaded")
                self._write_history("LBS Upload", total, ip, "✅ OK",
                                    f"{total} file(s)")
                self.after(0, lambda: (
                    top.destroy() if top.winfo_exists() else None,
                    self._toast(
                        f"✅  LBS/LUDR/SMS upload complete\n{total} file(s) transferred.",
                        "success")))
            except Exception as e:
                LOG.log("Upload", f"LUDR failed: {e}", "ERROR")
                self._write_history("LBS Upload", 0, ip, "❌ Failed", str(e)[:60])
                self.after(0, lambda err=e: (
                    top.destroy() if top.winfo_exists() else None,
                    self.popup("Error", str(err), "error")))
        self._active_uploads += 1
        _orig_ludr = task
        def task():
            try:
                _orig_ludr()
            finally:
                self._active_uploads = max(0, self._active_uploads - 1)
        threading.Thread(target=task, daemon=True).start()

    def upload_voice(self):
        _last = self.cfg.get("last_folder", {}).get("voice", "")
        folder = filedialog.askdirectory(title="Select folder containing Hi2 & Hi3",
                                          initialdir=_last if _last else None)
        if not folder:
            return
        self.cfg.setdefault("last_folder", {})["voice"] = folder
        save_config(self.cfg)
        hi2 = os.path.join(folder, "Hi2")
        hi3 = os.path.join(folder, "Hi3")
        if not os.path.isdir(hi2) or not os.path.isdir(hi3):
            return self.popup("Error",
                f"Hi2 or Hi3 folder not found inside:\n{folder}\n\n"
                f"Hi2 exists: {os.path.isdir(hi2)}\n"
                f"Hi3 exists: {os.path.isdir(hi3)}", "error")

        ip  = self.cfg.get("voice", {}).get("ip",  "")
        pwd = self.cfg.get("voice", {}).get("pwd", "")
        if not ip or not pwd:
            return self.popup("Error",
                "Voice server not configured. Go to Settings.", "error")

        # ── Collect files ───────────────────────────────────────────
        all_hi2 = [
            os.path.join(hi2, f)
            for f in os.listdir(hi2)
            if os.path.isfile(os.path.join(hi2, f)) and not f.startswith(".")
        ]
        hi3_files = sorted(
            os.path.join(r, f)
            for r, _, fs in os.walk(hi3) for f in fs
            if not f.startswith(".")
        )

        begin_files = sorted(
            f for f in all_hi2
            if "begin" in os.path.basename(f).lower())
        end_files = sorted(
            f for f in all_hi2
            if "end" in os.path.basename(f).lower())

        # Backward compat single-file references
        begin_file = begin_files[0] if begin_files else None
        end_file   = end_files[0]   if end_files   else None

        # ── Validate ────────────────────────────────────────────────
        errors = []
        if not begin_files:
            found = [os.path.basename(f) for f in all_hi2] or ["(empty)"]
            errors.append(
                f"No 'begin' file found in Hi2.\n"
                f"Files found: {', '.join(found)}")
        if not end_files:
            found = [os.path.basename(f) for f in all_hi2] or ["(empty)"]
            errors.append(
                f"No 'end' file found in Hi2.\n"
                f"Files found: {', '.join(found)}")
        if not hi3_files:
            errors.append(f"No files found in Hi3 folder:\n{hi3}")
        if errors:
            return self.popup("Error", "\n\n".join(errors), "error")

        WATCH     = "/data5/prism/Paths/InputDir1/WatchDir"
        VSF       = "/etc/vsf/input"
        WAIT_SECS = 1

        LOG.log("Upload", f"Voice upload starting — {folder}")
        LOG.log("Upload", f"  Cmd 1: cp -r HI3 {VSF}/")
        LOG.log("Upload", f"         → {len(hi3_files)} file(s) into {VSF}/HI3/")
        LOG.log("Upload", f"  [1s wait]")
        LOG.log("Upload", f"  Cmd 2: cp -r HI2/* {WATCH}/")
        LOG.log("Upload",
                f"         Begin files ({len(begin_files)}): "
                f"{[os.path.basename(f) for f in begin_files]}")
        LOG.log("Upload",
                f"         End files ({len(end_files)}): "
                f"{[os.path.basename(f) for f in end_files]}")

        # ── Progress window (deterministic stages) ─────────────────
        total_files = len(hi3_files) + len(begin_files) + len(end_files)
        top, _ = self.progress_window(
            f"Uploading Voice — {total_files} file(s)…")

        def _upd(msg, pct, sub=""):
            self.after(0, lambda m=msg, p=pct, s=sub:
                top._set_status(m, pct=p, sub=s)
                if top.winfo_exists() else None)

        def task():
            ssh_d = sftp_d = None
            try:
                _upd(f"Connecting to {ip}…", 0)
                LOG.log("Upload", f"Voice: opening SSH to {ip}")
                ssh_d = paramiko.SSHClient()
                ssh_d.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_d.connect(
                    ip, username="root", password=pwd,
                    timeout=20, banner_timeout=20, auth_timeout=20)
                sftp_d = ssh_d.open_sftp()
                LOG.log("Upload", "Voice: SSH/SFTP ready")

                def ssh_run(cmd):
                    _, out, err = ssh_d.exec_command(cmd, timeout=30)
                    rc   = out.channel.recv_exit_status()
                    sout = out.read().decode("utf-8", "ignore").strip()
                    serr = err.read().decode("utf-8", "ignore").strip()
                    LOG.log("Upload", f"  $ {cmd}  [rc={rc}]"
                            + (f"  stderr: {serr}" if serr else ""))
                    return rc, sout, serr

                def ensure_dir(remote_path):
                    rc, _, serr = ssh_run(
                        f"mkdir -p {shlex.quote(remote_path)}")
                    if rc != 0 and serr:
                        LOG.log("Upload",
                                f"  ⚠ mkdir -p {remote_path}: {serr}",
                                "WARNING")
                    else:
                        LOG.log("Upload", f"  ✓ dir ready: {remote_path}")

                _upd("Preparing remote directories…", 2)
                ensure_dir(VSF)
                ensure_dir(WATCH)

                # ── Stage 1: Hi3 → /etc/vsf/input/HI3/ ─────────────
                remote_hi3_dir = f"{VSF}/HI3"
                ensure_dir(remote_hi3_dir)
                LOG.log("Upload",
                        f"Stage 1: {len(hi3_files)} Hi3 file(s) → {remote_hi3_dir}")
                count = 0
                for i, local in enumerate(hi3_files, 1):
                    fname  = os.path.basename(local)
                    remote = f"{remote_hi3_dir}/{fname}"
                    pct    = int(i * 45 / len(hi3_files))  # 0 → 45%
                    _upd(f"Stage 1 of 2 — Hi3  ({i}/{len(hi3_files)})", pct, fname)
                    LOG.log("Upload", f"  PUT {local} → {remote}")
                    sftp_d.put(local, remote)
                    try:
                        sz = sftp_d.stat(remote).st_size
                        LOG.log("Upload", f"  ✓ {fname}  ({sz} bytes)")
                    except Exception:
                        LOG.log("Upload", f"  ⚠ could not verify {fname}", "WARNING")
                    count += 1

                LOG.log("Upload", f"Stage 1 complete — {len(hi3_files)} file(s)")

                # ── 1-second wait ────────────────────────────────────
                _upd("Stage 1 complete ✓   Waiting 1 second…", 50,
                     "Hi3 uploaded — pausing before Hi2")
                LOG.log("Upload", "Voice: 1s wait before HI2…")
                time.sleep(1)

                # ── Stage 2: Hi2 Begin + End → WatchDir ─────────────
                hi2_upload = (
                    [("Begin", f) for f in begin_files] +
                    [("End",   f) for f in end_files]
                )
                LOG.log("Upload",
                        f"Stage 2: {len(hi2_upload)} Hi2 file(s) → {WATCH}")
                for j, (label, local) in enumerate(hi2_upload, 1):
                    fname  = os.path.basename(local)
                    remote = f"{WATCH}/{fname}"
                    pct    = 50 + int(j * 50 / len(hi2_upload))  # 50 → 100%
                    _upd(f"Stage 2 of 2 — Hi2 {label}  ({j}/{len(hi2_upload)})",
                         pct, fname)
                    LOG.log("Upload", f"  PUT {local} → {remote}")
                    sftp_d.put(local, remote)
                    try:
                        sz = sftp_d.stat(remote).st_size
                        LOG.log("Upload", f"  ✓ {fname}  ({sz} bytes)")
                    except Exception:
                        LOG.log("Upload", f"  ⚠ could not verify {fname}", "WARNING")
                    count += 1

                LOG.log("Upload", f"Voice upload complete — {count} file(s)")
                self._write_history("Voice Upload", count, ip, "✅ OK",
                                    os.path.basename(folder))
                self.after(0, lambda: (
                    top.destroy() if top.winfo_exists() else None,
                    self.popup("Success",
                        f"Voice upload completed.\n\n"
                        f"  Stage 1 — Hi3 → {VSF}/HI3/\n"
                        f"            {len(hi3_files)} file(s)\n\n"
                        f"  [1 second wait]\n\n"
                        f"  Stage 2 — Hi2 → {WATCH}/\n" +
                        "".join(f"            → {os.path.basename(f)}\n"
                                for f in begin_files + end_files) +
                        f"\n  Total: {count} file(s) transferred",
                        "success")))

            except Exception as exc:
                import traceback
                tb = traceback.format_exc()
                LOG.log("Upload", f"Voice FAILED: {exc}\n{tb}", "ERROR")
                self._write_history("Voice Upload", 0, ip, "❌ Failed",
                                    str(exc)[:60])
                self.after(0, lambda m=str(exc): (
                    top.destroy() if top.winfo_exists() else None,
                    self.popup("Error",
                               f"Voice upload failed:\n\n{m}\n\n"
                               "Check the Logs tab for full details.",
                               "error")))
            finally:
                for obj in (sftp_d, ssh_d):
                    try:
                        if obj: obj.close()
                    except Exception: pass
                LOG.log("Upload", "Voice: SSH session closed")

        threading.Thread(target=task, daemon=True).start()


    def _bulk_voice_upload_picker(self):
        """
        Picker for bulk sequential upload.
        User selects a parent folder whose sub-folders each contain Hi2/ + Hi3/.
        Calls upload_voice_bulk_sequential with the list.
        """
        parent = filedialog.askdirectory(
            title="Select parent folder containing call sub-folders")
        if not parent:
            return

        sub_calls = sorted([
            os.path.join(parent, d)
            for d in os.listdir(parent)
            if os.path.isdir(os.path.join(parent, d))
            and os.path.isdir(os.path.join(parent, d, "Hi2"))
            and os.path.isdir(os.path.join(parent, d, "Hi3"))
        ])

        if not sub_calls:
            return self.popup(
                "No Call Folders Found",
                f"No sub-folders with Hi2/ and Hi3/ found inside:\n{parent}\n\n"
                "Each call must be in its own sub-folder containing Hi2/ and Hi3/.",
                "error")

        confirm = self.popup(
            "Confirm Bulk Upload",
            f"Found {len(sub_calls)} call folder(s) inside:\n{parent}\n\n"
            + "\n".join(f"  • {os.path.basename(f)}"
                         for f in sub_calls[:8])
            + ("\n  …" if len(sub_calls) > 8 else "")
            + f"\n\nUpload all {len(sub_calls)} calls sequentially?\n"
              "(Each call waits for MsgType[OBJ_GEN] before the next.)",
            "confirm")

        # popup returns True if user clicks Confirm
        if not confirm:
            return

        LOG.log("Upload",
                f"Bulk picker: {len(sub_calls)} folder(s) from {parent}")
        self.upload_voice_bulk_sequential(sub_calls)

    # ── Schedule helpers — upload with a known path, no file picker ──────
    def _upload_pcap_with_folder(self, folder, _done_event=None, _started_event=None):
        """Upload PCAP from a known folder without showing a browse dialog."""
        if not folder or not os.path.isdir(folder):
            self._toast(f"Scheduled PCAP: folder not found — {folder}", "error")
            if _done_event: _done_event.set()
            return
        self.cfg.setdefault("last_folder", {})["pcap"] = folder
        save_config(self.cfg)
        ip   = self.cfg.get("pcap", {}).get("ip", "")
        path = self.cfg.get("pcap", {}).get("path", "")
        if not ip:
            self._toast("Scheduled PCAP: server not configured.", "error")
            if _done_event: _done_event.set()
            return
        total_files = total_bytes = 0
        for _r, _, _fs in os.walk(folder):
            for _f in _fs:
                total_files += 1
                try: total_bytes += os.path.getsize(os.path.join(_r, _f))
                except Exception: pass
        _sz = f"{total_bytes/1024/1024:.1f} MB" if total_bytes >= 1024*1024 else f"{total_bytes//1024} KB"
        remote = path.rstrip("/") + "/" + os.path.basename(folder)
        pwd = self.cfg.get("pcap", {}).get("pwd", "")
        LOG.log("Schedule", f"PCAP upload: {folder} → {ip}:{remote} ({total_files} files, {_sz})")
        top, _ = self.progress_window(f"[Scheduled] PCAP — {total_files} file(s) · {_sz}")
        def task():
            ssh_t = sftp_t = None
            try:
                self._active_uploads += 1
                if _started_event: _started_event.set()
                # Dedicated SSH connection per job so multiple PCAP jobs don't share SFTP
                ssh_t = paramiko.SSHClient()
                ssh_t.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_t.connect(ip, username="root", password=pwd,
                              timeout=20, banner_timeout=20, auth_timeout=20)
                sftp_t = ssh_t.open_sftp()
                mkdirs_sftp(sftp_t, remote)
                for _r, _, _fs in os.walk(folder):
                    for _f in _fs:
                        lp = os.path.join(_r, _f)
                        rp = remote + "/" + os.path.relpath(lp, folder).replace(os.sep, "/")
                        sftp_t.put(lp, rp)
                self.after(0, lambda: (top.destroy(), self._toast(f"✅ Scheduled PCAP done", "success")))
                self._write_history("PCAP Upload", total_files, ip, "✅ OK", folder)
            except Exception as e:
                self._write_history("PCAP Upload", 0, ip, "❌ Failed", str(e)[:60])
                self.after(0, lambda err=str(e): (top.destroy(), self.popup("Upload Error", err, "error")))
            finally:
                self._active_uploads = max(0, self._active_uploads - 1)
                if _done_event: _done_event.set()
                for obj in (sftp_t, ssh_t):
                    try:
                        if obj: obj.close()
                    except Exception: pass
        threading.Thread(target=task, daemon=True).start()

    def _upload_ludr_with_files(self, files, _done_event=None, _started_event=None):
        """Upload LUDR/SMS files from a known list without showing a browse dialog."""
        files = [f for f in files if os.path.isfile(f)]
        if not files:
            self._toast("Scheduled LUDR: no valid files found.", "error")
            if _done_event: _done_event.set()
            return
        self.cfg.setdefault("last_folder", {})["ludr"] = os.path.dirname(files[0])
        save_config(self.cfg)
        ip   = self.cfg.get("ludr", {}).get("ip", "")
        path = self.cfg.get("ludr", {}).get("path", "")
        if not ip:
            self._toast("Scheduled LUDR: server not configured.", "error")
            if _done_event: _done_event.set()
            return
        total = len(files)
        _bytes = sum(os.path.getsize(f) for f in files)
        _sz = f"{_bytes/1024/1024:.1f} MB" if _bytes >= 1024*1024 else f"{_bytes//1024} KB"
        pwd = self.cfg.get("ludr", {}).get("pwd", "")
        LOG.log("Schedule", f"LUDR upload: {total} file(s), {_sz} → {ip}:{path}")
        top, _ = self.progress_window(f"[Scheduled] LUDR — {total} file(s) · {_sz}")
        def task():
            ssh_t = sftp_t = None
            try:
                self._active_uploads += 1
                if _started_event: _started_event.set()
                # Dedicated SSH connection per job so multiple LUDR jobs don't share SFTP
                ssh_t = paramiko.SSHClient()
                ssh_t.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_t.connect(ip, username="root", password=pwd,
                              timeout=20, banner_timeout=20, auth_timeout=20)
                sftp_t = ssh_t.open_sftp()
                for f in files:
                    sftp_t.put(f, path.rstrip("/") + "/" + os.path.basename(f))
                self.after(0, lambda: (top.destroy(), self._toast(f"✅ Scheduled LUDR done", "success")))
                self._write_history("LBS Upload", total, ip, "✅ OK", os.path.dirname(files[0]))
            except Exception as e:
                self._write_history("LBS Upload", 0, ip, "❌ Failed", str(e)[:60])
                self.after(0, lambda err=str(e): (top.destroy(), self.popup("Upload Error", err, "error")))
            finally:
                self._active_uploads = max(0, self._active_uploads - 1)
                if _done_event: _done_event.set()
                for obj in (sftp_t, ssh_t):
                    try:
                        if obj: obj.close()
                    except Exception: pass
        threading.Thread(target=task, daemon=True).start()

    def _upload_voice_with_folder(self, folder, _done_event=None, _started_event=None):
        """Scheduler voice upload — mirrors upload_voice (single) and
        upload_voice_bulk_sequential exactly.

        Auto-detect mode:
          • If folder itself contains Hi2/ and Hi3/ → single call upload
            (same as upload_voice: Hi3→/etc/vsf/input/HI3/, Hi2→WatchDir,
             1s wait between stages)
          • If folder contains sub-folders each having Hi2/ and Hi3/ → bulk
            sequential upload (same as upload_voice_bulk_sequential: processes
            each call in order, waits for MsgType[OBJ_GEN] before next call)

        Tracks _active_uploads throughout so scheduler poll can detect finish.
        """
        if not folder or not os.path.isdir(folder):
            self._toast(f"Scheduled Voice: folder not found — {folder}", "error")
            if _done_event: _done_event.set()
            return

        ip  = self.cfg.get("voice", {}).get("ip",  "")
        pwd = self.cfg.get("voice", {}).get("pwd", "")
        if not ip or not pwd:
            self._toast("Scheduled Voice: server not configured — check Settings.", "error")
            if _done_event: _done_event.set()
            return

        WATCH       = "/data5/prism/Paths/InputDir1/WatchDir"
        VSF         = "/etc/vsf/input"
        TMSC_LOG    = "/var/log/tmsc/tmsc.log"
        ACK_KEYWORD = "MsgType[OBJ_GEN]"
        ACK_TIMEOUT = 60

        self.cfg.setdefault("last_folder", {})["voice"] = folder
        save_config(self.cfg)

        # ── Auto-detect: single call vs bulk ──────────────────────────────
        is_single = (os.path.isdir(os.path.join(folder, "Hi2")) and
                     os.path.isdir(os.path.join(folder, "Hi3")))

        if is_single:
            call_folders = [folder]
        else:
            call_folders = sorted([
                os.path.join(folder, d)
                for d in os.listdir(folder)
                if os.path.isdir(os.path.join(folder, d))
                and os.path.isdir(os.path.join(folder, d, "Hi2"))
                and os.path.isdir(os.path.join(folder, d, "Hi3"))
            ])

        if not call_folders:
            self._toast(
                f"Scheduled Voice: no valid call folder(s) found in {os.path.basename(folder)}. "
                "Folder must contain Hi2/ and Hi3/, or sub-folders that do.", "error"); return

        mode_label = "single" if is_single else f"bulk ({len(call_folders)} calls)"
        LOG.log("Schedule", f"Voice upload — {mode_label}: {folder}")
        top, _ = self.progress_window(
            f"[Scheduled] Voice — {mode_label}…")

        def ui(msg, pct=None, sub=""):
            self.after(0, lambda m=msg, p=pct, s=sub:
                top._set_status(m, pct=p, sub=s)
                if top.winfo_exists() else None)

        def task():
            ssh_d = sftp_d = None
            succeeded = failed = total_files = 0
            try:
                self._active_uploads += 1
                if _started_event: _started_event.set()
                ui(f"Connecting to {ip}…", 0)
                ssh_d = paramiko.SSHClient()
                ssh_d.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_d.connect(ip, username="root", password=pwd,
                              timeout=20, banner_timeout=20, auth_timeout=20)
                sftp_d = ssh_d.open_sftp()
                LOG.log("Upload", f"Scheduled voice: SSH ready — {mode_label}")

                def ssh_run(cmd):
                    _, out, err = ssh_d.exec_command(cmd, timeout=30)
                    rc   = out.channel.recv_exit_status()
                    sout = out.read().decode("utf-8", "ignore").strip()
                    serr = err.read().decode("utf-8", "ignore").strip()
                    LOG.log("Upload", f"  $ {cmd}  [rc={rc}]"
                            + (f"  stderr: {serr}" if serr else ""))
                    return rc, sout, serr

                def ensure_dir(p):
                    rc, _, serr = ssh_run(f"mkdir -p {shlex.quote(p)}")
                    if rc != 0 and serr:
                        LOG.log("Upload", f"  ⚠ mkdir {p}: {serr}", "WARNING")

                def wait_obj_gen(call_name):
                    _, baseline, _ = ssh_run(
                        f"wc -l {shlex.quote(TMSC_LOG)} 2>/dev/null || echo 0")
                    try:
                        start_line = int(baseline.split()[0])
                    except Exception:
                        start_line = 0
                    deadline = time.time() + ACK_TIMEOUT
                    while time.time() < deadline:
                        time.sleep(2)
                        _, new_lines, _ = ssh_run(
                            f"tail -n +{start_line+1} {shlex.quote(TMSC_LOG)} 2>/dev/null")
                        if ACK_KEYWORD in new_lines:
                            LOG.log("Upload", f"  ✓ {ACK_KEYWORD} seen — {call_name} processed")
                            return True
                        remaining = int(deadline - time.time())
                        ui(f"Waiting ACK: {call_name}… ({remaining}s)",
                           sub="MsgType[OBJ_GEN] not yet seen in tmsc.log")
                    LOG.log("Upload", f"  ⚠ Timeout {ACK_TIMEOUT}s waiting for {ACK_KEYWORD}", "WARNING")
                    return False

                ensure_dir(VSF)
                ensure_dir(WATCH)

                for idx, call_folder in enumerate(call_folders, 1):
                    fname = os.path.basename(call_folder)
                    ui(f"[{idx}/{len(call_folders)}] {fname} — collecting…")

                    hi2 = os.path.join(call_folder, "Hi2")
                    hi3 = os.path.join(call_folder, "Hi3")
                    all_hi2 = [os.path.join(hi2, f) for f in os.listdir(hi2)
                               if os.path.isfile(os.path.join(hi2, f))
                               and not f.startswith(".")]
                    hi3_files   = sorted(
                        os.path.join(r, f)
                        for r, _, fs in os.walk(hi3) for f in fs
                        if not f.startswith("."))
                    begin_files = sorted(f for f in all_hi2
                                         if "begin" in os.path.basename(f).lower())
                    end_files   = sorted(f for f in all_hi2
                                         if "end"   in os.path.basename(f).lower())

                    if not hi3_files or not begin_files or not end_files:
                        LOG.log("Upload",
                                f"  Skipped {fname}: incomplete "
                                f"(Hi3:{len(hi3_files)} Begin:{len(begin_files)} End:{len(end_files)})",
                                "WARNING")
                        failed += 1
                        continue

                    try:
                        # Stage 1: Hi3 → /etc/vsf/input/HI3/
                        remote_hi3_dir = f"{VSF}/HI3"
                        ensure_dir(remote_hi3_dir)
                        for i, lf in enumerate(hi3_files, 1):
                            fn     = os.path.basename(lf)
                            remote = f"{remote_hi3_dir}/{fn}"
                            pct    = int(((idx-1)/len(call_folders) + i/(len(hi3_files)*len(call_folders)))*45)
                            ui(f"[{idx}/{len(call_folders)}] {fname} — Hi3 ({i}/{len(hi3_files)}): {fn}",
                               pct=pct, sub=fn)
                            sftp_d.put(lf, remote)
                            LOG.log("Upload", f"  Hi3: {fn} → {remote}")
                            total_files += 1

                        # 1-second wait (same as manual upload)
                        ui(f"[{idx}/{len(call_folders)}] {fname} — Hi3 done, waiting 1s…", pct=50)
                        time.sleep(1)

                        # Stage 2: Hi2 Begin → WatchDir
                        for lf in begin_files:
                            fn     = os.path.basename(lf)
                            remote = f"{WATCH}/{fn}"
                            ui(f"[{idx}/{len(call_folders)}] {fname} — Hi2 Begin: {fn}",
                               pct=75, sub=fn)
                            sftp_d.put(lf, remote)
                            LOG.log("Upload", f"  Hi2 Begin: {fn} → {remote}")
                            total_files += 1

                        # Stage 2: Hi2 End → WatchDir
                        for lf in end_files:
                            fn     = os.path.basename(lf)
                            remote = f"{WATCH}/{fn}"
                            ui(f"[{idx}/{len(call_folders)}] {fname} — Hi2 End: {fn}",
                               pct=90, sub=fn)
                            sftp_d.put(lf, remote)
                            LOG.log("Upload", f"  Hi2 End: {fn} → {remote}")
                            total_files += 1

                        LOG.log("Upload", f"  ✓ {fname} uploaded")
                        succeeded += 1

                        # Wait for MsgType[OBJ_GEN] before next call (bulk only)
                        if not is_single and idx < len(call_folders):
                            ui(f"[{idx}/{len(call_folders)}] {fname} — watching tmsc.log…",
                               sub=f"Waiting for {ACK_KEYWORD}")
                            wait_obj_gen(fname)

                    except Exception as exc:
                        import traceback
                        LOG.log("Upload",
                                f"  ✗ {fname}: {exc}\n" + traceback.format_exc(), "ERROR")
                        failed += 1

                    pct_overall = int(idx * 100 / len(call_folders))
                    ui(f"Bulk Voice — {idx}/{len(call_folders)} done", pct=pct_overall, sub=fname)

                # All done
                self._write_history(
                    "Voice Upload (Scheduled)", total_files, ip,
                    "✅ OK" if failed == 0 else f"⚠ {succeeded} OK / {failed} failed",
                    f"{len(call_folders)} call(s)")

                msg = (f"✅ {succeeded} call(s) uploaded"
                       if failed == 0
                       else f"⚠ {succeeded} OK, {failed} failed")
                self.after(0, lambda: (
                    top.destroy() if top.winfo_exists() else None,
                    self._toast(f"Scheduled Voice done — {msg}", "success" if failed == 0 else "warn")))

            except Exception as exc:
                import traceback
                LOG.log("Upload", f"Scheduled voice FAILED: {exc}\n" + traceback.format_exc(), "ERROR")
                self.after(0, lambda m=str(exc): (
                    top.destroy() if top.winfo_exists() else None,
                    self._toast(f"❌ Scheduled Voice failed: {m[:80]}", "error", 6000)))
            finally:
                self._active_uploads = max(0, self._active_uploads - 1)
                if _done_event: _done_event.set()
                for obj in (sftp_d, ssh_d):
                    try:
                        if obj: obj.close()
                    except Exception: pass
                LOG.log("Upload", "Scheduled voice: SSH session closed")

        threading.Thread(target=task, daemon=True).start()

    def upload_voice_individual(self):
        """Picker wrapper — lets user browse then calls core upload."""
        folder = filedialog.askdirectory(
            title="Select call folder containing Hi2 & Hi3")
        if not folder: return
        self.upload_voice_individual_folder(folder)

    def upload_voice_individual_folder(self, folder):
        """
        Core individual upload for ONE call folder.
        Hi3 files → /etc/vsf/input/  (flat, equivalent to cp -r HI3 /etc/vsf/input/)
        Hi2 files → /data5/prism/Paths/InputDir1/WatchDir/
        Sequence:  Hi3 → 1s wait → Hi2 Begin(s) → Hi2 End(s)
        """
        hi2 = os.path.join(folder, "Hi2")
        hi3 = os.path.join(folder, "Hi3")
        if not os.path.isdir(hi2) or not os.path.isdir(hi3):
            self._toast(
                f"Hi2 or Hi3 not found in:\n{os.path.basename(folder)}",
                "error")
            return

        ip  = self.cfg.get("voice", {}).get("ip",  "")
        pwd = self.cfg.get("voice", {}).get("pwd", "")
        if not ip or not pwd:
            self._toast("Voice server not configured.\nGo to ⚙️ Settings.",
                        "error")
            return

        # Hi3 files go into /etc/vsf/input/HI3/ — matches bulk and scheduled upload behaviour
        HI3_DEST = self.cfg.get("voice_hi3_path", "/etc/vsf/input/HI3")
        # WatchDir — Hi2 files go HERE
        HI2_DEST = self.cfg.get("voice_hi2_path",
                                  "/data5/prism/Paths/InputDir1/WatchDir")

        # Collect Hi3 files (all files inside Hi3/ folder)
        hi3_files = sorted(
            os.path.join(r, f)
            for r, _, fs in os.walk(hi3)
            for f in fs if not f.startswith("."))

        # Collect Hi2 files
        all_hi2 = [
            os.path.join(hi2, f) for f in os.listdir(hi2)
            if os.path.isfile(os.path.join(hi2, f))
            and not f.startswith(".")]
        begin_files = sorted(
            f for f in all_hi2
            if "begin" in os.path.basename(f).lower())
        end_files = sorted(
            f for f in all_hi2
            if "end" in os.path.basename(f).lower())

        if not hi3_files:
            self._toast(f"No files in Hi3/ folder:\n{hi3}", "error")
            return
        if not begin_files:
            self._toast("No Begin file found in Hi2/", "error")
            return
        if not end_files:
            self._toast("No End file found in Hi2/", "error")
            return

        fname = os.path.basename(folder)
        total = len(hi3_files) + len(begin_files) + len(end_files)
        top, _ = self.progress_window(f"Uploading {fname}…")
        LOG.log("Upload",
                f"Individual voice: {folder} "
                f"({len(hi3_files)} Hi3, "
                f"{len(begin_files)} Begin, {len(end_files)} End)")

        def task():
            ssh = sftp = None
            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(
                    paramiko.AutoAddPolicy())
                ssh.connect(ip, username="root", password=pwd,
                            timeout=20, banner_timeout=20)
                sftp = ssh.open_sftp()

                def _ssh_mkdir(p):
                    _, o, _ = ssh.exec_command(
                        f"mkdir -p {shlex.quote(p)}")
                    o.channel.recv_exit_status()

                _ssh_mkdir(HI3_DEST)
                _ssh_mkdir(HI2_DEST)

                done = 0

                # ── Stage 1: upload Hi3 files → /etc/vsf/input/HI3/ ──
                for lf in hi3_files:
                    fn = os.path.basename(lf)
                    remote = f"{HI3_DEST.rstrip('/')}/{fn}"
                    sftp.put(lf, remote)
                    done += 1
                    pct = int(done * 75 / total)
                    self.after(0, lambda p=pct, n=fn:
                        top._set_status(
                            "Stage 1 — Hi3 uploading…",
                            pct=p, sub=n))
                    LOG.log("Upload", f"  Hi3: {fn} → {remote}")

                # ── 1-second wait ─────────────────────────────
                self.after(0, lambda:
                    top._set_status(
                        "Hi3 done — waiting 1s…", pct=75))
                time.sleep(1)

                # ── Stage 2: Hi2 Begin files ──────────────────
                for lf in begin_files:
                    fn = os.path.basename(lf)
                    remote = f"{HI2_DEST.rstrip('/')}/{fn}"
                    sftp.put(lf, remote)
                    done += 1
                    pct = 75 + int(
                        (done - len(hi3_files)) * 25 / total)
                    self.after(0, lambda p=pct, n=fn:
                        top._set_status(
                            "Stage 2 — Hi2 Begin…",
                            pct=p, sub=n))
                    LOG.log("Upload", f"  Hi2 Begin: {fn} → {remote}")

                # ── Stage 2: Hi2 End files ────────────────────
                for lf in end_files:
                    fn = os.path.basename(lf)
                    remote = f"{HI2_DEST.rstrip('/')}/{fn}"
                    sftp.put(lf, remote)
                    done += 1
                    pct = 75 + int(
                        (done - len(hi3_files)) * 25 / total)
                    self.after(0, lambda p=pct, n=fn:
                        top._set_status(
                            "Stage 2 — Hi2 End…",
                            pct=p, sub=n))
                    LOG.log("Upload", f"  Hi2 End:   {fn} → {remote}")

                LOG.log("Upload",
                        f"Individual upload complete — {fname} "
                        f"({done} files)")
                self._write_history(
                    "Voice Upload (Individual)", done, ip,
                    "✅ OK", fname)
                self.after(0, lambda: (
                    top.destroy(),
                    self._toast(
                        f"✅  Voice call uploaded\n{fname}",
                        "success")))

            except Exception as exc:
                import traceback as _tb
                LOG.log("Upload",
                        f"Individual voice failed: {exc}\n"
                        + _tb.format_exc(), "ERROR")
                self.after(0, lambda m=str(exc): (
                    top.destroy(),
                    self._toast(
                        f"❌  Upload failed\n{m[:80]}",
                        "error", 6000)))
            finally:
                for obj in (sftp, ssh):
                    try:
                        if obj: obj.close()
                    except Exception:
                        pass

        threading.Thread(target=task, daemon=True).start()


    # ── Bulk Sequential Voice Upload ──────────────────────────────────
    def upload_voice_bulk_sequential(self, folders):
        """
        Upload a list of call folders ONE BY ONE.
        Same sequence as upload_voice per call:
          1. HI3 files → /etc/vsf/input/HI3/
          2. sleep 1s
          3. Hi2 Begin → WatchDir
          4. Hi2 End   → WatchDir
        After each call: watch /var/log/tmsc/tmsc.log for
        MsgType[OBJ_GEN] (backend processed the call).
        Only then upload next call. Timeout = 60s.
        """
        if not folders:
            return

        ip  = self.cfg.get("voice", {}).get("ip",  "")
        pwd = self.cfg.get("voice", {}).get("pwd", "")
        if not ip or not pwd:
            return self.popup("Error",
                "Voice server not configured. Go to Settings.", "error")

        WATCH       = "/data5/prism/Paths/InputDir1/WatchDir"
        VSF         = "/etc/vsf/input"
        TMSC_LOG    = "/var/log/tmsc/tmsc.log"
        ACK_KEYWORD = "MsgType[OBJ_GEN]"
        ACK_TIMEOUT = 60

        # Right-side progress bar (same as all other uploads)
        top, _ = self.progress_window(
            f"Bulk Voice Upload — 0 / {len(folders)}")

        def ui_status(msg, pct=None, sub=""):
            self.after(0, lambda m=msg, p=pct, s=sub:
                top._set_status(m, pct=p, sub=s)
                if top.winfo_exists() else None)

        def ui_progress(done, total=len(folders), fname=""):
            pct = int(done * 100 / max(total, 1))
            ui_status(
                f"Bulk Voice Upload — {done} / {total}",
                pct=pct,
                sub=fname)

        def task():
            ssh_d = sftp_d = None
            succeeded = 0
            failed    = 0
            try:
                ui_status(f"Connecting to {ip}\u2026")
                ssh_d = paramiko.SSHClient()
                ssh_d.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_d.connect(ip, username="root", password=pwd,
                              timeout=20, banner_timeout=20, auth_timeout=20)
                sftp_d = ssh_d.open_sftp()
                LOG.log("Upload",
                        f"Bulk voice: SSH ready, {len(folders)} call(s)")

                def ssh_run(cmd):
                    _, out, err = ssh_d.exec_command(cmd, timeout=30)
                    rc   = out.channel.recv_exit_status()
                    sout = out.read().decode("utf-8", "ignore").strip()
                    serr = err.read().decode("utf-8", "ignore").strip()
                    LOG.log("Upload", f"  $ {cmd}  [rc={rc}]"
                            + (f"  stderr: {serr}" if serr else ""))
                    return rc, sout, serr

                def ensure_dir(p):
                    rc, _, serr = ssh_run(
                        f"mkdir -p {shlex.quote(p)}")
                    if rc != 0 and serr:
                        LOG.log("Upload",
                                f"  \u26a0 mkdir {p}: {serr}", "WARNING")

                ensure_dir(VSF)
                ensure_dir(WATCH)

                def _wait_obj_gen(call_name):
                    """
                    Tail tmsc.log from current end, wait for
                    MsgType[OBJ_GEN] in new lines.
                    Returns True if found within ACK_TIMEOUT, else False.
                    """
                    _, baseline, _ = ssh_run(
                        f"wc -l {shlex.quote(TMSC_LOG)}"
                        f" 2>/dev/null || echo 0")
                    try:
                        start_line = int(baseline.split()[0])
                    except Exception:
                        start_line = 0

                    LOG.log("Upload",
                            f"  Watching {TMSC_LOG} from line "
                            f"{start_line} for '{ACK_KEYWORD}'\u2026")

                    deadline = time.time() + ACK_TIMEOUT
                    while time.time() < deadline:
                        time.sleep(2)
                        _, new_lines, _ = ssh_run(
                            f"tail -n +{start_line + 1} "
                            f"{shlex.quote(TMSC_LOG)} 2>/dev/null")
                        if ACK_KEYWORD in new_lines:
                            LOG.log("Upload",
                                    f"  \u2713 '{ACK_KEYWORD}' seen "
                                    f"\u2014 {call_name} processed")
                            return True
                        remaining = int(deadline - time.time())
                        ui_status(
                            f"Waiting ACK: {call_name}\u2026 ({remaining}s)",
                            sub="MsgType[OBJ_GEN] not yet seen in tmsc.log")
                    LOG.log("Upload",
                            f"  \u26a0 Timeout {ACK_TIMEOUT}s waiting for"
                            f" '{ACK_KEYWORD}' after {call_name}",
                            "WARNING")
                    return False

                for idx, folder in enumerate(folders, 1):
                    fname = os.path.basename(folder)
                    ui_status(
                        f"[{idx}/{len(folders)}] {fname} — collecting\u2026")

                    hi2 = os.path.join(folder, "Hi2")
                    hi3 = os.path.join(folder, "Hi3")
                    if not os.path.isdir(hi2) or not os.path.isdir(hi3):
                        LOG.log("Upload",
                                f"  Skipped {fname}: Hi2/Hi3 missing",
                                "WARNING")
                        failed += 1
                        ui_progress(idx, len(folders), fname)
                        continue

                    all_hi2 = [
                        os.path.join(hi2, f)
                        for f in os.listdir(hi2)
                        if os.path.isfile(os.path.join(hi2, f))
                        and not f.startswith(".")]
                    hi3_files = sorted(
                        os.path.join(r, f)
                        for r, _, fs in os.walk(hi3)
                        for f in fs if not f.startswith("."))
                    begin_files = sorted(
                        f for f in all_hi2
                        if "begin" in os.path.basename(f).lower())
                    end_files = sorted(
                        f for f in all_hi2
                        if "end" in os.path.basename(f).lower())

                    if not hi3_files or not begin_files or not end_files:
                        LOG.log("Upload",
                                f"  Skipped {fname}: incomplete files",
                                "WARNING")
                        failed += 1
                        ui_progress(idx, len(folders), fname)
                        continue

                    try:
                        LOG.log("Upload",
                                f"[{idx}/{len(folders)}] {fname}")

                        # Stage 1: HI3 → /etc/vsf/input/HI3/
                        remote_hi3_dir = f"{VSF}/HI3"
                        ensure_dir(remote_hi3_dir)
                        for i, local in enumerate(hi3_files, 1):
                            fn     = os.path.basename(local)
                            remote = f"{remote_hi3_dir}/{fn}"
                            ui_status(
                                f"[{idx}/{len(folders)}] {fname} "
                                f"— Hi3 ({i}/{len(hi3_files)}): {fn}")
                            sftp_d.put(local, remote)
                            LOG.log("Upload", f"  Hi3: {fn} \u2192 {remote}")

                        # 1-second wait
                        ui_status(
                            f"[{idx}/{len(folders)}] {fname} "
                            f"— Hi3 done, waiting 1s\u2026")
                        time.sleep(1)

                        # Stage 2: Hi2 Begin → WatchDir
                        for local in begin_files:
                            fn     = os.path.basename(local)
                            remote = f"{WATCH}/{fn}"
                            ui_status(
                                f"[{idx}/{len(folders)}] {fname} "
                                f"— Hi2 Begin: {fn}")
                            sftp_d.put(local, remote)
                            LOG.log("Upload",
                                    f"  Hi2 Begin: {fn} \u2192 {remote}")

                        # Stage 2: Hi2 End → WatchDir
                        for local in end_files:
                            fn     = os.path.basename(local)
                            remote = f"{WATCH}/{fn}"
                            ui_status(
                                f"[{idx}/{len(folders)}] {fname} "
                                f"— Hi2 End: {fn}")
                            sftp_d.put(local, remote)
                            LOG.log("Upload",
                                    f"  Hi2 End: {fn} \u2192 {remote}")

                        LOG.log("Upload", f"  \u2713 {fname} uploaded")
                        succeeded += 1

                        # Watch tmsc.log for MsgType[OBJ_GEN]
                        # before triggering next call
                        if idx < len(folders):
                            ui_status(
                                f"[{idx}/{len(folders)}] {fname} "
                                f"— watching tmsc.log for "
                                f"{ACK_KEYWORD}\u2026")
                            _wait_obj_gen(fname)

                    except Exception as exc:
                        import traceback
                        LOG.log("Upload",
                                f"  \u2717 {fname}: {exc}\n"
                                + traceback.format_exc(), "ERROR")
                        failed += 1

                    ui_progress(idx, len(folders), fname)

                # All done
                self._write_history(
                    "Voice Upload (Bulk)", succeeded, ip,
                    "\u2705 OK" if failed == 0
                    else f"\u26a0 {succeeded} OK / {failed} failed",
                    f"{len(folders)} calls")

                def _done():
                    try:
                        if top.winfo_exists():
                            top.destroy()
                    except Exception:
                        pass
                    msg = (
                        f"Bulk voice upload finished.\n\n"
                        f"  ✅  {succeeded} call(s) uploaded\n"
                        f"  ❌  {failed} skipped / failed\n\n"
                        f"Waited for  {ACK_KEYWORD}  in\n"
                        f"  {TMSC_LOG}\n"
                        f"before each next call was triggered.")
                    self.popup("Bulk Upload Complete", msg,
                               "success" if failed == 0 else "warning")
                self.after(0, _done)

            except Exception as exc:
                import traceback
                LOG.log("Upload",
                        f"Bulk voice FAILED: {exc}\n"
                        + traceback.format_exc(), "ERROR")
                def _err(m=str(exc)):
                    try: top.destroy()
                    except Exception: pass
                    self.popup("Error",
                        f"Bulk voice upload failed:\n\n{m}\n\n"
                        "Check the Logs tab for full details.", "error")
                self.after(0, _err)
            finally:
                for obj in (sftp_d, ssh_d):
                    try:
                        if obj: obj.close()
                    except Exception: pass
                LOG.log("Upload", "Bulk voice: SSH session closed")

        threading.Thread(target=task, daemon=True).start()

    def _download_success_popup(self, title, detail, local_dir):
        """Success popup with an 'Open Folder' button (#5)."""
        dlg = tk.Toplevel(self)
        dlg.title("Downloaded")
        dlg.configure(bg=self.C("bg"))
        dlg.resizable(False, False)
        dlg.grab_set()
        x = self.winfo_rootx() + self.winfo_width() // 2 - 200
        y = self.winfo_rooty() + self.winfo_height() // 2 - 80
        dlg.geometry(f"400x175+{x}+{y}")

        tk.Frame(dlg, bg=self.C("success"), height=4).pack(fill="x")
        body = tk.Frame(dlg, bg=self.C("bg"))
        body.pack(fill="both", expand=True, padx=20, pady=14)

        tk.Label(body, text=f"✅  {title}",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 11, "bold")).pack(anchor="w")
        tk.Label(body, text=detail,
                 bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT, 8), justify="left",
                 wraplength=360).pack(anchor="w", pady=(4, 12))

        btn_row = tk.Frame(body, bg=self.C("bg"))
        btn_row.pack(anchor="w")

        def _open_folder():
            try:
                import subprocess as _sp
                if sys.platform == "win32":
                    _sp.Popen(f'explorer "{local_dir}"')
                elif sys.platform == "darwin":
                    _sp.Popen(["open", local_dir])
                else:
                    _sp.Popen(["xdg-open", local_dir])
            except Exception:
                pass
            dlg.destroy()

        tk.Button(btn_row, text="📂  Open Folder",
                  bg=self.C("success"), fg="white", relief="flat",
                  font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", padx=12,
                  command=_open_folder).pack(side="left", ipady=5)
        tk.Button(btn_row, text="OK",
                  bg=self.C("input_bg"), fg=self.C("text"), relief="flat",
                  font=(_UI_FONT, 9),
                  cursor="hand2", padx=16,
                  command=dlg.destroy).pack(side="left", padx=(8, 0), ipady=5)

    # ──────────────────────────────────────────────────────────────
    # KAFKA VALIDATION DASHBOARD
    # ──────────────────────────────────────────────────────────────

    # ──────────────────────────────────────────────────────────────
    # TAG VALIDATION LANDING PAGE
    # ──────────────────────────────────────────────────────────────
    def show_tag_validation(self):
        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Validation")

        # Header
        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(22, 4))
        tk.Label(hdr, text="✅  Validation",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")
        tk.Frame(main, bg=self.C("border"), height=1).pack(
            fill="x", padx=50, pady=(4, 30))

        # Cards row
        cards_frame = tk.Frame(main, bg=self.C("bg"))
        cards_frame.pack(anchor="center", pady=20)

        def _make_card(parent, icon, title, subtitle, command, accent):
            # Debounce: ignore repeat calls within 500 ms (prevents triple-fire
            # from nested <Button-1> bindings propagating through outer/card/body)
            _last = [0.0]
            def _once(c=command):
                now = time.time()
                if now - _last[0] < 0.5:
                    return
                _last[0] = now
                c()

            outer = tk.Frame(parent, bg=accent, padx=2, pady=2)
            outer.pack(side="left", padx=30)
            card = tk.Frame(outer, bg=self.C("panel"), width=260, height=200)
            card.pack_propagate(False)
            card.pack()

            top_bar = tk.Frame(card, bg=accent, height=4)
            top_bar.pack(fill="x")
            body = tk.Frame(card, bg=self.C("panel"), cursor="hand2")
            body.pack(fill="both", expand=True, padx=24, pady=20)

            icon_lbl = tk.Label(body, text=icon, bg=self.C("panel"), fg=accent,
                     font=(_UI_FONT, 36), cursor="hand2")
            icon_lbl.pack(anchor="w")
            title_lbl = tk.Label(body, text=title, bg=self.C("panel"), fg=self.C("card_title"),
                     font=(_UI_FONT, 15, "bold"), cursor="hand2")
            title_lbl.pack(anchor="w", pady=(6, 2))
            subtitle_lbl = tk.Label(body, text=subtitle, bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9), wraplength=200, justify="left", cursor="hand2")
            subtitle_lbl.pack(anchor="w")

            btn = tk.Button(body, text=f"Open {title}", bg=accent, fg="white",
                            relief="flat", font=(_UI_FONT, 9, "bold"),
                            cursor="hand2", activebackground=self.C("border"),
                            command=_once)
            btn.pack(anchor="w", pady=(14, 0), ipadx=10, ipady=5)

            for w in (outer, card, top_bar, body, icon_lbl, title_lbl, subtitle_lbl):
                w.bind("<Button-1>", lambda e: _once())

        _make_card(cards_frame,
                   icon="🎯", title="Targets",
                   subtitle="View and validate target details and associated records.",
                   command=self.show_target_details,
                   accent="#8b5cf6")

        _make_card(cards_frame,
                   icon="📊", title="Kafka",
                   subtitle="Validate enriched messages from Kafka topics in real time.",
                   command=self.show_kafka_dashboard,
                   accent=self.C("primary"))

        _make_card(cards_frame,
                   icon="🔍", title="Solr",
                   subtitle="Query and inspect indexed records in the Solr collection.",
                   command=self.show_solr_dashboard,
                   accent="#0ea5e9")

    # ──────────────────────────────────────────────────────────────
    # SOLR DASHBOARD
    # ──────────────────────────────────────────────────────────────
    def show_solr_dashboard(self):
        import urllib.request as _ur
        import urllib.error   as _ue
        import json as _json
        import urllib.parse as _up

        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Solr Dashboard")

        # ── Header ─────────────────────────────────────────────────
        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(22, 4))
        tk.Label(hdr, text="🔍  Solr Query Dashboard",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", padx=10, pady=4,
                  command=self.show_tag_validation).pack(side="right")
        _total_records_var = tk.StringVar(value="")
        tk.Label(hdr, textvariable=_total_records_var,
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 12, "bold")).pack(side="right", padx=(0, 20))
        tk.Frame(main, bg=self.C("border"), height=1).pack(
            fill="x", padx=50, pady=(4, 10))

        # ── Info bar ───────────────────────────────────────────────
        info_bar = tk.Frame(main, bg=self.C("panel"),
                            highlightbackground=self.C("border"),
                            highlightthickness=1)
        info_bar.pack(fill="x", padx=50, pady=(0, 10))
        tk.Frame(info_bar, bg=self.C("primary"), height=3).pack(fill="x")
        ib = tk.Frame(info_bar, bg=self.C("panel"))
        ib.pack(fill="x", padx=16, pady=8)
        for lbl, val in [("Collection", SOLR_COLLECTION), ("Endpoint", SOLR_BASE)]:
            col = tk.Frame(ib, bg=self.C("panel"))
            col.pack(side="left", padx=(0, 32))
            tk.Label(col, text=lbl, bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 7, "bold")).pack(anchor="w")
            tk.Label(col, text=val, bg=self.C("panel"), fg=self.C("card_title"),
                     font=(_UI_FONT, 9)).pack(anchor="w")

        # ── Controls ───────────────────────────────────────────────
        ctrl_outer = tk.Frame(main, bg=self.C("panel"),
                              highlightbackground=self.C("border"),
                              highlightthickness=1)
        ctrl_outer.pack(fill="x", padx=50, pady=(0, 10))

        ctrl = tk.Frame(ctrl_outer, bg=self.C("panel"))
        ctrl.pack(fill="x", padx=12, pady=10)

        # Type selector — auto-fetch on change
        solr_type_var = tk.StringVar(value=list(SOLR_TYPES.keys())[0])
        tk.Label(ctrl, text="Type:", bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(0, 4))
        type_combo = ttk.Combobox(ctrl, textvariable=solr_type_var,
                                  values=list(SOLR_TYPES.keys()),
                                  state="readonly", width=10)
        type_combo.pack(side="left", padx=(0, 10))
        solr_type_var.trace_add("write", lambda *_: self.after(100, _fetch))

        # Search mode + search box
        tk.Frame(ctrl, bg=self.C("border"), width=1).pack(
            side="left", fill="y", pady=2, padx=8)
        _solr_mode_var = tk.StringVar(value="Filter Results")
        tk.Label(ctrl, text="🔍", bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 10)).pack(side="left")
        tk.Label(ctrl, text="Search:", bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(4, 4))
        solr_search_var = tk.StringVar()
        tk.Entry(ctrl, textvariable=solr_search_var, width=18,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"),
                 relief="flat", font=(_UI_FONT, 9)).pack(
            side="left", ipady=5, padx=(0, 4))
        for _sm, _sl in [("Filter Results", "Filter"), ("Query Solr", "Query Solr")]:
            tk.Radiobutton(ctrl, text=_sl, variable=_solr_mode_var, value=_sm,
                           bg=self.C("panel"), fg=self.C("text"),
                           selectcolor=self.C("input_bg"),
                           activebackground=self.C("panel"),
                           font=(_UI_FONT, 8)).pack(side="left", padx=2)
        def _on_search_change(*_):
            if _solr_mode_var.get() == "Filter Results":
                _filter_locally()
            # In Query Solr mode, user clicks Fetch manually

        solr_search_var.trace_add("write", lambda *_: _on_search_change())

        # ── Date range (From / To) ──────────────────────────────────
        tk.Frame(ctrl, bg=self.C("border"), width=1).pack(
            side="left", fill="y", pady=2, padx=8)

        solr_from_var = tk.StringVar(value="")
        solr_to_var   = tk.StringVar(value="")

        def _make_cal_button(label_text, date_var, lbl_ref):
            """Return a label+calendar-icon pair that opens a date picker."""
            lbl = tk.Label(ctrl, text=label_text,
                           bg=self.C("input_bg"), fg=self.C("muted"),
                           font=(_UI_FONT, 9), width=10, anchor="center",
                           relief="flat", padx=4, pady=5)
            lbl.pack(side="left", padx=(0, 2))
            lbl_ref.append(lbl)

            def _open_cal():
                import calendar as _cal
                from datetime import date as _date
                top = tk.Toplevel(self)
                top.title(f"Pick {label_text}")
                top.configure(bg=self.C("bg"))
                top.resizable(False, False)
                top.grab_set()
                x = ctrl.winfo_rootx() + lbl.winfo_x()
                y = ctrl.winfo_rooty() + ctrl.winfo_height() + 4
                top.geometry(f"+{x}+{y}")
                try:
                    cur = _date.fromisoformat(date_var.get())
                except Exception:
                    cur = _date.today()
                state = {"year": cur.year, "month": cur.month}
                hdr2 = tk.Frame(top, bg=self.C("bg"))
                hdr2.pack(fill="x", padx=8, pady=(8, 0))
                month_lbl2 = tk.Label(hdr2, bg=self.C("bg"), fg=self.C("text"),
                                      font=(_UI_FONT, 10, "bold"))
                month_lbl2.pack(side="left", expand=True)
                gf_ref = [None]

                def _build(year, month):
                    state["year"] = year; state["month"] = month
                    month_lbl2.config(text=f"{_cal.month_name[month]}  {year}")
                    if gf_ref[0]: gf_ref[0].destroy()
                    gf = tk.Frame(top, bg=self.C("bg"))
                    gf.pack(padx=8, pady=4)
                    gf_ref[0] = gf
                    for c, d in enumerate(["Mo","Tu","We","Th","Fr","Sa","Su"]):
                        tk.Label(gf, text=d, bg=self.C("bg"), fg=self.C("muted"),
                                 font=(_UI_FONT, 8, "bold"),
                                 width=3).grid(row=0, column=c, pady=(0, 2))
                    for r, week in enumerate(_cal.monthcalendar(year, month), 1):
                        for c, day in enumerate(week):
                            if day == 0:
                                tk.Label(gf, text="", bg=self.C("bg"),
                                         width=3).grid(row=r, column=c)
                            else:
                                ds = f"{year:04d}-{month:02d}-{day:02d}"
                                is_sel = ds == date_var.get()
                                btn = tk.Button(gf, text=str(day), width=3,
                                    relief="flat", cursor="hand2",
                                    bg=self.C("primary") if is_sel else self.C("input_bg"),
                                    fg="white" if is_sel else self.C("text"),
                                    font=(_UI_FONT, 9))
                                btn.config(command=lambda ds2=ds: _pick(ds2))
                                btn.grid(row=r, column=c, padx=1, pady=1)

                def _pick(ds):
                    date_var.set(ds)
                    lbl.config(text=ds, fg=self.C("text"))
                    top.destroy()
                    self.after(100, _fetch)

                def _prev():
                    m, y = state["month"]-1, state["year"]
                    if m < 1: m, y = 12, y-1
                    _build(y, m)
                def _next():
                    m, y = state["month"]+1, state["year"]
                    if m > 12: m, y = 1, y+1
                    _build(y, m)

                tk.Button(hdr2, text="◀", relief="flat", cursor="hand2",
                          bg=self.C("bg"), fg=self.C("text"),
                          command=_prev).pack(side="left")
                tk.Button(hdr2, text="▶", relief="flat", cursor="hand2",
                          bg=self.C("bg"), fg=self.C("text"),
                          command=_next).pack(side="right")
                _build(state["year"], state["month"])
                clr = tk.Frame(top, bg=self.C("bg"))
                clr.pack(fill="x", padx=8, pady=(0, 8))
                tk.Button(clr, text="Clear", relief="flat", cursor="hand2",
                          bg=self.C("input_bg"), fg=self.C("muted"),
                          font=(_UI_FONT, 8),
                          command=lambda: (date_var.set(""),
                                           lbl.config(text=label_text,
                                                       fg=self.C("muted")),
                                           top.destroy(),
                                           self.after(100, _fetch))).pack(fill="x")

            tk.Button(ctrl, text="📅", relief="flat", cursor="hand2",
                      bg=self.C("panel"), fg=self.C("text"),
                      font=(_UI_FONT, 10),
                      command=_open_cal).pack(side="left", padx=(0, 6))

        from_lbl_ref = []
        to_lbl_ref   = []
        tk.Label(ctrl, text="From:", bg=self.C("panel"), fg=self.C("text"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(0, 4))
        _make_cal_button("Select date", solr_from_var, from_lbl_ref)
        tk.Label(ctrl, text="To:", bg=self.C("panel"), fg=self.C("text"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(6, 4))
        _make_cal_button("Select date", solr_to_var, to_lbl_ref)

        # Rows
        tk.Frame(ctrl, bg=self.C("border"), width=1).pack(
            side="left", fill="y", pady=2, padx=8)
        tk.Label(ctrl, text="Rows:", bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(0, 4))
        rows_var = tk.StringVar(value="100")
        ttk.Combobox(ctrl, textvariable=rows_var,
                     values=["50", "100", "200", "500"],
                     state="readonly", width=6).pack(side="left", padx=(0, 10))

        def _make_tooltip(widget, text):
            tip = [None]
            def _show(e):
                tip[0] = tk.Toplevel(widget)
                tip[0].wm_overrideredirect(True)
                tip[0].wm_geometry(f"+{e.x_root+12}+{e.y_root+20}")
                tk.Label(tip[0], text=text, bg="#1e293b", fg="white",
                         font=(_UI_FONT, 8), relief="solid", padx=6, pady=3,
                         borderwidth=1).pack()
            def _hide(e):
                if tip[0]:
                    try: tip[0].destroy()
                    except Exception: pass
                    tip[0] = None
            widget.bind("<Enter>", _show)
            widget.bind("<Leave>", _hide)

        tk.Frame(ctrl, bg=self.C("border"), width=1).pack(side="left", fill="y", pady=2, padx=8)
        fetch_btn = tk.Button(ctrl, text="🔄",
                              bg=self.C("success"), fg="white", relief="flat",
                              font=(_UI_FONT, 12), cursor="hand2",
                              activebackground=self.C("border"),
                              command=lambda: (_solr_offset.__setitem__(0, 0), _fetch()))
        fetch_btn.pack(side="left", ipady=3, ipadx=6, padx=(0, 4))
        _make_tooltip(fetch_btn, "Fetch — Query Solr with current filters")

        clear_icon_btn = tk.Button(ctrl, text="🗑",
                                   bg="#374151", fg="white", relief="flat",
                                   font=(_UI_FONT, 12), cursor="hand2",
                                   activebackground="#4b5563",
                                   command=lambda: _clear_solr_filters())
        clear_icon_btn.pack(side="left", ipady=3, ipadx=6, padx=(0, 4))
        _make_tooltip(clear_icon_btn, "Clear Filters — Reset all search filters")

        export_icon_btn = tk.Button(ctrl, text="⬇",
                                    bg="#0891b2", fg="white", relief="flat",
                                    font=(_UI_FONT, 12), cursor="hand2",
                                    activebackground="#0e7490",
                                    command=lambda: _solr_export())
        export_icon_btn.pack(side="left", ipady=3, ipadx=6)
        _make_tooltip(export_icon_btn, "Export CSV — Download current results as CSV")

        # Clear Filters — function used by button above
        def _clear_solr_filters():
            solr_type_var.set(list(SOLR_TYPES.keys())[0])
            solr_search_var.set("")
            solr_from_var.set("")
            solr_to_var.set("")
            if from_lbl_ref:
                from_lbl_ref[0].config(text="Select date", fg=self.C("muted"))
            if to_lbl_ref:
                to_lbl_ref[0].config(text="Select date", fg=self.C("muted"))

        # (Total Records shown in header above)

        # Status
        status_var = tk.StringVar(value="Click Fetch to query Solr.")
        _solr_status_row = tk.Frame(main, bg=self.C("bg"))
        _solr_status_row.pack(fill="x", padx=50, pady=(0, 4))
        status_lbl = tk.Label(_solr_status_row, textvariable=status_var,
                              bg=self.C("bg"), fg=self.C("muted"),
                              font=(_UI_FONT, 8))
        status_lbl.pack(side="left")
        _solr_fetch_ts   = [None]   # time.time() of last fetch
        _solr_ts_job     = [None]
        _solr_ts_var     = tk.StringVar(value="")
        tk.Label(_solr_status_row, textvariable=_solr_ts_var,
                 bg=self.C("bg"), fg=self.C("dim"),
                 font=(_UI_FONT, 8)).pack(side="right")

        def _tick_solr_ts():
            if _solr_fetch_ts[0] is None:
                return
            ago = int(time.time() - _solr_fetch_ts[0])
            if ago < 60:
                _solr_ts_var.set(f"Last fetched: {ago}s ago")
            elif ago < 3600:
                _solr_ts_var.set(f"Last fetched: {ago//60}m ago")
            else:
                _solr_ts_var.set(f"Last fetched: {ago//3600}h ago")
            try:
                if _solr_status_row.winfo_exists():
                    _solr_ts_job[0] = self.after(30000, _tick_solr_ts)
            except Exception:
                pass

        def _set_status(msg, color=None):
            status_var.set(msg)
            status_lbl.config(fg=color or self.C("muted"))

        # ── Table ──────────────────────────────────────────────────
        tbl_outer = tk.Frame(main, bg=self.C("panel"),
                             highlightbackground=self.C("border"),
                             highlightthickness=1)
        tbl_outer.pack(fill="both", expand=True, padx=50, pady=(0, 10))

        style = ttk.Style()
        style.theme_use("default")
        style.configure("Solr.Treeview",
                        background=self.C("input_bg"),
                        foreground=self.C("text"),
                        rowheight=26, fieldbackground=self.C("input_bg"),
                        borderwidth=0, font=(_UI_FONT, 9))
        style.configure("Solr.Treeview.Heading",
                        background=self.C("primary"), foreground="white",
                        font=(_UI_FONT, 9, "bold"), relief="flat", padding=(4, 6))
        style.map("Solr.Treeview",
                  background=[("selected", self.C("primary"))],
                  foreground=[("selected", "#ffffff")])
        style.map("Solr.Treeview.Heading",
                  background=[("active", self.C("primary")),
                               ("!active", self.C("primary"))],
                  foreground=[("active", "white"), ("!active", "white")])

        cols = ("#", "Type", "Description", "Activity Time",
                "Call Start", "Call End", "Called Number", "Target Number", "Target Name")
        tf = tk.Frame(tbl_outer, bg=self.C("panel"))
        tf.pack(fill="both", expand=True, padx=8, pady=6)
        tf.grid_rowconfigure(0, weight=1)
        tf.grid_columnconfigure(0, weight=1)

        tree = ttk.Treeview(tf, columns=cols, show="headings",
                            style="Solr.Treeview")

        _sort_col = [None]
        _sort_rev = [False]

        def _sort_tree(col):
            if _sort_col[0] == col:
                _sort_rev[0] = not _sort_rev[0]
            else:
                _sort_col[0] = col
                _sort_rev[0] = False
            for c in cols:
                arrow = (" ▲" if not _sort_rev[0] else " ▼") if c == col else ""
                tree.heading(c, text=c + arrow,
                             command=lambda _c=c: _sort_tree(_c))
            items = [(tree.set(k, col), k) for k in tree.get_children("")]
            items.sort(key=lambda x: x[0].lower(), reverse=_sort_rev[0])
            for idx, (_, k) in enumerate(items):
                tree.move(k, "", idx)

        for col, w, anch in [
            ("#",              40, "center"),
            ("Type",           80, "center"),
            ("Description",   200, "w"),
            ("Activity Time", 155, "center"),
            ("Call Start",    140, "center"),
            ("Call End",      140, "center"),
            ("Called Number",  130, "center"),
            ("Target Number",  130, "center"),
            ("Target Name",    180, "w"),
        ]:
            tree.heading(col, text=col, anchor="center", command=lambda c=col: _sort_tree(c))
            tree.column(col, width=w, anchor=anch, minwidth=w, stretch=True)

        vsb = ttk.Scrollbar(tf, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(tf, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        _bind_mousewheel(tree, vsb)

        tree.tag_configure("voice", background="#1a2a1a" if self._theme == "dark" else "#f0fdf4")
        tree.tag_configure("sms",   background="#1a1a2a" if self._theme == "dark" else "#ecfeff")
        tree.tag_configure("ludr",  background="#2a1a1a" if self._theme == "dark" else "#fefce8")
        tree.tag_configure("ip",    background="#1a1a2e" if self._theme == "dark" else "#f5f3ff")

        # ── Bottom bar: type badges + pagination ───────────────────
        _solr_offset = [0]
        _solr_total  = [0]

        bot = tk.Frame(tbl_outer, bg=self.C("panel"))
        bot.pack(fill="x", padx=10, pady=(4, 6))

        # Type badges (left side)
        badges_frame = tk.Frame(bot, bg=self.C("panel"))
        badges_frame.pack(side="left")
        _badge_labels = {}
        for btype, color in [("Voice","#22c55e"),("SMS","#06b6d4"),("LUDR","#f59e0b"),("IP","#8b5cf6")]:
            bf = tk.Frame(badges_frame, bg=color, padx=1, pady=1)
            bf.pack(side="left", padx=(0, 6))
            bl = tk.Label(bf, text=f"{btype}: 0", bg=self.C("panel"),
                          fg=color, font=(_UI_FONT, 8, "bold"), padx=6, pady=2)
            bl.pack()
            _badge_labels[btype] = bl

        def _update_badges(docs):
            counts = {"Voice": 0, "SMS": 0, "LUDR": 0, "IP": 0}
            for doc in docs:
                tc = doc.get(SOLR_TYPE_FIELD, "")
                if isinstance(tc, list): tc = tc[0] if tc else ""
                tc = str(tc)
                if tc == "5001":   counts["Voice"] += 1
                elif tc == "5002": counts["SMS"]   += 1
                elif tc == "5003": counts["LUDR"]  += 1
                else:              counts["IP"]    += 1
            for btype, lbl in _badge_labels.items():
                lbl.config(text=f"{btype}: {counts[btype]}")

        # Pagination (right side)
        pag_frame = tk.Frame(bot, bg=self.C("panel"))
        pag_frame.pack(side="right")
        page_info_var = tk.StringVar(value="")
        tk.Label(pag_frame, textvariable=page_info_var,
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 8)).pack(side="left", padx=(0, 8))
        prev_btn = tk.Button(pag_frame, text="◀ Prev", relief="flat", cursor="hand2",
                             bg=self.C("input_bg"), fg=self.C("text"),
                             font=(_UI_FONT, 8, "bold"),
                             command=lambda: _go_page(-1))
        prev_btn.pack(side="left", ipadx=8, ipady=3, padx=(0, 4))
        next_btn = tk.Button(pag_frame, text="Next ▶", relief="flat", cursor="hand2",
                             bg=self.C("input_bg"), fg=self.C("text"),
                             font=(_UI_FONT, 8, "bold"),
                             command=lambda: _go_page(1))
        next_btn.pack(side="left", ipadx=8, ipady=3)

        def _go_page(direction):
            rows_per_page = int(rows_var.get() or "100")
            new_offset = _solr_offset[0] + direction * rows_per_page
            if new_offset < 0:
                new_offset = 0
            if new_offset >= _solr_total[0] and direction > 0:
                return
            _solr_offset[0] = new_offset
            _fetch()

        def _update_pagination():
            rows_per_page = int(rows_var.get() or "100")
            total  = _solr_total[0]
            offset = _solr_offset[0]
            page   = offset // rows_per_page + 1
            pages  = max(1, (total + rows_per_page - 1) // rows_per_page)
            page_info_var.set(f"Page {page} of {pages}  ({offset+1}–{min(offset+rows_per_page, total)} of {total})")
            prev_btn.config(state="normal" if offset > 0 else "disabled")
            next_btn.config(state="normal" if offset + rows_per_page < total else "disabled")

        tk.Label(bot, text="Double-click row for detail  |  Right-click for options",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 7)).pack(side="left", padx=(12, 0))

        # ── Detail popup / local filter ────────────────────────────
        _all_docs = []

        def _filter_locally(*_):
            """Filter _all_docs in-memory — no Solr call."""
            if _solr_mode_var.get() != "Filter Results":
                return
            sq = solr_search_var.get().strip().lower()
            tree.delete(*tree.get_children())
            _TYPE_TAG = {"5001": "voice", "5002": "sms", "5003": "ludr"}
            def _tag_for(tc):
                return _TYPE_TAG.get(tc, "ip")
            def _fv(doc, key):
                v = doc.get(key, "")
                return (v[0] if v else "") if isinstance(v, list) else str(v or "")
            import datetime as _dt
            def _epoch_to_dt(val):
                s = str(val).strip()
                if not s or s == "None": return ""
                try:
                    ms = int(float(s))
                    if ms > 1_000_000_000_000: ms = ms // 1000
                    return _dt.datetime.fromtimestamp(ms).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    return s
            def _fv_time(doc, key):
                raw = doc.get(key, "")
                if isinstance(raw, list): raw = raw[0] if raw else ""
                return _epoch_to_dt(str(raw))
            count = 0
            for i, doc in enumerate(_all_docs):
                if sq:
                    row_text = " ".join(str(v) for v in doc.values()).lower()
                    if sq not in row_text:
                        continue
                tc  = _fv(doc, SOLR_TYPE_FIELD)
                tag = _tag_for(tc)
                tree.insert("", "end", iid=str(i), tags=(tag,),
                            values=(i+1, tc,
                                    _fv(doc, "_116"),
                                    _fv_time(doc, "_12"),
                                    _fv_time(doc, "_15"),
                                    _fv_time(doc, "_16"),
                                    _fv(doc, "_68"),
                                    _fv(doc, "_182"),
                                    _fv(doc, "_198")))
                count += 1
            _total_records_var.set(
                f"Total Records: {count} of {_solr_total[0]}" if sq
                else f"Total Records: {_solr_total[0]}")

        def _show_detail(event=None):
            sel = tree.selection()
            if not sel:
                return
            try:
                idx = int(sel[0])
            except (ValueError, TypeError):
                return
            if not (0 <= idx < len(_all_docs)):
                return
            doc = _all_docs[idx]

            # Determine type for accent colour
            tc = doc.get(SOLR_TYPE_FIELD, "")
            if isinstance(tc, list): tc = tc[0] if tc else ""
            tc = str(tc)
            type_color = {"5001": "#22c55e", "5002": "#06b6d4",
                          "5003": "#f59e0b"}.get(tc, "#8b5cf6")
            type_label = {"5001": "Voice", "5002": "SMS",
                          "5003": "LUDR"}.get(tc, "IP")

            dlg = tk.Toplevel(self)
            dlg.title("Solr Document Detail")
            dlg.configure(bg=self.C("bg"))
            dlg.resizable(True, True)
            dlg.grab_set()
            x = self.winfo_rootx() + self.winfo_width()  // 2 - 310
            y = self.winfo_rooty() + self.winfo_height() // 2 - 260
            dlg.geometry(f"640x560+{x}+{y}")

            tk.Frame(dlg, bg=type_color, height=4).pack(fill="x")
            body = tk.Frame(dlg, bg=self.C("bg"))
            body.pack(fill="both", expand=True, padx=20, pady=14)

            # Title + type badge
            hdr_row = tk.Frame(body, bg=self.C("bg"))
            hdr_row.pack(fill="x", pady=(0, 6))
            tk.Label(hdr_row, text="📄  Solr Document",
                     bg=self.C("bg"), fg=self.C("card_title"),
                     font=(_UI_FONT, 13, "bold")).pack(side="left")
            tk.Label(hdr_row, text=f" {type_label} ",
                     bg=type_color, fg="white",
                     font=(_UI_FONT, 9, "bold"), padx=6, pady=2).pack(
                side="left", padx=(10, 0))

            tk.Frame(body, bg=self.C("border"), height=1).pack(fill="x", pady=(0, 8))

            # All fields — scrollable list
            tk.Label(body, text="All Fields",
                     bg=self.C("bg"), fg=self.C("muted"),
                     font=(_UI_FONT, 8, "bold")).pack(anchor="w", pady=(0, 4))
            fields_outer = tk.Frame(body, bg=self.C("panel"),
                                    highlightbackground=self.C("border"),
                                    highlightthickness=1)
            fields_outer.pack(fill="x", pady=(0, 8))
            fields_canvas = tk.Canvas(fields_outer, bg=self.C("panel"),
                                      highlightthickness=0, height=160)
            fields_vsb = ttk.Scrollbar(fields_outer, orient="vertical",
                                       command=fields_canvas.yview)
            fields_vsb.pack(side="right", fill="y")
            fields_canvas.pack(fill="both", expand=True)
            fields_canvas.configure(yscrollcommand=fields_vsb.set)
            fgrid = tk.Frame(fields_canvas, bg=self.C("panel"))
            fgrid_win = fields_canvas.create_window((0, 0), window=fgrid, anchor="nw")

            def _on_fgrid_configure(e):
                fields_canvas.configure(scrollregion=fields_canvas.bbox("all"))
            def _on_fields_canvas_resize(e):
                fields_canvas.itemconfig(fgrid_win, width=e.width)
            fgrid.bind("<Configure>", _on_fgrid_configure)
            fields_canvas.bind("<Configure>", _on_fields_canvas_resize)

            all_fields = sorted(doc.keys())
            for i, fld in enumerate(all_fields):
                val = doc.get(fld, "")
                if isinstance(val, list):
                    val = ", ".join(str(v) for v in val)
                display_name = SOLR_FIELDS.get(fld, fld)
                bg = self.C("input_bg") if i % 2 == 0 else self.C("panel")
                row_f = tk.Frame(fgrid, bg=bg)
                row_f.pack(fill="x")
                fgrid.columnconfigure(0, weight=0)
                fgrid.columnconfigure(1, weight=1)
                tk.Label(row_f, text=display_name,
                         bg=bg, fg=self.C("muted"),
                         font=(_UI_FONT, 8), width=22, anchor="w",
                         padx=8, pady=3).pack(side="left")
                tk.Label(row_f, text=str(val) if val != "" else "—",
                         bg=bg, fg=self.C("card_title"),
                         font=(_UI_FONT, 8, "bold"), anchor="w",
                         padx=4, pady=3, wraplength=340).pack(side="left", fill="x", expand=True)

            # Full JSON section
            tk.Label(body, text="Full Solr JSON",
                     bg=self.C("bg"), fg=self.C("muted"),
                     font=(_UI_FONT, 8, "bold")).pack(anchor="w", pady=(4, 2))

            raw_txt = _json.dumps(doc, indent=2, ensure_ascii=False)

            json_frame = tk.Frame(body, bg=self.C("input_bg"),
                                  highlightbackground=self.C("border"),
                                  highlightthickness=1)
            json_frame.pack(fill="both", expand=True)
            txt = tk.Text(json_frame, bg=self.C("input_bg"), fg="#a5f3fc",
                          font=("Consolas", 9), relief="flat",
                          wrap="none", padx=8, pady=6)
            jsb_v = ttk.Scrollbar(json_frame, orient="vertical",   command=txt.yview)
            jsb_h = ttk.Scrollbar(json_frame, orient="horizontal", command=txt.xview)
            txt.configure(yscrollcommand=jsb_v.set, xscrollcommand=jsb_h.set)
            jsb_v.pack(side="right",  fill="y")
            jsb_h.pack(side="bottom", fill="x")
            txt.pack(fill="both", expand=True)
            txt.insert("1.0", raw_txt)
            txt.config(state="disabled")

            # Button row
            btn_row = tk.Frame(body, bg=self.C("bg"))
            btn_row.pack(fill="x", pady=(8, 0))
            tk.Button(btn_row, text="📋  Copy JSON",
                      bg=self.C("input_bg"), fg=self.C("text"),
                      relief="flat", cursor="hand2", font=(_UI_FONT, 9),
                      command=lambda: (self.clipboard_clear(),
                                       self.clipboard_append(raw_txt))).pack(
                side="left", ipadx=10, ipady=4)
            tk.Button(btn_row, text="Close",
                      bg=self.C("input_bg"), fg=self.C("text"),
                      relief="flat", cursor="hand2", font=(_UI_FONT, 9),
                      command=dlg.destroy).pack(side="right", ipadx=12, ipady=4)
            dlg.bind("<Escape>", lambda e: dlg.destroy())

        tree.bind("<Double-1>", _show_detail)

        # ── Right-click context menu ────────────────────────────────
        def _solr_ctx(event):
            row = tree.identify_row(event.y)
            if not row:
                return
            tree.selection_set(row)
            col_id  = tree.identify_column(event.x)
            col_idx = int(col_id.replace("#", "")) - 1 if col_id else -1
            vals    = tree.item(row, "values")
            cell    = str(vals[col_idx]) if 0 <= col_idx < len(vals) else ""
            menu = tk.Menu(self, tearoff=0, bg=self.C("panel"), fg=self.C("text"),
                           font=(_UI_FONT, 9), relief="flat",
                           activebackground=self.C("primary"), activeforeground="white")
            menu.add_command(label="📋  Copy cell value",
                             command=lambda: (self.clipboard_clear(),
                                              self.clipboard_append(cell)))
            menu.add_command(label="📄  Copy row",
                             command=lambda: (self.clipboard_clear(),
                                              self.clipboard_append("\t".join(str(v) for v in vals))))
            menu.add_separator()
            menu.add_command(label="🔍  View detail", command=_show_detail)
            menu.tk_popup(event.x_root, event.y_root)

        tree.bind("<Button-3>", _solr_ctx)

        # ── Keyboard shortcuts ──────────────────────────────────────
        def _solr_keys(event):
            if event.keysym == "F5":
                _solr_offset[0] = 0
                _fetch()
            elif event.keysym == "f" and (event.state & 0x4):  # Ctrl+F
                solr_search_var.set("")
                # focus the search entry — find it by iterating ctrl children
                for w in ctrl.winfo_children():
                    if isinstance(w, tk.Entry):
                        w.focus_set()
                        break
            elif event.keysym == "Escape":
                solr_search_var.set("")
                solr_from_var.set("")
                solr_to_var.set("")
                if from_lbl_ref:
                    from_lbl_ref[0].config(text="Select date", fg=self.C("muted"))
                if to_lbl_ref:
                    to_lbl_ref[0].config(text="Select date", fg=self.C("muted"))

        tree.bind("<Key>", _solr_keys)
        tree.focus_set()

        # ── Fetch ──────────────────────────────────────────────────
        def _fetch():
            _set_status("Querying Solr…", "#f59e0b")
            fetch_btn.config(state="disabled")
            type_code = SOLR_TYPES.get(solr_type_var.get(), "5001")
            sq        = solr_search_var.get().strip()
            rows      = rows_var.get() or "100"

            # Build Solr query
            if type_code == "ALL":
                q = "*:*"
            elif type_code == "IP":
                # IP records are everything that is NOT Voice/SMS/LUDR
                q = f"NOT {SOLR_TYPE_FIELD}:(5001 OR 5002 OR 5003)"
            else:
                q = f"{SOLR_TYPE_FIELD}:{type_code}"
            if sq and _solr_mode_var.get() == "Query Solr":
                q += f" AND ({sq})"
            # Date range filter on _12 (Activity Time) — stored as epoch ms
            import datetime as _dt
            def _date_to_ms(dv, end_of_day=False):
                dv = dv.strip()
                if len(dv) == 10 and dv[4] == "-" and dv[7] == "-":
                    try:
                        _day = _dt.datetime.strptime(dv, "%Y-%m-%d")
                        if end_of_day:
                            _day = _day + _dt.timedelta(days=1)
                            return int(_day.timestamp() * 1000) - 1
                        return int(_day.timestamp() * 1000)
                    except Exception:
                        pass
                return None
            _from_ms = _date_to_ms(solr_from_var.get())
            _to_ms   = _date_to_ms(solr_to_var.get(), end_of_day=True)
            if _from_ms is not None or _to_ms is not None:
                _f = str(_from_ms) if _from_ms is not None else "*"
                _t = str(_to_ms)   if _to_ms   is not None else "*"
                q += f" AND _12:[{_f} TO {_t}]"
            fl = "*"
            params = _up.urlencode({
                "q":      q,
                "q.op":   "OR",
                "wt":     "json",
                "indent": "true",
                "rows":   rows,
                "start":  str(_solr_offset[0]),
                "fl":     fl,
            })
            url = f"{SOLR_SELECT_URL}?{params}"
            LOG.log("Solr", f"Fetching: {url}")

            def _run():
                try:
                    req = _ur.Request(url, headers={
                        "Accept": "application/json"})
                    with _ur.urlopen(req, timeout=30) as resp:
                        data = _json.loads(resp.read().decode("utf-8"))
                    docs  = data.get("response", {}).get("docs", [])
                    total = data.get("response", {}).get("numFound", 0)
                    LOG.log("Solr", f"Got {len(docs)} doc(s) of {total} total")

                    def _ui():
                        _all_docs.clear()
                        _all_docs.extend(docs)
                        _solr_total[0] = total
                        _total_records_var.set(f"Total Records: {total}")
                        _update_badges(docs)
                        _update_pagination()
                        tree.delete(*tree.get_children())
                        _TYPE_TAG = {"5001": "voice", "5002": "sms", "5003": "ludr",
                                     "ludr": "ludr"}
                        def _tag_for(tc):
                            return _TYPE_TAG.get(tc, "ip" if tc not in ("5001","5002","5003") else "sms")
                        import datetime as _dt

                        def _fv(doc, key):
                            v = doc.get(key, "")
                            return (v[0] if v else "") if isinstance(v, list) else str(v or "")

                        def _epoch_to_dt(val):
                            """Convert epoch ms/s to 'YYYY-MM-DD HH:MM:SS', return as-is if not numeric."""
                            s = _fv({"v": val}, "v") if not isinstance(val, str) else val
                            s = s.strip()
                            if not s or s == "None":
                                return ""
                            try:
                                ms = int(float(s))
                                # Distinguish ms from seconds
                                if ms > 1_000_000_000_000:
                                    ms = ms // 1000
                                return _dt.datetime.fromtimestamp(ms).strftime("%Y-%m-%d %H:%M:%S")
                            except Exception:
                                return s

                        def _fv_time(doc, key):
                            raw = doc.get(key, "")
                            if isinstance(raw, list):
                                raw = raw[0] if raw else ""
                            return _epoch_to_dt(str(raw))

                        for i, doc in enumerate(docs):
                            tc  = _fv(doc, SOLR_TYPE_FIELD)
                            tag = _tag_for(tc)
                            tree.insert("", "end", iid=str(i), tags=(tag,),
                                        values=(i+1, tc,
                                                _fv(doc, "_116"),
                                                _fv_time(doc, "_12"),
                                                _fv_time(doc, "_15"),
                                                _fv_time(doc, "_16"),
                                                _fv(doc, "_68"),
                                                _fv(doc, "_182"),
                                                _fv(doc, "_198")))
                        n = len(docs)
                        _set_status(
                            f"✅  {n} record(s) of {total} total — "
                            f"type {type_code} ({solr_type_var.get()})",
                            self.C("success") if n else self.C("muted"))
                        fetch_btn.config(state="normal")
                        _solr_fetch_ts[0] = time.time()
                        _solr_ts_var.set("Last fetched: just now")
                        if _solr_ts_job[0]:
                            self.after_cancel(_solr_ts_job[0])
                        _solr_ts_job[0] = self.after(30000, _tick_solr_ts)
                    self.after(0, _ui)

                except _ue.HTTPError as e:
                    msg = f"HTTP {e.code} {e.reason}"
                    LOG.log("Solr", f"Error: {msg}", "ERROR")
                    self.after(0, lambda m=msg: (
                        _set_status(f"❌  {m}", "#ef4444"),
                        fetch_btn.config(state="normal")))
                except Exception as e:
                    LOG.log("Solr", f"Error: {e}", "ERROR")
                    self.after(0, lambda err=e: (
                        _set_status(f"❌  {err}", "#ef4444"),
                        fetch_btn.config(state="normal")))

            threading.Thread(target=_run, daemon=True).start()

        # ── Action buttons row ─────────────────────────────────────
        def _solr_export():
            import csv as _csv
            import tkinter.filedialog as _fd
            path = _fd.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                title="Export Solr results")
            if not path:
                return
            try:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    w = _csv.writer(f)
                    w.writerow(cols)
                    for iid in tree.get_children():
                        w.writerow(tree.item(iid, "values"))
                _set_status(f"✅  Exported {len(tree.get_children())} rows to {path}",
                            self.C("success"))
            except Exception as ex:
                _set_status(f"❌  Export failed: {ex}", "#ef4444")

        # Auto-fetch always on open
        self.after(400, _fetch)

    def show_kafka_dashboard(self):
        import urllib.request as _ur
        import urllib.error   as _ue
        import json as _json

        def _kafka_fmt_ts(raw_ts):
            """Convert Kafka timestamp to readable datetime string.
            Kafka returns milliseconds since epoch as an integer.
            Falls back gracefully for ISO strings or empty values.
            """
            if raw_ts is None or raw_ts == "":
                return ""
            # Numeric ms-epoch (int or float-like string)
            try:
                ms = int(float(str(raw_ts)))
                if ms > 1_000_000_000_000:          # looks like ms
                    ms_val = ms
                elif ms > 1_000_000_000:             # looks like seconds
                    ms_val = ms * 1000
                else:
                    raise ValueError("not an epoch")
                from datetime import datetime as _DT, timezone as _tz, timedelta as _td
                # Kafka stores UTC ms; Kafka UI displays in IST (UTC+5:30)
                _IST = _tz(_td(hours=5, minutes=30))
                return (_DT.fromtimestamp(ms_val / 1000, tz=_tz.utc)
                        .astimezone(_IST)
                        .strftime("%Y-%m-%d %H:%M:%S"))
            except (ValueError, TypeError, OSError):
                pass
            # Already an ISO/string — convert UTC→IST if it ends with Z
            s = str(raw_ts)
            try:
                from datetime import datetime as _DT, timezone as _tz, timedelta as _td
                _IST = _tz(_td(hours=5, minutes=30))
                for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                            "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
                    try:
                        dt = _DT.strptime(s[:26], fmt)
                        return (dt.replace(tzinfo=_tz.utc)
                                .astimezone(_IST)
                                .strftime("%Y-%m-%d %H:%M:%S"))
                    except ValueError:
                        continue
            except Exception:
                pass
            return s[:19].replace("T", " ")

        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Kafka Validation Dashboard")

        # ── Header ─────────────────────────────────────────────────
        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(22, 4))
        tk.Label(hdr, text="📊  Kafka Validation Dashboard",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", padx=10, pady=4,
                  command=self.show_tag_validation).pack(side="right")

        # Topic selector
        if self._kafka_topic_var is None:
            self._kafka_topic_var = tk.StringVar(value=list(KAFKA_TOPICS.keys())[0])
        topic_sel_frame = tk.Frame(hdr, bg=self.C("bg"))
        topic_sel_frame.pack(side="left", padx=(24, 0))
        tk.Label(topic_sel_frame, text="Topic:",
                 bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(side="left", padx=(0, 6))
        topic_combo = ttk.Combobox(topic_sel_frame,
                                   textvariable=self._kafka_topic_var,
                                   values=list(KAFKA_TOPICS.keys()),
                                   state="readonly", width=18)
        topic_combo.pack(side="left")


        tk.Frame(main, bg=self.C("border"), height=1).pack(
            fill="x", padx=50, pady=(4, 10))

        # ── Topic info bar ──────────────────────────────────────────
        info_bar = tk.Frame(main,
                            bg=self.C("panel"),
                            highlightbackground=self.C("border"),
                            highlightthickness=1)
        info_bar.pack(fill="x", padx=50, pady=(0, 10))
        tk.Frame(info_bar, bg=self.C("primary"), height=3).pack(fill="x")

        ib = tk.Frame(info_bar, bg=self.C("panel"))
        ib.pack(fill="x", padx=16, pady=10)

        topic_name_var = tk.StringVar(value=KAFKA_TOPICS[self._kafka_topic_var.get()])
        self._kafka_topic_var.trace_add(
            "write", lambda *_: topic_name_var.set(
                KAFKA_TOPICS.get(self._kafka_topic_var.get(), "")))

        for lbl, val in [
            ("Cluster",  KAFKA_CLUSTER),
            ("Topic",    None),          # dynamic — set below
            ("Endpoint", KAFKA_UI_BASE),
        ]:
            col = tk.Frame(ib, bg=self.C("panel"))
            col.pack(side="left", padx=(0, 32))
            tk.Label(col, text=lbl,
                     bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 7, "bold")).pack(anchor="w")
            if val is None:
                tk.Label(col, textvariable=topic_name_var,
                         bg=self.C("panel"), fg=self.C("card_title"),
                         font=(_UI_FONT, 9)).pack(anchor="w")
            else:
                tk.Label(col, text=val,
                         bg=self.C("panel"), fg=self.C("card_title"),
                         font=(_UI_FONT, 9)).pack(anchor="w")

        # ── Control bar ─────────────────────────────────────────────
        ctrl_outer = tk.Frame(main, bg=self.C("panel"),
                              highlightbackground=self.C("border"),
                              highlightthickness=1)
        ctrl_outer.pack(fill="x", padx=50, pady=(0, 10))

        ctrl = tk.Frame(ctrl_outer, bg=self.C("panel"))
        ctrl.pack(fill="x", padx=12, pady=10)

        # Reuse persisted filter state across navigation
        if self._kafka_filter_var is None:
            self._kafka_filter_var    = tk.StringVar(value="ALL")
            self._kafka_search_var    = tk.StringVar(value="")
            self._kafka_date_var      = tk.StringVar(value="")   # legacy
            self._kafka_date_from_var = tk.StringVar(value="")
            self._kafka_date_to_var   = tk.StringVar(value="")
            self._kafka_limit_var     = tk.StringVar(value="500")
        filter_var    = self._kafka_filter_var
        search_var    = self._kafka_search_var
        date_from_var = self._kafka_date_from_var
        date_to_var   = self._kafka_date_to_var
        limit_var     = self._kafka_limit_var
        status_var    = tk.StringVar(value="Click Fetch to load messages from Kafka.")
        refresh_ts    = tk.StringVar(value="")
        _auto_job     = [None]
        _all_messages = self._kafka_messages   # persisted across navigation

        # Filter pills — only meaningful for Voice topic
        _filter_outer = tk.Frame(ctrl, bg=self.C("panel"))
        _filter_outer.pack(side="left")
        filter_frame = tk.Frame(_filter_outer, bg=self.C("panel"))
        filter_frame.pack(side="left")
        tk.Label(filter_frame, text="Filter:",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(0, 8))
        _filter_btns = {}
        for val, lbl in [("ALL","All"), ("SMS","SMS"), ("LBS","LBS"),
                         ("VOICE","Voice"), ("IP","IP")]:
            rb = tk.Radiobutton(filter_frame, text=lbl, variable=filter_var, value=val,
                                bg=self.C("panel"), fg=self.C("text"),
                                selectcolor=self.C("input_bg"),
                                activebackground=self.C("panel"),
                                font=(_UI_FONT, 9, "bold"),
                                command=lambda: _apply_filter())
            rb.pack(side="left", padx=6)
            _filter_btns[val] = rb
        _filter_pill_sep = tk.Frame(_filter_outer, bg=self.C("border"), width=1)
        _filter_pill_sep.pack(side="left", fill="y", pady=2, padx=10)

        def _update_kafka_filter_vis(*_):
            topic = self._kafka_topic_var.get() if self._kafka_topic_var else "Voice"
            if topic == "IP":
                _filter_outer.pack_forget()
                filter_var.set("ALL")
            else:
                _filter_outer.pack(side="left")
                if "IP" in _filter_btns:
                    _filter_btns["IP"].pack_forget()
                if filter_var.get() == "IP":
                    filter_var.set("ALL")
        self._kafka_topic_var.trace_add("write", _update_kafka_filter_vis)
        _update_kafka_filter_vis()

        # Search
        tk.Frame(ctrl, bg=self.C("border"), width=1).pack(
            side="left", fill="y", pady=2, padx=10)
        tk.Label(ctrl, text="🔍",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 10)).pack(side="left")
        tk.Label(ctrl, text="Search:",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(4, 4))
        tk.Entry(ctrl, textvariable=search_var, width=22,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"),
                 relief="flat", font=(_UI_FONT, 9)).pack(
            side="left", ipady=5, padx=(0, 4))
        _kafka_match_var = tk.StringVar(value="")
        tk.Label(ctrl, textvariable=_kafka_match_var,
                 bg=self.C("panel"), fg="#22c55e",
                 font=(_UI_FONT, 8, "bold")).pack(side="left", padx=(0, 8))
        search_var.trace_add("write", lambda *_: _apply_filter())

        # ── Date range filter ──────────────────────────────────────
        tk.Frame(ctrl, bg=self.C("border"), width=1).pack(
            side="left", fill="y", pady=2, padx=8)

        def _make_cal_btn(parent, var, placeholder):
            """Return a label-button that opens a calendar popup for var."""
            lbl = tk.Label(parent,
                           text=var.get() if var.get() else placeholder,
                           bg=self.C("input_bg"),
                           fg=self.C("text") if var.get() else self.C("muted"),
                           font=(_UI_FONT, 9), width=11, anchor="center",
                           relief="flat", padx=4, pady=5, cursor="hand2")

            def _open():
                import calendar as _cal
                from datetime import date as _date
                top = tk.Toplevel(self)
                top.title("Pick a date")
                top.configure(bg=self.C("bg"))
                top.resizable(False, False)
                top.grab_set()
                top.geometry(f"+{lbl.winfo_rootx()}+{lbl.winfo_rooty() + lbl.winfo_height() + 4}")

                try:
                    cur = _date.fromisoformat(var.get())
                except Exception:
                    cur = _date.today()
                state = {"year": cur.year, "month": cur.month}

                hdr2 = tk.Frame(top, bg=self.C("bg"))
                hdr2.pack(fill="x", padx=8, pady=(8, 0))
                mlbl = tk.Label(hdr2, bg=self.C("bg"), fg=self.C("text"),
                                font=(_UI_FONT, 10, "bold"))
                mlbl.pack(side="left", expand=True)
                gf_ref = [None]

                def _build(year, month):
                    state["year"] = year; state["month"] = month
                    mlbl.config(text=f"{_cal.month_name[month]}  {year}")
                    if gf_ref[0]: gf_ref[0].destroy()
                    gf = tk.Frame(top, bg=self.C("bg"))
                    gf.pack(padx=8, pady=4)
                    gf_ref[0] = gf
                    for ci, d in enumerate(["Mo","Tu","We","Th","Fr","Sa","Su"]):
                        tk.Label(gf, text=d, bg=self.C("bg"),
                                 fg=self.C("muted"), font=(_UI_FONT, 8, "bold"),
                                 width=3).grid(row=0, column=ci, pady=(0, 2))
                    for r, week in enumerate(_cal.monthcalendar(year, month), 1):
                        for ci, day in enumerate(week):
                            if day == 0:
                                tk.Label(gf, text="", bg=self.C("bg"),
                                         width=3).grid(row=r, column=ci)
                            else:
                                ds = f"{year:04d}-{month:02d}-{day:02d}"
                                is_sel = ds == var.get()
                                b = tk.Button(gf, text=str(day), width=3,
                                              relief="flat", cursor="hand2",
                                              bg=self.C("primary") if is_sel else self.C("input_bg"),
                                              fg="white" if is_sel else self.C("text"),
                                              font=(_UI_FONT, 9),
                                              command=lambda s=ds: _pick(s))
                                b.grid(row=r, column=ci, padx=1, pady=1)

                def _pick(ds):
                    var.set(ds)
                    lbl.config(text=ds, fg=self.C("text"))
                    top.destroy()
                    _apply_filter()

                def _prev():
                    m, y = state["month"]-1, state["year"]
                    if m < 1: m, y = 12, y-1
                    _build(y, m)
                def _next():
                    m, y = state["month"]+1, state["year"]
                    if m > 12: m, y = 1, y+1
                    _build(y, m)

                tk.Button(hdr2, text="◀", relief="flat", cursor="hand2",
                          bg=self.C("bg"), fg=self.C("text"), command=_prev).pack(side="left")
                tk.Button(hdr2, text="▶", relief="flat", cursor="hand2",
                          bg=self.C("bg"), fg=self.C("text"), command=_next).pack(side="right")
                _build(state["year"], state["month"])

                clr = tk.Frame(top, bg=self.C("bg"))
                clr.pack(fill="x", padx=8, pady=(0, 8))
                tk.Button(clr, text="Clear", relief="flat", cursor="hand2",
                          bg=self.C("input_bg"), fg=self.C("muted"),
                          font=(_UI_FONT, 8),
                          command=lambda: (var.set(""),
                                           lbl.config(text=placeholder, fg=self.C("muted")),
                                           top.destroy(),
                                           _apply_filter())).pack(fill="x")

            lbl.bind("<Button-1>", lambda e: _open())
            return lbl

        tk.Label(ctrl, text="From:", bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(0, 3))
        _make_cal_btn(ctrl, date_from_var, "Start date").pack(side="left", padx=(0, 4))
        tk.Label(ctrl, text="To:", bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(0, 3))
        _make_cal_btn(ctrl, date_to_var, "End date").pack(side="left", padx=(0, 8))

        # Limit
        tk.Frame(ctrl, bg=self.C("border"), width=1).pack(
            side="left", fill="y", pady=2, padx=8)
        tk.Label(ctrl, text="Limit:",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(0, 4))
        ttk.Combobox(ctrl, textvariable=limit_var,
                     values=["50","100","200","500","1000"],
                     state="readonly", width=6).pack(side="left", padx=(0, 12))

        # Fetch button
        fetch_btn = tk.Button(ctrl, text="🔄",
                              bg=self.C("success"), fg="white", relief="flat",
                              font=(_UI_FONT, 14), cursor="hand2",
                              activebackground=self.C("border"),
                              command=lambda: _fetch())
        fetch_btn.pack(side="left", ipady=4, ipadx=8, padx=(0, 6))
        _add_tooltip(fetch_btn, "Fetch messages from Kafka")

        # Export CSV button
        def _kafka_export():
            import csv as _csv
            import tkinter.filedialog as _fd
            path = _fd.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                title="Export Kafka messages")
            if not path:
                return
            try:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    w = _csv.writer(f)
                    w.writerow(cols)
                    for iid in tree.get_children():
                        w.writerow(tree.item(iid, "values"))
                _set_status(f"✅  Exported {len(tree.get_children())} rows to {path}", "ok")
            except Exception as ex:
                _set_status(f"❌  Export failed: {ex}", "error")

        export_btn = tk.Button(ctrl, text="⬇", relief="flat", cursor="hand2",
                               bg="#374151", fg="white",
                               font=(_UI_FONT, 14),
                               activebackground=self.C("border"),
                               command=_kafka_export)
        export_btn.pack(side="left", ipady=4, ipadx=8, padx=(0, 8))
        _add_tooltip(export_btn, "Export visible messages to CSV")

        # ── Status bar ──────────────────────────────────────────────
        sb = tk.Frame(main, bg=self.C("bg"))
        sb.pack(fill="x", padx=50, pady=(0, 6))
        sb_dot = tk.Label(sb, text="●",
                          bg=self.C("bg"), fg=self.C("muted"),
                          font=(_UI_FONT, 9))
        sb_dot.pack(side="left")
        tk.Label(sb, textvariable=status_var,
                 bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT, 8)).pack(side="left", padx=(4, 0))
        tk.Label(sb, textvariable=refresh_ts,
                 bg=self.C("bg"), fg=self.C("dim"),
                 font=(_UI_FONT, 7, "italic")).pack(side="right")

        _kafka_ts_job = [None]
        def _tick_kafka_ts():
            if self._kafka_last_fetch is None:
                return
            ago = int(time.time() - self._kafka_last_fetch)
            if ago < 60:
                refresh_ts.set(f"Last fetched: {ago}s ago")
            elif ago < 3600:
                refresh_ts.set(f"Last fetched: {ago//60}m ago")
            else:
                refresh_ts.set(f"Last fetched: {ago//3600}h ago")
            try:
                if sb.winfo_exists():
                    _kafka_ts_job[0] = self.after(30000, _tick_kafka_ts)
            except Exception:
                pass

        def _start_kafka_ts_tick():
            if _kafka_ts_job[0]:
                try: self.after_cancel(_kafka_ts_job[0])
                except Exception: pass
            _kafka_ts_job[0] = self.after(30000, _tick_kafka_ts)

        def _set_status(msg, state="idle"):
            color = {"idle":    self.C("muted"),
                     "working": "#f59e0b",
                     "ok":      self.C("success"),
                     "error":   "#ef4444"}.get(state, self.C("muted"))
            status_var.set(msg)
            sb_dot.config(fg=color)

        # ── Message table ───────────────────────────────────────────
        tbl_outer = tk.Frame(main,
                             highlightbackground=self.C("border"),
                             highlightthickness=1,
                             bg=self.C("panel"))
        tbl_outer.pack(fill="both", expand=True, padx=50, pady=(0, 10))
        tk.Frame(tbl_outer, bg=self.C("border"), height=1).pack(fill="x")

        tf = tk.Frame(tbl_outer, bg=self.C("panel"))
        tf.pack(fill="both", expand=True, padx=8, pady=6)
        tf.grid_rowconfigure(0, weight=1)
        tf.grid_columnconfigure(0, weight=1)

        style = ttk.Style()
        style.theme_use("default")
        style.configure("Kafka.Treeview",
                        background=self.C("input_bg"),
                        foreground=self.C("text"),
                        rowheight=26,
                        fieldbackground=self.C("input_bg"),
                        borderwidth=0,
                        font=(_UI_FONT, 9))
        style.configure("Kafka.Treeview.Heading",
                        background=self.C("primary"),
                        foreground="white",
                        font=(_UI_FONT, 9, "bold"),
                        relief="flat",
                        padding=(4, 6))
        style.map("Kafka.Treeview",
                  background=[("selected", self.C("primary"))],
                  foreground=[("selected", "#ffffff")])
        style.map("Kafka.Treeview.Heading",
                  background=[("active", self.C("primary")),
                               ("!active", self.C("primary"))],
                  foreground=[("active", "white"),
                               ("!active", "white")])

        cols = ("#", "Type", "Target Number", "Target Name", "CIN",
                "Description", "Timestamp", "Offset")
        tree = ttk.Treeview(tf, columns=cols, show="headings",
                            style="Kafka.Treeview")

        _k_sort_col = [None]
        _k_sort_rev = [False]

        def _k_sort_tree(col):
            if _k_sort_col[0] == col:
                _k_sort_rev[0] = not _k_sort_rev[0]
            else:
                _k_sort_col[0] = col
                _k_sort_rev[0] = False
            for c in cols:
                arrow = (" ▲" if not _k_sort_rev[0] else " ▼") if c == col else ""
                tree.heading(c, text=c + arrow,
                             command=lambda _c=c: _k_sort_tree(_c))
            items = [(tree.set(k, col), k) for k in tree.get_children("")]
            items.sort(key=lambda x: x[0].lower(), reverse=_k_sort_rev[0])
            for idx, (_, k) in enumerate(items):
                tree.move(k, "", idx)

        for col, w, anch in [
            ("#",             40, "center"),
            ("Type",          80, "center"),
            ("Target Number", 140, "center"),
            ("Target Name",  150, "w"),
            ("CIN",          110, "center"),
            ("Description",  200, "w"),
            ("Timestamp",    155, "center"),
            ("Offset",        70, "center"),
        ]:
            tree.heading(col, text=col, anchor="center", command=lambda c=col: _k_sort_tree(c))
            tree.column(col, width=w, anchor=anch, minwidth=w, stretch=True)

        tree.tag_configure("voice",     background="#1a2a1a" if self._theme == "dark"
                           else "#f0fdf4")
        tree.tag_configure("sms",       background="#1a1a2a" if self._theme == "dark"
                           else "#ecfeff")
        tree.tag_configure("ludr",      background="#2a1a1a" if self._theme == "dark"
                           else "#fefce8")
        tree.tag_configure("ip",        background="#1a1a2e" if self._theme == "dark"
                           else "#f5f3ff")
        tree.tag_configure("odd",   background=self.C("panel"))
        tree.tag_configure("even",  background=self.C("input_bg"))
        tree.tag_configure("search_match",
                           background="#0c4a3a" if self._theme == "dark" else "#d1fae5",
                           font=(_UI_FONT, 9, "bold"))

        vsb = ttk.Scrollbar(tf, orient="vertical",   command=tree.yview)
        hsb = ttk.Scrollbar(tf, orient="horizontal",  command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        _bind_mousewheel(tree, vsb)

        # Empty state label
        empty_lbl = tk.Label(tf,
                             text="No messages loaded\n\nClick  🔄 Fetch  to pull the latest messages from Kafka.",
                             bg=self.C("input_bg"), fg=self.C("dim"),
                             font=(_UI_FONT, 10), justify="center")

        def _show_empty(show):
            if show:
                empty_lbl.place(relx=0.5, rely=0.45, anchor="center")
            else:
                empty_lbl.place_forget()

        _show_empty(True)

        # ── Parse + apply filter ────────────────────────────────────
        def _parse_message(raw):
            """Extract structured fields from a Kafka message dict."""
            import csv as _csv, io as _io

            content_str = raw.get("content", raw.get("value", ""))
            if isinstance(content_str, bytes):
                content_str = content_str.decode("utf-8", errors="replace")

            target_number = ""
            target_name   = ""
            cin           = ""
            description   = ""
            msg_type      = ""

            # ── Try JSON first ──────────────────────────────────────
            if isinstance(content_str, str):
                stripped = content_str.strip()
                if stripped.startswith("{") or stripped.startswith("["):
                    try:
                        content = _json.loads(stripped)
                        if isinstance(content, dict):
                            def _get(*keys):
                                for k in keys:
                                    for variant in (k, k.lower(), k.upper(),
                                                    k[0].upper() + k[1:]):
                                        v = content.get(variant)
                                        if v:
                                            return str(v)
                                return ""
                            msg_type      = (_get("type", "dataType", "messageType")).upper()
                            target_number = _get("targetNumber", "target_number", "msisdn")
                            target_name   = _get("targetName", "target_name", "name")
                            cin           = _get("cin", "CIN", "id")
                            description   = _get("description", "Description")
                    except Exception:
                        pass

            # ── CSV fallback ──────────────────────────────────────────
            if isinstance(content_str, str) and content_str.strip() and not msg_type:
                try:
                    row = next(_csv.reader(_io.StringIO(content_str)))
                    if not row:
                        return None

                    col0 = row[0].strip().upper()

                    # Determine format by selected topic (exact match to avoid
                    # "IP" matching inside "Voice Input")
                    _cur_topic = self._kafka_topic_var.get() if self._kafka_topic_var else ""
                    _is_ip = _cur_topic == "IP"

                    if _is_ip:
                        # ── IP Input / IP Output ──────────────────────────
                        # Col 0  : TRANSACTIONTYPE (numeric)
                        # Col 2  : MOBILENUMBER    → target number (primary)
                        # Col 8  : DOMAIN          → description
                        # Col 41 : TRANSACTIONID   → CIN
                        # Col 102: ISCALLERNUMBER  → target number (fallback)
                        # Col 215: ALLMATCHEDFILTERNAME → target name
                        msg_type = "IP"
                        if len(row) > 2:
                            target_number = row[2].strip()
                        if not target_number and len(row) > 102:
                            target_number = row[102].strip()
                        if len(row) > 8:
                            description = row[8].strip()
                        if not description and len(row) > 3:
                            description = row[3].strip()   # SERVERIP fallback
                        if len(row) > 41:
                            cin = row[41].strip()
                        if len(row) > 215:
                            val = row[215].strip()
                            if val:
                                parts = val.split(":")
                                # "-1:Trg_8989116789" → "Trg_8989116789"
                                if parts[0] in ("-1", "") and len(parts) > 1 and parts[1]:
                                    target_name = parts[1]
                                else:
                                    target_name = parts[0] or val
                    else:
                        # ── Voice Input / Voice Output / SMS / LUDR ───────
                        # Col 0  : PROTOCOL type code (5001/5002/5003)
                        # Col 4  : LIID/MSISDN     → target number
                        # Col 5  : CIN
                        # Col 6  : ISDISPLAYSTRING → description
                        # Col 191: ALLMATCHEDFILTERNAME → target name
                        _TYPE_CODE = {"5001": "VOICE", "5002": "SMS", "5003": "LUDR"}
                        msg_type = _TYPE_CODE.get(row[0].strip(), "")
                        if len(row) > 6:
                            target_number = row[4].strip()
                            cin           = row[5].strip()
                            description   = row[6].strip()
                        if len(row) > 191:
                            val = row[191].strip()
                            if val:
                                parts = val.split(":")
                                if val.upper().startswith("TRG_") and len(parts) >= 2 \
                                        and parts[1] not in ("-1", ""):
                                    target_name = parts[1]
                                else:
                                    target_name = parts[0].strip() or val

                    # Fallback: Kafka message key
                    if not target_name:
                        key = str(raw.get("key", "") or "").strip()
                        if key and key not in ("null", ""):
                            target_name = key
                except Exception:
                    pass

            if not msg_type:
                msg_type = "UNKNOWN"

            return {
                "type":          msg_type,
                "target_number": target_number,
                "target_name":   target_name,
                "cin":           cin,
                "description":   description,
                "timestamp":     _kafka_fmt_ts(raw.get("timestamp", "")),
                "offset":        str(raw.get("offset", "")),
                "raw":           raw,
            }

        _NEW_HIGHLIGHT_SECS = 300  # 5 minutes

        def _apply_filter(*_):
            try:
                if not tree.winfo_exists():
                    return
            except Exception:
                return
            fv  = filter_var.get()
            sq  = search_var.get().strip().lower()
            dfr = date_from_var.get().strip()
            dto = date_to_var.get().strip()
            _valid_date = lambda s: len(s) == 10 and s[4] == "-" and s[7] == "-"
            has_from = _valid_date(dfr)
            has_to   = _valid_date(dto)

            tree.delete(*tree.get_children())
            _topic_is_ip = self._kafka_topic_var and \
                           self._kafka_topic_var.get() == "IP"
            visible = []
            for real_idx, m in enumerate(_all_messages):
                # IP topic: show all records, skip type/date/search filters
                if not _topic_is_ip:
                    if fv == "SMS"   and m["type"] != "SMS":
                        continue
                    if fv == "LBS"   and m["type"] not in ("LBS", "LUDR"):
                        continue
                    if fv == "VOICE" and m["type"] != "VOICE":
                        continue
                    if fv == "IP"    and m["type"] != "IP":
                        continue
                    ts = m["timestamp"][:10]   # YYYY-MM-DD portion
                    if ts:  # skip date filter for records with missing/unparseable timestamp
                        if has_from and ts < dfr:
                            continue
                        if has_to   and ts > dto:
                            continue
                if sq and not any(sq in str(v).lower()
                                  for v in m.values()):
                    continue
                visible.append((real_idx, m))

            total = len(_all_messages)
            shown = len(visible)
            _show_empty(shown == 0)

            # Show why nothing is visible if messages exist but all filtered out
            if total > 0 and shown == 0:
                hint = []
                if has_from or has_to:
                    hint.append(f"date {dfr or '…'} → {dto or '…'}")
                if fv != "ALL":
                    hint.append(f"type={fv}")
                if sq:
                    hint.append(f"search='{sq}'")
                empty_lbl.config(
                    text=f"{total} message(s) fetched but all filtered out.\n"
                         f"Active filters: {', '.join(hint) if hint else 'none'}\n\n"
                         f"Clear filters or change the date range to see messages.")

            for i, (real_idx, m) in enumerate(visible):
                base_tag = ("voice" if m["type"] == "VOICE"
                            else ("ludr" if m["type"] == "LUDR"
                                  else ("ip" if m["type"] == "IP" else "sms")))
                tags = (base_tag, "search_match") if sq else (base_tag,)
                tree.insert("", "end", iid=str(real_idx), tags=tags, values=(
                    i + 1,
                    m["type"],
                    m["target_number"],
                    m["target_name"],
                    m["cin"],
                    m["description"],
                    m["timestamp"],
                    m["offset"],
                ))
            # Update match count label
            if sq:
                _kafka_match_var.set(f"({shown} match{'es' if shown != 1 else ''})")
            else:
                _kafka_match_var.set("")

        # ── Fetch from Kafka UI API ─────────────────────────────────
        def _fetch():
            _set_status("Fetching messages from Kafka…", "working")
            fetch_btn.config(state="disabled")
            limit = limit_var.get() or "50"

            def _run():
                try:
                    _selected    = self._kafka_topic_var.get()
                    active_topic = KAFKA_TOPICS.get(_selected, KAFKA_TOPIC)

                    # Build seek parameters
                    import datetime as _dt
                    from datetime import timezone as _tz, timedelta as _td
                    _IST = _tz(_td(hours=5, minutes=30))
                    _dfr = date_from_var.get().strip()
                    _dto = date_to_var.get().strip()
                    _valid_d = lambda s: len(s) == 10 and s[4] == "-" and s[7] == "-"
                    _date_from_ok = _valid_d(_dfr)
                    _today_str = _dt.date.today().isoformat()
                    _end_is_today_or_unset = (not _valid_d(_dto)) or _dto >= _today_str

                    if _date_from_ok and not _end_is_today_or_unset:
                        # Both dates set and end is in the past — seek FORWARD from start date.
                        # Kafka UI requires seekTo as "partition::timestamp_ms" pairs.
                        # We cover partitions 0–15 to handle any topic.
                        _day_ist = _dt.datetime.strptime(_dfr, "%Y-%m-%d")
                        _day_utc = _day_ist.replace(tzinfo=_IST).astimezone(_tz.utc)
                        _day_ms  = int(_day_utc.timestamp() * 1000)
                        _seek_to = ",".join(f"{p}::{_day_ms}" for p in range(16))
                        _seek = f"seekType=TIMESTAMP&seekTo={_seek_to}&seekDirection=FORWARD"
                    else:
                        # End date is today/unset — fetch latest N messages newest-first
                        # so today's messages are always in the batch, then filter client-side.
                        _seek = "seekType=LATEST&seekDirection=BACKWARD"

                    url = f"{_kafka_api_url(active_topic)}?limit={limit}&{_seek}"
                    LOG.log("Kafka", f"Fetching: selected='{_selected}' "
                            f"topic='{active_topic}' seek={'date:'+_dfr if _date_from_ok else 'latest'}")
                    req = _ur.Request(url,
                                      headers={"Accept": "text/event-stream, application/json, */*"})
                    with _ur.urlopen(req, timeout=60) as resp:
                        raw = resp.read().decode("utf-8").strip()
                    if not raw:
                        raise ValueError("Kafka UI returned an empty response.")

                    msgs = _parse_sse_or_json(raw, _json)
                    LOG.log("Kafka", f"[DEBUG] Topic={active_topic} | "
                            f"SSE {len(msgs)} msg(s) | raw {len(raw)} chars")
                    if msgs:
                        sample = msgs[0]
                        content_preview = str(sample.get("content", sample.get("value", "")))[:120]
                        LOG.log("Kafka", f"[DEBUG] First msg keys: {list(sample.keys())}")
                        LOG.log("Kafka", f"[DEBUG] First msg content: {content_preview}")

                    all_parsed = [_parse_message(m) for m in msgs]
                    parsed     = [p for p in all_parsed if p is not None]
                    skipped    = len(all_parsed) - len(parsed)
                    if skipped:
                        LOG.log("Kafka", f"[DEBUG] {skipped} row(s) skipped (header/unparseable)")
                    LOG.log("Kafka", f"Fetched {len(parsed)} message(s) from {active_topic}")

                    def _ui():
                        _all_messages.clear()
                        _all_messages.extend(parsed)
                        _apply_filter()
                        _update_k_badges(parsed)
                        n = len(parsed)
                        _set_status(
                            f"✅  {n} message{'s' if n != 1 else ''} fetched "
                            f"from {active_topic}", "ok")
                        self._kafka_last_fetch = time.time()
                        refresh_ts.set("Last fetched: just now")
                        _start_kafka_ts_tick()
                        fetch_btn.config(state="normal")
                    self.after(0, _ui)

                except _ue.HTTPError as e:
                    msg = f"HTTP {e.code} {e.reason}"
                    LOG.log("Kafka", f"Fetch failed: {msg}", "ERROR")
                    self.after(0, lambda m=msg: (
                        _set_status(f"❌  Kafka UI returned {m}", "error"),
                        fetch_btn.config(state="normal")))
                except _ue.URLError as e:
                    LOG.log("Kafka", f"Fetch failed: {e}", "ERROR")
                    self.after(0, lambda err=e: (
                        _set_status(f"❌  Cannot reach Kafka UI: {err.reason}", "error"),
                        fetch_btn.config(state="normal")))
                except Exception as e:
                    LOG.log("Kafka", f"Fetch error: {e}", "ERROR")
                    self.after(0, lambda err=e: (
                        _set_status(f"❌  {err}", "error"),
                        fetch_btn.config(state="normal")))

            threading.Thread(target=_run, daemon=True).start()

        # ── Row detail popup on double-click ────────────────────────
        def _show_detail(event=None):
            sel = tree.selection()
            if not sel:
                return
            try:
                real_idx = int(sel[0])
            except (ValueError, TypeError):
                return
            if not (0 <= real_idx < len(_all_messages)):
                return
            m = _all_messages[real_idx]

            dlg = tk.Toplevel(self)
            dlg.title(f"Kafka Message — {m['type']}")
            dlg.configure(bg=self.C("bg"))
            dlg.resizable(True, True)
            dlg.grab_set()
            x = self.winfo_rootx() + self.winfo_width()  // 2 - 310
            y = self.winfo_rooty() + self.winfo_height() // 2 - 260
            dlg.geometry(f"620x520+{x}+{y}")

            # Accent bar colored by type
            type_color = {"VOICE": "#22c55e", "SMS": "#06b6d4",
                          "LUDR": "#f59e0b", "IP": "#8b5cf6"}.get(m["type"], self.C("primary"))
            tk.Frame(dlg, bg=type_color, height=4).pack(fill="x")

            body = tk.Frame(dlg, bg=self.C("bg"))
            body.pack(fill="both", expand=True, padx=20, pady=14)

            # Title row with copy-all button
            hdr_row = tk.Frame(body, bg=self.C("bg"))
            hdr_row.pack(fill="x", pady=(0, 6))
            tk.Label(hdr_row, text=f"📨  Kafka Message",
                     bg=self.C("bg"), fg=self.C("card_title"),
                     font=(_UI_FONT, 13, "bold")).pack(side="left")
            type_badge = tk.Label(hdr_row, text=f" {m['type']} ",
                                  bg=type_color, fg="white",
                                  font=(_UI_FONT, 9, "bold"), padx=6, pady=2)
            type_badge.pack(side="left", padx=(10, 0))

            tk.Frame(body, bg=self.C("border"), height=1).pack(fill="x", pady=(0, 8))

            # All raw fields — scrollable list
            tk.Label(body, text="All Fields",
                     bg=self.C("bg"), fg=self.C("muted"),
                     font=(_UI_FONT, 8, "bold")).pack(anchor="w", pady=(0, 4))
            fields_outer = tk.Frame(body, bg=self.C("panel"),
                                    highlightbackground=self.C("border"),
                                    highlightthickness=1)
            fields_outer.pack(fill="x", pady=(0, 8))
            fields_canvas = tk.Canvas(fields_outer, bg=self.C("panel"),
                                      highlightthickness=0, height=150)
            fields_vsb = ttk.Scrollbar(fields_outer, orient="vertical",
                                       command=fields_canvas.yview)
            fields_vsb.pack(side="right", fill="y")
            fields_canvas.pack(fill="both", expand=True)
            fields_canvas.configure(yscrollcommand=fields_vsb.set)
            fgrid = tk.Frame(fields_canvas, bg=self.C("panel"))
            fgrid_win = fields_canvas.create_window((0, 0), window=fgrid, anchor="nw")

            def _on_fgrid_cfg(e):
                fields_canvas.configure(scrollregion=fields_canvas.bbox("all"))
            def _on_fcanvas_resize(e):
                fields_canvas.itemconfig(fgrid_win, width=e.width)
            fgrid.bind("<Configure>", _on_fgrid_cfg)
            fields_canvas.bind("<Configure>", _on_fcanvas_resize)

            # Parsed summary fields first, then remaining raw fields
            parsed_fields = [
                ("Type",          m["type"]),
                ("Target Number", m["target_number"] or "—"),
                ("Target Name",   m["target_name"]   or "—"),
                ("CIN",           m["cin"]            or "—"),
                ("Description",   m["description"]    or "—"),
                ("Timestamp",     m["timestamp"]      or "—"),
                ("Offset",        m["offset"]         or "—"),
            ]
            raw_extras = {k: v for k, v in (m.get("raw") or {}).items()
                          if k not in ("content", "value")}
            all_field_rows = parsed_fields + [(k, str(v)) for k, v in sorted(raw_extras.items())]

            for i, (lbl, val) in enumerate(all_field_rows):
                bg = self.C("input_bg") if i % 2 == 0 else self.C("panel")
                row_f = tk.Frame(fgrid, bg=bg)
                row_f.pack(fill="x")
                tk.Label(row_f, text=lbl,
                         bg=bg, fg=self.C("muted"),
                         font=(_UI_FONT, 8), width=18, anchor="w",
                         padx=8, pady=3).pack(side="left")
                tk.Label(row_f, text=str(val),
                         bg=bg, fg=self.C("card_title"),
                         font=(_UI_FONT, 8, "bold"), anchor="w",
                         padx=4, pady=3, wraplength=360).pack(side="left", fill="x", expand=True)

            # JSON section
            tk.Label(body, text="Raw JSON Payload",
                     bg=self.C("bg"), fg=self.C("muted"),
                     font=(_UI_FONT, 8, "bold")).pack(anchor="w", pady=(4, 2))
            json_frame = tk.Frame(body, bg=self.C("input_bg"),
                                  highlightbackground=self.C("border"),
                                  highlightthickness=1)
            json_frame.pack(fill="both", expand=True)
            try:
                raw_txt = _json.dumps(m["raw"], indent=2, ensure_ascii=False)
            except Exception:
                raw_txt = str(m["raw"])

            txt = tk.Text(json_frame, bg=self.C("input_bg"), fg="#a5f3fc",
                          font=("Consolas", 9), relief="flat",
                          wrap="none", padx=8, pady=6)
            jsb_v = ttk.Scrollbar(json_frame, orient="vertical", command=txt.yview)
            jsb_h = ttk.Scrollbar(json_frame, orient="horizontal", command=txt.xview)
            txt.configure(yscrollcommand=jsb_v.set, xscrollcommand=jsb_h.set)
            jsb_v.pack(side="right", fill="y")
            jsb_h.pack(side="bottom", fill="x")
            txt.pack(fill="both", expand=True)
            txt.insert("1.0", raw_txt)
            txt.config(state="disabled")

            # Button row
            btn_row = tk.Frame(body, bg=self.C("bg"))
            btn_row.pack(fill="x", pady=(8, 0))
            tk.Button(btn_row, text="📋  Copy JSON",
                      bg=self.C("input_bg"), fg=self.C("text"),
                      relief="flat", cursor="hand2", font=(_UI_FONT, 9),
                      command=lambda: (self.clipboard_clear(),
                                       self.clipboard_append(raw_txt))).pack(
                side="left", ipadx=10, ipady=4)
            tk.Button(btn_row, text="Close",
                      bg=self.C("input_bg"), fg=self.C("text"),
                      relief="flat", cursor="hand2", font=(_UI_FONT, 9),
                      command=dlg.destroy).pack(side="right", ipadx=12, ipady=4)
            dlg.bind("<Escape>", lambda e: dlg.destroy())

        tree.bind("<Double-1>", _show_detail)

        # ── Right-click context menu ────────────────────────────────
        def _kafka_ctx(event):
            row = tree.identify_row(event.y)
            if not row:
                return
            tree.selection_set(row)
            col_id  = tree.identify_column(event.x)
            col_idx = int(col_id.replace("#", "")) - 1 if col_id else -1
            vals    = tree.item(row, "values")
            cell    = str(vals[col_idx]) if 0 <= col_idx < len(vals) else ""
            menu = tk.Menu(self, tearoff=0, bg=self.C("panel"), fg=self.C("text"),
                           font=(_UI_FONT, 9), relief="flat",
                           activebackground=self.C("primary"), activeforeground="white")
            menu.add_command(label="📋  Copy cell value",
                             command=lambda: (self.clipboard_clear(),
                                              self.clipboard_append(cell)))
            menu.add_command(label="📄  Copy row",
                             command=lambda: (self.clipboard_clear(),
                                              self.clipboard_append(
                                                  "\t".join(str(v) for v in vals))))
            menu.add_separator()
            menu.add_command(label="🔍  View detail", command=_show_detail)
            menu.tk_popup(event.x_root, event.y_root)

        tree.bind("<Button-3>", _kafka_ctx)

        # ── Keyboard shortcuts ──────────────────────────────────────
        def _kafka_keys(event):
            if event.keysym == "F5":
                _fetch()
            elif event.keysym == "f" and (event.state & 0x4):  # Ctrl+F
                search_var.set("")
                for w in ctrl.winfo_children():
                    if isinstance(w, tk.Entry):
                        w.focus_set()
                        break
            elif event.keysym == "Escape":
                search_var.set("")
                date_var.set("")
                filter_var.set("ALL")
                try:
                    date_lbl.config(text="All dates", fg=self.C("muted"))
                except Exception:
                    pass

        tree.bind("<Key>", _kafka_keys)
        tree.focus_set()

        # ── Bottom bar: stats badges + count ───────────────────────
        bot = tk.Frame(tbl_outer, bg=self.C("panel"))
        bot.pack(fill="x", padx=10, pady=(4, 6))

        # Type stat badges
        _k_badge_labels = {}
        for btype, color in [("VOICE","#22c55e"),("SMS","#06b6d4"),
                              ("LUDR","#f59e0b"),("IP","#8b5cf6"),("OTHER","#6b7280")]:
            bf = tk.Frame(bot, bg=color, padx=1, pady=1)
            bf.pack(side="left", padx=(0, 5))
            bl = tk.Label(bf, text=f"{btype}: 0", bg=self.C("panel"),
                          fg=color, font=(_UI_FONT, 8, "bold"), padx=5, pady=2)
            bl.pack()
            _k_badge_labels[btype] = bl

        def _update_k_badges(messages):
            counts = {k: 0 for k in _k_badge_labels}
            for m in messages:
                t = m.get("type", "OTHER")
                if t in counts:
                    counts[t] += 1
                else:
                    counts["OTHER"] += 1
            for btype, lbl in _k_badge_labels.items():
                lbl.config(text=f"{btype}: {counts[btype]}")

        count_var = tk.StringVar(value="")
        tk.Label(bot, textvariable=count_var,
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 7)).pack(side="left", padx=(8, 0))
        tk.Label(bot,
                 text="Double-click for detail  |  Right-click for options  |  F5 refresh  |  Ctrl+F search  |  Esc clear",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 7)).pack(side="right")

        # Keep count badge in sync with all filters
        orig_apply = _apply_filter
        def _apply_filter_with_count(*args):
            # Guard: widget may be destroyed if user navigated away
            try:
                if not tree.winfo_exists():
                    return
            except Exception:
                return
            orig_apply(*args)
            try:
                n = len(tree.get_children())
                if count_var:
                    count_var.set(f"{n} message{'s' if n != 1 else ''} shown")
            except Exception:
                pass

        # Remove stale traces from previous dashboard instances before adding new ones
        for var, attr in ((search_var,              "_kafka_trace_search"),
                          (date_from_var,           "_kafka_trace_date_from"),
                          (date_to_var,             "_kafka_trace_date_to"),
                          (self._kafka_topic_var,   "_kafka_trace_topic")):
            old_tid = getattr(self, attr, None)
            if old_tid:
                try:
                    var.trace_remove("write", old_tid)
                except Exception:
                    pass

        self._kafka_trace_search = search_var.trace_add(
            "write", lambda *_: _apply_filter_with_count())

        def _on_date_from_change(*_):
            # Re-fetch only when From date changes (drives the seek position)
            self._kafka_messages.clear()
            self.after(150, _fetch)

        def _on_date_to_change(*_):
            # To-date change only affects client-side filtering, no re-fetch needed
            _apply_filter_with_count()

        self._kafka_trace_date_from = date_from_var.trace_add("write", _on_date_from_change)
        self._kafka_trace_date_to   = date_to_var.trace_add("write", _on_date_to_change)

        def _on_topic_change(*_):
            self._kafka_messages.clear()
            # Reset filters so stale date/type don't hide new topic's data
            date_from_var.set("")
            date_to_var.set("")
            filter_var.set("ALL")
            try:
                date_lbl.config(text="All dates", fg=self.C("muted"))
            except Exception:
                pass
            self.after(100, _fetch)

        self._kafka_trace_topic = self._kafka_topic_var.trace_add(
            "write", _on_topic_change)
        fetch_btn.config(command=lambda: _fetch())

        # Always auto-fetch on open; re-apply filters if cached data exists
        if self._kafka_messages:
            self.after(100, _apply_filter_with_count)
        self.after(400, _fetch)

    # ──────────────────────────────────────────────────────────────
    def show_sample_data(self):
        import zipfile as _zf, io as _io, stat as _st, subprocess as _sp
        from datetime import datetime as _dt

        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Sample Data page")

        # ── Page header ────────────────────────────────────────────
        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(25, 2))
        hdr_left = tk.Frame(hdr, bg=self.C("bg"))
        hdr_left.pack(side="left")
        tk.Label(hdr_left, text="📁  Sample Data Repository",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(anchor="w")
        tk.Label(hdr_left,
                 text="Browse and download original sample files directly from the server.",
                 bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(anchor="w", pady=(2, 0))
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", padx=10, pady=4,
                  command=self.show_first_page).pack(side="right", anchor="n")
        tk.Frame(main, bg=self.C("border"), height=1).pack(
            fill="x", padx=50, pady=(8, 16))

        # ── Data type metadata ──────────────────────────────────────
        type_meta = {
            "PCAP":     {"icon": "📡", "desc": "Network capture files (.pcap)"},
            "LUDR":     {"icon": "📍", "desc": "Location Based Services & LUDR records"},
            "SMS":      {"icon": "💬", "desc": "SMS IRI intercept files"},
            "Voice":    {"icon": "📞", "desc": "Hi2 & Hi3 voice call files"},
            "CellID":   {"icon": "🗼", "desc": "Cell Tower ID reference data"},
            "SDR":      {"icon": "📋", "desc": "SMS Detail Records"},
            "CDR-IPDR": {"icon": "🌐", "desc": "CDR / IP Detail Records"},
        }
        SERVER_MAP = {
            "PCAP": "pcap", "LUDR": "ludr", "SMS": "ludr",
            "Voice": "voice", "CellID": "ludr",
            "SDR": "ludr", "CDR-IPDR": "ludr",
        }

        # Shared state
        dtype         = tk.StringVar(value="PCAP")
        dropdown_open = [False]
        _all_rows     = []   # cache for search filtering

        # ── Control card ────────────────────────────────────────────
        ctrl_card = tk.Frame(main,
                             highlightbackground=self.C("border"),
                             highlightthickness=1,
                             bg=self.C("panel"))
        ctrl_card.pack(fill="x", padx=50, pady=(0, 12))
        tk.Frame(ctrl_card, bg=self.C("primary"), height=4).pack(fill="x")

        ctrl_body = tk.Frame(ctrl_card, bg=self.C("panel"))
        ctrl_body.pack(fill="x", padx=20, pady=16)

        # Left — data type dropdown ─────────────────────────────────
        left_ctrl = tk.Frame(ctrl_body, bg=self.C("panel"))
        left_ctrl.pack(side="left", fill="x", expand=True)

        tk.Label(left_ctrl, text="DATA TYPE",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 8, "bold")).pack(anchor="w", pady=(0, 6))

        sel_frame = tk.Frame(left_ctrl,
                             bg=self.C("input_bg"),
                             highlightbackground=self.C("primary"),
                             highlightthickness=1,
                             cursor="hand2")
        sel_frame.pack(anchor="w")

        sel_icon = tk.Label(sel_frame, text="📡",
                            bg=self.C("input_bg"), fg=self.C("card_title"),
                            font=(_UI_FONT, 16), padx=10, pady=8)
        sel_icon.pack(side="left")

        sel_col = tk.Frame(sel_frame, bg=self.C("input_bg"))
        sel_col.pack(side="left", padx=(0, 6), pady=6)
        sel_title = tk.Label(sel_col, text="PCAP",
                             bg=self.C("input_bg"), fg=self.C("card_title"),
                             font=(_UI_FONT, 11, "bold"))
        sel_title.pack(anchor="w")
        sel_desc = tk.Label(sel_col, text=type_meta["PCAP"]["desc"],
                            bg=self.C("input_bg"), fg=self.C("muted"),
                            font=(_UI_FONT, 8))
        sel_desc.pack(anchor="w")

        sel_arrow = tk.Label(sel_frame, text="▾",
                             bg=self.C("input_bg"), fg=self.C("primary"),
                             font=(_UI_FONT, 13, "bold"), padx=10)
        sel_arrow.pack(side="right")

        popup_frame = tk.Frame(left_ctrl,
                               bg=self.C("panel"),
                               highlightbackground=self.C("primary"),
                               highlightthickness=1)

        # ── Status bar (color-coded dot + text + last-refreshed) ────
        status_bar = tk.Frame(left_ctrl, bg=self.C("panel"))
        status_bar.pack(anchor="w", fill="x", pady=(8, 0))
        status_dot = tk.Label(status_bar, text="●",
                              bg=self.C("panel"), fg=self.C("muted"),
                              font=(_UI_FONT, 9))
        status_dot.pack(side="left")
        status_var = tk.StringVar(
            value="Select a data type and click Refresh to browse files.")
        status_lbl = tk.Label(status_bar, textvariable=status_var,
                              bg=self.C("panel"), fg=self.C("muted"),
                              font=(_UI_FONT, 8))
        status_lbl.pack(side="left", padx=(4, 0))

        # Last-refreshed timestamp label (right-side of status bar)
        refresh_ts_var = tk.StringVar(value="")
        tk.Label(status_bar, textvariable=refresh_ts_var,
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 7, "italic")).pack(side="right")

        def _set_status(msg, state="idle"):
            color = {"idle":    self.C("muted"),
                     "working": "#f59e0b",
                     "ok":      self.C("success"),
                     "error":   "#ef4444"}.get(state, self.C("muted"))
            status_var.set(msg)
            status_dot.config(fg=color)
            status_lbl.config(fg=color)

        # ── Dropdown helpers ────────────────────────────────────────
        def select_type(key):
            dtype.set(key)
            meta = type_meta[key]
            sel_icon.config(text=meta["icon"])
            sel_title.config(text=key)
            sel_desc.config(text=meta["desc"])
            close_dropdown()
            # [#1] Auto-refresh list when type changes
            list_remote()

        def build_popup():
            for w in popup_frame.winfo_children():
                w.destroy()
            for i, (key, meta) in enumerate(type_meta.items()):
                is_sel  = key == dtype.get()
                item_bg = self.C("primary") if is_sel else self.C("panel")
                item_fg = "#ffffff"         if is_sel else self.C("card_title")

                item = tk.Frame(popup_frame, bg=item_bg, cursor="hand2")
                item.pack(fill="x")
                if i > 0:
                    tk.Frame(popup_frame, bg=self.C("border"),
                             height=1).pack(fill="x")

                row = tk.Frame(item, bg=item_bg, cursor="hand2")
                row.pack(fill="x", padx=12, pady=8)
                tk.Label(row, text=meta["icon"],
                         bg=item_bg, font=(_UI_FONT, 15),
                         width=2).pack(side="left", padx=(0, 10))
                col = tk.Frame(row, bg=item_bg)
                col.pack(side="left")
                tk.Label(col, text=key,
                         bg=item_bg, fg=item_fg,
                         font=(_UI_FONT, 10, "bold")).pack(anchor="w")
                tk.Label(col, text=meta["desc"],
                         bg=item_bg,
                         fg="#a5f3fc" if is_sel else self.C("muted"),
                         font=(_UI_FONT, 8)).pack(anchor="w")
                if is_sel:
                    tk.Label(row, text="✓",
                             bg=item_bg, fg="#ffffff",
                             font=(_UI_FONT, 11, "bold")).pack(side="right")

                def _click(e=None, k=key): select_type(k)
                for w in [item, row] + list(row.winfo_children()) + \
                         list(col.winfo_children()):
                    w.bind("<Button-1>", _click)

                def _h_on(e=None, f=item, r=row, c=col, k=key):
                    if k != dtype.get():
                        clr = self.C("input_bg")
                        f.config(bg=clr); r.config(bg=clr); c.config(bg=clr)
                        for w in r.winfo_children() + list(c.winfo_children()):
                            try: w.config(bg=clr)
                            except: pass
                def _h_off(e=None, f=item, r=row, c=col, k=key):
                    if k != dtype.get():
                        clr = self.C("panel")
                        f.config(bg=clr); r.config(bg=clr); c.config(bg=clr)
                        for w in r.winfo_children() + list(c.winfo_children()):
                            try: w.config(bg=clr)
                            except: pass
                for w in [item, row] + list(row.winfo_children()):
                    w.bind("<Enter>", _h_on)
                    w.bind("<Leave>", _h_off)

        def open_dropdown(e=None):
            if dropdown_open[0]:
                close_dropdown(); return
            build_popup()
            popup_frame.pack(anchor="w", fill="x")
            sel_arrow.config(text="▴")
            dropdown_open[0] = True

        def close_dropdown(e=None):
            popup_frame.pack_forget()
            sel_arrow.config(text="▾")
            dropdown_open[0] = False

        for w in [sel_frame, sel_icon, sel_col, sel_title,
                  sel_desc, sel_arrow]:
            w.bind("<Button-1>", open_dropdown)

        # [#6] Escape closes dropdown
        main.bind_all("<Escape>", close_dropdown)

        # Right — action buttons ─────────────────────────────────────
        right_ctrl = tk.Frame(ctrl_body, bg=self.C("panel"))
        right_ctrl.pack(side="right", padx=(24, 0), anchor="n")
        tk.Label(right_ctrl, text="ACTIONS",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 8, "bold")).pack(anchor="w", pady=(0, 6))

        # list_remote defined here so select_type can call it
        def list_remote():
            close_dropdown()
            SERVER_KEY = SERVER_MAP.get(dtype.get(), "ludr")
            ssh = CONN.get_ssh(SERVER_KEY)
            if not ssh:
                _set_status(
                    f"Not connected to {SERVER_KEY.upper()} server — check Settings.",
                    "error")
                _clear_tree()
                return
            remote = SAMPLE_LOCATIONS.get(dtype.get(), "")
            if not remote:
                _set_status(f"No path configured for {dtype.get()}.", "error")
                return
            _set_status("Listing remote files…", "working")
            _clear_tree()
            refresh_ts_var.set("")
            LOG.log("Sample Data", f"Listing {remote} via {SERVER_KEY}")

            def _run():
                try:
                    _, out, _ = ssh.exec_command(
                        f"ls -lh {shlex.quote(remote)}")
                    lines = out.read().decode("utf-8", "ignore").splitlines()
                    rows = []
                    for line in lines:
                        if not line or line.startswith("total"): continue
                        parts = line.split()
                        if len(parts) < 9: continue
                        name  = " ".join(parts[8:])
                        ftype = "📁 Dir" if parts[0][0] == "d" else "📄 File"
                        size  = parts[4] if len(parts) > 4 else "—"
                        rows.append((name, ftype, size))
                    LOG.log("Sample Data",
                            f"Listed {len(rows)} item(s) in {remote}")

                    def _update():
                        _all_rows.clear()
                        _all_rows.extend(rows)
                        _apply_filter()          # respects current search text
                        n = len(rows)
                        _set_status(
                            f"{n} item{'s' if n != 1 else ''} found in "
                            f"{dtype.get()} sample folder.", "ok")
                        _update_count(n, visible=True)
                        # [#10] Last-refreshed timestamp
                        refresh_ts_var.set(
                            f"Last refreshed: {_dt.now().strftime('%I:%M %p')}")
                    self.after(0, _update)
                except Exception as e:
                    LOG.log("Sample Data", f"Error: {e}", "ERROR")
                    self.after(0, lambda: _set_status(str(e), "error"))
            threading.Thread(target=_run, daemon=True).start()

        def download_all():
            """Download ALL sample data types from server as a single ZIP."""
            close_dropdown()
            sftp_any = CONN.get_sftp("pcap") or CONN.get_sftp("ludr")
            if not sftp_any:
                return self.popup("Error",
                    "Not connected to any server.\n"
                    "Connect a server in Settings first.", "error")
            zip_path = filedialog.asksaveasfilename(
                title="Save Sample Data as ZIP",
                defaultextension=".zip",
                filetypes=[("ZIP archive", "*.zip")],
                initialfile="sample_data_all.zip")
            if not zip_path: return

            top, _ = self.progress_window("Downloading sample data…")

            def _run():
                total_files = 0
                total_bytes = 0
                skipped     = []
                try:
                    with _zf.ZipFile(zip_path, "w",
                                     compression=_zf.ZIP_DEFLATED) as zout:
                        for dtype_key, remote_dir in SAMPLE_LOCATIONS.items():
                            skey   = "pcap" if dtype_key == "PCAP" else "ludr"
                            sftp_t = CONN.get_sftp(skey)
                            if not sftp_t:
                                skipped.append(dtype_key)
                                LOG.log("Sample Data",
                                        f"  Skip {dtype_key}: not connected",
                                        "WARNING")
                                continue
                            self.after(0, lambda d=dtype_key, rd=remote_dir:
                                top._set_status(f"Downloading {d}…", sub=rd)
                                if top.winfo_exists() else None)
                            LOG.log("Sample Data",
                                    f"Scanning {remote_dir} ({dtype_key})")

                            def _sftp_walk(sftp_obj, rpath):
                                try:
                                    entries = sftp_obj.listdir_attr(rpath)
                                except Exception:
                                    return
                                for entry in sorted(entries,
                                        key=lambda e: e.filename):
                                    rp = rpath.rstrip("/") + "/" + entry.filename
                                    if _st.S_ISDIR(entry.st_mode or 0):
                                        yield from _sftp_walk(sftp_obj, rp)
                                    else:
                                        rel = rp[len(remote_dir):]
                                        yield rp, dtype_key + rel

                            try:
                                for rfile, arcname in _sftp_walk(
                                        sftp_t, remote_dir):
                                    try:
                                        buf = _io.BytesIO()
                                        sftp_t.getfo(rfile, buf)
                                        data = buf.getvalue()
                                        zout.writestr(arcname, data)
                                        total_files += 1
                                        total_bytes += len(data)
                                        self.after(0,
                                            lambda n=total_files, fn=arcname:
                                            top._set_status(
                                                f"Packing {n} file(s)…",
                                                sub=fn)
                                            if top.winfo_exists() else None)
                                        LOG.log("Sample Data",
                                                f"  + {arcname} ({len(data):,}B)")
                                    except Exception as fe:
                                        LOG.log("Sample Data",
                                                f"  Skip {rfile}: {fe}", "WARNING")
                            except Exception as de:
                                LOG.log("Sample Data",
                                        f"  Error {dtype_key}: {de}", "ERROR")
                                skipped.append(dtype_key)

                    size_kb   = os.path.getsize(zip_path) // 1024
                    skip_note = (f"\n\nSkipped (not connected): "
                                 f"{', '.join(skipped)}" if skipped else "")
                    LOG.log("Sample Data",
                            f"ZIP done: {total_files} files, {size_kb} KB → {zip_path}")
                    self.after(0, lambda: (
                        top.destroy() if top.winfo_exists() else None,
                        self.popup("Downloaded",
                            f"Sample data ZIP saved:\n{zip_path}\n\n"
                            f"  {total_files} file(s)  ·  {size_kb} KB\n"
                            f"  Server folder structure preserved"
                            f"{skip_note}", "success")))
                except Exception as e:
                    LOG.log("Sample Data", f"ZIP failed: {e}", "ERROR")
                    self.after(0, lambda: (
                        top.destroy() if top.winfo_exists() else None,
                        self.popup("Error", f"Download failed:\n{e}", "error")))

            threading.Thread(target=_run, daemon=True).start()

        for txt, cmd, bg, tip_txt in [
            ("🔄  Refresh File List",
             list_remote, self.C("success"),
             "Load files from the server"),
            ("📦  Download All Types",
             download_all, "#7c3aed",
             "Download all data types as one ZIP"),
        ]:
            tk.Button(right_ctrl, text=txt, command=cmd,
                      bg=bg, fg="white", relief="flat",
                      font=(_UI_FONT, 10, "bold"),
                      activebackground=self.C("border"),
                      anchor="w", padx=14, cursor="hand2").pack(
                          fill="x", pady=3, ipady=7)
            tk.Label(right_ctrl, text=tip_txt,
                     bg=self.C("panel"), fg=self.C("dim"),
                     font=(_UI_FONT, 7)).pack(anchor="e", pady=(0, 3))

        # ── File browser panel ──────────────────────────────────────
        list_outer = tk.Frame(main,
                              highlightbackground=self.C("border"),
                              highlightthickness=1,
                              bg=self.C("panel"))
        list_outer.pack(fill="both", expand=True, padx=50, pady=(0, 10))

        # Section header bar (title + count badge)
        list_hdr = tk.Frame(list_outer, bg=self.C("panel"))
        list_hdr.pack(fill="x", padx=14, pady=(10, 6))
        tk.Label(list_hdr, text="SERVER FILE BROWSER",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 8, "bold")).pack(side="left")

        count_var = tk.StringVar(value="")
        count_lbl = tk.Label(list_hdr, textvariable=count_var,
                             bg=self.C("primary"), fg="white",
                             font=(_UI_FONT, 7, "bold"), padx=7, pady=2)

        def _update_count(n, visible=True):
            if visible and n > 0:
                count_var.set(f"  {n} item{'s' if n != 1 else ''}  ")
                count_lbl.pack(side="left", padx=(8, 0))
            else:
                count_lbl.pack_forget()

        # [#3] Search / filter bar
        tk.Frame(list_outer, bg=self.C("border"), height=1).pack(fill="x")
        search_row = tk.Frame(list_outer, bg=self.C("panel"))
        search_row.pack(fill="x", padx=10, pady=(6, 2))
        tk.Label(search_row, text="🔍",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 10)).pack(side="left", padx=(4, 4))
        search_var = tk.StringVar()
        search_entry = tk.Entry(search_row, textvariable=search_var,
                                bg=self.C("input_bg"), fg=self.C("text"),
                                insertbackground=self.C("text"),
                                relief="flat", font=(_UI_FONT, 9),
                                highlightbackground=self.C("border"),
                                highlightthickness=1)
        search_entry.pack(side="left", fill="x", expand=True, ipady=5)
        clear_btn = tk.Button(search_row, text="✕",
                              bg=self.C("input_bg"), fg=self.C("muted"),
                              relief="flat", font=(_UI_FONT, 9),
                              cursor="hand2",
                              command=lambda: search_var.set(""))
        clear_btn.pack(side="left", padx=(2, 4))
        tk.Label(search_row,
                 text="Filter by name",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 7, "italic")).pack(side="left", padx=(6, 0))

        tk.Frame(list_outer, bg=self.C("border"), height=1).pack(fill="x")

        # Treeview
        tree_frame = tk.Frame(list_outer, bg=self.C("panel"))
        tree_frame.pack(fill="both", expand=True, padx=10, pady=4)

        style = ttk.Style()
        style.theme_use("default")
        style.configure("Dark.Treeview",
                        background=self.C("input_bg"),
                        foreground=self.C("text"),
                        rowheight=28,
                        fieldbackground=self.C("input_bg"),
                        borderwidth=0)
        style.configure("Dark.Treeview.Heading",
                        background=self.C("primary"),
                        foreground="white",
                        font=(_UI_FONT, 10, "bold"),
                        relief="flat")
        style.map("Dark.Treeview",
                  background=[("selected", self.C("primary"))],
                  foreground=[("selected", "#ffffff")])

        cols = ("Name", "Type", "Size")
        tree = ttk.Treeview(tree_frame, columns=cols, show="headings",
                            height=14, style="Dark.Treeview")

        # [#4] Sortable column headers — click to sort asc/desc
        _sort_state = {"col": None, "asc": True}

        def _sort_by(col):
            asc = not _sort_state["asc"] if _sort_state["col"] == col else True
            _sort_state["col"] = col
            _sort_state["asc"] = asc
            arrow = "  ▲" if asc else "  ▼"
            for c in cols:
                lbl = {"Name": "📄  Name", "Type": "🏷  Type",
                       "Size": "📦  Size"}[c]
                tree.heading(c, text=lbl + (arrow if c == col else ""))
            items = [(tree.set(iid, col), iid)
                     for iid in tree.get_children("")]
            # Size: sort numerically by stripping units
            def _key(v):
                raw = v[0].replace("📁", "").replace("📄", "").strip()
                if col == "Size":
                    units = {"K": 1, "M": 1024, "G": 1024**2,
                             "T": 1024**3, "—": 0}
                    try:
                        num  = float(raw[:-1])
                        mult = units.get(raw[-1], 1)
                        return num * mult
                    except Exception:
                        return 0
                return raw.lower()
            items.sort(key=_key, reverse=not asc)
            for idx, (_, iid) in enumerate(items):
                tree.move(iid, "", idx)
                # Re-apply zebra after sort
                tag = "odd" if idx % 2 else "even"
                tree.item(iid, tags=(tag,))

        col_labels = {"Name": "📄  Name", "Type": "🏷  Type", "Size": "📦  Size"}
        col_widths  = {"Name": 450, "Type": 90, "Size": 110}
        col_anchors = {"Name": "w", "Type": "center", "Size": "e"}
        for c in cols:
            tree.heading(c, text=col_labels[c],
                         command=lambda _c=c: _sort_by(_c))
            tree.column(c, width=col_widths[c], anchor=col_anchors[c])

        # [#8] Zebra-stripe tags
        try:
            _even_bg = self.C("input_bg")
            _odd_bg  = self.C("panel")
        except Exception:
            _even_bg = _odd_bg = "#1e1e2e"
        tree.tag_configure("even", background=_even_bg)
        tree.tag_configure("odd",  background=_odd_bg)

        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        _bind_mousewheel(tree, vsb)

        # [#9] Empty-state placeholder label (shown when tree is empty)
        empty_lbl = tk.Label(tree_frame,
                             text="No files loaded\n\nSelect a data type and click  🔄 Refresh File List\nto browse the server's sample folder.",
                             bg=self.C("input_bg"), fg=self.C("dim"),
                             font=(_UI_FONT, 10),
                             justify="center")

        def _clear_tree():
            tree.delete(*tree.get_children())
            _all_rows.clear()
            search_var.set("")
            _update_count(0, visible=False)
            _show_empty_state(True)

        def _show_empty_state(show):
            if show:
                empty_lbl.place(relx=0.5, rely=0.45, anchor="center")
            else:
                empty_lbl.place_forget()

        _show_empty_state(True)   # visible on first load

        # [#3] Filter function — redraws tree from _all_rows cache
        def _apply_filter(*_):
            q = search_var.get().strip().lower()
            tree.delete(*tree.get_children())
            visible = [r for r in _all_rows
                       if not q or q in r[0].lower()]
            for idx, r in enumerate(visible):
                tag = "odd" if idx % 2 else "even"
                tree.insert("", "end", values=r, tags=(tag,))
            _show_empty_state(len(visible) == 0)
            # Update count badge to show filtered count
            if q:
                count_var.set(
                    f"  {len(visible)} of {len(_all_rows)}  ")
                if len(_all_rows) > 0:
                    count_lbl.pack(side="left", padx=(8, 0))
            else:
                _update_count(len(_all_rows), visible=True)

        search_var.trace_add("write", _apply_filter)

        # [#2] Single-click selection preview bar
        preview_bar = tk.Frame(list_outer, bg=self.C("panel"))
        preview_bar.pack(fill="x", padx=10, pady=(0, 2))
        preview_icon  = tk.Label(preview_bar, text="",
                                 bg=self.C("panel"), fg=self.C("primary"),
                                 font=(_UI_FONT, 10))
        preview_icon.pack(side="left", padx=(4, 6))
        preview_name  = tk.Label(preview_bar, text="",
                                 bg=self.C("panel"), fg=self.C("card_title"),
                                 font=(_UI_FONT, 9, "bold"))
        preview_name.pack(side="left")
        preview_path  = tk.Label(preview_bar, text="",
                                 bg=self.C("panel"), fg=self.C("muted"),
                                 font=(_UI_FONT, 7))
        preview_path.pack(side="left", padx=(8, 0))
        preview_size  = tk.Label(preview_bar, text="",
                                 bg=self.C("panel"), fg=self.C("dim"),
                                 font=(_UI_FONT, 8))
        preview_size.pack(side="right", padx=(0, 8))

        def _on_select(event=None):
            sel = tree.selection()
            if not sel:
                preview_icon.config(text="")
                preview_name.config(text="")
                preview_path.config(text="")
                preview_size.config(text="")
                return
            vals  = tree.item(sel[0])["values"]
            name  = str(vals[0]).strip() if vals else ""
            ftype = str(vals[1]).strip() if len(vals) > 1 else ""
            size  = str(vals[2]).strip() if len(vals) > 2 else ""
            remote_dir  = SAMPLE_LOCATIONS.get(dtype.get(), "")
            remote_path = f"{remote_dir.rstrip('/')}/{name}"
            icon = "📁" if "Dir" in ftype else "📄"
            kind = "Folder — will be downloaded as ZIP" if "Dir" in ftype \
                   else "File"
            preview_icon.config(text=icon)
            preview_name.config(text=name)
            preview_path.config(text=f"  {remote_path}")
            preview_size.config(text=f"{size}  ·  {kind}")

        tree.bind("<<TreeviewSelect>>", _on_select)

        # ── Download logic ──────────────────────────────────────────
        def _download_selected_file():
            sel = tree.selection()
            if not sel:
                return self.popup("Info",
                    "Select a file or folder in the list above first.", "info")
            item  = tree.item(sel[0])
            name  = str(item["values"][0]).strip()
            ftype = str(item["values"][1]).strip() if len(item["values"]) > 1 else ""
            is_dir_hint = "Dir" in ftype

            key    = SERVER_MAP.get(dtype.get(), "ludr")
            sftp_d = CONN.get_sftp(key)
            if not sftp_d:
                return self.popup("Error",
                    f"Not connected to {key.upper()} server. Check Settings.", "error")

            remote_dir  = SAMPLE_LOCATIONS.get(dtype.get(), "")
            remote_path = f"{remote_dir.rstrip('/')}/{name}"

            try:
                st     = sftp_d.stat(remote_path)
                is_dir = _st.S_ISDIR(st.st_mode or 0)
            except Exception as e:
                LOG.log("Sample Data", f"stat failed for {remote_path}: {e}", "ERROR")
                is_dir = is_dir_hint

            if is_dir:
                local = filedialog.asksaveasfilename(
                    title=f"Save folder '{name}' as ZIP",
                    initialfile=f"{name}.zip",
                    defaultextension=".zip",
                    filetypes=[("ZIP archive", "*.zip")])
                if not local: return

                top, _ = self.progress_window(f"Zipping {name}…")

                def _run_dir():
                    total_files = 0
                    try:
                        def _sftp_walk(rpath):
                            entries = sftp_d.listdir_attr(rpath)
                            for entry in sorted(entries, key=lambda e: e.filename):
                                rp = rpath.rstrip("/") + "/" + entry.filename
                                if _st.S_ISDIR(entry.st_mode or 0):
                                    yield from _sftp_walk(rp)
                                else:
                                    rel = rp[len(remote_path):]
                                    yield rp, name + rel

                        with _zf.ZipFile(local, "w",
                                         compression=_zf.ZIP_DEFLATED) as zout:
                            for rfile, arcname in _sftp_walk(remote_path):
                                buf = _io.BytesIO()
                                sftp_d.getfo(rfile, buf)
                                zout.writestr(arcname, buf.getvalue())
                                total_files += 1
                                self.after(0,
                                    lambda n=total_files, fn=arcname:
                                    top._set_status(f"Packing {n} file(s)…",
                                                    sub=fn)
                                    if top.winfo_exists() else None)
                                LOG.log("Sample Data", f"  + {arcname}")

                        size_kb = os.path.getsize(local) // 1024
                        LOG.log("Sample Data",
                                f"Folder ZIP done: {total_files} files, "
                                f"{size_kb} KB → {local}")
                        local_dir = os.path.dirname(os.path.abspath(local))
                        # [#5] offer Open Folder after success
                        def _done_dir():
                            if top.winfo_exists(): top.destroy()
                            self._download_success_popup(
                                f"Folder '{name}' saved as ZIP",
                                f"{local}\n\n  {total_files} file(s)  ·  {size_kb} KB",
                                local_dir)
                        self.after(0, _done_dir)
                    except Exception as e:
                        LOG.log("Sample Data", f"Folder ZIP failed: {e}", "ERROR")
                        self.after(0, lambda err=e: (
                            top.destroy() if top.winfo_exists() else None,
                            self.popup("Error",
                                f"Download failed:\n{err}", "error")))

                threading.Thread(target=_run_dir, daemon=True).start()

            else:
                local = filedialog.asksaveasfilename(
                    title=f"Save {name}",
                    initialfile=name,
                    filetypes=[("All files", "*.*")])
                if not local: return

                top, _ = self.progress_window(f"Downloading {name}…")

                def _run_file():
                    try:
                        sftp_d.get(remote_path, local)
                        size_kb   = os.path.getsize(local) // 1024
                        local_dir = os.path.dirname(os.path.abspath(local))
                        LOG.log("Sample Data",
                                f"Downloaded {name} → {local} ({size_kb} KB)")
                        # [#5] offer Open Folder after success
                        def _done_file():
                            if top.winfo_exists(): top.destroy()
                            self._download_success_popup(
                                f"{name} downloaded",
                                f"{local}\n\n  {size_kb} KB",
                                local_dir)
                        self.after(0, _done_file)
                    except Exception as e:
                        LOG.log("Sample Data", f"Download failed: {e}", "ERROR")
                        self.after(0, lambda err=e: (
                            top.destroy() if top.winfo_exists() else None,
                            self.popup("Error",
                                f"Download failed:\n{err}", "error")))

                threading.Thread(target=_run_file, daemon=True).start()

        # [#6] Keyboard navigation bindings
        tree.bind("<Double-1>", lambda e: _download_selected_file())
        tree.bind("<Return>",   lambda e: _download_selected_file())
        tree.bind("<Escape>",   lambda e: tree.selection_remove(
                                    *tree.selection()))

        # [#7] Right-click context menu
        ctx_menu = tk.Menu(self, tearoff=0,
                           bg=self.C("panel"), fg=self.C("text"),
                           activebackground=self.C("primary"),
                           activeforeground="white",
                           font=(_UI_FONT, 9),
                           relief="flat", bd=1)
        ctx_menu.add_command(label="⬇️  Download",
                             command=_download_selected_file)
        ctx_menu.add_separator()
        ctx_menu.add_command(label="📋  Copy Server Path",
                             command=lambda: _ctx_copy_path())
        ctx_menu.add_command(label="ℹ️  Properties",
                             command=lambda: _ctx_properties())

        def _ctx_copy_path():
            sel = tree.selection()
            if not sel: return
            name = str(tree.item(sel[0])["values"][0]).strip()
            remote_dir  = SAMPLE_LOCATIONS.get(dtype.get(), "")
            remote_path = f"{remote_dir.rstrip('/')}/{name}"
            self.clipboard_clear()
            self.clipboard_append(remote_path)
            _set_status(f"Copied path: {remote_path}", "ok")

        def _ctx_properties():
            sel = tree.selection()
            if not sel: return
            vals  = tree.item(sel[0])["values"]
            name  = str(vals[0]).strip()
            ftype = str(vals[1]).strip() if len(vals) > 1 else "—"
            size  = str(vals[2]).strip() if len(vals) > 2 else "—"
            remote_dir  = SAMPLE_LOCATIONS.get(dtype.get(), "")
            remote_path = f"{remote_dir.rstrip('/')}/{name}"
            self.popup("Properties",
                f"Name:    {name}\n"
                f"Type:    {ftype}\n"
                f"Size:    {size}\n"
                f"Server path:\n  {remote_path}", "info")

        def _show_ctx_menu(event):
            iid = tree.identify_row(event.y)
            if iid:
                tree.selection_set(iid)
                try:
                    ctx_menu.tk_popup(event.x_root, event.y_root)
                finally:
                    ctx_menu.grab_release()

        tree.bind("<Button-3>", _show_ctx_menu)

        # ── Action bar ──────────────────────────────────────────────
        tk.Frame(list_outer, bg=self.C("border"), height=1).pack(fill="x")

        action_row = tk.Frame(list_outer, bg=self.C("panel"))
        action_row.pack(fill="x", padx=14, pady=8)

        tk.Button(action_row,
                  text="⬇️  Download Selected",
                  bg=self.C("primary"), fg="white",
                  relief="flat",
                  font=(_UI_FONT, 10, "bold"),
                  activebackground=self.C("border"),
                  cursor="hand2",
                  command=_download_selected_file).pack(
            side="left", ipady=7, ipadx=16)

        hint_frame = tk.Frame(action_row, bg=self.C("panel"))
        hint_frame.pack(side="left", padx=14)
        tk.Label(hint_frame,
                 text="Files download directly  ·  Folders are zipped automatically",
                 bg=self.C("panel"), fg=self.C("text"),
                 font=(_UI_FONT, 8)).pack(anchor="w")
        tk.Label(hint_frame,
                 text="Double-click  ·  Enter to download  ·  Right-click for options  ·  Escape to deselect",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 7)).pack(anchor="w", pady=(1, 0))

        # ── Auto-refresh ────────────────────────────────────────────
        _auto_job = [None]
        _AUTO_MS  = 30_000   # 30 seconds

        def _auto_refresh():
            try:
                if not tree.winfo_exists():
                    return
            except Exception:
                return
            list_remote()
            _auto_job[0] = self.after(_AUTO_MS, _auto_refresh)

        # Auto-load on page open, then refresh every 30 s
        def _initial_load():
            try:
                if not tree.winfo_exists():
                    return
            except Exception:
                return
            list_remote()
            _auto_job[0] = self.after(_AUTO_MS, _auto_refresh)

        self.after(500, _initial_load)

    # ──────────────────────────────────────────────────────────────
    # TARGET DETAILS
    # ──────────────────────────────────────────────────────────────
    def show_target_details(self):
        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Target Details page")

        # ── Header ─────────────────────────────────────────────────
        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(25, 5))
        tk.Label(hdr, text="🎯  Target Details", bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")
        # Count badge — updated whenever tree is populated
        count_badge = tk.Label(hdr, text="", bg=self.C("primary"), fg="white",
                               font=(_UI_FONT, 9, "bold"), padx=10, pady=3)
        count_badge.pack(side="left", padx=14)
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", padx=10, pady=4,
                  command=self.show_tag_validation).pack(side="right")
        tk.Frame(main, bg=self.C("border"), height=1).pack(fill="x", padx=50, pady=(5, 15))

        # ── Two query paths ────────────────────────────────────────
        QUERY_PATHS = {
            "Voice  (CompleteQueryFile)":
                "/u01/TMSC/Filter/QueryUploadDirectory/"
                "VoiceFilterFile/CompleteQueryFile",
            "IP  (CompleteFilterFiles)":
                "/usr/local/cleartrail/FilterProvisionUtility/"
                "CompleteFilterFiles",
        }
        active_path = [list(QUERY_PATHS.values())[0]]
        path_hint_var = tk.StringVar(value=active_path[0])

        toggle_bar = tk.Frame(main, bg=self.C("panel"),
                              highlightbackground=self.C("border"),
                              highlightthickness=1)
        toggle_bar.pack(fill="x", padx=50, pady=(0, 8))
        tk.Label(toggle_bar, text="  Query Path:",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(
            side="left", padx=(8, 6), pady=8)
        _path_btns = {}

        def _switch_path(lbl, path):
            active_path[0] = path
            path_hint_var.set(f"  {path}")
            for lb2, b2 in _path_btns.items():
                b2.config(
                    bg=self.C("primary") if lb2 == lbl else self.C("input_bg"),
                    fg="white"           if lb2 == lbl else self.C("muted"))
            self.after(100, list_remote)

        for lbl, pth in QUERY_PATHS.items():
            short = lbl.split("(")[0].strip()
            b = tk.Button(toggle_bar, text=f"📂  {short}",
                          bg=self.C("primary") if pth == active_path[0] else self.C("input_bg"),
                          fg="white"           if pth == active_path[0] else self.C("muted"),
                          relief="flat", font=(_UI_FONT, 9, "bold"),
                          padx=14, pady=6, cursor="hand2",
                          command=lambda lb=lbl, p=pth: _switch_path(lb, p))
            b.pack(side="left", padx=4, pady=6)
            _path_btns[lbl] = b
        tk.Label(toggle_bar, textvariable=path_hint_var,
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 7)).pack(side="left", padx=10, fill="x", expand=True)

        body = tk.Frame(main, bg=self.C("bg"))
        body.pack(fill="both", expand=True, padx=50, pady=5)

        # ── Left: file list ────────────────────────────────────────
        left_card = tk.Frame(body, bg=self.C("panel"),
                             highlightbackground=self.C("border"), highlightthickness=1,
                             width=240)
        left_card.pack(side="left", fill="y", padx=(0, 12))
        left_card.pack_propagate(False)
        tk.Label(left_card, text="Query Files", bg=self.C("panel"),
                 fg=self.C("muted"), font=(_UI_FONT, 10, "bold")).pack(
                     anchor="w", padx=12, pady=(10, 4))
        tk.Frame(left_card, bg=self.C("border"), height=1).pack(fill="x", padx=8)
        lb_scroll = tk.Frame(left_card, bg=self.C("panel"))
        lb_scroll.pack(fill="both", expand=True, padx=8, pady=8)
        lb  = tk.Listbox(lb_scroll, bg=self.C("input_bg"), fg=self.C("text"),
                         font=("Consolas", 9), selectbackground=self.C("primary"),
                         selectforeground=self.C("text"), relief="flat",
                         highlightthickness=0, borderwidth=0)
        lsb = ttk.Scrollbar(lb_scroll, orient="vertical", command=lb.yview)
        lb.config(yscrollcommand=lsb.set)
        lb.pack(side="left", fill="both", expand=True)
        lsb.pack(side="right", fill="y")
        _bind_mousewheel(lb, lsb)

        for fname in self._target_file_list:
            lb.insert("end", fname)

        status_lbl = tk.Label(left_card, text="", bg=self.C("panel"),
                              fg=self.C("muted"), font=(_UI_FONT, 8), wraplength=200)
        status_lbl.pack(padx=10, pady=(0, 5))
        if self._target_file_list:
            status_lbl.config(text=f"{len(self._target_file_list)} file(s) cached.")

        # ── Right: entries table ───────────────────────────────────
        right_card = tk.Frame(body, bg=self.C("panel"),
                              highlightbackground=self.C("border"), highlightthickness=1)
        right_card.pack(side="left", fill="both", expand=True)

        style = ttk.Style()
        style.configure("Dark.Treeview",
                        background=self.C("input_bg"), foreground=self.C("text"),
                        rowheight=26, fieldbackground=self.C("input_bg"))
        style.configure("Dark.Treeview.Heading",
                        background=self.C("primary"), foreground="white",
                        font=(_UI_FONT, 10, "bold"), relief="flat")
        style.map("Dark.Treeview",
                  background=[("selected", self.C("primary"))],
                  foreground=[("selected", "#ffffff")])

        # ── Right header: title + search bar ──────────────────────
        hdr2 = tk.Frame(right_card, bg=self.C("panel"))
        hdr2.pack(fill="x", padx=12, pady=(10, 6))
        tk.Label(hdr2, text="Target Entries", bg=self.C("panel"),
                 fg=self.C("muted"), font=(_UI_FONT, 10, "bold")).pack(side="left")

        search_var = tk.StringVar()
        tk.Label(hdr2, text="🔍", bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 10)).pack(side="right", padx=(0, 4))
        search_entry = tk.Entry(hdr2, textvariable=search_var,
                                bg=self.C("input_bg"), fg=self.C("text"),
                                insertbackground=self.C("text"), relief="flat",
                                width=26, font=(_UI_FONT, 9))
        search_entry.pack(side="right", ipady=4, padx=(0, 6))
        tk.Label(hdr2, text="Search:", bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8)).pack(side="right", padx=(0, 4))

        tree_frame = tk.Frame(right_card, bg=self.C("panel"))
        tree_frame.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        COLS = ("Filter ID", "Mobile Number", "Target Name")
        tree = ttk.Treeview(tree_frame, columns=COLS, show="headings",
                            style="Dark.Treeview")
        for c, w in (("Filter ID", 140), ("Mobile Number", 160), ("Target Name", 260)):
            tree.heading(c, text=c); tree.column(c, width=w, anchor="w")
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        _bind_mousewheel(tree, vsb)

        # Holds all rows for the current file so filtering never needs re-fetch
        _all_rows = []
        _sort_state = {c: False for c in COLS}   # False = ascending

        def _update_count():
            n = len(tree.get_children())
            total = len(_all_rows)
            if n == total:
                count_badge.config(text=f"  {total} target{'s' if total != 1 else ''}  ")
            else:
                count_badge.config(text=f"  {n} / {total} shown  ")
            count_badge.pack(side="left", padx=14) if total else count_badge.pack_forget()

        def _populate_tree(rows):
            tree.delete(*tree.get_children())
            for r in rows:
                tree.insert("", "end", values=r)
            _update_count()

        def _apply_filter(*_):
            q = search_var.get().strip().lower()
            if not q:
                _populate_tree(_all_rows)
                return
            filtered = [r for r in _all_rows
                        if any(q in str(v).lower() for v in r)]
            _populate_tree(filtered)

        search_var.trace_add("write", _apply_filter)

        # ── Column sorting ─────────────────────────────────────────
        def _sort_column(col):
            reverse = _sort_state[col]
            _sort_state[col] = not reverse
            items = [(tree.set(k, col), k) for k in tree.get_children("")]
            items.sort(key=lambda t: t[0].lower(), reverse=reverse)
            for index, (_, k) in enumerate(items):
                tree.move(k, "", index)
            arrow = " ▲" if not reverse else " ▼"
            for c in COLS:
                tree.heading(c, text=c + (arrow if c == col else ""))
            _update_count()

        for col in COLS:
            tree.heading(col, command=lambda c=col: _sort_column(c))

        # ── Copy selected row to clipboard ─────────────────────────
        def _copy_selected():
            sel = tree.selection()
            if not sel:
                return self._toast("No row selected.", "error")
            vals = tree.item(sel[0], "values")
            text = "\t".join(str(v) for v in vals)
            self.clipboard_clear()
            self.clipboard_append(text)
            self._toast(f"Copied: {vals[1] if len(vals) > 1 else text}", "success")

        tree.bind("<Control-c>", lambda e: _copy_selected())

        def _tree_right_click(event):
            item = tree.identify_row(event.y)
            if not item:
                return
            tree.selection_set(item)
            ctx = tk.Menu(self, tearoff=0,
                          bg=self.C("panel"), fg=self.C("text"),
                          activebackground=self.C("primary"),
                          activeforeground="white", relief="flat")
            ctx.add_command(label="📋  Copy Row", command=_copy_selected)
            ctx.add_command(label="📱  Copy Mobile Number", command=lambda: (
                self.clipboard_clear(),
                self.clipboard_append(tree.item(item, "values")[1]
                                      if len(tree.item(item, "values")) > 1 else ""),
                self._toast("Mobile number copied.", "success")))
            ctx.post(event.x_root, event.y_root)

        tree.bind("<Button-3>", _tree_right_click)

        def _restore_cache():
            nonlocal _all_rows
            sel = lb.curselection()
            if not sel:
                return
            fname    = lb.get(sel[0])
            cache_key = f"{active_path[0]}|{fname}"
            rows     = self._target_cache.get(cache_key, [])
            _all_rows = list(rows)
            _apply_filter()

        lb.bind("<<ListboxSelect>>", lambda e: _restore_cache())

        # ── Toolbar ────────────────────────────────────────────────
        toolbar = tk.Frame(main, bg=self.C("bg"))
        toolbar.pack(fill="x", padx=50, pady=10)

        def list_remote():
            _is_ip_path = "CompleteFilterFiles" in active_path[0]
            _srv_key = "ip" if _is_ip_path else "ludr"
            _srv_name = "IP Probe" if _is_ip_path else "LUDR"
            ip = self.cfg.get(_srv_key, {}).get("ip", "")
            if not ip:
                return self.popup("Error", f"{_srv_name} server not configured.", "error")
            ssh = CONN.get_ssh(_srv_key)
            if not ssh:
                return self.popup("Error", f"{_srv_name} server is not connected.", "error")
            path = active_path[0]
            status_lbl.config(text="Listing files…")
            LOG.log("Target Details", f"Listing from {path} on {ip}")
            def _run():
                try:
                    _, out, _ = ssh.exec_command(f"ls -1 {shlex.quote(path)}")
                    files = [f.strip() for f in
                             out.read().decode("utf-8", "ignore").split() if f.strip()]
                    self._target_file_list = files
                    LOG.log("Target Details", f"Found {len(files)} query file(s)")
                    def _update():
                        lb.delete(0, "end")
                        for f in files:
                            lb.insert("end", f)
                        status_lbl.config(text=f"{len(files)} file(s) found.")
                        if files:
                            lb.selection_set(0)
                            lb.see(0)
                            load_selected()
                    self.after(0, _update)
                except Exception as e:
                    LOG.log("Target Details", f"List error: {e}", "ERROR")
                    self.after(0, lambda: status_lbl.config(text=f"Error: {e}"))
            threading.Thread(target=_run, daemon=True).start()

        def load_selected():
            nonlocal _all_rows
            sel = lb.curselection()
            if not sel:
                return
            fname     = lb.get(sel[0])
            cache_key = f"{active_path[0]}|{fname}"
            if cache_key in self._target_cache:
                rows = self._target_cache[cache_key]
                _all_rows = list(rows)
                _apply_filter()
                LOG.log("Target Details",
                        f"Loaded {fname} from cache ({len(rows)} targets)")
                return
            _is_ip_path = "CompleteFilterFiles" in active_path[0]
            _srv_key = "ip" if _is_ip_path else "ludr"
            sftp = CONN.get_sftp(_srv_key)
            if not sftp:
                return self.popup("Error", f"{'IP Probe' if _is_ip_path else 'LUDR'} server is not connected.", "error")
            remote = f"{active_path[0].rstrip('/')}/{fname}"
            LOG.log("Target Details", f"Fetching {remote}")
            def _run():
                try:
                    with sftp.file(remote, "rb") as f:
                        raw = f.read()
                    lines = []
                    for enc in ("utf-8", "utf-16", "latin-1"):
                        try: lines = raw.decode(enc).splitlines(); break
                        except Exception: continue
                    rows = []
                    cur_qn = cur_qi = cur_mobile = None
                    for line in lines:
                        s = line.strip()
                        if s.startswith("*QN"):
                            cur_qn = s.split()[1] if len(s.split()) > 1 else ""
                            cur_qi = cur_mobile = None
                        elif s.startswith("*QI") and cur_qn:
                            parts = s.split()
                            cur_qi = parts[1] if len(parts) > 1 else ""
                        elif s.startswith("*A") and cur_qn:
                            m = re.search(r'\*?\.?\*?(\d{7,15})\s*$', s)
                            if m: cur_mobile = m.group(1)
                            if cur_qn and cur_qi:
                                rows.append((cur_qn, cur_mobile or "—", cur_qi))
                                cur_qn = cur_qi = cur_mobile = None
                    self._target_cache[cache_key] = rows
                    self._target_last_load = time.time()
                    LOG.log("Target Details",
                            f"Parsed {len(rows)} target(s) from {fname}")
                    def _update():
                        nonlocal _all_rows
                        _all_rows = list(rows)
                        _apply_filter()
                    self.after(0, _update)
                except Exception as e:
                    LOG.log("Target Details", f"Load error: {e}", "ERROR")
                    self.after(0, lambda: self.popup("Error", str(e), "error"))
            threading.Thread(target=_run, daemon=True).start()

        # ── Export CSV ─────────────────────────────────────────────
        def _export_csv():
            if not _all_rows:
                return self.popup("Error", "No target data loaded to export.", "error")
            path = filedialog.asksaveasfilename(
                title="Save Targets as CSV",
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv")],
                initialfile="target_details.csv")
            if not path:
                return
            try:
                with open(path, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(COLS)
                    # Export what is currently visible (respects active filter)
                    visible = [tree.item(k, "values") for k in tree.get_children()]
                    w.writerows(visible)
                LOG.log("Target Details", f"Exported {len(visible)} row(s) → {path}")
                self._toast(f"✅  Exported {len(visible)} row(s).", "success")
            except Exception as e:
                self.popup("Error", str(e), "error")

        tk.Button(toolbar, text="🔄  Refresh File List", bg=self.C("success"),
                  fg="white", relief="flat", width=20, height=2,
                  font=(_UI_FONT, 10, "bold"), activebackground=self.C("border"),
                  command=list_remote).pack(side="left", padx=(0, 8))
        tk.Button(toolbar, text="⬇️  Load Selected File", bg=self.C("primary"),
                  fg="white", relief="flat", width=20, height=2,
                  font=(_UI_FONT, 10, "bold"), activebackground=self.C("border"),
                  command=load_selected).pack(side="left", padx=(0, 8))
        tk.Button(toolbar, text="📋  Copy Selected Row", bg="#5b6aae",
                  fg="white", relief="flat", width=20, height=2,
                  font=(_UI_FONT, 10, "bold"), activebackground=self.C("border"),
                  command=_copy_selected).pack(side="left", padx=(0, 8))
        tk.Button(toolbar, text="💾  Export CSV", bg="#0e7490",
                  fg="white", relief="flat", width=16, height=2,
                  font=(_UI_FONT, 10, "bold"), activebackground=self.C("border"),
                  command=_export_csv).pack(side="left", padx=(0, 8))


        # ── Auto-refresh ────────────────────────────────────────────
        _auto_job = [None]
        _AUTO_INTERVAL_MS = 30_000

        def _auto_refresh():
            try:
                if not lb.winfo_exists():
                    return
            except Exception:
                return

            def _list_and_reload():
                ip = self.cfg["ludr"].get("ip", "")
                if not ip:
                    return
                ssh = CONN.get_ssh("ludr")
                if not ssh:
                    return
                path = active_path[0]
                def _run():
                    try:
                        _, out, _ = ssh.exec_command(f"ls -1 {shlex.quote(path)}")
                        files = [f.strip() for f in
                                 out.read().decode("utf-8", "ignore").split()
                                 if f.strip()]
                        self._target_file_list = files
                        def _update():
                            if not lb.winfo_exists():
                                return
                            prev_sel  = lb.curselection()
                            prev_name = lb.get(prev_sel[0]) if prev_sel else None
                            lb.delete(0, "end")
                            for f in files:
                                lb.insert("end", f)
                            if prev_name and prev_name in files:
                                idx = files.index(prev_name)
                                lb.selection_set(idx)
                                lb.see(idx)
                                key = f"{active_path[0]}|{prev_name}"
                                self._target_cache.pop(key, None)
                                load_selected()
                            status_lbl.config(
                                text=f"{len(files)} file(s) — auto-refreshed")
                        self.after(0, _update)
                    except Exception:
                        pass
                threading.Thread(target=_run, daemon=True).start()

            _list_and_reload()
            _auto_job[0] = self.after(_AUTO_INTERVAL_MS, _auto_refresh)

        def _switch_path_and_refresh(lbl, path):
            nonlocal _all_rows
            _all_rows = []
            _switch_path(lbl, path)
            list_remote()

        for lbl2, b2 in _path_btns.items():
            pth2 = QUERY_PATHS[lbl2]
            b2.config(command=lambda lb=lbl2, p=pth2: _switch_path_and_refresh(lb, p))

        def _initial_load():
            if not lb.winfo_exists():
                return
            list_remote()
            _auto_job[0] = self.after(_AUTO_INTERVAL_MS, _auto_refresh)

        self.after(500, _initial_load)

    # ──────────────────────────────────────────────────────────────
    # SETTINGS
    # ──────────────────────────────────────────────────────────────
    def show_settings(self):
        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Settings page")

        canvas, sf = self._scrollable(main)

        # ── Professional page header ───────────────────────────────
        page_hdr = tk.Frame(sf, bg=self.C("bg"))
        page_hdr.pack(fill="x", padx=50, pady=(28, 0))

        # Left: title block
        title_blk = tk.Frame(page_hdr, bg=self.C("bg"))
        title_blk.pack(side="left")
        tk.Label(title_blk, text="⚙️  Settings",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 22, "bold")).pack(anchor="w")
        tk.Label(title_blk,
                 text="Configure server connections, web UI, and appearance.",
                 bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(anchor="w", pady=(2, 0))

        # Right: action buttons
        btn_blk = tk.Frame(page_hdr, bg=self.C("bg"))
        btn_blk.pack(side="right", anchor="center")
        tk.Button(btn_blk, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", padx=10, pady=4,
                  command=self.show_first_page).pack(side="right", padx=(8, 0))

        # Compact Appearance toggle — between Save and Back
        _ap = tk.Frame(btn_blk, bg=self.C("bg"))
        _ap.pack(side="right", padx=(0, 6))
        tk.Label(_ap, text="🎨", bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT, 10)).pack(side="left", padx=(0, 3))
        for _t_label, _t_key, _t_icon in [("Dark", "dark", "🌙"), ("Light", "light", "☀️")]:
            _is_active = self._theme == _t_key
            _tbg = self.C("primary") if _is_active else self.C("panel")
            _tfg = "white" if _is_active else self.C("muted")
            tk.Button(_ap, text=f"{_t_icon} {_t_label}",
                      bg=_tbg, fg=_tfg, relief="flat",
                      font=(_UI_FONT, 8, "bold"), cursor="hand2",
                      padx=7, pady=3,
                      command=lambda k=_t_key: self.switch_theme(k)).pack(
                side="left", padx=1)

        for btn_txt, btn_cmd, btn_bg in [
            ("📤  Import Config",     self.upload_settings_csv,   self.C("primary")),
            ("📥  Export Config",     self.download_settings_csv, "#059669"),
            ("💾  Save All Settings", self.save_all_settings,     "#0891b2"),
        ]:
            tk.Button(btn_blk, text=btn_txt, bg=btn_bg, fg="white",
                      relief="flat", font=(_UI_FONT, 9, "bold"),
                      cursor="hand2", activebackground=self.C("border"),
                      command=btn_cmd).pack(
                side="left", ipadx=12, ipady=5, padx=(0, 8))

        # Divider
        tk.Frame(sf, bg=self.C("primary"), height=2).pack(
            fill="x", padx=50, pady=(12, 20))

        container = tk.Frame(sf, bg=self.C("bg"))
        container.pack(fill="x", padx=50)
        self.settings_entries = {}

        # ── Section: Server Configuration ─────────────────────────
        self._prof_section(container, "🖥️", "Server Configuration",
                           "SSH connection details for each ComTrail server.")
        for key, title, icon in [
            ("pcap",  "PCAP Server",            "📡"),
            ("ludr",  "LBS / LUDR / SMS Server","📍"),
            ("voice", "Voice Server",           "📞"),
            ("ip",    "IP Probe Server",        "🌐"),
        ]:
            self._settings_card(container, key, title, icon)

        # ── Section: Kafka & Solr ──────────────────────────────────
        self._prof_section(container, "📊", "Kafka & Solr",
                           "Connection details for the Kafka UI and Solr query services.")
        self._kafka_solr_card(container)

        # ── Section: Web UI ────────────────────────────────────────
        self._prof_section(container, "🌐", "ComTrail Web UI",
                           "URL used to open the ComTrail browser interface.")
        self._url_card(container)

        self._prof_section(container, "🔡", "Font Size",
                           "Adjust text size across the entire application.")
        font_card = self._card(container)
        font_body = tk.Frame(font_card, bg=self.C("panel"))
        font_body.pack(fill="x", padx=20, pady=14)

        tk.Label(font_body, text="Select a preset or use fine-tune controls:",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(anchor="w", pady=(0, 10))

        _PRESETS = [
            ("XS",  0.75, "Extra Small"),
            ("S",   0.88, "Small"),
            ("M",   1.00, "Normal (Default)"),
            ("L",   1.15, "Large"),
            ("XL",  1.30, "Extra Large"),
        ]

        btn_row = tk.Frame(font_body, bg=self.C("panel"))
        btn_row.pack(anchor="w")

        current_scale = round(self._font_scale, 2)

        for code, scale, label in _PRESETS:
            is_active = abs(current_scale - scale) < 0.05
            btn = tk.Button(
                btn_row, text=f"{code}\n{label}",
                bg=self.C("primary") if is_active else self.C("input_bg"),
                fg="white" if is_active else self.C("muted"),
                relief="flat", cursor="hand2", width=12,
                font=(_UI_FONT, 9, "bold" if is_active else "normal"),
                activebackground=self.C("primary"),
                command=lambda s=scale: self._apply_font_scale(s))
            btn.pack(side="left", padx=(0, 8), ipady=6)

        # Fine-tune with +/- buttons
        fine_row = tk.Frame(font_body, bg=self.C("panel"))
        fine_row.pack(anchor="w", pady=(12, 0))
        tk.Label(fine_row, text="Fine tune:",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(side="left", padx=(0, 10))

        scale_var = tk.StringVar(value=f"{current_scale:.2f}×")
        tk.Label(fine_row, textvariable=scale_var,
                 bg=self.C("panel"), fg=self.C("card_title"),
                 font=(_UI_FONT, 10, "bold"), width=6).pack(side="left")

        def _nudge(delta):
            new_scale = round(max(0.6, min(1.6, self._font_scale + delta)), 2)
            scale_var.set(f"{new_scale:.2f}×")
            self._apply_font_scale(new_scale)

        tk.Button(fine_row, text="  −  ", relief="flat", cursor="hand2",
                  bg=self.C("input_bg"), fg=self.C("text"),
                  font=(_UI_FONT, 11, "bold"),
                  command=lambda: _nudge(-0.05)).pack(side="left", ipady=3, padx=(0, 4))
        tk.Button(fine_row, text="  +  ", relief="flat", cursor="hand2",
                  bg=self.C("input_bg"), fg=self.C("text"),
                  font=(_UI_FONT, 11, "bold"),
                  command=lambda: _nudge(+0.05)).pack(side="left", ipady=3)

        tk.Label(font_body,
                 text="Changes apply immediately to all text rendered by tkinter's "
                      "named fonts (menus, dialogs, labels). "
                      "Navigate away and back to apply to all page content.",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8), wraplength=520,
                 justify="left").pack(anchor="w", pady=(10, 0))


    # Known valid remote paths per server key
    _KNOWN_PATHS = {
        "pcap":  [
            "/data5/prism/Paths/InputDir4/WatchDir",
        ],
        "ludr":  [
            "/data5/prism/Paths/InputDir1/WatchDir",
        ],
        "voice": [
            "/data5/prism/Paths/InputDir1/WatchDir",
        ],
        "ip": [
            "/data5/prism/Paths/InputDir1/WatchDir",
        ],
    }

    def _prof_section(self, parent, icon, title, subtitle=""):
        """Professional section header with icon, bold title, and subtitle line."""
        sf = tk.Frame(parent, bg=self.C("bg"))
        sf.pack(fill="x", pady=(22, 6))
        # Left accent bar
        tk.Frame(sf, bg=self.C("primary"), width=4).pack(side="left", fill="y")
        txt = tk.Frame(sf, bg=self.C("bg"))
        txt.pack(side="left", padx=(10, 0))
        title_row = tk.Frame(txt, bg=self.C("bg"))
        title_row.pack(anchor="w")
        tk.Label(title_row, text=icon,
                 bg=self.C("bg"), fg=self.C("primary"),
                 font=(_UI_FONT, 13)).pack(side="left", padx=(0, 6))
        tk.Label(title_row, text=title,
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 13, "bold")).pack(side="left")
        if subtitle:
            tk.Label(txt, text=subtitle,
                     bg=self.C("bg"), fg=self.C("muted"),
                     font=(_UI_FONT, 8)).pack(anchor="w", pady=(1, 0))

    def _settings_card(self, parent, key, title, icon):
        cfg      = self.cfg.get(key, {"ip": "", "pwd": "", "path": ""})
        _accent  = {"pcap": "#06b6d4", "ludr": "#10b981", "voice": "#8b5cf6", "ip": "#f59e0b"}.get(key, self.C("primary"))
        _last_ts = self.cfg.get(f"_{key}_saved_ts", "")

        # Card frame — thin left accent bar, no heavy border
        card = tk.Frame(parent, bg=self.C("panel"),
                        highlightbackground=self.C("border"), highlightthickness=1)
        card.pack(fill="x", pady=3)
        tk.Frame(card, bg=_accent, width=4).pack(side="left", fill="y")

        body = tk.Frame(card, bg=self.C("panel"))
        body.pack(side="left", fill="x", expand=True, padx=12, pady=8)

        # ── Single row: title · status dot · fields · save ────────
        row = tk.Frame(body, bg=self.C("panel"))
        row.pack(fill="x")

        # Title
        tk.Label(row, text=f"{icon}  {title}",
                 bg=self.C("panel"), fg=self.C("card_title"),
                 font=(_UI_FONT, 10, "bold"), width=22, anchor="w").pack(side="left")

        # Connection status text
        _conn_lbl = tk.Label(row, text="", bg=self.C("panel"),
                             font=(_UI_FONT, 8, "bold"), padx=8)
        _conn_lbl.pack(side="left", padx=(0, 8))

        def _refresh_conn_lbl(k=key, lbl=_conn_lbl):
            try:
                if not lbl.winfo_exists():
                    return
            except Exception:
                return
            st = CONN.state(k)
            if st == "connected":
                lbl.config(text="Connected", fg="#22c55e")
            elif st == "failed":
                lbl.config(text="Failed", fg="#ef4444")
            else:
                lbl.config(text="Connecting…", fg="#f59e0b")
            lbl.after(2000, _refresh_conn_lbl)

        _refresh_conn_lbl()

        entries = {}

        # ── IP field ──────────────────────────────────────────────
        def _mini_field(parent_r, field, placeholder, show="", w=16):
            fr = tk.Frame(parent_r, bg=self.C("input_bg"),
                          highlightbackground=self.C("border"), highlightthickness=1)
            fr.pack(side="left", padx=(0, 6))
            tk.Label(fr, text=placeholder, bg=self.C("input_bg"),
                     fg=self.C("dim"), font=(_UI_FONT, 7)).pack(
                anchor="w", padx=6, pady=(3, 0))
            e = tk.Entry(fr, width=w, show=show,
                         bg=self.C("input_bg"), fg=self.C("text"),
                         insertbackground=self.C("text"),
                         relief="flat", font=(_UI_FONT, 9), bd=0)
            e.insert(0, cfg.get(field, ""))
            e.pack(side="left", padx=6, pady=(0, 4))
            # ✕ clear
            clr = tk.Label(fr, text="✕", bg=self.C("input_bg"),
                           fg=self.C("dim"), font=(_UI_FONT, 8), cursor="hand2")
            clr.pack(side="left", padx=(0, 4))
            clr.bind("<Button-1>", lambda ev, en=e: en.delete(0, "end"))
            return e

        ip_entry  = _mini_field(row, "ip",  "IP Address", w=15)
        entries["ip"] = ip_entry
        # 📋 Copy IP to clipboard
        def _copy_ip(ev=None, _e=ip_entry):
            v = _e.get().strip()
            if v:
                self.clipboard_clear(); self.clipboard_append(v)
                self._toast(f"Copied: {v}", "info", duration=1800)
        _cp_lbl = tk.Label(row, text="📋", bg=self.C("input_bg"),
                           fg=self.C("dim"), font=(_UI_FONT, 10), cursor="hand2")
        _cp_lbl.pack(side="left", padx=(0, 6))
        _cp_lbl.bind("<Button-1>", _copy_ip)
        _add_tooltip(_cp_lbl, "Copy IP to clipboard")

        # Password + eye toggle
        pwd_fr = tk.Frame(row, bg=self.C("input_bg"),
                          highlightbackground=self.C("border"), highlightthickness=1)
        pwd_fr.pack(side="left", padx=(0, 6))
        tk.Label(pwd_fr, text="Password", bg=self.C("input_bg"),
                 fg=self.C("dim"), font=(_UI_FONT, 7)).pack(
            anchor="w", padx=6, pady=(3, 0))
        pwd_row = tk.Frame(pwd_fr, bg=self.C("input_bg"))
        pwd_row.pack(padx=4, pady=(0, 4))
        pwd_show  = [False]
        pwd_entry = tk.Entry(pwd_row, width=14, show="*",
                             bg=self.C("input_bg"), fg=self.C("text"),
                             insertbackground=self.C("text"),
                             relief="flat", font=(_UI_FONT, 9), bd=0)
        pwd_entry.insert(0, cfg.get("pwd", ""))
        pwd_entry.pack(side="left")
        clr_p = tk.Label(pwd_row, text="✕", bg=self.C("input_bg"),
                         fg=self.C("dim"), font=(_UI_FONT, 8), cursor="hand2")
        clr_p.pack(side="left", padx=(3, 0))
        clr_p.bind("<Button-1>", lambda ev: pwd_entry.delete(0, "end"))
        eye = tk.Label(pwd_row, text="👁", bg=self.C("input_bg"),
                       fg=self.C("dim"), font=(_UI_FONT, 9), cursor="hand2")
        eye.pack(side="left", padx=(3, 0))
        def _eye(ev=None):
            pwd_show[0] = not pwd_show[0]
            pwd_entry.config(show="" if pwd_show[0] else "*")
            eye.config(fg=self.C("text") if pwd_show[0] else self.C("dim"))
        eye.bind("<Button-1>", _eye)
        entries["pwd"] = pwd_entry

        # Remote Path combobox
        path_fr = tk.Frame(row, bg=self.C("input_bg"),
                           highlightbackground=self.C("border"), highlightthickness=1)
        path_fr.pack(side="left", padx=(0, 6))
        tk.Label(path_fr, text="Remote Path", bg=self.C("input_bg"),
                 fg=self.C("dim"), font=(_UI_FONT, 7)).pack(
            anchor="w", padx=6, pady=(3, 0))
        known_paths = self._KNOWN_PATHS.get(key, [])
        path_var    = tk.StringVar(value=cfg.get("path", ""))
        path_cb     = ttk.Combobox(path_fr, textvariable=path_var,
                                   values=known_paths, width=28, font=(_UI_FONT, 9))
        path_cb.pack(padx=6, pady=(0, 4))

        class _PathEntry:
            def get(self_):        return path_var.get().strip()
            def delete(self_, *a): path_var.set("")
            def insert(self_, i, v): path_var.set(v)
        entries["path"] = _PathEntry()

        path_hint = tk.Label(body, text="", bg=self.C("panel"),
                             fg=self.C("dim"), font=(_UI_FONT, 7))
        path_hint.pack(anchor="w", pady=(2, 0))

        def _validate_path(event=None):
            p = path_var.get().strip()
            if not p:
                path_hint.config(text="")
            elif p in known_paths:
                path_hint.config(text="✅  Known valid path", fg=self.C("success"))
            else:
                path_hint.config(text="⚠️  Custom path — verify on server", fg="#f59e0b")
        path_cb.bind("<<ComboboxSelected>>", _validate_path)
        path_cb.bind("<FocusOut>", _validate_path)
        _validate_path()

        self.settings_entries[key] = entries

        # ── Save button + inline result ───────────────────────────
        result_lbl = tk.Label(row, text="", bg=self.C("panel"),
                              fg=self.C("success"), font=(_UI_FONT, 8))
        result_lbl.pack(side="right", padx=(0, 8))

        if not hasattr(self, "_settings_result_labels"):
            self._settings_result_labels = {}

        ts_lbl = tk.Label(row, text=f"  {_last_ts}" if _last_ts else "",
                          bg=self.C("panel"), fg=self.C("dim"),
                          font=(_UI_FONT, 7))
        ts_lbl.pack(side="right")
        self._settings_result_labels[key] = (result_lbl, ts_lbl)

        def _do_save(k=key):
            result_lbl.config(text="Saving…", fg=self.C("muted"))
            self._save_section_validated(k)

        tk.Button(row, text="💾  Save", bg=_accent, fg="white",
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", activebackground=self.C("border"),
                  command=_do_save).pack(side="right", ipadx=10, ipady=3, padx=(0, 6))

        for e in [ip_entry, pwd_entry]:
            e.bind("<Return>", lambda ev, k=key: _do_save(k))

    def _kafka_solr_card(self, parent):
        """Compact inline card for Kafka and Solr configuration."""
        card = tk.Frame(parent, bg=self.C("panel"),
                        highlightbackground=self.C("border"), highlightthickness=1)
        card.pack(fill="x", pady=3)

        def _service_row(container_frame, svc_key, svc_icon, svc_title,
                         accent, field_defs):
            """One compact row per service (Kafka / Solr)."""
            row = tk.Frame(container_frame, bg=self.C("panel"))
            row.pack(fill="x", padx=0, pady=0)
            tk.Frame(row, bg=accent, width=4).pack(side="left", fill="y")

            inner = tk.Frame(row, bg=self.C("panel"))
            inner.pack(side="left", fill="x", expand=True, padx=12, pady=8)

            fields_row = tk.Frame(inner, bg=self.C("panel"))
            fields_row.pack(fill="x")

            # Label
            tk.Label(fields_row, text=f"{svc_icon}  {svc_title}",
                     bg=self.C("panel"), fg=self.C("card_title"),
                     font=(_UI_FONT, 10, "bold"), width=14, anchor="w").pack(
                side="left", padx=(0, 12))

            cfg_svc = self.cfg.get(svc_key, {})
            entries = {}

            for field_key, placeholder, w in field_defs:
                fr = tk.Frame(fields_row, bg=self.C("input_bg"),
                              highlightbackground=self.C("border"), highlightthickness=1)
                fr.pack(side="left", padx=(0, 8))
                tk.Label(fr, text=placeholder, bg=self.C("input_bg"),
                         fg=self.C("dim"), font=(_UI_FONT, 7)).pack(
                    anchor="w", padx=6, pady=(3, 0))
                ef = tk.Frame(fr, bg=self.C("input_bg"))
                ef.pack(padx=4, pady=(0, 4))
                e = tk.Entry(ef, width=w, bg=self.C("input_bg"),
                             fg=self.C("text"), insertbackground=self.C("text"),
                             relief="flat", font=(_UI_FONT, 9), bd=0)
                e.insert(0, cfg_svc.get(field_key, ""))
                e.pack(side="left")
                clr = tk.Label(ef, text="✕", bg=self.C("input_bg"),
                               fg=self.C("dim"), font=(_UI_FONT, 8), cursor="hand2")
                clr.pack(side="left", padx=(3, 0))
                clr.bind("<Button-1>", lambda ev, en=e: en.delete(0, "end"))
                entries[field_key] = e

            # Result label
            res_lbl = tk.Label(fields_row, text="", bg=self.C("panel"),
                               fg=self.C("success"), font=(_UI_FONT, 8))
            res_lbl.pack(side="right", padx=(0, 8))

            def _save(sk=svc_key, ents=entries, rl=res_lbl):
                vals = {k: v.get().strip() for k, v in ents.items()}
                self.cfg.setdefault(sk, {}).update(vals)
                save_config(self.cfg)
                _apply_service_config(self.cfg)
                rl.config(text="✅  Saved", fg=self.C("success"))
                LOG.log("Settings", f"Saved {sk.upper()} config")
                self.after(3000, lambda: rl.config(text="") if rl.winfo_exists() else None)

            tk.Button(fields_row, text="💾  Save", bg=accent, fg="white",
                      relief="flat", font=(_UI_FONT, 9, "bold"),
                      cursor="hand2", activebackground=self.C("border"),
                      command=_save).pack(side="right", ipadx=10, ipady=3)

            for e in entries.values():
                e.bind("<Return>", lambda ev, s=_save: s())

            tk.Frame(container_frame, bg=self.C("border"), height=1).pack(fill="x")

        _service_row(card, "kafka", "📊", "Kafka",  "#f59e0b", [
            ("base_url",    "Kafka UI Base URL",  32),
            ("cluster",     "Cluster Name",       16),
            ("topic_voice", "Voice Topic Name",   28),
            ("topic_ip",    "IP Topic Name",      28),
        ])
        _service_row(card, "solr",  "🔍", "Solr",   "#0ea5e9", [
            ("base_url",   "Solr Base URL", 38),
            ("collection", "Collection",    18),
        ])

    def _url_card(self, parent):
        card = self._card(parent)
        tk.Label(card, text="🌐  Web UI URL", bg=self.C("panel"),
                 fg=self.C("card_title"), font=(_UI_FONT, 12, "bold")).pack(side="left", padx=15)
        inp = tk.Frame(card, bg=self.C("panel"))
        inp.pack(side="left", fill="x", expand=True, padx=15)
        tk.Label(inp, text="URL", bg=self.C("panel"),
                 fg=self.C("muted"), font=(_UI_FONT, 9)).pack(anchor="w")
        self.url_entry = tk.Entry(inp, width=55, bg=self.C("input_bg"),
                                  fg=self.C("text"), insertbackground=self.C("text"), relief="flat")
        self.url_entry.insert(0, self.cfg.get("comtrail_url", ""))
        self.url_entry.pack(ipady=5, fill="x")
        right = tk.Frame(card, bg=self.C("panel"))
        right.pack(side="right", padx=15)
        tk.Button(right, text="💾  Save URL", bg=self.C("success"), fg="white",
                  relief="flat", width=12, font=(_UI_FONT, 10, "bold"),
                  activebackground=self.C("border"), command=self.save_url).pack()
        tk.Button(right, text="🔗  Open URL", bg=self.C("primary"), fg="white",
                  relief="flat", width=12, font=(_UI_FONT, 10, "bold"),
                  activebackground=self.C("border"),
                  command=lambda: webbrowser.open(self.url_entry.get())).pack(pady=(6, 0))

    def _save_all_card(self, parent):
        """Single card with one button that saves all three servers + URL at once."""
        card = self._card(parent)
        info = tk.Frame(card, bg=self.C("panel"))
        info.pack(side="left", padx=20, pady=12, fill="x", expand=True)
        tk.Label(info, text="💾  Save All Server Settings",
                 bg=self.C("panel"), fg=self.C("card_title"),
                 font=(_UI_FONT, 12, "bold")).pack(anchor="w")
        tk.Label(info,
                 text="Saves PCAP, LUDR, Voice and Web UI settings in a single click, "
                      "then reconnects all servers with the new credentials.",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9), wraplength=600, justify="left").pack(
                     anchor="w", pady=(4, 0))
        right = tk.Frame(card, bg=self.C("panel"))
        right.pack(side="right", padx=20, pady=12)
        tk.Button(right, text="💾  Save All Settings",
                  bg=self.C("success"), fg="white", relief="flat",
                  width=20, height=2, font=(_UI_FONT, 11, "bold"),
                  activebackground=self.C("border"),
                  command=self.save_all_settings).pack()

    def _csv_card(self, parent):
        """Import = upload a CSV to load all settings; Download = save current settings to CSV."""
        card = self._card(parent)
        for side, label, sub, bg, cmd in [
            ("left",
             "📤  Upload Settings (CSV)",
             "Import all server settings from a CSV file in one go",
             self.C("primary"),
             self.upload_settings_csv),
            ("right",
             "📥  Download Settings (CSV)",
             "Save current settings to a CSV file for backup",
             self.C("success"),
             self.download_settings_csv),
        ]:
            pane = tk.Frame(card, bg=self.C("panel"))
            pane.pack(side=side, padx=30, pady=15, fill="x", expand=True)
            tk.Label(pane, text=label, bg=self.C("panel"), fg=self.C("card_title"),
                     font=(_UI_FONT, 11, "bold")).pack(anchor="w")
            tk.Label(pane, text=sub, bg=self.C("panel"), fg=self.C("muted"),
                     font=(_UI_FONT, 9)).pack(anchor="w", pady=(3, 10))
            tk.Button(pane, text=label, bg=bg, fg="white",
                      relief="flat", width=26, height=2,
                      font=(_UI_FONT, 10, "bold"),
                      activebackground=self.C("border"),
                      command=cmd).pack(anchor="w")
        tk.Frame(card, bg=self.C("border"), width=1).place(relx=0.5, rely=0, relheight=1)

    # ──────────────────────────────────────────────────────────────
    # SETTINGS ACTIONS
    # ──────────────────────────────────────────────────────────────

    def _save_section_validated(self, key):
        """
        Save section with path validation.
        If path is NOT in the known-valid list:
          - If server is connected: verify via SFTP stat.
            Block save if path does not exist on server.
          - If server not connected: warn and block.
        """
        e     = self.settings_entries.get(key, {})
        path  = e["path"].get() if "path" in e else ""
        known = self._KNOWN_PATHS.get(key, [])

        # Blank path — always allow
        if not path:
            self.save_section(key)
            return

        # Known path — always allow
        if path in known:
            self.save_section(key)
            return

        # Unknown / custom path — must verify on server
        sftp = CONN.get_sftp(key)
        if sftp is None:
            # Server not connected — block
            self.popup(
                "Invalid Path",
                f"The path you entered is not in the known-valid list:\n\n"
                f"  {path}\n\n"
                f"Cannot verify because the server is not connected.\n"
                f"Please select a known path from the dropdown, or connect\n"
                f"the server first then try again.",
                "error")
            return

        # Server connected — try stat
        try:
            sftp.stat(path)
            # Path exists on server — allow with info toast
            self._toast(
                f"Custom path verified on server:\n{path}",
                kind="info", duration=4000)
            self.save_section(key)
        except IOError:
            # Path does NOT exist on server — block
            self.popup(
                "Path Does Not Exist",
                f"The path you entered does not exist on the server:\n\n"
                f"  {path}\n\n"
                f"Please select a valid path from the dropdown or\n"
                f"create the directory on the server first.",
                "error")

    def save_section(self, key):
        e = self.settings_entries.get(key, {})
        new_ip   = e["ip"].get().strip()
        new_pwd  = e["pwd"].get().strip()
        new_path = e["path"].get().strip()
        self.cfg.setdefault(key, {}).update({
            "ip":   new_ip,
            "pwd":  new_pwd,
            "path": new_path,
        })
        # Record save timestamp
        ts_now = time.strftime("%H:%M:%S")
        self.cfg[f"_{key}_saved_ts"] = ts_now
        save_config(self.cfg)
        LOG.log("Settings", f"Saved {key.upper()} — IP: {new_ip}")

        # Update inline result labels if present
        def _set_result(text, color):
            lbls = getattr(self, "_settings_result_labels", {}).get(key)
            if lbls:
                res_lbl, ts_lbl2 = lbls
                try:
                    if res_lbl.winfo_exists():
                        res_lbl.config(text=text, fg=color)
                    if ts_lbl2.winfo_exists():
                        ts_lbl2.config(text=f"Last saved: {ts_now}")
                except Exception:
                    pass

        if not new_ip or not new_pwd:
            _set_result("✅  Saved (no IP/password to connect)", self.C("success"))
            return

        _set_result("⏳  Saved — connecting…", self.C("muted"))

        # Force close + reconnect
        with CONN._lock:
            if key in CONN._conns:
                CONN._close_entry(CONN._conns[key])
                CONN._conns[key]["state"] = "pending"
        CONN.register(key, new_ip, new_pwd)

        def _check():
            state = CONN.state(key)
            if state == "connected":
                _set_result(f"✅  Connected to {new_ip}", self.C("success"))
                self._toast(f"✅  {key.upper()} connected to {new_ip}", "success")
            elif state == "failed":
                _set_result(f"❌  Could not connect to {new_ip}", self.C("error"))
                self._toast(f"❌  {key.upper()} connection failed", "error")
            else:
                _set_result(f"⏳  Still connecting to {new_ip}…", self.C("muted"))

        self.after(4000, _check)

    def save_all_settings(self):
        """Save all server sections + URL and reconnect with live result."""
        saved = []
        for key in ("pcap", "ludr", "voice", "ip"):
            e = self.settings_entries.get(key, {})
            if not e:
                continue
            new_ip   = e["ip"].get().strip()
            new_pwd  = e["pwd"].get().strip()
            new_path = e["path"].get().strip()
            self.cfg.setdefault(key, {}).update({
                "ip":   new_ip,
                "pwd":  new_pwd,
                "path": new_path,
            })
            if new_ip and new_pwd:
                with CONN._lock:
                    if key in CONN._conns:
                        CONN._close_entry(CONN._conns[key])
                        CONN._conns[key]["state"] = "pending"
                CONN.register(key, new_ip, new_pwd)
                saved.append(key.upper())

        if hasattr(self, "url_entry"):
            self.cfg["comtrail_url"] = self.url_entry.get().strip()

        save_config(self.cfg)
        LOG.log("Settings", f"All settings saved: {', '.join(saved) if saved else 'none'}")

        if not saved:
            self.popup("Saved", "Settings saved.\nNo servers configured yet.", "success")
            return

        # Show a progress popup, then check actual connection results after 4s
        top = tk.Toplevel(self)
        top.overrideredirect(True)
        top.configure(bg=self.C("border"))
        inner = tk.Frame(top, bg=self.C("panel"), padx=2, pady=2)
        inner.pack(fill="both", expand=True, padx=2, pady=2)
        tk.Frame(inner, bg=self.C("primary"), height=34).pack(fill="x")
        tk.Label(inner.winfo_children()[0],
                 text="💾  Saving & Connecting",
                 bg=self.C("primary"), fg="white",
                 font=(_UI_FONT, 10, "bold")).pack(side="left", padx=12, pady=6)
        msg_lbl = tk.Label(inner,
                           text=f"Connecting to {len(saved)} server(s)…",
                           bg=self.C("panel"), fg=self.C("card_title"),
                           font=(_UI_FONT, 10))
        msg_lbl.pack(pady=(10, 4))
        pb = ttk.Progressbar(inner, mode="indeterminate")
        pb.pack(padx=20, fill="x"); pb.start(10)
        top.update_idletasks()
        w, h = 360, 110
        x = self.winfo_rootx() + (self.winfo_width()  - w) // 2
        y = self.winfo_rooty() + (self.winfo_height() - h) // 2
        top.geometry(f"{w}x{h}+{x}+{y}")

        def _check_results():
            results = []
            for key in ("pcap", "ludr", "voice", "ip"):
                if key.upper() not in saved:
                    continue
                state = CONN.state(key)
                ip    = self.cfg.get(key, {}).get("ip", "")
                label = {"pcap": "PCAP", "ludr": "LBS", "voice": "Voice", "ip": "IP Probe"}[key]
                if state == "connected":
                    results.append(f"✅  {label}  ({ip})  — Connected")
                elif state == "failed":
                    results.append(f"❌  {label}  ({ip})  — Failed to connect")
                else:
                    results.append(f"⏳  {label}  ({ip})  — Still connecting…")

            try:
                pb.stop(); top.destroy()
            except Exception:
                pass

            self.popup("Settings Saved",
                       "Settings saved.\n\n" + "\n".join(results),
                       "success")

        # Wait 4 seconds for connections to establish, then report
        self.after(4000, _check_results)

    def test_section(self, key):
        e = self.settings_entries.get(key, {})
        ip  = e["ip"].get()
        pwd = e["pwd"].get()
        if not ip or not pwd:
            return self.popup("Error", "Enter IP and Password first.", "error")

        with CONN._lock:
            if key in CONN._conns:
                CONN._close_entry(CONN._conns[key])
                CONN._conns[key]["state"] = "pending"
        CONN.register(key, ip, pwd)

        top = tk.Toplevel(self)
        top.overrideredirect(True)
        top.configure(bg=self.C("border"))
        inner = tk.Frame(top, bg=self.C("panel"), padx=2, pady=2)
        inner.pack(fill="both", expand=True, padx=2, pady=2)
        title_bar = tk.Frame(inner, bg=self.C("primary"), height=34)
        title_bar.pack(fill="x"); title_bar.pack_propagate(False)
        tk.Label(title_bar, text="⏳  Testing Connection", bg=self.C("primary"), fg="white",
                 font=(_UI_FONT, 10, "bold")).pack(side="left", padx=12)
        tk.Label(inner, text=f"Connecting to {ip}…",
                 bg=self.C("panel"), fg=self.C("card_title"), font=(_UI_FONT, 11)).pack(pady=12)
        pb = ttk.Progressbar(inner, mode="indeterminate")
        pb.pack(padx=25, fill="x"); pb.start(10)
        top.update_idletasks()
        w, h = 380, 120
        x = self.winfo_rootx() + (self.winfo_width()  - w) // 2
        y = self.winfo_rooty() + (self.winfo_height() - h) // 2
        top.geometry(f"{w}x{h}+{x}+{y}")

        def _poll():
            state = CONN.state(key)
            if state == "pending":
                self.after(500, _poll)
            else:
                top.destroy()
                if state == "connected":
                    LOG.log("Settings", f"Test OK: {key.upper()} ({ip})")
                    self.popup("Success", f"Connected to {ip} successfully.", "success")
                else:
                    LOG.log("Settings", f"Test FAILED: {key.upper()} ({ip})", "ERROR")
                    self.popup("Failed", f"Could not connect to {ip}.\nCheck IP and password.", "error")
        self.after(500, _poll)

    def save_url(self):
        self.cfg["comtrail_url"] = self.url_entry.get()
        save_config(self.cfg)
        LOG.log("Settings", f"URL saved: {self.cfg['comtrail_url']}")
        self.popup("Saved", "ComTrail Web UI URL saved.", "success")

    def download_settings_csv(self):
        """Download current settings to CSV."""
        try:
            filename = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                initialfile="comtrail_settings.csv",
                title="Download Settings to CSV")
            if not filename:
                return
            rows = []
            for sec in ("pcap", "ludr", "voice", "ip"):
                d = self.cfg.get(sec) or {}
                rows.append({
                    "Section":    sec.upper(),
                    "IP_Address": d.get("ip",   ""),
                    "Password":   d.get("pwd",  ""),
                    "Path":       d.get("path", ""),
                    "Extra1":     "",
                    "Extra2":     "",
                    "Extra3":     "",
                })
            rows.append({
                "Section":    "COMTRAIL_URL",
                "IP_Address": self.cfg.get("comtrail_url", ""),
                "Password":   "",
                "Path":       "",
                "Extra1":     "",
                "Extra2":     "",
                "Extra3":     "",
            })
            kafka = self.cfg.get("kafka") or {}
            rows.append({
                "Section":    "KAFKA",
                "IP_Address": kafka.get("base_url",    ""),
                "Password":   "",
                "Path":       "",
                "Extra1":     kafka.get("cluster",     ""),
                "Extra2":     kafka.get("topic_voice", ""),
                "Extra3":     kafka.get("topic_ip",    ""),
            })
            solr = self.cfg.get("solr") or {}
            rows.append({
                "Section":    "SOLR",
                "IP_Address": solr.get("base_url", ""),
                "Password":   "",
                "Path":       "",
                "Extra1":     solr.get("collection", ""),
                "Extra2":     "",
                "Extra3":     "",
            })
            with open(filename, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["Section", "IP_Address", "Password", "Path", "Extra1", "Extra2", "Extra3"])
                w.writeheader()
                w.writerows(rows)
            LOG.log("Settings", f"Downloaded config → {filename}")
            self.popup("Exported",
                       f"Settings saved to:\n{os.path.basename(filename)}", "success")
        except Exception as e:
            LOG.log("Settings", f"Export error: {e}", "ERROR")
            self.popup("Error", f"Export failed:\n{e}", "error")

    def upload_settings_csv(self):
        """Import settings from a CSV and save + reconnect all at once."""
        filename = filedialog.askopenfilename(filetypes=[("CSV", "*.csv")])
        if not filename:
            return
        try:
            with open(filename, "r", encoding="utf-8") as f:
                data = list(csv.DictReader(f))
            if not data:
                return self.popup("Error", "CSV file is empty or has no valid rows.", "error")
            for row in data:
                sec = row.get("Section", "").strip().lower()
                if sec in ("pcap", "ludr", "voice", "ip"):
                    self.cfg[sec] = {
                        "ip":   row.get("IP_Address", "").strip(),
                        "pwd":  row.get("Password",   "").strip(),
                        "path": row.get("Path",       "").strip(),
                    }
                elif sec == "comtrail_url":
                    self.cfg["comtrail_url"] = row.get("IP_Address", "").strip()
                elif sec == "kafka":
                    self.cfg["kafka"] = {
                        "base_url":    row.get("IP_Address", "").strip(),
                        "cluster":     row.get("Extra1",     "").strip(),
                        "topic_voice": row.get("Extra2",     "").strip(),
                        "topic_ip":    row.get("Extra3",     "").strip(),
                    }
                elif sec == "solr":
                    self.cfg["solr"] = {
                        "base_url":   row.get("IP_Address", "").strip(),
                        "collection": row.get("Extra1",     "").strip(),
                    }
            save_config(self.cfg)
            LOG.log("Settings", f"Imported config from {filename}")
            self._register_all_connections()
            self.popup("Imported",
                       f"All settings loaded from\n{os.path.basename(filename)}\n"
                       "Servers are reconnecting.", "success")
            self.show_settings()
        except Exception as e:
            LOG.log("Settings", f"Import failed: {e}", "ERROR")
            self.popup("Error", str(e), "error")

    # ──────────────────────────────────────────────────────────────
    # LOGS
    # ──────────────────────────────────────────────────────────────
    def show_logs(self):
        self.clear_main()
        main = self.active_frame
        # Reset unseen error badge
        self._log_unseen_errors = 0
        self._log_seen_count    = len(LOG.all())
        self._update_log_badge()
        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(22, 0))
        tk.Label(hdr, text="📋  Logs",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2", padx=10, pady=4,
                  command=self.show_first_page).pack(side="right")
        tk.Frame(main, bg=self.C("border"), height=1).pack(
            fill="x", padx=50, pady=(8, 0))
        # Tab strip — card-style bar with clear active/inactive states
        tab_bar = tk.Frame(main, bg=self.C("panel"),
                           highlightbackground=self.C("border"),
                           highlightthickness=1)
        tab_bar.pack(fill="x", padx=50, pady=(6, 0))
        content = tk.Frame(main, bg=self.C("bg"))
        content.pack(fill="both", expand=True, padx=50, pady=(0, 8))
        active_tab = [None]
        tab_btns   = {}

        def _switch(tab):
            if active_tab[0] == tab:
                return
            active_tab[0] = tab
            for t, b in tab_btns.items():
                if t == tab:
                    b.config(bg=self.C("primary"),
                             fg="white",
                             relief="flat")
                else:
                    b.config(bg=self.C("panel"),
                             fg=self.C("muted"),
                             relief="flat")
            for w in content.winfo_children():
                w.destroy()
            if tab == "activity":
                self._build_activity_log_tab(content)
            else:
                self._build_server_log_panel(content)

        for key, label in [
            ("activity", "📋  Activity Logs"),
            ("server",   "🖥️  Server Logs"),
        ]:
            b = tk.Button(tab_bar, text=label,
                          bg=self.C("panel"), fg=self.C("muted"),
                          relief="flat",
                          font=(_UI_FONT, 10, "bold"),
                          activebackground=self.C("primary"),
                          activeforeground="white",
                          padx=22, pady=10,
                          cursor="hand2",
                          command=lambda k=key: _switch(k))
            b.pack(side="left")
            tab_btns[key] = b
        _switch("activity")

    def _build_activity_log_tab(self, parent):
        """Builds the Activity Logs content inside the Logs tab."""
        all_entries = LOG.all()
        total_count = len(all_entries)
        error_count = sum(1 for e in all_entries if e["level"] == "ERROR")
        warn_count  = sum(1 for e in all_entries if e["level"] == "WARNING")
        info_count  = total_count - error_count - warn_count

        # Track which entries are "new" (added since last Logs visit)
        _new_threshold = self._log_seen_count

        # ── Filter bar ────────────────────────────────────────────
        fbar = tk.Frame(parent, bg=self.C("panel"),
                        highlightbackground=self.C("border"),
                        highlightthickness=1)
        fbar.pack(fill="x", pady=(8, 0))

        # Level filter pills
        tk.Label(fbar, text="Level:",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(12, 4), pady=8)
        filter_var = tk.StringVar(value="ALL")
        for val, lbl, col in [
            ("ALL",     "All",     self.C("card_title")),
            ("INFO",    "Info",    "#06b6d4"),
            ("ERROR",   "Errors",  "#ef4444"),
            ("WARNING", "Warns",   "#f59e0b"),
        ]:
            tk.Radiobutton(fbar, text=lbl, variable=filter_var, value=val,
                           bg=self.C("panel"), fg=col,
                           selectcolor=self.C("input_bg"),
                           activebackground=self.C("panel"),
                           font=(_UI_FONT, 9, "bold"),
                           command=lambda: _rf()).pack(side="left", padx=6, pady=8)

        # Category filter
        tk.Frame(fbar, bg=self.C("border"), width=1).pack(side="left", fill="y", pady=6, padx=8)
        tk.Label(fbar, text="Category:",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(0, 4))
        categories = ["All"] + sorted({e["action"] for e in all_entries})
        cat_var = tk.StringVar(value="All")
        cat_cb  = ttk.Combobox(fbar, textvariable=cat_var,
                               values=categories, state="readonly", width=18)
        cat_cb.pack(side="left", ipady=3, padx=(0, 8))
        cat_var.trace_add("write", lambda *_: _rf())

        # Text search
        tk.Frame(fbar, bg=self.C("border"), width=1).pack(side="left", fill="y", pady=6, padx=4)
        tk.Label(fbar, text="🔍",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 10)).pack(side="left", padx=(4, 0))
        search_var = tk.StringVar()
        tk.Entry(fbar, textvariable=search_var, width=26,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"),
                 relief="flat", font=(_UI_FONT, 9)).pack(
            side="left", padx=(4, 2), ipady=4)
        tk.Button(fbar, text="✕", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 8), cursor="hand2",
                  command=lambda: search_var.set("")).pack(side="left", padx=(0, 8))
        search_var.trace_add("write", lambda *_: _rf())

        # ── Stats bar ─────────────────────────────────────────────
        stats_bg = "#164e63" if self._theme == "dark" else "#ecfeff"
        stats = tk.Frame(parent, bg=stats_bg,
                         highlightbackground=self.C("primary"),
                         highlightthickness=1)
        stats.pack(fill="x", pady=(6, 8), ipady=8)

        count_labels = {}
        for txt, key, val, color in [
            ("Total Events", "total", total_count,
             "#e2e8f0" if self._theme=="dark" else "#1e293b"),
            ("Info",    "info",  info_count,  "#67e8f9"),
            ("Errors",  "err",   error_count, "#f87171"),
            ("Warnings","warn",  warn_count,  "#fbbf24"),
        ]:
            box = tk.Frame(stats, bg=stats_bg)
            box.pack(side="left", padx=24)
            lv = tk.Label(box, text=str(val), bg=stats_bg, fg=color,
                          font=(_UI_FONT, 18, "bold"))
            lv.pack()
            tk.Label(box, text=txt, bg=stats_bg,
                     fg="#a5f3fc" if self._theme=="dark" else "#475569",
                     font=(_UI_FONT, 8)).pack()
            count_labels[key] = lv

        # Right-side buttons
        tk.Button(stats, text="🗑️  Clear",
                  bg="#374151", fg="white", relief="flat",
                  activebackground="#4b5563", activeforeground="white",
                  font=(_UI_FONT, 9, "bold"), cursor="hand2",
                  command=lambda: [LOG._entries.clear(), self.show_logs()]).pack(
            side="right", padx=16, pady=4, ipadx=8, ipady=3)
        tk.Button(stats, text="💾  Export",
                  bg="#0891b2", fg="white", relief="flat",
                  activebackground="#0e7490", activeforeground="white",
                  font=(_UI_FONT, 9, "bold"), cursor="hand2",
                  command=self._export_logs).pack(
            side="right", padx=6, pady=4, ipadx=8, ipady=3)


        # Live toggle
        live_var = tk.BooleanVar(value=True)
        _live_job = [None]
        def _toggle_live():
            if live_var.get():
                live_btn.config(bg="#22c55e", text="🟢  Live  ON")
                _schedule_live()
            else:
                live_btn.config(bg="#374151", text="⏸  Live  OFF")
                if _live_job[0]:
                    self.after_cancel(_live_job[0])
                    _live_job[0] = None

        live_btn = tk.Button(stats, text="🟢  Live  ON",
                             bg="#22c55e", fg="white", relief="flat",
                             activebackground="#16a34a",
                             font=(_UI_FONT, 9, "bold"), cursor="hand2",
                             command=lambda: (live_var.set(not live_var.get()),
                                              _toggle_live()))
        live_btn.pack(side="right", padx=6, pady=4, ipadx=8, ipady=3)
        _add_tooltip(live_btn, "Auto-refresh every 2 s")

        # Follow toggle
        _auto_scroll = [True]
        def _toggle_follow():
            _auto_scroll[0] = not _auto_scroll[0]
            follow_btn.config(
                bg="#22c55e" if _auto_scroll[0] else "#374151",
                text="⬇ Follow  ON" if _auto_scroll[0] else "⬇ Follow OFF")
        follow_btn = tk.Button(stats, text="⬇ Follow  ON",
                               bg="#22c55e", fg="white", relief="flat",
                               activebackground="#16a34a",
                               font=(_UI_FONT, 9, "bold"), cursor="hand2",
                               command=_toggle_follow)
        follow_btn.pack(side="right", padx=6, pady=4, ipadx=8, ipady=3)
        _add_tooltip(follow_btn, "Auto-scroll to latest entry")

        # ── Treeview ──────────────────────────────────────────────
        tree_bg = "#161b22" if self._theme == "dark" else self.C("input_bg")
        tree_fg = "#e2e8f0" if self._theme == "dark" else self.C("text")

        tf = tk.Frame(parent, bg=self.C("bg"))
        tf.pack(fill="both", expand=True)
        tf.grid_rowconfigure(0, weight=1)
        tf.grid_columnconfigure(0, weight=1)
        style = ttk.Style()
        style.configure("Log.Treeview",
                        background=tree_bg, foreground=tree_fg,
                        rowheight=26, fieldbackground=tree_bg,
                        borderwidth=0, font=(_UI_FONT, 9))
        style.configure("Log.Treeview.Heading",
                        background=self.C("primary"), foreground="white",
                        font=(_UI_FONT, 10, "bold"), relief="flat")
        style.map("Log.Treeview",
                  background=[("selected", self.C("primary"))],
                  foreground=[("selected", "#ffffff")])

        cols = ("Timestamp", "Level", "Category", "Detail")
        tree = ttk.Treeview(tf, columns=cols, show="headings",
                            style="Log.Treeview", selectmode="extended")
        for c, w in zip(cols, (160, 80, 160, 500)):
            tree.heading(c, text=c)
            tree.column(c, width=w, anchor="w")

        tree.tag_configure("err",  foreground="#f87171",
                           background="#2d1b1b" if self._theme=="dark" else "#fee2e2")
        tree.tag_configure("warn", foreground="#fbbf24",
                           background="#2b2000" if self._theme=="dark" else "#fef9c3")
        tree.tag_configure("new",  font=(_UI_FONT, 9, "bold"))

        vsb = ttk.Scrollbar(tf, orient="vertical",   command=tree.yview)
        hsb = ttk.Scrollbar(tf, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        _bind_mousewheel(tree, vsb)
        self._log_tree = tree

        def _rf():
            fv  = filter_var.get()
            cv  = cat_var.get()
            sq  = search_var.get().strip().lower()
            tree.delete(*tree.get_children())
            entries = list(LOG.all())
            shown = 0
            for idx, entry in enumerate(reversed(entries)):
                if fv != "ALL" and entry["level"] != fv:
                    continue
                if cv != "All" and entry["action"] != cv:
                    continue
                if sq and not any(sq in str(v).lower() for v in entry.values()):
                    continue
                lvl_tag = {"ERROR": "err", "WARNING": "warn"}.get(entry["level"], "")
                # "new" tag for entries added after last Logs visit
                is_new  = (len(entries) - 1 - idx) >= _new_threshold
                tags    = tuple(t for t in (lvl_tag, "new" if is_new else "") if t)
                tree.insert("", "end",
                            values=(entry["ts"], entry["level"],
                                    entry["action"], entry["detail"]),
                            tags=tags)
                shown += 1
            # Update stats
            all_e = LOG.all()
            tc = len(all_e)
            ec = sum(1 for e in all_e if e["level"] == "ERROR")
            wc = sum(1 for e in all_e if e["level"] == "WARNING")
            ic = tc - ec - wc
            count_labels["total"].config(text=str(tc))
            count_labels["info"].config(text=str(ic))
            count_labels["err"].config(text=str(ec))
            count_labels["warn"].config(text=str(wc))
            if _auto_scroll[0] and tree.get_children():
                tree.see(tree.get_children()[-1])

        def _schedule_live():
            try:
                if not tree.winfo_exists():
                    return
            except Exception:
                return
            _rf()
            _live_job[0] = self.after(2000, _schedule_live)

        _rf()
        _schedule_live()

        # ── Row detail popup (double-click) ───────────────────────
        def _show_detail(event=None):
            sel = tree.selection()
            if not sel:
                return
            vals = tree.item(sel[0], "values")
            if not vals:
                return
            ts, level, action, detail = vals
            dlg = tk.Toplevel(self)
            dlg.title(f"{action}  —  {ts}")
            dlg.configure(bg=self.C("bg"))
            dlg.resizable(True, True)
            dlg.grab_set()
            W, H = 700, 360
            x = self.winfo_rootx() + (self.winfo_width()  - W) // 2
            y = self.winfo_rooty() + (self.winfo_height() - H) // 2
            dlg.geometry(f"{W}x{H}+{x}+{y}")

            # Header
            hdr_f = tk.Frame(dlg, bg=self.C("primary"), height=48)
            hdr_f.pack(fill="x")
            hdr_f.pack_propagate(False)
            col_map = {"ERROR": "#f87171", "WARNING": "#fbbf24", "INFO": "#67e8f9"}
            tk.Label(hdr_f,
                     text=f"  {level}  ",
                     bg=col_map.get(level, self.C("primary")),
                     fg="white",
                     font=(_UI_FONT, 9, "bold")).pack(side="left", padx=(14, 0), pady=12)
            tk.Label(hdr_f, text=f"  {action}  ·  {ts}",
                     bg=self.C("primary"), fg="white",
                     font=(_UI_FONT, 10, "bold")).pack(side="left", pady=12)

            # Detail text
            body_f = tk.Frame(dlg, bg=self.C("bg"))
            body_f.pack(fill="both", expand=True, padx=16, pady=12)
            txt = tk.Text(body_f, bg=self.C("input_bg"), fg=self.C("text"),
                          font=("Consolas", 10), relief="flat",
                          wrap="word", state="normal",
                          insertbackground=self.C("text"))
            txt.insert("1.0", detail)
            txt.config(state="disabled")
            sb = ttk.Scrollbar(body_f, command=txt.yview)
            txt.configure(yscrollcommand=sb.set)
            sb.pack(side="right", fill="y")
            txt.pack(fill="both", expand=True)

            # Copy button
            btn_f = tk.Frame(dlg, bg=self.C("bg"))
            btn_f.pack(fill="x", padx=16, pady=(0, 12))
            def _copy():
                self.clipboard_clear()
                self.clipboard_append(detail)
                self._toast("Detail copied to clipboard.", "success")
            tk.Button(btn_f, text="📋  Copy Detail", bg=self.C("primary"),
                      fg="white", relief="flat", font=(_UI_FONT, 9, "bold"),
                      cursor="hand2", command=_copy).pack(side="left", ipadx=10, ipady=4)
            tk.Button(btn_f, text="Close", bg=self.C("panel"),
                      fg=self.C("muted"), relief="flat", font=(_UI_FONT, 9),
                      cursor="hand2", command=dlg.destroy).pack(
                side="right", ipadx=10, ipady=4)
            dlg.bind("<Escape>", lambda e: dlg.destroy())

        tree.bind("<Double-1>", _show_detail)

        # ── Right-click context menu ───────────────────────────────
        def _ctx_menu(event):
            item = tree.identify_row(event.y)
            if not item:
                return
            tree.selection_set(item)
            vals = tree.item(item, "values")
            ctx  = tk.Menu(self, tearoff=0,
                           bg=self.C("panel"), fg=self.C("text"),
                           activebackground=self.C("primary"),
                           activeforeground="white", relief="flat")
            ctx.add_command(label="🔍  View Full Detail",
                            command=_show_detail)
            ctx.add_separator()
            ctx.add_command(label="📋  Copy Row",
                            command=lambda: (
                                self.clipboard_clear(),
                                self.clipboard_append("\t".join(str(v) for v in vals)),
                                self._toast("Row copied.", "success")))
            ctx.add_command(label="📋  Copy Detail Only",
                            command=lambda: (
                                self.clipboard_clear(),
                                self.clipboard_append(vals[3] if len(vals) > 3 else ""),
                                self._toast("Detail copied.", "success")))
            ctx.post(event.x_root, event.y_root)

        tree.bind("<Button-3>", _ctx_menu)
        tree.bind("<Control-c>", lambda e: (
            (lambda vals=tree.item(tree.selection()[0], "values")
             if tree.selection() else None:
             (self.clipboard_clear(),
              self.clipboard_append("\t".join(str(v) for v in vals)),
              self._toast("Row copied.", "success"))
             )() if tree.selection() else None))

    def _export_logs(self):
        try:
            entries_snapshot = list(LOG.all())   # snapshot before dialog
            safe_ts  = time.strftime("%Y%m%d_%H%M%S")   # no colons
            filename = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                initialfile=f"comtrail_logs_{safe_ts}.csv",
                title="Export Logs to CSV")
            if not filename:
                return
            with open(filename, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["ts", "level", "action", "detail"])
                w.writeheader()
                w.writerows(entries_snapshot)
            LOG.log("Logs", f"Exported {len(entries_snapshot)} entries → {filename}")
            self.popup("Exported",
                       f"{len(entries_snapshot)} log entries saved to:\n{os.path.basename(filename)}",
                       "success")
        except Exception as e:
            LOG.log("Logs", f"Export error: {e}", "ERROR")
            self.popup("Error", f"Export failed:\n{e}", "error")


    # ──────────────────────────────────────────────────────────────
    # (#20) EXIT SESSION SUMMARY
    # ──────────────────────────────────────────────────────────────
    def _on_close(self):
        """Show session summary inside the main window, then close on confirm."""
        c = self._session_counts
        total = c["pcap"] + c["ludr"] + c["voice"] + c["generated"]
        if total == 0 and c["failed"] == 0:
            self.destroy()
            return

        # Replace main content with a full-page session summary
        self.clear_main()
        main = self.active_frame

        # Centred card container
        outer = tk.Frame(main, bg=self.C("bg"))
        outer.place(relx=0.5, rely=0.5, anchor="center")

        card = tk.Frame(outer,
                        highlightbackground=self.C("border"),
                        highlightthickness=1,
                        bg=self.C("panel"))
        card.pack(ipadx=0, ipady=0)

        # Top accent
        tk.Frame(card, bg=self.C("primary"), height=5).pack(fill="x")

        body = tk.Frame(card, bg=self.C("panel"))
        body.pack(padx=40, pady=28)

        # Header
        hdr_row = tk.Frame(body, bg=self.C("panel"))
        hdr_row.pack(anchor="w", pady=(0, 4))
        tk.Label(hdr_row, text="📊",
                 bg=self.C("panel"),
                 font=(_UI_FONT, 22)).pack(side="left", padx=(0, 12))
        hdr_col = tk.Frame(hdr_row, bg=self.C("panel"))
        hdr_col.pack(side="left")
        tk.Label(hdr_col, text="Session Summary",
                 bg=self.C("panel"), fg=self.C("card_title"),
                 font=(_UI_FONT, 16, "bold")).pack(anchor="w")
        tk.Label(hdr_col, text="Here's what happened this session.",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(anchor="w", pady=(2, 0))

        tk.Frame(body, bg=self.C("border"), height=1).pack(
            fill="x", pady=(14, 16))

        # Stats grid
        stats_frame = tk.Frame(body, bg=self.C("panel"))
        stats_frame.pack(anchor="w")

        for i, (label, value, color) in enumerate([
            ("PCAP uploads",     c["pcap"],      self.C("success")),
            ("LUDR / SMS uploads", c["ludr"],     self.C("success")),
            ("Voice uploads",    c["voice"],      self.C("success")),
            ("Files generated",  c["generated"],  "#67e8f9"),
            ("Failures",         c["failed"],
             "#ef4444" if c["failed"] else self.C("dim")),
        ]):
            row_bg = self.C("input_bg") if i % 2 == 0 else self.C("panel")
            row = tk.Frame(stats_frame, bg=row_bg)
            row.pack(fill="x", pady=1)
            tk.Label(row, text=label,
                     bg=row_bg, fg=self.C("muted"),
                     font=(_UI_FONT, 10),
                     width=22, anchor="w",
                     padx=10, pady=6).pack(side="left")
            tk.Label(row, text=str(value),
                     bg=row_bg, fg=color,
                     font=(_UI_FONT, 13, "bold"),
                     width=6, anchor="e",
                     padx=10).pack(side="right")

        tk.Frame(body, bg=self.C("border"), height=1).pack(
            fill="x", pady=(16, 0))

        # Close button — clicking it exits the app
        tk.Button(body,
                  text="  Close Application  ",
                  bg="#ef4444", fg="white",
                  relief="flat",
                  font=(_UI_FONT, 11, "bold"),
                  activebackground="#dc2626",
                  cursor="hand2",
                  command=self.destroy).pack(pady=(16, 0), ipady=8)

    # ──────────────────────────────────────────────────────────────
    # (#17) KEYBOARD SHORTCUTS OVERLAY
    # ──────────────────────────────────────────────────────────────
    def _show_shortcuts_overlay(self):
        dlg = tk.Toplevel(self)
        dlg.title("Keyboard Shortcuts")
        dlg.configure(bg=self.C("bg"))
        dlg.resizable(False, False)
        dlg.grab_set()
        x = self.winfo_rootx() + self.winfo_width()  // 2 - 220
        y = self.winfo_rooty() + self.winfo_height() // 2 - 200
        dlg.geometry(f"440x400+{x}+{y}")
        tk.Frame(dlg, bg=self.C("primary"), height=4).pack(fill="x")
        hdr = tk.Frame(dlg, bg=self.C("bg"))
        hdr.pack(fill="x", padx=20, pady=(14, 8))
        tk.Label(hdr, text="⌨️  Keyboard Shortcuts",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 13, "bold")).pack(side="left")
        tk.Button(hdr, text="✕", bg=self.C("bg"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 11), cursor="hand2",
                  command=dlg.destroy).pack(side="right")
        tk.Frame(dlg, bg=self.C("border"), height=1).pack(fill="x", padx=20)
        body = tk.Frame(dlg, bg=self.C("bg"))
        body.pack(fill="both", expand=True, padx=20, pady=12)
        shortcuts = [
            ("Global", [
                ("?",          "Show this shortcuts overlay"),
                ("Escape",     "Close dropdown / deselect"),
                ("Alt + 1",    "Go to Home"),
                ("Alt + 2",    "Go to Sample Data"),
                ("Alt + 3",    "Go to Settings"),
                ("Alt + 4",    "Go to Validation"),
                ("Alt + 5",    "Go to Help"),
            ]),
            ("Sample Data", [
                ("Enter",      "Download selected file or folder"),
                ("Double-click","Download selected file or folder"),
                ("Right-click","Context menu (download, copy path, properties)"),
                ("Arrow keys", "Navigate file list"),
            ]),
            ("Logs", [
                ("Type in search", "Filter log entries by text"),
                ("Follow ON/OFF",  "Toggle auto-scroll to latest entry"),
            ]),
            ("Generators", [
                ("Drag file → input", "Drop a CSV or folder onto the field"),
                ("Ctrl + Enter",      "Trigger Generate on SMS / Voice generator pages"),
            ]),
        ]
        for section, items in shortcuts:
            tk.Label(body, text=section,
                     bg=self.C("bg"), fg=self.C("primary"),
                     font=(_UI_FONT, 9, "bold")).pack(anchor="w", pady=(8, 3))
            for key, desc in items:
                row = tk.Frame(body, bg=self.C("bg"))
                row.pack(fill="x", pady=1)
                tk.Label(row, text=key,
                         bg=self.C("input_bg"), fg=self.C("card_title"),
                         font=(_UI_FONT, 8, "bold"),
                         padx=8, pady=3, relief="flat",
                         width=20, anchor="w").pack(side="left")
                tk.Label(row, text=desc,
                         bg=self.C("bg"), fg=self.C("muted"),
                         font=(_UI_FONT, 8),
                         anchor="w").pack(side="left", padx=(10, 0))
        dlg.bind("<Escape>", lambda e: dlg.destroy())

    # ──────────────────────────────────────────────────────────────
    # (#19) NOTIFICATION HISTORY
    # ──────────────────────────────────────────────────────────────
    def _add_notification(self, msg, kind="info"):
        ts = time.strftime("%H:%M:%S")
        self._notif_history.insert(0, (ts, msg, kind))
        if len(self._notif_history) > 100:
            self._notif_history = self._notif_history[:100]
        self._notif_unread[0] += 1
        try:
            if self._update_notif_badge:
                self._update_notif_badge()
        except Exception:
            pass

    def _show_notif_panel(self):
        """Render notification history as an in-app page (no popup window)."""
        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Notification History")

        # ── Header ─────────────────────────────────────────────────
        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(25, 5))

        tk.Label(hdr, text="🔔  Notification History",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")

        def _clear_and_go_home():
            self._notif_history.clear()
            self._notif_unread[0] = 0
            try:
                if self._update_notif_badge:
                    self._update_notif_badge()
            except Exception:
                pass
            self.show_first_page()

        tk.Button(hdr, text="🗑️  Clear All",
                  bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9, "bold"),
                  cursor="hand2",
                  command=_clear_and_go_home).pack(side="right", ipady=4, ipadx=8)

        tk.Button(hdr, text="← Back",
                  bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 9),
                  cursor="hand2",
                  command=self.show_first_page).pack(
            side="right", padx=(0, 8), ipady=4, ipadx=6)

        tk.Frame(main, bg=self.C("border"), height=1).pack(
            fill="x", padx=50, pady=(5, 10))

        # ── Empty state ─────────────────────────────────────────────
        if not self._notif_history:
            empty = tk.Frame(main, bg=self.C("bg"))
            empty.pack(expand=True)
            tk.Label(empty, text="🔕",
                     bg=self.C("bg"),
                     font=(_UI_FONT, 36)).pack(pady=(60, 10))
            tk.Label(empty, text="No notifications yet.",
                     bg=self.C("bg"), fg=self.C("muted"),
                     font=(_UI_FONT, 12)).pack()
            tk.Label(empty,
                     text="Notifications from uploads, downloads, and errors appear here.",
                     bg=self.C("bg"), fg=self.C("dim"),
                     font=(_UI_FONT, 9)).pack(pady=(4, 0))
            return

        # ── Scrollable list ─────────────────────────────────────────
        _kind_colors = {
            "success": "#22c55e", "info": "#67e8f9",
            "warning": "#f59e0b", "error": "#ef4444",
        }
        _kind_icons = {
            "success": "✅", "info": "ℹ️",
            "warning": "⚠️", "error": "❌",
        }

        canvas, sf = self._scrollable(main)
        container = tk.Frame(sf, bg=self.C("bg"))
        container.pack(fill="x", padx=50, pady=5)

        for ts, msg, kind in self._notif_history:
            row = tk.Frame(container,
                           highlightbackground=self.C("border"),
                           highlightthickness=1,
                           bg=self.C("panel"))
            row.pack(fill="x", pady=3)

            # Left colour bar
            tk.Frame(row,
                     bg=_kind_colors.get(kind, "#67e8f9"),
                     width=5).pack(side="left", fill="y")

            # Icon
            tk.Label(row,
                     text=_kind_icons.get(kind, "•"),
                     bg=self.C("panel"),
                     font=(_UI_FONT, 13),
                     padx=12).pack(side="left")

            # Message + timestamp
            txt_col = tk.Frame(row, bg=self.C("panel"))
            txt_col.pack(side="left", fill="x", expand=True, pady=10)
            tk.Label(txt_col, text=msg,
                     bg=self.C("panel"), fg=self.C("card_title"),
                     font=(_UI_FONT, 9),
                     wraplength=680, justify="left",
                     anchor="w").pack(anchor="w")

            # Timestamp right-aligned
            tk.Label(row, text=ts,
                     bg=self.C("panel"), fg=self.C("dim"),
                     font=(_UI_FONT, 8),
                     padx=14).pack(side="right")

    # ──────────────────────────────────────────────────────────────
    # (#8) CSV PREVIEW HELPER
    # ──────────────────────────────────────────────────────────────
    def _build_csv_preview(self, parent, path, max_rows=5):
        for w in parent.winfo_children():
            w.destroy()
        if not path or not os.path.isfile(path):
            return
        try:
            with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
                reader = csv.DictReader(f)
                cols   = reader.fieldnames or []
                rows   = [row for row, _ in zip(reader, range(max_rows))]
        except Exception as e:
            tk.Label(parent, text=f"⚠️  Cannot read CSV: {e}",
                     bg=self.C("panel"), fg="#f59e0b",
                     font=(_UI_FONT, 8)).pack(anchor="w", padx=4, pady=2)
            return
        if not rows:
            tk.Label(parent, text="CSV is empty.",
                     bg=self.C("panel"), fg=self.C("dim"),
                     font=(_UI_FONT, 8)).pack(anchor="w", padx=4, pady=2)
            return

        lbl = tk.Label(parent,
                       text=f"📋  Preview — {len(cols)} columns"
                            f"  ·  showing first {len(rows)} row(s)",
                       bg=self.C("panel"), fg=self.C("muted"),
                       font=(_UI_FONT, 7, "italic"))
        lbl.pack(anchor="w", padx=4, pady=(4, 2))

        tbl = tk.Frame(parent,
                       highlightbackground=self.C("border"),
                       highlightthickness=1,
                       bg=self.C("input_bg"))
        tbl.pack(fill="x", padx=4, pady=(0, 4))

        col_w = max(80, min(160, 700 // max(len(cols), 1)))
        for ci, col in enumerate(cols):
            tk.Label(tbl, text=col,
                     bg=self.C("primary"), fg="white",
                     font=(_UI_FONT, 7, "bold"),
                     width=col_w // 7, anchor="w",
                     padx=4, pady=3,
                     relief="flat").grid(row=0, column=ci, sticky="ew", padx=1)
        for ri, row in enumerate(rows):
            rbg = self.C("input_bg") if ri % 2 == 0 else self.C("panel")
            for ci, col in enumerate(cols):
                val = str(row.get(col, ""))[:30]
                tk.Label(tbl, text=val,
                         bg=rbg, fg=self.C("text"),
                         font=(_UI_FONT, 7),
                         width=col_w // 7, anchor="w",
                         padx=4, pady=2).grid(row=ri+1, column=ci,
                                              sticky="ew", padx=1, pady=1)

    # ──────────────────────────────────────────────────────────────
    # (#10) PROFILE SAVE / LOAD BAR
    # ──────────────────────────────────────────────────────────────
    def _build_profile_bar(self, parent, profile_key, collect_fn, apply_fn):
        """Adds a Save Profile / Load Profile row to a generator card."""
        import json as _json
        PROFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "profiles")
        os.makedirs(PROFILE_DIR, exist_ok=True)
        profile_file = os.path.join(PROFILE_DIR, f"{profile_key}.json")

        bar = tk.Frame(parent, bg=self.C("panel"))
        bar.pack(fill="x", padx=8, pady=(6, 2))
        tk.Label(bar, text="Profile:",
                 bg=self.C("panel"), fg=self.C("dim"),
                 font=(_UI_FONT, 8)).pack(side="left", padx=(0, 6))

        def _save():
            try:
                data = collect_fn()
                with open(profile_file, "w", encoding="utf-8") as f:
                    _json.dump(data, f, indent=2)
                LOG.log("Profile", f"Saved {profile_key} profile")
                self._toast("Profile saved.", "success", 2000)
            except Exception as e:
                self._toast(f"Save failed: {e}", "error")

        def _load():
            if not os.path.isfile(profile_file):
                self._toast("No saved profile found.", "info", 2000)
                return
            try:
                with open(profile_file, "r", encoding="utf-8") as f:
                    data = _json.load(f)
                apply_fn(data)
                LOG.log("Profile", f"Loaded {profile_key} profile")
                self._toast("Profile loaded.", "success", 2000)
            except Exception as e:
                self._toast(f"Load failed: {e}", "error")

        has_profile = os.path.isfile(profile_file)
        tk.Button(bar, text="💾  Save Profile",
                  bg=self.C("input_bg"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 8), cursor="hand2",
                  command=_save).pack(side="left", padx=(0, 4), ipadx=4, ipady=2)
        load_btn = tk.Button(bar, text="📂  Load Profile",
                             bg=self.C("primary") if has_profile
                             else self.C("input_bg"),
                             fg="white" if has_profile else self.C("dim"),
                             relief="flat", font=(_UI_FONT, 8),
                             cursor="hand2", command=_load)
        load_btn.pack(side="left", ipadx=4, ipady=2)
        if has_profile:
            mtime = os.path.getmtime(profile_file)
            ts = time.strftime("%d/%m %H:%M", time.localtime(mtime))
            tk.Label(bar, text=f"  saved {ts}",
                     bg=self.C("panel"), fg=self.C("dim"),
                     font=(_UI_FONT, 7)).pack(side="left")

    # ──────────────────────────────────────────────────────────────
    # (#4) DRAG-AND-DROP HELPER
    # ──────────────────────────────────────────────────────────────
    def _enable_drop(self, widget, var, callback=None):
        """Register drag-and-drop on a tk.Entry using tkinterdnd2 if available."""
        try:
            from tkinterdnd2 import DND_FILES
            widget.drop_target_register(DND_FILES)
            def _on_drop(event):
                path = event.data.strip().strip("{}")
                var.set(path)
                if callback:
                    callback()
            widget.dnd_bind("<<Drop>>", _on_drop)
            widget.config(highlightbackground=self.C("primary"),
                          highlightthickness=1)
        except Exception:
            pass   # tkinterdnd2 not installed — browse button still works

    # ──────────────────────────────────────────────────────────────
    # (#6) PRE-UPLOAD VALIDATION HELPER
    # ──────────────────────────────────────────────────────────────
    def _validate_upload(self, paths, label="Upload"):
        """
        Show a pre-upload summary dialog with file count, total size,
        and any problems detected. Returns True to proceed, False to cancel.
        """
        if not paths:
            self.popup("Nothing to Upload",
                       "No files or folders selected.", "error")
            return False
        import stat as _st
        total_size  = 0
        file_count  = 0
        problems    = []
        for p in paths:
            if not os.path.exists(p):
                problems.append(f"Not found: {os.path.basename(p)}")
                continue
            if os.path.isfile(p):
                sz = os.path.getsize(p)
                total_size += sz
                file_count += 1
                if sz == 0:
                    problems.append(f"Empty file: {os.path.basename(p)}")
            elif os.path.isdir(p):
                for root, _, files in os.walk(p):
                    for fn in files:
                        fpath = os.path.join(root, fn)
                        try:
                            sz = os.path.getsize(fpath)
                            total_size += sz
                            file_count += 1
                        except Exception:
                            pass

        size_mb = total_size / (1024 * 1024)
        size_str = (f"{size_mb:.1f} MB" if size_mb >= 1
                    else f"{total_size // 1024} KB")

        dlg = tk.Toplevel(self)
        dlg.title(f"Confirm {label}")
        dlg.configure(bg=self.C("bg"))
        dlg.resizable(False, False)
        dlg.grab_set()
        x = self.winfo_rootx() + self.winfo_width()  // 2 - 210
        y = self.winfo_rooty() + self.winfo_height() // 2 - 120
        dlg.geometry(f"420x240+{x}+{y}")
        tk.Frame(dlg, bg=self.C("primary"), height=4).pack(fill="x")
        body = tk.Frame(dlg, bg=self.C("bg"))
        body.pack(fill="both", expand=True, padx=20, pady=14)
        tk.Label(body, text=f"📤  {label} Summary",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 12, "bold")).pack(anchor="w")
        tk.Frame(body, bg=self.C("border"), height=1).pack(
            fill="x", pady=(6, 8))
        for lbl, val in [("Files to upload:", str(file_count)),
                          ("Total size:",      size_str),
                          ("Destinations:",    str(len(paths)))]:
            row = tk.Frame(body, bg=self.C("bg"))
            row.pack(fill="x", pady=1)
            tk.Label(row, text=lbl, width=18, anchor="w",
                     bg=self.C("bg"), fg=self.C("muted"),
                     font=(_UI_FONT, 9)).pack(side="left")
            tk.Label(row, text=val,
                     bg=self.C("bg"), fg=self.C("card_title"),
                     font=(_UI_FONT, 9, "bold")).pack(side="left")
        if problems:
            tk.Label(body,
                     text="⚠️  " + " · ".join(problems[:3]),
                     bg=self.C("bg"), fg="#f59e0b",
                     font=(_UI_FONT, 8),
                     wraplength=370).pack(anchor="w", pady=(6, 0))
        result = [False]
        btn_row = tk.Frame(body, bg=self.C("bg"))
        btn_row.pack(anchor="e", pady=(12, 0))
        tk.Button(btn_row, text="Upload Now",
                  bg=self.C("success"), fg="white", relief="flat",
                  font=(_UI_FONT, 10, "bold"), cursor="hand2", padx=14,
                  command=lambda: (result.__setitem__(0, True),
                                   dlg.destroy())).pack(
            side="right", ipady=5)
        tk.Button(btn_row, text="Cancel",
                  bg=self.C("input_bg"), fg=self.C("text"), relief="flat",
                  font=(_UI_FONT, 10), cursor="hand2", padx=10,
                  command=dlg.destroy).pack(
            side="right", padx=(0, 8), ipady=5)
        dlg.wait_window()
        return result[0]

    # ──────────────────────────────────────────────────────────────
    # (#5) UPLOAD QUEUE WINDOW
    # ──────────────────────────────────────────────────────────────
    def _upload_queue_window(self, title, items):
        """
        Show a scrollable upload queue with per-item status rows.
        items: list of strings (file/folder names).
        Returns a dict with:
          set_status(idx, status)  — update one row  ("pending"/"ok"/"error"/text)
          destroy()                — close the window
        """
        dlg = tk.Toplevel(self)
        dlg.title(title)
        dlg.configure(bg=self.C("bg"))
        dlg.resizable(True, True)
        x = self.winfo_rootx() + self.winfo_width()  // 2 - 240
        y = self.winfo_rooty() + self.winfo_height() // 2 - 180
        dlg.geometry(f"480x360+{x}+{y}")
        tk.Frame(dlg, bg=self.C("primary"), height=4).pack(fill="x")
        hdr = tk.Frame(dlg, bg=self.C("bg"))
        hdr.pack(fill="x", padx=16, pady=(10, 4))
        tk.Label(hdr, text=f"📤  {title}",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 11, "bold")).pack(side="left")
        progress_lbl = tk.Label(hdr, text="",
                                bg=self.C("bg"), fg=self.C("muted"),
                                font=(_UI_FONT, 8))
        progress_lbl.pack(side="right")
        tk.Frame(dlg, bg=self.C("border"), height=1).pack(fill="x", padx=16)

        canvas, sf = self._scrollable(dlg)
        status_labels = {}
        icon_labels   = {}
        done_count    = [0]

        for i, name in enumerate(items):
            row = tk.Frame(sf, bg=self.C("panel"),
                           highlightbackground=self.C("border"),
                           highlightthickness=1)
            row.pack(fill="x", padx=10, pady=2)
            icon_lbl = tk.Label(row, text="⏳",
                                bg=self.C("panel"),
                                font=(_UI_FONT, 10))
            icon_lbl.pack(side="left", padx=(8, 6), pady=6)
            tk.Label(row, text=os.path.basename(name),
                     bg=self.C("panel"), fg=self.C("card_title"),
                     font=(_UI_FONT, 9),
                     anchor="w").pack(side="left", fill="x", expand=True)
            st_lbl = tk.Label(row, text="Waiting…",
                              bg=self.C("panel"), fg=self.C("dim"),
                              font=(_UI_FONT, 8))
            st_lbl.pack(side="right", padx=10)
            status_labels[i] = st_lbl
            icon_labels[i]   = icon_lbl

        def _set_status(idx, status):
            if idx not in status_labels:
                return
            color_map = {
                "ok":      (self.C("success"), "✅"),
                "error":   ("#ef4444",         "❌"),
                "working": ("#f59e0b",          "🔄"),
                "pending": (self.C("dim"),      "⏳"),
            }
            key = status.lower() if status.lower() in color_map else None
            fg, icon = color_map.get(key, (self.C("text"), "•"))
            try:
                status_labels[idx].config(text=status, fg=fg)
                icon_labels[idx].config(text=icon)
                if status.lower() in ("ok", "error"):
                    done_count[0] += 1
                    progress_lbl.config(
                        text=f"{done_count[0]} / {len(items)} done")
            except Exception:
                pass

        close_btn = tk.Button(dlg, text="Close",
                              bg=self.C("input_bg"), fg=self.C("text"),
                              relief="flat", font=(_UI_FONT, 9),
                              cursor="hand2", command=dlg.destroy)
        close_btn.pack(pady=8)

        return {"set_status": _set_status, "destroy": dlg.destroy,
                "window": dlg}

    # ──────────────────────────────────────────────────────────────
    # SESSION HISTORY HELPER
    # ──────────────────────────────────────────────────────────────
    def _record(self, action, files, server, status):
        import time as _t
        self._session_history.insert(0, {
            "ts":     _t.strftime("%d/%m/%Y %H:%M:%S"),
            "action": action,
            "files":  files,
            "server": server,
            "status": status,
        })
        if len(self._session_history) > 200:
            self._session_history = self._session_history[:200]
        # Increment the right counter by actual file count
        if "✅" in status:
            n = int(files) if str(files).isdigit() else 1
            n = max(1, n)
            if "PCAP" in action and "Generate" not in action:
                self._session_counts["pcap"] += n
            elif ("LBS" in action or "LUDR" in action or "SMS" in action) \
                    and "Generate" not in action:
                self._session_counts["ludr"] += n
            elif "Voice" in action and "Generate" not in action:
                self._session_counts["voice"] += n
            elif "Generate" in action:
                self._session_counts["generated"] += n
        elif "❌" in status:
            self._session_counts["failed"] += 1

    def _write_history(self, action, files, server, status, note=""):
        import time as _t, os as _os, csv as _csv
        # Update in-memory state and tkinter widgets on main thread
        def _main_thread_update():
            if "✅" in status and "Generate" not in action:
                ts_str = _t.strftime("%H:%M:%S")
                if "PCAP" in action:
                    self._last_upload["pcap"] = (ts_str, str(files), note)
                elif "LBS" in action or "LUDR" in action or "SMS" in action:
                    self._last_upload["ludr"] = (ts_str, str(files), note)
                elif "Voice" in action:
                    self._last_upload["voice"] = (ts_str, str(files), note)
            self._record(action, files, server, status)
            self._refresh_home_stats()
        try:
            self.after(0, _main_thread_update)
        except Exception:
            pass
        # Serialize CSV writes so concurrent jobs don't interleave
        HIST_FILE = _os.path.join(
            _os.path.dirname(_os.path.abspath(__file__)),
            "comtrail_upload_history.csv")
        row = {"ts": _t.strftime("%d/%m/%Y %H:%M:%S"), "action": action,
               "files": files, "server": server, "status": status, "note": note}
        with getattr(self, "_history_lock", threading.Lock()):
            write_header = not _os.path.isfile(HIST_FILE)
            try:
                with open(HIST_FILE, "a", newline="", encoding="utf-8") as f:
                    w = _csv.DictWriter(f, fieldnames=["ts","action","files","server","status","note"])
                    if write_header: w.writeheader()
                    w.writerow(row)
            except Exception as e:
                LOG.log("History", f"Write error: {e}", "ERROR")

    def _ask_confirm(self, title, message):
        result = [False]
        dlg = tk.Toplevel(self)
        dlg.title(title)
        dlg.configure(bg=self.C("panel"))
        dlg.resizable(False, False)
        dlg.grab_set()
        tk.Label(dlg, text=message, bg=self.C("panel"), fg=self.C("card_title"),
                 font=(_UI_FONT, 10), wraplength=340,
                 justify="center").pack(padx=30, pady=(20, 10))
        br = tk.Frame(dlg, bg=self.C("panel"))
        br.pack(pady=(0, 18))
        def _yes(): result[0] = True; dlg.destroy()
        tk.Button(br, text="Yes, Delete", bg=self.C("error"), fg="white",
                  relief="flat", font=(_UI_FONT, 10, "bold"),
                  command=_yes).pack(side="left", padx=8, ipady=5, ipadx=10)
        tk.Button(br, text="Cancel", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 10),
                  highlightbackground=self.C("border"), highlightthickness=1,
                  command=dlg.destroy).pack(side="left", padx=8, ipady=5, ipadx=10)
        dlg.wait_window()
        return result[0]

    def _sftp_put_with_retry(self, sftp, local, remote, retries=3, delay=2, ui_fn=None):
        last = None
        for attempt in range(1, retries + 1):
            try:
                sftp.put(local, remote); return True
            except Exception as e:
                last = e
                LOG.log("Upload", f"  ⚠ Attempt {attempt}/{retries}: {e}", "WARNING")
                if ui_fn: ui_fn(f"Retry {attempt}/{retries}: {os.path.basename(local)}")
                if attempt < retries: time.sleep(delay)
        raise RuntimeError(f"Failed after {retries} attempts: {os.path.basename(local)}\nLast: {last}")

    # ──────────────────────────────────────────────────────────────
    # DASHBOARD
    # ──────────────────────────────────────────────────────────────
    def show_upload_history(self):
        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Upload History")
        HIST_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "comtrail_upload_history.csv")
        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(22,5))
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT,9),
                  command=self.show_first_page).pack(side="left")
        tk.Label(hdr, text="📋  Upload & Generate History",
                 bg=self.C("bg"),
                 fg=self.C("card_title"),
                 font=(_UI_FONT,20,"bold")).pack(side="left", padx=15)
        tk.Frame(main, bg=self.C("border"), height=1).pack(
            fill="x", padx=50, pady=(5,18))

        canvas, sf = self._scrollable(main)
        body = tk.Frame(sf, bg=self.C("bg"))
        body.pack(fill="x", padx=50, pady=5)

        filter_var = tk.StringVar(value="ALL")
        search_var = tk.StringVar()
        toolbar = tk.Frame(body, bg=self.C("bg"))
        toolbar.pack(fill="x", pady=(0,12))
        tk.Label(toolbar, text="Filter:", bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT,9)).pack(side="left", padx=(0,4))
        ttk.Combobox(toolbar, textvariable=filter_var,
                     values=["ALL","PCAP Upload","LBS Upload","Voice Upload",
                             "✅ OK","❌ Failed"],
                     state="readonly", width=16).pack(side="left", ipady=4, padx=(0,12))
        tk.Label(toolbar, text="Search:", bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT,9)).pack(side="left", padx=(0,4))
        tk.Entry(toolbar, textvariable=search_var, bg=self.C("input_bg"),
                 fg=self.C("text"), insertbackground=self.C("text"),
                 relief="flat", width=28).pack(side="left", ipady=5)

        hist_outer = tk.Frame(body, highlightbackground=self.C("border"),
                              highlightthickness=1, bg=self.C("panel"))
        hist_outer.pack(fill="both", expand=True)
        style = ttk.Style()
        style.configure("Hist.Treeview", background=self.C("input_bg"),
                        foreground=self.C("text"), rowheight=24,
                        fieldbackground=self.C("input_bg"), borderwidth=0)
        style.configure("Hist.Treeview.Heading", background=self.C("primary"),
                        foreground="white", font=(_UI_FONT,9,"bold"), relief="flat")
        style.map("Hist.Treeview", background=[("selected",self.C("primary"))],
                  foreground=[("selected","#ffffff")])
        hf = tk.Frame(hist_outer, bg=self.C("panel"))
        hf.pack(fill="both", expand=True, padx=10, pady=8)
        h_cols = ("Date/Time","Action","Files","Server","Status","Note")
        h_tree = ttk.Treeview(hf, columns=h_cols, show="headings",
                              height=18, style="Hist.Treeview")
        for c, w in zip(h_cols, (140,130,55,150,90,280)):
            h_tree.heading(c, text=c); h_tree.column(c, width=w, anchor="w")
        vsb = ttk.Scrollbar(hf, orient="vertical", command=h_tree.yview)
        h_tree.configure(yscrollcommand=vsb.set)
        h_tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")
        all_rows = []
        status_lbl = tk.Label(body, text="", bg=self.C("bg"), fg=self.C("dim"),
                              font=(_UI_FONT,8))
        status_lbl.pack(anchor="w", pady=(6,0))

        def load():
            nonlocal all_rows; all_rows = []
            if os.path.isfile(HIST_FILE):
                try:
                    with open(HIST_FILE,"r",encoding="utf-8-sig",newline="") as f:
                        all_rows = list(csv.DictReader(f))
                    all_rows.reverse()
                except Exception as e:
                    LOG.log("History", f"Load error: {e}", "ERROR")
            render()

        def render():
            h_tree.delete(*h_tree.get_children())
            flt = filter_var.get(); srch = search_var.get().lower()
            shown = 0
            for row in all_rows:
                if flt != "ALL":
                    if flt in ("✅ OK","❌ Failed"):
                        if flt not in row.get("status",""): continue
                    elif row.get("action","") != flt: continue
                if srch and srch not in str(row).lower(): continue
                h_tree.insert("","end", values=(
                    row.get("ts",""), row.get("action",""), row.get("files",""),
                    row.get("server",""), row.get("status",""), row.get("note","")))
                shown += 1
            status_lbl.config(text=f"{shown} shown / {len(all_rows)} total")

        filter_var.trace_add("write", lambda *_: render())
        search_var.trace_add("write", lambda *_: render())

        btn_row = tk.Frame(body, bg=self.C("bg"))
        btn_row.pack(anchor="w", pady=(10,0))
        def exp():
            if not all_rows: return self.popup("Info","No history.","info")
            fname = filedialog.asksaveasfilename(defaultextension=".csv",
                       filetypes=[("CSV","*.csv")],
                       initialfile="upload_history.csv")
            if not fname: return
            with open(fname,"w",newline="",encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=["ts","action","files","server","status","note"])
                w.writeheader(); w.writerows(all_rows)
            self.popup("Exported",f"{len(all_rows)} records saved.","success")
        def clr():
            if not os.path.isfile(HIST_FILE): return
            if self._ask_confirm("Clear History","Delete all upload history?"):
                os.remove(HIST_FILE); load()
                self.popup("Cleared","History cleared.","success")
        for txt, cmd, bg in [
            ("🔄  Refresh", load, self.C("success")),
            ("📥  Export CSV", exp, self.C("primary")),
            ("🗑️  Clear History", clr, self.C("error")),
        ]:
            tk.Button(btn_row, text=txt, command=cmd, bg=bg, fg="white",
                      relief="flat", font=(_UI_FONT,10,"bold"),
                      activebackground=self.C("border")).pack(
                          side="left", padx=(0,8), ipady=6, ipadx=8)
        load()

    # ──────────────────────────────────────────────────────────────
    # CONNECTION TEST
    # ──────────────────────────────────────────────────────────────
    def show_connection_test(self):
        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Connection Test")
        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(22,5))
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT,9),
                  command=self.show_first_page).pack(side="left")
        tk.Label(hdr, text="🧪  Connection Test", bg=self.C("bg"),
                 fg=self.C("card_title"),
                 font=(_UI_FONT,20,"bold")).pack(side="left", padx=15)
        tk.Frame(main, bg=self.C("border"), height=1).pack(
            fill="x", padx=50, pady=(5,15))
        canvas, sf = self._scrollable(main)
        body = tk.Frame(sf, bg=self.C("bg"))
        body.pack(fill="x", padx=50, pady=5)
        self._section_label(body, "Tests SSH connectivity and verifies remote paths are writable.", pady=(0,12))

        CHECKS = {
            "pcap":  {"label":"PCAP Server","icon":"📡",
                      "paths":[self.cfg.get("pcap",{}).get("path","/data5/prism/Paths/InputDir1/pcap")]},
            "ludr":  {"label":"LBS / LUDR / SMS","icon":"📍",
                      "paths":[self.cfg.get("ludr",{}).get("path","/data5/prism/Paths/InputDir1/ludr")]},
            "voice": {"label":"Voice Server","icon":"📞",
                      "paths":["/data5/prism/Paths/InputDir1/WatchDir","/etc/vsf/input"]},
        }
        results = {}; cards = {}

        for key, info in CHECKS.items():
            outer = tk.Frame(body, highlightbackground=self.C("border"),
                             highlightthickness=1, bg=self.C("panel"))
            outer.pack(fill="x", pady=6)
            tk.Frame(outer, bg=self.C("primary"), height=4).pack(fill="x")
            inner = tk.Frame(outer, bg=self.C("panel"))
            inner.pack(fill="x", padx=18, pady=12)
            tr = tk.Frame(inner, bg=self.C("panel")); tr.pack(fill="x")
            tk.Label(tr, text=f"{info['icon']}  {info['label']}",
                     bg=self.C("panel"), fg=self.C("card_title"),
                     font=(_UI_FONT,12,"bold")).pack(side="left")
            sl = tk.Label(tr, text="⏳ Pending", bg=self.C("panel"),
                          fg=self.C("muted"), font=(_UI_FONT,10,"bold"))
            sl.pack(side="right")
            df = tk.Frame(inner, bg=self.C("panel")); df.pack(fill="x", pady=(8,0))
            cards[key] = {"status": sl, "detail": df}

        def run_tests():
            for key, info in CHECKS.items():
                cfg_k = self.cfg.get(key,{})
                ip = cfg_k.get("ip",""); pwd = cfg_k.get("pwd","")
                sl = cards[key]["status"]; df = cards[key]["detail"]
                if not ip or not pwd:
                    results[key] = {"ok":False,"msg":"Not configured","paths":[]}
                    def _upd(k=key):
                        r = results[k]
                        cards[k]["status"].config(text="❌ Not configured", fg=self.C("error"))
                        for w in cards[k]["detail"].winfo_children(): w.destroy()
                        tk.Label(cards[k]["detail"], text="→ Go to Settings to configure this server.",
                                 bg=self.C("panel"), fg=self.C("muted"),
                                 font=(_UI_FONT,9)).pack(anchor="w")
                    self.after(0, _upd); continue
                sl.config(text="🔄 Testing…", fg=self.C("warn"))
                def _test(k=key, i=ip, p=pwd, paths=info["paths"]):
                    try:
                        ssh = paramiko.SSHClient()
                        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                        ssh.connect(i, username="root", password=p, timeout=15)
                        sftp = ssh.open_sftp()
                        pr = []; all_ok = True
                        for path in paths:
                            try:
                                sftp.stat(path)
                                tf = f"{path}/.comtrail_test"
                                sftp.open(tf,"w").close(); sftp.remove(tf)
                                pr.append((path, True, ""))
                            except Exception as pe:
                                pr.append((path, False, str(pe))); all_ok = False
                        sftp.close(); ssh.close()
                        results[k] = {"ok":all_ok, "msg":f"SSH OK · {i}", "paths":pr}
                        LOG.log("ConnTest", f"{k}: {'✓' if all_ok else '✗'}")
                    except Exception as e:
                        results[k] = {"ok":False, "msg":f"SSH failed: {e}", "paths":[]}
                        LOG.log("ConnTest", f"{k}: FAILED {e}", "ERROR")
                    def _upd(k2=k):
                        r = results[k2]; c = cards[k2]
                        ok = r["ok"]
                        c["status"].config(text="✅ All Good" if ok else "❌ Failed",
                                           fg=self.C("success") if ok else self.C("error"))
                        for w in c["detail"].winfo_children(): w.destroy()
                        tk.Label(c["detail"], text=f"SSH: {r['msg']}",
                                 bg=self.C("panel"), fg=self.C("subtle"),
                                 font=(_UI_FONT,9)).pack(anchor="w")
                        for pth, pok, perr in r.get("paths",[]):
                            clr = self.C("success") if pok else self.C("error")
                            txt = ("✓  " if pok else "✗  ") + pth + ("" if pok else f"  ({perr})")
                            tk.Label(c["detail"], text=txt, bg=self.C("panel"), fg=clr,
                                     font=(_UI_FONT,9)).pack(anchor="w")
                    self.after(0, _upd)
                threading.Thread(target=_test, daemon=True).start()

        tk.Button(body, text="🧪  Run All Connection Tests",
                  bg=self.C("primary"), fg="white", relief="flat",
                  font=(_UI_FONT,11,"bold"),
                  activebackground=self.C("border"),
                  command=run_tests).pack(anchor="w", pady=(0,16), ipady=8, ipadx=16)

    # ──────────────────────────────────────────────────────────────
    # BULK VOICE UPLOAD
    # ──────────────────────────────────────────────────────────────
    # ──────────────────────────────────────────────────────────────
    # REMOTE FILE BROWSER
    # ──────────────────────────────────────────────────────────────
    def show_remote_browser(self):
        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation","Opened Remote File Browser")
        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(22,5))
        tk.Button(hdr, text="← Back", bg=self.C("panel"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT,9),
                  command=self.show_first_page).pack(side="left")
        tk.Label(hdr, text="🔍  Remote File Browser", bg=self.C("bg"),
                 fg=self.C("card_title"),
                 font=(_UI_FONT,20,"bold")).pack(side="left", padx=15)
        tk.Frame(main, bg=self.C("border"), height=1).pack(fill="x", padx=50, pady=(5,15))

        cc = self._card(main); cc.pack_configure(padx=50, pady=(0,8))
        cb = tk.Frame(cc, bg=self.C("panel")); cb.pack(fill="x", padx=14, pady=10)
        r1 = tk.Frame(cb, bg=self.C("panel")); r1.pack(fill="x", pady=4)
        server_var = tk.StringVar(value="PCAP")
        path_var   = tk.StringVar(value="/data5/prism")
        tk.Label(r1, text="Server:", bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT,9)).pack(side="left", padx=(0,6))
        ttk.Combobox(r1, textvariable=server_var,
                     values=["PCAP","LBS / LUDR / SMS","Voice"],
                     state="readonly", width=18).pack(side="left", ipady=4, padx=(0,20))
        tk.Label(r1, text="Path:", bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT,9)).pack(side="left", padx=(0,6))
        path_entry = tk.Entry(r1, textvariable=path_var, bg=self.C("input_bg"),
                              fg=self.C("text"), insertbackground=self.C("text"),
                              relief="flat", width=55)
        path_entry.pack(side="left", ipady=5, padx=(0,8))
        status_var = tk.StringVar(value="")
        tk.Label(cc, textvariable=status_var, bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT,8)).pack(anchor="w", padx=14, pady=(0,4))

        lo = tk.Frame(main, highlightbackground=self.C("border"),
                      highlightthickness=1, bg=self.C("panel"))
        lo.pack(fill="both", expand=True, padx=50, pady=(0,8))
        style = ttk.Style()
        style.configure("RB.Treeview", background=self.C("input_bg"),
                        foreground=self.C("text"), rowheight=24,
                        fieldbackground=self.C("input_bg"), borderwidth=0)
        style.configure("RB.Treeview.Heading", background=self.C("primary"),
                        foreground="white", font=(_UI_FONT,9,"bold"), relief="flat")
        style.map("RB.Treeview", background=[("selected",self.C("primary"))],
                  foreground=[("selected","#ffffff")])
        lf2 = tk.Frame(lo, bg=self.C("panel")); lf2.pack(fill="both", expand=True, padx=10, pady=8)
        rb_cols = ("Name","Type","Size","Modified")
        rb_tree = ttk.Treeview(lf2, columns=rb_cols, show="headings",
                               height=16, style="RB.Treeview")
        for c, w in zip(rb_cols, (400,80,100,160)):
            rb_tree.heading(c, text=c); rb_tree.column(c, width=w, anchor="w")
        rbsb = ttk.Scrollbar(lf2, orient="vertical", command=rb_tree.yview)
        rb_tree.configure(yscrollcommand=rbsb.set)
        rb_tree.pack(side="left", fill="both", expand=True); rbsb.pack(side="right", fill="y")
        file_data = {}

        def get_sftp():
            srv = server_var.get()
            key = {"PCAP":"pcap","LBS / LUDR / SMS":"ludr","Voice":"voice"}.get(srv,"pcap")
            return CONN.get_sftp(key), CONN.get_ssh(key)

        def browse(path=None):
            p = path or path_var.get().strip() or "/"
            path_var.set(p)
            status_var.set(f"🔄  Listing {p}…")
            rb_tree.delete(*rb_tree.get_children()); file_data.clear()
            def _run():
                sftp, _ = get_sftp()
                if not sftp:
                    self.after(0, lambda: status_var.set("⚠️  Not connected")); return
                try:
                    entries = []
                    for attr in sorted(sftp.listdir_attr(p),
                                       key=lambda a:(not bool(a.st_mode&0o40000), a.filename)):
                        import datetime as _dt
                        is_dir = bool(attr.st_mode & 0o40000)
                        sz = "—" if is_dir else f"{attr.st_size:,} B"
                        mt = _dt.datetime.fromtimestamp(attr.st_mtime).strftime(
                            "%d/%m/%Y %H:%M") if attr.st_mtime else "—"
                        entries.append((attr.filename,
                                        "📁 Dir" if is_dir else "📄 File",
                                        sz, mt, is_dir, attr))
                    def _upd():
                        rb_tree.delete(*rb_tree.get_children()); file_data.clear()
                        if p != "/":
                            iid = rb_tree.insert("","end",
                                values=("📁  ..", "Dir","—","—"))
                            file_data[iid] = {"is_dir":True,
                                             "path":os.path.dirname(p.rstrip("/"))}
                        for fn,ft,sz,mt,is_dir,attr in entries:
                            iid = rb_tree.insert("","end",
                                values=(("📁  " if is_dir else "📄  ")+fn, ft, sz, mt))
                            file_data[iid] = {"is_dir":is_dir,
                                             "path":p.rstrip("/")+"/"+fn}
                        status_var.set(f"✅  {len(entries)} item(s)  |  {p}")
                    self.after(0, _upd)
                except Exception as e:
                    self.after(0, lambda: status_var.set(f"❌  {e}"))
            threading.Thread(target=_run, daemon=True).start()

        rb_tree.bind("<Double-1>", lambda e: (
            sel := rb_tree.selection(),
            info := file_data.get(sel[0]) if sel else None,
            browse(info["path"]) if info and info["is_dir"] else None
        ) if rb_tree.selection() else None)

        def download_sel():
            sel = rb_tree.selection()
            if not sel: return self.popup("Info","Select a file.","info")
            info = file_data.get(sel[0])
            if not info or info["is_dir"]:
                return self.popup("Info","Select a file (not a folder).","info")
            fname = os.path.basename(info["path"])
            local = filedialog.asksaveasfilename(initialfile=fname)
            if not local: return
            sftp, _ = get_sftp()
            if not sftp: return self.popup("Error","Not connected.","error")
            try:
                sftp.get(info["path"], local)
                self.popup("Downloaded", f"{fname} saved.", "success")
            except Exception as e:
                self.popup("Error", str(e), "error")

        def delete_sel():
            sel = rb_tree.selection()
            if not sel: return self.popup("Info","Select a file.","info")
            info = file_data.get(sel[0])
            if not info or info["is_dir"]:
                return self.popup("Info","Only individual files can be deleted.","info")
            fname = os.path.basename(info["path"])
            if not self._ask_confirm("Delete File", f"Delete on server:\n{fname}?"): return
            sftp, _ = get_sftp()
            if not sftp: return self.popup("Error","Not connected.","error")
            try:
                sftp.remove(info["path"])
                LOG.log("Browser", f"Deleted: {info['path']}")
                browse()
                self.popup("Deleted", f"{fname} deleted.", "success")
            except Exception as e:
                self.popup("Error", str(e), "error")

        path_entry.bind("<Return>", lambda e: browse())
        ar = tk.Frame(main, bg=self.C("bg")); ar.pack(anchor="w", padx=50, pady=(0,10))
        for txt, cmd, bg in [
            ("🔄  Browse",        lambda: browse(),                 self.C("success")),
            ("⬆️  Go Up",         lambda: browse(
                str(os.path.dirname(path_var.get().rstrip("/")))),  self.C("primary")),
            ("⬇️  Download File", download_sel,                     self.C("primary")),
            ("🗑️  Delete File",   delete_sel,                       self.C("error")),
        ]:
            tk.Button(ar, text=txt, command=cmd, bg=bg, fg="white", relief="flat",
                      font=(_UI_FONT,10,"bold"),
                      activebackground=self.C("border")).pack(
                          side="left", padx=(0,8), ipady=6, ipadx=10)

    # ──────────────────────────────────────────────────────────────
    # SERVER LOG MONITOR
    # ──────────────────────────────────────────────────────────────
    # Persistent service list stored in comtrail_config.json
    # under key "server_log_services": [{name, path, server_key}]
    # ──────────────────────────────────────────────────────────────
    # ──────────────────────────────────────────────────────────────
    # SERVER LOG PANEL
    # ──────────────────────────────────────────────────────────────
    # ──────────────────────────────────────────────────────────────
    # SERVER LOG PANEL — embeds into any parent frame
    # ──────────────────────────────────────────────────────────────
    def _build_server_log_panel(self, parent):
        """
        Server log monitor with non-blocking SSH reads and per-service
        enable/disable toggles. Only enabled services appear clickable.
        """
        SERVER_KEY = "pcap"
        ALL_SVC = [
            {"name": "tmsc",      "path": "/var/log/tmsc",                   "enabled": True},
            {"name": "prism",     "path": "/var/log/prism",                  "enabled": True},
            {"name": "redis",     "path": "/var/log/redis",                  "enabled": False},
            {"name": "signal",    "path": "/var/log/signal",                 "enabled": True},
            {"name": "RmqToKafka",
             "path": "/var/log/RmqToKafka/RmqToKafka_Instance_1/lib/logs",  "enabled": True},
            {"name": "stm",       "path": "/var/log/stm",                    "enabled": False},
            {"name": "vsf",       "path": "/var/log/vsf",                    "enabled": False},
            {"name": "idm1",      "path": "/var/log/idm1",                   "enabled": False},
            {"name": "idm2",      "path": "/var/log/idm2",                   "enabled": False},
            {"name": "MavenLogs", "path": "/var/log/MavenLogs",              "enabled": False},
            {"name": "voice1",    "path": "/var/log/voice1",                 "enabled": False},
            {"name": "FilterProvisionUtility",
             "path": "/usr/local/cleartrail/FilterProvisionUtility/Logs",    "enabled": False},
            {"name": "FilterProvisionUtility4Voice",
             "path": "/usr/local/cleartrail/FilterProvisionUtility4Voice/Logs", "enabled": False},
            {"name": "QueryFetcherUtility",
             "path": "/usr/local/cleartrail/QueryFetcherUtility/LogFiles",   "enabled": False},
            {"name": "Monit",     "path": "/var/log",                        "enabled": False},
        ]
        # Load saved config; merge so new defaults always appear
        saved     = self.cfg.get("server_log_services", [])
        saved_map = {s["name"]: s for s in saved}
        services  = []
        for dflt in ALL_SVC:
            sv = dict(dflt)
            if dflt["name"] in saved_map:
                sv["path"]    = saved_map[dflt["name"]].get("path", dflt["path"])
                sv["enabled"] = bool(saved_map[dflt["name"]].get("enabled", False))
            services.append(sv)
        dflt_names = {s["name"] for s in ALL_SVC}
        for sv in saved:
            if sv["name"] not in dflt_names:
                services.append({"name": sv["name"],
                                  "path": sv.get("path", ""),
                                  "enabled": bool(sv.get("enabled", True))})

        def _save_svc():
            self.cfg["server_log_services"] = [
                {"name": s["name"], "path": s["path"], "enabled": s["enabled"]}
                for s in services]
            save_config(self.cfg)

        def _get_ssh_p():
            return CONN.get_ssh(SERVER_KEY)

        # Non-blocking SSH read — channel with hard timeout, never hangs UI
        def _ssh_read(cmd, timeout=8):
            ssh = _get_ssh_p()
            if not ssh:
                raise ConnectionError("Not connected — check Settings")
            transport = ssh.get_transport()
            if not transport or not transport.is_active():
                raise ConnectionError("SSH transport not active")
            chan = transport.open_session()
            chan.settimeout(timeout)
            chan.exec_command(cmd)
            chunks = []
            try:
                while True:
                    data = chan.recv(8192)
                    if not data:
                        break
                    chunks.append(data)
            except Exception:
                pass
            try:
                chan.close()
            except Exception:
                pass
            return b"".join(chunks).decode("utf-8", "ignore")

        # Layout
        layout = tk.Frame(parent, bg=self.C("bg"))
        layout.pack(fill="both", expand=True)

        # LEFT: service list panel — 3D card with scroll buttons
        _lo_bg = "#0d1117" if self._theme == "dark" else self.C("panel")
        lo = tk.Frame(layout, bg=_lo_bg,
                      highlightbackground=self.C("primary"),
                      highlightthickness=2, width=268)
        lo.pack(side="left", fill="y", padx=(0, 8), pady=2)
        lo.pack_propagate(False)

        # Header — gradient-style top bar
        lhdr = tk.Frame(lo, bg=self.C("primary"), height=40)
        lhdr.pack(fill="x")
        lhdr.pack_propagate(False)
        # 3D top highlight
        tk.Frame(lhdr, bg="#06b6d4", height=2).pack(fill="x", side="top")
        ltop = tk.Frame(lhdr, bg=self.C("primary"))
        ltop.pack(fill="x", padx=8, expand=True)
        tk.Label(ltop, text="🖥️  Services",
                 bg=self.C("primary"), fg="white",
                 font=(_UI_FONT, 10, "bold")).pack(side="left", pady=7)
        add_btn_top = tk.Button(ltop, text="+ Add",
                                bg="#0e7490", fg="white",
                                activebackground="#0891b2",
                                activeforeground="white",
                                relief="flat",
                                font=(_UI_FONT, 8, "bold"),
                                cursor="hand2")
        add_btn_top.pack(side="right", ipady=3, ipadx=8, pady=6)

        # Sub-header hint
        _hint_bg = "#161b22" if self._theme == "dark" else self.C("input_bg")
        hint_fr = tk.Frame(lo, bg=_hint_bg)
        hint_fr.pack(fill="x")
        tk.Label(hint_fr,
                 text="☑ monitored  ☐ disabled — tick to enable",
                 bg=_hint_bg, fg=self.C("primary"),
                 font=(_UI_FONT, 7)).pack(
            anchor="w", padx=10, pady=4)
        tk.Frame(lo, bg=self.C("primary"), height=1).pack(fill="x")

        # Scrollable list area
        list_area = tk.Frame(lo, bg=_lo_bg)
        list_area.pack(fill="both", expand=True)

        svc_cv = tk.Canvas(list_area, bg=_lo_bg,
                           highlightthickness=0)
        svc_cv.pack(side="left", fill="both", expand=True)
        svc_fr = tk.Frame(svc_cv, bg=_lo_bg)
        svc_cv.create_window((0, 0), window=svc_fr, anchor="nw")
        svc_fr.bind("<Configure>",
                    lambda e: svc_cv.configure(
                        scrollregion=svc_cv.bbox("all")))

        # ▲ ▼ Scroll buttons at the bottom of left panel
        # ── Mousewheel scroll — reliable cross-widget solution ────────
        # bind_all captures ALL wheel events app-wide, then we check
        # if the mouse is currently inside the service list panel.
        def _on_svc_wheel(event):
            try:
                # Only scroll if mouse is inside the left panel
                wx, wy = event.x_root, event.y_root
                lx1 = lo.winfo_rootx()
                ly1 = lo.winfo_rooty()
                lx2 = lx1 + lo.winfo_width()
                ly2 = ly1 + lo.winfo_height()
                if lx1 <= wx <= lx2 and ly1 <= wy <= ly2:
                    if event.num == 4:
                        svc_cv.yview_scroll(-2, "units")
                    elif event.num == 5:
                        svc_cv.yview_scroll(2, "units")
                    else:
                        svc_cv.yview_scroll(
                            int(-1 * (event.delta / 120)), "units")
            except Exception:
                pass

        lo.bind_all("<MouseWheel>", _on_svc_wheel)
        lo.bind_all("<Button-4>",   _on_svc_wheel)
        lo.bind_all("<Button-5>",   _on_svc_wheel)

        # RIGHT: log viewer
        right = tk.Frame(layout, bg=self.C("bg"))
        right.pack(side="left", fill="both", expand=True)

        sv_title  = tk.StringVar(value="← Tick a service checkbox then click it")
        sv_status = tk.StringVar(value="")

        # ── Multi-service tab strip ────────────────────────────────
        # Each pinned service gets its own tab. Tabs store independent state.
        _tabs       = {}   # name → {frame, st, log_txt, ...}
        _active_tab = [None]

        tab_strip = tk.Frame(right, bg=self.C("panel"),
                             highlightbackground=self.C("border"),
                             highlightthickness=1)
        tab_strip.pack(fill="x", pady=(2, 0))
        tk.Label(tab_strip, text="Pinned Services:",
                 bg=self.C("panel"), fg=self.C("muted"),
                 font=(_UI_FONT, 8)).pack(side="left", padx=(8, 6), pady=5)
        tabs_inner = tk.Frame(tab_strip, bg=self.C("panel"))
        tabs_inner.pack(side="left", fill="x", expand=True)

        def _make_tab(svc_name, svc_path):
            """Create or switch to a tab for svc_name."""
            if svc_name in _tabs:
                _activate_tab(svc_name)
                return
            # Tab button
            tab_btn_frame = tk.Frame(tabs_inner, bg=self.C("panel"))
            tab_btn_frame.pack(side="left", padx=2, pady=3)
            tbtn = tk.Button(tab_btn_frame, text=svc_name,
                             bg=self.C("primary"), fg="white",
                             relief="flat", font=(_UI_FONT, 8, "bold"),
                             cursor="hand2",
                             command=lambda n=svc_name: _activate_tab(n))
            tbtn.pack(side="left", ipadx=8, ipady=3)
            xcls = tk.Label(tab_btn_frame, text="✕",
                            bg=self.C("primary"), fg="#a5f3fc",
                            font=(_UI_FONT, 8), cursor="hand2")
            xcls.pack(side="left", padx=(0, 2))
            xcls.bind("<Button-1>", lambda e, n=svc_name: _close_tab(n))

            # Content frame for this tab
            cf = tk.Frame(right, bg=self.C("bg"))
            _tabs[svc_name] = {
                "btn_frame": tab_btn_frame, "tbtn": tbtn, "xcls": xcls,
                "frame": cf, "path": svc_path,
                "st": {"svc": svc_name, "path": svc_path,
                       "after_id": None, "content": ""},
            }
            _activate_tab(svc_name)

        def _activate_tab(name):
            if _active_tab[0] == name:
                return
            # Hide current tab content
            if _active_tab[0] and _active_tab[0] in _tabs:
                try:
                    _tabs[_active_tab[0]]["frame"].pack_forget()
                    _tabs[_active_tab[0]]["tbtn"].config(
                        bg=self.C("input_bg"), fg=self.C("muted"))
                    _tabs[_active_tab[0]]["xcls"].config(
                        bg=self.C("input_bg"))
                except Exception:
                    pass
            _active_tab[0] = name
            if name in _tabs:
                t = _tabs[name]
                t["tbtn"].config(bg=self.C("primary"), fg="white")
                t["xcls"].config(bg=self.C("primary"))
                t["frame"].pack(fill="both", expand=True)
                sv_title.set(f"📄  {name}  —  {t['path']}")
                # Always refresh shared controls when switching to a service tab
                _st["svc"]     = name
                _st["path"]    = t["path"]
                _st["content"] = ""
                _st["gen"]    += 1
                file_v.set("")
                file_cb["values"] = []
                log_txt.config(state="normal")
                log_txt.delete("1.0", "end")
                log_txt.config(state="disabled")
                sv_status.set(f"Listing {t['path']}…")
                def _cb_tab(files, _p=t["path"]):
                    file_cb["values"] = files
                    if files:
                        file_v.set(files[0])
                        sv_status.set(f"✅  {len(files)} file(s) · {_p}")
                        _load_p()
                    else:
                        sv_status.set(f"⚠️  No files in {_p}")
                _list_p(t["path"], _cb_tab)

        def _close_tab(name):
            if name not in _tabs:
                return
            t = _tabs.pop(name)
            try:
                if t["st"]["after_id"]:
                    self.after_cancel(t["st"]["after_id"])
            except Exception:
                pass
            t["btn_frame"].destroy()
            t["frame"].destroy()
            if _active_tab[0] == name:
                _active_tab[0] = None
                if _tabs:
                    _activate_tab(list(_tabs.keys())[-1])
                else:
                    sv_title.set("← Tick a service checkbox then click it")
                    sv_status.set("")

        tb1 = tk.Frame(right, bg=self.C("bg"))
        tb1.pack(fill="x", pady=(4, 2))
        tk.Label(tb1, textvariable=sv_title, bg=self.C("bg"),
                 fg=self.C("card_title"),
                 font=(_UI_FONT, 10, "bold")).pack(side="left")
        tk.Label(tb1, textvariable=sv_status, bg=self.C("bg"),
                 fg=self.C("muted"),
                 font=(_UI_FONT, 8)).pack(side="right")

        lines_v  = tk.StringVar(value="200")
        search_v = tk.StringVar()
        auto_v   = tk.BooleanVar(value=True)
        mode_v   = tk.StringVar(value="tail")   # "tail" or "grep"

        tb2 = tk.Frame(right, bg=self.C("bg"))
        tb2.pack(fill="x", pady=(0, 2))

        # Mode toggle: Tail / Grep
        tk.Label(tb2, text="Mode:", bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(side="left", padx=(0, 3))

        _lines_container = tk.Frame(tb2, bg=self.C("bg"))

        def _on_mode_change():
            if mode_v.get() == "grep":
                _lines_container.pack_forget()
            else:
                _lines_container.pack(side="left")
            _load_p()

        for mv, ml in [("tail", "Tail"), ("grep", "Grep")]:
            tk.Radiobutton(tb2, text=ml, variable=mode_v, value=mv,
                           bg=self.C("bg"), fg=self.C("muted"),
                           selectcolor=self.C("panel"),
                           activebackground=self.C("bg"),
                           font=(_UI_FONT, 9),
                           command=_on_mode_change).pack(side="left", padx=3)

        _lines_container.pack(side="left")
        tk.Frame(_lines_container, bg=self.C("border"), width=1).pack(
            side="left", fill="y", pady=3, padx=6)
        tk.Label(_lines_container, text="Lines:", bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(side="left", padx=(0, 3))
        ttk.Combobox(_lines_container, textvariable=lines_v,
                     values=["50", "100", "200", "500", "1000"],
                     state="readonly", width=6).pack(side="left")

        tk.Frame(tb2, bg=self.C("border"), width=1).pack(
            side="left", fill="y", pady=3, padx=6)
        tk.Label(tb2, text="Search:", bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(side="left", padx=(0, 3))
        tk.Entry(tb2, textvariable=search_v,
                 bg=self.C("input_bg"), fg=self.C("text"),
                 insertbackground=self.C("text"),
                 relief="flat", width=22).pack(side="left", ipady=4, padx=(0, 4))
        tk.Button(tb2, text="✕", bg=self.C("bg"), fg=self.C("muted"),
                  relief="flat", font=(_UI_FONT, 8), cursor="hand2",
                  command=lambda: search_v.set("")).pack(side="left", padx=(0, 8))

        tk.Checkbutton(tb2, text="Auto-refresh (5s)", variable=auto_v,
                       bg=self.C("bg"), fg=self.C("muted"),
                       selectcolor=self.C("panel"),
                       activebackground=self.C("bg"),
                       font=(_UI_FONT, 9)).pack(side="left")

        file_v = tk.StringVar()
        tb3 = tk.Frame(right, bg=self.C("bg"))
        tb3.pack(fill="x", pady=(0, 4))
        tk.Label(tb3, text="Log File:", bg=self.C("bg"), fg=self.C("muted"),
                 font=(_UI_FONT, 9)).pack(side="left", padx=(0, 5))
        file_cb = ttk.Combobox(tb3, textvariable=file_v,
                               state="readonly", width=42)
        file_cb.pack(side="left", ipady=4, padx=(0, 6))
        bc = dict(relief="flat", font=(_UI_FONT, 9, "bold"),
                  activebackground=self.C("border"))
        ref_btn = tk.Button(tb3, text="🔄  Refresh",
                            bg="#059669", fg="white",
                            activebackground="#047857",
                            activeforeground="white",
                            relief="flat",
                            font=(_UI_FONT, 9, "bold"),
                            cursor="hand2")
        ref_btn.pack(side="left", ipady=5, ipadx=10, padx=(0, 4))
        clr_btn = tk.Button(tb3, text="🗑️  Clear",
                            bg="#374151", fg="white",
                            activebackground="#4b5563",
                            activeforeground="white",
                            relief="flat",
                            font=(_UI_FONT, 9, "bold"),
                            cursor="hand2")
        clr_btn.pack(side="left", ipady=5, ipadx=10, padx=(0, 4))
        sav_btn = tk.Button(tb3, text="📥  Save Displayed",
                            bg="#0891b2", fg="white",
                            activebackground="#0e7490",
                            activeforeground="white",
                            relief="flat",
                            font=(_UI_FONT, 9, "bold"),
                            cursor="hand2")
        sav_btn.pack(side="left", ipady=5, ipadx=10, padx=(0, 4))
        _add_tooltip(sav_btn, "Save currently displayed lines to a local file")

        dl_btn = tk.Button(tb3, text="⬇  Full File",
                           bg="#0e7490", fg="white",
                           activebackground="#0891b2",
                           activeforeground="white",
                           relief="flat",
                           font=(_UI_FONT, 9, "bold"),
                           cursor="hand2")
        dl_btn.pack(side="left", ipady=5, ipadx=10)
        _add_tooltip(dl_btn, "Download complete log file from server via SFTP")

        # Dark terminal display
        _log_bg  = "#0d1117" if self._theme == "dark" else "#f8fafc"
        _log_fg  = "#c9d1d9" if self._theme == "dark" else "#1e293b"
        log_ou = tk.Frame(right, bg=_log_bg,
                          highlightbackground=self.C("border"),
                          highlightthickness=1)
        log_ou.pack(fill="both", expand=True)
        log_txt = tk.Text(log_ou, bg=_log_bg, fg=_log_fg,
                          font=("Consolas", 9), relief="flat",
                          wrap="none", insertbackground=_log_fg,
                          selectbackground=self.C("primary"),
                          state="disabled")
        lv = ttk.Scrollbar(log_ou, orient="vertical",
                           command=log_txt.yview)
        lh = ttk.Scrollbar(log_ou, orient="horizontal",
                           command=log_txt.xview)
        log_txt.configure(yscrollcommand=lv.set, xscrollcommand=lh.set)
        lv.pack(side="right", fill="y")
        lh.pack(side="bottom", fill="x")
        log_txt.pack(fill="both", expand=True, padx=2, pady=2)

        for tag, fg_c, bg_c in [
            ("error", "#f85149", "#2d1b1b"),
            ("warn",  "#e3b341", "#2b2000"),
            ("info",  "#58a6ff", ""),
            ("ok",    "#3fb950", ""),
            ("dim",   "#8b949e", ""),
            ("hl",    "#ffffff", "#2d4a1e"),
        ]:
            kw = {"foreground": fg_c}
            if bg_c:
                kw["background"] = bg_c
            log_txt.tag_configure(tag, **kw)

        _st = {"svc": None, "path": None, "after_id": None, "content": "", "gen": 0}

        # ── Core functions ─────────────────────────────────────────────
        def _write_p(c, srch=""):
            log_txt.config(state="normal")
            log_txt.delete("1.0", "end")
            match_count = [0]
            for line in c.splitlines():
                ll = line.lower()
                if any(w in ll for w in
                       ["error", "exception", "traceback", "fatal", "critical", "failed"]):
                    tag = "error"
                elif any(w in ll for w in ["warn", "warning"]):
                    tag = "warn"
                elif any(w in ll for w in
                         ["info", "started", "connected", "success", "initialized", "ready"]):
                    tag = "info"
                elif any(w in ll for w in ["debug", "trace", "verbose"]):
                    tag = "dim"
                else:
                    tag = ""
                log_txt.insert("end", line + "\n", tag)
            # Highlight search matches and count them
            if srch.strip():
                pos = "1.0"
                first = None
                while True:
                    pos = log_txt.search(srch, pos, stopindex="end", nocase=True)
                    if not pos:
                        break
                    end_pos = f"{pos}+{len(srch)}c"
                    log_txt.tag_add("hl", pos, end_pos)
                    match_count[0] += 1
                    if first is None:
                        first = pos
                    pos = end_pos
                if first:
                    log_txt.see(first)  # scroll to first match
            log_txt.config(state="disabled")
            if not srch.strip():
                log_txt.see("end")
            return match_count[0]

        def _load_p():
            d  = _st["path"]
            fn = file_v.get()
            if not d or not fn:
                return
            n   = lines_v.get() or "200"
            fp  = d.rstrip("/") + "/" + fn
            sq  = search_v.get().strip()
            mode = mode_v.get()
            _gen = _st["gen"]
            sv_status.set("🔄  Loading…")
            def _run():
                try:
                    if mode == "grep" and sq:
                        cmd = f"grep -in {shlex.quote(sq)} {shlex.quote(fp)} 2>/dev/null || true"
                    elif n == "Full":
                        cmd = "cat " + shlex.quote(fp)
                    else:
                        cmd = "tail -n " + shlex.quote(n) + " " + shlex.quote(fp)
                    c = _ssh_read(cmd, timeout=15)
                    if not c:
                        c = "(empty — no matches)" if (mode == "grep" and sq) else "(empty)"
                    _st["content"] = c
                    lc = c.count("\n")
                    ts = time.strftime("%H:%M:%S")
                    def _ui(cc=c, llc=lc, ffn=fn, tts=ts, g=_gen):
                        if _st["gen"] != g:
                            return  # stale load — user switched service
                        mc = _write_p(cc, sq if mode == "grep" else sq)
                        mode_note = f" · grep '{sq}'" if mode == "grep" and sq else ""
                        match_note = f" · {mc} match{'es' if mc != 1 else ''}" if sq and mc else ""
                        sv_status.set(f"✅  {llc} lines · {ffn} · {tts}{mode_note}{match_note}")
                    self.after(0, _ui)
                except Exception as e:
                    self.after(0, lambda err=e, g=_gen: sv_status.set(f"❌  {err}") if _st["gen"] == g else None)
            threading.Thread(target=_run, daemon=True).start()

        def _list_p(d, cb):
            def _run():
                try:
                    out   = _ssh_read(
                        "ls -1t " + shlex.quote(d) + " 2>/dev/null",
                        timeout=10)
                    files = [f.strip() for f in out.splitlines()
                             if f.strip()]
                    try:
                        self.after(0, lambda: cb(files))
                    except Exception:
                        pass
                except Exception as e:
                    try:
                        self.after(0, lambda err=e: sv_status.set(f"❌  {err}"))
                    except Exception:
                        pass
            threading.Thread(target=_run, daemon=True).start()

        # ── Service list builder ───────────────────────────────────────
        def _build_p():
            for w in svc_fr.winfo_children():
                w.destroy()
            for svc in services:
                name    = svc["name"]
                path    = svc["path"]
                enabled = svc.get("enabled", False)

                # Row bg: theme + enabled state
                if self._theme == "dark":
                    row_bg  = "#0c3d4a" if enabled else "#0d1117"
                    row_acc = "#0891b2" if enabled else "#1e293b"
                else:
                    row_bg  = "#cffafe" if enabled else self.C("panel")
                    row_acc = "#0891b2" if enabled else "#cbd5e1"

                row = tk.Frame(svc_fr, bg=row_bg, cursor="hand2")
                row._n = name
                row.pack(fill="x", padx=4, pady=2)

                # Left accent bar
                tk.Frame(row, bg=row_acc, width=4).pack(
                    side="left", fill="y")

                inner = tk.Frame(row, bg=row_bg)
                inner.pack(side="left", fill="x",
                           expand=True, padx=(6, 4), pady=4)

                tr = tk.Frame(inner, bg=row_bg)
                tr.pack(fill="x")

                en_var = tk.BooleanVar(value=enabled)

                # Pack delete/edit buttons FIRST (right side)
                # so they are never pushed off-screen by the name label
                dbtn = tk.Label(tr, text="✕", bg=row_bg,
                                fg="#ef4444",
                                font=(_UI_FONT, 9, "bold"),
                                cursor="hand2")
                dbtn.pack(side="right", padx=(0, 4))
                ebtn = tk.Label(tr, text="✎", bg=row_bg,
                                fg="#64748b",
                                font=(_UI_FONT, 9),
                                cursor="hand2")
                ebtn.pack(side="right", padx=(0, 2))

                # Checkbox (left)
                en_cb  = tk.Checkbutton(
                    tr, variable=en_var,
                    bg=row_bg,
                    activebackground=row_bg,
                    selectcolor="#164e63" if self._theme=="dark"
                    else "#a5f3fc",
                    cursor="hand2")
                en_cb.pack(side="left", padx=(0, 2))

                # Name label — fills remaining space between checkbox and buttons
                name_lbl = tk.Label(
                    tr, text=name, bg=row_bg,
                    fg=("#e2e8f0" if enabled else "#475569")
                    if self._theme == "dark"
                    else (self.C("card_title") if enabled
                          else self.C("muted")),
                    font=(_UI_FONT, 9, "bold" if enabled else "normal"),
                    anchor="w")
                name_lbl.pack(side="left", fill="x", expand=True)

                short = path.replace("/usr/local/cleartrail", "…")
                if len(short) > 34:
                    short = "…" + short[-32:]
                tk.Label(inner, text=short, bg=row_bg,
                         fg="#06b6d4" if enabled else "#1e293b",
                         font=(_UI_FONT, 7), anchor="w",
                         wraplength=188, justify="left").pack(
                    anchor="w")
                # Thin separator
                _sep_col = "#1e293b" if self._theme=="dark" else self.C("border")
                tk.Frame(svc_fr, bg=_sep_col,
                         height=1).pack(fill="x", padx=4)

                def _on_toggle(sv=svc, v=en_var):
                    sv["enabled"] = bool(v.get())
                    _save_svc()
                    _build_p()
                en_cb.config(command=_on_toggle)

                def _click(e=None, n=name, p=path, sv=svc, r=row):
                    if not sv.get("enabled", False):
                        sv_status.set(
                            f"⚠️  {n} is disabled — tick its checkbox to enable")
                        return
                    # Open / switch to tab; update shared controls
                    _make_tab(n, p)
                    _st["svc"]  = n
                    _st["path"] = p
                    file_v.set("")
                    file_cb["values"] = []
                    _st["content"] = ""
                    log_txt.config(state="normal")
                    log_txt.delete("1.0", "end")
                    log_txt.config(state="disabled")
                    sv_status.set(f"Listing {p}…")
                    def _cb(files, n2=n, p2=p):
                        file_cb["values"] = files
                        if files:
                            file_v.set(files[0])
                            sv_status.set(f"✅  {len(files)} file(s) · {p2}")
                            _load_p()
                        else:
                            sv_status.set(f"⚠️  No files in {p2}")
                    _list_p(p, _cb)

                def _ho(e=None, r=row):
                    if getattr(r, "_n", "") != _st.get("svc", ""):
                        for w in [r] + list(r.winfo_children()):
                            try:
                                w.config(bg="#243358")
                                for c in w.winfo_children():
                                    try:
                                        c.config(bg="#243358")
                                    except Exception:
                                        pass
                            except Exception:
                                pass

                def _hx(e=None, r=row, rb=row_bg):
                    col = ("#164e63"
                           if getattr(r, "_n", "") == _st.get("svc", "")
                           else rb)
                    for w in [r] + list(r.winfo_children()):
                        try:
                            w.config(bg=col)
                            for c in w.winfo_children():
                                try:
                                    c.config(bg=col)
                                except Exception:
                                    pass
                        except Exception:
                            pass

                def _edit(e=None, so=svc):
                    _open_edit_p(so)

                def _del(e=None, n=name):
                    if self._ask_confirm("Remove", f"Remove '{n}'?"):
                        services[:] = [s for s in services if s["name"] != n]
                        _save_svc()
                        _build_p()

                for w in ([row, tr] + list(tr.winfo_children()) +
                          list(row.winfo_children())):
                    if w not in (dbtn, ebtn, en_cb):
                        w.bind("<Button-1>", _click)
                    w.bind("<Enter>", _ho)
                    w.bind("<Leave>", _hx)
                ebtn.bind("<Button-1>", _edit)
                dbtn.bind("<Button-1>", _del)

        # ── Add / Edit dialog ──────────────────────────────────────────
        def _open_edit_p(existing=None):
            dlg = tk.Toplevel(self)
            dlg.title("")
            dlg.configure(bg="#0d1117")
            dlg.resizable(False, False)
            dlg.grab_set()
            dlg.update_idletasks()
            W, H = 520, 420
            x = self.winfo_rootx() + (self.winfo_width()  - W) // 2
            y = self.winfo_rooty() + (self.winfo_height() - H) // 2
            dlg.geometry(f"{W}x{H}+{x}+{y}")

            # ── Outer 3D shadow border ─────────────────────────────
            dlg.configure(bg="#0891b2")
            outer = tk.Frame(dlg, bg="#0891b2", padx=2, pady=2)
            outer.pack(fill="both", expand=True)

            card = tk.Frame(outer, bg=self.C("panel"))
            card.pack(fill="both", expand=True)

            # ── Coloured top header bar ────────────────────────────
            hdr_bar = tk.Frame(card, bg=self.C("primary"), height=58)
            hdr_bar.pack(fill="x", side="top")
            hdr_bar.pack_propagate(False)
            tk.Frame(hdr_bar, bg="#06b6d4", height=2).pack(
                fill="x", side="top")
            icon_lbl = tk.Label(hdr_bar, text="🖥️",
                                bg=self.C("primary"),
                                font=(_UI_FONT, 18))
            icon_lbl.pack(side="left", padx=(14, 6), pady=8)
            title_col = tk.Frame(hdr_bar, bg=self.C("primary"))
            title_col.pack(side="left", pady=8)
            tk.Label(title_col,
                     text="Edit Service" if existing else "Add New Service",
                     bg=self.C("primary"), fg="white",
                     font=(_UI_FONT, 13, "bold")).pack(anchor="w")
            tk.Label(title_col,
                     text="Update log path and settings"
                     if existing else "Configure a new service to monitor",
                     bg=self.C("primary"), fg="#a5f3fc",
                     font=(_UI_FONT, 8)).pack(anchor="w")
            tk.Frame(card, bg="#1e40af", height=2).pack(fill="x", side="top")

            # ── Footer — packed at bottom BEFORE body so it's always visible ──
            tk.Frame(card, bg=self.C("border"), height=1).pack(
                fill="x", side="bottom")
            footer = tk.Frame(card, bg=self.C("bg"), height=62)
            footer.pack(fill="x", side="bottom")
            footer.pack_propagate(False)

            cancel_btn = tk.Button(
                footer, text="✕   Cancel",
                bg="#374151", fg="white",
                activebackground="#4b5563",
                activeforeground="white",
                relief="flat",
                font=(_UI_FONT, 11, "bold"),
                cursor="hand2",
                command=dlg.destroy)
            cancel_btn.pack(side="right", padx=(8, 18),
                            pady=12, ipadx=20, ipady=8)

            save_btn = tk.Button(
                footer, text="💾   Save",
                bg="#0891b2", fg="white",
                activebackground="#0e7490",
                activeforeground="white",
                relief="flat",
                font=(_UI_FONT, 11, "bold"),
                cursor="hand2",
                command=lambda: _save_d())
            save_btn.pack(side="right", padx=(0, 6),
                          pady=12, ipadx=24, ipady=8)

            # ── Form body — fills space between header and footer ──
            body = tk.Frame(card, bg=self.C("panel"))
            body.pack(fill="both", expand=True, padx=22, pady=16)

            nv = tk.StringVar(value=existing["name"] if existing else "")
            pv = tk.StringVar(
                value=existing["path"] if existing else "/var/log/")
            ev = tk.BooleanVar(
                value=existing.get("enabled", True)
                if existing else True)

            for lbl, var, hint in [
                ("Service Name", nv, "e.g.  tmsc"),
                ("Log Path",     pv, "e.g.  /var/log/tmsc"),
            ]:
                grp = tk.Frame(body, bg=self.C("panel"))
                grp.pack(fill="x", pady=(0, 10))

                tk.Label(grp, text=lbl,
                         bg=self.C("panel"),
                         fg=self.C("card_title"),
                         font=(_UI_FONT, 9, "bold")).pack(
                    anchor="w", pady=(0, 3))

                # Entry with border frame (3D inset look)
                entry_wrap = tk.Frame(grp,
                                      bg=self.C("primary"),
                                      padx=1, pady=1)
                entry_wrap.pack(fill="x")
                entry_inner = tk.Frame(entry_wrap,
                                       bg=self.C("input_bg"))
                entry_inner.pack(fill="x")
                tk.Entry(entry_inner, textvariable=var,
                         bg=self.C("input_bg"),
                         fg=self.C("text"),
                         insertbackground=self.C("text"),
                         relief="flat",
                         font=(_UI_FONT, 10)).pack(
                    fill="x", ipady=7, padx=8)

                tk.Label(grp, text=hint,
                         bg=self.C("panel"),
                         fg=self.C("dim"),
                         font=(_UI_FONT, 7)).pack(
                    anchor="w", pady=(2, 0))

            # Enable monitoring toggle
            tog_frame = tk.Frame(body, bg=self.C("input_bg"),
                                 highlightbackground=self.C("border"),
                                 highlightthickness=1)
            tog_frame.pack(fill="x", pady=(0, 8))
            tk.Checkbutton(tog_frame,
                           text="  ✅  Enable monitoring for this service",
                           variable=ev,
                           bg=self.C("input_bg"),
                           fg=self.C("card_title"),
                           selectcolor=self.C("panel"),
                           activebackground=self.C("input_bg"),
                           font=(_UI_FONT, 9, "bold")).pack(
                side="left", padx=10, pady=8)

            # Error label
            el = tk.Label(body, text="",
                          bg=self.C("panel"),
                          fg=self.C("error"),
                          font=(_UI_FONT, 8))
            el.pack(anchor="w")

            def _save_d():
                n = nv.get().strip()
                p = pv.get().strip()
                if not n:
                    el.config(text="⚠️  Service name is required.")
                    return
                if not p:
                    el.config(text="⚠️  Log path is required.")
                    return
                if existing:
                    existing["name"]    = n
                    existing["path"]    = p
                    existing["enabled"] = ev.get()
                else:
                    if any(s["name"] == n for s in services):
                        el.config(text=f"⚠️  '{n}' already exists.")
                        return
                    services.append({"name": n, "path": p,
                                     "enabled": ev.get()})
                _save_svc()
                dlg.destroy()
                _build_p()

            # Keyboard shortcuts
            dlg.bind("<Return>", lambda e: _save_d())
            dlg.bind("<Escape>", lambda e: dlg.destroy())

        add_btn_top.config(command=lambda: _open_edit_p(None))

        # Wire up all buttons
        def _on_srch(*_):
            if _st["content"]:
                _write_p(_st["content"], search_v.get())

        def _auto_p():
            if auto_v.get():
                _load_p()
            _st["after_id"] = self.after(5000, _auto_p)

        def _do_save():
            if not _st["content"]:
                self.popup("Info", "No content to save.", "info")
                return
            p = filedialog.asksaveasfilename(
                defaultextension=".log",
                filetypes=[("Log", "*.log *.txt"), ("All", "*.*")],
                initialfile=file_v.get() or "service.log")
            if not p:
                return
            with open(p, "w", encoding="utf-8") as f:
                f.write(_st["content"])
            self.popup("Saved", f"Saved:\n{os.path.basename(p)}", "success")

        def _do_download_full():
            """Download the complete log file from the server via SFTP."""
            d  = _st["path"]
            fn = file_v.get()
            if not d or not fn:
                self.popup("Info", "Select a service and log file first.", "info")
                return
            sftp = CONN.get_sftp(SERVER_KEY)
            if not sftp:
                self.popup("Error", "Server not connected. Check Settings.", "error")
                return
            save_path = filedialog.asksaveasfilename(
                defaultextension=".log",
                filetypes=[("Log files", "*.log *.txt"), ("All files", "*.*")],
                initialfile=fn, title="Save Full Log File")
            if not save_path:
                return
            remote = d.rstrip("/") + "/" + fn
            sv_status.set("⬇  Downloading full file…")
            def _run():
                try:
                    sftp.get(remote, save_path)
                    sz = os.path.getsize(save_path)
                    self.after(0, lambda: (
                        sv_status.set(f"✅  Downloaded {sz // 1024} KB → {os.path.basename(save_path)}"),
                        self._toast(f"Downloaded: {os.path.basename(save_path)}", "success")))
                except Exception as e:
                    self.after(0, lambda: sv_status.set(f"❌  Download failed: {e}"))
            threading.Thread(target=_run, daemon=True).start()

        ref_btn.config(command=_load_p)
        clr_btn.config(command=lambda: (
            log_txt.config(state="normal"),
            log_txt.delete("1.0", "end"),
            log_txt.config(state="disabled"),
            sv_status.set("Cleared."),
            _st.update({"content": ""})))
        sav_btn.config(command=_do_save)
        dl_btn.config(command=_do_download_full)
        file_cb.bind("<<ComboboxSelected>>", lambda e: _load_p())
        search_v.trace_add("write", _on_srch)
        lines_v.trace_add("write", lambda *_: _load_p())
        _build_p()
        _auto_p()

        def _on_destroy(e=None):
            if _st["after_id"]:
                try: self.after_cancel(_st["after_id"])
                except Exception: pass
            try: lo.unbind_all("<MouseWheel>")
            except Exception: pass
            try: lo.unbind_all("<Button-4>")
            except Exception: pass
            try: lo.unbind_all("<Button-5>")
            except Exception: pass

        parent.bind("<Destroy>", _on_destroy)

    def show_server_log_monitor(self):
        """Full-page server log monitor."""
        self.clear_main()
        main = self.active_frame
        LOG.log("Navigation", "Opened Server Log Monitor")
        hdr = tk.Frame(main, bg=self.C("bg"))
        hdr.pack(fill="x", padx=50, pady=(22, 5))
        tk.Label(hdr, text="🖥️  Server Log Monitor",
                 bg=self.C("bg"), fg=self.C("card_title"),
                 font=(_UI_FONT, 20, "bold")).pack(side="left")
        tk.Frame(main, bg=self.C("border"), height=1).pack(
            fill="x", padx=50, pady=(5, 8))
        pf = tk.Frame(main, bg=self.C("bg"))
        pf.pack(fill="both", expand=True, padx=50, pady=(0, 8))
        self._build_server_log_panel(pf)




# ================================================================
if __name__ == "__main__":
    app = ComTrailApp()
    app.mainloop()
