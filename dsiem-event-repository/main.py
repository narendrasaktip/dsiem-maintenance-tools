# -*- coding: utf-8 -*-
from __future__ import print_function

def save_json_utf8(path, obj):
    """
    Python2/3-safe JSON writer: dumps with ensure_ascii=False and writes unicode.
    """
    import io as _io
    import json as _json
    data = _json.dumps(obj, ensure_ascii=False, indent=2)
    try:
        unicode  # noqa: F821 (py3 ignores)
        if isinstance(data, str):
            data = data.decode("utf-8")
    except NameError:
        pass
    with _io.open(path, "w", encoding="utf-8") as f:
        f.write(data)


import os, re, sys, json, base64, requests, shutil, subprocess
from collections import OrderedDict
try:
    import io
except Exception:
    io = None
from requests.auth import HTTPBasicAuth

# ====== CONFIG ENV ======
ES_HOST = os.getenv("ES_HOST", "http://opensearch:9200")
VERIFY_TLS = os.getenv("VERIFY_TLS", "false").lower() == "true"
TIMEOUT = int(os.getenv("TIMEOUT", "3000"))
PLUGIN_SID_START = int(os.getenv("PLUGIN_SID_START", "1"))

ES_PASSWD_FILE = os.getenv("ES_PASSWD_FILE")
ES_USER_LOOKUP = os.getenv("ES_USER_LOOKUP")

DEFAULT_TEMPLATE_PATH = os.getenv("TEMPLATE_PATH", "./template-70.js")
DEFAULT_VECTOR_TEMPLATE_PATH = os.getenv("VECTOR_TEMPLATE_PATH", "./template-vector.js")
OUT_DIR = os.getenv("OUT_DIR", ".")
META_PATH = os.getenv("META_PATH", "./build_meta.json")

# Refresh interval for Logstash translate dictionary reload
DICT_REFRESH_INTERVAL = int(os.getenv("DICT_REFRESH_INTERVAL", "60"))  # seconds

# Directory for Logstash JSON dictionaries
LOGSTASH_JSON_DICT_DIR = "/etc/logstash/pipelines/dsiem-events/dsiem-plugin-json/"


# GitHub API (Contents API)
GITHUB_REPO   = os.getenv("GITHUB_REPO")
GITHUB_TOKEN  = os.getenv("GITHUB_TOKEN")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH")

AUTO_USE_CONFIG = os.getenv("AUTO_USE_CONFIG", "0") == "1"
PLUGIN_REGISTRY_PATH = "plugin_id.json"

# Distribusi & restart
LOGSTASH_PIPE_DIR = "/root/kubeappl/logstash/configs/pipelines/dsiem-events/"
LOGSTASH_HOME     = "/root/kubeappl/logstash/"
FRONTEND_POD      = "dsiem-frontend-0"
BACKEND_POD       = "dsiem-backend-0"

# ====== I/O & helpers ======
def py_input(p):
    try:
        return raw_input(p)
    except NameError:
        return input(p)

def save_text_utf8(path, text):
    if sys.version_info[0] == 2:
        import io as _io
        if isinstance(text, str):
            try: text = text.decode("utf-8")
            except Exception: text = text.decode("latin-1")
        with _io.open(path,"w",encoding="utf-8") as f: f.write(text)
    else:
        with io.open(path,"w",encoding="utf-8") as f: f.write(text)

def ask_yes_no(p):
    while True:
        a = py_input(p).strip().lower()
        if a in ("y","n"): return a
        print("Ketik 'y' atau 'n' ya...")

def ask_hf(label, default_mode=None):
    while True:
        prompt = "{} mode [H=Hardcode / F=Field]".format(label)
        if default_mode in ("h","f"):
            prompt += " [default: {}]".format(default_mode.upper())
        prompt += ": "
        m = py_input(prompt).strip().lower()
        if not m and default_mode in ("h","f"):
            m = default_mode
        if m in ("h","f"): return m
        print("Pilih 'H' atau 'F' ya...")

def ensure_credentials_file(path):
    if not os.path.exists(path):
        raise SystemExit("[CRED ERROR] File kredensial tidak ditemukan: {}".format(path))
    if not os.path.isfile(path):
        raise SystemExit("[CRED ERROR] Path bukan file biasa: {}".format(path))

def write_json_dictionary(path, rows):
    """
    Tulis kamus JSON flat { "Event Title": SID, ... } dari rows parse_tsv().
    Output diurutkan ASC berdasarkan SID (1,2,3,...) agar rapi.
    Python2/3-safe: pastikan keys unicode dan tulis unicode.
    """
    import io
    import json as _json
    try:
        from collections import OrderedDict
    except ImportError:
        from ordereddict import OrderedDict  # jika environment lama

    # Kumpulkan pasangan (title, sid) + normalisasi unicode (Py2)
    pairs = []
    for r in rows or []:
        ev = r.get("event_name", "")
        try:
            basestring
            if isinstance(ev, str):
                try: ev = ev.decode("utf-8")
                except Exception: ev = ev.decode("latin-1", "ignore")
            elif not isinstance(ev, unicode):  # noqa: F821
                ev = unicode(ev)              # noqa: F821
        except NameError:
            ev = str(ev)
        ev = ev.strip()
        if not ev:
            continue
        try:
            sid = int(r.get("plugin_sid", 0))
        except Exception:
            continue
        pairs.append((ev, sid))

    # Urutkan berdasarkan SID ASC, lalu nama event
    pairs.sort(key=lambda x: (x[1], x[0]))

    ordered = OrderedDict((k, v) for k, v in pairs)

    data = _json.dumps(ordered, ensure_ascii=False, indent=2)
    try:
        unicode  # noqa: F821
        if isinstance(data, str):
            data = data.decode("utf-8")
    except NameError:
        pass

    with io.open(path, "w", encoding="utf-8") as f:
        f.write(data)
    return path

def load_credentials(file_path, username_wanted):
    try:
        with open(file_path,"r") as f:
            for line in f:
                s=line.strip()
                if not s or s.startswith("#"): continue
                parts=s.split(":")
                if len(parts)<2: continue
                u=parts[0].strip(); pw=":".join(parts[1:]).strip()
                if u==username_wanted:
                    if not pw: raise SystemExit("Password kosong untuk user '{}' di {}".format(username_wanted,file_path))
                    return u,pw
    except IOError as e:
        raise SystemExit("Gagal membaca file kredensial {}: {}".format(file_path,e))
    raise SystemExit("User '{}' tidak ditemukan di {}".format(username_wanted,file_path))

def sanitize(name):
    s = re.sub(r'[^A-Za-z0-9._-]+','_', name or "")
    s = re.sub(r'_+','_', s).strip('_')
    return s or "out"

def slug(s):
    s = (s or "").strip().lower()
    s = re.sub(r'[^a-z0-9]+','-', s)
    s = re.sub(r'-+','-', s).strip('-')
    return s or ""

def join_nonempty(*parts):
    arr = [slug(p) for p in parts if p and str(p).strip()]
    return "-".join([a for a in arr if a])

def dot_to_brackets(s):
    if not s: return s
    s=s.strip()
    if s.startswith("[") and s.endswith("]"): return s
    parts=[p for p in s.split(".") if p]
    if not parts: return s
    return "".join("["+p+"]" for p in parts)

def to_logstash_accessor(s):
    s = (s or "").strip()
    if not s: return "[@timestamp]"
    if s.startswith("[") and s.endswith("]"): return s
    if s.startswith("@"): return "[{}]".format(s)
    if "." in s: return "".join("[{}]".format(p) for p in s.split(".") if p)
    return "[{}]".format(s)

def to_vrl_accessor(s):
    """
    Converts a field path into valid VRL dot/bracket notation.
    'device.ip' -> '.device.ip'
    'field-with-dash' -> '.["field-with-dash"]'
    'nested.field-with-dash' -> '.nested["field-with-dash"]'
    """
    if not s: return "."
    s = s.strip()

    # Split the path by dots to handle nested fields
    parts = s.split('.')
    vrl_parts = []

    for part in parts:
        # A valid VRL identifier for dot notation generally contains letters, numbers, and underscores.
        # We check for any character that is NOT one of those.
        if re.search(r'[^a-zA-Z0-9_]', part):
            # This part contains special characters (like '-') and needs bracket notation.
            vrl_parts.append('["{}"]'.format(part.replace('"', '\\"')))
        else:
            # This is a simple identifier that can be prefixed with a dot.
            vrl_parts.append("." + part)

    # Join the parts together. A bracketed part doesn't need a preceding dot.
    result = "".join(vrl_parts)

    # The result might start with a dot (e.g., ".field1"), which is correct.
    # If it starts with brackets (e.g., '["field-1"]'), it also needs a leading dot.
    if result.startswith("["):
        result = "." + result
    
    return result

def to_hard_literal(val):
    s = (val or "").strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    m = re.match(r'%\{\[\s*(.*?)\s*\]\}$', s)
    if m:
        s = m.group(1).strip()
        if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
            s = s[1:-1].strip()
    m2 = re.match(r'^\{?\[?\s*(.*?)\s*\]?\}?$', s)
    if m2: s = m2.group(1).strip()
    while (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s_old=s; s=s[1:-1].strip()
        if s==s_old: break
    return '"{}"'.format(s.replace('"', r'\"'))

def field_to_interp(s):
    if not s: return ""  # Change this line to return an empty string
    if s.startswith("%{[") and s.endswith("]}"): return s
    return "%{"+dot_to_brackets(s)+"}"

def to_spt_slug(log_type, f1):
    s_log = slug(log_type)
    s_f1 = slug(f1)
    if s_f1:
        return "{}-{}".format(s_log, s_f1)
    return s_log

def guess_module_from_field(field_name):
    base = field_name.replace(".keyword","").strip().lower()
    if base in ("applicationprotocol","application.protocol","app","app_protocol"):
        return ("utm","app-ctrl")
    if base in ("threat","threatname","threat.name"):
        return ("utm","threat")
    if base in ("event_name","eventname"):
        return ("category","")
    if base in ("policyid","policy.id"):
        return ("fw","policy")
    return (base or "category","")

def ask_module_pair(default_module, default_submodule):
    mm = py_input("Module utama (default: {}): ".format(default_module)).strip()
    module_slug = slug(mm) if mm else None
    sm = py_input("Submodule (default: {}): ".format(default_submodule or "category")).strip()
    if (not sm) or (sm.lower()=="default"):
        submodule_slug = None
    else:
        submodule_slug = slug(sm)
    return module_slug, submodule_slug

# ====== GitHub (Contents API) ======
def require_github():
    if not GITHUB_REPO or not GITHUB_TOKEN:
        raise SystemExit("[GITHUB] Set GITHUB_REPO='owner/repo' dan GITHUB_TOKEN='ghp_xxx' dulu ya.")

def gh_headers():
    return {
        "Accept":"application/vnd.github+json",
        "Authorization":"Bearer {}".format(GITHUB_TOKEN),
        "X-GitHub-Api-Version":"2022-11-28"
    }

def gh_get_file(path):
    url = "https://api.github.com/repos/{}/contents/{}".format(GITHUB_REPO, path)
    r = requests.get(url, headers=gh_headers(), params={"ref": GITHUB_BRANCH}, timeout=60)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()

def gh_put_file(path, content_bytes, message, sha=None):
    url = "https://api.github.com/repos/{}/contents/{}".format(GITHUB_REPO, path)
    payload = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode("ascii"),
        "branch": GITHUB_BRANCH
    }
    if sha:
        payload["sha"] = sha
    r = requests.put(url, headers=gh_headers(), data=json.dumps(payload), timeout=60)
    if r.status_code >= 300:
        raise RuntimeError("[GITHUB PUT ERROR] {}: {}".format(r.status_code, r.text[:400]))
    return r.json()

def gh_load_plugin_registry():
    obj = gh_get_file(PLUGIN_REGISTRY_PATH)
    if obj is None:
        return {"used":[]}, None
    try:
        b = base64.b64decode(obj.get("content",""))
        data = json.loads(b.decode("utf-8","replace"))
    except Exception:
        data = {"used":[]}
    if "used" not in data or not isinstance(data["used"], list):
        data = {"used":[]}
    return data, obj.get("sha")

def gh_push_plugin_registry(registry, sha=None, msg="Update plugin_id registry"):
    payload = json.dumps(registry, indent=2, ensure_ascii=False).encode("utf-8")
    gh_put_file(PLUGIN_REGISTRY_PATH, payload, msg, sha=sha)

def registry_used_set(reg):
    vals=[]
    for x in reg.get("used",[]):
        pid = x.get("plugin_id")
        try: vals.append(int(pid))
        except Exception: continue
    return set(vals)

def registry_append(reg, plugin_id, log_type, module_slug, submodule_slug, filter_slug, spt):
    ent = {
        "plugin_id": int(plugin_id),
        "by": "{}/{}".format(log_type, module_slug) if not submodule_slug else "{}/{}/{}".format(log_type, module_slug, submodule_slug),
        "filter": filter_slug or "",
        "siem_plugin_type": spt
    }
    reg["used"].append(ent)
    return reg

def registry_find_pid_for_spt(registry, spt):
    for ent in registry.get("used", []):
        if ent.get("siem_plugin_type") == spt:
            try: return int(ent.get("plugin_id"))
            except Exception: pass
    return None

def gh_paths(log_type, module_name, submodule_name, filter1_slug):
    log_type_slug     = slug(log_type)
    module_slug       = slug(module_name) if module_name else None
    submodule_slug    = slug(submodule_name) if submodule_name and submodule_name.strip() else None
    filter1_slugified = slug(filter1_slug) if filter1_slug else None

    parts = [log_type_slug]
    if module_slug and module_slug != filter1_slugified:
        parts.append(module_slug)
    if submodule_slug and submodule_name and submodule_name.strip() and submodule_slug != filter1_slugified:
        parts.append(submodule_slug)
    if filter1_slugified and filter1_slugified not in parts:
        parts.append(filter1_slugified)
    full_slug = "-".join(parts)

    base_dir_parts = [log_type_slug]
    if module_slug and module_slug != filter1_slugified:
        base_dir_parts.append(module_slug)
    if submodule_slug and submodule_name and submodule_name.strip() and submodule_slug != filter1_slugified:
        base_dir_parts.append(submodule_slug)
    if filter1_slugified and filter1_slugified not in base_dir_parts:
        base_dir_parts.append(filter1_slugified)
    base_dir = "/".join(base_dir_parts)

    tsv_name = "{}_plugin-sids.tsv".format(full_slug)

    return {
        "tsv":        "{}/{}".format(base_dir, tsv_name),
        "json":       "{}/{}_plugin-sids.json".format(base_dir, full_slug),
        "updater_cfg":"{}/{}_updater.json".format(base_dir, full_slug),
        "config":     "{}/config.json".format(base_dir),
        "conf70":     "{}/70_dsiem-plugin_{}.conf".format(base_dir, full_slug),
        "vector_conf":"{}/70_transform_dsiem-plugin-{}.yaml".format(base_dir, full_slug),
        "directive":  "{}/directives_{}_{}.json".format(base_dir, BACKEND_POD, full_slug),
        "full_slug":  full_slug,
        "module_dir": "{}/{}".format(log_type_slug, module_slug) if module_slug else log_type_slug,
        "base_dir":   base_dir
    }

# Di file: main.py

def generate_updater_config(output_path, context):
    """
    Membuat file konfigurasi updater.json berdasarkan konteks dari eksekusi skrip.
    """
    print("\n=== GENERATE KONFIGURASI AUTO-UPDATER ===")
    
    # Merakit struktur JSON menggunakan OrderedDict agar urutannya rapi
    updater_config = OrderedDict([
        ("es", OrderedDict([
            ("host", context.get("es_host")),
            ("verify_tls", context.get("verify_tls")),
            ("timeout", context.get("timeout")),
        ])),
        ("query", OrderedDict([
            ("index", context.get("index_pattern")),
            ("field", context.get("translate_field_no_kw")),
            ("size", context.get("size")),
            ("filters", context.get("filters")),
            # --- TAMBAHKAN BLOK INI ---
            ("time_range", OrderedDict([
                ("field", "@timestamp"),
                ("gte", "now-1h"),
                ("lte", "now")
            ]))
            # --- AKHIR BLOK TAMBAHAN ---
        ])),
        ("layout", OrderedDict([
            ("device", context.get("log_type_auto")),
            ("module", context.get("module_slug")),
            ("submodule", context.get("submodule_slug")),
            ("filter_key", context.get("filter1_slug"))
        ])),
        ("file70", OrderedDict([
            ("plugin_id", context.get("plugin_id_final"))
        ])),
        ("directive", OrderedDict([
            ("HEADER", context.get("directive_cfg_out", {}).get("HEADER")),
            ("CATEGORY", context.get("directive_cfg_out", {}).get("CATEGORY")),
            ("KINGDOM", context.get("directive_cfg_out", {}).get("KINGDOM")),
            ("DISABLED", context.get("directive_cfg_out", {}).get("DISABLED")),
            ("template_id", context.get("directive_cfg_out", {}).get("TEMPLATE_ID"))
        ])),
        ("github", OrderedDict([
            ("template_path", context.get("template_path_70")),
            ("plugin_registry_path", context.get("plugin_registry_path"))
        ])),
        ("customer_config_path", "./customer.json"),
    ])
    
    try:
        save_json_utf8(output_path, updater_config)
        print("[OK] Berhasil membuat file auto-updater config -> {}".format(output_path))
    except Exception as e:
        print("[ERROR] Gagal membuat file auto-updater config: {}".format(e))

# ====== TSV utils (MODIFIED) ======
def write_tsv(path, rows, siem_plugin_type, plugin_id, category, kingdom):
    """Menulis TSV dengan format 6 kolom yang baru."""
    rows = sorted(rows, key=lambda r: int(r.get("plugin_sid", 0)))  # ← tambah ini
    if sys.version_info[0] == 2:
        import io as _io
        f = _io.open(path, "w", encoding="utf-8")
        with f:
            f.write(u"plugin\tid\tsid\ttitle\tcategory\tkingdom\n")
            for r in rows:
                plugin = unicode(siem_plugin_type or "")
                pid = unicode(plugin_id or "")
                sid = unicode(r.get("plugin_sid", ""))
                title = unicode(r.get("event_name", "")).replace(u"\t", u" ")
                cat = unicode(category or "")
                king = unicode(kingdom or "")
                f.write(u"{}\t{}\t{}\t{}\t{}\t{}\n".format(plugin, pid, sid, title, cat, king))
    else:
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write("plugin\tid\tsid\ttitle\tcategory\tkingdom\n")
            for r in rows:
                plugin = str(siem_plugin_type or "")
                pid = str(plugin_id or "")
                sid = str(r.get("plugin_sid", ""))
                title = str(r.get("event_name", "")).replace("\t", " ")
                cat = str(category or "")
                king = str(kingdom or "")
                f.write("{}\t{}\t{}\t{}\t{}\t{}\n".format(plugin, pid, sid, title, cat, king))

def parse_tsv(text):
    """Membaca TSV format baru (6 kolom) dan mengembalikan rows dan metadata."""
    rows = []
    metadata = {}
    lines = text.splitlines()
    if not lines:
        return rows, metadata

    header_line = lines[0].lower().strip()
    if not header_line.startswith("plugin\t"):
        # Fallback for old 2-column format
        if header_line.startswith("plugin_sid"):
            lines = lines[1:]
        for line in lines:
            parts = line.strip().split("\t", 1)
            if len(parts) < 2: continue
            try:
                sid = int(parts[0].strip())
                rows.append({"plugin_sid": sid, "event_name": parts[1].strip()})
            except Exception:
                continue
        return rows, metadata

    header = [h.strip() for h in header_line.split("\t")]
    lines = lines[1:]

    for i, line in enumerate(lines):
        line = line.strip()
        if not line: continue
        parts = line.split("\t")
        if len(parts) != len(header): continue
        row_data = dict(zip(header, parts))
        try:
            sid = int(row_data.get("sid", 0))
            row_obj = {"plugin_sid": sid, "event_name": row_data.get("title", "")}
            rows.append(row_obj)
            if i == 0: # Get metadata from the first data row
                metadata['plugin'] = row_data.get('plugin')
                metadata['id'] = int(row_data.get('id', 0))
                metadata['category'] = row_data.get('category')
                metadata['kingdom'] = row_data.get('kingdom')
        except (ValueError, TypeError):
            continue
    return rows, metadata

def render_tsv(rows, siem_plugin_type, plugin_id, category, kingdom):
    """Merender konten TSV sebagai string dengan format 6 kolom."""
    out = ["plugin\tid\tsid\ttitle\tcategory\tkingdom"]
    for r in rows:
        plugin = str(siem_plugin_type or "")
        pid = str(plugin_id or "")
        sid = str(r.get("plugin_sid", ""))
        title = str(r.get("event_name", "")).replace("\t", " ")
        cat = str(category or "")
        king = str(kingdom or "")
        out.append("{}\t{}\t{}\t{}\t{}\t{}".format(plugin, pid, sid, title, cat, king))
    return "\n".join(out) + "\n"

def merge_dictionary(existing_rows, new_event_names):
    rows = list(existing_rows)
    known = set([r["event_name"] for r in existing_rows])
    max_sid = 0
    for r in existing_rows:
        if r["plugin_sid"] > max_sid:
            max_sid = r["plugin_sid"]
    added = []
    for ev in new_event_names:
        if ev not in known:
            max_sid += 1
            rows.append({"plugin_sid": max_sid, "event_name": ev})
            known.add(ev)
            added.append(ev)
    return rows, added

# ====== ES/OpenSearch ======
def build_filters(filters):
    out=[]
    for f in filters:
        if f["op"]=="contains":
            out.append({"match_phrase":{f["field"]: f["value"]}})
        else:
            out.append({"term":{f["field"]: f["value"]}})
    return out

# Di file: main.py

def build_query(field_name, size, filters, time_range=None):
    q={"size":0,"aggs":{"event_names":{"terms":{"field":field_name,"size":size}}}}
    mf=build_filters(filters)
    
    # --- TAMBAHKAN BLOK INI ---
    if time_range:
        try:
            range_filter = {
                "range": {
                    time_range["field"]: {
                        "gte": time_range["gte"],
                        "lte": time_range["lte"]
                    }
                }
            }
            mf.append(range_filter)
        except KeyError:
            print("[WARN] Konfigurasi time_range tidak lengkap, diabaikan.")
    # --- AKHIR BLOK TAMBAHAN ---
    
    if mf: q["query"]={"bool":{"filter":mf}}
    return q

def do_request(url, field_name, size, filters, auth, time_range=None):
    body=build_query(field_name, size, filters, time_range=time_range) # <-- UBAH BARIS INI
    return requests.post(url, auth=auth, headers={"Content-Type":"application/json"},
                         data=json.dumps(body), timeout=TIMEOUT, verify=VERIFY_TLS)

def explain_http_error(resp):
    try: err=resp.json()
    except Exception: return "HTTP {}: {}".format(resp.status_code, resp.text[:400])
    if "error" in err:
        e=err.get("error") or {}; typ=e.get("type") or ""; reason=e.get("reason") or ""
        rc=e.get("root_cause") or []; rc_reason=rc[0].get("reason") if rc and isinstance(rc,list) and isinstance(rc[0],dict) else ""
        return "HTTP {} {}: {}".format(resp.status_code, typ, reason or rc_reason or str(e))
    return "HTTP {}: {}".format(resp.status_code, resp.text[:400])

def collect_filters():
    filters=[]; i=1
    print("=== Tambahkan filter (opsional) ===")
    print("- Isi Field & Value. Awali Value dengan '~' untuk 'contains'. Kosongkan Field untuk selesai.")
    while True:
        f=py_input("Filter {} - Field (kosong untuk selesai): ".format(i)).strip()
        if not f: break
        v=py_input("Filter {} - Value (awali ~ untuk contains): ".format(i)).strip()
        if not v:
            print("  [SKIP] Value kosong, filter diabaikan."); continue
        op="contains" if v.startswith("~") else "term"
        if v.startswith("~"): v=v[1:].strip()
        filters.append({"field": f, "value": v, "op": op}); i+=1
    if filters:
        print("[INFO] Filters aktif:")
        for f in filters:
            print("  - {} {} '{}'".format(f["field"], "contains" if f["op"]=="contains" else "=", f["value"]))
    else:
        print("[INFO] Tidak ada filter.")
    return filters

def add_keyword_fallback(filters, agg_field):
    nf=[]
    for f in filters:
        g=dict(f)
        if g["op"]=="term" and not g["field"].endswith(".keyword"):
            g["field"]=g["field"]+".keyword"
        nf.append(g)
    if not agg_field.endswith(".keyword"):
        agg_field=agg_field+".keyword"
    return nf, agg_field

# ====== Template helpers ======
def read_template(p):
    with open(p,"r") as f: return f.read()

def sanitize_final_template(tpl):
    tpl = re.sub(r'%\{\[\s*"([^"]+)"\s*\]\}', r'"\1"', tpl)
    tpl = re.sub(r"%\{\[\s*'([^']+)'\s*\]\}", r'"\1"', tpl)
    tpl = re.sub(r'(%\{\[[^\}]+\]\})\]+', r'\1', tpl)
    tpl = re.sub(r'(%\{\[[^\}]+\]\})\}+', r'\1', tpl)
    tpl = re.sub(r'(%\{\[[^\}]+\]\})\]\}', r'\1', tpl)
    tpl = re.sub(r'=>\s*""([^"]*?)""', r'=> "\1"', tpl)
    tpl = re.sub(r'\[\[([^\[\]]+)\]\]', r'[\1]', tpl)
    tpl = re.sub(r'%\{\[%\{\[', '%{[', tpl)
    tpl = re.sub(r'\]\]\}\}', ']}}', tpl)
    return tpl

def build_dictionary_block(rows):
    return "\n".join('        "{}" => "{}"'.format(r["event_name"].replace('"','\\"'), r["plugin_sid"]) for r in rows)

def build_source_if(log_type, event_field, filters):
    parts=['[@metadata][log_type] == "{}"'.format(log_type), dot_to_brackets(event_field)]
    for f in filters:
        if f.get("op")=="term":
            fld = f["field"].replace(".keyword","")
            parts.append('{} == "{}"'.format(dot_to_brackets(fld), f["value"]))
    return "if " + " and ".join(parts) + " {"

def inject_source_if(t, line):
    pat=re.compile(r'if\s+[^\n]*\[fields\]\[log_type\][^\n]*\{', re.IGNORECASE)
    if pat.search(t): return pat.sub(line, t, 1)
    return t.replace("filter {", "filter {\n  "+line, 1)

def inject_siem_if(t, line):
    pat=re.compile(r'if\s+[^\n]*\[@metadata\]\[siem_plugin_type\][^\n]*\{', re.IGNORECASE)
    if pat.search(t): return pat.sub(line, t, 1)
    return t.replace("filter {", "filter {\n  "+line, 1)

def deep_merge(a, b):
    out = dict(a or {})
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out

def deep_diff(base, new):
    diff = {}
    for k, v in (new or {}).items():
        if k not in base:
            diff[k] = v
        else:
            if isinstance(v, dict) and isinstance(base[k], dict):
                sub = deep_diff(base[k], v)
                if sub:
                    diff[k] = sub
            else:
                if base[k] != v:
                    diff[k] = v
    return diff

# ====== MAPPING COLLECTION ======
def collect_field_mappings(cfg, use_remote_defaults, directive_category=None, full_slug=None):
    mappings = {}
    print("\n=== Isian Field Mapping (H/F) ===")

    # This function handles the new default logic for specified fields.
    def get_mapping_with_defaults(key, default_field=None):
        # Define the special defaults requested by the user
        special_defaults = {
            "sensor": {"mode": "f", "value": "resource.labels.project_id"},
            "product": {"mode": "h", "value": "GCP - Audit"},
            "src_ips": {"mode": "f", "value": "src_ips"},
            "dst_ips": {"mode": "f", "value": "dst_ips"},
            "src_port": {"mode": "f", "value": "src_port"},
            "dst_port": {"mode": "f", "value": "dst_port"},
        }

        m_default = (cfg.get(key + "_mode") or "").lower()
        v_default = cfg.get(key + "_value")

        if (use_remote_defaults or AUTO_USE_CONFIG) and m_default in ("h", "f") and v_default is not None:
            return m_default, v_default

        # Get the special default for the current key, if it exists
        key_defaults = special_defaults.get(key)
        default_mode = key_defaults["mode"] if key_defaults else (m_default if m_default in ("h", "f") else None)

        mode = ask_hf(key, default_mode=default_mode)
        
        while True:
            placeholder = ""
            # Determine the placeholder text based on the mode and available defaults
            if mode == 'f':
                default_val = (key_defaults and key_defaults.get("value")) or default_field
                placeholder = " (default: {})".format(default_val) if default_val else ""
            elif mode == 'h':
                default_val = (key_defaults and key_defaults.get("value"))
                placeholder = " (default: {})".format(default_val) if default_val else ""

            val = py_input("{} value{}: ".format(key, placeholder)).strip()

            # If the user just presses Enter, apply the default
            if not val:
                if mode == 'f':
                    val = (key_defaults and key_defaults.get("value")) or default_field
                elif mode == 'h':
                    val = (key_defaults and key_defaults.get("value"))
            
            if not val:
                print("Field/value tidak boleh kosong.")
                continue
            
            return mode, val

    fields_to_map = [
        ("sensor", None), ("product", None), ("category", None),
        ("subcategory", "subcategory"), ("src_ips", "src_ips"), ("dst_ips", "dst_ips"),
        ("src_port", "src_port"), ("dst_port", "dst_port"), ("protocol", "protocol")
    ]
    for key, default in fields_to_map:
        if key == "category":
            print("[AUTO] Category diisi dari input CATEGORY/TECHNIQUE: {}".format(directive_category))
            mappings[key] = {"mode": "h", "value": directive_category}
        elif key == "subcategory":
            print("[AUTO] Subcategory diisi dari full_slug: {}".format(full_slug))
            mappings[key] = {"mode": "h", "value": full_slug}
        elif key == "protocol":
            print("[AUTO] Protocol diisi otomatis: hardcoded 'TCP'")
            mappings[key] = {"mode": "h", "value": "TCP"}
        else:
            # Use the new function with built-in defaults
            mode, value = get_mapping_with_defaults(key, default_field=default)
            mappings[key] = {"mode": mode, "value": value}


    print("\n=== Custom Data (label = hardcoded, data = field) ===")
    custom_data = {}
    # Define the new defaults for custom data
    custom_defaults = {
        1: {"label": "Severity", "value": "severity"},
        2: {"label": "Project ID", "value": "resource.labels.project_id"},
        3: {"label": "Project Status", "value": "projectstatus"},
    }

    for i in range(1, 4):
        label_key = "custom_label" + str(i)
        data_key = "custom_data" + str(i)
        
        # Get remote config value OR the new default
        label_val = cfg.get(label_key) or custom_defaults[i]["label"]
        data_val = cfg.get(data_key) or custom_defaults[i]["value"]
        
        if use_remote_defaults or AUTO_USE_CONFIG:
            if not label_val or not data_val:
                    raise SystemExit("[CFG ERROR] {}/{} belum ada di config.json".format(label_key, data_key))
            print("[CFG] {} = {}".format(label_key, label_val))
            print("[CFG] {} = {}".format(data_key, data_val))
        else:
            # Prompt the user, using the new default if they enter nothing
            label_val = py_input("{} (literal, default: {}): ".format(label_key, label_val)).strip() or label_val
            data_val = py_input("{} (field, default: {}): ".format(data_key, data_val)).strip() or data_val
        
        custom_data[label_key] = label_val
        custom_data[data_key] = data_val

    ts_in = cfg.get("timestamp_field")
    if not ts_in:
        if use_remote_defaults or AUTO_USE_CONFIG:
            raise SystemExit("[CFG ERROR] timestamp_field belum ada di config.json.")
        ts_in = py_input("Field timestamp (default: timestamp): ").strip() or "timestamp"
    else:
        print("[CFG] timestamp_field (from GitHub): {}".format(ts_in))

    return {"mappings": mappings, "custom": custom_data, "timestamp_field": ts_in}

# ====== VECTOR BUILDER ======
def generate_file_vector_from_template(tsv_path, template_path, out_dir,
                                         log_type_auto, event_field_no_keyword, spt,
                                         filters, field_data, forced_plugin_id,
                                         out_conf_name): # module_slug dihapus dari sini
    
    print("- Template          : {}".format(template_path))
    plugin_id = int(forced_plugin_id)
    if not os.path.exists(template_path): raise SystemExit("[ERROR] Template tidak ditemukan: {}".format(template_path))
    tpl = read_template(template_path)

    # --- Logika Filter VRL (tanpa .type) ---
    vrl_filter_parts = []
    
    # 1. Langsung ke kondisi match() untuk index_name
    vrl_filter_parts.append("match(string!(.index_name), r'(?i){}')".format(log_type_auto))

    # 2. Tambahkan kondisi dari collect_filters
    for f in filters:
        field_name = f["field"]
        if field_name.endswith(".keyword"):
            field_name = field_name[:-8]
        
        field = to_vrl_accessor(field_name)
        value = to_hard_literal(f["value"])

        if f["op"] == "contains":
            condition = 'contains({}, {})'.format(field, value)
        else: # 'term'
            condition = '{} == {}'.format(field, value)
        vrl_filter_parts.append(condition)

    # 3. Tambahkan kondisi exists() untuk field event utama
    vrl_filter_parts.append('exists({})'.format(to_vrl_accessor(event_field_no_keyword)))
    
    vrl_filter = " && ".join(vrl_filter_parts)
    # --- Akhir Logika Filter ---

    tpl = tpl.replace("{siem_plugin_type}", spt)
    tpl = tpl.replace("{log_type}", log_type_auto)
    tpl = tpl.replace("{filter}", vrl_filter)
    
    vector_cfg_out = {
        "plugin_id": plugin_id, 
        "timestamp_field": field_data["timestamp_field"]
    }

    # [PERBAIKAN] Logika untuk menangani integer hardcoded
    integer_fields = ["src_port", "dst_port"]
    for key, val_obj in field_data["mappings"].items():
        mode, value = val_obj["mode"], val_obj["value"]
        placeholder = "{" + key + "}"
        if mode == 'h':
            # Cek apakah field ini harus integer DAN nilainya adalah angka
            if key in integer_fields and (value or "").isdigit():
                # Tulis sebagai angka, tanpa tanda kutip
                tpl = tpl.replace(placeholder, str(value or 0))
            else:
                # Untuk field lain, tetap gunakan kutip
                tpl = tpl.replace(placeholder, to_hard_literal(value))
        else:
            tpl = tpl.replace(placeholder, to_vrl_accessor(value))
        vector_cfg_out[key+"_mode"] = mode
        vector_cfg_out[key+"_value"] = value
        
    for i in range(1, 4):
        label_key = "custom_label" + str(i)
        data_key = "custom_data" + str(i)
        label_val = field_data["custom"].get(label_key)
        data_val = field_data["custom"].get(data_key)
        tpl = tpl.replace("{" + label_key + "}", to_hard_literal(label_val))
        tpl = tpl.replace("{" + data_key + "}", to_vrl_accessor(data_val))
        vector_cfg_out[label_key] = label_val
        vector_cfg_out[data_key] = data_val

    tpl = tpl.replace("{timestamp}", to_vrl_accessor(field_data["timestamp_field"]))
    tpl = tpl.replace("{field_name}", event_field_no_keyword)
    
    out_conf = out_conf_name if out_conf_name else "70_transform_dsiem-plugin-{}.yaml".format(spt)
    out_path = os.path.join(out_dir, out_conf)
    save_text_utf8(out_path, tpl)
    print("\n[OK] Generated Vector config -> {}".format(out_path))
    
    return {
        "plugin_id": plugin_id, "siem_plugin_type": spt, "log_type": log_type_auto,
        "translate_field": event_field_no_keyword, "conf_path": out_path, "tsv_path": tsv_path,
        "vector_cfg": vector_cfg_out
    }

# ====== FILE 70 BUILDER (LOGSTASH) ======
def generate_file70_from_template(tsv_path, template_path, out_dir,
                                  log_type_auto, event_field_no_keyword, spt,
                                  index_base, index_pattern, filters_for_if, 
                                  field_data, forced_plugin_id,
                                  out_conf_name=None):
    field_identifier = dot_to_brackets(event_field_no_keyword)
    print("- Template          : {}".format(template_path))
    plugin_id = int(forced_plugin_id)
    ts_in = field_data["timestamp_field"]
    
    file70_cfg_out = { "plugin_id": int(plugin_id), "timestamp_field": ts_in }

    if not os.path.exists(template_path): raise SystemExit("[ERROR] Template tidak ditemukan: {}".format(template_path))
    tpl = read_template(template_path)

    for key, val_obj in field_data["mappings"].items():
        mode, value = val_obj["mode"], val_obj["value"]
        placeholder = "{" + key + "}"
        if mode == 'h':
            tpl = tpl.replace(placeholder, to_hard_literal(value))
        else:
            # FIX from previous error: ensure value is a string before interpolation
            tpl = tpl.replace(placeholder, field_to_interp(value or ""))
        file70_cfg_out[key+"_mode"] = mode
        file70_cfg_out[key+"_value"] = value
    
    for i in range(1, 4):
        label_key = "custom_label" + str(i)
        data_key = "custom_data" + str(i)
        label_val = field_data["custom"].get(label_key)
        data_val = field_data["custom"].get(data_key)
        tpl = tpl.replace("{" + label_key + "}", to_hard_literal(label_val))
        # FIX from previous error: ensure value is a string before interpolation
        tpl = tpl.replace("{" + data_key + "}", field_to_interp(data_val or ""))
        file70_cfg_out[label_key] = label_val
        file70_cfg_out[data_key] = data_val

    # V-- THIS IS THE FIX FOR THE TRACEBACK --V
    import io
    try:
        with io.open(tsv_path, 'r', encoding='utf-8') as f:
            tsv_content = f.read()
        rows, _ = parse_tsv(tsv_content)
    except Exception as e:
        raise SystemExit("[ERROR] Gagal membaca file TSV {}: {}".format(tsv_path, e))
    # ^-- END OF FIX --^
    
    json_name = "{}_plugin-sids.json".format(spt)
    local_json_path = os.path.join(out_dir, json_name)
    write_json_dictionary(local_json_path, rows)

    final_server_json_path = os.path.join(LOGSTASH_JSON_DICT_DIR, json_name)
    
    tpl = tpl.replace("{dictionary_path}", final_server_json_path.replace("\\", "/"))
    tpl = tpl.replace("{refresh_interval}", str(DICT_REFRESH_INTERVAL))

    tpl = tpl.replace("{log_type}", log_type_auto)
    tpl = tpl.replace("{plugin_id}", str(plugin_id))
    tpl = tpl.replace("{field}", dot_to_brackets(event_field_no_keyword))
    tpl = tpl.replace("{siem_plugin_type}", spt)
    tpl = tpl.replace("{src_index_pattern}", index_pattern)
    
    # FIX for timestamp field replacement
    # The template should use a simple placeholder like {timestamp}
    # And the script should replace it with the correct Logstash accessor
    tpl = tpl.replace("{timestamp}", to_logstash_accessor(ts_in))

    source_if = build_source_if(log_type_auto, event_field_no_keyword, filters_for_if)
    tpl = inject_source_if(tpl, source_if)
    tpl = inject_siem_if(tpl, 'if [@metadata][siem_plugin_type] == "{}" {{'.format(spt))
    
    tpl = sanitize_final_template(tpl)
    out_conf = out_conf_name if out_conf_name else "70_dsiem-plugin_{}.conf".format(spt)
    out_path = os.path.join(out_dir, out_conf)
    save_text_utf8(out_path, tpl)
    print("\n[OK] Generated file 70 -> {}".format(out_path))

    return {
        "plugin_id": int(plugin_id), "siem_plugin_type": spt, "log_type": log_type_auto,
        "translate_field": event_field_no_keyword, "src_index_pattern": "{}-*".format(index_base), "conf_path": out_path,
        "tsv_path": tsv_path, "timestamp_field": ts_in,
        "file70_cfg": file70_cfg_out,
        "json_dict_path": local_json_path
    }

# ====== DIRECTIVE (sorted by SID/ID) ======
def alarm_id(plugin_id, sid):
    return int(plugin_id) * 10000 + int(sid)

def json_safe(s):
    if s is None: return ""
    return s.replace("\\", "\\\\").replace('"', '\\"')

def default_header_from_spt(spt):
    if not spt: return "Alarm"
    return spt.replace("-", " ").title()

def read_tsv_rows(tsv_path):
    with io.open(tsv_path, 'r', encoding='utf-8') as f:
        content = f.read()
    rows, _ = parse_tsv(content)
    return rows

# ====== RULE TEMPLATES ======
def load_directive_rules():
    templates = {}
    rule_file = "directive_rules.json"
    if os.path.exists(rule_file):
        try:
            with open(rule_file, 'r') as f:
                # Menggunakan object_pairs_hook untuk menjaga urutan dari file JSON
                templates = json.load(f, object_pairs_hook=OrderedDict)
        except Exception as e:
            print("[WARN] Gagal load template dari {}: {}".format(rule_file, e))
    return templates

def ask_rule_template():
    all_templates = load_directive_rules()
    print("\n=== PILIHAN TEMPLATE RULES ===")
    print("0. Default template (3 stages)")
    template_keys = sorted(all_templates.keys())
    for i, key in enumerate(template_keys, 1):
        rules = all_templates[key]
        stages = len(rules)
        print("{}. {} ({} stage{})".format(i, key, stages, "s" if stages > 1 else ""))
    print("99. Manual input (buat custom rules)")
    while True:
        choice = py_input("Pilihan template [0-{},99] (default: 0): ".format(len(template_keys))).strip() or "0"
        if choice == "0": return "default", None, None
        elif choice == "99": return "manual", None, None
        elif choice.isdigit() and 1 <= int(choice) <= len(template_keys):
            return "file", None, template_keys[int(choice) - 1]
        else:
            print("Pilihan tidak valid. Pilih 0-{} atau 99".format(len(template_keys)))

def build_manual_rules(plugin_id, title, sid):
    rules = []
    print("\n=== MANUAL RULES CONFIGURATION ===")
    while True:
        try:
            num_stages = int(py_input("Jumlah stage yang diinginkan (1-5): ").strip())
            if 1 <= num_stages <= 5: break
            print("Masukkan angka antara 1-5")
        except ValueError:
            print("Masukkan angka yang valid")
            
    for stage in range(1, num_stages + 1):
        print("\n--- Stage {} ---".format(stage))
        
        occurrence_str = py_input("Occurrence [default: {}]: ".format(10 ** (stage - 1))).strip()
        occurrence = int(occurrence_str) if occurrence_str else 10 ** (stage - 1)
        
        reliability_str = py_input("Reliability [1-10, default: {}]: ".format(min(10, stage * 3))).strip()
        reliability = int(reliability_str) if reliability_str else min(10, stage * 3)
        
        timeout_str = py_input("Timeout (detik) [default: {}]: ".format(3600 * stage)).strip()
        timeout = int(timeout_str) if timeout_str else 3600 * stage
        
        default_from = ":1" if stage > 1 else "ANY"
        from_val = py_input("From [ANY/:1, default: {}]: ".format(default_from)).strip() or default_from
        
        to_val = py_input("To [ANY/:1, default: ANY]: ").strip() or "ANY"

        rule = {"stage": stage, "name": title, "plugin_id": plugin_id, "plugin_sid": [sid], "occurrence": occurrence, "reliability": reliability, "timeout": timeout, "from": from_val, "to": to_val, "port_from": "ANY", "port_to": "ANY", "protocol": "ANY", "type": "PluginRule", "custom_data1": "ANY", "custom_data2": "ANY", "custom_data3": "ANY"}
        rules.append(rule)
        
    return rules

def get_rule_template(template_name="default", plugin_id=None, sid=None, title=""):
    # Menggunakan OrderedDict untuk menjaga urutan field/key
    templates = {
        "default": [
            OrderedDict([
                ("stage", 1),
                ("name", title),
                ("plugin_id", plugin_id),
                ("plugin_sid", [sid]),
                ("occurrence", 1),
                ("reliability", 1),
                ("timeout", 0),
                ("from", "ANY"),
                ("to", "ANY"),
                ("port_from", "ANY"),
                ("port_to", "ANY"),
                ("protocol", "ANY"),
                ("type", "PluginRule"),
                ("custom_data1", "ANY"),
                ("custom_data2", "ANY"),
                ("custom_data3", "ANY")
            ]),
            OrderedDict([
                ("stage", 2),
                ("name", title),
                ("plugin_id", plugin_id),
                ("plugin_sid", [sid]),
                ("occurrence", 10),
                ("reliability", 10),
                ("timeout", 3600),
                ("from", ":1"),
                ("to", "ANY"),
                ("port_from", "ANY"),
                ("port_to", "ANY"),
                ("protocol", "ANY"),
                ("type", "PluginRule"),
                ("custom_data1", "ANY"),
                ("custom_data2", "ANY"),
                ("custom_data3", "ANY")
            ]),
            OrderedDict([
                ("stage", 3),
                ("name", title),
                ("plugin_id", plugin_id),
                ("plugin_sid", [sid]),
                ("occurrence", 10000),
                ("reliability", 10),
                ("timeout", 21600),
                ("from", ":1"),
                ("to", "ANY"),
                ("port_from", "ANY"),
                ("port_to", "ANY"),
                ("protocol", "ANY"),
                ("type", "PluginRule"),
                ("custom_data1", "ANY"),
                ("custom_data2", "ANY"),
                ("custom_data3", "ANY")
            ])
        ]
    }
    return templates.get(template_name, templates["default"])

def order_rule_fields(rule):
    order = ["stage","name","plugin_id","plugin_sid","occurrence","reliability","timeout", "from","to","port_from","port_to","protocol","type","custom_data1","custom_data2","custom_data3"]
    out = OrderedDict()
    for k in order:
        if k in rule: out[k] = rule[k]
    for k, v in rule.items():
        if k not in out: out[k] = v
    return out

def build_directive_entry(plugin_id, header, category, kingdom, disabled_lit, title, sid, rule_template="default", template_file=None, template_id=None):
    alarm_id_val = int(plugin_id) * 10000 + int(sid)
    rules_data = []
    if rule_template == "manual":
        rules_data = build_manual_rules(plugin_id, title, sid)
    elif rule_template == "file" and template_id:
        all_templates = load_directive_rules()
        template_rules = all_templates.get(template_id, [])
        for rule in template_rules:
            # Lakukan substitusi placeholder
            rule_str = json.dumps(rule)
            rule_str = rule_str.replace('"{PLUGIN_ID}"', str(plugin_id))
            rule_str = rule_str.replace('["{SID}"]', '[{}]'.format(sid))
            rule_str = rule_str.replace("{TITLE}", title)
            # Muat kembali sebagai OrderedDict
            processed_rule = json.loads(rule_str, object_pairs_hook=OrderedDict)
            rules_data.append(processed_rule)
    else:
        rules_data = get_rule_template(rule_template, plugin_id, sid, title)
        
    rules_list = [order_rule_fields(r) for r in rules_data]
    
    # --- Perbaikan Kunci: Definisikan urutan outer key secara eksplisit ---
    directive_obj = OrderedDict()
    directive_obj["id"] = alarm_id_val
    directive_obj["name"] = "{}, {}".format(header, title)
    directive_obj["category"] = category
    directive_obj["kingdom"] = kingdom
    directive_obj["priority"] = 3
    directive_obj["all_rules_always_active"] = False
    directive_obj["disabled"] = (disabled_lit.lower() == "true")
    directive_obj["rules"] = rules_list
    
    return directive_obj

def append_or_create_directive(meta_path, cfg_dir, registry, use_remote_defaults=False, out_filename=None):
    with io.open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    tsv_path = meta.get("tsv_path")
    if not tsv_path or not os.path.exists(tsv_path): raise SystemExit("[ERROR] TSV tidak ditemukan: {}".format(tsv_path))
    rows = read_tsv_rows(tsv_path)

    spt = meta.get("siem_plugin_type") or "directive"
    plugin_id_final = meta.get("plugin_id")
    if not plugin_id_final: raise SystemExit("[ERROR] plugin_id tidak ditemukan di meta.json")

    DICTIONARY = { r["event_name"]: int(r["plugin_sid"]) for r in rows }
    pairs = dict_items_sorted_by_sid(DICTIONARY)

    default_header = cfg_dir.get("HEADER") or default_header_from_spt(spt)
    default_category = cfg_dir.get("CATEGORY") or "Lateral Movement"
    default_kingdom  = cfg_dir.get("KINGDOM")  or "Internal Spearphishing"
    default_disabled = cfg_dir.get("DISABLED", True)

    rule_template, template_file, template_id = ask_rule_template()
    
    print("\n=== KONFIGURASI DIRECTIVE ===")
    HEADER = py_input("HEADER (prefix) [default: {}]: ".format(default_header)).strip() or default_header
    if cfg_dir.get("CATEGORY"):
        CATEGORY = cfg_dir["CATEGORY"]
        print("- CATEGORY/TECHNIQUE (from previous input): {}".format(CATEGORY))
    else:
        CATEGORY = py_input("CATEGORY [default: {}]: ".format(default_category)).strip() or default_category
    if cfg_dir.get("KINGDOM"):
        KINGDOM = cfg_dir["KINGDOM"]
        print("- KINGDOM/TACTIC (from previous input): {}".format(KINGDOM))
    else:
        KINGDOM = py_input("KINGDOM/TACTIC [default: {}]: ".format(default_kingdom)).strip() or default_kingdom
        
    DISABLED_ans = py_input("DISABLED? y/n [default: {}]: ".format("y" if default_disabled else "n")).strip().lower()
    DISABLED = default_disabled if not DISABLED_ans else (DISABLED_ans in ("y","yes","true","1"))

    disabled_lit = "true" if DISABLED else "false"
    out_name = out_filename or "directive_{}.json".format(spt)
    out_path = os.path.join(os.path.dirname(os.path.abspath(meta_path)) or ".", out_name)

    if os.path.exists(out_path):
        try:
            with open(out_path, "r", encoding="utf-8") as f2: existing = json.load(f2, object_pairs_hook=OrderedDict)
        except Exception: existing = OrderedDict([("directives", [])])
        if "directives" not in existing or not isinstance(existing["directives"], list): existing["directives"] = []
        existing_ids = set(int(d.get("id", 0)) for d in existing["directives"])
        for title, sid in pairs:
            _id = alarm_id(plugin_id_final, sid)
            if _id in existing_ids: continue
            directive_obj = build_directive_entry(plugin_id_final, HEADER, CATEGORY, KINGDOM, disabled_lit, title, sid, rule_template, template_file, template_id)
            existing["directives"].append(directive_obj)
        existing["directives"].sort(key=lambda d: int(d.get("id", 0)))
        final_obj = existing
    else:
        all_entries = [build_directive_entry(plugin_id_final, HEADER, CATEGORY, KINGDOM, disabled_lit, title, sid, rule_template, template_file, template_id) for (title, sid) in pairs]
        final_obj = OrderedDict([("directives", all_entries)])

    save_json_utf8(out_path, final_obj)
    print("[OK] Saved/Updated directive JSON -> {}".format(out_path))

    return { "HEADER": HEADER, "CATEGORY": CATEGORY, "KINGDOM": KINGDOM, "DISABLED": DISABLED, "RULE_TEMPLATE": rule_template, "TEMPLATE_ID": template_id if rule_template == "file" else None }, out_path

def dict_items_sorted_by_sid(dct):
    pairs = []
    for k, v in dct.items():
        try: sid = int(v)
        except Exception: continue
        pairs.append((k, sid))
    pairs.sort(key=lambda x: x[1])
    return pairs

# ====== Shell helpers (distribute & restart) ======
def run_cmd(cmd, cwd=None):
    try:
        p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=isinstance(cmd, (str,)))
        out, err = p.communicate()
        if out:
            try: sys.stdout.write(out.decode("utf-8"))
            except Exception: sys.stdout.write(str(out))
        if err:
            try: sys.stderr.write(err.decode("utf-8"))
            except Exception: sys.stderr.write(str(err))
        return p.returncode
    except OSError as e:
        print("[EXEC ERROR] {} -> {}".format(cmd, e))
        return 1

def distribute_artifacts(conf_meta, directive_path):
    any_distributed = False
    print("\n=== DISTRIBUTION ===")
    
    conf_path_70 = conf_meta.get("conf_path_70")
    conf_path_vector = conf_meta.get("conf_path_vector")
    
    if conf_path_70 and os.path.exists(conf_path_70):
        ans = ask_yes_no("Distribute file 70 ke Logstash? (y/n): ")
        if ans == "y":
            if not os.path.isdir(LOGSTASH_PIPE_DIR):
                print("[DIST] Directory tujuan tidak ada: {}".format(LOGSTASH_PIPE_DIR))
            else:
                dst = os.path.join(LOGSTASH_PIPE_DIR, os.path.basename(conf_path_70))
                try:
                    shutil.copy2(conf_path_70, dst)
                    print("[DIST] Copied 70 -> {}".format(dst))
                    any_distributed = True
                except Exception as e:
                    print("[DIST ERROR] Copy 70 gagal: {}".format(e))
    else:
        print("[DIST] File 70 (Logstash) tidak tersedia (skip).")

    # Distribute JSON dictionary (optional)
    json_path = conf_meta.get("json_dict_path")
    if json_path and os.path.exists(json_path):
        ans_json = ask_yes_no("Distribute dictionary JSON ke Logstash? (y/n): ")
        if ans_json == "y":
            try:
                # Ensure the destination directory exists
                if not os.path.isdir(LOGSTASH_PIPE_DIR):
                    print("[DIST] Creating directory: {}".format(LOGSTASH_PIPE_DIR))
                    os.makedirs(LOGSTASH_PIPE_DIR)
                
                # Destination path now uses the new constant
                dst_json = os.path.join(LOGSTASH_PIPE_DIR, os.path.basename(json_path))
                shutil.copy2(json_path, dst_json)
                print("[DIST] Copied JSON dict -> {}".format(dst_json))
                any_distributed = True
            except Exception as e:
                print("[DIST ERROR] Copy JSON gagal: {}".format(e))
    else:
        print("[DIST] JSON dictionary tidak tersedia (skip).")

    if conf_path_vector and os.path.exists(conf_path_vector):
        print("[DIST] Konfigurasi Vector '{}' tidak didistribusikan secara otomatis.".format(os.path.basename(conf_path_vector)))
    else:
        print("[DIST] File Vector tidak tersedia (skip).")

    if directive_path and os.path.exists(directive_path):
        ans2 = ask_yes_no("Distribute directive (kubectl cp) ke frontend? (y/n): ")
        if ans2 == "y":
            cmd = ["kubectl","cp", directive_path, "{}:/dsiem/configs/".format(FRONTEND_POD)]
            rc = run_cmd(cmd)
            if rc == 0:
                print("[DIST] kubectl cp directive sukses.")
                any_distributed = True
            else:
                print("[DIST ERROR] kubectl cp directive gagal.")
    else:
        print("[DIST] Directive JSON tidak tersedia (skip).")
    return any_distributed

def maybe_restart_system(did_distribute):
    print("\n=== RESTART / ROLL ===")
    if not did_distribute:
        ans = ask_yes_no("Tidak ada artefak yang didistribusikan. Tetap restart system? (y/n): ")
        if ans != "y": print("[RESTART] Dibatalkan."); return
    else:
        ans = ask_yes_no("Restart system sekarang? (y/n): ")
        if ans != "y": print("[RESTART] Dibatalkan."); return

    if not os.path.isdir(LOGSTASH_HOME): print("[RESTART] LOGSTASH_HOME tidak ada: {} (skip)".format(LOGSTASH_HOME))
    else:
        print("[RESTART] update-config-map.sh ...")
        rc1 = run_cmd(["bash","-lc","cd '{}' && ./update-config-map.sh".format(LOGSTASH_HOME)])
        print("[RESTART] restart-logstash.sh ...")
        rc2 = run_cmd(["bash","-lc","cd '{}' && ./restart-logstash.sh".format(LOGSTASH_HOME)])
        if rc1 != 0 or rc2 != 0: print("[RESTART] Ada error saat update/restart logstash (lanjut delete pod).")

    print("[RESTART] kubectl delete pod {} {}".format(BACKEND_POD, FRONTEND_POD))
    rc = run_cmd(["kubectl","delete","pod", BACKEND_POD, FRONTEND_POD])
    if rc == 0: print("[RESTART] Pod dihapus. Kubernetes akan recreate otomatis.")
    else: print("[RESTART] Gagal menghapus pod (cek kubectl context/permission).")

# ====== MAIN ======
def main():
    require_github()
    ensure_credentials_file(ES_PASSWD_FILE)
    es_user, es_pass = load_credentials(ES_PASSWD_FILE, ES_USER_LOOKUP)

    print("=== SETTINGS ELASTIC/OPENSEARCH ===")
    print("- Host      : {}".format(ES_HOST))
    print("- Verify TLS: {}".format(VERIFY_TLS))
    print("- Timeout   : {}s".format(TIMEOUT))

    # --- Bagian 1: Pengumpulan Input dari Pengguna ---
    device_name = py_input("\nNama device/log source (untuk nama folder, cth: fortigate): ").strip()
    log_type_auto = sanitize(device_name)

    default_index = "{}*".format(log_type_auto)
    index_pattern = py_input("Index pattern untuk query ES (default: {}): ".format(default_index)).strip() or default_index
    if not index_pattern.endswith("*"): 
        index_pattern += "*"
    
    field_name = py_input("Field name untuk terms agg (default event_name): ").strip() or "event_name"
    size_str = py_input("Jumlah maksimum bucket (default 100): ").strip() or "100"
    try:
        size = int(size_str)
    except ValueError:
        size = 100

    index_base = index_pattern.rstrip("*") 
    print("\n[INFO] log_type (untuk folder & filter) : {}".format(log_type_auto))
    print("[INFO] terms agg field input : {}".format(field_name))
    print("[INFO] index pattern (untuk query & file 70) : {}".format(index_pattern))

    guess_mod, guess_sub = guess_module_from_field(field_name)
    module_slug, submodule_slug = guess_mod, guess_sub
    if not AUTO_USE_CONFIG:
        module_slug, submodule_slug = ask_module_pair(guess_mod, guess_sub)
    print("[INFO] GitHub module/submodule : {}/{}".format(module_slug, submodule_slug or "(none)"))

    filters = collect_filters()

    time_range_config = None
    print("\n=== PENGATURAN RENTANG WAKTU (OPSIONAL) ===")
    
    # Minta input jumlah jam
    hours_input = py_input("Masukkan rentang jam terakhir (cth: 1, 4, 24). Kosongkan untuk 'Full Time': ").strip()

    if not hours_input:
        print("[INFO] Rentang waktu tidak diatur. Skrip akan menarik 'Full Time'.")
    else:
        try:
            hours = int(hours_input)
            if hours > 0:
                gte_val = "now-{}h".format(hours)
                time_range_config = {
                    "field": "@timestamp",  # Default field @timestamp
                    "gte": gte_val,
                    "lte": "now"
                }
                print("[INFO] Rentang waktu diaktifkan: {} jam terakhir ({} s/d now).".format(hours, gte_val))
            else:
                print("[WARN] Angka harus lebih besar dari 0. Menarik 'Full Time'.")
        except ValueError:
            print("[WARN] Input '{}' bukan angka. Menarik 'Full Time'.".format(hours_input))

    auth = HTTPBasicAuth(es_user, es_pass)
    url = "{}/{}/_search".format(ES_HOST.rstrip("/"), index_pattern)

    # --- Bagian 2: Query ke OpenSearch dengan Fallback ---
    print("\n[QUERY] Menjalankan agregasi terms untuk field: '{}'".format(field_name))
    r = None
    r2 = None
    query_success = False

    try:
        r = do_request(url, field_name, size, filters, auth, time_range=time_range_config)
        if r.status_code < 300:
            data_check = r.json()
            if data_check.get("aggregations",{}).get("event_names",{}).get("buckets",[]):
                query_success = True
            else:
                print("[WARN] Query sukses (HTTP 200) tapi tidak ada hasil. Akan mencoba fallback .keyword.")
    except requests.exceptions.RequestException as e:
        raise SystemExit("[HTTP ERROR] {}".format(e))
    except (ValueError, KeyError):
         print("[WARN] Query tidak menghasilkan JSON valid atau struktur tidak dikenali. Akan mencoba fallback .keyword.")

    if not query_success:
        alt_filters, alt_field = add_keyword_fallback(filters, field_name)
        if alt_field == field_name and alt_filters == filters:
            if r: raise SystemExit(explain_http_error(r))
            else: raise SystemExit("[QUERY ERROR] Gagal pada percobaan pertama dan tidak bisa fallback.")
        else:
            print("[WARN] Query awal gagal/kosong. Mencoba fallback dengan .keyword...")
            print("[QUERY] Menjalankan agregasi terms untuk field: '{}'".format(alt_field))
            try:
                r2 = do_request(url, alt_field, size, alt_filters, auth, time_range=time_range_config)
                if r2.status_code < 300:
                    data_check = r2.json()
                    if data_check.get("aggregations",{}).get("event_names",{}).get("buckets",[]):
                        r = r2; filters = alt_filters; field_name = alt_field
                        print("[INFO] Berhasil dengan fallback: field -> {}, filter 'term' -> *.keyword".format(field_name))
                        query_success = True
                    else:
                        raise SystemExit("[QUERY ERROR] Query fallback berhasil tapi tidak ada hasil. Cek field dan filter lagi.\n- Percobaan 1: {}\n- Percobaan 2: HTTP {}".format(explain_http_error(r) if r else "Request Error", r2.status_code))
                else:
                    raise SystemExit("[FIELD/QUERY ERROR]\n- Percobaan 1: {}\n- Percobaan 2: {}".format(explain_http_error(r) if r else "Request Error", explain_http_error(r2)))
            except requests.exceptions.RequestException as e:
                raise SystemExit("[HTTP ERROR] (saat retry .keyword) {}".format(e))
            except (ValueError, KeyError):
                raise SystemExit("[PARSE ERROR] Response dari query fallback bukan JSON valid.")

    if not query_success:
        raise SystemExit("[FATAL] Gagal mendapatkan data event setelah mencoba semua fallback.")
    
    print("[INFO] Sukses menggunakan field: '{}'".format(field_name))

    if r.status_code==404: raise SystemExit("[INDEX ERROR] Index pattern '{}' tidak ditemukan.".format(index_pattern))
    if r.status_code>=300: raise SystemExit(explain_http_error(r))
    try: data=r.json()
    except ValueError: raise SystemExit("[PARSE ERROR] Response bukan JSON valid")

    buckets = data.get("aggregations",{}).get("event_names",{}).get("buckets",[])
    if not buckets:
        print("[INFO] Tidak ada bucket dari agregasi. Selesai."); return

    rows=[]; sid=PLUGIN_SID_START
    for b in buckets:
        key=b.get("key")
        if key is None: continue
        rows.append({"plugin_sid": sid, "event_name": key}); sid+=1

    f1_val = filters[0]["value"] if filters else ""
    translate_field_no_kw = field_name.replace(".keyword", "")
    filter1_slug = slug(f1_val)
    
    # --- Bagian 3: Sinkronisasi TSV dan Plugin ID dengan GitHub ---
    print("\n[SYNC] GitHub repo: {}  branch: {}".format(GITHUB_REPO, GITHUB_BRANCH))
    ghp = gh_paths(log_type_auto, module_slug, submodule_slug, filter1_slug)

    print("\n=== INFORMASI DASAR UNTUK PLUGIN & DIRECTIVE ===")
    spt = ghp["full_slug"]
    print("- siem_plugin_type (nama plugin): {}".format(spt))

    registry, reg_sha = gh_load_plugin_registry()
    used_pids = registry_used_set(registry)
    
    if registry.get("used", []):
        print("[REGISTRY] Daftar Plugin ID yang sudah terdaftar:")
        for ent in sorted(registry["used"], key=lambda x: int(x.get("plugin_id", 0))):
            print("  - ID: {:<7} Plugin: {}".format(ent.get("plugin_id", "?"), ent.get("siem_plugin_type", "?")))
    else:
        print("[REGISTRY] Belum ada plugin_id yang terdaftar.")

    existing_pid_for_spt = registry_find_pid_for_spt(registry, spt)
    plugin_id_final = None
    is_new_plugin = True
    if existing_pid_for_spt is not None:
        plugin_id_final = int(existing_pid_for_spt)
        is_new_plugin = False
        print("\n[REGISTRY] Ditemukan plugin_id {} untuk SPT '{}'. Akan digunakan kembali.".format(plugin_id_final, spt))
    else:
        print("\n[REGISTRY] Belum ada plugin_id untuk SPT '{}'.".format(spt))
        while True:
            pid_in = py_input("Masukkan plugin_id BARU (integer, unik): ").strip()
            if not pid_in.isdigit(): print("Masukkan angka ya..."); continue
            chosen_pid = int(pid_in)
            if chosen_pid in used_pids: print("[REGISTRY] plugin_id {} sudah dipakai.".format(chosen_pid)); continue
            plugin_id_final = chosen_pid
            break

    print("\n[SYNC] Target dict TSV: {}".format(ghp["tsv"]))
    new_events = [r["event_name"] for r in rows]
    existing_file = gh_get_file(ghp["tsv"])

    if existing_file is None:
        rows_for_local = rows[:]
        print("[SYNC] Kamus pusat BELUM ada -> create versi pertama.")
    else:
        try: old_text = base64.b64decode(existing_file.get("content","")).decode("utf-8", "replace")
        except Exception: old_text = ""
        existing_rows, _ = parse_tsv(old_text)
        merged_rows, added = merge_dictionary(existing_rows, new_events)
        rows_for_local = merged_rows
        if not added:
            print("[SYNC] Up-to-date. Tidak ada penambahan.")
        else:
            print("[SYNC] Tambahan baru: {}".format(len(added)))
    
    local_tsv = os.path.join(OUT_DIR, "{}_plugin-sids.tsv".format(ghp["full_slug"]))
    
    # --- Bagian 4: Generate Artefak Lokal (TSV, .conf, .yaml, directive, etc.) ---
    cfg_remote = {}
    config_file = gh_get_file(ghp["config"])
    if config_file:
        try:
            cfg_remote = json.loads(base64.b64decode(config_file.get("content","")).decode("utf-8","replace"))
            print("[CFG] config.json ditemukan di GitHub.")
        except Exception: cfg_remote = {}
    else: print("[CFG] Belum ada config di GitHub.")

    directive_cfg_default = cfg_remote.get("directive", {})
    default_category = directive_cfg_default.get("CATEGORY") or "Internal Spearphishing"
    default_kingdom  = directive_cfg_default.get("KINGDOM")  or "Lateral Movement"
    directive_category = py_input("CATEGORY/TECHNIQUE [default: {}]: ".format(default_category)).strip() or default_category
    directive_kingdom  = py_input("KINGDOM/TACTIC [default: {}]: ".format(default_kingdom)).strip() or default_kingdom
    
    write_tsv(local_tsv, rows_for_local, spt, plugin_id_final, directive_category, directive_kingdom)
    print("\n[OK] Saved TSV -> {}".format(local_tsv))
    
    print("\n=== GENERATE KONFIGURASI ===")
    field_data = collect_field_mappings(
        cfg_remote, 
        use_remote_defaults=(AUTO_USE_CONFIG), 
        directive_category=directive_category, 
        full_slug=ghp["full_slug"]
    )

    print("\n--- Generating Logstash (File 70) Configuration ---")
    template_path_70 = py_input("Path template Logstash (default: {}): ".format(DEFAULT_TEMPLATE_PATH)).strip() or DEFAULT_TEMPLATE_PATH
    conf70_name_local = "70_dsiem-plugin_{}.conf".format(ghp["full_slug"])
    conf_meta_70 = generate_file70_from_template(local_tsv, template_path_70, OUT_DIR, log_type_auto, translate_field_no_kw, spt, index_base, index_pattern, filters, field_data, forced_plugin_id=plugin_id_final, out_conf_name=conf70_name_local)

    print("\n--- Generating Vector Configuration ---")
    template_path_vector = py_input("Path template Vector (default: {}): ".format(DEFAULT_VECTOR_TEMPLATE_PATH)).strip() or DEFAULT_VECTOR_TEMPLATE_PATH
    vector_conf_name_local = "70_transform_dsiem-plugin-{}.yaml".format(ghp["full_slug"])
    conf_meta_vector = generate_file_vector_from_template(local_tsv, template_path_vector, OUT_DIR, log_type_auto, translate_field_no_kw, spt, filters, field_data, forced_plugin_id=plugin_id_final, out_conf_name=vector_conf_name_local)
    
    conf_meta = dict(conf_meta_70)
    conf_meta.update({
        'vector_cfg': conf_meta_vector.get('vector_cfg'),
        'conf_path_70': conf_meta_70.get('conf_path'),
        'conf_path_vector': conf_meta_vector.get('conf_path'),
        'json_dict_path': conf_meta_70.get('json_dict_path')
    })
    conf_meta.pop('conf_path', None)
    
    save_json_utf8(META_PATH, conf_meta)
    print("[OK] Metadata disimpan -> {}".format(META_PATH))

    final_directive_config = deep_merge(cfg_remote.get("directive", {}), {"CATEGORY": directive_category, "KINGDOM": directive_kingdom})
    ans2 = ask_yes_no("\nSekarang generate/append directive.json? (y/n): ")
    directive_cfg_out = {}; directive_out_path = None
    if ans2 == "y":
        dir_out_local = "directives_{}_{}.json".format(BACKEND_POD, ghp["full_slug"])
        directive_cfg_out, directive_out_path = append_or_create_directive(META_PATH, final_directive_config, registry, use_remote_defaults=(AUTO_USE_CONFIG), out_filename=dir_out_local)
    
    # --- Bagian 5: Push Semua Artefak ke GitHub ---
    # Push TSV
    if existing_file is None or added:
        tsv_text_to_push = render_tsv(rows_for_local, spt, plugin_id_final, directive_category, directive_kingdom)
        gh_put_file(ghp["tsv"], tsv_text_to_push.encode("utf-8"), "Update dict: {}/{}".format(GITHUB_REPO, ghp["tsv"]), sha=existing_file.get("sha") if existing_file else None)
        print("[PUSH] Uploaded TSV:", ghp["tsv"])

    # Push JSON dictionary
    json_local = conf_meta.get('json_dict_path')
    gh_json_path = ghp.get("json")
    if json_local and gh_json_path and os.path.exists(json_local):
        with io.open(json_local, "r", encoding="utf-8") as jf: json_text = jf.read()
        existing_json = gh_get_file(gh_json_path)
        gh_put_file(gh_json_path, json_text.encode("utf-8"), "Upload JSON dict: {}/{}".format(GITHUB_REPO, gh_json_path), sha=existing_json.get("sha") if existing_json else None)
        print("[PUSH] Uploaded JSON dict:", gh_json_path)

    # Push config.json, .conf, .yaml, directive
    config_payload = { "module": module_slug, "submodule": submodule_slug, "file70": conf_meta.get("file70_cfg", {}), "vector": conf_meta.get("vector_cfg", {}), "directive": directive_cfg_out or cfg_remote.get("directive",{}) }
    gh_put_file(ghp["config"], json.dumps(config_payload, indent=2, ensure_ascii=False).encode("utf-8"), "Update config for {}".format(spt), sha=config_file.get("sha") if config_file else None)
    print("[PUSH] Updated config:", ghp["config"])

    for key, local_path_key, gh_path_key in [("conf70", "conf_path_70", "conf70"), ("vector config", "conf_path_vector", "vector_conf")]:
        local_path = conf_meta.get(local_path_key)
        if local_path and os.path.exists(local_path):
            with open(local_path, "rb") as f: content_bytes = f.read()
            gh_path = ghp[gh_path_key]
            existing_file = gh_get_file(gh_path)
            gh_put_file(gh_path, content_bytes, "Upload {}: {}".format(key, gh_path), sha=existing_file.get("sha") if existing_file else None)
            print("[PUSH] Uploaded {}: {}".format(key, gh_path))

    if directive_out_path and os.path.exists(directive_out_path):
        with open(directive_out_path, "rb") as f: dir_bytes = f.read()
        existing_dir = gh_get_file(ghp["directive"])
        gh_put_file(ghp["directive"], dir_bytes, "Upload directive: {}".format(ghp["directive"]), sha=existing_dir.get("sha") if existing_dir else None)
        print("[PUSH] Uploaded directive:", ghp["directive"])

    # --- Bagian 6: Generate dan Push file updater.json ---
    updater_context = {
        "es_host": ES_HOST, "verify_tls": VERIFY_TLS, "timeout": TIMEOUT,
        "es_passwd_file": ES_PASSWD_FILE, "es_user_lookup": ES_USER_LOOKUP,
        "index_pattern": index_pattern, "translate_field_no_kw": translate_field_no_kw,
        "size": size, "filters": filters, "log_type_auto": log_type_auto,
        "module_slug": module_slug, "submodule_slug": submodule_slug, "filter1_slug": filter1_slug,
        "plugin_id_final": plugin_id_final, "directive_cfg_out": directive_cfg_out,
        "github_repo": GITHUB_REPO, "github_token": GITHUB_TOKEN, "github_branch": GITHUB_BRANCH,
        "template_path_70": template_path_70, "plugin_registry_path": PLUGIN_REGISTRY_PATH,
    }
    updater_filename = "{}_updater.json".format(ghp["full_slug"])
    updater_output_path = os.path.join(OUT_DIR, updater_filename)
    generate_updater_config(updater_output_path, updater_context)

    if os.path.exists(updater_output_path):
        print("\n[PUSH] Mengunggah file updater config ke GitHub...")
        with open(updater_output_path, "rb") as f: updater_bytes = f.read()
        gh_updater_path = ghp["updater_cfg"]
        existing_updater = gh_get_file(gh_updater_path)
        gh_put_file(gh_updater_path, updater_bytes, "Upload/Update updater config: {}".format(gh_updater_path), sha=existing_updater.get("sha") if existing_updater else None)
        print("[PUSH] Berhasil mengunggah: {}".format(gh_updater_path))
    
    # --- Bagian 7: Registrasi Plugin ID Baru (jika ada) ---
    if is_new_plugin:
        registry = registry_append(registry, plugin_id_final, log_type_auto, module_slug, submodule_slug, filter1_slug, spt)
        gh_push_plugin_registry(registry, sha=reg_sha, msg="Add plugin_id {} for {}".format(plugin_id_final, ghp["full_slug"]))
        print("[REGISTRY] plugin_id {} didaftarkan.".format(plugin_id_final))

    # --- Bagian 8: Distribusi Lokal dan Restart ---
    did_dist = distribute_artifacts(conf_meta, directive_out_path)
    maybe_restart_system(did_dist)

if __name__=="__main__":
    main()