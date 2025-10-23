import os, re, sys, json, base64, io, requests, argparse, traceback, subprocess
from requests.auth import HTTPBasicAuth
from collections import OrderedDict
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta

# =========================================================
# CONFIG
# =========================================================
CFG_PATH = os.getenv("SYNC_CFG", "./auto-updater.json")
DEFAULT_GH_API_VERSION = "2022-11-28"

# Membaca kredensial dari Environment Variables, sama seperti main.py
ES_PASSWD_FILE = os.getenv("ES_PASSWD_FILE")
ES_USER_LOOKUP = os.getenv("ES_USER_LOOKUP")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

# === TAMBAHKAN BLOK INI UNTUK EMAIL ===
EMAIL_SMTP_SERVER = os.getenv("EMAIL_SMTP_SERVER", "smtp.gmail.com")
EMAIL_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT", 587))
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")
EMAIL_RECIPIENTS = os.getenv("EMAIL_RECIPIENTS")
# === AKHIR BLOK TAMBAHAN ===

# =========================================================
# PY2/3 string compat
# =========================================================
try:
    string_types = (basestring,)  # Py2
except NameError:
    string_types = (str,)         # Py3

# =========================================================
# LOGGER
# =========================================================
START_TS = datetime.utcnow() # <-- FIX 1: Menghapus .datetime tambahan

def ts():
    return datetime.utcnow().strftime("%H:%M:%S") # <-- FIX 2: Menghapus .datetime tambahan

def section(title):
    print("\n=== [{}] {} ===".format(ts(), title))

def info(msg):
    print("[{}] {}".format(ts(), msg))

def warn(msg):
    print("[{}][WARN] {}".format(ts(), msg))

def err(msg):
    print("[{}][ERROR] {}".format(ts(), msg))

def die(msg, code=2):
    err(msg)
    sys.exit(code)

# =========================================================
# Fungsi Baru: Notifikasi Email
# =========================================================
# auto-updated.py

def send_notification_email(email_cfg, customer_name, header_name, new_events):
    # 1. Tetap periksa flag 'enabled' dari file konfigurasi (email.json)
    if not email_cfg.get("enabled", False):
        info("Email notifications are disabled in the config file. Skipping.")
        return

    # 2. Validasi bahwa semua variabel environment yang dibutuhkan ada
    if not all([EMAIL_SENDER, EMAIL_APP_PASSWORD, EMAIL_RECIPIENTS]):
        warn("Email environment variables (EMAIL_SENDER, EMAIL_APP_PASSWORD, EMAIL_RECIPIENTS) are not fully set. Skipping email notification.")
        return

    section("Sending Email Notification")
    
    # 3. Ambil penerima dari env var, ubah string "a,b,c" menjadi list ['a', 'b', 'c']
    recipients = [email.strip() for email in EMAIL_RECIPIENTS.split(',')]
    if not recipients:
        err("EMAIL_RECIPIENTS is set but contains no valid email addresses. Aborting email.")
        return
        
    count = len(new_events)
    
    # Pindahkan definisi waktu ke atas agar bisa dipakai untuk subjek dan isi email
    now_in_wib = datetime.utcnow() + timedelta(hours=7)
    
    # 1. Buat format timestamp yang ringkas khusus untuk subjek
    subject_timestamp = now_in_wib.strftime('%d %b %Y | %H:%M WIB')
    
    # 2. Gabungkan timestamp ke dalam string subjek
    subject = "[New Event] [{}] - {} New Events for {} - ({})".format(customer_name, count, header_name, subject_timestamp)
    
    # Variabel ini tetap digunakan untuk isi (body) email agar tetap detail
    detection_time = now_in_wib.strftime('%d %B %Y, %H:%M:%S WIB')
    
    event_rows_html = "".join([
        "<tr><td style='padding: 8px; border: 1px solid #ddd;'>{}</td><td style='padding: 8px; border: 1px solid #ddd;'>{}</td></tr>".format(e["plugin_sid"], e["event_name"])
        for e in new_events
    ])
    
    body_html = """
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Automated Event Update Report</title>
    <meta name="x-apple-disable-message-reformatting">
    <style>
      :root {{ color-scheme: light; supported-color-schemes: light; }} /* force light */
      a[x-apple-data-detectors] {{ color: inherit !important; text-decoration: none !important; }}
      @media only screen and (max-width:600px){{
        h1{{font-size:18px !important}}
        h2{{font-size:15px !important}}
        .p-outer{{padding:16px !important}}
        .p-inner{{padding:18px !important}}
        .table td,.table th{{padding:8px !important}}
      }}
    </style>
  </head>
  <body style="margin:0; padding:0; background:#F3F5F7; -webkit-text-size-adjust:100%;" bgcolor="#F3F5F7">
    <div style="display:none; font-size:1px; line-height:1px; max-height:0; max-width:0; opacity:0; overflow:hidden;">
      Automated notification: new events have been added to the SIEM directory.
    </div>

    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#F3F5F7" style="background:#F3F5F7;">
      <tr>
        <td align="center" class="p-outer" style="padding:24px;">
          
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:640px; border:1px solid #E1E4E8; border-radius:10px; overflow:hidden; background:#FFFFFF;" bgcolor="#FFFFFF">
            <tr>
              <td align="center" style="background:#8B0016; padding:22px 24px; text-align:center;" bgcolor="#8B0016">
                <h1 style="margin:0; font-family:Segoe UI, Roboto, Helvetica, Arial, sans-serif; font-size:20px; line-height:1.35; color:#FFFFFF; text-align:center;">
                  Automated Event Update Report
                </h1>
              </td>
            </tr>

            <tr>
              <td class="p-inner" style="background:#FFFFFF; padding:24px; color:#111111; font-family:Segoe UI, Roboto, Helvetica, Arial, sans-serif;" bgcolor="#FFFFFF">
                <p style="margin:0 0 12px; font-size:14px; line-height:1.6; color:#111111;">Hello Team,</p>
                <p style="margin:0 0 16px; font-size:14px; line-height:1.6; color:#333333;">
                  The automated system has detected new events that have been successfully added to the SIEM directory.
                </p>

                <hr style="border:0; border-top:1px solid #E1E4E8; margin:16px 0;">

                <h2 style="margin:0 0 10px; font-size:16px; line-height:1.5; color:#111111;">Detection Summary</h2>

                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" class="table" style="border-collapse:collapse; font-size:14px;">
                  <tr>
                    <td width="36%" style="padding:10px 12px; border:1px solid #D6D9DE; font-weight:600; background:#F7F9FA; color:#111111;" bgcolor="#F7F9FA">Customer</td>
                    <td style="padding:10px 12px; border:1px solid #D6D9DE; color:#111111;">{customer}</td>
                  </tr>
                  <tr>
                    <td style="padding:10px 12px; border:1px solid #D6D9DE; font-weight:600; background:#F7F9FA; color:#111111;" bgcolor="#F7F9FA">Plugin</td>
                    <td style="padding:10px 12px; border:1px solid #D6D9DE; color:#111111;">{plugin}</td>
                  </tr>
                  <tr>
                    <td style="padding:10px 12px; border:1px solid #D6D9DE; font-weight:600; background:#F7F9FA; color:#111111;" bgcolor="#F7F9FA">New Events Count</td>
                    <td style="padding:10px 12px; border:1px solid #D6D9DE; color:#111111;">{count}</td>
                  </tr>
                  <tr>
                    <td style="padding:10px 12px; border:1px solid #D6D9DE; font-weight:600; background:#F7F9FA; color:#111111;" bgcolor="#F7F9FA">Detection Time</td>
                    <td style="padding:10px 12px; border:1px solid #D6D9DE; color:#111111;">{time}</td>
                  </tr>
                </table>

                <hr style="border:0; border-top:1px solid #E1E4E8; margin:20px 0;">

                <h2 style="margin:0 0 10px; font-size:16px; line-height:1.5; color:#111111;">New Event Details</h2>

                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" class="table" style="border-collapse:collapse; font-size:14px;">
                  <thead>
                    <tr>
                      <th align="left" style="padding:10px 12px; border:1px solid #D6D9DE; background:#ECEFF3; color:#111111; font-weight:700;" bgcolor="#ECEFF3">New SID</th>
                      <th align="left" style="padding:10px 12px; border:1px solid #D6D9DE; background:#ECEFF3; color:#111111; font-weight:700;" bgcolor="#ECEFF3">Event Name</th>
                    </tr>
                  </thead>
                  <tbody>
                    {event_rows}
                  </tbody>
                </table>

                <p style="margin:16px 0 0; font-size:12px; line-height:1.6; color:#555555;">
                  Note: This list reflects the latest additions detected by the automation and may not include previously known events.
                </p>
              </td>
            </tr>

            <tr>
              <td style="background:#F3F5F7; padding:14px 24px; text-align:center; color:#4B5563; font-family:Segoe UI, Roboto, Helvetica, Arial, sans-serif; font-size:12px;" bgcolor="#F3F5F7">
                This is an automated notification from the Event Auto-Update script.
              </td>
            </tr>
          </table>
          </td>
      </tr>
    </table>
    </body>
</html>
    """.format(
        customer=customer_name,
        plugin=header_name,
        count=count,
        time=detection_time,
        event_rows=event_rows_html
    )

    # Setup MIME
    msg = MIMEMultipart('alternative')
    msg['From'] = EMAIL_SENDER
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject
    msg.attach(MIMEText(body_html, 'html'))

    server = None
    try:
        # 4. Gunakan variabel global dari environment untuk koneksi SMTP
        server = smtplib.SMTP(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_APP_PASSWORD)
        server.sendmail(EMAIL_SENDER, recipients, msg.as_string())
        info("Email notification sent successfully to: {}.".format(", ".join(recipients)))
    except Exception as e:
        err("Failed to send email: {}".format(e))
    finally:
        if server:
            server.quit()
# =========================================================
# IO Utils & Shell Helper
# =========================================================
def read_json(path):
    with io.open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def read_text(path):
    with io.open(path, "r", encoding="utf-8") as f:
        return f.read()

def slug(s):
    if s is None: return ""
    s = s.strip()
    s = re.sub(r'[^a-zA-Z0-9]+', '-', s) 
    s = re.sub(r'-+', '-', s).strip('-')
    return s

def alarm_id(plugin_id, sid):
    return int(plugin_id) * 10000 + int(sid)

def run_cmd(cmd_list, dry=False):
    """Menjalankan perintah sebagai subprocess dan mengembalikan return code."""
    info("Executing command: {}".format(" ".join(cmd_list)))
    if dry:
        info("[DRY-RUN] Command not executed.")
        return 0
    try:
        p = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            warn("Command failed with code {}:\n{}".format(p.returncode, err.decode('utf-8', 'replace')))
        return p.returncode
    except OSError as e:
        err("Gagal menjalankan command: {}".format(e))
        return 1
        
def write_json_dictionary(rows):
    """Membuat konten string JSON dari baris TSV, diurutkan berdasarkan SID."""
    pairs = []
    for r in rows or []:
        ev = r.get("event_name", "").strip()
        if not ev: continue
        try:
            sid = int(r.get("plugin_sid", 0))
            pairs.append((ev, sid))
        except Exception:
            continue
    pairs.sort(key=lambda x: (x[1], x[0]))
    ordered = OrderedDict((k, v) for k, v in pairs)
    return json.dumps(ordered, ensure_ascii=False, indent=2)

# =========================================================
# TSV (6 kolom)
# =========================================================
TSV_HEADER = "plugin\tid\tsid\ttitle\tcategory\tkingdom"
def tsv_render(rows, plugin_label, plugin_id, category, kingdom):
    out = [TSV_HEADER]
    sorted_rows = sorted(rows, key=lambda r: int(r.get("plugin_sid", 0)))
    for r in sorted_rows:
        out.append("{}\t{}\t{}\t{}\t{}\t{}".format(
            str(plugin_label or ""), str(plugin_id or ""), str(r.get("plugin_sid", "")),
            str(r.get("event_name", "")).replace("\t", " "), str(category or ""), str(kingdom or "")
        ))
    return "\n".join(out) + "\n"

def tsv_parse(text):
    rows, meta = [], {}
    lines = text.splitlines()
    if not lines: return rows, meta
    header = lines[0].strip().lower()
    if not header.startswith("plugin\t"): return rows, meta
    hdr = [h.strip() for h in lines[0].split("\t")]
    for i, line in enumerate(lines[1:]):
        parts = line.strip().split("\t")
        if len(parts) != len(hdr): continue
        row_map = dict(zip(hdr, parts))
        try:
            sid = int(row_map.get("sid","0"))
            rows.append({"plugin_sid": sid, "event_name": row_map.get("title","")})
        except: continue
    return rows, meta

def tsv_merge(existing_rows, new_event_names):
    rows = list(existing_rows)
    known = set(r["event_name"] for r in existing_rows)
    existing_sids = [int(r.get("plugin_sid", 0)) for r in existing_rows]
    max_sid = max(existing_sids) if existing_sids else 0
    added_rows = []
    for ev in new_event_names:
        if ev not in known:
            max_sid += 1
            nr = {"plugin_sid": max_sid, "event_name": ev}
            rows.append(nr); known.add(ev); added_rows.append(nr)
    return rows, added_rows, max_sid

# =========================================================
# 70.conf Template Generator
# =========================================================
def generate_conf70_from_template(template_path, plugin_id, log_type, siem_plugin_type, field_name, category, json_dict_path_on_server):
    if not os.path.exists(template_path):
        die("Template 70.conf tidak ditemukan di: {}".format(template_path))
    tpl = read_text(template_path)
    tpl = tpl.replace("{plugin_id}", str(plugin_id))
    tpl = tpl.replace("{siem_plugin_type}", siem_plugin_type)
    tpl = tpl.replace("{log_type}", log_type)
    tpl = tpl.replace("{field}", field_name)
    tpl = tpl.replace("{category}", category)
    tpl = tpl.replace("{dictionary_path}", json_dict_path_on_server)
    return tpl

# =========================================================
# Directives (UPDATED)
# =========================================================
def load_directive_templates(path="./directive_rules.json"):
    if not os.path.exists(path): die("File directive_rules.json tidak ditemukan.")
    return read_json(path)

def order_rule_fields(rule):
    """Mengurutkan key di dalam sebuah rule agar sesuai format standar."""
    order = ["stage","name","plugin_id","plugin_sid","occurrence","reliability","timeout", "from","to","port_from","port_to","protocol","type","custom_data1","custom_data2","custom_data3"]
    out = OrderedDict()
    for k in order:
        if k in rule: out[k] = rule[k]
    # Tambahkan key lain jika ada (untuk fleksibilitas)
    for k, v in rule.items():
        if k not in out: out[k] = v
    return out

def build_directive_entry(template_rules, plugin_id, title, sid, header, category, kingdom, disabled=False, priority=3):
    _id = alarm_id(plugin_id, sid)
    
    # --- FUNGSI SUBST YANG DIPERBARUI ---
    def subst(obj):
        if isinstance(obj, dict):
            return {k: subst(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [subst(x) for x in obj]
        
        # Cek placeholder secara spesifik untuk menjaga tipe data
        if obj == "{PLUGIN_ID}":
            return plugin_id  # Mengembalikan integer
        if obj == "{SID}":
            return sid        # Mengembalikan integer
            
        # Jika bukan placeholder khusus, lakukan replace standar untuk string lain
        if isinstance(obj, string_types):
            return obj.replace("{TITLE}", title)
            
        return obj
    # --- AKHIR DARI FUNGSI SUBST YANG DIPERBARUI ---

    # Proses rules: lakukan substitusi LALU urutkan field-nya
    processed_rules = [order_rule_fields(subst(r)) for r in template_rules]
    
    # Buat directive utama dengan OrderedDict untuk menjaga urutan
    directive_obj = OrderedDict()
    directive_obj["id"] = _id
    directive_obj["name"] = "{}, {}".format(header, title.title())
    directive_obj["category"] = category
    directive_obj["kingdom"] = kingdom
    directive_obj["priority"] = priority
    directive_obj["all_rules_always_active"] = False
    directive_obj["disabled"] = bool(disabled)
    directive_obj["rules"] = processed_rules
    
    return directive_obj

def directive_append(existing_json, template_map, template_id, plugin_id, header, category, kingdom, disabled, rows_to_process):
    if not isinstance(existing_json, dict) or "directives" not in existing_json:
        existing_json = {"directives": []}
    directives = existing_json["directives"]
    exist_ids = set(d.get("id", 0) for d in directives)
    tpl_rules = template_map.get(template_id)
    if not tpl_rules: die("[Directive] template_id '{}' tidak ditemukan di directive_rules.json".format(template_id))
    appended, add_count = False, 0
    for r in rows_to_process:
        sid = int(r["plugin_sid"])
        _id = alarm_id(plugin_id, sid)
        if _id in exist_ids: continue
        entry = build_directive_entry(tpl_rules, plugin_id, r["event_name"], sid, header, category, kingdom, disabled=disabled)
        directives.append(entry)
        appended, add_count = True, add_count + 1
    if appended: existing_json["directives"] = sorted(directives, key=lambda x: x.get("id", 0))
    return existing_json, appended, add_count, None

# =========================================================
# GitHub
# =========================================================
def gh_headers(token):
    return {"Accept":"application/vnd.github+json", "Authorization":"Bearer {}".format(token), "X-GitHub-Api-Version": DEFAULT_GH_API_VERSION}
def gh_get(repo, branch, token, path, debug=False):
    url = "https://api.github.com/repos/{}/contents/{}".format(repo, path)
    r = requests.get(url, headers=gh_headers(token), params={"ref": branch}, timeout=60)
    if debug: info("GET {} -> {}".format(url, r.status_code))
    if r.status_code == 404: return None, None
    r.raise_for_status()
    return r.json(), r.headers.get("x-github-request-id")
def gh_put(repo, branch, token, path, bytes_content, message, sha=None, debug=False, dry=False):
    url = "https://api.github.com/repos/{}/contents/{}".format(repo, path)
    payload = {"message": message, "content": base64.b64encode(bytes_content).decode("ascii"), "branch": branch}
    if sha: payload["sha"] = sha
    if dry:
        info("[DRY-RUN] PUT {} ({} bytes), msg='{}' sha={}".format(path, len(bytes_content), message, sha))
        return {}
    r = requests.put(url, headers=gh_headers(token), data=json.dumps(payload), timeout=60)
    if debug: info("PUT {} -> {}".format(url, r.status_code))
    if r.status_code >= 300: die("[GITHUB PUT ERROR] {} {}\n{}".format(r.status_code, path, r.text[:400]))
    return r.json()
def gh_paths(log_type, module_name, submodule_name, filter_key, backend_pod="dsiem-backend-0"):
    parts = [p for p in [slug(log_type), slug(module_name), slug(submodule_name), slug(filter_key)] if p]
    unique_parts = list(OrderedDict.fromkeys(parts))
    full_slug = "-".join(unique_parts)
    base_dir = "/".join(unique_parts)
    return {
        "tsv":        "{}/{}_plugin-sids.tsv".format(base_dir, full_slug),
        "json_dict":  "{}/{}_plugin-sids.json".format(base_dir, full_slug),
        "conf70":     "{}/70_dsiem-plugin_{}.conf".format(base_dir, full_slug),
        "directive":  "{}/directives_{}_{}.json".format(base_dir, backend_pod, full_slug),
        "full_slug":  full_slug
    }
    
# =========================================================
# OpenSearch (UPDATED)
# =========================================================
# =========================================================
# OpenSearch (UPDATED)
# =========================================================
def load_cred(path, user):
    with io.open(path,"r",encoding="utf-8") as f:
        for ln in f:
            parts = ln.strip().split(":")
            if len(parts) >= 2 and parts[0].strip() == user:
                return user, ":".join(parts[1:]).strip()
    die("[CRED] user {} tidak ditemukan di {}".format(user, path))

def fetch_titles(es_cfg, q_cfg, debug=False):
    host, verify, timeout = es_cfg["host"], es_cfg.get("verify_tls", False), es_cfg.get("timeout", 3000)
    u,p = load_cred(ES_PASSWD_FILE, ES_USER_LOOKUP)
    auth = HTTPBasicAuth(u,p)
    index, field, size = q_cfg["index"], q_cfg["field"], int(q_cfg.get("size", 2000))
    agg_field = field if field.endswith(".keyword") else field + ".keyword"
    body={"size":0, "aggs":{"event_names":{"terms":{"field": agg_field, "size": size}}}}

    # --- LOGIKA FILTER YANG DIPERBARUI ---
    mf = [] # 'mf' adalah list untuk semua filter
    
    # 1. Logika untuk 'filters' (ini sudah ada di kodemu)
    for f in q_cfg.get("filters", []):
        op = f.get("op", "term")
        field_name = f["field"]
        value = f["value"]
        if op == "term":
            # Otomatis tambahkan .keyword untuk 'term' agar cocok dengan teks yang tidak dianalisis
            if not field_name.endswith(".keyword"):
                field_name += ".keyword"
            mf.append({"term": {field_name: value}})
        elif op == "contains":
            mf.append({"match_phrase": {field_name: value}})
        else:
            warn("Filter operation '{}' not recognized. Defaulting to 'term'.".format(op))
            if not field_name.endswith(".keyword"):
                field_name += ".keyword"
            mf.append({"term": {field_name: value}})
    
    # 2. === TAMBAHKAN BLOK INI UNTUK MEMBACA 'time_range' ===
    if "time_range" in q_cfg:
        time_cfg = q_cfg["time_range"]
        try:
            # Pastikan semua key yang diperlukan ada
            range_filter = {
                "range": {
                    time_cfg["field"]: {
                        "gte": time_cfg["gte"],
                        "lte": time_cfg["lte"]
                    }
                }
            }
            mf.append(range_filter)
            info("Applying time_range filter: {} from {} to {}".format(time_cfg["field"], time_cfg["gte"], time_cfg["lte"]))
        except KeyError as e:
            warn("Konfigurasi 'time_range' tidak lengkap. Key hilang: {}. Filter waktu dibatalkan.".format(e))
    # === AKHIR DARI BLOK TAMBAHAN ===
            
    # 3. Terapkan semua filter (dari 'filters' dan 'time_range') ke body query
    if mf: 
        body["query"]={"bool":{"filter": mf}}
    # --- AKHIR DARI LOGIKA FILTER BARU ---
    
    url = "{}/{}/_search".format(host.rstrip("/"), index)
    
    if debug:
        info("Mengirim OpenSearch Query Body:\n{}".format(json.dumps(body, indent=2)))
        
    r = requests.post(url, auth=auth, headers={"Content-Type":"application/json"}, data=json.dumps(body), timeout=timeout, verify=verify)
    if r.status_code != 200:
        die("OpenSearch error {}: {}".format(r.status_code, r.text[:400]), code=3)
    buckets = r.json().get("aggregations",{}).get("event_names",{}).get("buckets",[])
    return [b.get("key","") for b in buckets if b.get("key")], agg_field, len(buckets)
# =========================================================
# Fungsi Distribusi & Update LOKAL
# =========================================================
def distribute_and_update_local(merged_rows, paths, cfg, plugin_id, template_map, template_id, args):
    section("Distribute Local Files (Kubernetes)")
    logstash_json_dir = "/root/kubeappl/logstash/configs/pipelines/dsiem-events/dsiem-plugin-json/"
    json_filename = os.path.basename(paths["json_dict"])
    logstash_dest_path = os.path.join(logstash_json_dir, json_filename)
    info("Handling Logstash JSON dictionary...")
    if not os.path.isdir(logstash_json_dir) and not args.dry_run:
        try: os.makedirs(logstash_json_dir)
        except OSError as e: err("Gagal membuat direktori Logstash: {}".format(e)); return
    info("Writing JSON dictionary to {}".format(logstash_dest_path))
    if not args.dry_run:
        with io.open(logstash_dest_path, "w", encoding="utf-8") as f: f.write(write_json_dictionary(merged_rows))
    info("Logstash JSON dictionary updated.")
    info("\nHandling dsiem-frontend directive...")
    pod_name = "dsiem-frontend-0"
    remote_directive_filename = os.path.basename(paths["directive"])
    remote_path_in_pod = "/dsiem/configs/{}".format(remote_directive_filename)
    local_temp_path = "./{}.temp".format(remote_directive_filename)
    info("Fetching existing directive from pod: {}...".format(pod_name))
    rc = run_cmd(["kubectl", "cp", "{}:{}".format(pod_name, remote_path_in_pod), local_temp_path], dry=args.dry_run)
    existing_dir = {"directives": []}
    if rc == 0 and os.path.exists(local_temp_path):
        try: existing_dir = read_json(local_temp_path)
        except: warn("Gagal membaca directive JSON dari pod, akan membuat file baru.")
    else: info("Directive file not found in pod. Assuming new plugin.")
    
    info("Syncing/Appending directive entries based on full TSV list...")
    dircfg = cfg['directive']
    updated_dir_json, appended, add_count, _ = directive_append(
        existing_dir, template_map, template_id, plugin_id, 
        dircfg["HEADER"], dircfg["CATEGORY"], dircfg["KINGDOM"], 
        bool(dircfg.get("DISABLED", False)), merged_rows
    )
    
    if appended:
        info("Found {} missing/new directives. Distributing back to pod...".format(add_count))
        if not args.dry_run:
            with io.open(local_temp_path, "w", encoding="utf-8") as f: f.write(json.dumps(updated_dir_json, indent=2, ensure_ascii=False))
        run_cmd(["kubectl", "cp", local_temp_path, "{}:{}".format(pod_name, remote_path_in_pod)], dry=args.dry_run)
        info("Directive distribution complete.")
    else: info("No new directives to add. Local file is already in sync.")
    if os.path.exists(local_temp_path): os.remove(local_temp_path)

# =========================================================
# CLI & MAIN
# =========================================================
def parse_args():
    ap = argparse.ArgumentParser(description="auto-update full-sync")
    ap.add_argument("--dry-run", action="store_true", help="simulate everything")
    ap.add_argument("--debug", action="store_true", help="extra debug logs")
    return ap.parse_args()

# Di file: auto-updated.py

def main():
    args = parse_args()
    section("Load config")
    cfg = read_json(CFG_PATH)

    # 1. Muat file konfigurasi customer
    customer_path = cfg.get("customer_config_path", "./customer.json")
    info("Loading customer config from: {}".format(customer_path))
    try:
        customer_cfg = read_json(customer_path)
        cfg.update(customer_cfg)
    except FileNotFoundError:
        warn("Customer config file not found at '{}'. Using default values.".format(customer_path))
    except json.JSONDecodeError:
        err("Error decoding JSON from '{}'. Please check its format.".format(customer_path))

    # 2. Muat file konfigurasi email
    email_path = cfg.get("email_config_path", "./email.json")
    info("Loading email config from: {}".format(email_path))
    try:
        email_cfg = read_json(email_path)
        cfg.update(email_cfg)
    except FileNotFoundError:
        warn("Email config file not found at '{}'. Email notifications will be disabled.".format(email_path))
    except json.JSONDecodeError:
        err("Error decoding JSON from '{}'. Please check its format.".format(email_path))
    
    es_cfg, q_cfg, layout, file70, dircfg, gh_cfg = cfg["es"], cfg["query"], cfg["layout"], cfg["file70"], cfg["directive"], cfg["github"]
    
    gh_token = GITHUB_TOKEN
    if not gh_token:
        die("Environment variable GITHUB_TOKEN belum di-set atau kosong. Harap jalankan 'source config.sh' terlebih dahulu.", code=2)
    
    paths = gh_paths(layout["device"], layout["module"], layout.get("submodule"), layout.get("filter_key"))
    siem_plugin_type, plugin_id = paths["full_slug"], int(file70["plugin_id"])
    category, kingdom, disabled, template_id = dircfg.get("CATEGORY"), dircfg.get("KINGDOM"), bool(dircfg.get("DISABLED")), dircfg.get("template_id")
    
    # --- [ INI BAGIAN YANG DIUBAH ] ---
    # Prioritaskan Environment Variable dari config.sh
    env_repo = os.getenv("GITHUB_REPO")
    env_branch = os.getenv("GITHUB_BRANCH")

    # Gunakan Env Var jika ada, jika tidak, baru baca dari file _updater.json
    gh_repo = env_repo if env_repo else gh_cfg["repo"]
    gh_branch = env_branch if env_branch else gh_cfg.get("branch", "main")
    
    # Sisa config tetap dari file JSON
    template70_path = gh_cfg.get("template_path", "./template-70.js")
    registry_path = gh_cfg.get("plugin_registry_path", "plugin_id.json") # Default value

    # Tambahkan log untuk konfirmasi
    if env_repo:
        info("Using GITHUB_REPO from environment: {}".format(gh_repo))
    else:
        info("Using GITHUB_REPO from config file: {}".format(gh_repo))
    # --- [ AKHIR BAGIAN YANG DIUBAH ] ---

    info("CFG: {}, Repo: {}@{}, Slug: {}".format(CFG_PATH, gh_repo, gh_branch, siem_plugin_type))
    
    section("Check Plugin ID Registry")
    # (Sisa fungsi main() tetap sama...)
    reg_obj, reg_sha = gh_get(gh_repo, gh_branch, gh_token, registry_path, debug=args.debug)
    registry, found_in_reg = {}, False
    if reg_obj: registry = json.loads(base64.b64decode(reg_obj.get("content","")).decode("utf-8"))
    for item in registry.get("used", []):
        if item.get("siem_plugin_type") == siem_plugin_type:
            if int(item.get("plugin_id")) == plugin_id: info("Plugin ID {} untuk '{}' sudah terdaftar. OK.".format(plugin_id, siem_plugin_type)); found_in_reg = True; break
            else: die("Konflik! Slug '{}' terdaftar dengan ID {}, tapi config memakai {}.".format(siem_plugin_type, item.get("plugin_id"), plugin_id))

    section("OpenSearch aggregation")
    titles, agg_field_used, bucket_count = fetch_titles(es_cfg, q_cfg, debug=args.debug)
    info("Agg field used: '{}'".format(agg_field_used))
    info("Buckets found: {}".format(bucket_count))
    info("Titles (sample 5): {}".format(titles[:5]))

    section("Fetch & Merge TSV from GitHub")
    tsv_obj, _ = gh_get(gh_repo, gh_branch, gh_token, paths["tsv"], debug=args.debug)
    existing_rows, tsv_sha = [], None
    if tsv_obj:
        tsv_sha = tsv_obj.get("sha")
        content = base64.b64decode(tsv_obj.get("content", "")).decode("utf-8")
        existing_rows, _ = tsv_parse(content)
        info("TSV exists (sha={}), rows={}".format(tsv_sha, len(existing_rows)))
    else:
        info("TSV not found (new file).")
    merged_rows, added_rows, _ = tsv_merge(existing_rows, titles)
    info("Total rows after merge: {}, New titles from OpenSearch: {}".format(len(merged_rows), len(added_rows)))

    # --- PANGGIL FUNGSI EMAIL DI SINI ---
    if added_rows:
        # Cek apakah plugin ini terdaftar sebagai 'aktif'
        is_active_plugin = False
        active_plugins_file = './active_plugins.json'
        if os.path.exists(active_plugins_file):
            try:
                with open(active_plugins_file, 'r') as f:
                    active_list = json.load(f)
                if siem_plugin_type in active_list:
                    is_active_plugin = True
                    info("Plugin '{}' is marked as ACTIVE. Email notification will be sent.".format(siem_plugin_type))
                else:
                    info("Plugin '{}' is PASSIVE. Skipping email notification.".format(siem_plugin_type))
            except (IOError, json.JSONDecodeError) as e:
                warn("Could not read or parse active_plugins.json: {}. Assuming PASSIVE.".format(e))
        else:
            info("active_plugins.json not found. Assuming all plugins are PASSIVE.")

        # Email hanya dikirim jika ada baris baru DAN plugin iniaktif DAN email diaktifkan secara global
        email_cfg = cfg.get("email_notifications", {})
        if is_active_plugin and email_cfg.get("enabled", False):
            customer_name = "Default Customer"
            customer_info = cfg.get("customer_info", {})
            customer_name = customer_info.get("customer_name", customer_name)
            
            send_notification_email(email_cfg, customer_name, dircfg.get("HEADER", paths["full_slug"]), added_rows)
    # --- AKHIR DARI PEMANGGILAN EMAIL ---
    if added_rows or not tsv_obj:
        gh_put(gh_repo, gh_branch, gh_token, paths["tsv"], tsv_render(merged_rows, siem_plugin_type, plugin_id, category, kingdom).encode("utf-8"),
               "[auto] Update TSV for {}".format(siem_plugin_type), sha=tsv_sha, debug=args.debug, dry=args.dry_run)
        info("TSV push: OK")
    
    section("Sync GitHub JSON Dictionary")
    json_obj, _ = gh_get(gh_repo, gh_branch, gh_token, paths["json_dict"], debug=args.debug)

    # 1. Buat konten JSON baru dan muat sebagai objek Python
    new_json_content_str = write_json_dictionary(merged_rows)
    new_data_obj = json.loads(new_json_content_str)

    # 2. Muat konten JSON yang ada dari GitHub (jika ada) sebagai objek Python
    existing_data_obj = {}
    if json_obj and json_obj.get("content"):
        try:
            existing_json_content_str = base64.b64decode(json_obj.get("content", "")).decode("utf-8")
            existing_data_obj = json.loads(existing_json_content_str)
        except (json.JSONDecodeError, TypeError):
            warn("Gagal mem-parsing konten JSON dari GitHub. Akan dianggap sebagai perubahan.")

    # 3. Bandingkan kedua objek Python, bukan string mentah
    if new_data_obj != existing_data_obj:
        info("Konten JSON berbeda. Melakukan push sinkronisasi penuh ke GitHub...")
        gh_put(gh_repo, gh_branch, gh_token, paths["json_dict"], new_json_content_str.encode("utf-8"),
               "[auto] Sync JSON dictionary for {}".format(siem_plugin_type), sha=json_obj.get("sha") if json_obj else None, debug=args.debug, dry=args.dry_run)
        info("JSON Dict push: OK (Konten disinkronkan)")
    else:
        info("JSON Dict sudah sinkron dengan data TSV. Tidak ada perubahan.")

    section("Update 70.conf (Template-based)")
    conf70_obj, _ = gh_get(gh_repo, gh_branch, gh_token, paths["conf70"], debug=args.debug)
    if not conf70_obj:
        json_path_on_server = "/etc/logstash/pipelines/dsiem-events/dsiem-plugin-json/{}_plugin-sids.json".format(siem_plugin_type)
        conf70_text = generate_conf70_from_template(template70_path, plugin_id, layout["device"], siem_plugin_type, q_cfg["field"].replace(".keyword", ""), category, json_path_on_server)
        gh_put(gh_repo, gh_branch, gh_token, paths["conf70"], conf70_text.encode("utf-8"),
               "[auto] Create 70.conf for {}".format(siem_plugin_type), sha=None, debug=args.debug, dry=args.dry_run)
        info("70.conf push: CREATED")
    
    section("Sync GitHub Directives")
    template_map = load_directive_templates("./directive_rules.json")
    dir_obj, _ = gh_get(gh_repo, gh_branch, gh_token, paths["directive"], debug=args.debug)
    existing_dir, dir_sha = ({"directives":[]}, None) if not dir_obj else (json.loads(base64.b64decode(dir_obj.get("content","")).decode("utf-8")), dir_obj.get("sha"))
    
    updated_dir_json, appended, add_count, _ = directive_append(existing_dir, template_map, template_id, plugin_id, dircfg["HEADER"], category, kingdom, disabled, merged_rows)
    if appended:
        info("Directive content mismatch. Found {} missing/new entries. Pushing full sync to GitHub...".format(add_count))
        gh_put(gh_repo, gh_branch, gh_token, paths["directive"], json.dumps(updated_dir_json, indent=2, ensure_ascii=False).encode("utf-8"),
               "[auto] Sync directives for {}".format(siem_plugin_type), sha=dir_sha, debug=args.debug, dry=args.dry_run)
        info("Directives push: Synced {} entries".format(add_count))
    else:
        info("Directives are already in sync with TSV.")

    if not found_in_reg:
        section("Update Plugin ID Registry")
        if "used" not in registry: registry["used"] = []
        registry["used"].append({"plugin_id": plugin_id, "siem_plugin_type": siem_plugin_type, "by": layout["device"]})
        registry["used"] = sorted(registry["used"], key=lambda x: x.get("plugin_id", 0))
        gh_put(gh_repo, gh_branch, gh_token, registry_path, json.dumps(registry, indent=2).encode("utf-8"),
               "[auto] Register plugin_id {} for {}".format(plugin_id, siem_plugin_type), sha=reg_sha, debug=args.debug, dry=args.dry_run)
        info("Plugin Registry push: OK")

    distribute_and_update_local(merged_rows, paths, cfg, plugin_id, template_map, template_id, args)

    section("Summary")
    info("DONE.")
    return 0

if __name__ == "__main__":
    try: sys.exit(main())
    except Exception as e:
        err("Unexpected error: {}".format(e)); traceback.print_exc(); sys.exit(99)