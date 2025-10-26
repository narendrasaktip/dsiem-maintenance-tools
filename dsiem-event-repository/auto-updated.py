# -*- coding: utf-8 -*-
from __future__ import print_function
import os, re, sys, json, base64, io, requests, argparse, traceback, subprocess
from requests.auth import HTTPBasicAuth
from collections import OrderedDict
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta

# =========================================================
# CONFIG & ENV VARS
# =========================================================
CFG_PATH = os.getenv("SYNC_CFG", "./auto-updater.json")
DEFAULT_GH_API_VERSION = "2022-11-28"
ES_PASSWD_FILE = os.getenv("ES_PASSWD_FILE")
ES_USER_LOOKUP = os.getenv("ES_USER_LOOKUP")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
EMAIL_SMTP_SERVER = os.getenv("EMAIL_SMTP_SERVER")
EMAIL_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT"))
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")
EMAIL_RECIPIENTS = os.getenv("EMAIL_RECIPIENTS")
# =========================================================

# =========================================================
# PY2/3 COMPATIBILITY
# =========================================================
try: string_types = (basestring,)
except NameError: string_types = (str,)
try: JSONDecodeError = json.JSONDecodeError
except AttributeError: JSONDecodeError = ValueError
try: FileNotFoundError
except NameError: FileNotFoundError = IOError
# =========================================================

# =========================================================
# UTILS (LOGGER, IO, SHELL, SLUG, ETC)
# =========================================================
def ts(): return datetime.utcnow().strftime("%H:%M:%S")
def section(title): print("\n=== [{}] {} ===".format(ts(), title))
def info(msg): print("[{}] {}".format(ts(), msg))
def warn(msg): print("[{}][WARN] {}".format(ts(), msg))
def err(msg): print("[{}][ERROR] {}".format(ts(), msg))
def die(msg, code=2): err(msg); sys.exit(code)

def parse_args():
    # Definisikan parser argumen command line
    ap = argparse.ArgumentParser(description="Auto-update SIEM event dictionary & directives.")
    # Tambahkan argumen --dry-run (opsional, simpan sebagai True jika ada)
    ap.add_argument("--dry-run", action="store_true", help="Simulate without pushing or distributing.")
    # Tambahkan argumen --debug (opsional, simpan sebagai True jika ada)
    ap.add_argument("--debug", action="store_true", help="Enable extra debug logging.")
    # Parsing argumen yang diberikan saat skrip dijalankan
    return ap.parse_args()
# =========================================================

# =========================================================
# LOGGER
# =========================================================
START_TS = datetime.utcnow()

def read_json(path):
    # Gunakan io.open untuk encoding konsisten
    with io.open(path, "r", encoding="utf-8") as f:
        # Gunakan object_pairs_hook untuk menjaga urutan saat dibaca
        return json.load(f, object_pairs_hook=OrderedDict)

def read_text(path):
    with io.open(path, "r", encoding="utf-8") as f: return f.read()

def slug(s):
    if s is None: return ""
    s = s.strip().lower(); s = re.sub(r'[^a-z0-9]+', '-', s); s = re.sub(r'-+', '-', s).strip('-'); return s

def alarm_id(plugin_id, sid): return int(plugin_id) * 10000 + int(sid)

def run_cmd(cmd_list, dry=False):
    # Hanya log, dry run tidak relevan di sini karena hanya dipakai distribute_local
    info("Executing command: {}".format(" ".join(cmd_list)))
    try:
        p = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            warn("Command failed (rc={}):\n{}".format(p.returncode, err.decode('utf-8', 'replace')))
        # Return the actual return code
        return p.returncode
    except OSError as e: err("Failed to run command: {}".format(e)); return 1

def write_json_dictionary(rows):
    """Membuat konten string JSON dari baris TSV, diurutkan berdasarkan SID."""
    pairs = [];
    for r in rows or []:
        ev = r.get("event_name", "").strip();
        if not ev: continue
        try: sid = int(r.get("plugin_sid", 0)); pairs.append((ev, sid))
        except Exception: continue
    pairs.sort(key=lambda x: (x[1], x[0])) # Urutkan by SID, lalu nama
    ordered = OrderedDict((k, v) for k, v in pairs)
    # Kembalikan string, bukan path
    return json.dumps(ordered, ensure_ascii=False, indent=2)
# =========================================================

# =========================================================
# EMAIL FUNCTION
# =========================================================
def send_notification_email(customer_name, header_name, new_events):
    # Cek env var di awal
    if not all([EMAIL_SENDER, EMAIL_APP_PASSWORD, EMAIL_RECIPIENTS]):
        warn("Email environment variables (EMAIL_SENDER, EMAIL_APP_PASSWORD, EMAIL_RECIPIENTS) are not fully set. Skipping email notification.")
        return

    section("Sending Email Notification")
    recipients = [email.strip() for email in EMAIL_RECIPIENTS.split(',') if email.strip()] # Filter email kosong
    if not recipients:
        err("EMAIL_RECIPIENTS is set but contains no valid email addresses. Aborting email.")
        return

    count = len(new_events)
    # Konversi waktu ke WIB (+7 jam)
    now_in_wib = datetime.utcnow() + timedelta(hours=7)
    subject_timestamp = now_in_wib.strftime('%d %b %Y | %H:%M WIB')
    subject = "[New Event] [{}] - {} New Events for {} - ({})".format(customer_name, count, header_name, subject_timestamp)
    detection_time = now_in_wib.strftime('%d %B %Y, %H:%M:%S WIB')

    # Buat baris tabel HTML untuk event baru
    event_rows_html = "".join([
        "<tr><td style='padding: 8px; border: 1px solid #ddd;'>{}</td><td style='padding: 8px; border: 1px solid #ddd;'>{}</td></tr>".format(e["plugin_sid"], e["event_name"])
        for e in new_events # Loop langsung dari list of dicts
    ])

    # Template HTML (gunakan versi lengkapmu di sini)
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
        h1{{font-size:18px !important}} h2{{font-size:15px !important}}
        .p-outer{{padding:16px !important}} .p-inner{{padding:18px !important}}
        .table td,.table th{{padding:8px !important}}
      }}
    </style>
  </head>
  <body style="margin:0; padding:0; background:#F3F5F7; -webkit-text-size-adjust:100%;" bgcolor="#F3F5F7">
    <div style="display:none; font-size:1px; line-height:1px; max-height:0; max-width:0; opacity:0; overflow:hidden;">
      Automated notification: new events have been added to the SIEM directory.
    </div>
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" bgcolor="#F3F5F7" style="background:#F3F5F7;">
      <tr><td align="center" class="p-outer" style="padding:24px;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:640px; border:1px solid #E1E4E8; border-radius:10px; overflow:hidden; background:#FFFFFF;" bgcolor="#FFFFFF">
          <tr><td align="center" style="background:#8B0016; padding:22px 24px; text-align:center;" bgcolor="#8B0016">
            <h1 style="margin:0; font-family:Segoe UI, Roboto, Helvetica, Arial, sans-serif; font-size:20px; line-height:1.35; color:#FFFFFF; text-align:center;">Automated Event Update Report</h1>
          </td></tr>
          <tr><td class="p-inner" style="background:#FFFFFF; padding:24px; color:#111111; font-family:Segoe UI, Roboto, Helvetica, Arial, sans-serif;" bgcolor="#FFFFFF">
            <p style="margin:0 0 12px; font-size:14px; line-height:1.6; color:#111111;">Hello Team,</p>
            <p style="margin:0 0 16px; font-size:14px; line-height:1.6; color:#333333;">The automated system has detected new events that have been successfully added to the SIEM directory.</p>
            <hr style="border:0; border-top:1px solid #E1E4E8; margin:16px 0;">
            <h2 style="margin:0 0 10px; font-size:16px; line-height:1.5; color:#111111;">Detection Summary</h2>
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" class="table" style="border-collapse:collapse; font-size:14px;">
              <tr><td width="36%" style="padding:10px 12px; border:1px solid #D6D9DE; font-weight:600; background:#F7F9FA; color:#111111;" bgcolor="#F7F9FA">Customer</td><td style="padding:10px 12px; border:1px solid #D6D9DE; color:#111111;">{customer}</td></tr>
              <tr><td style="padding:10px 12px; border:1px solid #D6D9DE; font-weight:600; background:#F7F9FA; color:#111111;" bgcolor="#F7F9FA">Plugin</td><td style="padding:10px 12px; border:1px solid #D6D9DE; color:#111111;">{plugin}</td></tr>
              <tr><td style="padding:10px 12px; border:1px solid #D6D9DE; font-weight:600; background:#F7F9FA; color:#111111;" bgcolor="#F7F9FA">New Events Count</td><td style="padding:10px 12px; border:1px solid #D6D9DE; color:#111111;">{count}</td></tr>
              <tr><td style="padding:10px 12px; border:1px solid #D6D9DE; font-weight:600; background:#F7F9FA; color:#111111;" bgcolor="#F7F9FA">Detection Time</td><td style="padding:10px 12px; border:1px solid #D6D9DE; color:#111111;">{time}</td></tr>
            </table>
            <hr style="border:0; border-top:1px solid #E1E4E8; margin:20px 0;">
            <h2 style="margin:0 0 10px; font-size:16px; line-height:1.5; color:#111111;">New Event Details</h2>
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" class="table" style="border-collapse:collapse; font-size:14px;">
              <thead><tr>
                <th align="left" style="padding:10px 12px; border:1px solid #D6D9DE; background:#ECEFF3; color:#111111; font-weight:700;" bgcolor="#ECEFF3">New SID</th>
                <th align="left" style="padding:10px 12px; border:1px solid #D6D9DE; background:#ECEFF3; color:#111111; font-weight:700;" bgcolor="#ECEFF3">Event Name</th>
              </tr></thead>
              <tbody>{event_rows}</tbody>
            </table>
            <p style="margin:16px 0 0; font-size:12px; line-height:1.6; color:#555555;">Note: This list reflects the latest additions detected by the automation.</p>
          </td></tr>
          <tr><td style="background:#F3F5F7; padding:14px 24px; text-align:center; color:#4B5563; font-family:Segoe UI, Roboto, Helvetica, Arial, sans-serif; font-size:12px;" bgcolor="#F3F5F7">
            This is an automated notification from the SIEM Event Auto-Update script.
          </td></tr>
        </table>
      </td></tr>
    </table>
  </body>
</html>
    """.format(
        customer=customer_name, plugin=header_name, count=count,
        time=detection_time, event_rows=event_rows_html
    )

    msg = MIMEMultipart('alternative')
    msg['From'] = EMAIL_SENDER
    msg['To'] = ", ".join(recipients) # Bergabung dengan koma jika multiple
    msg['Subject'] = subject
    msg.attach(MIMEText(body_html, 'html')) # Set content type ke HTML

    server = None
    try:
        # Gunakan env var untuk koneksi
        server = smtplib.SMTP(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT)
        server.starttls() # Aktifkan enkripsi
        server.login(EMAIL_SENDER, EMAIL_APP_PASSWORD)
        server.sendmail(EMAIL_SENDER, recipients, msg.as_string())
        info("Email notification sent successfully to: {}.".format(", ".join(recipients)))
    except smtplib.SMTPAuthenticationError:
        err("Failed to send email: Authentication failed. Check EMAIL_SENDER and EMAIL_APP_PASSWORD.")
    except Exception as e:
        # Tangkap error SMTP lainnya
        err("Failed to send email: {}".format(e))
    finally:
        if server:
            server.quit()
# =========================================================

# =========================================================
# TSV FUNCTIONS
# =========================================================
TSV_HEADER = "plugin\tid\tsid\ttitle\tcategory\tkingdom"
def tsv_render(rows, plugin_label, plugin_id, category, kingdom):
    out = [TSV_HEADER]
    # Sort rows by SID before rendering
    sorted_rows = sorted(rows, key=lambda r: int(r.get("plugin_sid", 0)))
    for r in sorted_rows:
        # Ensure all parts are strings for joining
        out.append(u"{}\t{}\t{}\t{}\t{}\t{}".format(
            unicode(plugin_label or u""), unicode(plugin_id or u""), unicode(r.get("plugin_sid", u"")),
            unicode(r.get("event_name", u"")).replace(u"\t", u" "), # Replace tabs in title
            unicode(category or u""), unicode(kingdom or u"")
        ))
    # Return as a single unicode string with newlines
    return u"\n".join(out) + u"\n"

def tsv_parse(text):
    rows, meta = [], {}
    # Handle potential None or empty text
    if not text: return rows, meta
    lines = text.splitlines();
    if not lines: return rows, meta
    header = lines[0].strip().lower()
    # Only support the new 6-column format now
    if not header.startswith("plugin\t"):
        warn("TSV header does not start with 'plugin\\t'. Assuming invalid format.")
        return rows, meta
    hdr = [h.strip() for h in lines[0].split("\t")]
    for i, line in enumerate(lines[1:]):
        parts = line.strip().split("\t")
        if len(parts) != len(hdr):
            warn("Skipping TSV line {}: Incorrect number of columns (expected {}, got {}).".format(i+2, len(hdr), len(parts)))
            continue
        row_map = dict(zip(hdr, parts))
        try:
            # Validate SID is integer
            sid = int(row_map.get("sid","0"))
            # Basic structure for internal use
            rows.append({"plugin_sid": sid, "event_name": row_map.get("title","")})
        except ValueError:
            warn("Skipping TSV line {}: Invalid SID '{}'.".format(i+2, row_map.get("sid")))
            continue
    return rows, meta # Meta is not currently used from TSV

def tsv_merge(existing_rows, new_event_names):
    rows = list(existing_rows); known = set(r["event_name"] for r in existing_rows)
    # Safely get existing SIDs, default to 0 if invalid
    existing_sids = [int(r.get("plugin_sid", 0)) for r in existing_rows if unicode(r.get("plugin_sid", "")).isdigit()]
    max_sid = max(existing_sids) if existing_sids else 0 # Start from 0 if no valid SIDs
    added_rows = [] # Store dicts {plugin_sid, event_name}
    for ev in new_event_names:
        # Ensure event name is not empty and not already known
        if ev and ev not in known:
            max_sid += 1
            nr = {"plugin_sid": max_sid, "event_name": ev}
            rows.append(nr); known.add(ev); added_rows.append(nr)
    return rows, added_rows, max_sid # Return added_rows as list of dicts
# =========================================================

# =========================================================
# 70.CONF GENERATOR
# =========================================================
def generate_conf70_from_template(template_path, plugin_id, log_type, siem_plugin_type, field_name, category, json_dict_path_on_server):
    if not os.path.exists(template_path): die("Template 70.conf not found: {}".format(template_path))
    tpl = read_text(template_path)
    # Replace placeholders
    tpl = tpl.replace("{plugin_id}", unicode(plugin_id))
    tpl = tpl.replace("{siem_plugin_type}", unicode(siem_plugin_type))
    tpl = tpl.replace("{log_type}", unicode(log_type))
    # Ensure field name is bracketed for Logstash
    logstash_field = u"[{}]".format(field_name.strip(u"[]"))
    tpl = tpl.replace("{field}", logstash_field)
    tpl = tpl.replace("{category}", unicode(category or u""))
    tpl = tpl.replace("{dictionary_path}", unicode(json_dict_path_on_server))
    tpl = tpl.replace("{refresh_interval}", u"60") # Hardcode refresh interval
    return tpl
# =========================================================

# =========================================================
# DIRECTIVES FUNCTIONS
# =========================================================
def load_directive_templates(path="./directive_rules.json"):
    if not os.path.exists(path): die("File directive_rules.json not found.")
    try: return read_json(path)
    except Exception as e: die("Failed to load directive templates: {}".format(e)); return {}

def order_rule_fields(rule):
    order = ["stage","name","plugin_id","plugin_sid","occurrence","reliability","timeout", "from","to","port_from","port_to","protocol","type","custom_data1","custom_data2","custom_data3"]
    out = OrderedDict();
    for k in order:
        if k in rule: out[k] = rule[k]
    for k, v in rule.items(): # Add any extra fields
        if k not in out: out[k] = v
    return out

def build_directive_entry(template_rules, plugin_id, title, sid, header, category, kingdom, disabled=False, priority=3):
    _id = alarm_id(plugin_id, sid)
    # Recursive substitution function
    def subst(obj):
        if isinstance(obj, dict): return OrderedDict((k, subst(v)) for k, v in obj.items()) # Keep order
        if isinstance(obj, list):
             # Special case for plugin_sid array
             if len(obj) == 1 and obj[0] == "{SID}": return [sid] # Replace with integer SID
             return [subst(x) for x in obj]
        # Check specific placeholders first to maintain type
        if obj == "{PLUGIN_ID}": return plugin_id # Return integer
        if obj == "{SID}": return sid # Return integer (should be handled by list case above)
        # General string replacement
        if isinstance(obj, string_types): return obj.replace("{TITLE}", title)
        return obj # Return other types (int, bool) as is
    # Apply substitution and ordering to each rule in the template
    processed_rules = [order_rule_fields(subst(r)) for r in template_rules]
    # Build the main directive object with OrderedDict
    directive_obj = OrderedDict()
    directive_obj["id"] = _id; directive_obj["name"] = u"{}, {}".format(header, title.title()) # Title case name
    directive_obj["category"] = category; directive_obj["kingdom"] = kingdom
    directive_obj["priority"] = priority; directive_obj["all_rules_always_active"] = False
    directive_obj["disabled"] = bool(disabled); directive_obj["rules"] = processed_rules
    return directive_obj

def directive_append(existing_json, template_map, template_id, plugin_id, header, category, kingdom, disabled, rows_to_process):
    # Ensure existing_json is a dict with a 'directives' list
    if not isinstance(existing_json, dict) or "directives" not in existing_json or not isinstance(existing_json.get("directives"), list):
        warn("Existing directive JSON format invalid or missing 'directives' list. Creating new list.")
        existing_json = OrderedDict([("directives", [])]) # Use OrderedDict here too
    directives = existing_json["directives"]; exist_ids = set(d.get("id", 0) for d in directives)
    tpl_rules = template_map.get(template_id)
    if not tpl_rules: die("[Directive] template_id '{}' not found in directive_rules.json.".format(template_id))
    appended, add_count = False, 0
    for r in rows_to_process:
        try: sid = int(r["plugin_sid"]) # Ensure SID is valid int
        except (KeyError, ValueError): warn("Skipping directive: Invalid SID in row {}.".format(r)); continue
        _id = alarm_id(plugin_id, sid)
        if _id in exist_ids: continue # Skip if ID already exists
        # Build new entry
        entry = build_directive_entry(tpl_rules, plugin_id, r.get("event_name",u""), sid, header, category, kingdom, disabled=disabled)
        directives.append(entry); appended, add_count = True, add_count + 1
    # Sort final list by ID if changes were made
    if appended: existing_json["directives"] = sorted(directives, key=lambda x: x.get("id", 0))
    return existing_json, appended, add_count, None # Return updated JSON, flag, count
# =========================================================

# =========================================================
# GITHUB FUNCTIONS
# =========================================================
def gh_headers(token): return {"Accept":"application/vnd.github+json", "Authorization":"Bearer {}".format(token), "X-GitHub-Api-Version": DEFAULT_GH_API_VERSION}
def gh_get(repo, branch, token, path, debug=False):
    url = "https://api.github.com/repos/{}/contents/{}".format(repo, path.replace("\\", "/").lstrip('/')) # Clean path
    try: r = requests.get(url, headers=gh_headers(token), params={"ref": branch}, timeout=60)
    except requests.exceptions.RequestException as e: die("GitHub GET Connection Error: {}".format(e)); return None, None
    if debug: info("GET {} -> {}".format(url, r.status_code))
    if r.status_code == 404: return None, None # File not found is ok
    if r.status_code == 403: warn("GitHub GET Forbidden (403): Check token permissions for '{}'. Path: {}".format(repo, path)); return None, None
    if r.status_code >= 300: die("GitHub GET Error {} {}: {}".format(r.status_code, path, r.text[:200])); return None, None
    try: return r.json(), r.headers.get("x-github-request-id")
    except ValueError: die("GitHub GET Response for {} is not valid JSON.".format(path)); return None, None
def gh_put(repo, branch, token, path, bytes_content, message, sha=None, debug=False, dry=False):
    url = "https://api.github.com/repos/{}/contents/{}".format(repo, path.replace("\\", "/").lstrip('/')) # Clean path
    payload = {"message": message, "content": base64.b64encode(bytes_content).decode("ascii"), "branch": branch}
    if sha: payload["sha"] = sha # SHA is crucial for updates
    if dry: info("[DRY-RUN] PUT {} ({} bytes), sha={}".format(path, len(bytes_content), sha)); return {"sha": "dry_run_sha"} # Simulate success
    try: r = requests.put(url, headers=gh_headers(token), data=json.dumps(payload), timeout=60)
    except requests.exceptions.RequestException as e: die("GitHub PUT Connection Error: {}".format(e)); return {}
    if debug: info("PUT {} -> {}".format(url, r.status_code))
    # Handle specific errors
    if r.status_code == 409: warn("GitHub PUT Conflict (409) for {}: SHA mismatch or branch conflict.".format(path)); return {} # Conflict, maybe retry needed
    if r.status_code == 403: die("GitHub PUT Forbidden (403): Check token permissions for '{}'. Path: {}".format(repo, path)); return {}
    if r.status_code == 422: warn("GitHub PUT Unprocessable (422) for {}: {}".format(path, r.text[:400])); return {} # Often content issues
    if r.status_code >= 300: die("GitHub PUT Error {} {}:\n{}".format(r.status_code, path, r.text[:400])); return {}
    try: return r.json() # Return response which includes new SHA
    except ValueError: die("GitHub PUT Response for {} is not valid JSON.".format(path)); return {}
def gh_paths(log_type, module_name, submodule_name, filter_key, backend_pod="dsiem-backend-0"):
    # Generate slugs safely
    parts = [p for p in [slug(log_type), slug(module_name), slug(submodule_name), slug(filter_key)] if p]
    # Use OrderedDict to keep order and remove duplicates for base_dir/full_slug
    unique_parts = list(OrderedDict.fromkeys(parts)); full_slug = u"-".join(unique_parts)
    base_dir = u"/".join(unique_parts) # Use forward slash for paths
    # Return dictionary with correctly formatted paths
    return { "tsv": u"{}/{}_plugin-sids.tsv".format(base_dir, full_slug),
             "json_dict": u"{}/{}_plugin-sids.json".format(base_dir, full_slug),
             "conf70": u"{}/70_dsiem-plugin_{}.conf".format(base_dir, full_slug),
             "directive": u"{}/directives_{}_{}.json".format(base_dir, backend_pod, full_slug),
             "full_slug": full_slug }
# =========================================================

# =========================================================
# OPENSEARCH FUNCTION
# =========================================================
def load_cred(path, user):
    if not path or not user: die("[CRED] ES_PASSWD_FILE or ES_USER_LOOKUP env var not set.")
    try:
        with io.open(path,"r",encoding="utf-8") as f:
            for ln in f:
                parts = ln.strip().split(":")
                if len(parts) >= 2 and parts[0].strip() == user:
                     pwd = ":".join(parts[1:]).strip()
                     if pwd: return user, pwd # Ensure password is not empty
                     else: die("[CRED] Empty password for user {} in {}".format(user, path))
    except IOError as e: die("[CRED] Cannot read credentials file {}: {}".format(path, e))
    die("[CRED] User '{}' not found in {}".format(user, path))
def fetch_titles(es_cfg, q_cfg, debug=False):
    host, verify, timeout = es_cfg.get("host"), es_cfg.get("verify_tls", False), es_cfg.get("timeout", 3000)
    if not host: die("Elasticsearch host not configured in updater JSON.")
    u,p = load_cred(ES_PASSWD_FILE, ES_USER_LOOKUP)
    auth = HTTPBasicAuth(u,p)
    index, field, size = q_cfg.get("index"), q_cfg.get("field"), int(q_cfg.get("size", 2000))
    if not index or not field: die("Updater JSON missing 'index' or 'field' in query section.")
    # Ensure field for terms agg ends with .keyword if needed
    agg_field = field if field.endswith(".keyword") else field + ".keyword"
    body={"size":0, "aggs":{"event_names":{"terms":{"field": agg_field, "size": size}}}}
    # Build filters
    mf = []
    for f in q_cfg.get("filters", []):
        try: op = f.get("op", "term"); field_name = f["field"]; value = f["value"]
        except KeyError as e: warn("Skipping invalid filter (missing key {}): {}".format(e, f)); continue
        if op == "term": mf.append({"term": {(field_name if field_name.endswith(".keyword") else field_name + ".keyword"): value}})
        elif op == "contains": mf.append({"match_phrase": {field_name: value}})
        else: warn("Filter op '{}' unknown, using term.".format(op)); mf.append({"term": {(field_name if field_name.endswith(".keyword") else field_name + ".keyword"): value}})
    # Add time range filter
    if "time_range" in q_cfg:
        time_cfg = q_cfg["time_range"]
        try: mf.append({"range": {time_cfg["field"]: {"gte": time_cfg["gte"], "lte": time_cfg["lte"]}}})
        except KeyError as e: warn("time_range configuration incomplete (missing key {}), skipped.".format(e))
    # Add filters to query body if any exist
    if mf: body["query"]={"bool":{"filter": mf}}
    # Construct URL and make request
    url = "{}/{}/_search".format(host.rstrip("/"), index)
    if debug: info("OpenSearch Query Body:\n{}".format(json.dumps(body, indent=2)))
    try: r = requests.post(url, auth=auth, headers={"Content-Type":"application/json"}, data=json.dumps(body), timeout=timeout, verify=verify)
    except requests.exceptions.RequestException as e: die("OpenSearch connection error to {}: {}".format(host, e)); return [], "", 0
    # Check response status
    if r.status_code != 200: die("OpenSearch query failed ({}) {}: {}".format(r.status_code, url, r.text[:400]), code=3)
    try: data = r.json()
    except ValueError: die("OpenSearch response from {} is not valid JSON.".format(url)); return [], "", 0
    # Extract buckets safely
    buckets = data.get("aggregations",{}).get("event_names",{}).get("buckets",[])
    # Return list of keys, the aggregation field used, and count
    return [b.get("key",u"") for b in buckets if b.get("key")], agg_field, len(buckets)
# =========================================================

# =========================================================
# DISTRIBUTE LOCAL FUNCTION (Return True jika ada perubahan)
# =========================================================
def distribute_and_update_local(merged_rows, paths, cfg, plugin_id, template_map, template_id, args):
    section("Distribute Local Files (Kubernetes)")
    made_local_changes = False
    # Path ke direktori JSON di server Logstash
    logstash_json_dir = "/root/kubeappl/logstash/configs/pipelines/dsiem-events/dsiem-plugin-json/" # Sesuaikan path ini
    json_filename = os.path.basename(paths["json_dict"])
    logstash_dest_path = os.path.join(logstash_json_dir, json_filename)
    info("Handling Logstash JSON dictionary...")

    # Buat konten JSON baru dari merged_rows
    new_json_content_str = write_json_dictionary(merged_rows);
    try: new_data_obj = json.loads(new_json_content_str)
    except (ValueError, JSONDecodeError): err("Failed to generate valid new JSON content."); return False # Gagal jika JSON baru tidak valid

    # Baca konten JSON lama jika file ada
    existing_data_obj = {}
    if os.path.exists(logstash_dest_path):
        try:
            # Gunakan io.open untuk baca
            with io.open(logstash_dest_path, "r", encoding="utf-8") as f_exist: existing_data_obj = json.load(f_exist)
        except (IOError, JSONDecodeError, ValueError): warn("Failed read old JSON at {}, overwriting.".format(logstash_dest_path))

    # Bandingkan konten (deep compare)
    if new_data_obj != existing_data_obj:
        info("JSON content differs, writing to {}".format(logstash_dest_path))
        if not args.dry_run:
            # Pastikan direktori tujuan ada
            if not os.path.isdir(logstash_json_dir):
                try: os.makedirs(logstash_json_dir); info("Created directory: {}".format(logstash_json_dir))
                except OSError as e: err("Failed create Logstash dir {}: {}".format(logstash_json_dir, e)); return False # Gagal buat dir
            # Tulis file baru
            try:
                # Gunakan io.open untuk tulis
                with io.open(logstash_dest_path, "w", encoding="utf-8") as f:
                    # Handle Py2 unicode vs str
                    try: unicode; f.write(new_json_content_str.decode('utf-8') if isinstance(new_json_content_str, str) else new_json_content_str)
                    except NameError: f.write(new_json_content_str)
                    f.write(u'\n') # Newline di akhir
                made_local_changes = True; info("Logstash JSON dictionary updated.")
            except IOError as e: err("Failed write JSON to {}: {}".format(logstash_dest_path, e)) # Gagal tulis file
        else: info("[DRY-RUN] JSON write skipped."); made_local_changes = True # Anggap berubah di dry run
    else: info("Logstash JSON dictionary already synced.")

    # --- Update Frontend Directive ---
    info("\nHandling dsiem-frontend directive...")
    pod_name = "dsiem-frontend-0"; # Sesuaikan jika perlu
    remote_directive_filename = os.path.basename(paths["directive"])
    remote_path_in_pod = "/dsiem/configs/{}".format(remote_directive_filename) # Path di dalam pod
    local_temp_path = "./{}.temp".format(remote_directive_filename) # File temporary lokal

    info("Fetching existing directive from pod: {}...".format(pod_name))
    # Copy dari pod ke lokal (run_cmd mengembalikan exit code)
    rc = run_cmd(["kubectl", "cp", "{}:{}".format(pod_name, remote_path_in_pod), local_temp_path], dry=args.dry_run)

    existing_dir = OrderedDict([("directives", [])]) # Default: list kosong, pakai OrderedDict
    # Hanya baca jika kubectl cp berhasil DAN file temporary ada
    if rc == 0 and os.path.exists(local_temp_path):
        try:
            existing_dir = read_json(local_temp_path) # read_json sudah pakai OrderedDict
            if not isinstance(existing_dir.get("directives"), list): # Validasi struktur
                 warn("Directive JSON from pod invalid format. Creating new list.")
                 existing_dir = OrderedDict([("directives", [])])
        except Exception as e: # Tangkap semua error baca/parse
            warn("Failed read/parse directive JSON from pod ({}). Creating new list.".format(e))
            existing_dir = OrderedDict([("directives", [])])
    elif rc != 0 and not args.dry_run: # kubectl cp gagal (dan bukan dry run)
         warn("Failed to copy directive from pod (kubectl rc={}). Assuming new.".format(rc))
    elif not os.path.exists(local_temp_path) and not args.dry_run: # File temp tidak ada setelah cp (aneh)
         warn("Temp file {} not found after kubectl cp. Assuming new.".format(local_temp_path))
    else: # Dry run atau file memang tidak ada di pod
        info("Directive file not found in pod or kubectl failed. Assuming new.")

    info("Syncing/Appending directive entries...")
    dircfg = cfg.get('directive', {}) # Ambil directive config, default ke dict kosong
    # Pastikan template_id ada sebelum memanggil directive_append
    if not template_id: err("Missing 'template_id' in directive config."); return made_local_changes # Return status sejauh ini
    # Panggil directive_append
    updated_dir_json, appended, add_count, _ = directive_append( existing_dir, template_map, template_id, plugin_id,
        dircfg.get("HEADER", "Default Header"), dircfg.get("CATEGORY", "Default Category"), dircfg.get("KINGDOM", "Default Kingdom"), # Beri default jika missing
        bool(dircfg.get("DISABLED", False)), merged_rows )

    # Jika ada directive baru ditambahkan
    if appended:
        info("Found {} missing/new directives. Distributing back to pod...".format(add_count))
        temp_write_ok = False
        if not args.dry_run:
            try:
                # Tulis ke file temporary lokal menggunakan io.open
                with io.open(local_temp_path, "w", encoding="utf-8") as f:
                    # Serialize OrderedDict ke string JSON
                    json_str_directive = json.dumps(updated_dir_json, indent=2, ensure_ascii=False)
                    # Tulis string (handle Py2 unicode)
                    try: unicode; f.write(json_str_directive.decode('utf-8') if isinstance(json_str_directive, str) else json_str_directive)
                    except NameError: f.write(json_str_directive)
                    f.write(u'\n') # Newline
                temp_write_ok = True
            except IOError as e: err("Failed write temp directive {}: {}".format(local_temp_path, e))
        else: info("[DRY-RUN] Temp directive write skipped."); temp_write_ok = True

        # Jika penulisan temp berhasil, copy ke pod
        if temp_write_ok:
            # run_cmd mengembalikan exit code
            if run_cmd(["kubectl", "cp", local_temp_path, "{}:{}".format(pod_name, remote_path_in_pod)], dry=args.dry_run) == 0:
                 made_local_changes = True; info("Directive distribution complete.")
            else: err("Failed copy directive to pod.") # Gagal copy ke pod
    else: info("No new directives to add. Local directive already synced.")

    # Hapus file temporary jika ada (dan bukan dry run)
    if os.path.exists(local_temp_path) and not args.dry_run:
        try: os.remove(local_temp_path)
        except OSError as e: warn("Failed remove temp file {}: {}".format(local_temp_path, e))

    # Kembalikan status apakah ada perubahan lokal atau tidak
    return made_local_changes
# =========================================================

# =========================================================
# MAIN FUNCTION (Logic Updated)
# =========================================================
def main():
    args = parse_args()
    section("Load config")
    try: cfg = read_json(CFG_PATH)
    except (FileNotFoundError, IOError): die("Config file '{}' not found.".format(CFG_PATH)); return 1
    except (JSONDecodeError, ValueError): die("Config file '{}' is not valid JSON.".format(CFG_PATH)); return 1

    customer_path = cfg.get("customer_config_path", "./customer.json")
    info("Loading customer config from: {}".format(customer_path))
    try: customer_cfg = read_json(customer_path); cfg.update(customer_cfg)
    except FileNotFoundError: warn("Customer config '{}' not found.".format(customer_path))
    except (JSONDecodeError, ValueError): err("Error decoding customer JSON '{}'.".format(customer_path))
    info("Email config loading from env vars.")

    required_keys = ["es", "query", "layout", "file70", "directive", "github"]
    if not all(k in cfg for k in required_keys):
        missing = [k for k in required_keys if k not in cfg]; die("Config '{}' missing: {}".format(CFG_PATH, ", ".join(missing))); return 1

    es_cfg, q_cfg, layout, file70, dircfg, gh_cfg = cfg["es"], cfg["query"], cfg["layout"], cfg["file70"], cfg["directive"], cfg["github"]

    if not GITHUB_TOKEN: die("GITHUB_TOKEN env var not set.", code=2)

    try: paths = gh_paths(layout.get("device"), layout.get("module"), layout.get("submodule"), layout.get("filter_key"))
    except KeyError as e: die("Layout section missing key: {}".format(e)); return 1
    try: plugin_id = int(file70["plugin_id"])
    except (KeyError, ValueError): die("file70 section missing or invalid 'plugin_id'."); return 1

    siem_plugin_type = paths["full_slug"]
    category, kingdom, disabled = dircfg.get("CATEGORY"), dircfg.get("KINGDOM"), bool(dircfg.get("DISABLED", False)) # Default disabled ke False
    template_id = dircfg.get("template_id")
    if not template_id: die("Directive section missing 'template_id'."); return 1

    env_repo = os.getenv("GITHUB_REPO"); env_branch = os.getenv("GITHUB_BRANCH")
    gh_repo = env_repo if env_repo else gh_cfg.get("repo") # Fallback (seharusnya tidak dipakai)
    gh_branch = env_branch if env_branch else gh_cfg.get("branch", "main") # Fallback (seharusnya tidak dipakai)
    template70_path = gh_cfg.get("template_path", "./template-70.js")
    registry_path = gh_cfg.get("plugin_registry_path", "plugin_id.json")

    if not gh_repo: die("GitHub repo not defined."); return 1

    info("Using GITHUB_REPO: {}".format(gh_repo))
    info("CFG: {}, Repo: {}@{}, Slug: {}".format(CFG_PATH, gh_repo, gh_branch, siem_plugin_type))

    # --- Determine status based on flags ---
    needs_distribution = layout.get("needs_distribution", False) # Default False
    is_active_for_email = False
    active_plugins_file = './active_plugins.json'
    if os.path.exists(active_plugins_file):
        try:
            with io.open(active_plugins_file, 'r', encoding='utf-8') as f: active_list = json.load(f)
            if isinstance(active_list, list) and siem_plugin_type in active_list: is_active_for_email = True
        except (IOError, JSONDecodeError, ValueError): warn("Cannot parse active_plugins.json.")

    if needs_distribution and is_active_for_email: plugin_status = "Distribute (Active + Email)"
    elif needs_distribution and not is_active_for_email: plugin_status = "Distribute (Passive, No Email)"
    else: plugin_status = "Update Only (No Distribute, No Email)"
    info("Plugin Status: {}".format(plugin_status))
    # --- End Status Determination ---

    section("Check Plugin ID Registry")
    reg_obj, _ = gh_get(gh_repo, gh_branch, GITHUB_TOKEN, registry_path, debug=args.debug)
    registry, found_in_reg = {}, False; reg_sha = None
    if reg_obj:
        reg_sha = reg_obj.get("sha")
        try: registry = json.loads(base64.b64decode(reg_obj.get("content","")).decode("utf-8"))
        except (TypeError, ValueError, base64.binascii.Error): warn("Failed parse plugin registry.")
    if not isinstance(registry.get("used"), list): registry["used"] = []
    for item in registry.get("used", []):
        if item.get("siem_plugin_type") == siem_plugin_type:
            try: reg_pid = int(item.get("plugin_id"))
            except (ValueError, TypeError): continue
            if reg_pid == plugin_id: found_in_reg = True; info("Plugin ID {} OK.".format(plugin_id)); break
            else: die("Conflict! Slug '{}' uses ID {}, registry has {}.".format(siem_plugin_type, plugin_id, reg_pid))
    if not found_in_reg and not args.dry_run:
        section("Auto-registering Plugin ID")
        registry["used"].append({"plugin_id": plugin_id, "siem_plugin_type": siem_plugin_type, "by": layout.get("device", "unknown")})
        registry["used"] = sorted(registry["used"], key=lambda x: int(x.get("plugin_id", 0))) # Sort by int
        gh_put(gh_repo, gh_branch, GITHUB_TOKEN, registry_path, json.dumps(registry, indent=2, ensure_ascii=False).encode("utf-8"), # ensure_ascii=False
               "[auto] Register plugin_id {} for {}".format(plugin_id, siem_plugin_type), sha=reg_sha, debug=args.debug, dry=args.dry_run)
        info("Plugin Registry push: OK")
    elif not found_in_reg and args.dry_run: info("[DRY-RUN] Plugin ID {} would be registered.".format(plugin_id))

    section("OpenSearch aggregation")
    titles, _, _ = fetch_titles(es_cfg, q_cfg, debug=args.debug)

    section("Fetch & Merge TSV from GitHub")
    tsv_obj, _ = gh_get(gh_repo, gh_branch, GITHUB_TOKEN, paths["tsv"], debug=args.debug)
    existing_rows, tsv_sha = [], None
    if tsv_obj:
        tsv_sha = tsv_obj.get("sha")
        try: content = base64.b64decode(tsv_obj.get("content", "")).decode("utf-8")
        except (TypeError, base64.binascii.Error): content = ""; warn("Failed decode TSV.")
        existing_rows, _ = tsv_parse(content)
        info("TSV exists (sha={}), rows={}".format(tsv_sha, len(existing_rows)))
    else: info("TSV not found (new file).")
    merged_rows, added_rows, _ = tsv_merge(existing_rows, titles)
    info("Total rows: {}, New events: {}".format(len(merged_rows), len(added_rows)))

    # Push TSV if changed
    if added_rows or not tsv_obj:
        # Gunakan tsv_render yang menghasilkan unicode, lalu encode ke utf-8
        tsv_content_bytes = tsv_render(merged_rows, siem_plugin_type, plugin_id, category, kingdom).encode('utf-8')
        gh_put(gh_repo, gh_branch, GITHUB_TOKEN, paths["tsv"], tsv_content_bytes,
               "[auto] Update TSV for {}".format(siem_plugin_type), sha=tsv_sha, debug=args.debug, dry=args.dry_run)
        info("TSV push: OK")

    # Send Email only if active and new events
    if added_rows and is_active_for_email:
        customer_name = cfg.get("customer_info", {}).get("customer_name", "Default")
        send_notification_email(customer_name, dircfg.get("HEADER", paths["full_slug"]), added_rows)
    elif added_rows: info("Plugin is Passive or Update Only. Skipping email.")

    # Sync GitHub JSON Dictionary
    section("Sync GitHub JSON Dictionary")
    json_obj, _ = gh_get(gh_repo, gh_branch, GITHUB_TOKEN, paths["json_dict"], debug=args.debug)
    new_json_content_str = write_json_dictionary(merged_rows); # Ini sudah string
    try: new_data_obj = json.loads(new_json_content_str)
    except (ValueError, JSONDecodeError): die("Failed generate valid new JSON dict content."); return 1
    existing_data_obj = {}
    if json_obj and json_obj.get("content"):
        try: existing_data_obj = json.loads(base64.b64decode(json_obj.get("content", "")).decode("utf-8"))
        except (JSONDecodeError, TypeError, ValueError, base64.binascii.Error): warn("Failed parse GitHub JSON dict.")
    if new_data_obj != existing_data_obj:
        info("JSON dict differs. Pushing sync...")
        # Encode string ke bytes untuk gh_put
        gh_put(gh_repo, gh_branch, GITHUB_TOKEN, paths["json_dict"], new_json_content_str.encode('utf-8'),
               "[auto] Sync JSON dict for {}".format(siem_plugin_type), sha=json_obj.get("sha") if json_obj else None, debug=args.debug, dry=args.dry_run)
        info("JSON Dict push: OK")
    else: info("JSON Dict already synced.")

    # Sync 70.conf (if missing)
    section("Update 70.conf (if missing)")
    conf70_obj, _ = gh_get(gh_repo, gh_branch, GITHUB_TOKEN, paths["conf70"], debug=args.debug)
    if not conf70_obj:
        info("70.conf missing. Generating and pushing...")
        json_path_on_server = "/root/kubeappl/logstash/configs/pipelines/dsiem-events/dsiem-plugin-json/{}_plugin-sids.json".format(siem_plugin_type)
        field_no_keyword = q_cfg.get("field","").replace(".keyword", "")
        if not field_no_keyword: die("Query field missing in config."); return 1
        device_name = layout.get("device")
        if not device_name: die("Layout device missing in config."); return 1
        conf70_text = generate_conf70_from_template(template70_path, plugin_id, device_name, siem_plugin_type, field_no_keyword, category, json_path_on_server)
        gh_put(gh_repo, gh_branch, GITHUB_TOKEN, paths["conf70"], conf70_text.encode('utf-8'), # Encode unicode ke bytes
               "[auto] Create 70.conf for {}".format(siem_plugin_type), sha=None, debug=args.debug, dry=args.dry_run)
        info("70.conf push: CREATED")
    else: info("70.conf already exists.")

    # Sync GitHub Directives
    section("Sync GitHub Directives")
    template_map = load_directive_templates("./directive_rules.json")
    dir_obj, _ = gh_get(gh_repo, gh_branch, GITHUB_TOKEN, paths["directive"], debug=args.debug)
    existing_dir, dir_sha = (OrderedDict([("directives", [])]), None) # Default pakai OrderedDict
    if dir_obj:
        dir_sha = dir_obj.get("sha")
        try: existing_dir = json.loads(base64.b64decode(dir_obj.get("content","")).decode("utf-8"), object_pairs_hook=OrderedDict) # Baca dengan order
        except (JSONDecodeError, ValueError, TypeError, base64.binascii.Error): warn("Failed parse GitHub directives JSON.")
    if not isinstance(existing_dir.get("directives"), list): existing_dir["directives"] = []

    # Pastikan header ada sebelum append
    directive_header = dircfg.get("HEADER")
    if not directive_header: warn("Directive header missing in config, using slug as fallback."); directive_header = siem_plugin_type

    updated_dir_json, appended, add_count, _ = directive_append(existing_dir, template_map, template_id, plugin_id, directive_header, category, kingdom, disabled, merged_rows)
    if appended:
        info("Directives differ ({} new). Pushing sync...".format(add_count))
        # Encode string JSON (yang sudah unicode) ke bytes
        directive_bytes = json.dumps(updated_dir_json, indent=2, ensure_ascii=False).encode('utf-8')
        gh_put(gh_repo, gh_branch, GITHUB_TOKEN, paths["directive"], directive_bytes,
               "[auto] Sync directives for {}".format(siem_plugin_type), sha=dir_sha, debug=args.debug, dry=args.dry_run)
        info("Directives push: OK")
    else: info("Directives already synced.")

    # --- Conditional Local Distribution & Exit Code ---
    made_local_changes = False # Default

    # Jalankan HANYA jika needs_distribution=True DAN ada event baru/directive baru
    # Kita cek added_rows (event baru) ATAU appended (directive baru ditambahkan ke JSON)
    if needs_distribution and (added_rows or appended):
        info("Distribution enabled and changes detected, running local distribution...")
        made_local_changes = distribute_and_update_local(merged_rows, paths, cfg, plugin_id, template_map, template_id, args)
    elif needs_distribution:
        info("Distribution enabled, but no new events or directive changes. Skipping local distribution.")
    else: # needs_distribution is False
        info("Plugin is 'Update Only'. Skipping local distribution.")

    section("Summary")
    info("DONE.")

    # Exit code 5 HANYA jika distribusi aktif DAN ada perubahan lokal
    if needs_distribution and made_local_changes:
        info("Local changes detected for distributed plugin, signaling for restart.")
        return 5 # Sinyal restart
    else:
        info("No restart needed.")
        return 0 # Normal exit
# =========================================================

# --- Main execution block ---
if __name__ == "__main__":
    exit_code = 99 # Default error code
    try:
        exit_code = main() # Jalankan main() dan simpan exit code-nya
    except SystemExit as e:
        # Tangkap exit code dari sys.exit() atau die()
        exit_code = e.code if isinstance(e.code, int) else 1 # Default ke 1 jika code bukan int
    except Exception as e:
        # Tangkap error tak terduga
        err("Unexpected error: {}".format(e))
        traceback.print_exc() # Cetak traceback lengkap
        # Biarkan exit_code tetap 99
    finally:
        # Keluar dengan exit code yang sudah ditentukan
        sys.exit(exit_code)
# =========================================================