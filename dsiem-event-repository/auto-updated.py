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
# CONFIG
# =========================================================
CFG_PATH = os.getenv("SYNC_CFG", "./auto-updater.json")
DEFAULT_GH_API_VERSION = "2022-11-28"

# Membaca kredensial dari Environment Variables
ES_PASSWD_FILE = os.getenv("ES_PASSWD_FILE")
ES_USER_LOOKUP = os.getenv("ES_USER_LOOKUP")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

# Variabel Email dibaca dari Environment
EMAIL_SMTP_SERVER = os.getenv("EMAIL_SMTP_SERVER", "smtp.gmail.com")
EMAIL_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT", 587))
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")
EMAIL_RECIPIENTS = os.getenv("EMAIL_RECIPIENTS")
# =========================================================

# =========================================================
# PY2/3 string & error compat
# =========================================================
try:
    string_types = (basestring,)
except NameError:
    string_types = (str,)
try:
    JSONDecodeError = json.JSONDecodeError
except AttributeError:
    JSONDecodeError = ValueError
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError
# =========================================================

# =========================================================
# LOGGER
# =========================================================
START_TS = datetime.utcnow()
def ts(): return datetime.utcnow().strftime("%H:%M:%S")
def section(title): print("\n=== [{}] {} ===".format(ts(), title))
def info(msg): print("[{}] {}".format(ts(), msg))
def warn(msg): print("[{}][WARN] {}".format(ts(), msg))
def err(msg): print("[{}][ERROR] {}".format(ts(), msg))
def die(msg, code=2): err(msg); sys.exit(code)
# =========================================================

# =========================================================
# Fungsi Notifikasi Email
# =========================================================
def send_notification_email(customer_name, header_name, new_events):
    if not all([EMAIL_SENDER, EMAIL_APP_PASSWORD, EMAIL_RECIPIENTS]):
        warn("Email environment variables not fully set. Skipping email.")
        return
    section("Sending Email Notification")
    recipients = [email.strip() for email in EMAIL_RECIPIENTS.split(',') if email.strip()]
    if not recipients:
        err("EMAIL_RECIPIENTS contains no valid addresses. Aborting email.")
        return
    count = len(new_events)
    now_in_wib = datetime.utcnow() + timedelta(hours=7)
    subject_timestamp = now_in_wib.strftime('%d %b %Y | %H:%M WIB')
    subject = "[New Event] [{}] - {} New Events for {} - ({})".format(customer_name, count, header_name, subject_timestamp)
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
        customer=customer_name, plugin=header_name, count=count,
        time=detection_time, event_rows=event_rows_html
    )
    msg = MIMEMultipart('alternative')
    msg['From'] = EMAIL_SENDER
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject
    msg.attach(MIMEText(body_html, 'html'))
    server = None
    try:
        server = smtplib.SMTP(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_APP_PASSWORD)
        server.sendmail(EMAIL_SENDER, recipients, msg.as_string())
        info("Email notification sent successfully to: {}.".format(", ".join(recipients)))
    except Exception as e:
        err("Failed to send email: {}".format(e))
    finally:
        if server: server.quit()
# =========================================================

# =========================================================
# IO Utils & Shell Helper
# =========================================================
def read_json(path):
    with io.open(path, "r", encoding="utf-8") as f:
        return json.load(f, object_pairs_hook=OrderedDict) # Baca dengan urutan
def read_text(path):
    with io.open(path, "r", encoding="utf-8") as f:
        return f.read()
def slug(s):
    if s is None: return ""
    s = s.strip().lower(); s = re.sub(r'[^a-z0-9]+', '-', s); s = re.sub(r'-+', '-', s).strip('-'); return s
def alarm_id(plugin_id, sid): return int(plugin_id) * 10000 + int(sid)
def run_cmd(cmd_list, dry=False):
    info("Executing command: {}".format(" ".join(cmd_list)))
    if dry: info("[DRY-RUN] Command not executed."); return 0 # Return 0 for success in dry run
    try:
        p = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            warn("Command failed with code {}:\n{}".format(p.returncode, err.decode('utf-8', 'replace')))
        # Return the actual return code
        return p.returncode
    except OSError as e:
        err("Gagal menjalankan command: {}".format(e)); return 1
def write_json_dictionary(rows):
    pairs = []
    for r in rows or []:
        ev = r.get("event_name", "").strip()
        if not ev: continue
        try: sid = int(r.get("plugin_sid", 0)); pairs.append((ev, sid))
        except Exception: continue
    pairs.sort(key=lambda x: (x[1], x[0]))
    ordered = OrderedDict((k, v) for k, v in pairs)
    return json.dumps(ordered, ensure_ascii=False, indent=2)
# =========================================================

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
    lines = text.splitlines();
    if not lines: return rows, meta
    header = lines[0].strip().lower()
    if not header.startswith("plugin\t"): return rows, meta # Hanya support format baru
    hdr = [h.strip() for h in lines[0].split("\t")]
    for i, line in enumerate(lines[1:]):
        parts = line.strip().split("\t")
        if len(parts) != len(hdr): continue
        row_map = dict(zip(hdr, parts))
        try: sid = int(row_map.get("sid","0")); rows.append({"plugin_sid": sid, "event_name": row_map.get("title","")})
        except: continue
    return rows, meta
def tsv_merge(existing_rows, new_event_names):
    rows = list(existing_rows); known = set(r["event_name"] for r in existing_rows)
    existing_sids = [int(r.get("plugin_sid", 0)) for r in existing_rows]
    max_sid = max(existing_sids) if existing_sids else 0
    added_rows = []
    for ev in new_event_names:
        if ev not in known:
            max_sid += 1; nr = {"plugin_sid": max_sid, "event_name": ev}
            rows.append(nr); known.add(ev); added_rows.append(nr)
    return rows, added_rows, max_sid
# =========================================================

# =========================================================
# 70.conf Template Generator
# =========================================================
def generate_conf70_from_template(template_path, plugin_id, log_type, siem_plugin_type, field_name, category, json_dict_path_on_server):
    if not os.path.exists(template_path): die("Template 70.conf not found: {}".format(template_path))
    tpl = read_text(template_path)
    tpl = tpl.replace("{plugin_id}", str(plugin_id))
    tpl = tpl.replace("{siem_plugin_type}", siem_plugin_type)
    tpl = tpl.replace("{log_type}", log_type)
    tpl = tpl.replace("{field}", field_name) # Asumsi field sudah di-bracket [field]
    tpl = tpl.replace("{category}", category)
    tpl = tpl.replace("{dictionary_path}", json_dict_path_on_server)
    # Tambahkan refresh interval jika ada placeholder
    tpl = tpl.replace("{refresh_interval}", "60") # Default 60 detik
    return tpl
# =========================================================

# =========================================================
# Directives
# =========================================================
def load_directive_templates(path="./directive_rules.json"):
    if not os.path.exists(path): die("File directive_rules.json not found.")
    return read_json(path)
def order_rule_fields(rule):
    order = ["stage","name","plugin_id","plugin_sid","occurrence","reliability","timeout", "from","to","port_from","port_to","protocol","type","custom_data1","custom_data2","custom_data3"]
    out = OrderedDict()
    for k in order:
        if k in rule: out[k] = rule[k]
    for k, v in rule.items():
        if k not in out: out[k] = v
    return out
def build_directive_entry(template_rules, plugin_id, title, sid, header, category, kingdom, disabled=False, priority=3):
    _id = alarm_id(plugin_id, sid)
    def subst(obj):
        if isinstance(obj, dict): return {k: subst(v) for k, v in obj.items()}
        if isinstance(obj, list): return [subst(x) for x in obj]
        if obj == "{PLUGIN_ID}": return plugin_id
        if obj == "{SID}": return sid
        if isinstance(obj, string_types): return obj.replace("{TITLE}", title)
        return obj
    processed_rules = [order_rule_fields(subst(r)) for r in template_rules]
    directive_obj = OrderedDict()
    directive_obj["id"] = _id; directive_obj["name"] = "{}, {}".format(header, title.title())
    directive_obj["category"] = category; directive_obj["kingdom"] = kingdom
    directive_obj["priority"] = priority; directive_obj["all_rules_always_active"] = False
    directive_obj["disabled"] = bool(disabled); directive_obj["rules"] = processed_rules
    return directive_obj
def directive_append(existing_json, template_map, template_id, plugin_id, header, category, kingdom, disabled, rows_to_process):
    if not isinstance(existing_json, dict) or "directives" not in existing_json: existing_json = {"directives": []}
    directives = existing_json["directives"]; exist_ids = set(d.get("id", 0) for d in directives)
    tpl_rules = template_map.get(template_id)
    if not tpl_rules: die("[Directive] template_id '{}' not found.".format(template_id))
    appended, add_count = False, 0
    for r in rows_to_process:
        try: sid = int(r["plugin_sid"])
        except (KeyError, ValueError): continue # Lewati jika SID tidak valid
        _id = alarm_id(plugin_id, sid)
        if _id in exist_ids: continue
        entry = build_directive_entry(tpl_rules, plugin_id, r.get("event_name",""), sid, header, category, kingdom, disabled=disabled)
        directives.append(entry); appended, add_count = True, add_count + 1
    if appended: existing_json["directives"] = sorted(directives, key=lambda x: x.get("id", 0))
    return existing_json, appended, add_count, None
# =========================================================

# =========================================================
# GitHub
# =========================================================
def gh_headers(token): return {"Accept":"application/vnd.github+json", "Authorization":"Bearer {}".format(token), "X-GitHub-Api-Version": DEFAULT_GH_API_VERSION}
def gh_get(repo, branch, token, path, debug=False):
    url = "https://api.github.com/repos/{}/contents/{}".format(repo, path.replace("\\", "/"))
    try: r = requests.get(url, headers=gh_headers(token), params={"ref": branch}, timeout=60)
    except requests.exceptions.RequestException as e: die("GitHub GET Error: {}".format(e)); return None, None
    if debug: info("GET {} -> {}".format(url, r.status_code))
    if r.status_code == 404: return None, None
    if r.status_code >= 300: die("GitHub GET Error {}: {}".format(r.status_code, r.text[:200])); return None, None
    try: return r.json(), r.headers.get("x-github-request-id")
    except ValueError: die("GitHub GET Response is not valid JSON."); return None, None
def gh_put(repo, branch, token, path, bytes_content, message, sha=None, debug=False, dry=False):
    url = "https://api.github.com/repos/{}/contents/{}".format(repo, path.replace("\\", "/"))
    payload = {"message": message, "content": base64.b64encode(bytes_content).decode("ascii"), "branch": branch}
    if sha: payload["sha"] = sha
    if dry: info("[DRY-RUN] PUT {} ({} bytes)...".format(path, len(bytes_content))); return {}
    try: r = requests.put(url, headers=gh_headers(token), data=json.dumps(payload), timeout=60)
    except requests.exceptions.RequestException as e: die("GitHub PUT Error: {}".format(e)); return {}
    if debug: info("PUT {} -> {}".format(url, r.status_code))
    if r.status_code >= 300: die("GitHub PUT Error {} {}:\n{}".format(r.status_code, path, r.text[:400])); return {}
    try: return r.json()
    except ValueError: die("GitHub PUT Response is not valid JSON."); return {}
def gh_paths(log_type, module_name, submodule_name, filter_key, backend_pod="dsiem-backend-0"):
    parts = [p for p in [slug(log_type), slug(module_name), slug(submodule_name), slug(filter_key)] if p]
    unique_parts = list(OrderedDict.fromkeys(parts)); full_slug = "-".join(unique_parts)
    base_dir = "/".join(unique_parts)
    return { "tsv": "{}/{}_plugin-sids.tsv".format(base_dir, full_slug),
             "json_dict": "{}/{}_plugin-sids.json".format(base_dir, full_slug),
             "conf70": "{}/70_dsiem-plugin_{}.conf".format(base_dir, full_slug),
             "directive": "{}/directives_{}_{}.json".format(base_dir, backend_pod, full_slug),
             "full_slug": full_slug }
# =========================================================

# =========================================================
# OpenSearch
# =========================================================
def load_cred(path, user):
    if not path or not user: die("[CRED] ES_PASSWD_FILE or ES_USER_LOOKUP not set.")
    try:
        with io.open(path,"r",encoding="utf-8") as f:
            for ln in f:
                parts = ln.strip().split(":")
                if len(parts) >= 2 and parts[0].strip() == user: return user, ":".join(parts[1:]).strip()
    except IOError as e: die("[CRED] Cannot read {}: {}".format(path, e))
    die("[CRED] user {} not found in {}".format(user, path))
def fetch_titles(es_cfg, q_cfg, debug=False):
    host, verify, timeout = es_cfg["host"], es_cfg.get("verify_tls", False), es_cfg.get("timeout", 3000)
    u,p = load_cred(ES_PASSWD_FILE, ES_USER_LOOKUP)
    auth = HTTPBasicAuth(u,p)
    index, field, size = q_cfg["index"], q_cfg["field"], int(q_cfg.get("size", 2000))
    agg_field = field if field.endswith(".keyword") else field + ".keyword"
    body={"size":0, "aggs":{"event_names":{"terms":{"field": agg_field, "size": size}}}}
    mf = []
    for f in q_cfg.get("filters", []):
        op = f.get("op", "term"); field_name = f["field"]; value = f["value"]
        if op == "term": mf.append({"term": {(field_name if field_name.endswith(".keyword") else field_name + ".keyword"): value}})
        elif op == "contains": mf.append({"match_phrase": {field_name: value}})
        else: warn("Filter op '{}' unknown, using term.".format(op)); mf.append({"term": {(field_name if field_name.endswith(".keyword") else field_name + ".keyword"): value}})
    if "time_range" in q_cfg:
        time_cfg = q_cfg["time_range"]
        try: mf.append({"range": {time_cfg["field"]: {"gte": time_cfg["gte"], "lte": time_cfg["lte"]}}})
        except KeyError as e: warn("time_range incomplete (missing {}), skipped.".format(e))
    if mf: body["query"]={"bool":{"filter": mf}}
    url = "{}/{}/_search".format(host.rstrip("/"), index)
    if debug: info("OpenSearch Query Body:\n{}".format(json.dumps(body, indent=2)))
    try: r = requests.post(url, auth=auth, headers={"Content-Type":"application/json"}, data=json.dumps(body), timeout=timeout, verify=verify)
    except requests.exceptions.RequestException as e: die("OpenSearch connection error: {}".format(e)); return [], "", 0
    if r.status_code != 200: die("OpenSearch error {}: {}".format(r.status_code, r.text[:400]), code=3)
    try: data = r.json()
    except ValueError: die("OpenSearch response is not JSON."); return [], "", 0
    buckets = data.get("aggregations",{}).get("event_names",{}).get("buckets",[])
    return [b.get("key","") for b in buckets if b.get("key")], agg_field, len(buckets)
# =========================================================

# =========================================================
# Fungsi Distribusi & Update LOKAL (Return True jika ada perubahan)
# =========================================================
def distribute_and_update_local(merged_rows, paths, cfg, plugin_id, template_map, template_id, args):
    section("Distribute Local Files (Kubernetes)")
    made_local_changes = False
    logstash_json_dir = "/root/kubeappl/logstash/configs/pipelines/dsiem-events/dsiem-plugin-json/"
    json_filename = os.path.basename(paths["json_dict"])
    logstash_dest_path = os.path.join(logstash_json_dir, json_filename)
    info("Handling Logstash JSON dictionary...")
    new_json_content_str = write_json_dictionary(merged_rows); new_data_obj = json.loads(new_json_content_str)
    existing_data_obj = {}
    if os.path.exists(logstash_dest_path):
        try:
            with io.open(logstash_dest_path, "r", encoding="utf-8") as f_exist: existing_data_obj = json.load(f_exist)
        except (IOError, JSONDecodeError, ValueError): warn("Failed read old JSON at {}, overwriting.".format(logstash_dest_path))
    if new_data_obj != existing_data_obj:
        info("JSON content differs, writing to {}".format(logstash_dest_path))
        if not args.dry_run:
            if not os.path.isdir(logstash_json_dir):
                try: os.makedirs(logstash_json_dir)
                except OSError as e: err("Failed create Logstash dir: {}".format(e)); return False
            try:
                with io.open(logstash_dest_path, "w", encoding="utf-8") as f:
                    try: unicode; f.write(new_json_content_str.decode('utf-8') if isinstance(new_json_content_str, str) else new_json_content_str)
                    except NameError: f.write(new_json_content_str)
                    f.write(u'\n') # Add newline
                made_local_changes = True; info("Logstash JSON dictionary updated.")
            except IOError as e: err("Failed write JSON to {}: {}".format(logstash_dest_path, e))
        else: info("[DRY-RUN] JSON write skipped."); made_local_changes = True
    else: info("Logstash JSON dictionary already synced.")
    info("\nHandling dsiem-frontend directive...")
    pod_name = "dsiem-frontend-0"; remote_directive_filename = os.path.basename(paths["directive"])
    remote_path_in_pod = "/dsiem/configs/{}".format(remote_directive_filename)
    local_temp_path = "./{}.temp".format(remote_directive_filename)
    info("Fetching existing directive from pod: {}...".format(pod_name))
    # run_cmd returns return code (0 = success)
    rc = run_cmd(["kubectl", "cp", "{}:{}".format(pod_name, remote_path_in_pod), local_temp_path], dry=args.dry_run)
    existing_dir = {"directives": []}
    # Check rc == 0 for successful kubectl cp
    if rc == 0 and os.path.exists(local_temp_path):
        try: existing_dir = read_json(local_temp_path)
        except: warn("Failed read directive JSON from pod, will create new.")
    elif rc != 0 and not args.dry_run: # If kubectl failed (and not dry run), log warning
         warn("Failed to copy directive from pod (kubectl rc={}). Assuming new.".format(rc))
    else: info("Directive file not found in pod or kubectl failed. Assuming new.")
    info("Syncing/Appending directive entries...")
    dircfg = cfg['directive']
    updated_dir_json, appended, add_count, _ = directive_append( existing_dir, template_map, template_id, plugin_id,
        dircfg["HEADER"], dircfg["CATEGORY"], dircfg["KINGDOM"], bool(dircfg.get("DISABLED", False)), merged_rows )
    if appended:
        info("Found {} missing/new directives. Distributing back to pod...".format(add_count))
        temp_write_ok = False
        if not args.dry_run:
            try:
                with io.open(local_temp_path, "w", encoding="utf-8") as f:
                    json_str_directive = json.dumps(updated_dir_json, indent=2, ensure_ascii=False)
                    try: unicode; f.write(json_str_directive.decode('utf-8') if isinstance(json_str_directive, str) else json_str_directive)
                    except NameError: f.write(json_str_directive)
                    f.write(u'\n') # Add newline
                temp_write_ok = True
            except IOError as e: err("Failed write temp directive {}: {}".format(local_temp_path, e))
        else: info("[DRY-RUN] Temp directive write skipped."); temp_write_ok = True
        if temp_write_ok:
            # run_cmd returns return code (0 = success)
            if run_cmd(["kubectl", "cp", local_temp_path, "{}:{}".format(pod_name, remote_path_in_pod)], dry=args.dry_run) == 0:
                 made_local_changes = True; info("Directive distribution complete.")
            else: err("Failed copy directive to pod.")
    else: info("No new directives to add. Local directive already synced.")
    if os.path.exists(local_temp_path) and not args.dry_run:
        try: os.remove(local_temp_path)
        except OSError as e: warn("Failed remove temp file {}: {}".format(local_temp_path, e))
    return made_local_changes
# =========================================================

# =========================================================
# CLI & MAIN
# =========================================================
def parse_args():
    ap = argparse.ArgumentParser(description="Auto-update SIEM event dictionary & directives.")
    ap.add_argument("--dry-run", action="store_true", help="Simulate without pushing or distributing.")
    ap.add_argument("--debug", action="store_true", help="Enable extra debug logging (e.g., query bodies).")
    return ap.parse_args()

def main():
    args = parse_args()
    section("Load config")
    try: cfg = read_json(CFG_PATH)
    except (FileNotFoundError, IOError): die("Config file '{}' not found.".format(CFG_PATH)); return 1 # Exit if config missing
    except (JSONDecodeError, ValueError): die("Config file '{}' is not valid JSON.".format(CFG_PATH)); return 1

    customer_path = cfg.get("customer_config_path", "./customer.json")
    info("Loading customer config from: {}".format(customer_path))
    try: customer_cfg = read_json(customer_path); cfg.update(customer_cfg)
    except FileNotFoundError: warn("Customer config '{}' not found.".format(customer_path))
    except (JSONDecodeError, ValueError): err("Error decoding customer JSON '{}'.".format(customer_path))
    info("Email config loading from env vars.")

    # Validate essential config sections
    required_keys = ["es", "query", "layout", "file70", "directive", "github"]
    if not all(k in cfg for k in required_keys):
        missing = [k for k in required_keys if k not in cfg]
        die("Config file '{}' is missing required sections: {}".format(CFG_PATH, ", ".join(missing)))
        return 1
        
    es_cfg, q_cfg, layout, file70, dircfg, gh_cfg = cfg["es"], cfg["query"], cfg["layout"], cfg["file70"], cfg["directive"], cfg["github"]

    if not GITHUB_TOKEN: die("GITHUB_TOKEN env var not set.", code=2)

    try: paths = gh_paths(layout["device"], layout["module"], layout.get("submodule"), layout.get("filter_key"))
    except KeyError as e: die("Layout section missing key: {}".format(e)); return 1
    try: plugin_id = int(file70["plugin_id"])
    except (KeyError, ValueError): die("file70 section missing or invalid 'plugin_id'."); return 1
    
    siem_plugin_type = paths["full_slug"]
    category, kingdom, disabled, template_id = dircfg.get("CATEGORY"), dircfg.get("KINGDOM"), bool(dircfg.get("DISABLED")), dircfg.get("template_id")
    if not template_id: die("Directive section missing 'template_id'."); return 1

    env_repo = os.getenv("GITHUB_REPO"); env_branch = os.getenv("GITHUB_BRANCH")
    gh_repo = env_repo if env_repo else gh_cfg.get("repo") # Fallback to JSON (should be removed)
    gh_branch = env_branch if env_branch else gh_cfg.get("branch", "main") # Fallback to JSON (should be removed)
    template70_path = gh_cfg.get("template_path", "./template-70.js") # Default if missing
    registry_path = gh_cfg.get("plugin_registry_path", "plugin_id.json") # Default if missing

    if not gh_repo: die("GitHub repo not defined in env var or config JSON."); return 1

    info("Using GITHUB_REPO: {}".format(gh_repo))
    info("CFG: {}, Repo: {}@{}, Slug: {}".format(CFG_PATH, gh_repo, gh_branch, siem_plugin_type))

    section("Check Plugin ID Registry")
    reg_obj, _ = gh_get(gh_repo, gh_branch, GITHUB_TOKEN, registry_path, debug=args.debug)
    registry, found_in_reg = {}, False
    reg_sha = None # Initialize reg_sha
    if reg_obj:
        reg_sha = reg_obj.get("sha") # Get sha for potential update
        try: registry = json.loads(base64.b64decode(reg_obj.get("content","")).decode("utf-8"))
        except (TypeError, ValueError, base64.binascii.Error): warn("Failed parse plugin registry from GitHub.")
    if not isinstance(registry.get("used"), list): registry["used"] = [] # Ensure 'used' is a list
    for item in registry.get("used", []):
        if item.get("siem_plugin_type") == siem_plugin_type:
            try: reg_pid = int(item.get("plugin_id"))
            except (ValueError, TypeError): continue
            if reg_pid == plugin_id: found_in_reg = True; info("Plugin ID {} OK.".format(plugin_id)); break
            else: die("Conflict! Slug '{}' uses ID {}, but registry has {}.".format(siem_plugin_type, plugin_id, reg_pid))
    if not found_in_reg and not args.dry_run:
        # Auto-register if not found and not dry run
        section("Auto-registering Plugin ID")
        registry["used"].append({"plugin_id": plugin_id, "siem_plugin_type": siem_plugin_type, "by": layout.get("device", "unknown")})
        registry["used"] = sorted(registry["used"], key=lambda x: x.get("plugin_id", 0))
        gh_put(gh_repo, gh_branch, GITHUB_TOKEN, registry_path, json.dumps(registry, indent=2).encode("utf-8"),
               "[auto] Register plugin_id {} for {}".format(plugin_id, siem_plugin_type), sha=reg_sha, debug=args.debug, dry=args.dry_run)
        info("Plugin Registry push: OK")
        found_in_reg = True # Mark as found now
    elif not found_in_reg and args.dry_run:
         info("[DRY-RUN] Plugin ID {} would be registered.".format(plugin_id))
         found_in_reg = True # Simulate registration for dry run

    section("OpenSearch aggregation")
    titles, _, _ = fetch_titles(es_cfg, q_cfg, debug=args.debug)

    section("Fetch & Merge TSV from GitHub")
    tsv_obj, _ = gh_get(gh_repo, gh_branch, GITHUB_TOKEN, paths["tsv"], debug=args.debug)
    existing_rows, tsv_sha = [], None
    if tsv_obj:
        tsv_sha = tsv_obj.get("sha")
        try: content = base64.b64decode(tsv_obj.get("content", "")).decode("utf-8")
        except (TypeError, base64.binascii.Error): content = ""; warn("Failed decode TSV from GitHub.")
        existing_rows, _ = tsv_parse(content)
        info("TSV exists (sha={}), rows={}".format(tsv_sha, len(existing_rows)))
    else: info("TSV not found (new file).")
    merged_rows, added_rows, _ = tsv_merge(existing_rows, titles)
    info("Total rows: {}, New events: {}".format(len(merged_rows), len(added_rows)))

    if added_rows:
        is_active_plugin = False
        active_plugins_file = './active_plugins.json'
        if os.path.exists(active_plugins_file):
            try:
                # Use io.open here too
                with io.open(active_plugins_file, 'r', encoding='utf-8') as f: active_list = json.load(f)
                if isinstance(active_list, list) and siem_plugin_type in active_list:
                    is_active_plugin = True; info("Plugin ACTIVE. Email will be sent.")
                else: info("Plugin PASSIVE. Skipping email.")
            except (IOError, JSONDecodeError, ValueError): warn("Cannot parse active_plugins.json. Assuming PASSIVE.")
        else: info("active_plugins.json not found. Assuming PASSIVE.")
        if is_active_plugin:
            customer_name = cfg.get("customer_info", {}).get("customer_name", "Default")
            send_notification_email(customer_name, dircfg.get("HEADER", paths["full_slug"]), added_rows)

    if added_rows or not tsv_obj:
        gh_put(gh_repo, gh_branch, GITHUB_TOKEN, paths["tsv"], tsv_render(merged_rows, siem_plugin_type, plugin_id, category, kingdom).encode("utf-8"),
               "[auto] Update TSV for {}".format(siem_plugin_type), sha=tsv_sha, debug=args.debug, dry=args.dry_run)
        info("TSV push: OK")

    section("Sync GitHub JSON Dictionary")
    json_obj, _ = gh_get(gh_repo, gh_branch, GITHUB_TOKEN, paths["json_dict"], debug=args.debug)
    new_json_content_str = write_json_dictionary(merged_rows); new_data_obj = json.loads(new_json_content_str)
    existing_data_obj = {}
    if json_obj and json_obj.get("content"):
        try:
            existing_json_content_str = base64.b64decode(json_obj.get("content", "")).decode("utf-8")
            existing_data_obj = json.loads(existing_json_content_str)
        except (JSONDecodeError, TypeError, ValueError, base64.binascii.Error): warn("Failed parse GitHub JSON dict.")
    if new_data_obj != existing_data_obj:
        info("JSON dict differs. Pushing sync...")
        gh_put(gh_repo, gh_branch, GITHUB_TOKEN, paths["json_dict"], new_json_content_str.encode("utf-8"),
               "[auto] Sync JSON dict for {}".format(siem_plugin_type), sha=json_obj.get("sha") if json_obj else None, debug=args.debug, dry=args.dry_run)
        info("JSON Dict push: OK")
    else: info("JSON Dict already synced.")

    section("Update 70.conf (if missing)")
    conf70_obj, _ = gh_get(gh_repo, gh_branch, GITHUB_TOKEN, paths["conf70"], debug=args.debug)
    if not conf70_obj:
        info("70.conf missing on GitHub. Generating and pushing...")
        # Path on server where Logstash reads the JSON
        json_path_on_server = "/root/kubeappl/logstash/configs/pipelines/dsiem-events/dsiem-plugin-json/{}_plugin-sids.json".format(siem_plugin_type)
        # Use field without .keyword for template {field} placeholder
        field_no_keyword = q_cfg["field"].replace(".keyword", "")
        conf70_text = generate_conf70_from_template(template70_path, plugin_id, layout["device"], siem_plugin_type, field_no_keyword, category, json_path_on_server)
        gh_put(gh_repo, gh_branch, GITHUB_TOKEN, paths["conf70"], conf70_text.encode("utf-8"),
               "[auto] Create 70.conf for {}".format(siem_plugin_type), sha=None, debug=args.debug, dry=args.dry_run)
        info("70.conf push: CREATED")
    else: info("70.conf already exists on GitHub.")

    section("Sync GitHub Directives")
    template_map = load_directive_templates("./directive_rules.json")
    dir_obj, _ = gh_get(gh_repo, gh_branch, GITHUB_TOKEN, paths["directive"], debug=args.debug)
    existing_dir, dir_sha = ({"directives":[]}, None)
    if dir_obj:
        dir_sha = dir_obj.get("sha")
        try: existing_dir = json.loads(base64.b64decode(dir_obj.get("content","")).decode("utf-8"))
        except (JSONDecodeError, ValueError, TypeError, base64.binascii.Error): warn("Failed parse GitHub directives JSON.")
    if not isinstance(existing_dir.get("directives"), list): existing_dir["directives"] = [] # Ensure structure

    updated_dir_json, appended, add_count, _ = directive_append(existing_dir, template_map, template_id, plugin_id, dircfg["HEADER"], category, kingdom, disabled, merged_rows)
    if appended:
        info("Directives differ ({} new). Pushing sync...".format(add_count))
        gh_put(gh_repo, gh_branch, GITHUB_TOKEN, paths["directive"], json.dumps(updated_dir_json, indent=2, ensure_ascii=False).encode("utf-8"),
               "[auto] Sync directives for {}".format(siem_plugin_type), sha=dir_sha, debug=args.debug, dry=args.dry_run)
        info("Directives push: OK")
    else: info("Directives already synced.")

    # Call distribute_and_update_local AFTER GitHub updates
    made_local_changes = distribute_and_update_local(merged_rows, paths, cfg, plugin_id, template_map, template_id, args)

    section("Summary")
    info("DONE.")
    if made_local_changes:
        info("Local changes detected, signaling for restart.")
        return 5 # Exit code for restart needed
    else:
        return 0 # Normal exit

if __name__ == "__main__":
    exit_code = 99
    try: exit_code = main()
    except SystemExit as e: exit_code = e.code if isinstance(e.code, int) else 1 # Ensure integer exit code
    except Exception as e: err("Unexpected error: {}".format(e)); traceback.print_exc()
    finally: sys.exit(exit_code)