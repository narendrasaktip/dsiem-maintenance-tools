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
EMAIL_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT") or 25) # Default port 25 if None
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD")
EMAIL_RECIPIENTS = os.getenv("EMAIL_RECIPIENTS")

LOGSTASH_JSON_DICT_DIR = os.getenv("LOGSTASH_JSON_DICT_DIR")
VECTOR_CONFIG_BASE_DIR = os.getenv("VECTOR_CONFIG_BASE_DIR")
NFS_BASE_DIR = os.getenv("NFS_BASE_DIR")
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
    ap = argparse.ArgumentParser(description="Auto-update SIEM event dictionary & directives.")
    ap.add_argument("--dry-run", action="store_true", help="Simulate without pushing or distributing.")
    ap.add_argument("--debug", action="store_true", help="Enable extra debug logging.")
    return ap.parse_args()
# =========================================================

# =========================================================
# LOGGER & IO
# =========================================================
START_TS = datetime.utcnow()

def read_json(path):
    with io.open(path, "r", encoding="utf-8") as f:
        return json.load(f, object_pairs_hook=OrderedDict)

def read_text(path):
    with io.open(path, "r", encoding="utf-8") as f: return f.read()

def slug(s):
    if s is None: return ""
    s = s.strip().lower(); s = re.sub(r'[^a-z0-9]+', '-', s); s = re.sub(r'-+', '-', s).strip('-'); return s

def alarm_id(plugin_id, sid): return int(plugin_id) * 10000 + int(sid)

def run_cmd(cmd_list, dry=False):
    info("Executing command: {}".format(" ".join(cmd_list)))
    try:
        p = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode != 0:
            warn("Command failed (rc={}):\n{}".format(p.returncode, err.decode('utf-8', 'replace')))
        return p.returncode
    except OSError as e: err("Failed to run command: {}".format(e)); return 1

def write_json_dictionary(rows):
    pairs = [];
    for r in rows or []:
        ev = r.get("event_name", "").strip();
        if not ev: continue
        try: sid = int(r.get("plugin_sid", 0)); pairs.append((ev, sid))
        except Exception: continue
    pairs.sort(key=lambda x: (x[1], x[0])) 
    ordered = OrderedDict((k, v) for k, v in pairs)
    return json.dumps(ordered, ensure_ascii=False, indent=2)
# =========================================================

# =========================================================
# EMAIL FUNCTION
# =========================================================
def send_notification_email(customer_name, header_name, new_events):
    if not all([EMAIL_SENDER, EMAIL_APP_PASSWORD, EMAIL_RECIPIENTS]):
        warn("Email environment variables not fully set. Skipping email notification.")
        return

    section("Sending Email Notification")
    recipients = [email.strip() for email in EMAIL_RECIPIENTS.split(',') if email.strip()]
    if not recipients:
        err("EMAIL_RECIPIENTS is set but contains no valid email addresses. Aborting email.")
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
    <title>Automated Event Update Report</title>
    <style>
      :root {{ color-scheme: light; supported-color-schemes: light; }}
      .table td, .table th {{ padding: 8px !important; }}
    </style>
  </head>
  <body style="margin:0; padding:0; background:#F3F5F7;" bgcolor="#F3F5F7">
    <table role="presentation" width="100%" border="0" bgcolor="#F3F5F7">
      <tr><td align="center" style="padding:24px;">
        <table role="presentation" width="100%" border="0" style="max-width:640px; border:1px solid #E1E4E8; background:#FFFFFF;" bgcolor="#FFFFFF">
          <tr><td align="center" style="background:#8B0016; padding:22px 24px;">
            <h1 style="color:#FFFFFF; margin:0; font-family:Arial, sans-serif; font-size:20px;">Automated Event Update Report</h1>
          </td></tr>
          <tr><td style="padding:24px; color:#111111; font-family:Arial, sans-serif;">
            <p>Hello Team,</p>
            <p>The automated system has detected new events that have been successfully added to the SIEM directory.</p>
            <hr style="border:0; border-top:1px solid #E1E4E8; margin:16px 0;">
            <h3>Detection Summary</h3>
            <table width="100%" style="border-collapse:collapse; font-size:14px;">
              <tr><td width="36%" style="padding:10px; border:1px solid #D6D9DE; background:#F7F9FA; font-weight:bold;">Customer</td><td style="padding:10px; border:1px solid #D6D9DE;">{customer}</td></tr>
              <tr><td style="padding:10px; border:1px solid #D6D9DE; background:#F7F9FA; font-weight:bold;">Plugin</td><td style="padding:10px; border:1px solid #D6D9DE;">{plugin}</td></tr>
              <tr><td style="padding:10px; border:1px solid #D6D9DE; background:#F7F9FA; font-weight:bold;">Count</td><td style="padding:10px; border:1px solid #D6D9DE;">{count}</td></tr>
              <tr><td style="padding:10px; border:1px solid #D6D9DE; background:#F7F9FA; font-weight:bold;">Time</td><td style="padding:10px; border:1px solid #D6D9DE;">{time}</td></tr>
            </table>
            <hr style="border:0; border-top:1px solid #E1E4E8; margin:20px 0;">
            <h3>New Event Details</h3>
            <table width="100%" style="border-collapse:collapse; font-size:14px;">
              <thead><tr>
                <th align="left" style="padding:10px; border:1px solid #D6D9DE; background:#ECEFF3;">SID</th>
                <th align="left" style="padding:10px; border:1px solid #D6D9DE; background:#ECEFF3;">Event Name</th>
              </tr></thead>
              <tbody>{event_rows}</tbody>
            </table>
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
# TSV FUNCTIONS
# =========================================================
TSV_HEADER = "plugin\tid\tsid\ttitle\tcategory\tkingdom"
def tsv_render(rows, plugin_label, plugin_id, category, kingdom):
    out = [TSV_HEADER]
    sorted_rows = sorted(rows, key=lambda r: int(r.get("plugin_sid", 0)))
    for r in sorted_rows:
        out.append(u"{}\t{}\t{}\t{}\t{}\t{}".format(
            unicode(plugin_label or u""), unicode(plugin_id or u""), unicode(r.get("plugin_sid", u"")),
            unicode(r.get("event_name", u"")).replace(u"\t", u" "),
            unicode(category or u""), unicode(kingdom or u"")
        ))
    return u"\n".join(out) + u"\n"

def tsv_parse(text):
    rows, meta = [], {}
    if not text: return rows, meta
    lines = text.splitlines();
    if not lines: return rows, meta
    header = lines[0].strip().lower()
    if not header.startswith("plugin\t"):
        warn("TSV header does not start with 'plugin\\t'. Assuming invalid format.")
        return rows, meta
    hdr = [h.strip() for h in lines[0].split("\t")]
    for i, line in enumerate(lines[1:]):
        parts = line.strip().split("\t")
        if len(parts) != len(hdr):
            warn("Skipping TSV line {}: Incorrect columns.".format(i+2))
            continue
        row_map = dict(zip(hdr, parts))
        try:
            sid = int(row_map.get("sid","0"))
            rows.append({"plugin_sid": sid, "event_name": row_map.get("title","")})
        except ValueError: continue
    return rows, meta

def tsv_merge(existing_rows, new_event_names):
    rows = list(existing_rows); known = set(r["event_name"] for r in existing_rows)
    existing_sids = [int(r.get("plugin_sid", 0)) for r in existing_rows if unicode(r.get("plugin_sid", "")).isdigit()]
    max_sid = max(existing_sids) if existing_sids else 0
    added_rows = []
    for ev in new_event_names:
        if ev and ev not in known:
            max_sid += 1
            nr = {"plugin_sid": max_sid, "event_name": ev}
            rows.append(nr); known.add(ev); added_rows.append(nr)
    return rows, added_rows, max_sid
# =========================================================

# =========================================================
# 70.CONF GENERATOR
# =========================================================
def generate_conf70_from_template(template_path, plugin_id, log_type, siem_plugin_type, field_name, category, json_dict_path_on_server):
    if not os.path.exists(template_path): die("Template 70.conf not found: {}".format(template_path))
    tpl = read_text(template_path)
    tpl = tpl.replace("{plugin_id}", unicode(plugin_id))
    tpl = tpl.replace("{siem_plugin_type}", unicode(siem_plugin_type))
    tpl = tpl.replace("{log_type}", unicode(log_type))
    logstash_field = u"[{}]".format(field_name.strip(u"[]"))
    tpl = tpl.replace("{field}", logstash_field)
    tpl = tpl.replace("{category}", unicode(category or u""))
    tpl = tpl.replace("{dictionary_path}", unicode(json_dict_path_on_server))
    tpl = tpl.replace("{refresh_interval}", u"60")
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
    for k, v in rule.items():
        if k not in out: out[k] = v
    return out

def build_directive_entry(template_rules, plugin_id, title, sid, header, category, kingdom, disabled=False, priority=3):
    _id = alarm_id(plugin_id, sid)
    def subst(obj):
        if isinstance(obj, dict): return OrderedDict((k, subst(v)) for k, v in obj.items())
        if isinstance(obj, list):
             if len(obj) == 1 and obj[0] == "{SID}": return [sid]
             return [subst(x) for x in obj]
        if obj == "{PLUGIN_ID}": return plugin_id
        if obj == "{SID}": return sid
        if isinstance(obj, string_types): return obj.replace("{TITLE}", title)
        return obj
    processed_rules = [order_rule_fields(subst(r)) for r in template_rules]
    directive_obj = OrderedDict()
    directive_obj["id"] = _id; directive_obj["name"] = u"{}, {}".format(header, title.title())
    directive_obj["category"] = category; directive_obj["kingdom"] = kingdom
    directive_obj["priority"] = priority; directive_obj["all_rules_always_active"] = False
    directive_obj["disabled"] = bool(disabled); directive_obj["rules"] = processed_rules
    return directive_obj

def directive_append(existing_json, template_map, template_id, plugin_id, header, category, kingdom, disabled, rows_to_process):
    if not isinstance(existing_json, dict) or "directives" not in existing_json or not isinstance(existing_json.get("directives"), list):
        warn("Existing directive JSON invalid. Creating new list.")
        existing_json = OrderedDict([("directives", [])])
    directives = existing_json["directives"]; exist_ids = set(d.get("id", 0) for d in directives)
    tpl_rules = template_map.get(template_id)
    if not tpl_rules: die("[Directive] template_id '{}' not found.".format(template_id))
    appended, add_count = False, 0
    for r in rows_to_process:
        try: sid = int(r["plugin_sid"])
        except (KeyError, ValueError): continue
        _id = alarm_id(plugin_id, sid)
        if _id in exist_ids: continue
        entry = build_directive_entry(tpl_rules, plugin_id, r.get("event_name",u""), sid, header, category, kingdom, disabled=disabled)
        directives.append(entry); appended, add_count = True, add_count + 1
    if appended: existing_json["directives"] = sorted(directives, key=lambda x: x.get("id", 0))
    return existing_json, appended, add_count, None
# =========================================================

# =========================================================
# GITHUB FUNCTIONS
# =========================================================
def gh_headers(token): return {"Accept":"application/vnd.github+json", "Authorization":"Bearer {}".format(token), "X-GitHub-Api-Version": DEFAULT_GH_API_VERSION}
def gh_get(repo, branch, token, path, debug=False):
    url = "https://api.github.com/repos/{}/contents/{}".format(repo, path.replace("\\", "/").lstrip('/'))
    try: r = requests.get(url, headers=gh_headers(token), params={"ref": branch}, timeout=60)
    except requests.exceptions.RequestException as e: die("GitHub GET Error: {}".format(e)); return None, None
    if debug: info("GET {} -> {}".format(url, r.status_code))
    if r.status_code == 404: return None, None
    if r.status_code >= 300: die("GitHub GET Error {} {}: {}".format(r.status_code, path, r.text[:200])); return None, None
    try: return r.json(), r.headers.get("x-github-request-id")
    except ValueError: return None, None
def gh_put(repo, branch, token, path, bytes_content, message, sha=None, debug=False, dry=False):
    url = "https://api.github.com/repos/{}/contents/{}".format(repo, path.replace("\\", "/").lstrip('/'))
    payload = {"message": message, "content": base64.b64encode(bytes_content).decode("ascii"), "branch": branch}
    if sha: payload["sha"] = sha
    if dry: info("[DRY-RUN] PUT {} ({} bytes), sha={}".format(path, len(bytes_content), sha)); return {"sha": "dry_run_sha"}
    try: r = requests.put(url, headers=gh_headers(token), data=json.dumps(payload), timeout=60)
    except requests.exceptions.RequestException as e: die("GitHub PUT Error: {}".format(e)); return {}
    if debug: info("PUT {} -> {}".format(url, r.status_code))
    if r.status_code >= 300: die("GitHub PUT Error {} {}:\n{}".format(r.status_code, path, r.text[:400])); return {}
    try: return r.json()
    except ValueError: return {}
    
def gh_paths(log_type, module_name, submodule_name, filter_key, backend_pod="dsiem-backend-0"):
    parts = [p for p in [slug(log_type), slug(module_name), slug(submodule_name), slug(filter_key)] if p]
    unique_parts = list(OrderedDict.fromkeys(parts)); full_slug = u"-".join(unique_parts)
    base_dir = u"/".join(unique_parts)
    return { "tsv": u"{}/{}_plugin-sids.tsv".format(base_dir, full_slug),
             "json_dict": u"{}/{}_plugin-sids.json".format(base_dir, full_slug),
             "conf70": u"{}/70_dsiem-plugin_{}.conf".format(base_dir, full_slug),
             "vector_conf": u"{}/70_transform_dsiem-plugin-{}.yaml".format(base_dir, full_slug),
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
                     if pwd: return user, pwd
                     else: die("[CRED] Empty password for user {} in {}".format(user, path))
    except IOError as e: die("[CRED] Cannot read credentials file {}: {}".format(path, e))
    die("[CRED] User '{}' not found in {}".format(user, path))
def fetch_titles(es_cfg, q_cfg, debug=False):
    host, verify, timeout = es_cfg.get("host"), es_cfg.get("verify_tls", False), es_cfg.get("timeout", 3000)
    if not host: die("Elasticsearch host not configured.")
    u,p = load_cred(ES_PASSWD_FILE, ES_USER_LOOKUP)
    auth = HTTPBasicAuth(u,p)
    index, field, size = q_cfg.get("index"), q_cfg.get("field"), int(q_cfg.get("size", 2000))
    if not index or not field: die("Query missing index or field.")
    agg_field = field if field.endswith(".keyword") else field + ".keyword"
    body={"size":0, "aggs":{"event_names":{"terms":{"field": agg_field, "size": size}}}}
    mf = []
    for f in q_cfg.get("filters", []):
        try: op = f.get("op", "term"); field_name = f["field"]; value = f["value"]
        except KeyError: continue
        if op == "term": mf.append({"term": {(field_name if field_name.endswith(".keyword") else field_name + ".keyword"): value}})
        elif op == "contains": mf.append({"match_phrase": {field_name: value}})
        else: mf.append({"term": {(field_name if field_name.endswith(".keyword") else field_name + ".keyword"): value}})
    if "time_range" in q_cfg:
        time_cfg = q_cfg["time_range"]
        try: mf.append({"range": {time_cfg["field"]: {"gte": time_cfg["gte"], "lte": time_cfg["lte"]}}})
        except KeyError: pass
    if mf: body["query"]={"bool":{"filter": mf}}
    url = "{}/{}/_search".format(host.rstrip("/"), index)
    try: r = requests.post(url, auth=auth, headers={"Content-Type":"application/json"}, data=json.dumps(body), timeout=timeout, verify=verify)
    except requests.exceptions.RequestException as e: die("OpenSearch error: {}".format(e)); return [], "", 0
    if r.status_code != 200: die("OpenSearch failed ({})".format(r.status_code)); return [], "", 0
    try: data = r.json()
    except ValueError: return [], "", 0
    buckets = data.get("aggregations",{}).get("event_names",{}).get("buckets",[])
    return [b.get("key",u"") for b in buckets if b.get("key")], agg_field, len(buckets)
# =========================================================

# =========================================================
# DISTRIBUTE LOCAL FUNCTIONS
# =========================================================
def distribute_logstash_local(merged_rows, paths, cfg, plugin_id, template_map, template_id, args):
    section("Distribute Local Files (Logstash)")
    made_local_changes = False
    
    if not LOGSTASH_JSON_DICT_DIR:
        err("LOGSTASH_JSON_DICT_DIR env var not set.")
        return False
    logstash_json_dir = LOGSTASH_JSON_DICT_DIR

    json_filename = os.path.basename(paths["json_dict"])
    logstash_dest_path = os.path.join(logstash_json_dir, json_filename)
    info("Handling Logstash JSON dictionary...")

    new_json_content_str = write_json_dictionary(merged_rows);
    try: new_data_obj = json.loads(new_json_content_str)
    except (ValueError, JSONDecodeError): err("Invalid new JSON."); return False

    existing_data_obj = {}
    if os.path.exists(logstash_dest_path):
        try:
            with io.open(logstash_dest_path, "r", encoding="utf-8") as f_exist: existing_data_obj = json.load(f_exist)
        except (IOError, JSONDecodeError, ValueError): pass

    if new_data_obj != existing_data_obj:
        info("JSON content differs, writing to {}".format(logstash_dest_path))
        if not args.dry_run:
            if not os.path.isdir(logstash_json_dir):
                try: os.makedirs(logstash_json_dir)
                except OSError: return False
            try:
                with io.open(logstash_dest_path, "w", encoding="utf-8") as f:
                    try: unicode; f.write(new_json_content_str.decode('utf-8') if isinstance(new_json_content_str, str) else new_json_content_str)
                    except NameError: f.write(new_json_content_str)
                    f.write(u'\n')
                made_local_changes = True; info("Logstash JSON dictionary updated.")
            except IOError as e: err("Failed write JSON: {}".format(e))
        else: info("[DRY-RUN] JSON write skipped."); made_local_changes = True
    else: info("Logstash JSON dictionary already synced.")

    info("\nHandling dsiem-frontend directive...")
    pod_name = "dsiem-frontend-0";
    remote_directive_filename = os.path.basename(paths["directive"])
    remote_path_in_pod = "/dsiem/configs/{}".format(remote_directive_filename)
    local_temp_path = "./{}.temp".format(remote_directive_filename)

    rc = run_cmd(["kubectl", "cp", "{}:{}".format(pod_name, remote_path_in_pod), local_temp_path], dry=args.dry_run)
    existing_dir = OrderedDict([("directives", [])])
    if rc == 0 and os.path.exists(local_temp_path):
        try:
            existing_dir = read_json(local_temp_path)
            if not isinstance(existing_dir.get("directives"), list): existing_dir = OrderedDict([("directives", [])])
        except Exception: existing_dir = OrderedDict([("directives", [])])

    dircfg = cfg.get('directive', {})
    if not template_id: err("Missing 'template_id'."); return made_local_changes
    updated_dir_json, appended, add_count, _ = directive_append( existing_dir, template_map, template_id, plugin_id,
        dircfg.get("HEADER", "Default Header"), dircfg.get("CATEGORY", "Default Category"), dircfg.get("KINGDOM", "Default Kingdom"),
        bool(dircfg.get("DISABLED", False)), merged_rows )

    if appended:
        info("Found {} missing/new directives. Distributing back to pod...".format(add_count))
        temp_write_ok = False
        if not args.dry_run:
            try:
                with io.open(local_temp_path, "w", encoding="utf-8") as f:
                    json_str_directive = json.dumps(updated_dir_json, indent=2, ensure_ascii=False)
                    try: unicode; f.write(json_str_directive.decode('utf-8') if isinstance(json_str_directive, str) else json_str_directive)
                    except NameError: f.write(json_str_directive)
                    f.write(u'\n')
                temp_write_ok = True
            except IOError as e: err("Failed write temp directive: {}".format(e))
        else: info("[DRY-RUN] Temp directive write skipped."); temp_write_ok = True

        if temp_write_ok:
            if run_cmd(["kubectl", "cp", local_temp_path, "{}:{}".format(pod_name, remote_path_in_pod)], dry=args.dry_run) == 0:
                 made_local_changes = True; info("Directive distribution complete.")
            else: err("Failed copy directive to pod.")
    else: info("No new directives to add.")

    if os.path.exists(local_temp_path) and not args.dry_run:
        try: os.remove(local_temp_path)
        except OSError: pass

    return made_local_changes

def distribute_vector_local(merged_rows, paths, cfg, args):
    section("Distribute Local Files (Vector)")
    made_local_changes = False

    if not VECTOR_CONFIG_BASE_DIR or not NFS_BASE_DIR:
        err("VECTOR_CONFIG_BASE_DIR or NFS_BASE_DIR env vars not set.")
        return False
        
    info("Handling Vector TSV dictionary...")
    
    nfs_target_dir = None
    try:
        if os.path.isdir(NFS_BASE_DIR):
            for item in os.listdir(NFS_BASE_DIR):
                item_path = os.path.join(NFS_BASE_DIR, item)
                if os.path.isdir(item_path) and item.startswith("pvc-"):
                    potential_target = os.path.join(item_path, "dsiem-plugin-tsv")
                    if os.path.isdir(potential_target): 
                        nfs_target_dir = potential_target
                        break
    except Exception as e: err("Failed to search NFS: {}".format(e)); return False

    if not nfs_target_dir: err("Directory 'dsiem-plugin-tsv' not found in NFS."); return False

    tsv_filename = os.path.basename(paths["tsv"])
    nfs_dest_path = os.path.join(nfs_target_dir, tsv_filename)
    
    dircfg = cfg.get('directive', {})
    category = dircfg.get("CATEGORY", "")
    kingdom = dircfg.get("KINGDOM", "")
    plugin_id = cfg.get("file70", {}).get("plugin_id", 0)
    
    new_tsv_content_str = tsv_render(merged_rows, paths["full_slug"], plugin_id, category, kingdom)
    
    existing_tsv_content_str = u""
    if os.path.exists(nfs_dest_path):
        try:
            with io.open(nfs_dest_path, "r", encoding="utf-8") as f_exist: existing_tsv_content_str = f_exist.read()
        except IOError: pass

    if new_tsv_content_str != existing_tsv_content_str:
        info("TSV content differs, writing to {}".format(nfs_dest_path))
        if not args.dry_run:
            try:
                with io.open(nfs_dest_path, "w", encoding="utf-8") as f: f.write(new_tsv_content_str)
                made_local_changes = True; info("Vector TSV dictionary updated.")
            except IOError as e: err("Failed write TSV: {}".format(e))
        else: info("[DRY-RUN] TSV write skipped."); made_local_changes = True
    else: info("Vector TSV dictionary already synced.")

    return made_local_changes
# =========================================================

# =========================================================
# MAIN FUNCTION
# =========================================================
def main():
    args = parse_args()
    section("Load config")
    try: cfg = read_json(CFG_PATH)
    except (FileNotFoundError, IOError): die("Config file '{}' not found.".format(CFG_PATH)); return 1
    except (JSONDecodeError, ValueError): die("Config file '{}' is not valid JSON.".format(CFG_PATH)); return 1

    # --- [PATCH] Standardize Customer Config Loading (Anchor to Root) ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    customer_path = os.path.join(script_dir, "customer.json")

    info("Loading customer config from ROOT: {}".format(customer_path))
    try: 
        customer_cfg = read_json(customer_path)
        cfg.update(customer_cfg) 
    except FileNotFoundError: 
        warn("Customer config '{}' not found in root. Using default/placeholder.".format(customer_path))
    except (JSONDecodeError, ValueError): 
        err("Error decoding customer JSON '{}'.".format(customer_path))
    
    # --- [PATCH] Setup Customer Name for Commits & Email ---
    c_info = cfg.get("customer_info", {})
    raw_name = c_info.get("customer_name") or cfg.get("customer_name")
    if not raw_name or raw_name == "Nama Customer Anda":
        customer_name = "Unknown"
    else:
        customer_name = raw_name
    # --------------------------------------------------------

    info("Email config loading from env vars.")

    required_keys = ["es", "query", "layout", "file70", "directive", "github"]
    if not all(k in cfg for k in required_keys):
        die("Config '{}' missing required keys.".format(CFG_PATH)); return 1

    es_cfg, q_cfg, layout, file70, dircfg, gh_cfg = cfg["es"], cfg["query"], cfg["layout"], cfg["file70"], cfg["directive"], cfg["github"]

    if not GITHUB_TOKEN: die("GITHUB_TOKEN env var not set.", code=2)

    try: paths = gh_paths(layout.get("device"), layout.get("module"), layout.get("submodule"), layout.get("filter_key"))
    except KeyError as e: die("Layout missing key: {}".format(e)); return 1
    try: plugin_id = int(file70["plugin_id"])
    except (KeyError, ValueError): die("Invalid 'plugin_id'."); return 1

    siem_plugin_type = paths["full_slug"]
    category, kingdom, disabled = dircfg.get("CATEGORY"), dircfg.get("KINGDOM"), bool(dircfg.get("DISABLED", False))
    template_id = dircfg.get("template_id")
    if not template_id: die("Directive missing 'template_id'."); return 1

    env_repo = os.getenv("GITHUB_REPO"); env_branch = os.getenv("GITHUB_BRANCH")
    gh_repo = env_repo if env_repo else gh_cfg.get("repo")
    gh_branch = env_branch if env_branch else gh_cfg.get("branch", "main")
    template70_path = gh_cfg.get("template_path", "./template-70.js")
    registry_path = gh_cfg.get("plugin_registry_path", "plugin_id.json")

    if not gh_repo: die("GitHub repo not defined."); return 1

    info("Using GITHUB_REPO: {}".format(gh_repo))
    info("CFG: {}, Repo: {}@{}, Slug: {}".format(CFG_PATH, gh_repo, gh_branch, siem_plugin_type))

    needs_distribution = layout.get("needs_distribution", False)
    is_active_for_email = False
    active_plugins_file = './active_plugins.json'
    if os.path.exists(active_plugins_file):
        try:
            with io.open(active_plugins_file, 'r', encoding='utf-8') as f: active_list = json.load(f)
            if isinstance(active_list, list) and siem_plugin_type in active_list: is_active_for_email = True
        except (IOError, JSONDecodeError, ValueError): pass

    if needs_distribution and is_active_for_email: plugin_status = "Distribute (Active + Email)"
    elif needs_distribution and not is_active_for_email: plugin_status = "Distribute (Passive, No Email)"
    else: plugin_status = "Update Only (No Distribute, No Email)"
    info("Plugin Status: {}".format(plugin_status))

    section("Check Plugin ID Registry")
    reg_obj, _ = gh_get(gh_repo, gh_branch, GITHUB_TOKEN, registry_path, debug=args.debug)
    registry, found_in_reg = {}, False; reg_sha = None
    if reg_obj:
        reg_sha = reg_obj.get("sha")
        try: registry = json.loads(base64.b64decode(reg_obj.get("content","")).decode("utf-8"))
        except (TypeError, ValueError, base64.binascii.Error): pass
    if not isinstance(registry.get("used"), list): registry["used"] = []
    for item in registry.get("used", []):
        if item.get("siem_plugin_type") == siem_plugin_type:
            if int(item.get("plugin_id",0)) == plugin_id: found_in_reg = True; info("Plugin ID {} OK.".format(plugin_id)); break
            else: die("Conflict! Slug uses ID {}, registry has {}.".format(plugin_id, item.get("plugin_id")))
    if not found_in_reg and not args.dry_run:
        section("Auto-registering Plugin ID")
        registry["used"].append({"plugin_id": plugin_id, "siem_plugin_type": siem_plugin_type, "by": layout.get("device", "unknown")})
        registry["used"] = sorted(registry["used"], key=lambda x: int(x.get("plugin_id", 0)))
        gh_put(gh_repo, gh_branch, GITHUB_TOKEN, registry_path, json.dumps(registry, indent=2, ensure_ascii=False).encode("utf-8"),
               "[auto][{}] Register plugin_id {} for {}".format(customer_name, plugin_id, siem_plugin_type), sha=reg_sha, debug=args.debug, dry=args.dry_run)
        info("Plugin Registry push: OK")

    section("OpenSearch aggregation")
    titles, _, _ = fetch_titles(es_cfg, q_cfg, debug=args.debug)

    section("Fetch & Merge TSV from GitHub")
    tsv_obj, _ = gh_get(gh_repo, gh_branch, GITHUB_TOKEN, paths["tsv"], debug=args.debug)
    existing_rows, tsv_sha = [], None
    if tsv_obj:
        tsv_sha = tsv_obj.get("sha")
        try: content = base64.b64decode(tsv_obj.get("content", "")).decode("utf-8")
        except (TypeError, base64.binascii.Error): content = ""
        existing_rows, _ = tsv_parse(content)
        info("TSV exists (sha={}), rows={}".format(tsv_sha, len(existing_rows)))
    else: info("TSV not found (new file).")
    merged_rows, added_rows, _ = tsv_merge(existing_rows, titles)
    info("Total rows: {}, New events: {}".format(len(merged_rows), len(added_rows)))

    # Push TSV if changed
    if added_rows or not tsv_obj:
        tsv_content_bytes = tsv_render(merged_rows, siem_plugin_type, plugin_id, category, kingdom).encode('utf-8')
        # --- [PATCH] Update Commit Message ---
        gh_put(gh_repo, gh_branch, GITHUB_TOKEN, paths["tsv"], tsv_content_bytes,
               "[auto][{}] Update TSV for {}".format(customer_name, siem_plugin_type), sha=tsv_sha, debug=args.debug, dry=args.dry_run)
        info("TSV push: OK")

    if added_rows and is_active_for_email:
        send_notification_email(customer_name, dircfg.get("HEADER", paths["full_slug"]), added_rows)
    elif added_rows: info("Plugin is Passive or Update Only. Skipping email.")

    section("Sync GitHub JSON Dictionary")
    json_obj, _ = gh_get(gh_repo, gh_branch, GITHUB_TOKEN, paths["json_dict"], debug=args.debug)
    new_json_content_str = write_json_dictionary(merged_rows);
    try: new_data_obj = json.loads(new_json_content_str)
    except (ValueError, JSONDecodeError): die("Failed generate valid new JSON dict."); return 1
    existing_data_obj = {}
    if json_obj and json_obj.get("content"):
        try: existing_data_obj = json.loads(base64.b64decode(json_obj.get("content", "")).decode("utf-8"))
        except (JSONDecodeError, TypeError, ValueError, base64.binascii.Error): pass
    if new_data_obj != existing_data_obj:
        info("JSON dict differs. Pushing sync...")
        # --- [PATCH] Update Commit Message ---
        gh_put(gh_repo, gh_branch, GITHUB_TOKEN, paths["json_dict"], new_json_content_str.encode('utf-8'),
               "[auto][{}] Sync JSON dict for {}".format(customer_name, siem_plugin_type), sha=json_obj.get("sha") if json_obj else None, debug=args.debug, dry=args.dry_run)
        info("JSON Dict push: OK")
    else: info("JSON Dict already synced.")

    section("Update 70.conf (if missing)")
    conf70_obj, _ = gh_get(gh_repo, gh_branch, GITHUB_TOKEN, paths["conf70"], debug=args.debug)
    if not conf70_obj:
        info("70.conf missing. Generating and pushing...")
        if not LOGSTASH_JSON_DICT_DIR: die("LOGSTASH_JSON_DICT_DIR env var not set."); return 1
        json_path_on_server = os.path.join(LOGSTASH_JSON_DICT_DIR, "{}_plugin-sids.json".format(siem_plugin_type))
        field_no_keyword = q_cfg.get("field","").replace(".keyword", "")
        if not field_no_keyword: die("Query field missing."); return 1
        device_name = layout.get("device")
        if not device_name: die("Layout device missing."); return 1
        conf70_text = generate_conf70_from_template(template70_path, plugin_id, device_name, siem_plugin_type, field_no_keyword, category, json_path_on_server)
        # --- [PATCH] Update Commit Message ---
        gh_put(gh_repo, gh_branch, GITHUB_TOKEN, paths["conf70"], conf70_text.encode('utf-8'),
               "[auto][{}] Create 70.conf for {}".format(customer_name, siem_plugin_type), sha=None, debug=args.debug, dry=args.dry_run)
        info("70.conf push: CREATED")
    else: info("70.conf already exists.")

    section("Sync GitHub Directives")
    template_map = load_directive_templates("./directive_rules.json")
    dir_obj, _ = gh_get(gh_repo, gh_branch, GITHUB_TOKEN, paths["directive"], debug=args.debug)
    existing_dir, dir_sha = (OrderedDict([("directives", [])]), None)
    if dir_obj:
        dir_sha = dir_obj.get("sha")
        try: existing_dir = json.loads(base64.b64decode(dir_obj.get("content","")).decode("utf-8"), object_pairs_hook=OrderedDict)
        except (JSONDecodeError, ValueError, TypeError, base64.binascii.Error): pass
    if not isinstance(existing_dir.get("directives"), list): existing_dir["directives"] = []

    directive_header = dircfg.get("HEADER")
    if not directive_header: directive_header = siem_plugin_type

    updated_dir_json, appended, add_count, _ = directive_append(existing_dir, template_map, template_id, plugin_id, directive_header, category, kingdom, disabled, merged_rows)
    if appended:
        info("Directives differ ({} new). Pushing sync...".format(add_count))
        directive_bytes = json.dumps(updated_dir_json, indent=2, ensure_ascii=False).encode('utf-8')
        # --- [PATCH] Update Commit Message ---
        gh_put(gh_repo, gh_branch, GITHUB_TOKEN, paths["directive"], directive_bytes,
               "[auto][{}] Sync directives for {}".format(customer_name, siem_plugin_type), sha=dir_sha, debug=args.debug, dry=args.dry_run)
        info("Directives push: OK")
    else: info("Directives already synced.")

    made_local_changes = False 
    distribution_target = layout.get("distribution_target", "Logstash")

    if needs_distribution and (added_rows or appended):
        info("Distribution enabled (Target: {}) and changes detected...".format(distribution_target))
        if distribution_target == "Logstash":
            made_local_changes = distribute_logstash_local(merged_rows, paths, cfg, plugin_id, template_map, template_id, args)
        elif distribution_target == "Vector":
            made_local_changes = distribute_vector_local(merged_rows, paths, cfg, args)
        else:
            warn("Target '{}' unknown. Skipping local distribution.".format(distribution_target))
    elif needs_distribution:
        info("Distribution enabled, but no new events. Skipping local.")
    else: 
        info("Plugin is 'Update Only'. Skipping local.")

    section("Summary")
    info("DONE.")

    if needs_distribution and made_local_changes:
        info("Local changes detected. Signaling restart.")
        return 5 
    else:
        info("No restart needed.")
        return 0

if __name__ == "__main__":
    exit_code = 99
    try: exit_code = main()
    except SystemExit as e: exit_code = e.code if isinstance(e.code, int) else 1
    except Exception as e:
        err("Unexpected error: {}".format(e))
        traceback.print_exc()
    finally: sys.exit(exit_code)