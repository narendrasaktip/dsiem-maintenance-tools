#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import re  
import sys
import json
import base64
import requests
import shutil
import subprocess
import argparse
from collections import OrderedDict
import io 
import datetime 
import time

# --- Penyesuaian Kompatibilitas Py2/Py3 ---
try: JSONDecodeError = json.JSONDecodeError 
except AttributeError: JSONDecodeError = ValueError          
try: FileNotFoundError 
except NameError: FileNotFoundError = IOError           
# --- AKHIR BLOK KOMPATIBILITAS ---

# ====== KONFIGURASI ENV ======
GITHUB_REPO   = os.getenv("GITHUB_REPO")
GITHUB_TOKEN  = os.getenv("GITHUB_TOKEN")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH")
OUT_DIR = os.getenv("OUT_DIR", "./pulled_configs")

# ====== KONFIGURASI DISTRIBUSI & RESTART ======
LOGSTASH_PIPE_DIR = None
LOGSTASH_JSON_DICT_DIR = None
LOGSTASH_HOME     = None
VECTOR_CONFIG_BASE_DIR = None
NFS_BASE_DIR           = None
FRONTEND_POD      = "dsiem-frontend-0"
BACKEND_POD       = "dsiem-backend-0"
VECTOR_POD_LABEL  = "app=vector-parser"

# Konstanta untuk navigasi
LAST_SELECTION_FILE = "last_selection.json"
CUSTOMER_FILE = "customer.json"
BACK_COMMAND = "__BACK__"
DRY_RUN = False

# ====== KONSTANTA REPORTING ======
REPORT_DATA_FILE = "integrations.json" 
ACTIVE_PLUGINS_FILE = "active_plugins.json"
REPORT_DIR = "monitoring-integration" 
REPORT_FILENAME_SUFFIX = "_integration-status.md" 

# ====== I/O & HELPERS ======
def print_header(title):
    print("\n" + "="*60); print("=== {}".format(title.upper())); print("="*60)
def die(msg, code=1):
    print("\n[ERROR] {}".format(msg)); sys.exit(code)

# Fungsi I/O yang krusial
def safe_save_json(path, obj):
    """Menyimpan data, mendukung JSON dan String (Markdown)."""
    print("[FILE] Menyiapkan untuk menyimpan: {}".format(path))
    if DRY_RUN:
        print("    -> [DRY RUN] Penulisan file dilewati.")
        return True
    try:
        # [FIX] Penanganan Unicode yang lebih robust untuk Python 2
        if isinstance(obj, str) or (sys.version_info[0] < 3 and isinstance(obj, unicode)):
             try: 
                if isinstance(obj, unicode): obj = obj
                else: obj = obj.decode('utf-8')
             except (NameError, AttributeError, UnicodeError): pass
             
             with io.open(path, 'w', encoding='utf-8') as f:
                 f.write(obj); f.write(u'\n')
             print("    -> [OK] Berhasil disimpan (Text).")
             return True
             
        json_string = json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False)
        
        # [FIX] Decode bytes ke unicode explicit untuk json dump di Py2
        try: 
            if isinstance(json_string, str): json_string = json_string.decode('utf-8')
        except (NameError, AttributeError, UnicodeError): pass

        with io.open(path, 'w', encoding='utf-8') as f:
            f.write(json_string); f.write(u'\n')
        print("    -> [OK] Berhasil disimpan (JSON).")
        return True
    except Exception as e:
        print("    -> [ERROR] Gagal menyimpan file {}: {}".format(path, e))
        return False

def safe_copy(src, dst_dir_or_file):
    import shutil
    print("[FILE] Menyiapkan untuk menyalin '{}' ke '{}'".format(src, dst_dir_or_file))
    if DRY_RUN:
        print("    -> [DRY RUN] Penyalinan dilewati.")
        return True
    try:
        shutil.copy(src, dst_dir_or_file)
        print("    -> [OK] Berhasil disalin.")
        return True
    except Exception as e:
        print("    -> [ERROR] Gagal menyalin: {}".format(e))
        return False

def safe_makedirs(path):
    import os
    if os.path.exists(path): return True
    print("[FILE] Menyiapkan untuk membuat direktori: {}".format(path))
    if DRY_RUN:
        print("    -> [DRY RUN] Pembuatan direktori dilewati.")
        return True
    try:
        os.makedirs(path)
        print("    -> [OK] Direktori berhasil dibuat.")
        return True
    except Exception as e:
        print("[ERROR] Gagal membuat direktori: {}".format(e))
        return False

def safe_run_cmd(cmd, cwd=None, shell=False):
    import subprocess
    cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
    print("\n[CMD] Menyiapkan untuk menjalankan: {}".format(cmd_str))
    if DRY_RUN:
        print("    -> [DRY RUN] Eksekusi perintah dilewati.")
        return True
    try:
        p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
        out, err = p.communicate()
        if out: print("--- stdout ---\n{}".format(out.decode('utf-8', 'replace')))
        if err: print("--- stderr ---\n{}".format(err.decode('utf-8', 'replace')))
        return p.returncode == 0
    except OSError as e:
        print("[ERROR] Gagal menjalankan perintah '{}': {}".format(cmd_str, e))
        return False

try: input = raw_input
except NameError: pass

def ask_yes_no(p, allow_back=False):
    prompt = p; options = "(y/n)"; valid_chars = set(['y', 'n'])
    if allow_back: options = "(y/n/b untuk kembali)"; valid_chars.add('b')
    prompt += " {}: ".format(options)
    while True:
        a = input(prompt).strip().lower()
        if a in valid_chars:
            if a == 'b': return BACK_COMMAND
            return a
        print("Pilihan tidak valid. Harap masukkan {}".format('/'.join(sorted(list(valid_chars)))))

def ask_for_path(prompt, default_value):
    while True:
        user_input = input("{} [Default: {}]: ".format(prompt, default_value)).strip()
        if not user_input: return default_value
        if not user_input.startswith('/'):
            print("[ERROR] Harap masukkan path absolut (dimulai dengan '/').")
            continue
        return user_input

def update_config_sh(paths_to_update):
    CONFIG_SH_PATH = './config.sh'
    print("[CONFIG] Memperbarui {}...".format(CONFIG_SH_PATH))
    
    # Buat file jika belum ada (untuk menghindari error read)
    if not os.path.exists(CONFIG_SH_PATH):
        try:
            with io.open(CONFIG_SH_PATH, 'w', encoding='utf-8') as f:
                f.write(u"#!/bin/bash\n")
        except Exception as e:
            print("[WARN] Gagal membuat config.sh baru: {}".format(e))
            return False

    try:
        with io.open(CONFIG_SH_PATH, 'r', encoding='utf-8') as f: lines = f.readlines()
        new_lines = []; export_re = re.compile(r'^(export\s+)([A-Za-z_][A-Za-z0-9_]+)=("?)(.*?)("?\s*)$')
        updated_keys = set()
        
        # 1. Update baris yang sudah ada
        for line in lines:
            match = export_re.match(line)
            if match:
                key = match.group(2)
                if key in paths_to_update:
                    new_value = paths_to_update[key]
                    # [FIX] Tambahkan 'u' di depan string agar menjadi Unicode
                    try:
                        if isinstance(new_value, str): new_value = new_value.decode('utf-8')
                    except: pass
                    
                    new_line = u'export {}="{}"\n'.format(key, new_value)
                    new_lines.append(new_line)
                    updated_keys.add(key)
                    print("    -> Memperbarui {}...".format(key))
                    continue
            new_lines.append(line)
        
        # 2. Tambahkan variabel baru yang belum ada (APPEND)
        for key, value in paths_to_update.items():
            if key not in updated_keys:
                print("    -> Menambahkan variabel baru: {}".format(key))
                # [FIX] Decode value jika masih bytes, dan gunakan format string Unicode
                try:
                    if isinstance(value, str): value = value.decode('utf-8')
                except: pass
                
                new_lines.append(u'export {}="{}"\n'.format(key, value))

        if not DRY_RUN:
            with io.open(CONFIG_SH_PATH, 'w', encoding='utf-8') as f: f.writelines(new_lines)
            print("[CONFIG] {} berhasil diperbarui.".format(CONFIG_SH_PATH))
            return True
        else:
            print("[DRY RUN] Perubahan {} dilewati.".format(CONFIG_SH_PATH))
            return True
    except Exception as e:
        print("[ERROR] Gagal menulis ke {}: {}".format(CONFIG_SH_PATH, e))
        return False

def reload_global_paths():
    global LOGSTASH_PIPE_DIR, LOGSTASH_JSON_DICT_DIR, LOGSTASH_HOME
    global VECTOR_CONFIG_BASE_DIR, NFS_BASE_DIR
    
    print("[INFO] Memuat ulang variabel path dari environment...")
    
    # Reload file config.sh agar env vars masuk ke os.environ
    if os.path.exists("./config.sh"):
        command = ['bash', '-c', 'source ./config.sh && env']
        proc = subprocess.Popen(command, stdout=subprocess.PIPE)
        for line in proc.stdout:
            (key, _, value) = line.partition(b"=")
            os.environ[key.decode('utf-8').strip()] = value.decode('utf-8').strip()

    LOGSTASH_PIPE_DIR = os.getenv("LOGSTASH_PIPE_DIR")
    LOGSTASH_JSON_DICT_DIR = os.getenv("LOGSTASH_JSON_DICT_DIR")
    LOGSTASH_HOME     = os.getenv("LOGSTASH_HOME")
    VECTOR_CONFIG_BASE_DIR = os.getenv("VECTOR_CONFIG_BASE_DIR")
    NFS_BASE_DIR           = os.getenv("NFS_BASE_DIR")
    
def load_json_safe(path):
    if not os.path.exists(path): return None
    try:
        with io.open(path, 'r', encoding='utf-8') as f: return json.load(f)
    except Exception as e:
        print("[ERROR] Gagal membaca {}: {}".format(path, e))
        return None

def get_active_plugins():
    data = load_json_safe(ACTIVE_PLUGINS_FILE)
    if isinstance(data, list): return set(data)
    return set()

# FUNGSI PENTING: Mendapatkan status display (Harus sinkron dengan 03.manage_distributed.py)
def get_status_display_sync(item_needs_dist, item_target):
    if item_target == 'None':
        return u"❌ Target Not Found"
    elif item_needs_dist:
        return u"⚠️ Local Only"
    else:
        return u"✅ Active"

def scan_integrations_for_current_customer():
    jobs = load_json_safe('master_jobs.json')
    if not jobs: return []

    active_slugs = get_active_plugins()
    integrations = []

    for job_path in jobs:
        if not os.path.exists(job_path): continue
        job_data = load_json_safe(job_path)
        if not job_data: continue

        filename = os.path.basename(job_path)
        slug = filename.replace("_updater.json", "")
        layout = job_data.get("layout", {})
        
        target = layout.get("distribution_target", "None")
        needs_dist = layout.get("needs_distribution", False)
        is_active = slug in active_slugs
        
        # MENGGUNAKAN STATUS SINKRON DENGAN 03.MANAGE_DISTRIBUTED.PY
        status_text = get_status_display_sync(needs_dist, target)
        notif_str = u"AKTIF (Email)" if is_active else u"Pasif"

        try: last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(job_path)).strftime('%Y-%m-%d %H:%M')
        except: last_modified = "N/A"

        integrations.append({
            "slug": slug, "target": target, "status_text": status_text, 
            "notification": notif_str, "last_modified": last_modified,
            "needs_dist": needs_dist, "is_active": is_active
        })
    return integrations

def get_gmt_plus_7_timestamp():
    utc_dt = datetime.datetime.utcnow()
    gmt_plus_7_dt = utc_dt + datetime.timedelta(hours=7)
    return gmt_plus_7_dt.strftime("%Y-%m-%d %H:%M:%S WIB (GMT+7)")

def update_integration_report(customer_name, new_integrations):
    customer_slug = re.sub(r'[^a-z0-9]+', '-', customer_name.lower()).strip('-')
    if not customer_slug: customer_slug = "unknown-customer"

    current_timestamp = get_gmt_plus_7_timestamp()
    full_report = load_json_safe(REPORT_DATA_FILE)
    if not isinstance(full_report, dict): full_report = {}

    integration_map = {}
    for item in new_integrations:
        temp_item = item.copy()
        temp_item.pop('last_modified', None) 
        integration_map[item['slug']] = temp_item

    current_customer_data = {
        "customer_name": customer_name, "last_updated": current_timestamp,
        "integrations": integration_map
    }

    full_report[customer_slug] = current_customer_data
    safe_save_json(REPORT_DATA_FILE, full_report)
    print("[REPORT] Data mentah berhasil diupdate dan disimpan: {}".format(REPORT_DATA_FILE))
    
    markdown_filename = "{}{}".format(customer_slug, REPORT_FILENAME_SUFFIX)
    safe_makedirs(REPORT_DIR)
    local_markdown_path = os.path.join(REPORT_DIR, markdown_filename)
    markdown_content = generate_single_markdown_report_sync(current_customer_data, new_integrations)
    safe_save_json(local_markdown_path, markdown_content)
    print("[REPORT] File laporan Markdown berhasil dibuat: {}".format(local_markdown_path))
    return local_markdown_path

def generate_single_markdown_report_sync(cust_data, all_integrations):
    """Fungsi generasi laporan Markdown yang sinkron (menggunakan logika 03)."""
    lines = []; timestamp = cust_data['last_updated']
    customer_name = cust_data['customer_name']; integrations_map = cust_data['integrations']
    
    lines.append(u"# Status Integrasi SIEM - {}".format(customer_name))
    lines.append(u"\n**Waktu Pemrosesan Terakhir:** {}\n".format(timestamp))
    lines.append(u"---")
    
    # === TABEL DETAIL ===
    lines.append(u"\n## ⚙️ Detail Integrasi Terdaftar")

    # Kolom baru: Aksi/Status (Menggabungkan kebutuhan pull dan target)
    header = u"| Plugin / Device (Slug) | Target Engine | Status Distribusi | Notifikasi Email | Kesimpulan Aksi Wajib |"
    separator = u"| :--- | :---: | :---: | :---: | :--- |"
    lines.append(header); lines.append(separator)

    # Variabel untuk Kesimpulan Umum
    needs_pull_count = sum(1 for item in all_integrations if item['needs_dist'] and item['target'] != 'None')
    none_target_count = sum(1 for item in all_integrations if item['target'] == 'None')

    if not integrations_map:
        lines.append(u"| *Belum ada integrasi yang terdaftar.* | - | - | - | - |")
    else:
        sorted_integrations = sorted(all_integrations, key=lambda x: x['slug'])
        for item in sorted_integrations:
             # MENGGUNAKAN LOGIKA STATUS DISPLAY SINKRON DENGAN 03
             status_text = get_status_display_sync(item['needs_dist'], item['target'])
             notification = item['is_active'] and u"🔔 AKTIF" or u"🔕 PASIF"
             
             # Menentukan Kesimpulan Aksi Wajib Inline
             if item['target'] == 'None':
                 action_summary = "SET TARGET (RUN 01)"
             elif item['needs_dist']:
                 action_summary = "RUN 01.PULL-DIRECTIVE.PY"
             else:
                 action_summary = "N/A"
             
             row = u"| **{}** | {} | {} | {} | **{}** |".format(
                item['slug'], item['target'], status_text, notification, action_summary
            )
             lines.append(row)
    lines.append(u"---")

    # === BAGIAN KESIMPULAN UMUM (SAMA PERSIS DENGAN 03) ===
    lines.append(u"\n## 🧠 Ringkasan Status Global & Aksi")
    
    if none_target_count > 0:
         lines.append(u"\n* **❌ Target Belum Diset (Kritis):** Terdapat **{}** plugin yang memiliki **Target Engine: None**.".format(none_target_count))
         lines.append(u"  * **Aksi Wajib:** Target harus ditetapkan saat menjalankan **`01.pull-directive.py`**.")
    
    if needs_pull_count > 0:
        lines.append(u"\n* **⚠️ Status Pull/Deploy:** Terdapat **{}** plugin yang sudah memiliki target tetapi ditandai **⚠️ Local Only**.".format(needs_pull_count))
        lines.append(u"  * **Aksi Wajib:** Jalankan **`01.pull-directive.py`** untuk mendorong perubahan ke pipeline.")

    if none_target_count == 0 and needs_pull_count == 0:
         lines.append(u"\n* **Status Final:** Semua {} plugin berada dalam status **✅ Active**.".format(len(all_integrations)))
    
    active_notif_count = sum(1 for item in all_integrations if item['is_active'])
    if active_notif_count > 0:
        lines.append(u"\n* **🔔 Notifikasi Email:** Sebanyak **{}** plugin memiliki notifikasi email yang **AKTIF**.".format(active_notif_count))
    else:
        lines.append(u"\n* **🔕 Notifikasi Email:** Semua plugin ditandai **PASIF**.")
    
    return u"\n".join(lines)


def gh_api_put_file(file_path):
    import requests, base64
    if DRY_RUN: print("[GH UPLOAD] [DRY RUN] Upload file '{}' dilewati.".format(file_path)); return True
    if not GITHUB_REPO or not GITHUB_TOKEN:
        print("[GH UPLOAD] [ERROR] GITHUB_REPO/TOKEN tidak diset. Upload dilewati."); return False

    remote_path = os.path.join(REPORT_DIR, os.path.basename(file_path)).replace("\\", "/")
    url = "https://api.github.com/repos/{}/contents/{}".format(GITHUB_REPO, remote_path)

    try:
        with open(file_path, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode('utf-8')
    except Exception as e:
        print("[GH UPLOAD] [ERROR] Gagal membaca file lokal: {}".format(e)); return False

    sha = None; r_put = None
    try:
        r_get = requests.get(url, headers=gh_headers(), params={"ref": GITHUB_BRANCH}, timeout=30)
        if r_get.status_code == 200: sha = r_get.json().get('sha')
    except requests.exceptions.RequestException as e: pass

    customer_name = get_customer_name()
    commit_msg = "AUTOREPORT: Update status report for {} ({})".format(customer_name, os.path.basename(file_path))
    payload = { "message": commit_msg, "content": content_b64, "branch": GITHUB_BRANCH or "main" }
    if sha: payload['sha'] = sha

    print("[GH UPLOAD] Mengupload '{}' ke '{}'...".format(file_path, GITHUB_REPO))
    try:
        r_put = requests.put(url, headers=gh_headers(), json=payload, timeout=60)
        r_put.raise_for_status()
        print("[GH UPLOAD] [OK] Berhasil diupload.")
        return True
    except requests.exceptions.RequestException as e:
        print("[GH UPLOAD] [FATAL] Gagal mengupload file ke GitHub: {}".format(e))
        if r_put is not None: print("    -> Response: {}".format(r_put.text))
        return False

def setup_customer_info():
    print_header("Konfigurasi Customer")
    customer_name = ""

    if os.path.exists(CUSTOMER_FILE):
        try:
            with io.open(CUSTOMER_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f); customer_name = data.get("customer_info", {}).get("customer_name", "")
        except (IOError, JSONDecodeError):
            print("[WARN] File customer.json tidak bisa dibaca. Akan dibuat ulang."); customer_name = ""

    if not customer_name or customer_name == "Nama Customer Anda":
        if not customer_name: print("[INFO] File customer.json belum ada.")
        else: print("[INFO] Nama customer masih placeholder.")

        while True:
            new_name = input("Masukkan Nama Customer baru: ").strip()
            if new_name and new_name != "Nama Customer Anda":
                customer_name = new_name; break
            print("[ERROR] Nama customer tidak boleh kosong atau placeholder.")

        data_to_save = {"customer_info": {"customer_name": customer_name}}
        safe_save_json(CUSTOMER_FILE, data_to_save)
    else:
        print("[OK] Nama customer sudah dikonfigurasi: '{}'".format(customer_name))
    return customer_name

def get_customer_name():
    data = load_json_safe(CUSTOMER_FILE)
    if data: return data.get("customer_info", {}).get("customer_name", "Unknown Customer")
    return "Belum Dikonfigurasi"

# ====== FUNGSI GITHUB ======
def require_github():
    if not GITHUB_REPO or not GITHUB_TOKEN:
        die("Set GITHUB_REPO='owner/repo' dan GITHUB_TOKEN='ghp_xxx' dulu ya.")

def gh_headers():
    return { "Accept": "application/vnd.github+json", "Authorization": "Bearer {}".format(GITHUB_TOKEN), "X-GitHub-Api-Version": "2022-11-28" }

def gh_api_get(path):
    import requests
    clean_path = path.replace("\\", "/").lstrip('/')
    url = "https://api.github.com/repos/{}/contents/{}".format(GITHUB_REPO, clean_path)
    try:
        r = requests.get(url, headers=gh_headers(), params={"ref": GITHUB_BRANCH}, timeout=60)
        if r.status_code == 404: return None
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print("\n[ERROR] Gagal menghubungi GitHub API ({}): {}".format(url, e))
        return None

def find_parent_devices():
    print("[INFO] Mencari perangkat induk di repositori...")
    items = gh_api_get("")
    if not isinstance(items, list):
        print("[ERROR] Gagal mendapatkan daftar dari root repositori.")
        return []
    
    # --- PERBAIKAN: Mengabaikan direktori laporan (REPORT_DIR) ---
    devices = sorted([
        item['name'] 
        for item in items 
        if item.get('type') == 'dir' 
        and not item.get('name', '').startswith('.')
        and item.get('name') != REPORT_DIR  # Whitelist: Abaikan 'monitoring-integration'
    ])
    # --- AKHIR PERBAIKAN ---
    
    print("[INFO] Ditemukan {} perangkat induk.".format(len(devices)))
    return devices

def find_plugins_in_parent(parent_path, current_path=""):
    full_path = os.path.join(parent_path, current_path).replace("\\", "/")
    items = gh_api_get(full_path)
    if not isinstance(items, list): return []

    found_plugins = []
    
    # [LOGIC BARU]
    # Cek apakah folder ini punya ciri-ciri plugin (ada config.json ATAU ada file .tsv)
    has_config = any(item.get('type') == 'file' and item.get('name') == 'config.json' for item in items)
    has_tsv = any(item.get('type') == 'file' and item.get('name', '').endswith('_plugin-sids.tsv') for item in items)

    # Kuncinya disini: KITA HAPUS "and not has_subdirs"
    # Jadi mau dia punya anak atau cucu, kalo dia punya file config/tsv, dia dianggap plugin.
    if has_config or has_tsv: 
        found_plugins.append(full_path)

    # Tetep cari ke dalem buat listing submodul lainnya
    for item in items:
        if item.get('type') == 'dir':
            new_relative_path = os.path.join(current_path, item['name']).replace("\\", "/")
            found_plugins.extend(find_plugins_in_parent(parent_path, new_relative_path))
    return found_plugins

def download_and_save(remote_path, local_path):
    import base64
    print("\n[*] Mencoba mengunduh: {}".format(remote_path))
    file_meta = gh_api_get(remote_path)
    if file_meta is None or 'content' not in file_meta:
        print("    -> [INFO] File tidak ditemukan atau konten kosong, dilewati.")
        return None, None
    try:
        content_b64 = file_meta.get("content", "")
        content_bytes = base64.b64decode(content_b64)
        local_dir = os.path.dirname(local_path)
        safe_makedirs(local_dir)

        print("    -> [FILE] Menyiapkan untuk menyimpan: {}".format(local_path))
        if not DRY_RUN:
            with open(local_path, "wb") as f: f.write(content_bytes)
        else:
            print("    -> [DRY RUN] Penulisan file dilewati.")

        full_slug = None
        if remote_path.endswith('_plugin-sids.tsv'):
            full_slug = os.path.basename(remote_path).replace('_plugin-sids.tsv', '')

        return local_path, full_slug
    except (TypeError, base64.binascii.Error) as e:
         print("    -> [ERROR] Gagal decode base64 dari {}: {}".format(remote_path, e))
         return None, None
    except Exception as e:
        print("    -> [ERROR] Gagal memproses/menyimpan file {}: {}".format(local_path, e))
        return None, None

# ====== FUNGSI ALUR BARU (NEW FLOW) ======
def select_from_list(options, title, can_go_back=False):
    print_header(title)
    if not options: print("[WARN] Tidak ada pilihan tersedia."); return None
    if can_go_back: print("0. Kembali ke langkah sebelumnya")
    for i, option in enumerate(options, 1): print("{}. {}".format(i, option))
    while True:
        prompt = "Pilihan Anda [1-{} {}]: ".format(len(options), "atau 0" if can_go_back else "")
        choice = input(prompt).strip()
        if can_go_back and choice == '0': return BACK_COMMAND
        if choice.isdigit():
             idx = int(choice) - 1
             if 0 <= idx < len(options): return options[idx]
        print("[ERROR] Pilihan tidak valid.")

def select_plugins_from_list(available_plugins, title):
    print_header(title)
    if not available_plugins: print("[WARN] Tidak ada plugin tersedia."); return []
    for i, plugin in enumerate(available_plugins, 1): print("{}. {}".format(i, plugin))
    while True:
        choice_str = input("\nMasukkan nomor (cth: 1, 3, 5-7) atau 'b' kembali: ").strip().lower()
        if choice_str == 'b': return BACK_COMMAND
        if not choice_str: continue
        selected_indices = set(); valid = True
        for part in choice_str.split(','):
            part = part.strip();
            if not part: continue
            if '-' in part:
                try:
                    start, end = map(int, part.split('-'))
                    if start > end or not (1 <= start <= len(available_plugins)) or not (1 <= end <= len(available_plugins)): raise ValueError
                    selected_indices.update(range(start - 1, end))
                except ValueError: print("[ERROR] Rentang '{}' tidak valid.".format(part)); valid = False; break
            else:
                try:
                    idx = int(part) - 1
                    if not (0 <= idx < len(available_plugins)): raise ValueError
                    selected_indices.add(idx)
                except ValueError: print("[ERROR] Pilihan '{}' tidak valid.".format(part)); valid = False; break
        if valid and selected_indices: return sorted([available_plugins[i] for i in selected_indices])
        elif valid: print("[ERROR] Tidak ada nomor valid yang dimasukkan.")
def select_passive_scope(focal_plugin_path):
    print_header("Cakupan Sinkronisasi Pasif")
    print("Fokus utama: '{}'".format(focal_plugin_path))
    print("Plugin lain dalam cakupan akan diunduh & didaftarkan, tapi tidak didistribusi.")
    parts = focal_plugin_path.split('/')
    options = [("none", "Jangan sertakan plugin lain.")]
    if len(parts) >= 4: options.append(("submodule", "Sertakan semua di submodule '{}'.".format("/".join(parts[:3]))))
    if len(parts) >= 3: options.append(("module", "Sertakan semua di modul '{}'.".format("/".join(parts[:2]))))
    if len(parts) >= 2: options.append(("parent", "Sertakan semua di parent '{}'.".format(parts[0])))
    for i, (_, desc) in enumerate(options, 1): print("{}. {}".format(i, desc))
    while True:
        choice = input("Pilihan Anda [1-{}]: ".format(len(options))).strip()
        if choice.isdigit() and 1 <= int(choice) <= len(options):
            scope_key, _ = options[int(choice) - 1]
            if scope_key == "parent": return "parent", parts[0]
            if scope_key == "module": return "module", "/".join(parts[:2])
            if scope_key == "submodule": return "submodule", "/".join(parts[:3])
            return "none", None
        print("[ERROR] Pilihan tidak valid.")

def select_active_notifications(focal_plugins):
    print_header("Aktivasi Notifikasi Email (Hanya Fokal)")
    if not focal_plugins: print("[INFO] Tidak ada plugin fokal untuk dipilih."); return []
    print("Pilih plugin FOKAL yang ingin diaktifkan notifikasinya.\n")
    for i, plugin in enumerate(focal_plugins, 1): print("{}. {}".format(i, plugin))
    print("\nPilihan: Nomor (1, 3), 'A' (Semua), 'b' (Kembali), Enter (Tidak ada)")
    while True:
        choice_str = input("\nPlugin yang notifikasinya aktif: ").strip().lower()
        if not choice_str: return []
        if choice_str == 'b': return BACK_COMMAND
        if choice_str == 'a': return focal_plugins
        selected_indices = set(); valid = True
        for part in choice_str.split(','):
            part = part.strip();
            if not part: continue
            if not part.isdigit(): valid = False; break
            idx = int(part) - 1
            if not (0 <= idx < len(focal_plugins)): valid = False; break
            selected_indices.add(idx)
        if valid and selected_indices: return sorted([focal_plugins[i] for i in selected_indices])
        elif valid: print("[ERROR] Tidak ada nomor valid yang dimasukkan.")
        else: print("[ERROR] Pilihan '{}' tidak valid.".format(choice_str))

def display_summary(selection):
    print_header("Ringkasan Pekerjaan")
    focal = selection.get('focal_plugins', [])
    passive = selection.get('passive_plugins', [])
    total = selection.get('plugins_to_process', [])
    active_notif = selection.get('active_plugins', [])
    print("Plugin Fokal         : {} plugin".format(len(focal)))
    for p in focal: print("    - {}".format(p))
    print("Cakupan Pasif         : {}".format(selection.get('passive_scope_desc', 'N/A')))
    print("Total Plugin Proses  : {} ({} fokal + {} pasif)".format(len(total), len(focal), len(passive)))
    print("Aksi Utama           : {}".format(selection.get('action', 'N/A')))
    print("Plugin Aktif (Notif) : {} dari {} fokal".format(len(active_notif), len(focal)))
    for p in active_notif: print("    - {}".format(p))
    print("="*60)

# ====== FUNGSI PROSES & DISTRIBUSI ======
def process_plugin(plugin_path):
    print_header("Mengunduh Plugin (Files Only): {}".format(plugin_path))
    
    # Ambil daftar isi folder dari GitHub
    items = gh_api_get(plugin_path)
    if not isinstance(items, list): return None

    local_plugin_dir = os.path.join(OUT_DIR, plugin_path)
    safe_makedirs(local_plugin_dir)

    full_slug = None
    downloaded_files = {"path": plugin_path}
    successful_downloads = 0

    # [LOGIC DOWNLOAD "DANGKAL"]
    for item in items:
        # Kalo dia FOLDER --> SKIP / ABAIKAN (Sesuai request)
        if item.get('type') == 'dir':
            continue 
            
        # Kalo dia FILE --> DOWNLOAD
        if item.get('type') == 'file':
            remote_file_path = item.get('path')
            local_file_path = os.path.join(local_plugin_dir, item.get('name'))
            
            saved_path, slug_detected = download_and_save(remote_file_path, local_file_path)
            
            if saved_path: 
                successful_downloads += 1
                # Coba tangkap slug dari file tsv kalo ketemu
                if slug_detected and not full_slug: full_slug = slug_detected
                
                # Mapping file untuk keperluan distribusi nanti
                fname = item.get('name')
                if fname.endswith('_updater.json'): downloaded_files["updater_cfg"] = saved_path
                elif fname.endswith('_plugin-sids.tsv'): downloaded_files["tsv"] = saved_path
                elif fname.endswith('_plugin-sids.json'): downloaded_files["json_dict"] = saved_path
                elif fname.startswith('70_dsiem-plugin_') and fname.endswith('.conf'): downloaded_files["conf70"] = saved_path
                elif fname.startswith('70_transform_') and fname.endswith('.yaml'): downloaded_files["vector_conf"] = saved_path
                elif fname.startswith('directives_') and fname.endswith('.json'): downloaded_files["directive"] = saved_path

    # Fallback: Kalo slug gak ketemu dari fungsi download (misal tsv gak kedownload), coba cari manual
    if not full_slug and "tsv" in downloaded_files:
        full_slug = os.path.basename(downloaded_files["tsv"]).replace('_plugin-sids.tsv', '')

    if not full_slug:
        # Coba cari dari nama updater json
        if "updater_cfg" in downloaded_files:
             full_slug = os.path.basename(downloaded_files["updater_cfg"]).replace('_updater.json', '')
        else:
            print("[WARN] Gagal menentukan 'full_slug'. Plugin mungkin tidak lengkap.")
            return None

    print("[INFO] Ditemukan 'full_slug': {}".format(full_slug))
    downloaded_files["full_slug"] = full_slug

    if "updater_cfg" in downloaded_files:
        print("[INFO] Unduhan selesai. Total {} file (Subfolder diabaikan).".format(successful_downloads))
        return downloaded_files
    else:
        print("[ERROR] File krusial '_updater.json' tidak ditemukan di root folder ini.")
        return None

def distribute_logstash(downloaded_files):
    print_header("Distribusi ke Logstash untuk: {}".format(downloaded_files.get('path', 'N/A')))
    success = True
    if "conf70" in downloaded_files and LOGSTASH_PIPE_DIR: success &= safe_copy(downloaded_files["conf70"], LOGSTASH_PIPE_DIR)
    else: print("[WARN] File conf70 atau LOGSTASH_PIPE_DIR hilang."); success = False
    
    if "json_dict" in downloaded_files and LOGSTASH_JSON_DICT_DIR:
        safe_makedirs(LOGSTASH_JSON_DICT_DIR)
        success &= safe_copy(downloaded_files["json_dict"], LOGSTASH_JSON_DICT_DIR)
    else: print("[WARN] File json_dict atau LOGSTASH_JSON_DICT_DIR hilang."); success = False

    if "directive" in downloaded_files: success &= safe_run_cmd(["kubectl", "cp", downloaded_files["directive"], "{}:/dsiem/configs/".format(FRONTEND_POD)])
    else: print("[WARN] File directive hilang."); success = False
    return success

def distribute_vector(downloaded_files, parent):
    print_header("Distribusi ke Vector untuk: {}".format(downloaded_files.get('path', 'N/A')))
    success = True
    if "vector_conf" in downloaded_files and VECTOR_CONFIG_BASE_DIR:
        vector_target_dir = os.path.join(VECTOR_CONFIG_BASE_DIR, parent); safe_makedirs(vector_target_dir)
        success &= safe_copy(downloaded_files["vector_conf"], vector_target_dir)
    else: print("[WARN] File vector_conf atau VECTOR_CONFIG_BASE_DIR hilang."); success = False

    if "tsv" in downloaded_files and NFS_BASE_DIR:
        print("[DIST] Mencari direktori NFS 'dsiem-plugin-tsv' di {}...".format(NFS_BASE_DIR))
        nfs_target_dir = None
        
        # [FIX] LOGIC PINTAR NFS
        if not DRY_RUN:
            # 1. Cek langsung: apakah NFS_BASE_DIR itu sendiri adalah targetnya? (e.g. /mnt/NAS/dsiem-plugin-tsv)
            if os.path.basename(NFS_BASE_DIR.rstrip('/')) == 'dsiem-plugin-tsv':
                 if os.path.isdir(NFS_BASE_DIR):
                     nfs_target_dir = NFS_BASE_DIR
            
            # 2. Cek subfolder: apakah target ada DI DALAM path yg diinput? (e.g. input /mnt/NAS, target /mnt/NAS/dsiem-plugin-tsv)
            if not nfs_target_dir:
                 candidate = os.path.join(NFS_BASE_DIR, 'dsiem-plugin-tsv')
                 if os.path.isdir(candidate):
                     nfs_target_dir = candidate

            # 3. Fallback: Logika lama (Cari di dalam folder PVC)
            if not nfs_target_dir:
                try:
                    if os.path.isdir(NFS_BASE_DIR):
                        for item in os.listdir(NFS_BASE_DIR):
                            item_path = os.path.join(NFS_BASE_DIR, item)
                            if os.path.isdir(item_path) and item.startswith("pvc-"):
                                potential_target = os.path.join(item_path, "dsiem-plugin-tsv")
                                if os.path.isdir(potential_target): nfs_target_dir = potential_target; break
                except Exception as e: print("    -> [ERROR] Gagal listing direktori NFS: {}".format(e))
            
            if nfs_target_dir: 
                print("    -> Ditemukan: {}".format(nfs_target_dir))
                success &= safe_copy(downloaded_files["tsv"], nfs_target_dir)
            else: 
                print("    -> [ERROR] Direktori 'dsiem-plugin-tsv' tidak ditemukan di NFS path '{}'.".format(NFS_BASE_DIR)); 
                success = False
        else: print("    -> [DRY RUN] Pencarian dan penyalinan NFS dilewati."); success = True
    else: print("[WARN] File tsv atau NFS_BASE_DIR hilang."); success = False
    return success

def register_job(updater_path, is_focal_plugin, selected_action, distributed_physically):
    """Mendaftarkan pekerjaan dan menetapkan status Target/Distribution."""
    if not updater_path or not os.path.exists(updater_path):
        print("[REG] File updater '{}' tidak valid. Pendaftaran dilewati.".format(updater_path)); return
    
    print("[REG] Mendaftarkan pekerjaan: {}".format(os.path.basename(updater_path)))
    updaters_dir = "updaters"; safe_makedirs(updaters_dir)
    final_config_path = os.path.join(updaters_dir, os.path.basename(updater_path))

    try:
        with io.open(updater_path, 'r', encoding='utf-8') as f: updater_data = json.load(f, object_pairs_hook=OrderedDict)
        
        distribution_flag = selected_action.startswith("Distribusi")
        
        # LOGIKA PENTING: needs_distribution
        # REVISI: Flag ini adalah KONFIGURASI ("Apakah plugin ini harus didistribusikan otomatis?"),
        # bukan STATE ("Apakah plugin ini butuh distribusi sekarang?").
        needs_dist_flag = distribution_flag

        distribution_target = "None"
        if distribution_flag:
            if "Logstash" in selected_action: distribution_target = "Logstash"
            elif "Vector" in selected_action: distribution_target = "Vector"
        
        if 'layout' not in updater_data: updater_data['layout'] = OrderedDict()
        updater_data['layout']['needs_distribution'] = needs_dist_flag
        updater_data['layout']['distribution_target'] = distribution_target
        
        print("  [FLAG] Menetapkan needs_distribution = {}".format(needs_dist_flag))
        print("  [FLAG] Menetapkan distribution_target = {}".format(distribution_target))

        json_string_mod = json.dumps(updater_data, indent=2, ensure_ascii=False)
        try: 
            if isinstance(json_string_mod, str): json_string_mod = json_string_mod.decode('utf-8')
        except NameError: pass
        with io.open(updater_path, 'w', encoding='utf-8') as f_mod: f_mod.write(json_string_mod); f_mod.write(u'\n')

    except Exception as e:
        print("[ERROR] Gagal membaca/memodifikasi {}: {}. Flag mungkin tidak benar.".format(updater_path, e))

    safe_copy(updater_path, final_config_path)

    jobs_file = 'master_jobs.json'; jobs = []
    if os.path.exists(jobs_file):
        try:
            with io.open(jobs_file, 'r', encoding='utf-8') as f: jobs = json.load(f)
            if not isinstance(jobs, list): jobs = []
        except: jobs = []
    if final_config_path not in jobs: jobs.append(final_config_path); safe_save_json(jobs_file, sorted(jobs))

def activate_plugin_notification(slug_to_activate, all_active_slugs_set):
    if slug_to_activate not in all_active_slugs_set:
        print("[ACTIVATE] Menandai '{}' untuk notifikasi email.".format(slug_to_activate))
        all_active_slugs_set.add(slug_to_activate); return True
    return False

def restart_stack(action):
    if ask_yes_no("\nKonfigurasi didistribusikan. Restart stack sekarang?") != 'y':
        print("[INFO] Restart dibatalkan."); return
    print_header("Memulai Proses Restart")
    logstash_restarted = False
    if "Logstash" in action:
        if LOGSTASH_HOME and os.path.isdir(LOGSTASH_HOME):
             safe_run_cmd(["./update-config-map.sh"], cwd=LOGSTASH_HOME, shell=True)
             safe_run_cmd(["./restart-logstash.sh"], cwd=LOGSTASH_HOME, shell=True)
             logstash_restarted = True
        else: print("[WARN] Direktori LOGSTASH_HOME '{}' tidak ada atau tidak diset.".format(LOGSTASH_HOME))
    elif "Vector" in action:
        safe_run_cmd(["kubectl", "delete", "pod", "-l", VECTOR_POD_LABEL])
    
    if logstash_restarted or "Vector" in action:
         safe_run_cmd(["kubectl", "delete", "pod", BACKEND_POD, FRONTEND_POD])
    else:
         print("[INFO] Tidak ada pipeline (Logstash/Vector) yang di-restart, backend/frontend juga tidak.")

# ====== FUNGSI MAIN (STATE MACHINE) ======
def main():
    global DRY_RUN
    parser = argparse.ArgumentParser(description="Pull & Distribute SIEM plugin configs.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without making changes.")
    args = parser.parse_args(); DRY_RUN = args.dry_run
    if DRY_RUN: print("\n" + "#"*60 + "\n### DRY RUN MODE. NO CHANGES WILL BE MADE. ###\n" + "#"*60)

    reload_global_paths()
    
    print_header("Skrip Pull & Distribusi Konfigurasi"); require_github()
    customer_name = setup_customer_info()

    selection = {}
    if os.path.exists(LAST_SELECTION_FILE):
        if ask_yes_no("Ditemukan sesi terakhir. Lanjutkan?") == 'y':
            try:
                with io.open(LAST_SELECTION_FILE, 'r', encoding='utf-8') as f: selection = json.load(f)
            except Exception as e: print("[WARN] Gagal load sesi: {}. Mulai baru.".format(e)); selection = {}

    current_step = selection.get('current_step', 'parent')

    while current_step != 'done':
        selection['current_step'] = current_step

        if current_step == 'parent':
            parent_devices = find_parent_devices()
            if not parent_devices: die("Tidak ada perangkat induk ditemukan di repo."); return
            result = select_from_list(parent_devices, "Pilih Perangkat (Parent)")
            if result is None: continue
            selection['parent'] = result; current_step = 'scope_choice'

        elif current_step == 'scope_choice':
            options = ["Proses SEMUA plugin di bawah '{}'".format(selection['parent']), "Pilih plugin spesifik"]
            result = select_from_list(options, "Tentukan Cakupan Awal", can_go_back=True)
            if result == BACK_COMMAND: selection.pop('parent', None); current_step = 'parent'; continue
            selection['scope_choice'] = result; current_step = 'focal_plugins'

        elif current_step == 'focal_plugins':
            print("[INFO] Mencari semua plugin di bawah '{}'...".format(selection['parent']))
            all_plugins_in_parent = find_plugins_in_parent(selection['parent'])
            if not all_plugins_in_parent:
                print("[ERROR] Tidak ada plugin config ditemukan di '{}'.".format(selection['parent']))
                selection.pop('scope_choice', None); current_step = 'scope_choice'; continue
            if "spesifik" in selection['scope_choice']:
                result = select_plugins_from_list(all_plugins_in_parent, "Pilih Plugin Fokal")
                if result == BACK_COMMAND: selection.pop('scope_choice', None); current_step = 'scope_choice'; continue
                if not result: print("[INFO] Tidak ada plugin dipilih."); continue
                selection['focal_plugins'] = result; current_step = 'passive_scope'
            else:
                selection['focal_plugins'] = all_plugins_in_parent
                selection['passive_scope'] = 'none'; selection['passive_scope_desc'] = 'N/A (semua fokal)'; selection['passive_plugins'] = []
                selection['plugins_to_process'] = all_plugins_in_parent; current_step = 'action'; continue

        elif current_step == 'passive_scope':
            scope_key, scope_path = select_passive_scope(selection['focal_plugins'][0])
            selection['passive_scope'] = scope_key
            if scope_key != 'none':
                print("[INFO] Mencari plugin pasif di scope '{}'...".format(scope_path))
                all_plugins_in_scope = find_plugins_in_parent(scope_path)
                selection['passive_plugins'] = sorted([p for p in all_plugins_in_scope if p not in selection['focal_plugins']])
                selection['passive_scope_desc'] = "Semua di '{}' (kecuali fokal)"
            else:
                selection['passive_plugins'] = []; selection['passive_scope_desc'] = "Tidak ada"
            selection['plugins_to_process'] = sorted(list(set(selection['focal_plugins'] + selection['passive_plugins']))); current_step = 'action'

        elif current_step == 'action':
            actions = ["Distribusi & Konfigurasi Auto-Update ke Logstash", "Distribusi & Konfigurasi Auto-Update ke Vector", "HANYA Konfigurasi Auto-Update (Tanpa Distribusi)"]
            result = select_from_list(actions, "Pilih Aksi Utama", can_go_back=True)
            if result == BACK_COMMAND:
                prev_step = 'passive_scope' if 'passive_scope' in selection else ('focal_plugins' if 'spesifik' in selection.get('scope_choice','') else 'scope_choice')
                selection.pop('passive_scope', None); selection.pop('plugins_to_process', None); selection.pop('passive_plugins', None); selection.pop('passive_scope_desc', None)
                if prev_step == 'focal_plugins': selection.pop('focal_plugins', None)
                current_step = prev_step; continue
            
            selection['action'] = result
            if result == actions[0] or result == actions[1]:
                current_step = 'distribution_paths'
            else:
                current_step = 'active_plugins'

        elif current_step == 'distribution_paths':
            print_header("Konfigurasi Path Distribusi")
            
            action_type = "Logstash" if "Logstash" in selection['action'] else ("Vector" if "Vector" in selection['action'] else "None")

            if action_type == "None":
                print("[WARN] Kesalahan alur: Masuk ke distribution_paths tanpa aksi distribusi."); current_step = 'action'; continue 

            print("Aksi dipilih: Distribusi ke {}".format(action_type))
            
            print("\nPath default saat ini (dari environment):")
            if action_type == "Logstash":
                print("  - Config .conf : {}".format(LOGSTASH_PIPE_DIR or "BELUM DISET"))
                print("  - Dict .json   : {}".format(LOGSTASH_JSON_DICT_DIR or "BELUM DISET"))
                print("  - Logstash Home: {}".format(LOGSTASH_HOME or "BELUM DISET"))
            elif action_type == "Vector": 
                print("  - Config .yaml : {}".format(VECTOR_CONFIG_BASE_DIR or "BELUM DISET"))
                print("  - NFS TSV      : {}".format(NFS_BASE_DIR or "BELUM DISET"))
            
            options = ["Gunakan path default", "Masukkan path custom (akan disimpan ke config.sh)"]
            result = select_from_list(options, "Pilih Path", can_go_back=True)

            if result == BACK_COMMAND: selection.pop('action', None); current_step = 'action'; continue
            
            if "custom" in result:
                print_header("Masukkan Path Custom")
                new_paths = {}
                
                if action_type == "Logstash":
                    new_paths["LOGSTASH_PIPE_DIR"] = ask_for_path("Path Pipa .conf", os.getenv("LOGSTASH_PIPE_DIR", ""))
                    new_paths["LOGSTASH_JSON_DICT_DIR"] = ask_for_path("Path Diksi .json", os.getenv("LOGSTASH_JSON_DICT_DIR", ""))
                    new_paths["LOGSTASH_HOME"] = ask_for_path("Path Logstash Home (untuk restart)", os.getenv("LOGSTASH_HOME", ""))
                else:
                    new_paths["VECTOR_CONFIG_BASE_DIR"] = ask_for_path("Path Base Config Vector", os.getenv("VECTOR_CONFIG_BASE_DIR", ""))
                    new_paths["NFS_BASE_DIR"] = ask_for_path("Path Base NFS (untuk TSV)", os.getenv("NFS_BASE_DIR", ""))
                
                if update_config_sh(new_paths):
                    for key, value in new_paths.items(): os.environ[key] = value
                    reload_global_paths()
                    print("[INFO] Variabel path telah diperbarui untuk sesi ini.")
                else: continue
            
            current_step = 'active_plugins'
        
        elif current_step == 'active_plugins':
            result = select_active_notifications(selection.get('focal_plugins', []))
            if result == BACK_COMMAND:
                prev_step = 'distribution_paths' if 'Distribusi' in selection.get('action', '') else 'action'
                selection.pop('active_plugins', None)
                current_step = prev_step
                continue
            selection['active_plugins'] = result; current_step = 'summary'

        elif current_step == 'summary':
            display_summary(selection)
            confirm = ask_yes_no("Lanjutkan dengan pekerjaan ini?", allow_back=True)
            if confirm == 'n': print("[INFO] Proses dibatalkan."); current_step = 'done'; continue
            if confirm == BACK_COMMAND: selection.pop('active_plugins', None); current_step = 'active_plugins'; continue
            current_step = 'execute'

        elif current_step == 'execute':
            print_header("Memulai Eksekusi")
            selection['current_step'] = 'execute'
            safe_save_json(LAST_SELECTION_FILE, selection)

            all_downloaded_files = []
            print("[INFO] Mengunduh {} plugin...".format(len(selection['plugins_to_process'])))
            for path in selection['plugins_to_process']:
                processed_data = process_plugin(path)
                if processed_data: all_downloaded_files.append(processed_data)
            if not all_downloaded_files: die("Tidak ada file plugin yang berhasil diunduh. Proses dihentikan.")

            print_header("Registrasi Pekerjaan Auto-Update & Set Flag Distribusi")
            focal_paths_set = set(selection['focal_plugins'])
            selected_action = selection['action']
            distributed_physically = False
            
            # Distribusi HANYA JIKA dipilih & HANYA untuk FOKAL
            if "Distribusi" in selected_action:
                print_header("Distribusi Plugin Fokal")
                distributed_count = 0
                for downloaded in all_downloaded_files:
                    if downloaded['path'] in focal_paths_set:
                        
                        dist_success = False
                        if "Logstash" in selected_action: 
                            dist_success = distribute_logstash(downloaded)
                        elif "Vector" in selected_action: 
                            dist_success = distribute_vector(downloaded, selection['parent'])
                            
                        if dist_success:
                             distributed_count += 1
                
                if distributed_count > 0: 
                    distributed_physically = True
                    print("[INFO] {} plugin fokal didistribusikan.".format(distributed_count))
                else: 
                    print("[INFO] Tidak ada plugin fokal untuk didistribusikan atau distribusi gagal.")

            else: print("[INFO] Aksi 'HANYA Konfigurasi Auto-Update', distribusi dilewati.")

            # Registrasi SEMUA job + set flag distribusi
            for downloaded in all_downloaded_files:
                if "updater_cfg" in downloaded:
                    is_focal = downloaded['path'] in focal_paths_set
                    # Logika needs_distribution di register_job() menggunakan distributed_physically
                    register_job(downloaded["updater_cfg"], is_focal, selected_action, distributed_physically)
                else: print("[WARN] File updater_cfg tidak ada untuk {}, registrasi dilewati.".format(downloaded.get('path', 'N/A')))

            print_header("Aktivasi Notifikasi Email")
            active_slugs_set = set()
            active_file_path = ACTIVE_PLUGINS_FILE
            if os.path.exists(active_file_path):
                try:
                    with io.open(active_file_path, 'r', encoding='utf-8') as f: loaded_list = json.load(f)
                    if isinstance(loaded_list, list): active_slugs_set = set(loaded_list)
                except (JSONDecodeError, IOError): print("[WARN] Gagal baca {}.".format(active_file_path))
            
            slugs_to_activate = set()
            for active_path in selection['active_plugins']:
                for d in all_downloaded_files:
                     if d.get('path') == active_path and d.get('full_slug'):
                          slugs_to_activate.add(d['full_slug']); break
            
            made_changes_to_active = False
            for slug_to_add in slugs_to_activate:
                 if activate_plugin_notification(slug_to_add, active_slugs_set): made_changes_to_active = True
            
            if made_changes_to_active:
                 print("[INFO] Menyimpan perubahan ke {}...".format(active_file_path))
                 safe_save_json(active_file_path, sorted(list(active_slugs_set)))
            else: print("[INFO] Tidak ada perubahan status notifikasi.")

            if distributed_physically: restart_stack(selected_action)
            elif "Distribusi" in selected_action: print("[INFO] Distribusi fisik dilewati, restart dilewati.")

            print_header("Otomatisasi Laporan Status")
            current_integrations = scan_integrations_for_current_customer()
            customer_name_current = get_customer_name()
            local_report_path = update_integration_report(customer_name_current, current_integrations)

            if local_report_path: gh_api_put_file(local_report_path)

            if os.path.exists(LAST_SELECTION_FILE) and not DRY_RUN:
                try: print("[INFO] Menghapus file state sesi..."); os.remove(LAST_SELECTION_FILE)
                except OSError as e: print("[WARN] Gagal hapus {}: {}".format(LAST_SELECTION_FILE, e))
            print_header("Proses Selesai")
            if DRY_RUN: print("Mode Dry Run selesai.")
            current_step = 'done'

        else:
             print("[WARN] Step tidak dikenali: {}. Kembali ke awal.".format(current_step))
             selection = {}; current_step = 'parent'

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nProses dibatalkan oleh pengguna.")
        sys.exit(1)
    except Exception as e:
         print("\n\n[FATAL ERROR] Terjadi kesalahan:")
         import traceback
         traceback.print_exc()
         sys.exit(1)