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

# ====== KONFIGURASI ENV ======
GITHUB_REPO   = os.getenv("GITHUB_REPO")
GITHUB_TOKEN  = os.getenv("GITHUB_TOKEN")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH")
OUT_DIR = os.getenv("OUT_DIR", "./pulled_configs")

# ====== KONFIGURASI DISTRIBUSI & RESTART ======
LOGSTASH_PIPE_DIR = "/root/kubeappl/logstash/configs/pipelines/dsiem-events/"
LOGSTASH_JSON_DICT_DIR = "/root/kubeappl/logstash/configs/pipelines/dsiem-events/dsiem-plugin-json/"
LOGSTASH_HOME     = "/root/kubeappl/logstash/"
VECTOR_CONFIG_BASE_DIR = "/root/data/mgmt/kubeappl/vector-parser/configs/"
NFS_BASE_DIR           = "/root/data/nfs/"
FRONTEND_POD      = "dsiem-frontend-0"
BACKEND_POD       = "dsiem-backend-0"
VECTOR_POD_LABEL  = "app=vector-parser"

# Konstanta untuk navigasi
LAST_SELECTION_FILE = "last_selection.json"
CUSTOMER_FILE = "customer.json"
BACK_COMMAND = "__BACK__"
DRY_RUN = False

# ====== I/O & HELPERS ======
def print_header(title):
    print("\n" + "="*60)
    print("=== {}".format(title.upper()))
    print("="*60)

def die(msg, code=1):
    print("\n[ERROR] {}".format(msg))
    sys.exit(code)

def safe_save_json(path, obj):
    print("[FILE] Menyiapkan untuk menyimpan: {}".format(path))
    if DRY_RUN:
        print("    -> [DRY RUN] Penulisan file dilewati.")
        return
    try:
        with open(path, 'w') as f:
            json.dump(obj, f, indent=2, sort_keys=True)
            f.write("\n")
        print("    -> [OK] Berhasil disimpan.")
    except Exception as e:
        print("    -> [ERROR] Gagal menyimpan file: {}".format(e))

def safe_copy(src, dst_dir_or_file):
    print("[FILE] Menyiapkan untuk menyalin '{}' ke '{}'".format(src, dst_dir_or_file))
    if DRY_RUN:
        print("    -> [DRY RUN] Penyalinan dilewati.")
        return
    try:
        shutil.copy(src, dst_dir_or_file)
        print("    -> [OK] Berhasil disalin.")
    except Exception as e:
        print("    -> [ERROR] Gagal menyalin: {}".format(e))

def safe_makedirs(path):
    if os.path.exists(path): return
    print("[FILE] Menyiapkan untuk membuat direktori: {}".format(path))
    if DRY_RUN:
        print("    -> [DRY RUN] Pembuatan direktori dilewati.")
        return
    try:
        os.makedirs(path)
        print("    -> [OK] Direktori berhasil dibuat.")
    except Exception as e:
        print("    -> [ERROR] Gagal membuat direktori: {}".format(e))

def safe_run_cmd(cmd, cwd=None, shell=False):
    cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
    print("\n[CMD] Menyiapkan untuk menjalankan: {}".format(cmd_str))
    if DRY_RUN:
        print("    -> [DRY RUN] Eksekusi perintah dilewati.")
        return True
    try:
        p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
        out, err = p.communicate()
        if out: print(out.decode('utf-8', 'replace'))
        if err: print("--- stderr ---\n{}".format(err.decode('utf-8', 'replace')))
        return p.returncode == 0
    except OSError as e:
        print("[ERROR] Gagal menjalankan perintah: {}".format(e))
        return False

def py_input(p):
    try: return raw_input(p)
    except NameError: return input(p)

def ask_yes_no(p, allow_back=False):
    prompt = p
    if allow_back: prompt += " (y/n/b untuk kembali): "
    else: prompt += " (y/n): "
    
    while True:
        a = py_input(prompt).strip().lower()
        valid_options = ("y", "n")
        if allow_back: valid_options += ("b",)
        
        if a in valid_options:
            if a == 'b': return BACK_COMMAND
            return a
        print("Pilihan tidak valid.")

def setup_customer_info():
    print_header("Konfigurasi Customer")
    customer_name = ""
    
    if os.path.exists(CUSTOMER_FILE):
        try:
            with open(CUSTOMER_FILE, 'r') as f:
                data = json.load(f)
                customer_name = data.get("customer_info", {}).get("customer_name", "")
        except (IOError, json.JSONDecodeError):
            print("[WARN] File customer.json tidak bisa dibaca. Akan dibuat ulang.")
            customer_name = ""

    if not customer_name or customer_name == "Nama Customer Anda":
        if not customer_name:
             print("[INFO] File customer.json belum ada. Silakan konfigurasikan.")
        else:
             print("[INFO] Nama customer masih menggunakan placeholder. Silakan diganti.")
             
        while True:
            new_name = py_input("Masukkan Nama Customer baru (untuk notifikasi email): ").strip()
            if new_name and new_name != "Nama Customer Anda":
                customer_name = new_name
                break
            print("[ERROR] Nama customer tidak boleh kosong atau sama dengan placeholder.")
        
        data_to_save = {"customer_info": {"customer_name": customer_name}}
        safe_save_json(CUSTOMER_FILE, data_to_save)
    else:
        print("[OK] Nama customer sudah dikonfigurasi: '{}'".format(customer_name))

# ====== FUNGSI GITHUB ======
def require_github():
    if not GITHUB_REPO or not GITHUB_TOKEN:
        raise SystemExit("[GITHUB] Set GITHUB_REPO='owner/repo' dan GITHUB_TOKEN='ghp_xxx' dulu ya.")

def gh_headers():
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": "Bearer {}".format(GITHUB_TOKEN),
        "X-GitHub-Api-Version": "2022-11-28"
    }

def gh_api_get(path):
    url = "https://api.github.com/repos/{}/contents/{}".format(GITHUB_REPO, path)
    try:
        r = requests.get(url, headers=gh_headers(), params={"ref": GITHUB_BRANCH}, timeout=60)
        if r.status_code == 404: return None
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print("\n[ERROR] Gagal menghubungi GitHub API: {}".format(e))
        return None

def find_parent_devices():
    print("[INFO] Mencari perangkat induk di repositori...")
    items = gh_api_get("")
    if not isinstance(items, list):
        print("[ERROR] Gagal mendapatkan daftar dari root repositori.")
        return []
    devices = sorted([item['name'] for item in items if item['type'] == 'dir' and not item['name'].startswith('.')])
    print("[INFO] Ditemukan {} perangkat induk.".format(len(devices)))
    return devices

def find_plugins_in_parent(parent_path, current_path=""):
    full_path = os.path.join(parent_path, current_path).replace("\\", "/")
    items = gh_api_get(full_path)
    if not isinstance(items, list): return []
    
    found_plugins = []
    has_config = any(item.get('name') == 'config.json' for item in items)
    has_subdirs = any(item.get('type') == 'dir' for item in items)

    if has_config and not has_subdirs:
        found_plugins.append(full_path)

    for item in items:
        if item.get('type') == 'dir':
            new_path = os.path.join(current_path, item['name']).replace("\\", "/")
            found_plugins.extend(find_plugins_in_parent(parent_path, new_path))
            
    return found_plugins

def download_and_save(remote_path, local_path):
    print("\n[*] Mencoba mengunduh: {}".format(remote_path))
    file_meta = gh_api_get(remote_path)
    if file_meta is None:
        print("    -> [INFO] File tidak ditemukan, dilewati.")
        return None, None
    try:
        content_b64 = file_meta.get("content", "")
        content_bytes = base64.b64decode(content_b64)
        local_dir = os.path.dirname(local_path)
        safe_makedirs(local_dir)
        
        print("    -> [FILE] Menyiapkan untuk menyimpan: {}".format(local_path))
        if not DRY_RUN:
            with open(local_path, "wb") as f:
                f.write(content_bytes)
        else:
            print("    -> [DRY RUN] Penulisan file dilewati.")
        
        full_slug = None
        if remote_path.endswith('_plugin-sids.tsv'):
            full_slug = os.path.basename(remote_path).replace('_plugin-sids.tsv', '')
        return local_path, full_slug
    except Exception as e:
        print("    -> [ERROR] Gagal memproses file: {}".format(e))
        return None, None

# ====== FUNGSI ALUR BARU (NEW FLOW) ======
def select_from_list(options, title, can_go_back=False):
    print_header(title)
    if can_go_back:
        print("0. Kembali ke langkah sebelumnya")
    
    for i, option in enumerate(options, 1):
        print("{}. {}".format(i, option))
    
    while True:
        prompt = "Pilihan Anda: "
        choice = py_input(prompt).strip()
        if can_go_back and choice == '0': return BACK_COMMAND
        if choice.isdigit() and 1 <= int(choice) <= len(options):
            return options[int(choice) - 1]
        print("[ERROR] Pilihan tidak valid.")

def select_plugins_from_list(available_plugins, title):
    print_header(title)
    for i, plugin in enumerate(available_plugins, 1):
        print("{}. {}".format(i, plugin))

    while True:
        choice_str = py_input("\nMasukkan nomor pilihan (cth: 1, 3, 5-7) atau ketik 'b' untuk kembali: ").strip().lower()
        if choice_str == 'b': return BACK_COMMAND
        if not choice_str: continue
        
        selected_indices = set()
        valid = True
        for part in choice_str.split(','):
            part = part.strip()
            if not part: continue
            if '-' in part:
                try:
                    start, end = map(int, part.split('-'))
                    if start > end or not (1 <= start <= len(available_plugins) and 1 <= end <= len(available_plugins)):
                        raise ValueError
                    selected_indices.update(range(start - 1, end))
                except ValueError:
                    print("[ERROR] Rentang '{}' tidak valid.".format(part)); valid = False; break
            else:
                try:
                    idx = int(part) - 1
                    if not (0 <= idx < len(available_plugins)): raise ValueError
                    selected_indices.add(idx)
                except ValueError:
                    print("[ERROR] Pilihan '{}' tidak valid.".format(part)); valid = False; break
        
        if valid:
            return sorted([available_plugins[i] for i in selected_indices])

def select_passive_scope(focal_plugin_path):
    print_header("Cakupan Sinkronisasi Pasif")
    print("Anda memilih '{}' sebagai fokus utama.".format(focal_plugin_path))
    print("Bagaimana Anda ingin menangani plugin lain yang terkait?")
    print("Plugin lain akan diunduh & didaftarkan untuk auto-update, tapi tidak didistribusikan.")

    parts = focal_plugin_path.split('/')
    options = [("none", "Jangan sertakan plugin lain (proses HANYA yang dipilih).")]
    
    if len(parts) >= 4:
        submodule_path = "/".join(parts[:3])
        options.append(("submodule", "Sertakan semua plugin lain di submodule '{}'.".format(submodule_path)))
    if len(parts) >= 3:
        module_path = "/".join(parts[:2])
        options.append(("module", "Sertakan semua plugin lain di modul '{}'.".format(module_path)))
    if len(parts) >= 2:
        parent_path = parts[0]
        options.append(("parent", "Sertakan semua plugin lain di parent folder '{}'.".format(parent_path)))
    
    for i, (_, desc) in enumerate(options, 1):
        print("{}. {}".format(i, desc))

    while True:
        choice = py_input("Pilihan Anda [1-{}]: ".format(len(options))).strip()
        if choice.isdigit() and 1 <= int(choice) <= len(options):
            scope_key, _ = options[int(choice) - 1]
            if scope_key == "parent": return "parent", parts[0]
            if scope_key == "module": return "module", "/".join(parts[:2])
            if scope_key == "submodule": return "submodule", "/".join(parts[:3])
            return "none", None
        print("[ERROR] Pilihan tidak valid.")

def select_active_notifications(focal_plugins):
    print_header("Aktivasi Notifikasi (Hanya untuk Plugin Fokal)")
    print("Pilih plugin FOKAL mana yang ingin diaktifkan notifikasi email-nya.")
    print("Plugin pasif akan selalu non-aktif.\n")
    for i, plugin in enumerate(focal_plugins, 1):
        print("{}. {}".format(i, plugin))
    
    print("\nPilihan:")
    print("- Masukkan nomor untuk mengaktifkan (cth: 1, 3)")
    print("- Ketik 'A' untuk mengaktifkan SEMUA")
    print("- Ketik 'b' untuk kembali ke langkah sebelumnya")
    print("- Tekan Enter untuk tidak mengaktifkan satupun")

    while True:
        choice_str = py_input("\nPlugin yang akan diaktifkan notifikasinya: ").strip().lower()
        if not choice_str: return []
        if choice_str == 'b': return BACK_COMMAND
        if choice_str == 'a': return focal_plugins
        
        selected_indices = set()
        valid = True
        for part in choice_str.split(','):
            part = part.strip()
            if not part.isdigit(): valid = False; break
            idx = int(part) - 1
            if not (0 <= idx < len(focal_plugins)): valid = False; break
            selected_indices.add(idx)

        if valid:
            return sorted([focal_plugins[i] for i in selected_indices])
        else:
            print("[ERROR] Pilihan '{}' tidak valid.".format(choice_str))

def display_summary(selection):
    print_header("Ringkasan Pekerjaan")
    if 'focal_plugins' in selection:
        print("Plugin Fokal         : {} plugin".format(len(selection['focal_plugins'])))
        for p in selection['focal_plugins']: print("    - {}".format(p))
    
    if 'passive_scope_desc' in selection:
        print("Cakupan Pasif        : {}".format(selection['passive_scope_desc']))
        print("Total Plugin Proses  : {} ({} fokal + {} pasif)".format(
            len(selection['plugins_to_process']), len(selection['focal_plugins']), len(selection['passive_plugins'])
        ))

    if 'action' in selection: print("Aksi Utama           : {}".format(selection['action']))
    if 'active_plugins' in selection:
        active_count = len(selection['active_plugins'])
        print("Plugin Aktif (Notif) : {} dari {} plugin fokal".format(active_count, len(selection['focal_plugins'])))
        if active_count > 0:
            for plugin in selection['active_plugins']:
                print("    - {}".format(plugin))
    print("="*60)

# ====== FUNGSI PROSES & DISTRIBUSI ======
def process_plugin(plugin_path):
    print_header("Mengunduh Plugin: {}".format(plugin_path))
    dir_contents = gh_api_get(plugin_path)
    full_slug = None
    if dir_contents:
        for item in dir_contents:
            if item['type'] == 'file' and item['name'].endswith('_plugin-sids.tsv'):
                full_slug = item['name'].replace('_plugin-sids.tsv', '')
                break
    if not full_slug:
        print("[ERROR] Gagal menentukan 'full_slug' untuk {}. Dilewati.".format(plugin_path))
        return None
    print("[INFO] Ditemukan 'full_slug': {}".format(full_slug))
    local_plugin_dir = os.path.join(OUT_DIR, plugin_path)
    paths_to_download = {
        "conf70":      "70_dsiem-plugin_{}.conf".format(full_slug), "vector_conf": "70_transform_dsiem-plugin-{}.yaml".format(full_slug),
        "tsv":         "{}_plugin-sids.tsv".format(full_slug), "directive":   "directives_{}_{}.json".format(BACKEND_POD, full_slug),
        "json_dict":   "{}_plugin-sids.json".format(full_slug), "updater_cfg": "{}_updater.json".format(full_slug),
    }
    downloaded_files = {"full_slug": full_slug, "path": plugin_path}
    for key, filename in paths_to_download.items():
        remote_path = os.path.join(plugin_path, filename).replace("\\", "/")
        local_path = os.path.join(local_plugin_dir, filename)
        saved_path, _ = download_and_save(remote_path, local_path)
        if saved_path: downloaded_files[key] = saved_path
    return downloaded_files

def distribute_logstash(downloaded_files):
    print_header("Distribusi ke Logstash untuk: {}".format(downloaded_files['path']))
    if "conf70" in downloaded_files: safe_copy(downloaded_files["conf70"], LOGSTASH_PIPE_DIR)
    if "json_dict" in downloaded_files:
        safe_makedirs(LOGSTASH_JSON_DICT_DIR)
        safe_copy(downloaded_files["json_dict"], LOGSTASH_JSON_DICT_DIR)
    if "directive" in downloaded_files:
        target_path = "{}:/dsiem/configs/".format(FRONTEND_POD)
        safe_run_cmd(["kubectl", "cp", downloaded_files["directive"], target_path])

def distribute_vector(downloaded_files, parent):
    print_header("Distribusi ke Vector untuk: {}".format(downloaded_files['path']))
    if "vector_conf" in downloaded_files:
        vector_target_dir = os.path.join(VECTOR_CONFIG_BASE_DIR, parent)
        safe_makedirs(vector_target_dir)
        safe_copy(downloaded_files["vector_conf"], vector_target_dir)
    if "tsv" in downloaded_files:
        print("[DIST] Mencari direktori dsiem-plugin-tsv di {}...".format(NFS_BASE_DIR))
        if not DRY_RUN:
            nfs_target_dir = None
            try:
                for item in os.listdir(NFS_BASE_DIR):
                    item_path = os.path.join(NFS_BASE_DIR, item)
                    if os.path.isdir(item_path) and item.startswith("pvc-"):
                        potential_target = os.path.join(item_path, "dsiem-plugin-tsv")
                        if os.path.isdir(potential_target): nfs_target_dir = potential_target; break
            except Exception as e: print("    -> [ERROR] Gagal mencari direktori NFS: {}".format(e))
            if nfs_target_dir: safe_copy(downloaded_files["tsv"], nfs_target_dir)
            else: print("    -> [ERROR] Direktori 'dsiem-plugin-tsv' tidak ditemukan di NFS.")
        else: print("    -> [DRY RUN] Pencarian dan penyalinan NFS dilewati.")

def register_job(updater_path):
    if not updater_path or not os.path.exists(updater_path): return
    print("[REG] Mendaftarkan pekerjaan: {}".format(updater_path))
    updaters_dir = "updaters"; safe_makedirs(updaters_dir)
    final_config_path = os.path.join(updaters_dir, os.path.basename(updater_path))
    safe_copy(updater_path, final_config_path)
    jobs_file = 'master_jobs.json'; jobs = []
    if os.path.exists(jobs_file):
        try:
            with open(jobs_file, 'r') as f: jobs = json.load(f)
        except json.JSONDecodeError: pass
    if final_config_path not in jobs:
        jobs.append(final_config_path); safe_save_json(jobs_file, sorted(jobs))
    else: print("[REG] Pekerjaan sudah terdaftar.")

def activate_plugin_notification(slug_to_activate, all_active_slugs):
    if slug_to_activate not in all_active_slugs:
        all_active_slugs.append(slug_to_activate)

def restart_stack(action):
    if "Logstash" in action:
        if ask_yes_no("\nRestart Logstash & Dsiem pods sekarang?") == 'y':
            print_header("Memulai Proses Restart Logstash")
            safe_run_cmd(["./update-config-map.sh"], cwd=LOGSTASH_HOME, shell=True)
            safe_run_cmd(["./restart-logstash.sh"], cwd=LOGSTASH_HOME, shell=True)
            safe_run_cmd(["kubectl", "delete", "pod", BACKEND_POD, FRONTEND_POD])
    elif "Vector" in action:
        if ask_yes_no("\nRestart Vector & Dsiem pods sekarang?") == 'y':
            print_header("Memulai Proses Restart Vector")
            safe_run_cmd(["kubectl", "delete", "pod", "-l", VECTOR_POD_LABEL])
            safe_run_cmd(["kubectl", "delete", "pod", BACKEND_POD, FRONTEND_POD])

# ====== FUNGSI MAIN (STATE MACHINE) ======
def main():
    global DRY_RUN
    parser = argparse.ArgumentParser(description="Skrip untuk mengunduh dan mendistribusikan konfigurasi plugin.")
    parser.add_argument("--dry-run", action="store_true", help="Jalankan skrip dalam mode simulasi.")
    args = parser.parse_args(); DRY_RUN = args.dry_run
    if DRY_RUN:
        print("\n" + "#"*60 + "\n### MODE DRY RUN AKTIF. TIDAK ADA PERUBAHAN YANG AKAN DIBUAT. ###\n" + "#"*60)

    print_header("Skrip Pull & Distribusi Konfigurasi"); require_github()
    setup_customer_info()

    selection = {}
    if os.path.exists(LAST_SELECTION_FILE):
        if ask_yes_no("Ditemukan sesi terakhir. Lanjutkan?") == 'y':
            try:
                with open(LAST_SELECTION_FILE, 'r') as f: selection = json.load(f)
            except Exception as e:
                print("[WARN] Gagal memuat sesi: {}. Memulai sesi baru.".format(e)); selection = {}

    while True:
        if 'parent' not in selection:
            parent_devices = find_parent_devices()
            if not parent_devices: return
            result = select_from_list(parent_devices, "Pilih Perangkat (Parent)")
            selection['parent'] = result

        if 'scope_choice' not in selection:
            result = select_from_list(["Proses SEMUA plugin di bawah '{}'".format(selection['parent']), "Pilih plugin spesifik"], "Tentukan Cakupan Awal", can_go_back=True)
            if result == BACK_COMMAND: selection.pop('parent', None); continue
            selection['scope_choice'] = result

        if 'focal_plugins' not in selection:
            all_plugins_in_parent = find_plugins_in_parent(selection['parent'])
            if not all_plugins_in_parent: die("Tidak ada plugin di bawah '{}'.".format(selection['parent']));
            
            if "spesifik" in selection['scope_choice']:
                result = select_plugins_from_list(all_plugins_in_parent, "Pilih Plugin Fokal")
                if result == BACK_COMMAND: selection.pop('scope_choice', None); continue
                if not result: print("[INFO] Tidak ada plugin yang dipilih. Silakan pilih lagi."); continue
                selection['focal_plugins'] = result
            else:
                # [PERBAIKAN 1] Logika yang benar untuk alur "Proses SEMUA"
                selection['focal_plugins'] = all_plugins_in_parent
                selection['passive_scope'] = 'none'
                selection['passive_scope_desc'] = 'Tidak ada (semua plugin adalah fokal)'
                selection['passive_plugins'] = []
                selection['plugins_to_process'] = all_plugins_in_parent

        if 'passive_scope' not in selection:
            scope_key, scope_path = select_passive_scope(selection['focal_plugins'][0])
            selection['passive_scope'] = scope_key
            if scope_key != 'none':
                all_plugins_in_scope = find_plugins_in_parent(scope_path)
                selection['passive_plugins'] = [p for p in all_plugins_in_scope if p not in selection['focal_plugins']]
                selection['passive_scope_desc'] = "Semua plugin di '{}'".format(scope_path)
            else:
                selection['passive_plugins'] = []
                selection['passive_scope_desc'] = "Tidak ada (hanya proses plugin fokal)"
            
            selection['plugins_to_process'] = sorted(list(set(selection['focal_plugins'] + selection['passive_plugins'])))

        if 'action' not in selection:
            actions = ["Distribusi & Konfigurasi Auto-Update ke Logstash", "Distribusi & Konfigurasi Auto-Update ke Vector", "HANYA Konfigurasi Auto-Update (Tanpa Distribusi)"]
            result = select_from_list(actions, "Pilih Aksi Utama", can_go_back=True)
            if result == BACK_COMMAND: 
                selection.pop('focal_plugins', None); selection.pop('passive_scope', None); selection.pop('plugins_to_process', None); continue
            selection['action'] = result

        if 'active_plugins' not in selection:
            result = select_active_notifications(selection['focal_plugins'])
            if result == BACK_COMMAND: selection.pop('action', None); continue
            selection['active_plugins'] = result
        
        display_summary(selection)
        confirm = ask_yes_no("Lanjutkan dengan pekerjaan ini?", allow_back=True)
        if confirm == 'n': print("[INFO] Proses dibatalkan."); return
        if confirm == BACK_COMMAND: selection.pop('active_plugins', None); continue
        
        break

    # --- EKSEKUSI UTAMA ---
    print_header("Memulai Eksekusi")
    safe_save_json(LAST_SELECTION_FILE, selection)
    all_downloaded_files = [p for p in [process_plugin(path) for path in selection['plugins_to_process']] if p]

    if not all_downloaded_files: die("Tidak ada file yang berhasil diunduh. Proses dihentikan.")
    
    distributed_something = False
    if "Distribusi" in selection['action']:
        print_header("Distribusi Plugin Fokal")
        focal_paths = selection['focal_plugins']
        for downloaded in all_downloaded_files:
            if downloaded['path'] in focal_paths:
                if "Logstash" in selection['action']: distribute_logstash(downloaded)
                elif "Vector" in selection['action']: distribute_vector(downloaded, selection['parent'])
                distributed_something = True
        if not distributed_something:
            print("[INFO] Tidak ada plugin fokal yang dipilih untuk didistribusikan.")
    
    print_header("Registrasi Pekerjaan & Aktivasi Notifikasi")
    active_slugs_from_file = []
    if os.path.exists('active_plugins.json'):
        try:
            with open('active_plugins.json', 'r') as f: active_slugs_from_file = json.load(f)
        except: pass
        
    for downloaded in all_downloaded_files:
        register_job(downloaded.get("updater_cfg"))
        if downloaded.get('path') in selection['active_plugins']:
            activate_plugin_notification(downloaded['full_slug'], active_slugs_from_file)
            
    safe_save_json('active_plugins.json', sorted(list(set(active_slugs_from_file))))
    
    # [PERBAIKAN 2] Gunakan `selection['action']` saat memanggil restart_stack
    if "Distribusi" in selection['action'] and distributed_something:
        restart_stack(selection['action'])
    
    if os.path.exists(LAST_SELECTION_FILE) and not DRY_RUN: os.remove(LAST_SELECTION_FILE)

    print_header("Proses Selesai")
    if DRY_RUN: print("Mode Dry Run selesai. Tidak ada perubahan yang dibuat pada sistem.")

if __name__ == "__main__":
    main()