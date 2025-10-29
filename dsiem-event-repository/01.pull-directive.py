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
import io # <-- Pastikan io diimport

# --- Penyesuaian Kompatibilitas Py2/Py3 (PASTIKAN BLOK INI ADA DI ATAS) ---
try:
    JSONDecodeError = json.JSONDecodeError # Coba ambil nama Py3
except AttributeError:                     # Jika gagal (berarti Py2)
    JSONDecodeError = ValueError          # Gunakan nama Py2 (ValueError) sebagai gantinya
try:
    FileNotFoundError                     # Coba ambil nama Py3
except NameError:                         # Jika gagal (berarti Py2)
    FileNotFoundError = IOError           # Gunakan nama Py2 (IOError) sebagai gantinya
# --- AKHIR BLOK KOMPATIBILITAS ---

# ====== KONFIGURASI ENV ======
GITHUB_REPO   = os.getenv("GITHUB_REPO")
GITHUB_TOKEN  = os.getenv("GITHUB_TOKEN")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH")
OUT_DIR = os.getenv("OUT_DIR", "./pulled_configs")

# ====== KONFIGURASI DISTRIBUSI & RESTART ======
LOGSTASH_PIPE_DIR = os.getenv("LOGSTASH_PIPE_DIR")
LOGSTASH_JSON_DICT_DIR = os.getenv("LOGSTASH_JSON_DICT_DIR")
LOGSTASH_HOME     = os.getenv("LOGSTASH_HOME")
VECTOR_CONFIG_BASE_DIR = os.getenv("VECTOR_CONFIG_BASE_DIR")
NFS_BASE_DIR           = os.getenv("NFS_BASE_DIR")
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
        # Gunakan io.open untuk kompatibilitas Py2/3
        json_string = json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False)
        # Handle unicode di Py2
        try:
            unicode # Cek Py2
            if isinstance(json_string, str): # Jika bytes
                json_string = json_string.decode('utf-8')
        except NameError:
            pass # Py3 sudah unicode

        with io.open(path, 'w', encoding='utf-8') as f:
            f.write(json_string)
            f.write(u'\n') # Newline unicode
        print("    -> [OK] Berhasil disimpan.")
    except Exception as e:
        print("    -> [ERROR] Gagal menyimpan file {}: {}".format(path, e))

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
        return True # Anggap sukses di dry run
    try:
        p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
        out, err = p.communicate()
        # Selalu print output & error agar terlihat
        if out: print("--- stdout ---\n{}".format(out.decode('utf-8', 'replace')))
        if err: print("--- stderr ---\n{}".format(err.decode('utf-8', 'replace')))
        return p.returncode == 0
    except OSError as e:
        print("[ERROR] Gagal menjalankan perintah '{}': {}".format(cmd_str, e))
        return False

# Kompatibilitas input Py2/3
try: input = raw_input
except NameError: pass

def ask_yes_no(p, allow_back=False):
    prompt = p
    options = "(y/n)"
    valid_chars = set(['y', 'n'])
    if allow_back:
        options = "(y/n/b untuk kembali)"
        valid_chars.add('b')
    prompt += " {}: ".format(options)

    while True:
        a = input(prompt).strip().lower()
        if a in valid_chars:
            if a == 'b': return BACK_COMMAND
            return a
        print("Pilihan tidak valid. Harap masukkan {}".format('/'.join(sorted(list(valid_chars)))))

def setup_customer_info():
    print_header("Konfigurasi Customer")
    customer_name = ""

    if os.path.exists(CUSTOMER_FILE):
        try:
            # Gunakan io.open
            with io.open(CUSTOMER_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                customer_name = data.get("customer_info", {}).get("customer_name", "")
        # Gunakan variabel kompatibilitas
        except (IOError, JSONDecodeError):
            print("[WARN] File customer.json tidak bisa dibaca. Akan dibuat ulang.")
            customer_name = ""

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

# ====== FUNGSI GITHUB ======
def require_github():
    if not GITHUB_REPO or not GITHUB_TOKEN:
        die("Set GITHUB_REPO='owner/repo' dan GITHUB_TOKEN='ghp_xxx' dulu ya.")

def gh_headers():
    return { "Accept": "application/vnd.github+json", "Authorization": "Bearer {}".format(GITHUB_TOKEN), "X-GitHub-Api-Version": "2022-11-28" }

def gh_api_get(path):
    # Bersihkan path dan buat URL
    clean_path = path.replace("\\", "/").lstrip('/')
    url = "https://api.github.com/repos/{}/contents/{}".format(GITHUB_REPO, clean_path)
    try:
        r = requests.get(url, headers=gh_headers(), params={"ref": GITHUB_BRANCH}, timeout=60)
        if r.status_code == 404: return None # Tidak ditemukan itu normal
        r.raise_for_status() # Error untuk status lain
        return r.json() # Kembalikan JSON jika sukses
    except requests.exceptions.RequestException as e:
        print("\n[ERROR] Gagal menghubungi GitHub API ({}): {}".format(url, e))
        return None # Kembalikan None jika ada error koneksi/request

def find_parent_devices():
    print("[INFO] Mencari perangkat induk di repositori...")
    items = gh_api_get("") # Mulai dari root
    if not isinstance(items, list):
        print("[ERROR] Gagal mendapatkan daftar dari root repositori (respon bukan list).")
        return []
    # Filter hanya direktori yang tidak diawali '.'
    devices = sorted([item['name'] for item in items if item.get('type') == 'dir' and not item.get('name', '').startswith('.')])
    print("[INFO] Ditemukan {} perangkat induk.".format(len(devices)))
    return devices

def find_plugins_in_parent(parent_path, current_path=""):
    # Gabungkan path dan bersihkan
    full_path = os.path.join(parent_path, current_path).replace("\\", "/")
    items = gh_api_get(full_path)
    # Jika path tidak valid atau error API
    if not isinstance(items, list):
         # Jika path adalah file (bukan direktori), cek apakah itu plugin
         if isinstance(items, dict) and items.get('path') == full_path:
              # Ini kemungkinan hanya terjadi jika parent_path adalah file
              return [] # Anggap tidak ada plugin di dalam file
         else:
              print("[WARN] Gagal membaca isi direktori: {}".format(full_path))
              return [] # Kembalikan list kosong jika error

    found_plugins = []
    # Cek apakah direktori ini punya 'config.json'
    has_config = any(item.get('type') == 'file' and item.get('name') == 'config.json' for item in items)
    # Cek apakah ada subdirektori
    has_subdirs = any(item.get('type') == 'dir' for item in items)

    # Kondisi plugin: ada config.json DAN tidak ada subdirektori lagi
    # Juga pastikan path yang dicek bukan parent awalnya (jika current_path kosong)
    if has_config and not has_subdirs: # Hanya tambahkan jika bukan parent itu sendiri
        found_plugins.append(full_path)

    # Lakukan rekursi ke subdirektori
    for item in items:
        if item.get('type') == 'dir':
            # Buat path relatif baru untuk rekursi
            new_relative_path = os.path.join(current_path, item['name']).replace("\\", "/")
            # Tambahkan hasil rekursi ke daftar
            found_plugins.extend(find_plugins_in_parent(parent_path, new_relative_path))

    return found_plugins

def download_and_save(remote_path, local_path):
    print("\n[*] Mencoba mengunduh: {}".format(remote_path))
    file_meta = gh_api_get(remote_path)
    if file_meta is None or 'content' not in file_meta:
        print("    -> [INFO] File tidak ditemukan atau konten kosong, dilewati.")
        return None, None # Kembalikan None jika file tidak ada atau error
    try:
        content_b64 = file_meta.get("content", "")
        content_bytes = base64.b64decode(content_b64)
        local_dir = os.path.dirname(local_path)
        safe_makedirs(local_dir) # Buat direktori lokal jika belum ada

        print("    -> [FILE] Menyiapkan untuk menyimpan: {}".format(local_path))
        if not DRY_RUN:
            # Tulis sebagai binary ('wb')
            with open(local_path, "wb") as f:
                f.write(content_bytes)
        else:
            print("    -> [DRY RUN] Penulisan file dilewati.")

        full_slug = None
        # Ekstrak full_slug jika ini file TSV
        if remote_path.endswith('_plugin-sids.tsv'):
            full_slug = os.path.basename(remote_path).replace('_plugin-sids.tsv', '')

        # Kembalikan path lokal dan slug (jika ada)
        return local_path, full_slug
    except (TypeError, base64.binascii.Error) as e:
         print("    -> [ERROR] Gagal decode base64 dari {}: {}".format(remote_path, e))
         return None, None
    except Exception as e:
        # Tangkap error lain saat membuat direktori atau menulis file
        print("    -> [ERROR] Gagal memproses/menyimpan file {}: {}".format(local_path, e))
        return None, None

# ====== FUNGSI ALUR BARU (NEW FLOW) ======
def select_from_list(options, title, can_go_back=False):
    print_header(title)
    if not options: print("[WARN] Tidak ada pilihan tersedia."); return None # Handle jika list kosong
    if can_go_back: print("0. Kembali ke langkah sebelumnya")
    for i, option in enumerate(options, 1): print("{}. {}".format(i, option))
    while True:
        prompt = "Pilihan Anda [1-{} {}]: ".format(len(options), "atau 0" if can_go_back else "")
        choice = input(prompt).strip()
        if can_go_back and choice == '0': return BACK_COMMAND
        if choice.isdigit():
             idx = int(choice) - 1
             if 0 <= idx < len(options): return options[idx] # Kembalikan item yang dipilih
        print("[ERROR] Pilihan tidak valid.")

def select_plugins_from_list(available_plugins, title):
    print_header(title)
    if not available_plugins: print("[WARN] Tidak ada plugin tersedia."); return [] # Handle list kosong
    for i, plugin in enumerate(available_plugins, 1): print("{}. {}".format(i, plugin))
    while True:
        choice_str = input("\nMasukkan nomor (cth: 1, 3, 5-7) atau 'b' kembali: ").strip().lower()
        if choice_str == 'b': return BACK_COMMAND
        if not choice_str: continue # Ulangi jika kosong
        selected_indices = set()
        valid = True
        for part in choice_str.split(','):
            part = part.strip();
            if not part: continue
            if '-' in part:
                try:
                    start, end = map(int, part.split('-'))
                    if start > end or not (1 <= start <= len(available_plugins)) or not (1 <= end <= len(available_plugins)): raise ValueError
                    selected_indices.update(range(start - 1, end)) # Indeks 0-based
                except ValueError: print("[ERROR] Rentang '{}' tidak valid.".format(part)); valid = False; break
            else:
                try:
                    idx = int(part) - 1 # Indeks 0-based
                    if not (0 <= idx < len(available_plugins)): raise ValueError
                    selected_indices.add(idx)
                except ValueError: print("[ERROR] Pilihan '{}' tidak valid.".format(part)); valid = False; break
        if valid and selected_indices: return sorted([available_plugins[i] for i in selected_indices])
        elif valid: print("[ERROR] Tidak ada nomor valid yang dimasukkan.") # Jika input valid tapi kosong

def select_passive_scope(focal_plugin_path):
    print_header("Cakupan Sinkronisasi Pasif")
    print("Fokus utama: '{}'".format(focal_plugin_path))
    print("Plugin lain dalam cakupan akan diunduh & didaftarkan, tapi tidak didistribusi.")
    parts = focal_plugin_path.split('/')
    options = [("none", "Jangan sertakan plugin lain.")]
    # Tambahkan opsi scope dari yang paling spesifik
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
            return "none", None # 'none' adalah pilihan 1
        print("[ERROR] Pilihan tidak valid.")

def select_active_notifications(focal_plugins):
    print_header("Aktivasi Notifikasi Email (Hanya Fokal)")
    if not focal_plugins: print("[INFO] Tidak ada plugin fokal untuk dipilih."); return [] # Handle jika tidak ada fokal
    print("Pilih plugin FOKAL yang ingin diaktifkan notifikasinya.\n")
    for i, plugin in enumerate(focal_plugins, 1): print("{}. {}".format(i, plugin))
    print("\nPilihan: Nomor (1, 3), 'A' (Semua), 'b' (Kembali), Enter (Tidak ada)")
    while True:
        choice_str = input("\nPlugin yang notifikasinya aktif: ").strip().lower()
        if not choice_str: return [] # Enter -> Kosong
        if choice_str == 'b': return BACK_COMMAND
        if choice_str == 'a': return focal_plugins # 'A' -> Semua fokal
        selected_indices = set(); valid = True
        for part in choice_str.split(','):
            part = part.strip();
            if not part: continue
            if not part.isdigit(): valid = False; break
            idx = int(part) - 1 # Indeks 0-based
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
    print("Cakupan Pasif        : {}".format(selection.get('passive_scope_desc', 'N/A')))
    print("Total Plugin Proses  : {} ({} fokal + {} pasif)".format(len(total), len(focal), len(passive)))
    print("Aksi Utama           : {}".format(selection.get('action', 'N/A')))
    print("Plugin Aktif (Notif) : {} dari {} fokal".format(len(active_notif), len(focal)))
    for p in active_notif: print("    - {}".format(p))
    print("="*60)

# ====== FUNGSI PROSES & DISTRIBUSI ======
def process_plugin(plugin_path):
    print_header("Mengunduh Plugin: {}".format(plugin_path))
    # Dapatkan slug dari path
    parts = plugin_path.split('/');
    if not parts: return None # Path aneh
    # Cari file TSV secara eksplisit untuk dapat slug
    slug_found = None
    potential_tsv_name = "{}_plugin-sids.tsv".format("-".join(parts)) # Coba tebak nama TSV
    items_in_dir = gh_api_get(plugin_path) # Baca isi direktori plugin
    if isinstance(items_in_dir, list):
         for item in items_in_dir:
              if item.get('type') == 'file' and item.get('name','').endswith('_plugin-sids.tsv'):
                   slug_found = item['name'].replace('_plugin-sids.tsv', '')
                   break # Ambil slug dari nama file TSV
    if not slug_found:
        print("[WARN] Gagal menentukan 'full_slug' (file TSV tidak ditemukan?) di {}. Dilewati.".format(plugin_path))
        return None
    full_slug = slug_found
    print("[INFO] Ditemukan 'full_slug': {}".format(full_slug))
    local_plugin_dir = os.path.join(OUT_DIR, plugin_path) # Path lokal = pulled_configs/path/asli
    # Daftar file penting untuk diunduh
    paths_to_download = {
        "tsv":         "{}_plugin-sids.tsv".format(full_slug),
        "updater_cfg": "{}_updater.json".format(full_slug), # Krusial
        "json_dict":   "{}_plugin-sids.json".format(full_slug),
        "directive":   "directives_{}_{}.json".format(BACKEND_POD, full_slug),
        "conf70":      "70_dsiem-plugin_{}.conf".format(full_slug),
        "vector_conf": "70_transform_dsiem-plugin-{}.yaml".format(full_slug),
    }
    downloaded_files = {"full_slug": full_slug, "path": plugin_path} # Simpan slug & path asli
    successful_downloads = 0
    for key, filename in paths_to_download.items():
        # Path file di repo = path direktori plugin + nama file
        remote_path = os.path.join(plugin_path, filename).replace("\\", "/")
        local_path = os.path.join(local_plugin_dir, filename) # Path simpan lokal
        saved_path, _ = download_and_save(remote_path, local_path)
        if saved_path: downloaded_files[key] = saved_path; successful_downloads += 1
    # Kembalikan hanya jika file updater (krusial) berhasil diunduh
    if "updater_cfg" in downloaded_files:
        print("[INFO] Unduhan selesai untuk {}. Berhasil {} file.".format(full_slug, successful_downloads))
        return downloaded_files
    else:
        print("[ERROR] File krusial '{}_updater.json' gagal diunduh untuk {}. Plugin dilewati.".format(full_slug, plugin_path))
        return None

def distribute_logstash(downloaded_files):
    print_header("Distribusi ke Logstash untuk: {}".format(downloaded_files.get('path', 'N/A')))
    if "conf70" in downloaded_files: safe_copy(downloaded_files["conf70"], LOGSTASH_PIPE_DIR)
    else: print("[WARN] File conf70 tidak ada.")
    if "json_dict" in downloaded_files: safe_makedirs(LOGSTASH_JSON_DICT_DIR); safe_copy(downloaded_files["json_dict"], LOGSTASH_JSON_DICT_DIR)
    else: print("[WARN] File json_dict tidak ada.")
    if "directive" in downloaded_files: safe_run_cmd(["kubectl", "cp", downloaded_files["directive"], "{}:/dsiem/configs/".format(FRONTEND_POD)])
    else: print("[WARN] File directive tidak ada.")

def distribute_vector(downloaded_files, parent):
    print_header("Distribusi ke Vector untuk: {}".format(downloaded_files.get('path', 'N/A')))
    if "vector_conf" in downloaded_files:
        vector_target_dir = os.path.join(VECTOR_CONFIG_BASE_DIR, parent); safe_makedirs(vector_target_dir)
        safe_copy(downloaded_files["vector_conf"], vector_target_dir)
    else: print("[WARN] File vector_conf tidak ada.")
    if "tsv" in downloaded_files:
        print("[DIST] Mencari direktori NFS 'dsiem-plugin-tsv' di {}...".format(NFS_BASE_DIR))
        if not DRY_RUN:
            nfs_target_dir = None
            try:
                if os.path.isdir(NFS_BASE_DIR): # Cek base dir dulu
                    for item in os.listdir(NFS_BASE_DIR):
                        item_path = os.path.join(NFS_BASE_DIR, item)
                        if os.path.isdir(item_path) and item.startswith("pvc-"):
                            potential_target = os.path.join(item_path, "dsiem-plugin-tsv")
                            if os.path.isdir(potential_target): nfs_target_dir = potential_target; break
            except Exception as e: print("    -> [ERROR] Gagal mencari direktori NFS: {}".format(e))
            if nfs_target_dir: print("    -> Ditemukan: {}".format(nfs_target_dir)); safe_copy(downloaded_files["tsv"], nfs_target_dir)
            else: print("    -> [ERROR] Direktori 'dsiem-plugin-tsv' tidak ditemukan di NFS path '{}'.".format(NFS_BASE_DIR))
        else: print("    -> [DRY RUN] Pencarian dan penyalinan NFS dilewati.")
    else: print("[WARN] File tsv tidak ada.")

def register_job(updater_path, is_focal_plugin, selected_action):
    """Menyalin, mendaftarkan, DAN menambahkan flag needs_distribution."""
    if not updater_path or not os.path.exists(updater_path):
        print("[REG] File updater '{}' tidak valid. Pendaftaran dilewati.".format(updater_path))
        return
    print("[REG] Mendaftarkan pekerjaan: {}".format(os.path.basename(updater_path)))
    updaters_dir = "updaters"; safe_makedirs(updaters_dir)
    final_config_path = os.path.join(updaters_dir, os.path.basename(updater_path))

    # --- Blok Baca, Modifikasi Flag, Tulis Ulang ---
    try:
        with io.open(updater_path, 'r', encoding='utf-8') as f: updater_data = json.load(f, object_pairs_hook=OrderedDict)
        # Tentukan flag: True jika aksi = "Distribusi...", False jika "HANYA..."
        distribution_flag = "Distribusi" in selected_action
        # Tambahkan/Update flag di 'layout'
        if 'layout' not in updater_data: updater_data['layout'] = OrderedDict() # Buat jika belum ada
        updater_data['layout']['needs_distribution'] = distribution_flag
        print("  [FLAG] Menetapkan needs_distribution = {} based on action.".format(distribution_flag))
        # Tulis kembali file ASLI (di pulled_configs) dengan flag baru
        # Gunakan io.open untuk tulis
        json_string_mod = json.dumps(updater_data, indent=2, ensure_ascii=False)
        try: unicode; json_string_mod = json_string_mod.decode('utf-8') if isinstance(json_string_mod, str) else json_string_mod
        except NameError: pass
        with io.open(updater_path, 'w', encoding='utf-8') as f_mod: f_mod.write(json_string_mod); f_mod.write(u'\n')
    except (IOError, JSONDecodeError, ValueError) as e:
        print("[ERROR] Gagal membaca/memodifikasi {}: {}. Flag mungkin tidak benar.".format(updater_path, e))
        # Lanjutkan proses, tapi flag mungkin salah
    # --- Akhir Blok Modifikasi ---

    # Salin file (yang mungkin sudah dimodif) ke folder 'updaters'
    safe_copy(updater_path, final_config_path)

    # Daftarkan path final ke master_jobs.json
    jobs_file = 'master_jobs.json'; jobs = []
    if os.path.exists(jobs_file):
        try:
            with io.open(jobs_file, 'r', encoding='utf-8') as f: jobs = json.load(f)
            if not isinstance(jobs, list): jobs = []
        except (JSONDecodeError, IOError): print("[WARN] Gagal baca {}, menimpa.".format(jobs_file)); jobs = []
    if final_config_path not in jobs: jobs.append(final_config_path); safe_save_json(jobs_file, sorted(jobs))
    else: print("[REG] Pekerjaan '{}' sudah terdaftar.".format(os.path.basename(final_config_path)))

def activate_plugin_notification(slug_to_activate, all_active_slugs_set):
    if slug_to_activate not in all_active_slugs_set:
        print("[ACTIVATE] Menandai '{}' untuk notifikasi email.".format(slug_to_activate))
        all_active_slugs_set.add(slug_to_activate); return True
    return False

def restart_stack(action):
    """Menjalankan restart stack berdasarkan aksi ('Logstash' atau 'Vector')."""
    if ask_yes_no("\nKonfigurasi didistribusikan. Restart stack sekarang?") != 'y':
        print("[INFO] Restart dibatalkan."); return
    print_header("Memulai Proses Restart")
    logstash_restarted = False
    if "Logstash" in action:
        print("[INFO] Menjalankan update & restart Logstash...")
        if os.path.isdir(LOGSTASH_HOME):
             safe_run_cmd(["./update-config-map.sh"], cwd=LOGSTASH_HOME, shell=True)
             safe_run_cmd(["./restart-logstash.sh"], cwd=LOGSTASH_HOME, shell=True)
             logstash_restarted = True
        else: print("[WARN] Direktori LOGSTASH_HOME '{}' tidak ada.".format(LOGSTASH_HOME))
    elif "Vector" in action:
        print("[INFO] Merestart pod Vector...")
        safe_run_cmd(["kubectl", "delete", "pod", "-l", VECTOR_POD_LABEL])
    # Selalu restart backend/frontend SETELAH pipeline (jika distribusi)
    # Cek apakah Logstash di-restart atau Vector dipilih
    if logstash_restarted or "Vector" in action:
         print("[INFO] Merestart pod Backend dan Frontend...")
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

    print_header("Skrip Pull & Distribusi Konfigurasi"); require_github()
    setup_customer_info()

    selection = {} # Dictionary untuk menyimpan state pilihan user
    # Coba load state terakhir
    if os.path.exists(LAST_SELECTION_FILE):
        if ask_yes_no("Ditemukan sesi terakhir. Lanjutkan?") == 'y':
            try:
                with io.open(LAST_SELECTION_FILE, 'r', encoding='utf-8') as f: selection = json.load(f)
            except Exception as e: print("[WARN] Gagal load sesi: {}. Mulai baru.".format(e)); selection = {}

    current_step = selection.get('current_step', 'parent') # Mulai dari step terakhir atau 'parent'

    # Loop state machine
    while current_step != 'done':

        # Simpan step saat ini untuk resume
        selection['current_step'] = current_step

        if current_step == 'parent':
            parent_devices = find_parent_devices()
            if not parent_devices: die("Tidak ada perangkat induk ditemukan di repo."); return
            result = select_from_list(parent_devices, "Pilih Perangkat (Parent)")
            if result is None: continue # Jika select_from_list return None (list kosong)
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
                selection.pop('scope_choice', None); current_step = 'scope_choice'; continue # Balik
            if "spesifik" in selection['scope_choice']:
                result = select_plugins_from_list(all_plugins_in_parent, "Pilih Plugin Fokal")
                if result == BACK_COMMAND: selection.pop('scope_choice', None); current_step = 'scope_choice'; continue
                if not result: print("[INFO] Tidak ada plugin dipilih."); continue # Ulangi
                selection['focal_plugins'] = result; current_step = 'passive_scope'
            else: # Proses SEMUA
                selection['focal_plugins'] = all_plugins_in_parent
                # Langsung set state pasif & lanjut ke aksi
                selection['passive_scope'] = 'none'; selection['passive_scope_desc'] = 'N/A (semua fokal)'; selection['passive_plugins'] = []
                selection['plugins_to_process'] = all_plugins_in_parent; current_step = 'action'; continue

        elif current_step == 'passive_scope':
             # Hanya dijalankan jika memilih 'spesifik'
            scope_key, scope_path = select_passive_scope(selection['focal_plugins'][0])
            selection['passive_scope'] = scope_key
            if scope_key != 'none':
                print("[INFO] Mencari plugin pasif di scope '{}'...".format(scope_path))
                all_plugins_in_scope = find_plugins_in_parent(scope_path)
                selection['passive_plugins'] = sorted([p for p in all_plugins_in_scope if p not in selection['focal_plugins']])
                selection['passive_scope_desc'] = "Semua di '{}' (kecuali fokal)".format(scope_path)
            else:
                selection['passive_plugins'] = []; selection['passive_scope_desc'] = "Tidak ada"
            selection['plugins_to_process'] = sorted(list(set(selection['focal_plugins'] + selection['passive_plugins']))); current_step = 'action'

        elif current_step == 'action':
            actions = ["Distribusi & Konfigurasi Auto-Update ke Logstash", "Distribusi & Konfigurasi Auto-Update ke Vector", "HANYA Konfigurasi Auto-Update (Tanpa Distribusi)"]
            result = select_from_list(actions, "Pilih Aksi Utama", can_go_back=True)
            if result == BACK_COMMAND:
                # Tentukan step sebelumnya berdasarkan alur
                prev_step = 'passive_scope' if 'passive_scope' in selection else ('focal_plugins' if 'spesifik' in selection.get('scope_choice','') else 'scope_choice')
                # Hapus state yang relevan saat kembali
                selection.pop('passive_scope', None); selection.pop('plugins_to_process', None); selection.pop('passive_plugins', None); selection.pop('passive_scope_desc', None)
                if prev_step == 'focal_plugins': selection.pop('focal_plugins', None)
                current_step = prev_step; continue
            selection['action'] = result; current_step = 'active_plugins'

        elif current_step == 'active_plugins':
            result = select_active_notifications(selection.get('focal_plugins', []))
            if result == BACK_COMMAND: selection.pop('action', None); current_step = 'action'; continue
            selection['active_plugins'] = result; current_step = 'summary'

        elif current_step == 'summary':
            display_summary(selection)
            confirm = ask_yes_no("Lanjutkan dengan pekerjaan ini?", allow_back=True)
            if confirm == 'n': print("[INFO] Proses dibatalkan."); current_step = 'done'; continue
            if confirm == BACK_COMMAND: selection.pop('active_plugins', None); current_step = 'active_plugins'; continue
            current_step = 'execute' # Lanjut ke eksekusi

        elif current_step == 'execute':
            print_header("Memulai Eksekusi")
            # Simpan state terakhir sebelum mulai
            selection['current_step'] = 'execute' # Tandai sudah di eksekusi
            safe_save_json(LAST_SELECTION_FILE, selection)

            all_downloaded_files = [] # List untuk menyimpan hasil process_plugin
            print("[INFO] Mengunduh {} plugin...".format(len(selection['plugins_to_process'])))
            for path in selection['plugins_to_process']:
                processed_data = process_plugin(path)
                if processed_data: all_downloaded_files.append(processed_data)
                # Berhenti jika salah satu gagal? Atau lanjut? Lanjut saja.
            if not all_downloaded_files: die("Tidak ada file plugin yang berhasil diunduh. Proses dihentikan.")

            # Registrasi SEMUA job + set flag distribusi
            print_header("Registrasi Pekerjaan Auto-Update & Set Flag Distribusi")
            focal_paths_set = set(selection['focal_plugins'])
            selected_action = selection['action']
            for downloaded in all_downloaded_files:
                if "updater_cfg" in downloaded:
                    is_focal = downloaded['path'] in focal_paths_set
                    register_job(downloaded["updater_cfg"], is_focal, selected_action)
                else: print("[WARN] File updater_cfg tidak ada untuk {}, registrasi dilewati.".format(downloaded.get('path', 'N/A')))

            # Aktivasi notifikasi (Update active_plugins.json)
            print_header("Aktivasi Notifikasi Email")
            active_slugs_set = set() # Mulai dengan set kosong
            active_file_path = 'active_plugins.json'
            if os.path.exists(active_file_path):
                try:
                    with io.open(active_file_path, 'r', encoding='utf-8') as f: loaded_list = json.load(f)
                    if isinstance(loaded_list, list): active_slugs_set = set(loaded_list)
                except (JSONDecodeError, IOError): print("[WARN] Gagal baca {}.".format(active_file_path))
            # Tambahkan slug dari pilihan user ('active_plugins')
            slugs_to_activate = set()
            for active_path in selection['active_plugins']:
                # Cari slug berdasarkan path dari hasil download
                for d in all_downloaded_files:
                     if d.get('path') == active_path and d.get('full_slug'):
                          slugs_to_activate.add(d['full_slug']); break
            made_changes_to_active = False
            for slug_to_add in slugs_to_activate:
                 if activate_plugin_notification(slug_to_add, active_slugs_set): made_changes_to_active = True
            # Tulis kembali HANYA jika ada perubahan
            if made_changes_to_active:
                 print("[INFO] Menyimpan perubahan ke {}...".format(active_file_path))
                 safe_save_json(active_file_path, sorted(list(active_slugs_set)))
            else: print("[INFO] Tidak ada perubahan status notifikasi.")

            # Distribusi HANYA JIKA dipilih & HANYA untuk FOKAL
            distributed_something = False
            if "Distribusi" in selection['action']:
                print_header("Distribusi Plugin Fokal")
                distributed_count = 0
                for downloaded in all_downloaded_files:
                    if downloaded['path'] in focal_paths_set: # Cek jika fokal
                        if "Logstash" in selection['action']: distribute_logstash(downloaded); distributed_count += 1
                        elif "Vector" in selection['action']: distribute_vector(downloaded, selection['parent']); distributed_count += 1
                if distributed_count > 0: distributed_something = True; print("[INFO] {} plugin fokal didistribusikan.".format(distributed_count))
                else: print("[INFO] Tidak ada plugin fokal untuk didistribusikan.")
            else: print("[INFO] Aksi 'HANYA Konfigurasi Auto-Update', distribusi dilewati.")

            # Restart HANYA JIKA distribusi terjadi
            if distributed_something: restart_stack(selection['action'])
            elif "Distribusi" in selection['action']: print("[INFO] Tidak ada file didistribusikan, restart dilewati.")

            # Hapus file state jika selesai (dan bukan dry run)
            if os.path.exists(LAST_SELECTION_FILE) and not DRY_RUN:
                try: print("[INFO] Menghapus file state sesi..."); os.remove(LAST_SELECTION_FILE)
                except OSError as e: print("[WARN] Gagal hapus {}: {}".format(LAST_SELECTION_FILE, e))
            print_header("Proses Selesai")
            if DRY_RUN: print("Mode Dry Run selesai.")
            current_step = 'done' # Keluar loop utama

        else: # Step tidak valid
             print("[WARN] Step tidak dikenali: {}. Kembali ke awal.".format(current_step))
             selection = {}; current_step = 'parent' # Reset dan mulai ulang

# Entry point
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nProses dibatalkan oleh pengguna.")
        sys.exit(1)
    except Exception as e:
         # Tangkap error tak terduga lainnya
         print("\n\n[FATAL ERROR] Terjadi kesalahan:")
         import traceback
         traceback.print_exc() # Cetak traceback lengkap
         sys.exit(1)