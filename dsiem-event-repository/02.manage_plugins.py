#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import sys
import json
import io
import re
import datetime
import shutil
import subprocess
import requests
import base64
from collections import OrderedDict

# --- Penyesuaian Kompatibilitas Py2/Py3 ---
try: JSONDecodeError = json.JSONDecodeError 
except AttributeError: JSONDecodeError = ValueError          
try: FileNotFoundError 
except NameError: FileNotFoundError = IOError           
# --- AKHIR BLOK KOMPATIBILITAS ---

# ====== KONFIGURASI UMUM ======
GITHUB_REPO   = os.getenv("GITHUB_REPO")
GITHUB_TOKEN  = os.getenv("GITHUB_TOKEN")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH")
OUT_DIR = os.getenv("OUT_DIR", "./pulled_configs")

# ====== KONFIGURASI DISTRIBUSI & RESTART (Dibutuhkan untuk Helper) ======
LOGSTASH_PIPE_DIR = os.getenv("LOGSTASH_PIPE_DIR")
LOGSTASH_JSON_DICT_DIR = os.getenv("LOGSTASH_JSON_DICT_DIR")
LOGSTASH_HOME     = os.getenv("LOGSTASH_HOME")
VECTOR_CONFIG_BASE_DIR = os.getenv("VECTOR_CONFIG_BASE_DIR")
NFS_BASE_DIR           = os.getenv("NFS_BASE_DIR")
FRONTEND_POD      = "dsiem-frontend-0"
BACKEND_POD       = "dsiem-backend-0"
VECTOR_POD_LABEL  = "app=vector-parser"
DRY_RUN = False 

# Konstan file
MASTER_JOBS_FILE = 'master_jobs.json'
ACTIVE_PLUGINS_FILE = 'active_plugins.json'
CUSTOMER_FILE = 'customer.json'
REPORT_DATA_FILE = "integrations.json"
REPORT_DIR = "monitoring-integration"
REPORT_FILENAME_SUFFIX = "_integration-status.md"

# ====== FUNGSI I/O & HELPERS (Disederhanakan, diasumsikan lengkap di file Anda) ======

def py_input(prompt):
    try: return raw_input(prompt)
    except NameError: return input(prompt)

def print_header(title):
    print("\n" + "="*70); print("=== {}".format(title.upper())); print("="*70)

def load_json_safe(path, ordered=False):
    if not os.path.exists(path): return None
    try:
        with io.open(path, 'r', encoding='utf-8') as f:
            if ordered: return json.load(f, object_pairs_hook=OrderedDict)
            return json.load(f)
    except Exception as e:
        print("[ERROR] Gagal membaca {}: {}".format(path, e)); return None

def safe_save_json(path, obj):
    """Menyimpan data, mendukung JSON dan String (Markdown)."""
    print("[FILE] Menyiapkan untuk menyimpan: {}".format(path))
    if DRY_RUN: print("    -> [DRY RUN] Penulisan file dilewati."); return True
    try:
        # Jika obj adalah string (untuk file Markdown)
        if isinstance(obj, str):
             try: unicode; obj = obj.decode('utf-8') if isinstance(obj, str) else obj
             except NameError: pass
             
             with io.open(path, 'w', encoding='utf-8') as f:
                 f.write(obj); f.write(u'\n')
             print("    -> [OK] Berhasil disimpan (Text).")
             return True
        
        # Jika obj adalah JSON (Dict/List)
        json_string = json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False)
        try: unicode; json_string = json_string.decode('utf-8') if isinstance(json_string, str) else json_string
        except NameError: pass

        with io.open(path, 'w', encoding='utf-8') as f:
            f.write(json_string); f.write(u'\n')
        print("    -> [OK] Berhasil disimpan (JSON).")
        return True
    except Exception as e:
        print("    -> [ERROR] Gagal menyimpan file {}: {}".format(path, e))
        return False

def get_customer_name():
    data = load_json_safe(CUSTOMER_FILE)
    if data: return data.get("customer_info", {}).get("customer_name", "Unknown Customer")
    return "Belum Dikonfigurasi"

# --- FUNGSI REPORTING & STATUS ---

def get_active_plugins():
    data = load_json_safe(ACTIVE_PLUGINS_FILE)
    if isinstance(data, list): return set(data)
    return set()

def get_gmt_plus_7_timestamp():
    utc_dt = datetime.datetime.utcnow()
    gmt_plus_7_dt = utc_dt + datetime.timedelta(hours=7)
    return gmt_plus_7_dt.strftime("%Y-%m-%d %H:%M:%S WIB (GMT+7)")

def scan_integrations_from_jobs():
    jobs = load_json_safe(MASTER_JOBS_FILE)
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
        
        integrations.append({
            "job_path": job_path, 
            "slug": slug, "target": target, "needs_dist": needs_dist, 
            "is_active": is_active, 
        })
    
    return sorted(integrations, key=lambda x: x['slug'])

def get_status_display(item):
    """Menentukan status display berdasarkan terminologi baru."""
    if item['target'] == 'None':
        return "❌ Target Not Found"
    elif item['needs_dist']:
        return "⚠️ Local Only"
    else:
        return "✅ Active"


def update_integration_report(customer_name, new_integrations):
    customer_slug = re.sub(r'[^a-z0-9]+', '-', customer_name.lower()).strip('-')
    if not customer_slug: customer_slug = "unknown-customer"

    current_timestamp = get_gmt_plus_7_timestamp()
    full_report = load_json_safe(REPORT_DATA_FILE)
    if not isinstance(full_report, dict): full_report = {}

    integration_map = {}
    for item in new_integrations:
        temp_item = item.copy()
        temp_item.pop('job_path', None)
        integration_map[item['slug']] = temp_item

    current_customer_data = {
        "customer_name": customer_name, "last_updated": current_timestamp,
        "integrations": integration_map
    }

    full_report[customer_slug] = current_customer_data
    safe_save_json(REPORT_DATA_FILE, full_report)
    
    markdown_filename = "{}{}".format(customer_slug, REPORT_FILENAME_SUFFIX)
    
    # PERBAIKAN: Membuat direktori dengan penanganan error yang baik
    try: 
         import os
         if not os.path.exists(REPORT_DIR): 
             os.makedirs(REPORT_DIR)
    except Exception as e:
        print("[ERROR] Gagal membuat direktori REPORT_DIR: {}".format(e))
        
    local_markdown_path = os.path.join(REPORT_DIR, markdown_filename)
    markdown_content = generate_single_markdown_report(current_customer_data, new_integrations)
    
    safe_save_json(local_markdown_path, markdown_content) 
    
    print("[REPORT] File laporan Markdown berhasil dibuat: {}".format(local_markdown_path))
    return local_markdown_path

def generate_single_markdown_report(cust_data, all_integrations):
    lines = []; timestamp = cust_data['last_updated']
    customer_name = cust_data['customer_name']; integrations_map = cust_data['integrations']
    
    lines.append("# Status Integrasi SIEM - {}".format(customer_name))
    lines.append("\n**Waktu Pemrosesan Terakhir:** {}\n".format(timestamp))
    lines.append("---")
    
    # === TABEL DETAIL ===
    lines.append("\n## ⚙️ Detail Integrasi Terdaftar")

    # Kolom baru: Aksi/Status (Menggabungkan kebutuhan pull dan target)
    header = "| Plugin / Device (Slug) | Target Engine | Status Distribusi | Notifikasi Email | Kesimpulan Aksi Wajib |"
    separator = "| :--- | :---: | :---: | :---: | :--- |"
    lines.append(header); lines.append(separator)

    # Variabel untuk Kesimpulan Umum
    needs_pull_count = sum(1 for item in all_integrations if item['needs_dist'] and item['target'] != 'None')
    none_target_count = sum(1 for item in all_integrations if item['target'] == 'None')

    if not integrations_map:
        lines.append("| *Belum ada integrasi yang terdaftar.* | - | - | - | - |")
    else:
        sorted_integrations = sorted(all_integrations, key=lambda x: x['slug'])
        for item in sorted_integrations:
             # Menggunakan terminologi baru
             status_text = get_status_display(item)
             notification = item['is_active'] and "🔔 AKTIF" or "🔕 PASIF"
             
             # Menentukan Kesimpulan Aksi Wajib Inline
             if item['target'] == 'None':
                 action_summary = "SET TARGET (RUN 01)"
             elif item['needs_dist']:
                 action_summary = "RUN 01.PULL-DIRECTIVE.PY"
             else:
                 action_summary = "N/A"
             
             row = "| **{}** | {} | {} | {} | **{}** |".format(
                item['slug'], item['target'], status_text, notification, action_summary
            )
             lines.append(row)
    lines.append("---")

    # === BAGIAN KESIMPULAN UMUM (Di bawah tabel) ===
    
    lines.append("\n## 🧠 Ringkasan Status Global & Aksi")
    
    # Aturan 1: Status Target
    if none_target_count > 0:
         lines.append("\n* **❌ Target Belum Diset (Kritis):** Terdapat **{}** plugin yang memiliki **Target Engine: None**.".format(none_target_count))
         lines.append("  * **Aksi Wajib:** Target harus ditetapkan saat menjalankan **`01.pull-directive.py`**.")
    
    # Aturan 2: Status Need Pull
    if needs_pull_count > 0:
        lines.append("\n* **⚠️ Status Pull/Deploy:** Terdapat **{}** plugin yang sudah memiliki target tetapi ditandai **⚠️ Local Only**.".format(needs_pull_count))
        lines.append("  * **Aksi Wajib:** Jalankan **`01.pull-directive.py`** untuk mendorong perubahan ke pipeline.")

    # Status Final
    if none_target_count == 0 and needs_pull_count == 0:
         lines.append("\n* **Status Final:** Semua {} plugin berada dalam status **✅ Active**.".format(len(all_integrations)))
    
    # Aturan 3: Notifikasi
    active_notif_count = sum(1 for item in all_integrations if item['is_active'])
    if active_notif_count > 0:
        lines.append("\n* **🔔 Notifikasi Email:** Sebanyak **{}** plugin memiliki notifikasi email yang **AKTIF**.".format(active_notif_count))
    else:
        lines.append("\n* **🔕 Notifikasi Email:** Semua plugin ditandai **PASIF**.")
    
    return "\n".join(lines)

def gh_api_put_file(file_path):
    # Implementasi disalin dari konteks sebelumnya
    import requests, base64
    
    print("[GH UPLOAD] Menyiapkan upload otomatis...")
    if DRY_RUN: print("[GH UPLOAD] [DRY RUN] Upload file '{}' dilewati.".format(file_path)); return True
    if not GITHUB_REPO or not GITHUB_TOKEN:
        print("[GH UPLOAD] [ERROR] GITHUB_REPO/TOKEN tidak diset. Upload dilewati."); return False

    remote_path = os.path.join(REPORT_DIR, os.path.basename(file_path)).replace("\\", "/")
    url = "https://api.github.com/repos/{}/contents/{}".format(GITHUB_REPO, remote_path)

    try:
        with open(file_path, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode('utf-8')
    except Exception as e:
        print("[GH UPLOAD] [FATAL] Gagal membaca file lokal: {}".format(e)); return False

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
        return False
        
def gh_headers():
    return { "Accept": "application/vnd.github+json", "Authorization": "Bearer {}".format(GITHUB_TOKEN), "X-GitHub-Api-Version": "2022-11-28" }

# --- FUNGSI MANAJEMEN UTAMA LOGIKA APLIKASI ---

def update_job_distribution_status(job_path, needs_distribution):
    updater_data = load_json_safe(job_path, ordered=True)
    if not updater_data: return False
        
    target = updater_data.get("layout", {}).get("distribution_target", "None")
    
    if needs_distribution and target == "None":
         print("[ERROR] Plugin wajib memiliki Target Engine (Logstash/Vector) sebelum ditandai 'Need Pull/Deploy'.")
         return False
        
    if 'layout' not in updater_data: updater_data['layout'] = OrderedDict()
    
    if not needs_distribution and 'needs_distribution' in updater_data['layout']:
        del updater_data['layout']['needs_distribution']
    elif needs_distribution:
        updater_data['layout']['needs_distribution'] = True
        
    return safe_save_json(job_path, updater_data)


def toggle_plugin_status(integrations_list, item_indices_to_toggle, active_plugins_set, field):
    toggled_summary = []
    
    for index in sorted(list(item_indices_to_toggle)):
        item = integrations_list[index]
        slug = item['slug']
        
        if field == 'notification':
            is_currently_active = slug in active_plugins_set
            if is_currently_active:
                active_plugins_set.remove(slug)
                toggled_summary.append("-> Notifikasi '{}' diubah menjadi 🔕 Pasif.".format(slug))
            else:
                active_plugins_set.add(slug)
                toggled_summary.append("-> Notifikasi '{}' diubah menjadi 🔔 AKTIF.".format(slug))
        
        elif field == 'needs_dist':
            job_path = item['job_path']
            is_currently_needed = item['needs_dist']
            
            if not is_currently_needed and item['target'] == "None":
                toggled_summary.append("[ERROR] Gagal mengubah: '{}' wajib memiliki Target Engine sebelum ditandai 'Local Only'.".format(slug))
                continue 
            
            if update_job_distribution_status(job_path, not is_currently_needed):
                if is_currently_needed:
                    toggled_summary.append("-> Status '{}' diubah menjadi ✅ Active.".format(slug))
                else:
                    toggled_summary.append("-> Status '{}' diubah menjadi ⚠️ Local Only.".format(slug))
            else:
                 toggled_summary.append("-> [ERROR] Gagal mengubah status distribusi untuk '{}'.".format(slug))
    
    return toggled_summary

def parse_choice_input(choice_str, max_len):
    indices_to_toggle = set()
    is_valid_input = True
    
    parts = choice_str.split(',')
    for part in parts:
        part = part.strip()
        if not part: continue

        if '-' in part:
            try:
                start, end = map(int, part.split('-'))
                if start > end or not (1 <= start <= max_len) or not (1 <= end <= max_len):
                    is_valid_input = False; break
                indices_to_toggle.update(range(start - 1, end))
            except ValueError:
                is_valid_input = False; break
        else:
            try:
                index = int(part) - 1
                if not (0 <= index < max_len):
                    is_valid_input = False; break
                indices_to_toggle.add(index)
            except ValueError:
                is_valid_input = False; break
    
    return indices_to_toggle if is_valid_input else None

def manage_distribution_flow(integrations_list, active_plugins):
    """Mengelola Target Engine dan Needs Distribution flag (Opsi 2)."""
    
    while True:
        print_header("2. KELOLA STATUS DISTRIBUSI (PULL FLAG)")
        
        for i, item in enumerate(integrations_list, 1):
            status_display = get_status_display({'target': item['target'], 'needs_dist': item['needs_dist']})
            
            # Format output baris
            output = "{:3d}. {:<25} [Target: {:<8}] Status: {}".format(i, item['slug'], item['target'], status_display)
            if item['needs_dist'] and item['target'] == 'None':
                 output += " (KRITIS: SET TARGET DENGAN 01!)" 
            print(output)
        
        print("-" * 70)
        print("Aksi Tersedia:")
        print("  - Masukkan nomor/range (cth: 1, 3-5) untuk **MENGUBAH STATUS PULL/DEPLOY** (Active <-> Local Only).")
        print("  - 'S' untuk **SIMPAN PERUBAHAN**")
        print("  - 'b' untuk Kembali ke menu utama.")

        # Ambil input aksi
        action_input = py_input("\nAksi Anda (Nomor/Range atau S): ").strip().lower()
        
        if action_input == 'b': return
        
        if action_input == 's':
            print("\n[INFO] Menyimpan perubahan dan memulai sinkronisasi laporan (Opsi 3)...")
            return 'save_and_sync'

        # Aksi: Ubah needs_distribution flag
        indices = parse_choice_input(action_input, len(integrations_list))
        if indices is None:
            print("\n[ERROR] Input tidak valid. Harap masukkan nomor/range atau 'S'/'b'.")
            continue
        
        summary = toggle_plugin_status(integrations_list, indices, active_plugins, 'needs_dist')
        print("\n" + "\n".join(summary))
        integrations_list = scan_integrations_from_jobs()

# --- MAIN LOOP ---

def main_loop():
    
    if not all([GITHUB_REPO, GITHUB_TOKEN]):
         print("[ERROR] Pastikan variabel lingkungan GITHUB_REPO dan GITHUB_TOKEN sudah diset.")
         return

    integrations_list = scan_integrations_from_jobs()
    if not integrations_list:
        print("[ERROR] Tidak ada plugin yang terdaftar di '{}'. Jalankan 01.pull-directive.py terlebih dahulu.".format(MASTER_JOBS_FILE))
        return

    active_plugins = get_active_plugins()
    
    while True:
        integrations_list = scan_integrations_from_jobs()
        active_plugins = get_active_plugins()
        
        print_header("Manajemen Status Plugin Terdistribusi")
        
        # MENU UTAMA YANG DISEDERHANAKAN
        print("Pilih opsi manajemen:")
        print("1. Kelola **Notifikasi Email** (Aktif/Pasif)")
        print("2. Kelola **Status Distribusi** (Target Engine & Pull Flag)")
        print("3. Sinkronisasi & **Upload Laporan Status (.md)** ke GitHub")
        print("q. Keluar")
        
        choice = py_input("\nPilihan Anda: ").strip().lower()
        
        if choice == 'q':
            print("Keluar dari manajemen.")
            break
        
        elif choice == '3':
            print_header("SINKRONISASI & UPLOAD LAPORAN")
            print("[INFO] Menggunakan data terbaru untuk membuat laporan dan mengunggahnya...")
            customer_name_current = get_customer_name()
            local_report_path = update_integration_report(customer_name_current, integrations_list)
            if local_report_path: gh_api_put_file(local_report_path)
            continue
            
        elif choice == '2':
            result = manage_distribution_flow(integrations_list, active_plugins)
            if result == 'save_and_sync':
                 # Jika Opsi 2 keluar dengan sinyal simpan/sinkronisasi, eksekusi Opsi 3
                 print_header("SINKRONISASI & UPLOAD LAPORAN (OTOMATIS)")
                 customer_name_current = get_customer_name()
                 local_report_path = update_integration_report(customer_name_current, integrations_list)
                 if local_report_path: gh_api_put_file(local_report_path)
            continue

        elif choice == '1':
            field_name = 'notification'
            
            print_header("1. KELOLA NOTIFIKASI EMAIL")
            
            for i, item in enumerate(integrations_list, 1):
                status_notif = "🔔 AKTIF" if item['is_active'] else "🔕 Pasif"
                print("{:3d}. {:<25} [Target: {:<8}] {}".format(i, item['slug'], item['target'], status_notif))
            
            print("-" * 70)
            print("Pilihan:")
            print("  - Masukkan nomor/range untuk mengubah status (cth: 1, 3-5)")
            print("  - 's' untuk **SIMPAN PERUBAHAN**.")
            print("  - 'b' untuk Kembali")

            temp_active_plugins = active_plugins.copy()
            
            while True:
                sub_choice = py_input("\nPilihan Anda: ").strip().lower()
                
                if sub_choice == 'b': break
                
                if sub_choice == 's':
                    if active_plugins != temp_active_plugins:
                        safe_save_json(ACTIVE_PLUGINS_FILE, sorted(list(temp_active_plugins)))
                        print("\n[INFO] Status notifikasi berhasil disimpan. Memulai sinkronisasi laporan (Opsi 3)...")
                        
                        print_header("SINKRONISASI & UPLOAD LAPORAN (OTOMATIS)")
                        customer_name_current = get_customer_name()
                        local_report_path = update_integration_report(customer_name_current, integrations_list)
                        if local_report_path: gh_api_put_file(local_report_path)
                    else:
                        print("\n[INFO] Tidak ada perubahan notifikasi untuk disimpan.")
                    break
                
                indices = parse_choice_input(sub_choice, len(integrations_list))
                
                if indices is None:
                    print("\n[ERROR] Input tidak valid.")
                    continue
                
                summary = toggle_plugin_status(integrations_list, indices, temp_active_plugins, field_name)
                     
                print("\n" + "\n".join(summary))

        else:
            print("\n[ERROR] Pilihan tidak valid.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Manage & Redistribute installed SIEM plugin configs.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without making changes.")
    args = parser.parse_args(); DRY_RUN = args.dry_run
    if DRY_RUN: print("\n" + "#"*60 + "\n### DRY RUN MODE. NO CHANGES WILL BE MADE. ###\n" + "#"*60)

    try:
        # Pengecekan dependensi dan eksekusi main loop
        import requests, base64, shutil, subprocess 
             
        main_loop()
        
    except KeyboardInterrupt:
        print("\n\nProses dibatalkan oleh pengguna.")
        sys.exit(1)
    except Exception as e:
         print("\n\n[FATAL ERROR] Terjadi kesalahan tak terduga:")
         import traceback
         traceback.print_exc()
         sys.exit(1)