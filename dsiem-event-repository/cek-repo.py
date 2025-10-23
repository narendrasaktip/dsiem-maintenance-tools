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

# ====== KONFIGURASI ENV ======
GITHUB_REPO   = os.getenv("GITHUB_REPO")
GITHUB_TOKEN  = os.getenv("GITHUB_TOKEN")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH")
OUT_DIR = os.getenv("OUT_DIR", "./pulled_configs")

# ====== KONFIGURASI DISTRIBUSI & RESTART ======
# --- Logstash ---
LOGSTASH_PIPE_DIR = "/root/kubeappl/logstash/configs/pipelines/dsiem-events/"
LOGSTASH_JSON_DICT_DIR = "/root/kubeappl/logstash/configs/pipelines/dsiem-events/dsiem-plugin-json/"
LOGSTASH_HOME     = "/root/kubeappl/logstash/"
# --- Vector ---
VECTOR_CONFIG_BASE_DIR = "/root/data/mgmt/kubeappl/vector-parser/configs/"
NFS_BASE_DIR           = "/root/data/nfs/"
# --- Kubernetes ---
FRONTEND_POD      = "dsiem-frontend-0"
BACKEND_POD       = "dsiem-backend-0"
VECTOR_POD_LABEL  = "app=vector-parser"


# ====== I/O & HELPERS ======
def save_json_utf8(path, obj):
    """
    Python2/3-safe JSON writer: dumps dengan ensure_ascii=False dan writes unicode.
    """
    import io as _io
    import json as _json
    data = _json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
    try:
        unicode  # noqa: F821 (py3 ignores)
        if isinstance(data, str):
            data = data.decode("utf-8")
    except NameError:
        pass
    with _io.open(path, "w", encoding="utf-8") as f:
        f.write(data + "\n")

def py_input(p):
    try:
        return raw_input(p)
    except NameError:
        return input(p)

def ask_yes_no(p):
    while True:
        a = py_input(p).strip().lower()
        if a in ("y", "n"): return a
        print("Ketik 'y' atau 'n'.")

def run_cmd(cmd, cwd=None, shell=False):
    print("\n[CMD] Menjalankan: {}".format(" ".join(cmd)))
    try:
        p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
        out, err = p.communicate()
        if out:
            print(out.decode('utf-8', 'replace'))
        if err:
            print("--- stderr ---\n{}".format(err.decode('utf-8', 'replace')))
        return p.returncode == 0
    except OSError as e:
        print("[ERROR] Gagal menjalankan perintah: {}".format(e))
        return False

# ====== FUNGSI INTI GITHUB ======
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
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print("\n[ERROR] Gagal menghubungi GitHub API: {}".format(e))
        return None

def find_plugins_recursively(path=""):
    found_plugins = []
    items = gh_api_get(path)
    if not isinstance(items, list):
        return []

    has_config = any(item.get('type') == 'file' and item.get('name') == 'config.json' for item in items)
    has_subdirs = any(item.get('type') == 'dir' for item in items)
    
    if has_config and not has_subdirs and path:
        found_plugins.append(path)

    for item in items:
        if item.get('type') == 'dir':
            deeper_plugins = find_plugins_recursively(item.get('path', ''))
            if deeper_plugins:
                found_plugins.extend(deeper_plugins)
    
    return found_plugins

def display_and_select_plugins(all_plugins):
    """Menampilkan menu dan mengizinkan pengguna memilih satu, lebih dari satu, atau rentang plugin."""
    print("\nSilakan pilih plugin yang akan diunduh:")
    for i, plugin_path in enumerate(all_plugins, 1):
        print("{}. {}".format(i, plugin_path))
    
    selected_paths = []
    while not selected_paths:
        prompt = "\nMasukkan nomor pilihan (cth: 1, 3, 7, 11-30): "
        choice_str = py_input(prompt).strip()
        if not choice_str:
            continue
            
        all_indices = []
        is_all_valid = True
        
        # Pertama, pisahkan berdasarkan koma untuk mendapatkan setiap input
        tokens = choice_str.split(',')
        
        for token in tokens:
            token = token.strip()
            if not token: continue

            # Kedua, periksa apakah token adalah sebuah rentang (mengandung '-')
            if '-' in token:
                try:
                    parts = token.split('-')
                    if len(parts) != 2:
                        raise ValueError("Format rentang tidak valid.")
                    
                    start = int(parts[0].strip())
                    end = int(parts[1].strip())
                    
                    if start > end:
                        print("[ERROR] Angka awal rentang '{}' harus lebih kecil dari angka akhir '{}'.".format(start, end))
                        is_all_valid = False
                        break
                        
                    if not (1 <= start <= len(all_plugins) and 1 <= end <= len(all_plugins)):
                        print("[ERROR] Rentang '{}' berada di luar pilihan valid (1-{}).".format(token, len(all_plugins)))
                        is_all_valid = False
                        break
                        
                    # Tambahkan semua nomor dalam rentang ke daftar (ubah ke indeks 0-based)
                    for i in range(start, end + 1):
                        all_indices.append(i - 1)
                        
                except ValueError:
                    print("[ERROR] Format rentang '{}' tidak valid. Gunakan format angka-angka (cth: 11-30).".format(token))
                    is_all_valid = False
                    break
            else:
                # Jika bukan rentang, proses sebagai angka tunggal
                try:
                    index = int(token) - 1 # Ubah ke indeks 0-based
                    if 0 <= index < len(all_plugins):
                        all_indices.append(index)
                    else:
                        print("[ERROR] Nomor '{}' tidak ada di dalam pilihan.".format(token))
                        is_all_valid = False
                        break
                except ValueError:
                    print("[ERROR] Input '{}' bukan angka yang valid.".format(token))
                    is_all_valid = False
                    break
        
        # Jika ada satu saja token yang tidak valid, ulangi proses input
        if not is_all_valid:
            continue

        # Jika semua valid, proses dan kembalikan hasilnya
        if all_indices:
            # Gunakan set untuk menghilangkan duplikat dan urutkan
            unique_indices = sorted(list(set(all_indices)))
            selected_paths = [all_plugins[i] for i in unique_indices]
            return selected_paths

# Ganti fungsi process_single_plugin() Anda dengan ini
def process_single_plugin(selected_path, dist_choice, is_explicit_choice):
    """
    Menjalankan seluruh proses untuk satu plugin.
    Parameter 'is_explicit_choice' menentukan apakah plugin ini akan diaktifkan.
    """
    print("\n" + "="*50)
    print("=== Memproses Plugin: {} ===".format(selected_path))
    if is_explicit_choice:
        print("=== Mode: Aktif (Pilihan Pengguna) ===")
    else:
        print("=== Mode: Pasif (Sinkronisasi Repositori) ===")
    print("="*50)

    # 1. Analisis direktori dan tentukan full_slug
    dir_contents = gh_api_get(selected_path)
    full_slug = None
    if dir_contents:
        for item in dir_contents:
            if item['type'] == 'file' and item['name'].endswith('_plugin-sids.tsv'):
                full_slug = item['name'].replace('_plugin-sids.tsv', '')
                break
    if not full_slug:
        print("[ERROR] Tidak dapat menentukan 'full_slug' dari direktori {}. Plugin dilewati.".format(selected_path))
        return False

    print("[INFO] Ditemukan 'full_slug': {}".format(full_slug))
    local_plugin_dir = os.path.join(OUT_DIR, selected_path)

    # 2. Rekonstruksi path dan unduh semua file terkait
    paths = {
        "conf70":      "70_dsiem-plugin_{}.conf".format(full_slug),
        "vector_conf": "70_transform_dsiem-plugin-{}.yaml".format(full_slug),
        "tsv":         "{}_plugin-sids.tsv".format(full_slug),
        "directive":   "directives_{}_{}.json".format(BACKEND_POD, full_slug),
        "json_dict":   "{}_plugin-sids.json".format(full_slug),
        "updater_cfg": "{}_updater.json".format(full_slug),
    }

    local_files = {}
    success_count = 0
    print("\n[*] Memulai proses unduh untuk {}...".format(full_slug))
    for key, filename in paths.items():
        remote_path = "{}/{}".format(selected_path, filename)
        local_path = os.path.join(local_plugin_dir, filename)
        local_files[key] = local_path
        if download_and_save(remote_path, local_path):
            success_count += 1
    
    if success_count == 0:
        print("[WARN] Tidak ada file yang berhasil diunduh untuk {}. Plugin dilewati.".format(full_slug))
        return False

    print("\n[*] Proses unduh untuk {} selesai. Berhasil {} file.".format(full_slug, success_count))

    # 3. Distribusi, Registrasi, dan Aktivasi SELEKTIF
    if dist_choice == '1':
        # Distribusi hanya dijalankan jika plugin dipilih secara eksplisit
        if is_explicit_choice:
            print("[INFO] Plugin ini aktif, menjalankan distribusi Logstash...")
            distribute_logstash(local_files["conf70"], local_files["directive"], local_files["json_dict"])
        else:
            print("[INFO] Melewatkan distribusi Logstash untuk plugin pasif.")
    elif dist_choice == '2':
        # Sama untuk Vector
        if is_explicit_choice:
            print("[INFO] Plugin ini aktif, menjalankan distribusi Vector...")
            master_folder = selected_path.split('/')[0]
            distribute_vector(local_files["vector_conf"], local_files["tsv"], master_folder)
        else:
            print("[INFO] Melewatkan distribusi Vector untuk plugin pasif.")
    
    # Registrasi pekerjaan SELALU dijalankan untuk semua plugin terkait
    register_pulled_job(local_files["updater_cfg"])
    
    # Aktivasi HANYA dijalankan jika plugin dipilih secara eksplisit
    if is_explicit_choice:
        activate_plugin(full_slug)
    
    return True

def download_and_save(remote_path, local_path):
    print("\n[*] Mencoba mengunduh: {}".format(remote_path))
    file_meta = gh_api_get(remote_path)

    if file_meta is None:
        print("    -> [INFO] File tidak ditemukan, dilewati.")
        return False

    try:
        content_b64 = file_meta.get("content", "")
        content_bytes = base64.b64decode(content_b64)
        
        local_dir = os.path.dirname(local_path)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        
        with open(local_path, "wb") as f:
            f.write(content_bytes)
            
        print("    -> [OK] Disimpan di: {}".format(local_path))
        return True
    except Exception as e:
        print("    -> [ERROR] Gagal menyimpan file: {}".format(e))
        return False

# ====== FUNGSI DISTRIBUSI & RESTART ======
def distribute_logstash(conf70_path, directive_path, json_dict_path):
    print("\n--- Memulai Distribusi untuk Logstash ---")
    # 1. Salin file .conf ke Logstash
    if os.path.exists(conf70_path):
        print("[DIST] Menyalin {} ke {}".format(conf70_path, LOGSTASH_PIPE_DIR))
        try:
            shutil.copy(conf70_path, LOGSTASH_PIPE_DIR)
            print("    -> [OK] Berhasil disalin.")
        except Exception as e:
            print("    -> [ERROR] Gagal menyalin: {}".format(e))
    else:
        print("[WARN] File .conf tidak ditemukan di {}, dilewati.".format(conf70_path))

    # 2. Salin file kamus .json ke direktori spesifik
    if os.path.exists(json_dict_path):
        # Gunakan variabel path yang baru
        print("[DIST] Menyalin {} ke {}".format(json_dict_path, LOGSTASH_JSON_DICT_DIR))
        try:
            # Pastikan direktori tujuan ada
            if not os.path.isdir(LOGSTASH_JSON_DICT_DIR):
                print("    -> [INFO] Direktori {} tidak ditemukan, membuat direktori...".format(LOGSTASH_JSON_DICT_DIR))
                os.makedirs(LOGSTASH_JSON_DICT_DIR)
            
            shutil.copy(json_dict_path, LOGSTASH_JSON_DICT_DIR)
            print("    -> [OK] Berhasil disalin.")
        except Exception as e:
            print("    -> [ERROR] Gagal menyalin file JSON: {}".format(e))
    else:
        print("[WARN] File kamus .json tidak ditemukan di {}, dilewati.".format(json_dict_path))
        
    # 3. Salin file directive ke pod frontend
    if os.path.exists(directive_path):
        target_path = "{}:/dsiem/configs/".format(FRONTEND_POD)
        print("[DIST] Menyalin {} ke pod {} ({})".format(directive_path, FRONTEND_POD, target_path))
        if not run_cmd(["kubectl", "cp", directive_path, target_path]):
            print("    -> [ERROR] Gagal menjalankan kubectl cp.")
    else:
        print("[WARN] File directive tidak ditemukan di {}, dilewati.".format(directive_path))
        
def restart_logstash_stack():
    if ask_yes_no("\nRestart Logstash & Dsiem pods sekarang? (y/n): ") == 'y':
        print("\n--- Memulai Proses Restart ---")
        run_cmd(["./update-config-map.sh"], cwd=LOGSTASH_HOME, shell=True)
        run_cmd(["./restart-logstash.sh"], cwd=LOGSTASH_HOME, shell=True)
        run_cmd(["kubectl", "delete", "pod", BACKEND_POD, FRONTEND_POD])
        print("\n[INFO] Perintah restart telah dikirim.")
    else:
        print("[INFO] Proses restart dibatalkan.")

def register_pulled_job(downloaded_updater_path):
    """
    Menyalin file updater yang sudah diunduh ke folder 'updaters' 
    dan mendaftarkannya ke master_jobs.json.
    """
    print("\n--- Mendaftarkan Pekerjaan Auto-Update yang Diunduh ---")

    if not os.path.exists(downloaded_updater_path):
        print("[WARN] File updater '{}' tidak ditemukan setelah diunduh. Pendaftaran dilewati.".format(downloaded_updater_path))
        return

    updaters_dir = "updaters"
    if not os.path.exists(updaters_dir):
        os.makedirs(updaters_dir)
        
    # Salin file dari folder 'pulled_configs' ke folder 'updaters' yang lebih rapi
    final_config_path = os.path.join(updaters_dir, os.path.basename(downloaded_updater_path))
    try:
        shutil.copy(downloaded_updater_path, final_config_path)
        print("[REG] Menyalin config updater ke: {}".format(final_config_path))
    except Exception as e:
        print("[ERROR] Gagal menyalin config updater: {}".format(e))
        return

    # Daftarkan path final ke master_jobs.json
    jobs_file = 'master_jobs.json'
    jobs = []
    if os.path.exists(jobs_file):
        with open(jobs_file, 'r') as f:
            try:
                jobs = json.load(f)
            except json.JSONDecodeError:
                print("[WARN] Gagal membaca '{}', akan membuat file baru.".format(jobs_file))
    
    if final_config_path not in jobs:
        print("[REG] Menambahkan '{}' ke daftar pekerjaan master.".format(final_config_path))
        jobs.append(final_config_path)
        with open(jobs_file, 'w') as f:
            json.dump(jobs, f, indent=2)
        print("    -> [OK] Berhasil didaftarkan.")
    else:
        print("[REG] Pekerjaan untuk '{}' sudah terdaftar.".format(final_config_path))

def distribute_vector(vector_conf_path, tsv_path, master_folder):
    print("\n--- Memulai Distribusi untuk Vector ---")
    # 1. Salin file .yaml ke folder Vector
    if os.path.exists(vector_conf_path):
        vector_target_dir = os.path.join(VECTOR_CONFIG_BASE_DIR, master_folder)
        print("[DIST] Menyalin {} ke {}".format(vector_conf_path, vector_target_dir))
        if not os.path.isdir(vector_target_dir):
            print("    -> [WARN] Direktori tujuan {} tidak ada. Membuat direktori...".format(vector_target_dir))
            try:
                os.makedirs(vector_target_dir)
            except Exception as e:
                print("    -> [ERROR] Gagal membuat direktori: {}".format(e))
        try:
            shutil.copy(vector_conf_path, vector_target_dir)
            print("    -> [OK] Berhasil disalin.")
        except Exception as e:
            print("    -> [ERROR] Gagal menyalin file YAML: {}".format(e))
    else:
        print("[WARN] File Vector .yaml tidak ditemukan di {}, dilewati.".format(vector_conf_path))
        
    # 2. Cari dan salin file .tsv ke NFS
    if os.path.exists(tsv_path):
        print("[DIST] Mencari direktori dsiem-plugin-tsv di {}...".format(NFS_BASE_DIR))
        nfs_target_dir = None
        try:
            for item in os.listdir(NFS_BASE_DIR):
                item_path = os.path.join(NFS_BASE_DIR, item)
                if os.path.isdir(item_path) and item.startswith("pvc-"):
                    potential_target = os.path.join(item_path, "dsiem-plugin-tsv")
                    if os.path.isdir(potential_target):
                        nfs_target_dir = potential_target
                        break
        except Exception as e:
            print("    -> [ERROR] Gagal mencari direktori NFS: {}".format(e))

        if nfs_target_dir:
            print("[DIST] Direktori ditemukan: {}. Menyalin file TSV...".format(nfs_target_dir))
            try:
                shutil.copy(tsv_path, nfs_target_dir)
                print("    -> [OK] Berhasil disalin.")
            except Exception as e:
                print("    -> [ERROR] Gagal menyalin file TSV: {}".format(e))
        else:
            print("    -> [ERROR] Direktori 'dsiem-plugin-tsv' tidak ditemukan di dalam folder pvc-* manapun.")
    else:
        print("[WARN] File TSV tidak ditemukan di {}, dilewati.".format(tsv_path))
        
def restart_vector_stack():
    if ask_yes_no("\nRestart Vector & Dsiem pods sekarang? (y/n): ") == 'y':
        print("\n--- Memulai Proses Restart ---")
        run_cmd(["kubectl", "delete", "pod", "-l", VECTOR_POD_LABEL])
        run_cmd(["kubectl", "delete", "pod", BACKEND_POD, FRONTEND_POD])
        print("\n[INFO] Perintah restart telah dikirim.")
    else:
        print("[INFO] Proses restart dibatalkan.")

def activate_plugin(full_slug):
    """Menambahkan full_slug ke daftar plugin aktif di active_plugins.json."""
    activation_file = 'active_plugins.json'
    active_list = []

    # Baca daftar yang sudah ada
    if os.path.exists(activation_file):
        with open(activation_file, 'r') as f:
            try:
                active_list = json.load(f)
            except json.JSONDecodeError:
                print("[WARN] Gagal membaca '{}', akan membuat file baru.".format(activation_file))

    # Tambahkan slug baru jika belum ada
    if full_slug not in active_list:
        print("[ACTIVATE] Mengaktifkan mode 'full' untuk plugin: {}".format(full_slug))
        active_list.append(full_slug)

        # Tulis kembali ke file
        with open(activation_file, 'w') as f:
            json.dump(sorted(active_list), f, indent=2)
    else:
        print("[ACTIVATE] Plugin '{}' sudah dalam mode 'full'.".format(full_slug))

# ====== FUNGSI MAIN ======
def main():
    # --- Blok BARU untuk manajemen nama customer ---
    customer_file = "customer.json"
    current_name = ""
    needs_update = False

    # 1. Cek file customer.json
    if os.path.exists(customer_file):
        try:
            with open(customer_file, 'r') as f:
                data = json.load(f)
                current_name = data.get("customer_info", {}).get("customer_name", "").strip()
        except (IOError, json.JSONDecodeError):
            print("[WARN] Gagal membaca file customer.json. Anda akan diminta untuk mengaturnya.")
            needs_update = True
    else:
        # File tidak ada, ini adalah setup pertama kali.
        needs_update = True

    # 2. Periksa apakah nama customer adalah placeholder default atau kosong.
    if not current_name or current_name == "Nama Customer Anda":
        needs_update = True
    
    # 3. Minta input HANYA jika diperlukan.
    if needs_update:
        print("\n" + "="*50)
        print("=== SETUP NAMA CUSTOMER ===")
        print("Nama customer untuk instalasi ini belum diatur dengan benar.")
        
        new_name = ""
        while not new_name or new_name == "Nama Customer Anda":
            new_name = py_input("Harap masukkan nama customer yang valid: ").strip()
            if not new_name:
                print("[ERROR] Nama customer tidak boleh kosong.")
            elif new_name == "Nama Customer Anda":
                 print("[ERROR] Harap ganti nama customer default.")
        
        save_json_utf8(customer_file, {"customer_info": {"customer_name": new_name}})
        print("[OK] Nama customer diatur menjadi: '{}'".format(new_name))
        print("="*50)
    else:
        # Jika nama sudah ada dan valid, cukup tampilkan pesan konfirmasi.
        print("\n[INFO] Dijalankan untuk customer: '{}'".format(current_name))
    # --- Akhir blok nama customer ---

    print("\n======================================================")
    print("=== Skrip Interaktif Mengunduh & Distribusi Config ===")
    print("======================================================")
    
    require_github()
    print("\n[*] Mencari plugin spesifik yang tersedia di repositori...")
    
    all_plugins = find_plugins_recursively()
    
    if not all_plugins:
        print("\n[ERROR] Tidak ada plugin spesifik yang ditemukan di repositori.")
        return
        
    all_plugins.sort()
    # Pilihan eksplisit dari pengguna
    selected_paths = display_and_select_plugins(all_plugins)

    if not selected_paths:
        print("[INFO] Tidak ada plugin yang dipilih. Proses dihentikan.")
        return

    # --- Blok Logika Ekspansi Berdasarkan Perangkat Master ---
    print("\n[*] Menganalisis perangkat master dari pilihan Anda...")
    master_devices = set(path.split('/')[0] for path in selected_paths)
    
    print("[INFO] Perangkat master yang teridentifikasi: {}".format(", ".join(master_devices)))
    
    # Cari semua plugin lain yang cocok dengan perangkat master
    plugins_to_process = sorted(list(set(
        plugin for plugin in all_plugins if plugin.split('/')[0] in master_devices
    )))
    
    print("\n------------------------------------------------------")
    print("Anda memilih {} plugin secara eksplisit.".format(len(selected_paths)))
    print("Total {} plugin terkait akan diproses (sinkronisasi repositori).".format(len(plugins_to_process)))
    print("------------------------------------------------------")
    
    # --- Akhir Blok Ekspansi ---
    
    print("\n--- Pilihan Distribusi ---")
    print("1. Distribusi ke Logstash")
    print("2. Distribusi ke Vector")
    dist_choice = ""
    while dist_choice not in ["1", "2"]:
        dist_choice = py_input("Pilih target platform untuk SEMUA plugin di atas [1/2]: ").strip()
        
    success_plugins = 0
    # Loop sekarang menggunakan daftar plugin yang sudah diperluas
    for path in plugins_to_process:
        # Tandai apakah plugin ini dipilih secara eksplisit oleh pengguna atau tidak
        is_explicit_choice = path in selected_paths
        
        if process_single_plugin(path, dist_choice, is_explicit_choice):
            success_plugins += 1
            
    print("\n======================================================")
    print("PROSES SELESAI: {} dari {} plugin berhasil diproses.".format(success_plugins, len(plugins_to_process)))
    print("======================================================")

    if success_plugins > 0:
        if dist_choice == '1':
            restart_logstash_stack()
        elif dist_choice == '2':
            restart_vector_stack()

    print("\n--- Selesai ---")

if __name__ == "__main__":
    main()