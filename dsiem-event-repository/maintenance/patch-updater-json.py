# -*- coding: utf-8 -*-
from __future__ import print_function
import json
import os
import io
import re
import sys
import base64
import requests
from collections import OrderedDict

# --- Penyesuaian Error Handling Py2/Py3 ---
try:
    JSONDecodeError = json.JSONDecodeError
except AttributeError:
    JSONDecodeError = ValueError
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError

# === KONFIGURASI ===
# File yang berisi daftar semua file updater.json
JOBS_FILE = 'master_jobs.json'

# Blok time_range yang ingin kamu tambahkan
NEW_TIME_RANGE = OrderedDict([
    ("field", "@timestamp"),
    ("gte", "now-1h"),
    ("lte", "now")
])
# ===================

# --- Fungsi Bantuan Slug (diambil dari main.py) ---
def slug(s):
    if s is None: return ""
    s = s.strip().lower()
    s = re.sub(r'[^a-z0-9]+','-', s)
    s = re.sub(r'-+','-', s).strip('-')
    return s or ""

def get_gh_path_for_updater(layout_obj, updater_filename):
    """
    Merekonstruksi path GitHub berdasarkan info layout.
    Contoh: 'akamai/waf/akamai-waf_updater.json'
    """
    device = slug(layout_obj.get("device"))
    module = slug(layout_obj.get("module"))
    submodule = slug(layout_obj.get("submodule"))
    filter_key = slug(layout_obj.get("filter_key"))

    # Logika dari main.py untuk membuat base_dir
    parts = [device]
    if module and module != filter_key:
        parts.append(module)
    if submodule and submodule != filter_key:
        parts.append(submodule)
    if filter_key and filter_key not in parts:
        parts.append(filter_key)
    
    # Hilangkan duplikat tapi jaga urutan (hack Py2/3)
    unique_parts = []
    seen = set()
    for p in parts:
        if p and p not in seen:
            unique_parts.append(p)
            seen.add(p)

    base_dir = "/".join(unique_parts)
    return "{}/{}".format(base_dir, updater_filename)

# --- Fungsi Bantuan GitHub ---
def gh_headers(token):
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": "Bearer {}".format(token),
        "X-GitHub-Api-Version": "2022-11-28"
    }

def gh_get_file(repo, branch, token, path):
    """Mengambil file dan SHA-nya dari GitHub."""
    url = "https://api.github.com/repos/{}/contents/{}".format(repo, path)
    try:
        r = requests.get(url, headers=gh_headers(token), params={"ref": branch}, timeout=60)
        if r.status_code == 404:
            print("    [GH] File tidak ditemukan di repo (404): {}".format(path))
            return None  # SHA-nya None, berarti file baru
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print("    [GH ERROR] Gagal GET file {}: {}".format(path, e))
        return None # Anggap error, jangan ditimpa

def gh_put_file(repo, branch, token, path, bytes_content, message, sha):
    """Push (membuat/update) file ke GitHub."""
    url = "https://api.github.com/repos/{}/contents/{}".format(repo, path)
    payload = {
        "message": message,
        "content": base64.b64encode(bytes_content).decode("ascii"),
        "branch": branch
    }
    if sha:
        payload["sha"] = sha
    
    try:
        r = requests.put(url, headers=gh_headers(token), data=json.dumps(payload), timeout=60)
        r.raise_for_status()
        print("    [GH PUSH] Berhasil: {}".format(path))
        return True
    except requests.exceptions.RequestException as e:
        print("    [GH PUSH ERROR] Gagal PUT file {}: {}".format(path, e.response.text if e.response else e))
        return False

# --- Fungsi Inti Patch ---
def patch_and_push(local_path, gh_repo, gh_branch, gh_token):
    """
    Fungsi utama per file:
    1. Baca file lokal
    2. Patch di memori
    3. Tentukan path GitHub
    4. Ambil SHA dari GitHub
    5. Tulis patch ke file lokal
    6. Push patch ke GitHub
    """
    print("\n--- Memproses: {} ---".format(local_path))
    
    # 1. Baca file lokal
    try:
        with io.open(local_path, 'r', encoding='utf-8') as f:
            data = json.load(f, object_pairs_hook=OrderedDict)
    except (FileNotFoundError, IOError):
        print("[ERROR] File lokal tidak ditemukan: {}. Dilewati.".format(local_path))
        return False
    except (JSONDecodeError, ValueError):
        print("[ERROR] Gagal membaca JSON lokal: {}. Dilewati.".format(local_path))
        return False

    changes_made = False
    
    # 2. Patch di memori
    if 'query' in data:
        if 'time_range' not in data['query']:
            data['query']['time_range'] = NEW_TIME_RANGE
            changes_made = True
            print("  [+] Menambahkan time_range")
    
    if 'github' in data:
        if 'repo' in data['github']:
            data['github'].pop('repo')
            changes_made = True
            print("  [-] Menghapus github.repo")
        if 'branch' in data['github']:
            data['github'].pop('branch')
            changes_made = True
            print("  [-] Menghapus github.branch")

    if not changes_made:
        print("[OK] Tidak ada perubahan, file sudah sesuai. Melewati push.")
        return True # Dianggap sukses karena sudah benar

    # 3. Tentukan path GitHub
    try:
        updater_filename = os.path.basename(local_path)
        remote_path = get_gh_path_for_updater(data['layout'], updater_filename)
    except KeyError as e:
        print("[ERROR] JSON tidak memiliki key 'layout'. Tidak bisa menentukan path GitHub. Dilewati. ({})".format(e))
        return False

    print("  [PATH] Path GitHub terdeteksi: {}".format(remote_path))

    # 4. Ambil SHA dari GitHub
    existing_file_meta = gh_get_file(gh_repo, gh_branch, gh_token, remote_path)
    remote_sha = None
    if existing_file_meta:
        remote_sha = existing_file_meta.get("sha")
        print("  [GH] SHA file di repo: {}".format(remote_sha))
    else:
        print("  [GH] File belum ada di repo, akan dibuat baru.")

    # --- Konversi data baru ke bytes (Kompatibel Py2/3) ---
    try:
        json_string = json.dumps(data, indent=2, ensure_ascii=False)
        try:
            unicode # Cek Py2
            if isinstance(json_string, str):
                json_string = json_string.decode('utf-8')
        except NameError:
            pass # Ini Py3, sudah benar
        
        final_bytes = json_string.encode('utf-8')
    except Exception as e:
        print("[ERROR] Gagal meng-encode JSON yang baru: {}".format(e))
        return False

    # 5. Tulis patch ke file LOKAL (agar sinkron)
    try:
        with io.open(local_path, 'wb') as f: # Buka sebagai 'wb' (write bytes)
            f.write(final_bytes)
            # Tulis newline sebagai bytes
            f.write(b'\n' if sys.version_info[0] > 2 else '\n') 
        print("  [LOKAL] File lokal berhasil di-patch.")
    except (IOError, TypeError) as e:
        print("[ERROR] Gagal menyimpan file LOKAL {}: {}".format(local_path, e))
        return False

    # 6. Push patch ke GitHub
    commit_msg = "[patch] Perbarui _updater.json: tambah time_range, hapus repo/branch"
    return gh_put_file(gh_repo, gh_branch, gh_token, remote_path, final_bytes, commit_msg, remote_sha)


def main():
    """Fungsi utama."""
    print("=== Memulai Skrip Patch & Push Updater JSON ===")
    
    # 1. Baca Environment Variables
    gh_repo = os.getenv("GITHUB_REPO")
    gh_token = os.getenv("GITHUB_TOKEN")
    gh_branch = os.getenv("GITHUB_BRANCH")

    if not all([gh_repo, gh_token, gh_branch]):
        print("[FATAL] Variabel GITHUB_REPO, GITHUB_TOKEN, dan GITHUB_BRANCH harus di-set.")
        print("Jalankan 'source config.sh' terlebih dahulu.")
        return

    print("Target Repo: {}@{}".format(gh_repo, gh_branch))

    # 2. Baca master_jobs.json
    if not os.path.exists(JOBS_FILE):
        print("[FATAL] File {} tidak ditemukan.".format(JOBS_FILE))
        return

    try:
        with io.open(JOBS_FILE, 'r', encoding='utf-8') as f:
            jobs = json.load(f)
    except Exception as e:
        print("[FATAL] Gagal membaca {}: {}".format(JOBS_FILE, e))
        return

    if not isinstance(jobs, list):
        print("[FATAL] {} tidak berisi daftar (list) JSON yang valid.".format(JOBS_FILE))
        return

    print("Ditemukan {} file updater untuk diproses...".format(len(jobs)))
    
    success_count = 0
    fail_count = 0

    # 3. Loop dan proses setiap file
    for job_path in jobs:
        if patch_and_push(job_path, gh_repo, gh_branch, gh_token):
            success_count += 1
        else:
            fail_count += 1
    
    print("\n=== Patch & Push Selesai ===")
    print("Berhasil: {}".format(success_count))
    print("Gagal/Dilewati: {}".format(fail_count))

if __name__ == "__main__":
    main()