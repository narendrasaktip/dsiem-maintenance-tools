# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import json
import subprocess
import sys
import io # <-- Import io
from datetime import datetime

JOBS_FILE = 'master_jobs.json'
UPDATER_SCRIPT = 'auto-updated.py'

# Konfigurasi Restart (sesuaikan jika perlu)
LOGSTASH_HOME     = os.getenv("LOGSTASH_HOME")
BACKEND_POD = "dsiem-backend-0"
FRONTEND_POD = "dsiem-frontend-0"

# --- Penyesuaian Kompatibilitas Py2/Py3 ---
try:
    JSONDecodeError = json.JSONDecodeError
except AttributeError:
    JSONDecodeError = ValueError
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError
# ---

def log(message):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[{}] {}".format(ts, message))

# --- Fungsi safe_run_cmd (didefinisikan ulang di sini) ---
def safe_run_cmd(cmd, cwd=None, shell=False):
    cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
    log("[CMD] Menjalankan: {}".format(cmd_str))
    # Di sini tidak perlu DRY RUN karena hanya dipanggil jika needs_restart=True
    try:
        p = subprocess.Popen(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
        out, err = p.communicate()
        # Log output dan error untuk debugging restart
        if out: log("--- stdout ---\n{}".format(out.decode('utf-8', 'replace')))
        if err: log("--- stderr ---\n{}".format(err.decode('utf-8', 'replace')))
        if p.returncode == 0:
            log("[CMD] Sukses.")
        else:
            log("[CMD ERROR] Gagal dengan kode {}.".format(p.returncode))
        return p.returncode == 0
    except OSError as e:
        log("[CMD FATAL] Gagal menjalankan perintah: {}".format(e))
        return False
# --- Akhir safe_run_cmd ---

def main():
    """Fungsi utama untuk menjalankan semua pekerjaan auto-update."""
    log("=== Memulai Master Koordinator Auto-Update ===")

    if not os.path.exists(UPDATER_SCRIPT):
        log("[ERROR] Skrip worker '{}' tidak ditemukan...".format(UPDATER_SCRIPT))
        return 1
    if not os.path.exists(JOBS_FILE):
        log("[ERROR] File daftar pekerjaan '{}' tidak ditemukan...".format(JOBS_FILE))
        return 1

    log("Membaca daftar pekerjaan dari '{}'...".format(JOBS_FILE))
    try:
        # Gunakan io.open untuk baca
        with io.open(JOBS_FILE, 'r', encoding='utf-8') as f:
            jobs = json.load(f)
        if not isinstance(jobs, list):
            raise ValueError("Format file JSON harus berupa array/list.")
    except (JSONDecodeError, ValueError, IOError) as e: # Gunakan var kompatibel
        log("[ERROR] Gagal memproses '{}': {}".format(JOBS_FILE, e))
        return 1

    log("Ditemukan {} pekerjaan untuk dieksekusi.".format(len(jobs)))

    success_count = 0
    fail_count = 0
    needs_restart = False # Flag restart

    for i, config_path in enumerate(jobs, 1):
        log("\n--- Menjalankan Pekerjaan {}/{} (Config: {}) ---".format(i, len(jobs), config_path))

        if not os.path.exists(config_path):
            log("[WARN] File konfigurasi '{}' tidak ditemukan. Dilewati.".format(config_path))
            fail_count += 1
            continue

        try:
            command = [sys.executable, UPDATER_SCRIPT]
            # Jalankan worker dan tunggu selesai
            process = subprocess.Popen(command, env=dict(os.environ, SYNC_CFG=config_path))
            process.wait() # Tunggu worker selesai

            # Cek return code dari worker
            if process.returncode == 0:
                log("--- Pekerjaan '{}' sukses (tanpa perlu restart). ---\n".format(config_path))
                success_count += 1
            elif process.returncode == 5: # Sinyal restart diterima
                log("--- Pekerjaan '{}' sukses (membutuhkan restart stack). ---\n".format(config_path))
                success_count += 1
                needs_restart = True # Set flag untuk restart nanti
            else:
                log("[ERROR] Pekerjaan '{}' gagal (return code {}). ---\n".format(config_path, process.returncode))
                fail_count += 1

        except Exception as e:
            log("[FATAL] Error saat menjalankan subprocess untuk '{}': {}".format(config_path, e))
            import traceback # Import traceback untuk detail error
            traceback.print_exc() # Cetak traceback
            fail_count += 1

    log("=== Master Koordinator Selesai ===")
    log("Ringkasan: {} sukses, {} gagal.".format(success_count, fail_count))

    # Jalankan restart HANYA JIKA flag needs_restart aktif
    if needs_restart:
        log("\n=== Memulai Restart Stack (Karena ada update lokal) ===")
        log("[INFO] Menjalankan update dan restart untuk Logstash...")

        # Jalankan perintah restart
        if os.path.isdir(LOGSTASH_HOME): # Cek jika direktori Logstash ada
             safe_run_cmd(["./update-config-map.sh"], cwd=LOGSTASH_HOME, shell=True)
             safe_run_cmd(["./restart-logstash.sh"], cwd=LOGSTASH_HOME, shell=True)
        else:
             log("[WARN] Direktori LOGSTASH_HOME '{}' tidak ditemukan. Melewati restart Logstash.".format(LOGSTASH_HOME))

        log("[INFO] Merestart pod Backend dan Frontend...")
        safe_run_cmd(["kubectl", "delete", "pod", BACKEND_POD, FRONTEND_POD])
        log("=== Restart Stack Selesai ===")
    else:
        log("Tidak ada update lokal yang terdeteksi, restart stack dilewati.")

    # Return code 1 jika ada yg gagal, 0 jika semua sukses (termasuk yg butuh restart)
    return 1 if fail_count > 0 else 0

if __name__ == "__main__":
    sys.exit(main())