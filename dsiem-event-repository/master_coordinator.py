#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import json
import subprocess
import sys
import io # Pastikan io diimport
from datetime import datetime

# --- Penyesuaian Kompatibilitas Py2/Py3 ---
try:
    JSONDecodeError = json.JSONDecodeError # Coba ambil nama Py3
except AttributeError:                     # Jika gagal (berarti Py2)
    JSONDecodeError = ValueError          # Gunakan nama Py2 (ValueError) sebagai gantinya
try:
    FileNotFoundError                     # Coba ambil nama Py3
except NameError:                         # Jika gagal (berarti Py2)
    FileNotFoundError = IOError           # Gunakan nama Py2 (IOError) sebagai gantinya
# --- AKHIR BLOK KOMPATIBILITAS ---

JOBS_FILE = 'master_jobs.json'
UPDATER_SCRIPT = 'auto-updated.py'

# --- [DIPERBAIKI] Konfigurasi Restart (membaca semua var) ---
LOGSTASH_HOME     = os.getenv("LOGSTASH_HOME")
BACKEND_POD       = os.getenv("BACKEND_POD", "dsiem-backend-0")
FRONTEND_POD      = os.getenv("FRONTEND_POD", "dsiem-frontend-0")
VECTOR_POD_LABEL  = os.getenv("VECTOR_POD_LABEL", "app=vector-parser") # Ditambahkan
# --- AKHIR PERBAIKAN ---

def log(message):
    """Mencetak log dengan timestamp."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[{}] {}".format(ts, message))

def safe_run_cmd(cmd, cwd=None, shell=False):
    """Menjalankan perintah shell dengan aman dan mencatat outputnya."""
    cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd
    log("[CMD] Menjalankan: {}".format(cmd_str))
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

# --- [BARU] Fungsi Helper Restart ---
def restart_logstash_stack():
    """Melakukan restart stack Logstash (termasuk pod)."""
    log("[RESTART] Menjalankan update dan restart untuk Logstash...")
    if LOGSTASH_HOME and os.path.isdir(LOGSTASH_HOME): # Cek jika direktori Logstash ada
         safe_run_cmd(["./update-config-map.sh"], cwd=LOGSTASH_HOME, shell=True)
         safe_run_cmd(["./restart-logstash.sh"], cwd=LOGSTASH_HOME, shell=True)
    elif LOGSTASH_HOME:
         log("[WARN] Direktori LOGSTASH_HOME '{}' tidak ditemukan. Melewati restart Logstash.".format(LOGSTASH_HOME))
    else:
         log("[WARN] LOGSTASH_HOME env var tidak diset. Melewati restart Logstash.")

    log("[RESTART] Merestart pod Backend dan Frontend (untuk Logstash)...")
    safe_run_cmd(["kubectl", "delete", "pod", BACKEND_POD, FRONTEND_POD])

def restart_vector_stack():
    """Melakukan restart stack Vector (termasuk pod)."""
    log("[RESTART] Merestart pod Vector...")
    if not VECTOR_POD_LABEL:
        log("[WARN] VECTOR_POD_LABEL tidak diset. Tidak dapat merestart pod Vector.")
    else:
        safe_run_cmd(["kubectl", "delete", "pod", "-l", VECTOR_POD_LABEL])
    
    log("[RESTART] Merestart pod Backend dan Frontend (untuk Vector)...")
    safe_run_cmd(["kubectl", "delete", "pod", BACKEND_POD, FRONTEND_POD])
# --- AKHIR FUNGSI HELPER RESTART ---

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
    # --- [DIPERBAIKI] Menggunakan set untuk melacak target restart ---
    restart_targets = set() 
    # --- AKHIR PERBAIKAN ---

    for i, config_path in enumerate(jobs, 1):
        log("\n--- Menjalankan Pekerjaan {}/{} (Config: {}) ---".format(i, len(jobs), config_path))

        if not os.path.exists(config_path):
            log("[WARN] File konfigurasi '{}' tidak ditemukan. Dilewati.".format(config_path))
            fail_count += 1
            continue

        # --- [DIPERBAIKI] Baca config SEBELUM menjalankan worker ---
        job_target = "None"
        try:
            with io.open(config_path, 'r', encoding='utf-8') as f_cfg:
                job_cfg = json.load(f_cfg)
                # Ambil target, default ke 'Logstash' jika tidak ada (untuk kompatibilitas lama)
                job_target = job_cfg.get("layout", {}).get("distribution_target", "Logstash") 
        except Exception as e:
            log("[WARN] Gagal membaca config '{}': {}. Mengasumsikan target 'Logstash'.".format(config_path, e))
            job_target = "Logstash" # Default jika file config rusak
        # --- AKHIR PERBAIKAN ---

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
                # --- [DIPERBAIKI] Catat target yg perlu di-restart ---
                if job_target in ["Logstash", "Vector"]:
                    log("[INFO] Menandai '{}' untuk di-restart.".format(job_target))
                    restart_targets.add(job_target)
                else:
                    log("[WARN] Menerima sinyal restart, tapi target '{}' tidak dikenali.".format(job_target))
                # --- AKHIR PERBAIKAN ---
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

    # --- [DIPERBAIKI] Jalankan restart berdasarkan target ---
    if restart_targets:
        log("\n=== Memulai Restart Stack (Karena ada update lokal) ===")
        
        # Logstash diutamakan karena me-restart pod backend/frontend
        if "Logstash" in restart_targets:
            restart_logstash_stack()
            
        if "Vector" in restart_targets:
            # Jika Logstash sudah restart pods, kita tidak perlu restart lagi
            if "Logstash" not in restart_targets:
                restart_vector_stack()
            else:
                log("[INFO] Restart pod Vector dilewati karena Logstash sudah merestart pod (termasuk backend/frontend).")

        log("=== Restart Stack Selesai ===")
    else:
        log("Tidak ada update lokal yang terdeteksi, restart stack dilewati.")
    # --- AKHIR PERBAIKAN ---

    # Return code 1 jika ada yg gagal, 0 jika semua sukses (termasuk yg butuh restart)
    return 1 if fail_count > 0 else 0

if __name__ == "__main__":
    sys.exit(main())