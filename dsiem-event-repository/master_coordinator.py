import os
import json
import subprocess
import sys
from datetime import datetime

JOBS_FILE = 'master_jobs.json'
UPDATER_SCRIPT = 'auto-updated.py'

def log(message):
    """Fungsi logging sederhana."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print("[{}] {}".format(ts, message))

def main():
    """Fungsi utama untuk menjalankan semua pekerjaan auto-update."""
    log("=== Memulai Master Koordinator Auto-Update ===")

    # 1. Pastikan skrip worker ada
    if not os.path.exists(UPDATER_SCRIPT):
        log("[ERROR] Skrip worker '{}' tidak ditemukan. Proses dihentikan.".format(UPDATER_SCRIPT))
        return 1

    # 2. Baca file daftar pekerjaan
    if not os.path.exists(JOBS_FILE):
        log("[ERROR] File daftar pekerjaan '{}' tidak ditemukan. Tidak ada pekerjaan untuk dijalankan.".format(JOBS_FILE))
        return 1
        
    log("Membaca daftar pekerjaan dari '{}'...".format(JOBS_FILE))
    try:
        with open(JOBS_FILE, 'r') as f:
            jobs = json.load(f)
        if not isinstance(jobs, list):
            raise ValueError("Format file JSON harus berupa array/list.")
    except (json.JSONDecodeError, ValueError) as e:
        log("[ERROR] Gagal memproses '{}': {}".format(JOBS_FILE, e))
        return 1

    log("Ditemukan {} pekerjaan untuk dieksekusi.".format(len(jobs)))

    # 3. Lakukan loop dan eksekusi setiap pekerjaan
    success_count = 0
    fail_count = 0
    for i, config_path in enumerate(jobs, 1):
        log("\n--- Menjalankan Pekerjaan {}/{} (Config: {}) ---".format(i, len(jobs), config_path))
        
        if not os.path.exists(config_path):
            log("[WARN] File konfigurasi '{}' tidak ditemukan. Pekerjaan dilewati.".format(config_path))
            fail_count += 1
            continue

        try:
            # Gunakan sys.executable untuk memastikan versi Python yang sama digunakan
            command = [sys.executable, UPDATER_SCRIPT]
            
            # Panggil subprocess dan tunggu hingga selesai
            # Ini akan menampilkan output dari auto-updated.py secara real-time
            process = subprocess.Popen(command, env=dict(os.environ, SYNC_CFG=config_path))
            process.wait()

            if process.returncode == 0:
                log("--- Pekerjaan untuk '{}' selesai dengan sukses. ---\n".format(config_path))
                success_count += 1
            else:
                log("[ERROR] Pekerjaan untuk '{}' gagal dengan return code {}. ---\n".format(config_path, process.returncode))
                fail_count += 1

        except Exception as e:
            log("[FATAL] Terjadi error saat menjalankan subprocess untuk '{}': {}".format(config_path, e))
            fail_count += 1

    log("=== Master Koordinator Selesai ===")
    log("Ringkasan: {} sukses, {} gagal.".format(success_count, fail_count))
    
    return 1 if fail_count > 0 else 0

if __name__ == "__main__":
    sys.exit(main())