# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import json
import glob

# --- Konfigurasi ---
UPDATER_DIR = 'updaters'
ACTIVE_PLUGINS_FILE = 'active_plugins.json'

# --- Fungsi Bantuan ---
def print_header(title):
    """Mencetak header yang rapi untuk menu."""
    print("\n" + "="*70)
    print("=== {}".format(title.upper()))
    print("="*70)

def py_input(prompt):
    """Fungsi input yang kompatibel dengan Python 2 dan 3."""
    try:
        return raw_input(prompt)
    except NameError:
        return input(prompt)

def save_json(path, data):
    """Menyimpan data ke file JSON dengan format yang rapi."""
    try:
        with open(path, 'w') as f:
            json.dump(data, f, indent=2, sort_keys=True)
            f.write("\n")
        print("\n[OK] File '{}' berhasil disimpan.".format(path))
        return True
    except IOError as e:
        print("\n[ERROR] Gagal menyimpan file '{}': {}".format(path, e))
        return False

# --- Logika Inti ---
def get_all_plugins():
    """Memindai direktori 'updaters' untuk menemukan semua slug plugin yang tersedia."""
    if not os.path.isdir(UPDATER_DIR):
        print("[ERROR] Direktori '{}' tidak ditemukan. Pastikan Anda sudah menjalankan skrip pull terlebih dahulu.".format(UPDATER_DIR))
        return []
    
    updater_files = glob.glob(os.path.join(UPDATER_DIR, '*_updater.json'))
    plugins = [os.path.basename(f).replace('_updater.json', '') for f in updater_files]
    return sorted(plugins)

def get_active_plugins():
    """Membaca file active_plugins.json dan mengembalikan sebuah set berisi slug yang aktif."""
    if not os.path.exists(ACTIVE_PLUGINS_FILE):
        return set()
    try:
        with open(ACTIVE_PLUGINS_FILE, 'r') as f:
            data = json.load(f)
            if not isinstance(data, list):
                print("[WARN] Format '{}' tidak valid (harus berupa list). File akan diabaikan.".format(ACTIVE_PLUGINS_FILE))
                return set()
            return set(data)
    except (IOError, ValueError):
        print("[WARN] Gagal membaca atau mem-parsing '{}'. File akan diabaikan.".format(ACTIVE_PLUGINS_FILE))
        return set()

def main_loop():
    """Menjalankan loop menu interaktif utama."""
    all_plugins = get_all_plugins()
    if not all_plugins:
        return
        
    active_plugins = get_active_plugins()
    
    while True:
        print_header("Manajemen Status Notifikasi Plugin (Aktif/Pasif)")
        
        active_list = sorted([p for p in all_plugins if p in active_plugins])
        passive_list = sorted([p for p in all_plugins if p not in active_plugins])
        
        display_plugins = active_list + passive_list
        
        for i, plugin_slug in enumerate(display_plugins, 1):
            # [PERBAIKAN] Mengubah format tampilan status
            status = "+++ [ Aktif ] +++" if plugin_slug in active_plugins else "--- [ Pasif ] ---"
            # Menyesuaikan padding agar tetap rapi
            print("{:3d}. {:<19} {}".format(i, status, plugin_slug))
            
        print("-" * 70)
        print("Pilihan:")
        print("  - Masukkan nomor untuk mengubah status (cth: 1, 3, 5-8)")
        print("  - 's' untuk Simpan dan Keluar")
        print("  - 'q' untuk Keluar tanpa menyimpan")
        
        choice = py_input("\nPilihan Anda: ").strip().lower()
        
        if choice == 'q':
            print("Keluar tanpa menyimpan perubahan.")
            break
            
        elif choice == 's':
            save_json(ACTIVE_PLUGINS_FILE, sorted(list(active_plugins)))
            break
            
        else:
            indices_to_toggle = set()
            is_valid_input = True
            
            parts = choice.split(',')
            for part in parts:
                part = part.strip()
                if not part: continue

                if '-' in part:
                    try:
                        start, end = map(int, part.split('-'))
                        if start > end or not (1 <= start <= len(display_plugins)) or not (1 <= end <= len(display_plugins)):
                            print("\n[ERROR] Rentang '{}' tidak valid.".format(part))
                            is_valid_input = False; break
                        indices_to_toggle.update(range(start - 1, end))
                    except ValueError:
                        print("\n[ERROR] Format rentang '{}' tidak valid.".format(part))
                        is_valid_input = False; break
                else:
                    try:
                        index = int(part) - 1
                        if not (0 <= index < len(display_plugins)):
                            print("\n[ERROR] Nomor '{}' di luar jangkauan.".format(part))
                            is_valid_input = False; break
                        indices_to_toggle.add(index)
                    except ValueError:
                        print("\n[ERROR] Input '{}' bukan angka yang valid.".format(part))
                        is_valid_input = False; break
            
            if not is_valid_input:
                continue

            if not indices_to_toggle:
                print("\n[ERROR] Perintah tidak dikenali.")
                continue

            toggled_summary = []
            for index in sorted(list(indices_to_toggle)):
                selected_slug = display_plugins[index]
                if selected_slug in active_plugins:
                    active_plugins.remove(selected_slug)
                    toggled_summary.append("-> Status '{}' diubah menjadi --- [ Pasif ] ---.".format(selected_slug))
                else:
                    active_plugins.add(selected_slug)
                    toggled_summary.append("-> Status '{}' diubah menjadi +++ [ Aktif ] +++.".format(selected_slug))
            
            print("\n" + "\n".join(toggled_summary))

if __name__ == "__main__":
    main_loop()