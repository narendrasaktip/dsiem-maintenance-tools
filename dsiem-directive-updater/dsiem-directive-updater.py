#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
import os
import sys
import subprocess
import json
import re
import collections
import time

try:
    import configparser
except ImportError:
    import ConfigParser as configparser

CONFIG_FILE = 'config.ini'

# ======================= CONFIGURATION =======================
def load_config():
    config = configparser.ConfigParser()
    defaults = {
        'Kubernetes': {'PodName': 'dsiem-frontend-0', 'RemotePath': '/dsiem/configs/', 'Namespace': ''},
        'Paths': {'LocalDir': 'dsiem_configs_edited', 'FilePattern': 'directives_*.json'},
        'Display': {'ItemsPerPage': '20'}
    }
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, CONFIG_FILE)

    if not os.path.exists(config_path):
        if hasattr(config, 'read_dict'):
            config.read_dict(defaults)
        else:
            for section, options in defaults.items():
                try: config.add_section(section)
                except: pass
                for key, value in options.items(): config.set(section, key, value)
        try:
            with open(config_path, 'w') as cf: config.write(cf)
        except: pass
    
    config.read(config_path)
    
    def get_conf(section, key):
        try:
            val = config.get(section, key)
            if section == 'Display' and key == 'ItemsPerPage':
                try: return int(val)
                except: return 20
            return val
        except:
            return defaults.get(section, {}).get(key, '')
    
    local_dir_name = get_conf('Paths', 'LocalDir')
    absolute_local_dir = os.path.join(script_dir, local_dir_name)
    
    return {
        'pod_name': get_conf('Kubernetes', 'PodName'),
        'remote_path': get_conf('Kubernetes', 'RemotePath'),
        'namespace': get_conf('Kubernetes', 'Namespace'),
        'local_dir': absolute_local_dir,
        'file_pattern': get_conf('Paths', 'FilePattern'),
        'items_per_page': get_conf('Display', 'ItemsPerPage')
    }

CONFIG = load_config()
POD_NAME = CONFIG['pod_name']
REMOTE_PATH = CONFIG['remote_path']
NAMESPACE = CONFIG['namespace']
LOCAL_DIR = CONFIG['local_dir']
FILE_PATTERN = CONFIG['file_pattern']
ITEMS_PER_PAGE = CONFIG['items_per_page']

try: input = raw_input
except NameError: pass

# ======================= UI SYSTEM =======================
class TColors:
    BOLD='\033[1m'; GREEN='\033[92m'; YELLOW='\033[93m'; RED='\033[91m'
    CYAN='\033[96m'; BLUE='\033[94m'; MAGENTA='\033[95m'; WHITE='\033[97m'
    RESET='\033[0m'; DIM='\033[2m'; UNDERLINE='\033[4m'; BG_BLUE='\033[44m'
    BG_GREEN='\033[42m'; BG_RED='\033[41m'; BG_YELLOW='\033[43m'
    if not os.popen('tput sgr0 2>/dev/null').read():
        BOLD=GREEN=YELLOW=RED=CYAN=BLUE=MAGENTA=WHITE=RESET=DIM=UNDERLINE=""
        BG_BLUE=BG_GREEN=BG_RED=BG_YELLOW=""

def clear_screen():
    os.system('clear' if os.name != 'nt' else 'cls')
    
def print_separator(char="═", width=80, color=TColors.CYAN):
    print("{}{}{}".format(color, char * width, TColors.RESET))

def print_header(text, icon="", subtitle=""):
    clear_screen()
    print("")
    print_separator("═", 80)
    centered = "{} {} {}".format(icon, text, icon).center(80)
    print("{}{}{}".format(TColors.BOLD + TColors.CYAN, centered, TColors.RESET))
    if subtitle:
        sub_centered = subtitle.center(80)
        print("{}{}{}".format(TColors.DIM, sub_centered, TColors.RESET))
    print_separator("═", 80)
    print("")

def print_info_box(title, items, width=78):
    print("\n{}{}╭─[ {} ]{}".format(TColors.BLUE, TColors.BOLD, title, "─" * (width - len(title) - 5)))
    for item in items:
        if isinstance(item, tuple):
            key, value = item
            print("{}│{} {}: {}{}{}".format(
                TColors.BLUE, TColors.RESET,
                TColors.BOLD + TColors.WHITE + str(key).ljust(18) + TColors.RESET,
                TColors.CYAN, str(value), TColors.RESET
            ))
        else:
            print("{}│{} {}".format(TColors.BLUE, TColors.RESET, item))
    print("{}╰{}{}".format(TColors.BLUE, "─" * (width-2), TColors.RESET))

def print_menu_card(options):
    print("\n{}{}╭─ MENU OPTIONS ─{}".format(TColors.CYAN, TColors.BOLD, "─" * 61))
    for opt in options:
        key, desc, color, icon = opt if len(opt) == 4 else (opt + ("▸",))
        print("{}│{}  {} [{}{}{}] {}{}".format(
            TColors.CYAN, TColors.RESET, icon,
            color + TColors.BOLD, key, TColors.RESET,
            desc, TColors.RESET
        ))
    print("{}╰{}{}".format(TColors.CYAN, "─" * 77, TColors.RESET))

def print_success(text): 
    print("\n{}{}✓{} {}".format(TColors.GREEN, TColors.BOLD, TColors.RESET, text))

def print_error(text): 
    print("\n{}{}✗{} {}".format(TColors.RED, TColors.BOLD, TColors.RESET, text))

def print_warning(text): 
    print("\n{}{}⚠{} {}".format(TColors.YELLOW, TColors.BOLD, TColors.RESET, text))

def print_loading(text): 
    sys.stdout.write("\r{}⟳ {}{}".format(TColors.CYAN, text, TColors.RESET))
    sys.stdout.flush()

def print_progress_bar(current, total, prefix="Progress", bar_length=40):
    percent = float(current) / total
    filled = int(bar_length * percent)
    bar = '█' * filled + '░' * (bar_length - filled)
    sys.stdout.write('\r{}{}:{} [{}] {:.1f}% ({}/{})'.format(
        TColors.CYAN, prefix, TColors.RESET, bar, percent * 100, current, total
    ))
    sys.stdout.flush()
    if current == total:
        print()

# ======================= HELP SYSTEM =======================
def show_help_panel():
    print_header("QUICK HELP GUIDE", "❓", "Panduan Penggunaan DSIEM Manager")
    
    help_data = [
        ("📂 FILE SELECTION", [
            "• Option 1: Browse list. Script akan list semua file JSON di pod.",
            "• Option 2: Search. Cari directive berdasarkan nama di semua file.",
            "• File akan otomatis di-download ke folder lokal saat dipilih."
        ]),
        ("📋 EDITING DIRECTIVES", [
            "• Active Directives: Ditampilkan dengan border HIJAU.",
            "• Inactive Directives: Ditampilkan dengan border MERAH.",
            "• Gunakan angka (mis: 1) atau range (mis: 1-5) untuk memilih.",
            "• Menu Edit: Bisa ubah Priority, Toggle Status, atau Hapus."
        ]),
        ("📊 BATCH UPDATE", [
            "• Siapkan file .txt/.csv dengan format: Nama Event | Action",
            "• Action support: active, enable, passive, disable.",
            "• Script akan scan semua file JSON dan update statusnya otomatis."
        ]),
        ("🔧 NAVIGATION", [
            "• [N]ext / [P]rev : Pindah halaman list.",
            "• [F]ilter : Filter list berdasarkan kata kunci nama.",
            "• [S]tatus : Filter tampilan (ALL, ACTIVE Only, PASSIVE Only).",
            "• [C]lear : Hapus filter kata kunci.",
            "• [A]ll Active / [Z]Passive : Opsi batch select untuk edit semua.",
            "• [Q]uit : Keluar dari aplikasi."
        ])
    ]
    
    for title, items in help_data:
        print_info_box(title, items)
        print("")
    
    input("{}Press Enter to return to main menu...{}".format(TColors.DIM, TColors.RESET))

# ======================= SYSTEM LOGIC =======================
def run_command(cmd_list, check_stderr=False):
    try:
        full_cmd = list(cmd_list)
        if cmd_list[0] == "kubectl" and NAMESPACE:
            full_cmd.insert(1, "-n")
            full_cmd.insert(2, NAMESPACE)
        process = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        out_str = out.decode('utf-8').strip()
        err_str = err.decode('utf-8').strip()
        if process.returncode != 0:
            if not ("Defaulting container" in err_str or "pod default value" in err_str):
                return None
        return out_str
    except Exception as e:
        print_error("Command failed: {}".format(str(e)))
        return None

def check_deps():
    print_loading("Checking dependencies...")
    time.sleep(0.5)
    if run_command(["kubectl", "version", "--client"]) is None:
        print_error("kubectl not found")
        sys.exit(1)
    print_success("Dependencies OK")

def verify_pod_connection():
    print_loading("Verifying pod connection...")
    test_cmd = ["kubectl", "exec", POD_NAME, "--", "ls", REMOTE_PATH]
    if run_command(test_cmd) is None:
        print_error("Cannot connect to pod: {}".format(POD_NAME))
        return False
    print_success("Pod connection OK")
    return True

def restart_pods_logic(changed_filename=None):
    print_header("POD RESTART PROCESS", "🔄")
    print_info_box("RESTART INFO", [("Target Pod", POD_NAME)])
    
    print("\n{}Restarting Frontend Pod...{}".format(TColors.YELLOW, TColors.RESET))
    run_command(["kubectl", "delete", "pod", POD_NAME])
    
    if changed_filename:
        m = re.search(r'directives_(dsiem-backend-\d+)_', changed_filename)
        if m:
            be_pod = m.group(1)
            print("\n{}Restarting Backend Pod: {}{}".format(TColors.YELLOW, be_pod, TColors.RESET))
            run_command(["kubectl", "delete", "pod", be_pod])
    
    time.sleep(1)

# ======================= BATCH UPDATE =======================
def sync_all_files():
    print_header("FILE SYNCHRONIZATION", "⬇️")
    print_loading("Scanning remote files...")
    file_list_raw = run_command(["kubectl", "exec", POD_NAME, "--", "find", REMOTE_PATH, "-maxdepth", "1", "-name", FILE_PATTERN])
    if not file_list_raw: return False

    all_lines = file_list_raw.split('\n')
    valid_paths = [f for f in all_lines if f.startswith(REMOTE_PATH) and f.endswith('.json')]
    
    print("\n{}Found {} files, downloading...{}\n".format(TColors.GREEN, len(valid_paths), TColors.RESET))
    for i, r_path in enumerate(valid_paths, 1):
        fname = os.path.basename(r_path)
        print_progress_bar(i, len(valid_paths), "Downloading", 40)
        run_command(["kubectl", "cp", "{}:{}{}".format(POD_NAME, REMOTE_PATH, fname), os.path.join(LOCAL_DIR, fname)])
    
    print_success("Synced files")
    return True

def get_action_status(action):
    a = action.lower().strip()
    if a in ['active', 'enable', 'on', 'false', 'aktif']: return False
    if a in ['passive', 'disable', 'off', 'true', 'pasif']: return True
    return None

def process_batch_pipe():
    print_header("BATCH UPDATE MODE", "📊")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    files_found = [f for f in os.listdir(script_dir) if f.endswith('.csv') or f.endswith('.txt')]
    
    if not files_found:
        print_error("No .csv/.txt files found")
        input("Press Enter...")
        return

    print("\nAvailable files:")
    for i, f in enumerate(files_found, 1):
        print("[{}] {}".format(i, f))
    
    try:
        sel = int(input("\nSelect File: ")) - 1
        file_path = os.path.join(script_dir, files_found[sel])
    except: return

    if not sync_all_files(): return

    target_updates = {}
    with open(file_path, 'r') as f:
        for line in f:
            if '|' in line:
                parts = line.strip().split('|')
                st = get_action_status(parts[-1])
                if st is not None: target_updates[parts[0].strip().lower()] = st

    print_success("Loaded {} rules".format(len(target_updates)))
    
    modified_files = set()
    for jf in os.listdir(LOCAL_DIR):
        if not jf.endswith('.json'): continue
        fpath = os.path.join(LOCAL_DIR, jf)
        try:
            with open(fpath, 'r') as f: data = json.load(f, object_pairs_hook=collections.OrderedDict)
            is_mod = False
            dirs = data.get("directives", []) if "directives" in data else [data]
            for d in dirs:
                if d.get('name', '').lower() in target_updates:
                    ns = target_updates[d.get('name', '').lower()]
                    if d.get('disabled', False) != ns:
                        d['disabled'] = ns
                        is_mod = True
            if is_mod:
                with open(fpath, 'w') as f: json.dump(data, f, indent=4)
                modified_files.add(jf)
        except: pass

    if modified_files:
        print_success("Updated {} files".format(len(modified_files)))
        if input("Upload to pod? (y/n): ").lower() == 'y':
            for mf in modified_files:
                run_command(["kubectl", "cp", os.path.join(LOCAL_DIR, mf), "{}:{}{}".format(POD_NAME, REMOTE_PATH, mf)])
            if input("Restart pods? (y/n): ").lower() == 'y':
                restart_pods_logic(list(modified_files)[0])
    else:
        print_warning("No changes needed")
    input("Press Enter...")

# ======================= EDITING LOGIC =======================

def parse_selection(input_str, max_total_items):
    indices = set()
    input_str = input_str.strip().lower()
    parts = re.split(r'[\s,]+', input_str)
    
    for part in parts:
        if not part: continue
        match = re.match(r'^(\d+)-(\d+)$', part)
        if match:
            s, e = int(match.group(1)), int(match.group(2))
            if s > e: s, e = e, s
            for i in range(s-1, e):
                if 0 <= i < max_total_items: indices.add(i)
        elif part.isdigit():
            i = int(part) - 1
            if 0 <= i < max_total_items: indices.add(i)
    return sorted(list(indices))

def select_directives_from_file(data, show_az_options=False, initial_filter=None):
    current_page = 1
    search_term = initial_filter if initial_filter else ""
    view_mode = 0 # 0=All, 1=Active, 2=Passive
    
    while True:
        print_header("SELECT DIRECTIVES", "📋")
        
        all_directives = data.get("directives", [])
        if not all_directives:
            print_warning("No directives found in this file")
            input("\n{}Press Enter to return...{}".format(TColors.DIM, TColors.RESET))
            return ['back']

        status_text = "ALL"
        if view_mode == 1: status_text = "ACTIVE ONLY"
        elif view_mode == 2: status_text = "PASSIVE ONLY"
        
        filter_text = search_term if search_term else "None"
        print("{}🔍 Filter: {}{} {}│{} {}👁️  View: {}{}".format(
            TColors.CYAN, TColors.BOLD + TColors.WHITE, filter_text, TColors.RESET,
            TColors.DIM, TColors.RESET,
            TColors.MAGENTA + TColors.BOLD, status_text, TColors.RESET
        ))

        # Filter Name
        if search_term:
            name_filtered = [d for d in all_directives if search_term.lower() in d.get('name', '').lower()]
        else:
            name_filtered = list(all_directives)

        # Filter View Mode
        if view_mode == 0: filtered_directives = name_filtered
        elif view_mode == 1: filtered_directives = [d for d in name_filtered if not d.get('disabled')]
        elif view_mode == 2: filtered_directives = [d for d in name_filtered if d.get('disabled')]

        if not filtered_directives:
            print_warning("No directives match criteria.")
            display_list = []
            total_items, total_pages = 0, 1
        else:
            sorted_directives = sorted(filtered_directives, key=lambda d: d.get('name', '').lower())
            if view_mode == 0:
                active_dirs = [d for d in sorted_directives if not d.get('disabled')]
                passive_dirs = [d for d in sorted_directives if d.get('disabled')]
                display_list = active_dirs + passive_dirs
            else:
                display_list = sorted_directives
            
            total_items = len(display_list)
            total_pages = (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
            if total_pages == 0: total_pages = 1
            current_page = max(1, min(current_page, total_pages))
            
            start = (current_page - 1) * ITEMS_PER_PAGE
            page_items = display_list[start:start + ITEMS_PER_PAGE]

            # --- STATS CALCULATION FOR DISPLAY ---
            disp_active = sum(1 for d in display_list if not d.get('disabled'))
            disp_passive = sum(1 for d in display_list if d.get('disabled'))

            print("\n{}Page {} of {} {} Total: {} {}│ {}Active: {} {}│ {}Passive: {}{}".format(
                TColors.BOLD + TColors.CYAN, current_page, total_pages,
                TColors.RESET + TColors.DIM + "│", total_items,
                TColors.RESET + TColors.DIM + "│", # Separator
                TColors.GREEN, disp_active,        # Active count with Green
                TColors.RESET + TColors.DIM + "│", # Separator
                TColors.RED, disp_passive,         # Passive count with Red
                TColors.RESET
            ))
            
            def print_item(d, color_code, is_dim=False):
                try: idx = display_list.index(d) + 1
                except: idx = 0
                id_val = d.get('id', 'N/A')
                prio = d.get('priority', '-')
                name = d.get('name', 'No Name')
                name_style = TColors.DIM if is_dim else ""
                
                # --- UPDATE: Center Alignment for Index {:^3} and ID {:^8} ---
                print("{}│{} {}[{:^3}]{} {}[ID:{:^8}]{} {}[P:{}]{} {}{}{}".format(
                    color_code, TColors.RESET,
                    TColors.BOLD + TColors.WHITE, idx, TColors.RESET,
                    TColors.CYAN, id_val, TColors.RESET,
                    TColors.YELLOW, prio, TColors.RESET,
                    name_style, name, TColors.RESET
                ))

            page_active = [d for d in page_items if not d.get('disabled')]
            page_passive = [d for d in page_items if d.get('disabled')]

            if page_active:
                print("\n{}╭─ ACTIVE DIRECTIVES (Disabled: False) {}".format(
                    TColors.GREEN + TColors.BOLD, "─" * 42 + "╮" + TColors.RESET
                ))
                for d in page_active: print_item(d, TColors.GREEN)
                print("{}╰{}╯{}".format(TColors.GREEN, "─" * 76, TColors.RESET))

            if page_passive:
                if page_active: print("") 
                print("{}╭─ INACTIVE DIRECTIVES (Disabled: True) {}".format(
                    TColors.RED + TColors.BOLD, "─" * 41 + "╮" + TColors.RESET
                ))
                for d in page_passive: print_item(d, TColors.RED, is_dim=True)
                print("{}╰{}╯{}".format(TColors.RED, "─" * 76, TColors.RESET))

        print("\n{}{}{}".format(TColors.CYAN, "═" * 78, TColors.RESET))
        opts = []
        if total_pages > 1:
            if current_page > 1: opts.append("{}P{}=Prev".format(TColors.YELLOW, TColors.RESET))
            if current_page < total_pages: opts.append("{}N{}=Next".format(TColors.YELLOW, TColors.RESET))
        
        opts.append("{}F{}=Filter".format(TColors.CYAN, TColors.RESET))
        if search_term: opts.append("{}C{}=Clear".format(TColors.CYAN, TColors.RESET))
        opts.append("{}S{}=Status".format(TColors.MAGENTA, TColors.RESET))
        
        if show_az_options:
            opts.append("{}A{}=All Active".format(TColors.GREEN, TColors.RESET))
            opts.append("{}Z{}=All Passive".format(TColors.RED, TColors.RESET))
        opts.append("{}B{}=Back".format(TColors.RED, TColors.RESET))
        
        print("{}Options: {}{}".format(TColors.DIM, " │ ".join(opts), TColors.RESET))
        print("{}{}{}".format(TColors.CYAN, "═" * 78, TColors.RESET))

        sel = input("\n{}▸{} Your Choice: ".format(TColors.BOLD, TColors.RESET)).strip().lower()
        
        if sel == 'b': return ['back']
        elif sel == 'n' and current_page < total_pages: current_page += 1
        elif sel == 'p' and current_page > 1: current_page -= 1
        elif sel == 'f': 
            search_term = input("{}Enter filter term: {}".format(TColors.YELLOW, TColors.RESET)).strip()
            current_page = 1
        elif sel == 'c': 
            search_term = ""
            current_page = 1
        elif sel == 's':
            view_mode = (view_mode + 1) % 3
            current_page = 1
        elif show_az_options and sel == 'a': return ['set_all_active']
        elif show_az_options and sel == 'z': return ['set_all_passive']
        elif not sel: continue
        else:
            parsed = parse_selection(sel, len(display_list))
            if not parsed: 
                print_warning("Invalid selection")
                time.sleep(1)
                continue
            return [display_list[i].get('id') for i in parsed]

def run_edit_session(filename, structure, initial_data):
    current_data = initial_data
    base_filename = os.path.basename(filename)
    
    if structure == "single_object":
        print_error("Single object editing not fully styled in this view.")
        input("Press Enter...")
        return

    while True:
        print_header("EDIT MENU", "✏️", base_filename)
        
        all_dirs = current_data.get('directives', [])
        active_cnt = sum(1 for d in all_dirs if not d.get('disabled', False))
        passive_cnt = sum(1 for d in all_dirs if d.get('disabled', False))
        
        print_info_box("STATISTICS", [
            ("Total Directives", len(all_dirs)),
            ("Active", "{}{}{}".format(TColors.GREEN, active_cnt, TColors.RESET)),
            ("Inactive", "{}{}{}".format(TColors.RED, passive_cnt, TColors.RESET))
        ])

        print_menu_card([
            ("1", "Change Priority Level", TColors.CYAN, ""),
            ("2", "Toggle Active/Passive Status", TColors.YELLOW, ""),
            ("3", "Delete Directive(s)", TColors.RED, ""),
            ("4", "Save & Upload to Pod", TColors.GREEN, ""),
            ("5", "Return to Main Menu", TColors.WHITE, "")
        ])
        
        act = input("\n{}▸{} Select Action: ".format(TColors.BOLD, TColors.RESET)).strip()
        
        if act == '5': break
        
        if act == '4':
            with open(filename, 'w') as f: json.dump(current_data, f, indent=4)
            print_success("Saved locally")
            if input("\nUpload to pod? (y/n): ").lower() == 'y':
                print_loading("Uploading...")
                if run_command(["kubectl", "cp", filename, "{}:{}{}".format(POD_NAME, REMOTE_PATH, base_filename)]) is not None:
                    print_success("Uploaded")
                    if input("\nRestart pods? (y/n): ").lower() == 'y': restart_pods_logic(base_filename)
                else: print_error("Upload failed")
            break
        
        if act not in ['1', '2', '3']: continue
        
        while True:
            show_az = (act == '2')
            ids = select_directives_from_file(current_data, show_az_options=show_az)
            
            if not ids or ids[0] == 'back': 
                break 
            
            if ids[0] in ['set_all_active', 'set_all_passive']:
                val = (ids[0] == 'set_all_passive')
                status_str = "PASSIVE" if val else "ACTIVE"
                if input("\nSet ALL to {}? (y/n): ".format(status_str)).lower() == 'y':
                    for d in current_data['directives']: d['disabled'] = val
                    print_success("Updated all directives")
                    time.sleep(0.5)
                continue
            
            ids_set = set(int(x) for x in ids)
            
            if act == '2':  # Toggle
                count = 0
                for d in current_data['directives']:
                    if int(d.get('id')) in ids_set:
                        d['disabled'] = not d.get('disabled', False)
                        count += 1
                print_success("Toggled {} directive(s)".format(count))
                
            elif act == '3':  # Delete
                print_warning("Deleting {} directive(s)".format(len(ids_set)))
                if input("Type 'DELETE' to confirm: ").strip() == 'DELETE':
                    current_data['directives'] = [d for d in current_data['directives'] if int(d.get('id')) not in ids_set]
                    print_success("Deleted")
                
            elif act == '1':  # Priority
                try:
                    val = int(input("\nEnter new priority number: "))
                    for d in current_data['directives']:
                        if int(d.get('id')) in ids_set: d['priority'] = val
                    print_success("Priority updated")
                except: 
                    print_error("Invalid number")
                    time.sleep(1)
            
            time.sleep(0.3)

# ======================= MAIN WORKFLOW =======================

def select_file_workflow():
    print_header("FILE BROWSER", "📂")
    print_loading("Fetching files...")
    
    res = run_command(["kubectl", "exec", POD_NAME, "--", "find", REMOTE_PATH, "-maxdepth", "1", "-name", FILE_PATTERN])
    if not res: return None
    
    files = sorted([os.path.basename(f) for f in res.split('\n') if f.endswith('.json')])
    if not files: return None
    
    print("\n{}📄 AVAILABLE FILES:{}".format(TColors.BOLD + TColors.CYAN, TColors.RESET))
    print_separator("─", 78, TColors.DIM)
    for i, f in enumerate(files, 1):
        print("  {}[{:2d}]{} {}".format(TColors.CYAN, i, TColors.RESET, f))
    print_separator("─", 78, TColors.DIM)
    
    try:
        sel = input("\nSelect file # (or B to back): ").strip()
        if sel.lower() == 'b': return None
        fname = files[int(sel) - 1]
        
        print_loading("Downloading {}...".format(fname))
        local_path = os.path.join(LOCAL_DIR, fname)
        if run_command(["kubectl", "cp", "{}:{}{}".format(POD_NAME, REMOTE_PATH, fname), local_path]) is not None:
            return local_path
    except: pass
    return None

def search_directive_workflow():
    # 1. Hitung file lokal dulu untuk info statistik
    local_files_count = 0
    if os.path.exists(LOCAL_DIR):
        local_files_count = len([f for f in os.listdir(LOCAL_DIR) if f.endswith('.json')])

    # 2. Tampilkan Header & Dashboard
    print_header("GLOBAL SEARCH", "🔍", "Search directives across all files")
    
    print("\n{}{}╭─ SEARCH CONTEXT ──────────────────────────────────────────╮{}".format(TColors.CYAN, TColors.BOLD, TColors.RESET))
    print("{}│{} Scope       : {}All JSON files in {}{}".format(
        TColors.CYAN, TColors.RESET, TColors.BOLD, LOCAL_DIR, TColors.RESET))
    print("{}│{} Local Cache : {}{} files detected{}".format(
        TColors.CYAN, TColors.RESET, TColors.YELLOW, local_files_count, TColors.RESET))
    print("{}│{} Match Type  : {}Case-insensitive, Partial match{}".format(
        TColors.CYAN, TColors.RESET, TColors.DIM, TColors.RESET))
    print("{}{}├─────────────────────────────────────────────────────────────┤{}".format(TColors.CYAN, TColors.BOLD, TColors.RESET))
    
    # --- BARIS YANG DIPERBAIKI ADA DI BAWAH INI (Ditambah TColors.RESET di akhir) ---
    print("{}│{} {}Tips:{}".format(TColors.CYAN, TColors.RESET, TColors.BOLD, TColors.RESET))
    
    print("{}│{} • Type keywords like 'SQL', 'XSS', or specific ID.".format(TColors.CYAN, TColors.RESET))
    print("{}│{} • Leave empty and press Enter to Cancel/Back.".format(TColors.CYAN, TColors.RESET))
    print("{}{}╰─────────────────────────────────────────────────────────────╯{}".format(TColors.CYAN, TColors.BOLD, TColors.RESET))
    
    # 3. Input Prompt
    print("\n{}Enter search keyword:{}".format(TColors.CYAN, TColors.RESET))
    term = input("{}▸{} ".format(TColors.BOLD + TColors.WHITE, TColors.RESET)).strip()
    
    if not term:
        return None
    
    # 4. Proses Sync & Search
    print("") # Spacer
    if not sync_all_files(): return None
    
    matches = []
    print_loading("Indexing directives...")
    
    for f in os.listdir(LOCAL_DIR):
        if f.endswith('.json'):
            try:
                with open(os.path.join(LOCAL_DIR, f)) as jf:
                    data = json.load(jf)
                    dirs = data.get('directives', []) if 'directives' in data else [data]
                    for d in dirs:
                        # Search by Name OR ID
                        d_name = d.get('name', '').lower()
                        d_id = str(d.get('id', ''))
                        if term.lower() in d_name or term in d_id:
                            matches.append(f)
                            break # Found match in this file, move to next file
            except: pass
    
    if not matches:
        print_error("No matches found for keyword: '{}'".format(term))
        input("\n{}Press Enter to return...{}".format(TColors.DIM, TColors.RESET))
        return None
        
    print_success("Found matches in {} file(s)".format(len(matches)))
    
    # Tampilkan hasil
    print("\n{}📄 MATCHING FILES:{}".format(TColors.BOLD + TColors.CYAN, TColors.RESET))
    print_separator("─", 78, TColors.DIM)
    for i, f in enumerate(matches, 1):
        print("  {}[{:2d}]{} {}".format(TColors.CYAN, i, TColors.RESET, f))
    print_separator("─", 78, TColors.DIM)
        
    try:
        sel = input("\n{}▸{} Select file number to edit: ".format(TColors.BOLD, TColors.RESET)).strip()
        idx = int(sel) - 1
        return os.path.join(LOCAL_DIR, matches[idx])
    except: return None
    
def main():
    try:
        check_deps()
        if not os.path.exists(LOCAL_DIR): os.makedirs(LOCAL_DIR)
        if not verify_pod_connection(): sys.exit(1)
        
        while True:
            clear_screen()
            print_info_box("SYSTEM CONFIGURATION", [
                ("Pod", POD_NAME), ("Local Dir", LOCAL_DIR), ("Pattern", FILE_PATTERN)
            ])
            
            print_menu_card([
                ("1", "Browse & Select File", TColors.CYAN, ""),
                ("2", "Search Directive Name", TColors.GREEN, ""),
                ("3", "Batch Update File", TColors.MAGENTA, ""),
                ("H", "Help", TColors.BLUE, ""),
                ("Q", "Quit", TColors.RED, "")
            ])
            
            c = input("\n{}▸{} Your choice: ".format(TColors.BOLD, TColors.RESET)).strip().lower()
            
            if c == 'q': sys.exit(0)
            elif c == 'h': show_help_panel()
            elif c == '3': process_batch_pipe()
            elif c in ['1', '2']:
                target = select_file_workflow() if c == '1' else search_directive_workflow()
                if target and os.path.exists(target):
                    try:
                        with open(target, 'r') as f: data = json.load(f, object_pairs_hook=collections.OrderedDict)
                        struct = "array" if "directives" in data and isinstance(data["directives"], list) else "single_object"
                        run_edit_session(target, struct, data)
                    except Exception as e:
                        print_error("Error: {}".format(e))
                        input("Press Enter...")

    except KeyboardInterrupt:
        print("\n\nExiting...")
        sys.exit(0)

if __name__ == "__main__":
    main()
