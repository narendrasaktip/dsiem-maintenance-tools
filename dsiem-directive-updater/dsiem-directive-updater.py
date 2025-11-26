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
import datetime
import csv

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
    if total == 0: total = 1
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
    print_header("QUICK HELP GUIDE", "❓", "Panduan Penggunaan")
    
    help_data = [
        ("📂 FILE SELECTION", [
            "• Option 1: Browse list. Script akan list semua file JSON di pod.",
            "• Option 2: Search. Cari directive berdasarkan nama di semua file.",
            "• File akan otomatis di-download ke folder lokal saat dipilih."
        ]),
        ("📊 BATCH & LOGGING", [
            "• Batch Update: Support .txt/.csv (Name | Action).",
            "• Logging: Semua aksi (Manual/Batch) dicatat di CSV.",
            "• Timezone: Semua log menggunakan GMT+7 (WIB).",
            "• File Log: batch_history_log.csv (di folder script)."
        ]),
        ("🔧 ACTIONS", [
            "• Edit Priority: Ubah prioritas rule.",
            "• Toggle: Enable/Disable rule.",
            "• Delete: Hapus rule dari file JSON.",
            "• Semua perubahan akan dicatat di history log."
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

# ======================= LOGGING SYSTEM (GMT+7) =======================

def get_wib_timestamp():
    """Returns current time in GMT+7 (WIB)"""
    utc_now = datetime.datetime.utcnow()
    wib_time = utc_now + datetime.timedelta(hours=7)
    return wib_time.strftime("%Y-%m-%d %H:%M:%S")

def append_to_csv_log(log_entries):
    """
    Appends audit logs to a CSV file in the same directory as the script.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_file = os.path.join(script_dir, "batch_history_log.csv")
    
    file_exists = os.path.exists(log_file)
    
    try:
        with open(log_file, 'a') as f: # 'a' is for Append
            writer = csv.writer(f)
            # Write header if file is new
            if not file_exists:
                writer.writerow(["Timestamp (WIB)", "Filename", "Directive ID", "Directive Name", "Action", "Previous Status/Value"])
            
            for entry in log_entries:
                writer.writerow([
                    entry.get('timestamp', get_wib_timestamp()),
                    entry['filename'],
                    entry['id'],
                    entry['name'],
                    entry['action'],
                    entry['prev_status']
                ])
        # Only show success message if called from batch, to avoid spamming in manual mode
        # print_success("Log updated") 
    except Exception as e:
        print_error("Failed to write log file: {}".format(str(e)))

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
    if a in ['enabled', 'enable', 'on', 'false', 'active']: return False
    if a in ['disabled', 'disable', 'off', 'true', 'deactive', 'passive']: return True
    return None

def process_batch_pipe():
    print_header("BATCH UPDATE MODE", "📊")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    files_found = [f for f in os.listdir(script_dir) if f.endswith('.csv') or f.endswith('.txt') if 'history_log' not in f]
    
    if not files_found:
        print_error("No input files (.csv/.txt) found")
        input("Press Enter...")
        return

    print("\nAvailable Input Files:")
    for i, f in enumerate(files_found, 1):
        print("[{}] {}".format(i, f))
    
    try:
        sel_input = input("\nSelect File: ")
        if not sel_input: return
        sel = int(sel_input) - 1
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

    print_success("Loaded {} update rules".format(len(target_updates)))
    
    print("\n{}Scanning files for matches...{}".format(TColors.CYAN, TColors.RESET))
    
    batch_report = collections.OrderedDict() 
    files_to_scan = [f for f in os.listdir(LOCAL_DIR) if f.endswith('.json')]
    total_files = len(files_to_scan)

    for idx, jf in enumerate(files_to_scan, 1):
        print_progress_bar(idx, total_files, "Scanning", 30)
        fpath = os.path.join(LOCAL_DIR, jf)
        try:
            with open(fpath, 'r') as f: 
                data = json.load(f, object_pairs_hook=collections.OrderedDict)
            
            dirs = data.get("directives", []) if "directives" in data else [data]
            file_changes = []

            for d in dirs:
                d_name = d.get('name', '').lower().strip()
                if d_name in target_updates:
                    new_state_disabled = target_updates[d_name]
                    curr_state_disabled = d.get('disabled', False)
                    
                    if curr_state_disabled != new_state_disabled:
                        action_label = "DISABLE" if new_state_disabled else "ENABLE"
                        file_changes.append({
                            'id': d.get('id'),
                            'name': d.get('name'),
                            'action': action_label,
                            'new_val': new_state_disabled,
                            'prev_status': "Disabled" if curr_state_disabled else "Enabled"
                        })
            
            if file_changes:
                batch_report[jf] = file_changes
                
        except Exception as e: pass
    
    print("") 

    if not batch_report:
        print_warning("No matching directives found to update.")
        input("Press Enter...")
        return

    clear_screen()
    print_header("BATCH PREVIEW", "📊", "Review changes before applying")
    
    total_changes = sum(len(v) for v in batch_report.values())
    print("{}Found {} directives to update across {} files.{}".format(
        TColors.CYAN, total_changes, len(batch_report), TColors.RESET))

    for fname, changes in batch_report.items():
        print("\n{}📄 File: {}{}".format(TColors.BOLD + TColors.WHITE, fname, TColors.RESET))
        print_separator("─", 78, TColors.DIM)
        
        for item in changes:
            if item['action'] == "ENABLE":
                arrow = "{}➔ ENABLED{}".format(TColors.GREEN + TColors.BOLD, TColors.RESET)
                old_st = "Disabled"
            else:
                arrow = "{}➔ DISABLED{}".format(TColors.RED + TColors.BOLD, TColors.RESET)
                old_st = "Enabled "

            print("  {}• ID: {:<8} {}| {}{:<45} {}| {} {}".format(
                TColors.CYAN, item['id'], TColors.DIM, 
                TColors.RESET + TColors.BOLD, item['name'][:45], 
                TColors.DIM,
                TColors.DIM + old_st + TColors.RESET, arrow
            ))
    
    print_separator("═", 78, TColors.CYAN)
    
    confirm = input("\n{}Apply these {} changes? (y/n): {}".format(TColors.YELLOW, total_changes, TColors.RESET)).lower()
    if confirm != 'y':
        print_warning("Operation Cancelled.")
        time.sleep(1.5)
        return

    print_loading("Applying changes and generating logs...")
    
    modified_files = set()
    audit_logs = []
    
    for jf, changes in batch_report.items():
        fpath = os.path.join(LOCAL_DIR, jf)
        try:
            with open(fpath, 'r') as f: 
                data = json.load(f, object_pairs_hook=collections.OrderedDict)
            
            dirs = data.get("directives", []) if "directives" in data else [data]
            change_map = {c['id']: c for c in changes}
            
            for d in dirs:
                if d.get('id') in change_map:
                    change_info = change_map[d.get('id')]
                    d['disabled'] = change_info['new_val']
                    
                    audit_logs.append({
                        'timestamp': get_wib_timestamp(),
                        'filename': jf,
                        'id': d.get('id'),
                        'name': d.get('name'),
                        'action': change_info['action'],
                        'prev_status': change_info['prev_status']
                    })
            
            with open(fpath, 'w') as f: 
                json.dump(data, f, indent=4)
            
            modified_files.add(jf)
            
        except Exception as e:
            print_error("Failed to write {}: {}".format(jf, e))

    if modified_files:
        print_success("Successfully updated {} files".format(len(modified_files)))
        append_to_csv_log(audit_logs)
        
        if input("\nUpload to pod? (y/n): ").lower() == 'y':
            for i, mf in enumerate(modified_files, 1):
                print_progress_bar(i, len(modified_files), "Uploading", 30)
                run_command(["kubectl", "cp", os.path.join(LOCAL_DIR, mf), "{}:{}{}".format(POD_NAME, REMOTE_PATH, mf)])
            
            print_success("Upload complete")
            if input("Restart pods? (y/n): ").lower() == 'y':
                restart_pods_logic(list(modified_files)[0])
    else:
        print_warning("No files were modified.")
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
    view_mode = 0 # 0=All, 1=Enabled, 2=Disabled
    
    while True:
        print_header("SELECT DIRECTIVES", "📋")
        
        all_directives = data.get("directives", [])
        if not all_directives:
            print_warning("No directives found in this file")
            input("\n{}Press Enter to return...{}".format(TColors.DIM, TColors.RESET))
            return ['back']

        status_text = "ALL"
        if view_mode == 1: status_text = "Enabled ONLY"
        elif view_mode == 2: status_text = "Disabled ONLY"
        
        filter_text = search_term if search_term else "None"
        print("{}🔍 Filter: {}{} {}│{} {}👁️  View: {}{}".format(
            TColors.CYAN, TColors.BOLD + TColors.WHITE, filter_text, TColors.RESET,
            TColors.DIM, TColors.RESET,
            TColors.MAGENTA + TColors.BOLD, status_text, TColors.RESET
        ))

        if search_term:
            name_filtered = [d for d in all_directives if search_term.lower() in d.get('name', '').lower()]
        else:
            name_filtered = list(all_directives)

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
                Enabled_dirs = [d for d in sorted_directives if not d.get('disabled')]
                Disabled_dirs = [d for d in sorted_directives if d.get('disabled')]
                display_list = Enabled_dirs + Disabled_dirs
            else:
                display_list = sorted_directives
            
            total_items = len(display_list)
            total_pages = (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
            if total_pages == 0: total_pages = 1
            current_page = max(1, min(current_page, total_pages))
            
            start = (current_page - 1) * ITEMS_PER_PAGE
            page_items = display_list[start:start + ITEMS_PER_PAGE]

            disp_Enabled = sum(1 for d in display_list if not d.get('disabled'))
            disp_Disabled = sum(1 for d in display_list if d.get('disabled'))

            print("\n{}Page {} of {} {} Total: {} {}│ {}Enabled: {} {}│ {}Disabled: {}{}".format(
                TColors.BOLD + TColors.CYAN, current_page, total_pages,
                TColors.RESET + TColors.DIM + "│", total_items,
                TColors.RESET + TColors.DIM + "│", 
                TColors.GREEN, disp_Enabled,
                TColors.RESET + TColors.DIM + "│", 
                TColors.RED, disp_Disabled, 
                TColors.RESET
            ))
            
            def print_item(d, color_code, is_dim=False):
                try: idx = display_list.index(d) + 1
                except: idx = 0
                id_val = d.get('id', 'N/A')
                prio = d.get('priority', '-')
                name = d.get('name', 'No Name')
                name_style = TColors.DIM if is_dim else ""
                
                print("{}│{} {}[{:^3}]{} {}[ID:{:^8}]{} {}[P:{}]{} {}{}{}".format(
                    color_code, TColors.RESET,
                    TColors.BOLD + TColors.WHITE, idx, TColors.RESET,
                    TColors.CYAN, id_val, TColors.RESET,
                    TColors.YELLOW, prio, TColors.RESET,
                    name_style, name, TColors.RESET
                ))

            page_Enabled = [d for d in page_items if not d.get('disabled')]
            page_Disabled = [d for d in page_items if d.get('disabled')]

            if page_Enabled:
                print("\n{}╭─ ENABLED DIRECTIVES (Disabled: False) {}".format(
                    TColors.GREEN + TColors.BOLD, "─" * 42 + "╮" + TColors.RESET
                ))
                for d in page_Enabled: print_item(d, TColors.GREEN)
                print("{}╰{}╯{}".format(TColors.GREEN, "─" * 76, TColors.RESET))

            if page_Disabled:
                if page_Enabled: print("") 
                print("{}╭─ DISABLED DIRECTIVES (Disabled: True) {}".format(
                    TColors.RED + TColors.BOLD, "─" * 41 + "╮" + TColors.RESET
                ))
                for d in page_Disabled: print_item(d, TColors.RED, is_dim=True)
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
            opts.append("{}A{}=All Enabled".format(TColors.GREEN, TColors.RESET))
            opts.append("{}Z{}=All Disabled".format(TColors.RED, TColors.RESET))
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
        elif show_az_options and sel == 'a': return ['set_all_Enabled']
        elif show_az_options and sel == 'z': return ['set_all_Disabled']
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
        Enabled_cnt = sum(1 for d in all_dirs if not d.get('disabled', False))
        Disabled_cnt = sum(1 for d in all_dirs if d.get('disabled', False))
        
        print_info_box("STATISTICS", [
            ("Total Directives", len(all_dirs)),
            ("Enabled", "{}{}{}".format(TColors.GREEN, Enabled_cnt, TColors.RESET)),
            ("Disabled", "{}{}{}".format(TColors.RED, Disabled_cnt, TColors.RESET))
        ])

        print_menu_card([
            ("1", "Change Priority Level", TColors.CYAN, ""),
            ("2", "Toggle Enabled/Disabled Status", TColors.YELLOW, ""),
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
            
            # --- HANDLE BATCH ENABLE/DISABLE ALL FROM MENU ---
            if ids[0] in ['set_all_Enabled', 'set_all_Disabled']:
                val = (ids[0] == 'set_all_Disabled')
                status_str = "Disabled" if val else "Enabled"
                if input("\nSet ALL to {}? (y/n): ".format(status_str)).lower() == 'y':
                    manual_logs = []
                    for d in current_data['directives']:
                        old_val = d.get('disabled', False)
                        if old_val != val:
                            d['disabled'] = val
                            manual_logs.append({
                                'filename': base_filename,
                                'id': d.get('id'),
                                'name': d.get('name'),
                                'action': "DISABLE" if val else "ENABLE",
                                'prev_status': "Disabled" if old_val else "Enabled"
                            })
                    if manual_logs:
                        append_to_csv_log(manual_logs)
                        print_success("Updated and logged all directives")
                    else:
                        print_warning("No changes made (already in state)")
                    time.sleep(1)
                continue
            
            ids_set = set(int(x) for x in ids)
            manual_logs = [] # Collector for logging
            
            if act == '2':  # Toggle
                count = 0
                for d in current_data['directives']:
                    if int(d.get('id')) in ids_set:
                        old_status = "Disabled" if d.get('disabled', False) else "Enabled"
                        d['disabled'] = not d.get('disabled', False)
                        new_action = "DISABLE" if d['disabled'] else "ENABLE"
                        
                        manual_logs.append({
                            'filename': base_filename,
                            'id': d.get('id'),
                            'name': d.get('name'),
                            'action': new_action,
                            'prev_status': old_status
                        })
                        count += 1
                
                if manual_logs:
                    append_to_csv_log(manual_logs)
                    print_success("Toggled {} directive(s) & logged".format(count))
                
            elif act == '3':  # Delete
                print_warning("Deleting {} directive(s)".format(len(ids_set)))
                if input("Type 'DELETE' to confirm: ").strip() == 'DELETE':
                    # Log first before deleting
                    to_delete = [d for d in current_data['directives'] if int(d.get('id')) in ids_set]
                    for d in to_delete:
                        manual_logs.append({
                            'filename': base_filename,
                            'id': d.get('id'),
                            'name': d.get('name'),
                            'action': "DELETE",
                            'prev_status': "Exists"
                        })
                    
                    # Actual deletion
                    current_data['directives'] = [d for d in current_data['directives'] if int(d.get('id')) not in ids_set]
                    
                    if manual_logs:
                        append_to_csv_log(manual_logs)
                        print_success("Deleted and logged")
                
            elif act == '1':  # Priority
                try:
                    val = int(input("\nEnter new priority number: "))
                    for d in current_data['directives']:
                        if int(d.get('id')) in ids_set:
                            old_prio = d.get('priority', '-')
                            if str(old_prio) != str(val):
                                d['priority'] = val
                                manual_logs.append({
                                    'filename': base_filename,
                                    'id': d.get('id'),
                                    'name': d.get('name'),
                                    'action': "PRIORITY_CHANGE",
                                    'prev_status': "{} -> {}".format(old_prio, val)
                                })
                    
                    if manual_logs:
                        append_to_csv_log(manual_logs)
                        print_success("Priority updated and logged")
                except ValueError: 
                    print_error("Invalid number")
                    time.sleep(1)
            
            time.sleep(0.5)

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
    local_files_count = 0
    if os.path.exists(LOCAL_DIR):
        local_files_count = len([f for f in os.listdir(LOCAL_DIR) if f.endswith('.json')])

    print_header("GLOBAL SEARCH", "🔍", "Search directives across all files")
    
    print("\n{}{}╭─ SEARCH CONTEXT ──────────────────────────────────────────╮{}".format(TColors.CYAN, TColors.BOLD, TColors.RESET))
    print("{}│{} Scope       : {}All JSON files in {}{}".format(
        TColors.CYAN, TColors.RESET, TColors.BOLD, LOCAL_DIR, TColors.RESET))
    print("{}│{} Local Cache : {}{} files detected{}".format(
        TColors.CYAN, TColors.RESET, TColors.YELLOW, local_files_count, TColors.RESET))
    print("{}│{} Match Type  : {}Case-insensitive, Partial match{}".format(
        TColors.CYAN, TColors.RESET, TColors.DIM, TColors.RESET))
    print("{}{}├─────────────────────────────────────────────────────────────┤{}".format(TColors.CYAN, TColors.BOLD, TColors.RESET))
    print("{}│{} {}Tips:{}".format(TColors.CYAN, TColors.RESET, TColors.BOLD, TColors.RESET))
    print("{}│{} • Type keywords like 'SQL', 'XSS', or specific ID.".format(TColors.CYAN, TColors.RESET))
    print("{}│{} • Leave empty and press Enter to Cancel/Back.".format(TColors.CYAN, TColors.RESET))
    print("{}{}╰─────────────────────────────────────────────────────────────╯{}".format(TColors.CYAN, TColors.BOLD, TColors.RESET))
    
    print("\n{}Enter search keyword:{}".format(TColors.CYAN, TColors.RESET))
    term = input("{}▸{} ".format(TColors.BOLD + TColors.WHITE, TColors.RESET)).strip()
    
    if not term:
        return None
    
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
                        d_name = d.get('name', '').lower()
                        d_id = str(d.get('id', ''))
                        if term.lower() in d_name or term in d_id:
                            matches.append(f)
                            break 
            except: pass
    
    if not matches:
        print_error("No matches found for keyword: '{}'".format(term))
        input("\n{}Press Enter to return...{}".format(TColors.DIM, TColors.RESET))
        return None
        
    print_success("Found matches in {} file(s)".format(len(matches)))
    
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
