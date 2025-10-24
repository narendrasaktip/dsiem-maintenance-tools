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

DISTRIBUTED_SUCCESSFULLY = False
try: input = raw_input
except NameError: pass

class TColors:
    BOLD='\033[1m'; GREEN='\033[92m'; YELLOW='\033[93m'; RED='\033[91m'
    CYAN='\033[96m'; BLUE='\033[94m'; MAGENTA='\033[95m'; WHITE='\033[97m'
    BG_GREEN='\033[102m'; BG_BLUE='\033[104m'; BG_RED='\033[101m'
    BG_YELLOW='\033[103m'; BG_CYAN='\033[106m'; RESET='\033[0m'
    DIM='\033[2m'; UNDERLINE='\033[4m'
    if not os.popen('tput sgr0 2>/dev/null').read():
        BOLD=GREEN=YELLOW=RED=CYAN=BLUE=MAGENTA=WHITE=RESET=""
        BG_GREEN=BG_BLUE=BG_RED=BG_YELLOW=BG_CYAN=DIM=UNDERLINE=""

def print_info_box(title, items, icon="ℹ"):
    """Print an information box with dynamic width based on content - FULL BOX"""
    # Calculate maximum content width
    max_content_length = len(title) + len(icon) + 2  # title + icon + spaces

    for key, value in items:
        # Calculate visible length (without color codes)
        content_line = "  {}: {}".format(key, str(value))
        max_content_length = max(max_content_length, len(content_line))

    # Add padding for borders and ensure reasonable width
    box_width = max_content_length + 4  # +4 for borders and padding
    inner_width = box_width - 2

    # Top border
    print("\n{}┌{}┐{}".format(TColors.CYAN, "─" * inner_width, TColors.RESET))

    # Title line
    title_text = " {} {} ".format(icon, title)
    padding = inner_width - len(title_text)
    # PERBAIKAN: Spasi ekstra dihapus dari sini
    print("{}│{}{}{} {} │{}".format(
        TColors.CYAN,
        TColors.RESET + TColors.BOLD,
        title_text,
        TColors.RESET,
        " " * padding,
        TColors.CYAN + TColors.RESET
    ))

    # Middle border
    print("{}├{}┤{}".format(TColors.CYAN, "─" * inner_width, TColors.RESET))

    # Content lines
    for key, value in items:
        line = "  {}{}: {}{}".format(TColors.YELLOW, key, TColors.WHITE, value)
        # Calculate visible text length (without ANSI codes)
        visible_text = "  {}: {}".format(key, str(value))
        padding = inner_width - len(visible_text)
        # PERBAIKAN: Spasi ekstra dihapus dari sini
        print("{}│{}{}{}│{}".format(
            TColors.CYAN,
            TColors.RESET,
            line,
            " " * max(0, padding),
            TColors.CYAN + TColors.RESET
        ))

    # Bottom border
    print("{}└{}┘{}".format(TColors.CYAN, "─" * inner_width, TColors.RESET))

def print_separator(char="═", width=80, color=None):
    pre = color if color else ""
    print("{}{}{}".format(pre, char * width, TColors.RESET))

def print_header(text, icon=""):
    clear_screen()
    print("\n")
    print_separator("═", 80, TColors.CYAN)
    centered = "{} {} {}".format(icon, text, icon).center(80)
    print("{}{}{}".format(TColors.BOLD + TColors.CYAN, centered, TColors.RESET))
    print_separator("═", 80, TColors.CYAN)
    print("")

def print_panel(title, content, color=TColors.BLUE, width=78):
    """Print a panel with title and content"""
    print("\n{}{}┌─[ {} ]{}".format(color, TColors.BOLD, title, "─" * (width - len(title) - 5)))
    for line in content.split('\n'):
        print("{}│{} {}".format(color, TColors.RESET, line))
    print("{}└{}{}".format(color, "─" * (width-2), TColors.RESET))

def print_menu(title, options, show_back=True, show_quit=True):
    """Print a styled menu"""
    print("\n{}{}{}".format(TColors.BOLD + TColors.CYAN, "╭─ " + title + " " + "─" * (75 - len(title)), TColors.RESET))
    for i, (key, desc, color) in enumerate(options):
        icon = "▸" if i < len(options) else "◈"
        print("{}│{} [{}{}{}] {}{}".format(
            TColors.CYAN, TColors.RESET,
            color + TColors.BOLD, key, TColors.RESET,
            desc, TColors.RESET
        ))
    
    if show_back or show_quit:
        print("{}├{}{}".format(TColors.CYAN, "─" * 77, TColors.RESET))
    if show_back:
        print("{}│{} [{}B{}] {}Back{}".format(
            TColors.CYAN, TColors.RESET,
            TColors.YELLOW + TColors.BOLD, TColors.RESET,
            TColors.YELLOW, TColors.RESET
        ))
    if show_quit:
        print("{}│{} [{}Q{}] {}Quit{}".format(
            TColors.CYAN, TColors.RESET,
            TColors.RED + TColors.BOLD, TColors.RESET,
            TColors.RED, TColors.RESET
        ))
    print("{}╰{}{}".format(TColors.CYAN, "─" * 77, TColors.RESET))

def print_success(text):
    print("\n{}✓{} {}{}".format(TColors.GREEN + TColors.BOLD, TColors.RESET, TColors.GREEN, text + TColors.RESET))

def print_error(text):
    print("\n{}✗{} {}{}".format(TColors.RED + TColors.BOLD, TColors.RESET, TColors.RED, text + TColors.RESET))

def print_warning(text):
    print("\n{}⚠{} {}{}".format(TColors.YELLOW + TColors.BOLD, TColors.RESET, TColors.YELLOW, text + TColors.RESET))

def print_loading(text):
    print("\n{}⟳{} {}{}".format(TColors.CYAN + TColors.BOLD, TColors.RESET, TColors.CYAN, text + TColors.RESET))

def clear_screen():
    os.system('clear' if os.name != 'nt' else 'cls')

def check_deps():
    try:
        with open(os.devnull, 'w') as FNULL:
            cmd = ["kubectl"]
            if NAMESPACE: cmd.extend(["-n", NAMESPACE])
            cmd.extend(["version", "--client"])
            subprocess.Popen(cmd, stdout=FNULL, stderr=FNULL).wait()
    except OSError:
        print_error("'kubectl' not found. Please install kubectl.")
        sys.exit(1)

def run_command(cmd_list, check_stderr=False):
    try:
        full_cmd = list(cmd_list)
        if cmd_list[0] == "kubectl" and NAMESPACE:
            full_cmd.insert(1, "-n")
            full_cmd.insert(2, NAMESPACE)
        
        process = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout_bytes, stderr_bytes = process.communicate()
        stdout_str = stdout_bytes.decode('utf-8').strip()
        stderr_str = stderr_bytes.decode('utf-8').strip()
        
        if process.returncode != 0:
            if not (stderr_str.startswith("Defaulting container name") or "pod default value" in stderr_str):
                print_error("Command failed: {}".format(' '.join(full_cmd)))
                if stdout_str: print("{}Stdout: {}{}".format(TColors.DIM, stdout_str, TColors.RESET))
                if stderr_str: print("{}Stderr: {}{}".format(TColors.DIM, stderr_str, TColors.RESET))
                return None
        
        if check_stderr: return stdout_str, stderr_str
        else: return stdout_str
    except OSError:
        print_error("Command '{}' not found.".format(full_cmd[0]))
        return None
    except Exception as e:
        print_error("Unexpected error: {}".format(e))
        return None

def parse_selection(input_str, max_total_items, current_page=1, items_per_page=20):
    indices = set()
    input_str = input_str.strip().lower()
    parts = re.split(r'[\s,]+', input_str)
    
    for part in parts:
        if not part: continue
        match = re.match(r'^(\d+)-(\d+)$', part)
        if match:
            start_disp, end_disp = int(match.group(1)), int(match.group(2))
            if start_disp > end_disp: start_disp, end_disp = end_disp, start_disp
            start_idx = start_disp - 1
            end_idx = end_disp - 1
            if 0 <= start_idx < max_total_items and 0 <= end_idx < max_total_items:
                for i_idx in range(start_idx, end_idx + 1): indices.add(i_idx)
            else:
                print_warning("Range '{}-{}' out of bounds (max {}).".format(start_disp, end_disp, max_total_items))
        elif part.isdigit():
            i_disp = int(part)
            i_idx = i_disp - 1
            if 0 <= i_idx < max_total_items: indices.add(i_idx)
            else: print_warning("Choice '{}' out of bounds (max {}).".format(i_disp, max_total_items))
        else:
            if part not in ['n', 'p', 'a', 'z', 'b', 'f', 'c']:
                print_warning("Input '{}' invalid.".format(part))
    
    return sorted(list(indices))

def get_status_filter():
    """Asks user for the desired status filter."""
    while True:
        print_header("STATUS FILTER", "🔍")
        print_menu("Select Directive Status", [
            ("1", "Active (Disabled: False)", TColors.GREEN),
            ("2", "Passive (Disabled: True)", TColors.YELLOW),
            ("3", "Both (All Directives)", TColors.CYAN)
        ], show_back=False, show_quit=False)
        
        choice = input("\n{}▸{} Select status [1, 2, 3]: ".format(TColors.BOLD, TColors.RESET)).strip()
        if choice == '1': return False
        elif choice == '2': return True
        elif choice == '3': return None
        else: print_error("Invalid choice.")

def check_directive_status(directive_data, desired_disabled_status):
    if desired_disabled_status is None: return True
    return directive_data.get('disabled', False) == desired_disabled_status

def search_and_select_file():
    """Searches files in pod by directive name and status."""
    while True:
        print_header("SEARCH BY DIRECTIVE NAME", "🔍")
        
        print("{}Enter directive name (or part of name) to search{}".format(TColors.CYAN, TColors.RESET))
        search_term_input = input("\n{}▸{} Search term (or B to Back): ".format(TColors.BOLD, TColors.RESET)).strip()
        
        if not search_term_input:
            print_error("Search term empty.")
            time.sleep(1.5)
            continue
        if search_term_input.lower() == 'b':
            return None, None

        desired_status = get_status_filter()
        status_desc = "any status"
        if desired_status is False: status_desc = "Active"
        elif desired_status is True: status_desc = "Passive"

        print_loading("Searching '{}' files for '{}' (Status: {})...".format(FILE_PATTERN, search_term_input, status_desc))
        
        find_cmd = ["kubectl", "exec", POD_NAME, "--", "find", REMOTE_PATH, "-maxdepth", "1", "-name", FILE_PATTERN]
        file_list_raw = run_command(find_cmd)
        
        if file_list_raw is None or not file_list_raw.strip():
            print_error("Could not list files.")
            time.sleep(2)
            return None, None
        
        all_lines = file_list_raw.split('\n')
        valid_paths = [f for f in all_lines if f.startswith(REMOTE_PATH) and f.endswith('.json')]
        
        if not valid_paths:
            print_error("No valid JSON files.")
            time.sleep(2)
            return None, None

        matching_files = []
        for file_path in valid_paths:
            filename = os.path.basename(file_path)
            cat_cmd = ["kubectl", "exec", POD_NAME, "--", "cat", file_path]
            file_content_str = run_command(cat_cmd)
            
            if file_content_str is None:
                print_warning("Could not read {}. Skipping.".format(filename))
                continue
            
            try:
                data = json.loads(file_content_str, object_pairs_hook=collections.OrderedDict)
                found_in_file = False
                
                if "directives" in data and isinstance(data.get("directives"), list):
                    for directive in data.get("directives", []):
                        if check_directive_status(directive, desired_status) and search_term_input.lower() in directive.get('name', '').lower():
                            found_in_file = True
                            break
                else:
                    if check_directive_status(data, desired_status) and search_term_input.lower() in data.get('name', '').lower():
                        found_in_file = True
                
                if found_in_file:
                    matching_files.append(filename)
            except (ValueError, json.JSONDecodeError):
                print_warning("File {} not valid JSON. Skipping.".format(filename))
                continue

        if not matching_files:
            print_error("No file found matching '{}' (Status: {}).".format(search_term_input, status_desc))
            input("\n{}Press Enter to continue...{}".format(TColors.DIM, TColors.RESET))
            continue
        elif len(matching_files) == 1:
            selected_file = matching_files[0]
            print_success("Found match: {}".format(selected_file))
            print_loading("Copying {}...".format(selected_file))
            
            cp_cmd = ["kubectl", "cp", "{}:{}{}".format(POD_NAME, REMOTE_PATH, selected_file), "./{}".format(selected_file)]
            if run_command(cp_cmd) is None:
                print_error("Failed to copy.")
                time.sleep(2)
                return None, None
            
            print_success("Copied successfully.")
            return selected_file, search_term_input
        else:
            print_header("MULTIPLE MATCHES FOUND", "📋")
            matching_files.sort()
            
            print("\n{}Found {} matching files:{}".format(TColors.GREEN + TColors.BOLD, len(matching_files), TColors.RESET))
            for i, fname in enumerate(matching_files, 1):
                print("  {}[{:2d}]{} {}".format(TColors.CYAN, i, TColors.RESET, fname))
            
            print("\n{}{}{}".format(TColors.CYAN, "─" * 78, TColors.RESET))
            
            while True:
                try:
                    choice = input("\n{}▸{} Select file number (or B to Back): ".format(TColors.BOLD, TColors.RESET)).strip()
                    if choice.lower() == 'b':
                        return None, None
                    
                    choice_idx = int(choice) - 1
                    if 0 <= choice_idx < len(matching_files):
                        selected_file = matching_files[choice_idx]
                        print_success("Selected: {}".format(selected_file))
                        print_loading("Copying {}...".format(selected_file))
                        
                        cp_cmd = ["kubectl", "cp", "{}:{}{}".format(POD_NAME, REMOTE_PATH, selected_file), "./{}".format(selected_file)]
                        if run_command(cp_cmd) is None:
                            print_error("Failed to copy.")
                            time.sleep(2)
                            return None, None
                        
                        print_success("Copied successfully.")
                        return selected_file, search_term_input
                    else:
                        print_error("Invalid choice.")
                except ValueError:
                    print_error("Invalid input.")

def setup_and_select_file():
    """Step 1: Offer choice, then select file."""
    while True:
        print_header(" DIRECTIVE CONFIGURATION EDITOR", "⚙️")
        
        print_info_box("Configuration", [
            ("Pod Name", POD_NAME),
            ("Remote Path", REMOTE_PATH),
            ("Local Directory", LOCAL_DIR),
            ("File Pattern", FILE_PATTERN)
        ], "📋")
        
        print("\n{}💡 Note: Changes are saved locally until distributed.{}".format(TColors.YELLOW, TColors.RESET))
        
        if not os.path.isdir(LOCAL_DIR):
            try:
                os.makedirs(LOCAL_DIR)
            except OSError as e:
                print_error("Failed to create dir {}: {}".format(LOCAL_DIR, e))
                sys.exit(1)
        
        try:
            os.chdir(LOCAL_DIR)
        except OSError as e:
            print_error("Failed to cd into {}: {}".format(LOCAL_DIR, e))
            script_dir = os.path.dirname(os.path.abspath(__file__))
            os.chdir(script_dir)
            sys.exit(1)

        print_menu("Select File Method", [
            ("1", "Select from file list", TColors.CYAN),
            ("2", "Search by directive name", TColors.GREEN)
        ])
        
        method_choice = input("\n{}▸{} Select method [1, 2, B, Q]: ".format(TColors.BOLD, TColors.RESET)).strip().lower()
        
        initial_filter = None

        if method_choice == '1':
            print_loading("Fetching file list...")
            
            cmd = ["kubectl", "exec", POD_NAME, "--", "find", REMOTE_PATH, "-maxdepth", "1", "-name", FILE_PATTERN]
            file_list_raw = run_command(cmd)
            
            if file_list_raw is None or not file_list_raw.strip():
                print_error("No files found.")
                input("\n{}Press Enter to continue...{}".format(TColors.DIM, TColors.RESET))
                continue
            
            all_lines = file_list_raw.split('\n')
            valid_paths = [f for f in all_lines if f.startswith(REMOTE_PATH) and f.endswith('.json')]
            
            if not valid_paths:
                print_error("No valid JSON files.")
                input("\n{}Press Enter to continue...{}".format(TColors.DIM, TColors.RESET))
                continue
            
            file_names = [os.path.basename(f) for f in valid_paths]
            file_names.sort()

            print_header("SELECT FILE FROM LIST", "📄")
            print("\n{}Found {} files:{}".format(TColors.GREEN + TColors.BOLD, len(file_names), TColors.RESET))
            
            for i, filename in enumerate(file_names, 1):
                print("  {}[{:2d}]{} {}".format(TColors.CYAN, i, TColors.RESET, filename))
            
            print("\n{}{}{}".format(TColors.CYAN, "─" * 78, TColors.RESET))
            
            while True:
                try:
                    choice = input("\n{}▸{} Select file number (or B to Back): ".format(TColors.BOLD, TColors.RESET)).strip()
                    if choice.lower() == 'b':
                        break
                    
                    choice_idx = int(choice) - 1
                    if 0 <= choice_idx < len(file_names):
                        selected_file = file_names[choice_idx]
                        print_success("Selected: {}".format(selected_file))
                        print_loading("Copying {}...".format(selected_file))
                        
                        cp_cmd = ["kubectl", "cp", "{}:{}{}".format(POD_NAME, REMOTE_PATH, selected_file), "./{}".format(selected_file)]
                        if run_command(cp_cmd) is None:
                            print_error("Failed to copy.")
                            input("\n{}Press Enter to continue...{}".format(TColors.DIM, TColors.RESET))
                            break
                        
                        print_success("Copied to {}".format(LOCAL_DIR))
                        return selected_file, initial_filter
                    else:
                        print_error("Invalid choice.")
                except ValueError:
                    print_error("Invalid input.")
            continue
            
        elif method_choice == '2':
            selected_file, initial_filter = search_and_select_file()
            if selected_file:
                print_success("Copied to {}".format(LOCAL_DIR))
                return selected_file, initial_filter
            else:
                continue
                
        elif method_choice == 'q':
            print("\n{}Exiting...{}".format(TColors.YELLOW, TColors.RESET))
            script_dir = os.path.dirname(os.path.abspath(__file__))
            os.chdir(script_dir)
            sys.exit(0)
        else:
            print_error("Invalid selection.")
            time.sleep(1.5)
            continue

def check_file_structure(filename):
    """Reads JSON, determines structure."""
    try:
        with open(filename, 'r') as f:
            data = json.load(f, object_pairs_hook=collections.OrderedDict)
        if "directives" in data and isinstance(data.get("directives"), list):
            return "array", data
        else:
            return "single_object", data
    except (ValueError, json.JSONDecodeError):
        print_error("File {} not valid JSON.".format(filename))
        return None, None
    except IOError as e:
        print_error("Failed to read file {}: {}".format(filename, e))
        return None, None

def get_valid_input(prompt, validation_type):
    """Requests valid input."""
    while True:
        val = input("{}{}".format(TColors.BOLD + "▸ " + TColors.RESET, prompt)).strip().lower()
        if validation_type == "priority":
            if val.isdigit():
                return int(val)
            else:
                print_error("Must be a number.")
        elif validation_type == "disabled":
            if val in ['true', 'false']:
                return val == 'true'
            else:
                print_error("Must be 'true' or 'false'.")

def update_json_file(filename, data, mode, new_prio, new_dis, ids_to_update=None):
    """Modifies JSON data and saves."""
    print_loading("Updating file {} (locally)...".format(filename))
    updated_items = []
    update_successful = False
    
    try:
        if ids_to_update is None:
            if mode in ["priority", "both"] and new_prio is not None:
                data['priority'] = new_prio
            if mode in ["disabled", "both", "set_all_status"] and new_dis is not None:
                data['disabled'] = new_dis
            updated_items.append(data)
        else:
            ids_set = set(int(i) for i in ids_to_update)
            for directive in data.get("directives", []):
                dir_id = directive.get('id')
                item_updated = False
                if dir_id is not None and int(dir_id) in ids_set:
                    if mode in ["priority", "both"] and new_prio is not None:
                        directive['priority'] = new_prio
                        item_updated = True
                    if mode in ["both", "set_all_status"] and new_dis is not None:
                        directive['disabled'] = new_dis
                        item_updated = True
                    if item_updated:
                        updated_items.append(directive)
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        update_successful = True
        
        if updated_items:
            print_success("Update SUCCESSFUL for {} item(s).".format(len(updated_items)))
            if mode != "toggle_status":
                print("\n{}Check new values:{}".format(TColors.BOLD + TColors.CYAN, TColors.RESET))
                for item in updated_items:
                    print_data = {
                        "id": item.get('id'),
                        "name": item.get('name'),
                        "priority": item.get('priority'),
                        "disabled": item.get('disabled')
                    }
                    print("{}{}{}".format(TColors.DIM, json.dumps(print_data, indent=2, sort_keys=True), TColors.RESET))
        else:
            print_warning("No items were updated.")
    except Exception as e:
        print_error("Failed to update JSON: {}".format(e))
        update_successful = False
    
    return update_successful

def delete_directives_from_file(filename, data, ids_to_delete):
    """Deletes directives from file."""
    print_loading("Deleting directives from {}...".format(filename))
    delete_successful = False
    
    try:
        ids_set = set(int(i) for i in ids_to_delete)
        original_directives = data.get("directives", [])
        original_count = len(original_directives)
        new_directives_list = [d for d in original_directives if int(d.get('id', 0)) not in ids_set]
        data['directives'] = new_directives_list
        new_count = len(new_directives_list)
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        delete_successful = True
        
        print_success("Deletion SUCCESSFUL. {} directive(s) removed.".format(original_count - new_count))
    except Exception as e:
        print_error("Failed to delete directives: {}".format(e))
        delete_successful = False
    
    return delete_successful

def toggle_directives_status(filename, data, ids_to_toggle):
    """SWAP/TOGGLE disabled status."""
    print_loading("Toggling status (swap) for {} (locally)...".format(filename))
    toggled_summary = []
    ids_set = set(int(i) for i in ids_to_toggle)
    items_toggled_count = 0
    toggle_successful = False
    
    try:
        for directive in data.get("directives", []):
            dir_id = directive.get('id')
            if dir_id is not None and int(dir_id) in ids_set:
                current_status = directive.get('disabled', False)
                new_status = not current_status
                directive['disabled'] = new_status
                items_toggled_count += 1
                status_str = "{}PASSIVE{}".format(TColors.YELLOW, TColors.RESET) if new_status else "{}ACTIVE{}".format(TColors.GREEN, TColors.RESET)
                toggled_summary.append("  → Status for '{}' changed to {}".format(directive.get('name', 'N/A'), status_str))
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        toggle_successful = True
        
        print_success("Update SUCCESSFUL for {} item(s).".format(items_toggled_count))
        if toggled_summary:
            print("\n{}Change Details:{}".format(TColors.BOLD + TColors.CYAN, TColors.RESET))
            for line in toggled_summary:
                print(line)
    except Exception as e:
        print_error("Failed to update JSON (toggle): {}".format(e))
        toggle_successful = False
    
    return toggle_successful

def select_directives_from_file(data, show_az_options=False, initial_filter=None):
    """Displays directive list with pagination, filtering, A/Z options."""
    current_page = 1
    search_term = initial_filter if initial_filter else ""

    while True:
        print_header("SELECT DIRECTIVE(S)", "📋")
        
        if search_term:
            print("{}🔍 Filter active: '{}'{}".format(TColors.CYAN + TColors.BOLD, search_term, TColors.RESET))
        
        all_directives = data.get("directives", [])
        if not all_directives:
            print_warning("This file has no directives.")
            return ['back']

        # Filter
        if search_term:
            filtered_directives = [d for d in all_directives if search_term.lower() in d.get('name', '').lower()]
        else:
            filtered_directives = list(all_directives)

        if not filtered_directives:
            print_warning("No directives match filter '{}'.".format(search_term))
            total_items = 0
            display_directives_list = []
            max_num_on_page = 0
            total_pages = 1
        else:
            # Sort & Group
            sorted_directives = sorted(filtered_directives, key=lambda d: d.get('name', '').lower())
            enabled_directives = [d for d in sorted_directives if not d.get('disabled')]
            disabled_directives = [d for d in sorted_directives if d.get('disabled')]
            display_directives_list = enabled_directives + disabled_directives
            
            # Pagination
            total_items = len(display_directives_list)
            total_pages = (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
            if total_pages == 0:
                total_pages = 1
            current_page = max(1, min(current_page, total_pages))
            start_index = (current_page - 1) * ITEMS_PER_PAGE
            end_index = start_index + ITEMS_PER_PAGE
            page_items = display_directives_list[start_index:end_index]

            # Display Menu
            print("\n{}Page {} of {} {} Total: {} directives{}".format(
                TColors.BOLD + TColors.CYAN, current_page, total_pages,
                TColors.RESET + TColors.DIM + "│", total_items, TColors.RESET
            ))
            
            enabled_on_page = [d for d in page_items if not d.get('disabled')]
            disabled_on_page = [d for d in page_items if d.get('disabled')]

            if enabled_on_page:
                print("\n{}╭─ ACTIVE DIRECTIVES (Disabled: False) {}".format(
                    TColors.GREEN + TColors.BOLD, "─" * 42 + "╮" + TColors.RESET
                ))
                for idx, d in enumerate(enabled_on_page):
                    global_index = -1
                    try:
                        global_index = display_directives_list.index(d)
                    except ValueError:
                        pass
                    
                    display_number = global_index + 1 if global_index != -1 else '?'
                    id_str = "ID:{}".format(str(d.get('id', 'N/A')))
                    prio_str = "P:{}".format(str(d.get('priority', 'N/A')))
                    name_str = d.get('name', 'No Name')
                    
                    print("{}│{} {}[{:3d}]{} {}[{}]{} {}[{}]{} {}".format(
                        TColors.GREEN, TColors.RESET,
                        TColors.BOLD + TColors.WHITE, display_number, TColors.RESET,
                        TColors.CYAN, id_str, TColors.RESET,
                        TColors.YELLOW, prio_str, TColors.RESET,
                        name_str
                    ))
                print("{}╰{}╯{}".format(TColors.GREEN, "─" * 76, TColors.RESET))
            
            if disabled_on_page:
                print("\n{}╭─ INACTIVE DIRECTIVES (Disabled: True) {}".format(
                    TColors.RED + TColors.BOLD, "─" * 41 + "╮" + TColors.RESET
                ))
                for idx, d in enumerate(disabled_on_page):
                    global_index = -1
                    try:
                        global_index = display_directives_list.index(d)
                    except ValueError:
                        pass
                    
                    display_number = global_index + 1 if global_index != -1 else '?'
                    id_str = "ID:{}".format(str(d.get('id', 'N/A')))
                    prio_str = "P:{}".format(str(d.get('priority', 'N/A')))
                    name_str = d.get('name', 'No Name')
                    
                    print("{}│{} {}[{:3d}]{} {}[{}]{} {}[{}]{} {}{}".format(
                        TColors.YELLOW, TColors.RESET,
                        TColors.BOLD + TColors.WHITE, display_number, TColors.RESET,
                        TColors.CYAN, id_str, TColors.RESET,
                        TColors.YELLOW, prio_str, TColors.RESET,
                        TColors.DIM, name_str + TColors.RESET
                    ))
                print("{}╰{}╯{}".format(TColors.RED, "─" * 76, TColors.RESET))
            
            max_num_on_page = len(page_items)

        # Display Action Options
        print("\n{}{}{}".format(TColors.CYAN, "═" * 78, TColors.RESET))
        
        example_start = 1
        example_end = total_items
        prompt_parts = ["Select directive(s)"]
        prompt_parts.append("Examples: {}, {}-{}".format(example_start, example_start, example_end if example_end >= example_start else example_start))
        
        options = []
        if total_pages > 1:
            if current_page > 1:
                options.append("{}P{}=Prev".format(TColors.YELLOW, TColors.RESET))
            if current_page < total_pages:
                options.append("{}N{}=Next".format(TColors.YELLOW, TColors.RESET))
        
        options.append("{}F{}=Filter".format(TColors.CYAN, TColors.RESET))
        if search_term:
            options.append("{}C{}=Clear Filter".format(TColors.CYAN, TColors.RESET))
        
        if show_az_options:
            options.append("{}A{}=All Active".format(TColors.GREEN, TColors.RESET))
            options.append("{}Z{}=All Passive".format(TColors.YELLOW, TColors.RESET))
        
        options.append("{}B{}=Back".format(TColors.RED, TColors.RESET))
        
        print("{}Options: {}{}".format(TColors.DIM, " │ ".join(options), TColors.RESET))
        print("{}{}{}".format(TColors.CYAN, "═" * 78, TColors.RESET))
        
        selection_string = input("\n{}▸{} Your choice: ".format(TColors.BOLD, TColors.RESET))
        choice_lower = selection_string.strip().lower()

        # Handle Commands
        if choice_lower == 'b':
            print("\n{}Returning...{}".format(TColors.YELLOW, TColors.RESET))
            return ['back']
        elif choice_lower == 'n' and current_page < total_pages:
            current_page += 1
            continue
        elif choice_lower == 'p' and current_page > 1:
            current_page -= 1
            continue
        elif choice_lower == 'f':
            new_filter = input("{}▸{} Enter filter term: ".format(TColors.BOLD, TColors.RESET)).strip()
            search_term = new_filter
            current_page = 1
            continue
        elif choice_lower == 'c' and search_term:
            search_term = ""
            current_page = 1
            print_success("Filter cleared.")
            time.sleep(1)
            continue
        elif show_az_options and choice_lower == 'a':
            return ['set_all_active']
        elif show_az_options and choice_lower == 'z':
            return ['set_all_passive']
        elif not selection_string.strip():
            print_error("Input cannot be empty.")
            time.sleep(1.5)
            continue

        # Parse numbers/ranges
        parsed_indices_actual = parse_selection(selection_string, total_items, 1, total_items)
        selected_ids = [display_directives_list[idx].get('id') for idx in parsed_indices_actual if idx < len(display_directives_list) and display_directives_list[idx] is not None]

        if not selected_ids:
            print_error("No valid directives selected.")
            time.sleep(1.5)
            continue
        
        print_success("You selected {} directive(s).".format(len(selected_ids)))
        return selected_ids

def restart_pods(filename):
    """Restarts relevant pods."""
    print_header("RESTARTING PODS", "🔄")
    
    print_loading("Restarting {}...".format(POD_NAME))
    fe_output = run_command(["kubectl", "delete", "pod", POD_NAME])
    if fe_output:
        print("{}{}{}".format(TColors.DIM, fe_output, TColors.RESET))
    
    match = re.search(r'directives_(dsiem-backend-\d+)_', filename)
    if match:
        backend_pod_name = match.group(1)
        print_loading("Restarting {}...".format(backend_pod_name))
        be_output = run_command(["kubectl", "delete", "pod", backend_pod_name])
        if be_output:
            print("{}{}{}".format(TColors.DIM, be_output, TColors.RESET))
    else:
        print_warning("No specific backend pod found.")

def distribute_to_pod(filename):
    """Uploads file and optionally restarts pods."""
    global DISTRIBUTED_SUCCESSFULLY
    
    print_header("DISTRIBUTE TO POD", "📤")
    print_loading("Uploading {} to pod...".format(filename))
    
    cp_cmd = ["kubectl", "cp", "./{}".format(filename), "{}:{}{}".format(POD_NAME, REMOTE_PATH, filename)]
    upload_output = run_command(cp_cmd)
    
    if upload_output is None:
        print_error("Failed to upload.")
        return False
    
    if upload_output:
        print("{}{}{}".format(TColors.DIM, upload_output, TColors.RESET))
    
    print_success("Upload SUCCESSFUL.")
    
    confirm = input("\n{}▸{} Restart pods? (y/n): ".format(TColors.BOLD, TColors.RESET)).strip().lower()
    if confirm == 'y':
        restart_pods(filename)
    else:
        print_warning("Pods not restarted.")
    
    DISTRIBUTED_SUCCESSFULLY = True
    return True

def run_edit_session(filename, structure, initial_data, initial_filter=None):
    """Step 2: Main editing process."""
    global DISTRIBUTED_SUCCESSFULLY
    DISTRIBUTED_SUCCESSFULLY = False
    current_data = initial_data

    if structure == "single_object":
        print_header("EDIT FILE (Single Object)", "✏️")
        print("\n{}File: {}{}".format(TColors.CYAN, filename, TColors.RESET))
        
        current_prio = current_data.get('priority', 'N/A')
        current_dis = current_data.get('disabled', 'N/A')
        
        print("\n{}Current Values:{}".format(TColors.BOLD, TColors.RESET))
        print("  Priority: {}{}{}".format(TColors.YELLOW, current_prio, TColors.RESET))
        print("  Disabled: {}{}{}".format(TColors.YELLOW, current_dis, TColors.RESET))
        
        print("\n{}Enter New Values:{}".format(TColors.BOLD, TColors.RESET))
        new_prio = get_valid_input("New 'priority': ", "priority")
        new_dis = get_valid_input("New 'disabled' (true/false): ", "disabled")
        
        if update_json_file(filename, current_data, "both", new_prio, new_dis):
            confirm_dist = input("\n{}▸{} Distribute now? (y/n): ".format(TColors.BOLD, TColors.RESET)).strip().lower()
            if confirm_dist == 'y':
                distribute_to_pod(filename)
                confirm_back = input("\n{}▸{} Back to start file selection? (y/n): ".format(TColors.BOLD, TColors.RESET)).strip().lower()
                return confirm_back == 'y'
            else:
                confirm_back = input("\n{}▸{} Back to start file selection? (y/n): ".format(TColors.BOLD, TColors.RESET)).strip().lower()
                return confirm_back == 'y'
        else:
            input("\n{}Press Enter to continue...{}".format(TColors.DIM, TColors.RESET))
            return True

    else:  # Array structure
        while True:  # Main Menu Loop
            print_header("EDIT MENU - {}".format(filename), "✏️")
            
            print_menu("Select Action", [
                ("1", "Change Priority Only", TColors.CYAN),
                ("2", "Change Disabled Status (Toggle)", TColors.YELLOW),
                ("3", "Change Priority & Toggle Status", TColors.MAGENTA),
                ("4", "DELETE Directive(s)", TColors.RED)
            ], show_back=False, show_quit=False)
            
            print("\n{}{}{}".format(TColors.CYAN, "─" * 78, TColors.RESET))
            print("{}│{} [{}S{}] {}Save Locally & Exit/Back{}".format(
                TColors.CYAN, TColors.RESET,
                TColors.GREEN + TColors.BOLD, TColors.RESET,
                TColors.GREEN, TColors.RESET
            ))
            print("{}│{} [{}D{}] {}Distribute to Pod (Upload & Exit/Back){}".format(
                TColors.CYAN, TColors.RESET,
                TColors.GREEN + TColors.BOLD, TColors.RESET,
                TColors.GREEN + TColors.BOLD, TColors.RESET
            ))
            print("{}│{} [{}B{}] {}Back (to File Selection){}".format(
                TColors.CYAN, TColors.RESET,
                TColors.YELLOW + TColors.BOLD, TColors.RESET,
                TColors.YELLOW, TColors.RESET
            ))
            print("{}╰{}{}".format(TColors.CYAN, "─" * 77, TColors.RESET))
            
            aksi = input("\n{}▸{} Select Action [1-4, B, S, D]: ".format(TColors.BOLD, TColors.RESET)).strip().lower()

            if aksi == 'b':
                print_header("BACK CONFIRMATION", "⚠️")
                print("\n{}Changes in '{}' might not be distributed.{}".format(
                    TColors.YELLOW, filename, TColors.RESET
                ))
                
                print_menu("Choose Option", [
                    ("1", "Save Locally & Back", TColors.GREEN),
                    ("2", "Distribute & Back", TColors.GREEN),
                    ("3", "Discard Local Changes & Back", TColors.RED),
                    ("4", "Cancel (Stay)", TColors.CYAN)
                ], show_back=False, show_quit=False)
                
                back_choice = input("\n{}▸{} Choice [1-4]: ".format(TColors.BOLD, TColors.RESET)).strip()
                
                if back_choice == '1':
                    print_success("Changes saved locally.")
                    return True
                elif back_choice == '2':
                    if distribute_to_pod(filename):
                        input("\n{}Press Enter to continue...{}".format(TColors.DIM, TColors.RESET))
                        return True
                    else:
                        input("\n{}Press Enter to continue...{}".format(TColors.DIM, TColors.RESET))
                        continue
                elif back_choice == '3':
                    print_warning("Local changes discarded. Returning...")
                    return True
                else:
                    continue
                    
            elif aksi == 's':
                print_success("Done. File saved locally.")
                confirm_back_after_save = input("\n{}▸{} Back to start file selection? (y/n): ".format(TColors.BOLD, TColors.RESET)).strip().lower()
                return confirm_back_after_save == 'y'
                
            elif aksi == 'd':
                if distribute_to_pod(filename):
                    confirm_back_after_dist = input("\n{}▸{} Back to start file selection? (y/n): ".format(TColors.BOLD, TColors.RESET)).strip().lower()
                    return confirm_back_after_dist == 'y'
                else:
                    input("\n{}Press Enter to continue...{}".format(TColors.DIM, TColors.RESET))
                    continue

            edit_mode = ""
            show_az = False
            
            if aksi == '1':
                edit_mode = "priority"
            elif aksi == '2':
                edit_mode = "toggle_status"
                show_az = True
            elif aksi == '3':
                edit_mode = "priority_and_toggle"
            elif aksi == '4':
                edit_mode = "delete"
            else:
                print_error("Invalid choice.")
                time.sleep(1)
                continue

            # Enter Directive Selection Loop
            current_loop_filter = initial_filter
            initial_filter = None

            while True:
                selection_result = select_directives_from_file(current_data, show_az_options=show_az, initial_filter=current_loop_filter)
                current_loop_filter = None

                if not selection_result or selection_result[0] == 'back':
                    break
                elif selection_result[0] in ['input_empty', 'invalid_selection']:
                    continue
                elif selection_result[0] in ['set_all_active', 'set_all_passive']:
                    new_status = (selection_result[0] == 'set_all_passive')
                    status_str = "PASSIVE" if new_status else "ACTIVE"
                    
                    print_header("CONFIRMATION", "⚠️")
                    print("\n{}Set ALL directives in file to {}?{}".format(
                        TColors.YELLOW + TColors.BOLD, status_str, TColors.RESET
                    ))
                    
                    confirm = input("\n{}▸{} Are you sure? (y/n): ".format(TColors.BOLD, TColors.RESET)).strip().lower()
                    if confirm == 'y':
                        all_ids = [d.get('id') for d in current_data.get("directives", []) if d.get('id') is not None]
                        if all_ids:
                            update_json_file(filename, current_data, "set_all_status", None, new_status, ids_to_update=all_ids)
                        else:
                            print_warning("No directives to change.")
                    else:
                        print_warning("Cancelled.")
                    time.sleep(1.5)
                    continue
                else:  # User selected numbers/range
                    selected_ids = selection_result
                    action_successful = False
                    
                    if edit_mode == "toggle_status":
                        action_successful = toggle_directives_status(filename, current_data, selected_ids)
                    elif edit_mode == "delete":
                        print_header("CONFIRM DELETE", "⚠️")
                        print("\n{}Delete {} directive(s): {}{}".format(
                            TColors.RED + TColors.BOLD,
                            len(selected_ids),
                            ", ".join(str(s) for s in selected_ids),
                            TColors.RESET
                        ))
                        confirm = input("\n{}▸{} Are you sure? (y/n): ".format(TColors.BOLD, TColors.RESET)).strip().lower()
                        if confirm == 'y':
                            action_successful = delete_directives_from_file(filename, current_data, selected_ids)
                        else:
                            print_warning("Cancelled.")
                    elif edit_mode == "priority":
                        print("\n{}Enter New Value:{}".format(TColors.BOLD + TColors.CYAN, TColors.RESET))
                        new_prio = get_valid_input("New 'priority': ", "priority")
                        action_successful = update_json_file(filename, current_data, "priority", new_prio, None, ids_to_update=selected_ids)
                    elif edit_mode == "priority_and_toggle":
                        print("\n{}Enter New Value:{}".format(TColors.BOLD + TColors.CYAN, TColors.RESET))
                        new_prio = get_valid_input("New 'priority': ", "priority")
                        if update_json_file(filename, current_data, "priority", new_prio, None, ids_to_update=selected_ids):
                            action_successful = toggle_directives_status(filename, current_data, selected_ids)
                        else:
                            print_error("Priority update failed, toggle skipped.")

                    if selected_ids:
                        time.sleep(1.5)
                    if action_successful is not False:
                        continue
                    else:
                        print_error("Action failed.")
                        input("\n{}Press Enter to continue...{}".format(TColors.DIM, TColors.RESET))
                        break
        return False

def show_upload_instructions(filename):
    """Displays final manual upload command."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    relative_path = os.path.join(os.path.basename(LOCAL_DIR), filename)

    print_header("DONE - SAVED LOCALLY", "✓")
    print_success("File {} edited.".format(os.path.join(LOCAL_DIR, filename)))
    
    print("\n{}Manual upload command (from script directory):{}".format(
        TColors.BOLD + TColors.CYAN, TColors.RESET
    ))
    print("\n{}{}kubectl cp {} {}:{}{}{}".format(
        TColors.YELLOW + TColors.BOLD,
        " " * 2,
        relative_path, POD_NAME, REMOTE_PATH, filename,
        TColors.RESET
    ))
    print("")
    os.chdir(script_dir)

def main():
    """Main execution function."""
    original_dir = os.getcwd()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    try:
        check_deps()
        
        while True:
            selected_file, initial_filter = setup_and_select_file()
            if selected_file:
                structure, data = check_file_structure(selected_file)
                if not structure:
                    print_error("Failed to read/parse.")
                    input("\n{}Press Enter to continue...{}".format(TColors.DIM, TColors.RESET))
                    continue

                should_continue = run_edit_session(selected_file, structure, data, initial_filter)

                if not DISTRIBUTED_SUCCESSFULLY and not should_continue:
                    show_upload_instructions(selected_file)
                    break
                elif DISTRIBUTED_SUCCESSFULLY and not should_continue:
                    os.chdir(script_dir)
                    break
                elif should_continue:
                    continue
                else:
                    os.chdir(script_dir)
                    break
            else:
                print("\n{}No file selected. Exiting...{}".format(TColors.YELLOW, TColors.RESET))
                os.chdir(original_dir)
                break
                
    except KeyboardInterrupt:
        print("\n\n{}Cancelled by user.{}".format(TColors.YELLOW, TColors.RESET))
        os.chdir(original_dir)
        sys.exit(0)
    except Exception as e:
        print_error("Unexpected error: {}".format(e))
        import traceback
        traceback.print_exc()
        try:
            os.chdir(original_dir)
        except OSError:
            pass
        sys.exit(1)

if __name__ == "__main__":
    main()