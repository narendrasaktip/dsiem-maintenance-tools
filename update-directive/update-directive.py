#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Make print() behave the same in Py2 and Py3
from __future__ import print_function
import os
import sys
import subprocess
import json
import re
import collections # <-- For OrderedDict
import time         # <-- For brief pauses

# --- Config File Handling ---
try:
    # Python 3
    import configparser
except ImportError:
    # Python 2
    import ConfigParser as configparser

CONFIG_FILE = 'config.ini'

def load_config():
    """Loads configuration from config.ini."""
    config = configparser.ConfigParser()
    defaults = {
        'Kubernetes': { 'PodName': 'dsiem-frontend-0', 'RemotePath': '/dsiem/configs/', 'Namespace': '' },
        'Paths': { 'LocalDir': 'dsiem_configs_edited', 'FilePattern': 'directives_*.json' },
        'Display': { 'ItemsPerPage': '20' }
    }
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, CONFIG_FILE)

    if not os.path.exists(config_path):
        print("[INFO] Config file '{}' not found. Creating default.".format(config_path))
        try:
            if hasattr(config, 'read_dict'): config.read_dict(defaults) # Py3
            else: # Py2
                 config = configparser.ConfigParser();
                 for section, options in defaults.items():
                     try: config.add_section(section)
                     except configparser.DuplicateSectionError: pass
                     for key, value in options.items(): config.set(section, key, value)
            with open(config_path, 'w') as cf: config.write(cf)
        except Exception as e:
             print("[ERROR] Failed to create default config '{}': {}".format(config_path, e))
             return { # Fallback
                 'pod_name': defaults['Kubernetes']['PodName'], 'remote_path': defaults['Kubernetes']['RemotePath'],
                 'namespace': defaults['Kubernetes']['Namespace'], 'local_dir': os.path.join(script_dir, defaults['Paths']['LocalDir']),
                 'file_pattern': defaults['Paths']['FilePattern'], 'items_per_page': int(defaults['Display']['ItemsPerPage'])
             }
    config.read(config_path)
    def get_conf(section, key):
        try:
             val = config.get(section, key)
             if section == 'Display' and key == 'ItemsPerPage':
                 try: return int(val)
                 except ValueError: print("[WARN] Invalid 'ItemsPerPage'. Using 20."); return 20
             return val
        except (configparser.NoSectionError, configparser.NoOptionError):
            default_val = defaults.get(section, {}).get(key, ''); print("[WARN] Missing '{}/{}'. Using default.".format(section, key))
            if section == 'Display' and key == 'ItemsPerPage':
                 try: return int(default_val)
                 except ValueError: return 20
            return default_val
        except Exception as e:
            default_val = defaults.get(section, {}).get(key, ''); print("[ERROR] Reading config '{}/{}': {}".format(section, key, e))
            if section == 'Display' and key == 'ItemsPerPage':
                 try: return int(default_val)
                 except ValueError: return 20
            return default_val
    local_dir_name = get_conf('Paths', 'LocalDir'); absolute_local_dir = os.path.join(script_dir, local_dir_name)
    return {
        'pod_name': get_conf('Kubernetes', 'PodName'), 'remote_path': get_conf('Kubernetes', 'RemotePath'),
        'namespace': get_conf('Kubernetes', 'Namespace'), 'local_dir': absolute_local_dir,
        'file_pattern': get_conf('Paths', 'FilePattern'), 'items_per_page': get_conf('Display', 'ItemsPerPage')
    }

CONFIG = load_config(); POD_NAME = CONFIG['pod_name']; REMOTE_PATH = CONFIG['remote_path']; NAMESPACE = CONFIG['namespace']
LOCAL_DIR = CONFIG['local_dir']; FILE_PATTERN = CONFIG['file_pattern']; ITEMS_PER_PAGE = CONFIG['items_per_page']
# ------------------------------

DISTRIBUTED_SUCCESSFULLY = False
try: input = raw_input
except NameError: pass

class TColors:
    BOLD='\033[1m'; GREEN='\033[92m'; YELLOW='\033[93m'; RED='\033[91m'; CYAN='\033[96m'; RESET='\033[0m'
    if not os.popen('tput sgr0 2>/dev/null').read(): BOLD=GREEN=YELLOW=RED=CYAN=RESET=""

def cprint(text, color=None, bold=False):
    pre="";
    if bold: pre += TColors.BOLD
    if color: pre += color
    print("{}{}{}".format(pre, text, TColors.RESET))

def clear_screen(): os.system('clear')

def check_deps():
    try:
        with open(os.devnull, 'w') as FNULL:
            cmd = ["kubectl"];
            if NAMESPACE: cmd.extend(["-n", NAMESPACE])
            cmd.extend(["version", "--client"]); subprocess.Popen(cmd, stdout=FNULL, stderr=FNULL).wait()
    except OSError as e: cprint("ERROR: 'kubectl' not found.", TColors.RED, bold=True); sys.exit(1)

def run_command(cmd_list, check_stderr=False):
    try:
        full_cmd = list(cmd_list);
        if cmd_list[0] == "kubectl" and NAMESPACE: full_cmd.insert(1, "-n"); full_cmd.insert(2, NAMESPACE)
        process = subprocess.Popen(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout_bytes, stderr_bytes = process.communicate(); stdout_str = stdout_bytes.decode('utf-8').strip(); stderr_str = stderr_bytes.decode('utf-8').strip()
        if process.returncode != 0:
             if not (stderr_str.startswith("Defaulting container name") or "pod default value" in stderr_str):
                 cprint("Error running: {}".format(' '.join(full_cmd)), TColors.RED, bold=True)
                 if stdout_str: cprint("Stdout: {}".format(stdout_str), TColors.RED)
                 if stderr_str: cprint("Stderr: {}".format(stderr_str), TColors.RED)
                 return None
        if check_stderr: return stdout_str, stderr_str
        else: return stdout_str
    except OSError as e: cprint("Error: Cmd '{}' not found.".format(full_cmd[0]), TColors.RED, bold=True); return None
    except Exception as e: cprint("Unexpected error: {}".format(e), TColors.RED, bold=True); return None

def parse_selection(input_str, max_total_items, current_page=1, items_per_page=20):
    indices = set(); input_str = input_str.strip().lower(); parts = re.split(r'[\s,]+', input_str)
    page_start_index = (current_page - 1) * items_per_page
    for part in parts:
        if not part: continue
        match = re.match(r'^(\d+)-(\d+)$', part)
        if match:
            start_disp, end_disp = int(match.group(1)), int(match.group(2))
            if start_disp > end_disp: start_disp, end_disp = end_disp, start_disp
            start_idx = start_disp - 1; end_idx = end_disp - 1 # Display number = global index
            if 0 <= start_idx < max_total_items and 0 <= end_idx < max_total_items:
                 for i_idx in range(start_idx, end_idx + 1): indices.add(i_idx)
            else: cprint("Warn: Range '{}-{}' out of bounds (max {}).".format(start_disp, end_disp, max_total_items), TColors.YELLOW)
        elif part.isdigit():
            i_disp = int(part); i_idx = i_disp - 1 # Display number = global index
            if 0 <= i_idx < max_total_items: indices.add(i_idx)
            else: cprint("Warn: Choice '{}' out of bounds (max {}).".format(i_disp, max_total_items), TColors.YELLOW)
        else:
            if part not in ['n', 'p', 'a', 'z', 'b', 'f', 'c']: cprint("Warn: Input '{}' invalid.".format(part), TColors.YELLOW)
    return sorted(list(indices))

def get_status_filter():
    """Asks user for the desired status filter."""
    while True:
        cprint("\nSearch within which status?", TColors.CYAN)
        print("  1) Active (Disabled: False)"); print("  2) Passive (Disabled: True)"); print("  3) Both")
        choice = input("Select status [1, 2, 3]: ").strip()
        if choice == '1': return False
        elif choice == '2': return True
        elif choice == '3': return None
        else: cprint("Invalid choice.", TColors.RED)

def check_directive_status(directive_data, desired_disabled_status):
    """Checks if a directive's status matches the filter."""
    if desired_disabled_status is None: return True
    return directive_data.get('disabled', False) == desired_disabled_status

def search_and_select_file():
    """Searches files in pod by directive name and status. Returns (filename, search_term) or (None, None)."""
    while True:
        clear_screen(); cprint("--- STEP 1: SEARCH FILE BY DIRECTIVE NAME ---", bold=True)
        search_term_input = input("Enter directive name (or part) to search (or B to Back): ").strip()
        if not search_term_input: cprint("Search term empty.", TColors.RED); time.sleep(1.5); continue
        if search_term_input.lower() == 'b': return None, None # Return None for both

        desired_status = get_status_filter(); status_desc = "any status"
        if desired_status is False: status_desc = "Active"
        elif desired_status is True: status_desc = "Passive"

        cprint("\nSearching '{}' files for '{}' (Status: {})...".format(FILE_PATTERN, search_term_input, status_desc), TColors.YELLOW)
        find_cmd = ["kubectl", "exec", POD_NAME, "--", "find", REMOTE_PATH, "-maxdepth", "1", "-name", FILE_PATTERN]
        file_list_raw = run_command(find_cmd)
        if file_list_raw is None or not file_list_raw.strip(): cprint("ERROR: Could not list files.", TColors.RED); time.sleep(2); return None, None
        all_lines=file_list_raw.split('\n'); valid_paths = [f for f in all_lines if f.startswith(REMOTE_PATH) and f.endswith('.json')]
        if not valid_paths: cprint("ERROR: No valid JSON files.", TColors.RED); time.sleep(2); return None, None

        matching_files = []
        for file_path in valid_paths:
            filename = os.path.basename(file_path); cat_cmd = ["kubectl", "exec", POD_NAME, "--", "cat", file_path]
            file_content_str = run_command(cat_cmd)
            if file_content_str is None: cprint("Warn: Could not read {}. Skip.".format(filename), TColors.YELLOW); continue
            try:
                data = json.loads(file_content_str, object_pairs_hook=collections.OrderedDict); found_in_file = False
                if "directives" in data and isinstance(data.get("directives"), list):
                    for directive in data.get("directives", []):
                        if check_directive_status(directive, desired_status) and search_term_input.lower() in directive.get('name', '').lower(): found_in_file = True; break
                else: # Single object
                    if check_directive_status(data, desired_status) and search_term_input.lower() in data.get('name', '').lower(): found_in_file = True
                if found_in_file: matching_files.append(filename)
            except (ValueError, json.JSONDecodeError): cprint("Warn: File {} not valid JSON. Skip.".format(filename), TColors.YELLOW); continue

        if not matching_files: cprint("\nNo file found matching '{}' (Status: {}).".format(search_term_input, status_desc), TColors.RED); input("Press Enter..."); continue
        elif len(matching_files) == 1:
            selected_file = matching_files[0]; cprint("\nFound match: {}".format(selected_file), TColors.GREEN)
            print("\nCopying {}...".format(selected_file)); cp_cmd = ["kubectl", "cp", "{}:{}{}".format(POD_NAME, REMOTE_PATH, selected_file), "./{}".format(selected_file)]
            if run_command(cp_cmd) is None: cprint("Failed to copy.", TColors.RED); time.sleep(2); return None, None
            print("Copied successfully.")
            return selected_file, search_term_input
        else:
            cprint("\nMultiple matches:", TColors.YELLOW); matching_files.sort()
            for i, fname in enumerate(matching_files, 1): print("  {:2d}) {}".format(i, fname))
            print("   B) Back")
            while True:
                try:
                    choice = input("Select file: ").strip()
                    if choice.lower() == 'b': return None, None
                    choice_idx = int(choice) - 1
                    if 0 <= choice_idx < len(matching_files):
                        selected_file = matching_files[choice_idx]; cprint("Selected: {}".format(selected_file), TColors.GREEN)
                        print("\nCopying {}...".format(selected_file)); cp_cmd = ["kubectl", "cp", "{}:{}{}".format(POD_NAME, REMOTE_PATH, selected_file), "./{}".format(selected_file)]
                        if run_command(cp_cmd) is None: cprint("Failed to copy.", TColors.RED); time.sleep(2); return None, None
                        print("Copied successfully.")
                        return selected_file, search_term_input
                    else: cprint("Invalid choice.", TColors.RED)
                except ValueError: cprint("Invalid input.", TColors.RED)


def setup_and_select_file():
    """Step 1: Offer choice, then select file. Returns (filename, initial_filter)"""
    while True:
        clear_screen(); cprint("--- Directive Configuration Editor ---", TColors.GREEN, bold=True)
        print("Working directory: {}{}{}".format(TColors.BOLD, LOCAL_DIR, TColors.RESET))
        print("Changes saved locally until distributed.")
        print("")
        if not os.path.isdir(LOCAL_DIR):
            try: os.makedirs(LOCAL_DIR)
            except OSError as e: cprint("Failed to create dir {}: {}".format(LOCAL_DIR, e), TColors.RED); sys.exit(1)
        try: os.chdir(LOCAL_DIR)
        except OSError as e: cprint("Failed to cd into {}: {}".format(LOCAL_DIR, e), TColors.RED); script_dir=os.path.dirname(os.path.abspath(__file__)); os.chdir(script_dir); sys.exit(1)

        cprint("--- STEP 1: SELECT FILE ---", bold=True); cprint("How to select file?", TColors.CYAN)
        print("  1) Select from list"); print("  2) Search by directive name"); print("  Q) Quit")
        method_choice = input("Select method [1, 2, Q]: ").strip().lower()

        initial_filter = None # Default

        if method_choice == '1':
            print("\nFetching list..."); cmd = ["kubectl", "exec", POD_NAME, "--", "find", REMOTE_PATH, "-maxdepth", "1", "-name", FILE_PATTERN]
            file_list_raw = run_command(cmd)
            if file_list_raw is None or not file_list_raw.strip(): cprint("ERROR: No files found.", TColors.RED); input("Press Enter..."); continue
            all_lines=file_list_raw.split('\n'); valid_paths = [f for f in all_lines if f.startswith(REMOTE_PATH) and f.endswith('.json')]
            if not valid_paths: cprint("ERROR: No valid JSON files.", TColors.RED); input("Press Enter..."); continue
            file_names = [os.path.basename(f) for f in valid_paths]; file_names.sort()

            print("");
            for i, filename in enumerate(file_names, 1): print("  {:2d}) {}".format(i, filename))
            print("   B) Back")
            while True:
                try:
                    choice = input("Select file number: ").strip()
                    if choice.lower() == 'b': break
                    choice_idx = int(choice) - 1
                    if 0 <= choice_idx < len(file_names):
                        selected_file = file_names[choice_idx]; cprint("Selected: {}".format(selected_file), TColors.GREEN)
                        print("\nCopying {}...".format(selected_file)); cp_cmd = ["kubectl", "cp", "{}:{}{}".format(POD_NAME, REMOTE_PATH, selected_file), "./{}".format(selected_file)]
                        if run_command(cp_cmd) is None: cprint("Failed to copy.", TColors.RED); input("Press Enter..."); break
                        print("Copied to {}".format(LOCAL_DIR)); return selected_file, initial_filter # SUCCESS
                    else: cprint("Invalid choice.", TColors.RED)
                except ValueError: cprint("Invalid input.", TColors.RED)
            continue
        elif method_choice == '2':
            selected_file, initial_filter = search_and_select_file()
            if selected_file: print("Copied to {}".format(LOCAL_DIR)); return selected_file, initial_filter # SUCCESS
            else: continue
        elif method_choice == 'q':
            print("Exiting."); script_dir = os.path.dirname(os.path.abspath(__file__)); os.chdir(script_dir); sys.exit(0)
        else: cprint("Invalid selection.", TColors.RED); time.sleep(1.5); continue


def check_file_structure(filename):
    """Reads JSON, determines structure, uses OrderedDict."""
    try:
        with open(filename, 'r') as f: data = json.load(f, object_pairs_hook=collections.OrderedDict)
        if "directives" in data and isinstance(data.get("directives"), list): return "array", data
        else: return "single_object", data
    except (ValueError, json.JSONDecodeError): cprint("ERROR: File {} not valid JSON.".format(filename), TColors.RED, bold=True); return None, None
    except IOError as e: cprint("ERROR: Failed to read file {}: {}".format(filename, e), TColors.RED, bold=True); return None, None

def get_valid_input(prompt, validation_type):
    """Requests valid input."""
    while True:
        val = input(prompt).strip().lower()
        if validation_type == "priority":
            if val.isdigit(): return int(val)
            else: cprint("Error: Must be a number.", TColors.RED)
        elif validation_type == "disabled":
            if val in ['true', 'false']: return val == 'true'
            else: cprint("Error: Must be 'true' or 'false'.", TColors.RED)

def update_json_file(filename, data, mode, new_prio, new_dis, ids_to_update=None):
    """Modifies JSON data and saves."""
    cprint("\nUpdating file {} (locally)...".format(filename), TColors.YELLOW)
    updated_items = []; update_successful = False
    try:
        if ids_to_update is None: # Single Object
            if mode in ["priority", "both"] and new_prio is not None: data['priority'] = new_prio
            if mode in ["disabled", "both", "set_all_status"] and new_dis is not None: data['disabled'] = new_dis
            updated_items.append(data)
        else: # Array
            ids_set = set(int(i) for i in ids_to_update)
            for directive in data.get("directives", []):
                dir_id = directive.get('id'); item_updated = False
                if dir_id is not None and int(dir_id) in ids_set:
                    if mode in ["priority", "both"] and new_prio is not None: directive['priority'] = new_prio; item_updated = True
                    if mode in ["both", "set_all_status"] and new_dis is not None: directive['disabled'] = new_dis; item_updated = True
                    if item_updated: updated_items.append(directive)
        with open(filename, 'w') as f: json.dump(data, f, indent=4)
        update_successful = True
        if updated_items:
            cprint("Update SUCCESSFUL for {} item(s).".format(len(updated_items)), TColors.GREEN, bold=True)
            if mode != "toggle_status":
                cprint("Check new values:", bold=True)
                for item in updated_items:
                    print_data = {"id": item.get('id'), "name": item.get('name'), "priority": item.get('priority'), "disabled": item.get('disabled')}
                    print(json.dumps(print_data, indent=2, sort_keys=True))
        else: cprint("No items were updated.", TColors.YELLOW)
    except Exception as e: cprint("ERROR: Failed to update JSON: {}".format(e), TColors.RED, bold=True); update_successful = False
    return update_successful

def delete_directives_from_file(filename, data, ids_to_delete):
    """Deletes directives from file."""
    cprint("\nDeleting directives from {}...".format(filename), TColors.RED)
    delete_successful = False
    try:
        ids_set = set(int(i) for i in ids_to_delete); original_directives = data.get("directives", []); original_count = len(original_directives)
        new_directives_list = [d for d in original_directives if int(d.get('id', 0)) not in ids_set]
        data['directives'] = new_directives_list; new_count = len(new_directives_list)
        with open(filename, 'w') as f: json.dump(data, f, indent=4)
        delete_successful = True
        cprint("Deletion SUCCESSFUL. {} directive(s) removed.".format(original_count - new_count), TColors.GREEN, bold=True)
    except Exception as e: cprint("ERROR: Failed to delete directives: {}".format(e), TColors.RED, bold=True); delete_successful = False
    return delete_successful

def toggle_directives_status(filename, data, ids_to_toggle):
    """SWAP/TOGGLE disabled status."""
    cprint("\nToggling status (swap) for {} (locally)...".format(filename), TColors.YELLOW)
    toggled_summary = []; ids_set = set(int(i) for i in ids_to_toggle); items_toggled_count = 0; toggle_successful = False
    try:
        for directive in data.get("directives", []):
            dir_id = directive.get('id')
            if dir_id is not None and int(dir_id) in ids_set:
                current_status = directive.get('disabled', False); new_status = not current_status
                directive['disabled'] = new_status; items_toggled_count += 1
                status_str = "--- [ Passive ] ---" if new_status else "+++ [ Active ] +++"
                toggled_summary.append("-> Status for '{}' changed to {}.".format(directive.get('name', 'N/A'), status_str))
        with open(filename, 'w') as f: json.dump(data, f, indent=4)
        toggle_successful = True
        cprint("Update SUCCESSFUL for {} item(s).".format(items_toggled_count), TColors.GREEN, bold=True)
        if toggled_summary:
            cprint("Change Details:", bold=True)
            for line in toggled_summary: print(line)
    except Exception as e: cprint("ERROR: Failed to update JSON (toggle): {}".format(e), TColors.RED, bold=True); toggle_successful = False
    return toggle_successful

def select_directives_from_file(data, show_az_options=False, initial_filter=None):
    """Displays directive list with pagination, filtering, A/Z options."""
    current_page = 1
    search_term = initial_filter if initial_filter else ""

    while True:
        clear_screen(); cprint("--- SELECT DIRECTIVE(S) ---", bold=True)
        if search_term: cprint("Filter active: '{}'".format(search_term), TColors.CYAN)
        all_directives = data.get("directives", []);
        if not all_directives: cprint("This file has no directives.", TColors.YELLOW); return ['back']

        # 1. Filter
        if search_term: filtered_directives = [d for d in all_directives if search_term.lower() in d.get('name', '').lower()]
        else: filtered_directives = list(all_directives)

        if not filtered_directives:
             cprint("No directives match filter '{}'.".format(search_term), TColors.YELLOW)
             total_items = 0; display_directives_list = []; max_num_on_page = 0; total_pages = 1
        else:
            # 2. Sort & 3. Group
            sorted_directives = sorted(filtered_directives, key=lambda d: d.get('name', '').lower())
            enabled_directives = [d for d in sorted_directives if not d.get('disabled')]
            disabled_directives = [d for d in sorted_directives if d.get('disabled')]
            display_directives_list = enabled_directives + disabled_directives
            # 4. Pagination
            total_items = len(display_directives_list); total_pages = (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
            if total_pages == 0: total_pages = 1
            current_page = max(1, min(current_page, total_pages)); start_index = (current_page - 1) * ITEMS_PER_PAGE
            end_index = start_index + ITEMS_PER_PAGE; page_items = display_directives_list[start_index:end_index]

            # --- Display Menu ---
            cprint("\nPage {} of {}".format(current_page, total_pages), bold=True)
            enabled_on_page = [d for d in page_items if not d.get('disabled')]
            disabled_on_page = [d for d in page_items if d.get('disabled')]

            if enabled_on_page:
                 cprint("\n--- ACTIVE (Disabled: False) ---", TColors.GREEN, bold=True)
                 for idx, d in enumerate(enabled_on_page):
                     # --- FIX v28: Calculate global_index correctly ---
                     global_index = -1
                     try:
                         # Find index in the *full* filtered+sorted list
                         global_index = display_directives_list.index(d)
                     except ValueError: pass # Should not happen

                     display_number = global_index + 1 if global_index != -1 else '?'
                     id_str = "[ID: {}]".format(str(d.get('id', 'N/A'))); prio_str = "[Priority: {}]".format(str(d.get('priority', 'N/A')))
                     name_str = d.get('name', 'No Name'); print("  {:3d}) {} {} {}".format(display_number, id_str, prio_str, name_str))
            if disabled_on_page:
                 cprint("\n--- INACTIVE (Disabled: True) ---", TColors.YELLOW, bold=True)
                 for idx, d in enumerate(disabled_on_page):
                     # --- FIX v28: Calculate global_index correctly ---
                     global_index = -1
                     try:
                         # Find index in the *full* filtered+sorted list
                         global_index = display_directives_list.index(d)
                     except ValueError: pass

                     display_number = global_index + 1 if global_index != -1 else '?'
                     id_str = "[ID: {}]".format(str(d.get('id', 'N/A'))); prio_str = "[Priority: {}]".format(str(d.get('priority', 'N/A')))
                     name_str = d.get('name', 'No Name'); print("  {:3d}) {} {} {}".format(display_number, id_str, prio_str, name_str))
            max_num_on_page = len(page_items)


        # --- Display Action Options ---
        print("\n" + "-"*70);
        example_start = 1; example_end = total_items
        prompt = "Select (# e.g., {}, {}-{}, ".format(example_start, example_start, example_end if example_end >= example_start else example_start)
        options = [];
        if total_pages > 1:
            if current_page > 1: options.append("P=Prev")
            if current_page < total_pages: options.append("N=Next")
        options.append("F=Filter");
        if search_term: options.append("C=Clear Filter")
        if show_az_options: options.append("A=All Active"); options.append("Z=All Passive"); prompt += " A, Z,"
        options.append("B=Back"); prompt += " {})".format(", ".join(options))

        # Request input
        selection_string = input("{}{}{}".format(TColors.BOLD, prompt + ": ", TColors.RESET))
        choice_lower = selection_string.strip().lower()

        # Handle Commands
        if choice_lower == 'b': print("Returning..."); return ['back']
        elif choice_lower == 'n' and current_page < total_pages: current_page += 1; continue
        elif choice_lower == 'p' and current_page > 1: current_page -= 1; continue
        elif choice_lower == 'f': new_filter = input("Filter term: ").strip(); search_term = new_filter; current_page = 1; continue
        elif choice_lower == 'c' and search_term: search_term = ""; current_page = 1; cprint("Filter cleared.", TColors.CYAN); time.sleep(1); continue
        elif show_az_options and choice_lower == 'a': return ['set_all_active']
        elif show_az_options and choice_lower == 'z': return ['set_all_passive']
        elif not selection_string.strip(): cprint("Input cannot be empty.", TColors.RED); time.sleep(1.5); continue

        # Parse numbers/ranges (relative to GLOBAL indices)
        parsed_indices_actual = parse_selection(selection_string, total_items, 1, total_items) # Use total_items from filtered list
        selected_ids = [display_directives_list[idx].get('id') for idx in parsed_indices_actual if idx < len(display_directives_list) and display_directives_list[idx] is not None]

        if not selected_ids: cprint("No valid directives selected.", TColors.RED); time.sleep(1.5); continue
        cprint("You selected {} directive(s).".format(len(selected_ids)), TColors.GREEN); return selected_ids


def restart_pods(filename):
    """Restarts relevant pods."""
    cprint("\n--- RESTARTING PODS ---", TColors.YELLOW, bold=True)
    cprint("Restarting {}...".format(POD_NAME), TColors.YELLOW); fe_output = run_command(["kubectl", "delete", "pod", POD_NAME]); print(fe_output)
    match = re.search(r'directives_(dsiem-backend-\d+)_', filename)
    if match:
        backend_pod_name = match.group(1); cprint("Restarting {}...".format(backend_pod_name), TColors.YELLOW)
        be_output = run_command(["kubectl", "delete", "pod", backend_pod_name]); print(be_output)
    else: cprint("No specific backend pod found.", TColors.YELLOW)

def distribute_to_pod(filename):
    """Uploads file and optionally restarts pods."""
    global DISTRIBUTED_SUCCESSFULLY
    clear_screen(); cprint("--- STEP 3: DISTRIBUTE TO POD ---", TColors.GREEN, bold=True)
    cprint("Uploading {} to pod...".format(filename), TColors.YELLOW)
    cp_cmd = ["kubectl", "cp", "./{}".format(filename), "{}:{}{}".format(POD_NAME, REMOTE_PATH, filename)]
    upload_output = run_command(cp_cmd)
    if upload_output is None: cprint("ERROR: Failed to upload.", TColors.RED, bold=True); return False
    print(upload_output); cprint("Upload SUCCESSFUL.", TColors.GREEN)
    confirm = input("\nRestart pods? (y/n): ").strip().lower()
    if confirm == 'y': restart_pods(filename)
    else: cprint("Pods not restarted.", TColors.YELLOW)
    DISTRIBUTED_SUCCESSFULLY = True;
    return True

# --- Updated v27/v28: Refactored run_edit_session, pass initial_filter ---
def run_edit_session(filename, structure, initial_data, initial_filter=None): # Accept initial data
    """Step 2: Main editing process. Returns True if user wants to go back."""
    global DISTRIBUTED_SUCCESSFULLY
    DISTRIBUTED_SUCCESSFULLY = False
    current_data = initial_data # Use the data passed in (loaded once)

    if structure == "single_object":
        # ... (Single object logic remains the same) ...
        clear_screen(); cprint("--- STEP 2: EDIT FILE (Single Object) ---", bold=True); cprint("File: {}".format(filename))
        current_prio = current_data.get('priority', 'N/A'); current_dis = current_data.get('disabled', 'N/A')
        print("Current: Priority={}, Disabled={}".format(current_prio, current_dis))
        new_prio = get_valid_input("New 'priority': ", "priority"); new_dis = get_valid_input("New 'disabled' (true/false): ", "disabled")
        if update_json_file(filename, current_data, "both", new_prio, new_dis):
            confirm_dist = input("\nDistribute now? (y/n): ").strip().lower()
            if confirm_dist == 'y':
                distribute_to_pod(filename)
                confirm_back = input("\nBack to start file selection? (y/n): ").strip().lower()
                return confirm_back == 'y'
            else:
                confirm_back = input("\nBack to start file selection? (y/n): ").strip().lower()
                return confirm_back == 'y'
        else: input("Update failed. Press Enter..."); return True

    else: # Array structure
        while True: # Main Menu Loop
            clear_screen();
            # --- OPTIMIZATION v27: Use current_data in memory ---
            # structure, current_data = check_file_structure(filename) # No longer needed here
            # if not structure: cprint("Read error.", TColors.RED); time.sleep(2); return True

            cprint("--- STEP 2: EDIT MENU ({}) ---".format(filename), bold=True); print("Action:")
            cprint("  1) Change Priority Only", TColors.CYAN); cprint("  2) Toggle Status", TColors.CYAN)
            cprint("  3) Change Priority & Toggle", TColors.CYAN); cprint("  4) DELETE Directive(s)", TColors.RED, bold=True); print("")
            cprint("  B) Back (to File Selection)", bold=True)
            cprint("  S) Done (Save Locally & Exit/Back)", bold=True)
            cprint("  D) Distribute to Pod (Upload & Exit/Back)", TColors.GREEN, bold=True)
            aksi = input("Select Action [1-4, B, S, D]: ").strip().lower()

            if aksi == 'b':
                clear_screen(); cprint("--- Back to File Selection ---", TColors.YELLOW, bold=True); print("Changes in '{}' might not be distributed.".format(filename))
                cprint("  1) Save Locally & Back", TColors.GREEN); cprint("  2) Distribute & Back", TColors.GREEN)
                cprint("  3) Discard Local Changes & Back", TColors.RED); cprint("  4) Cancel (Stay)", TColors.CYAN)
                back_choice = input("Choice [1-4]: ").strip()
                if back_choice == '1': cprint("Changes saved locally.", TColors.GREEN); return True
                elif back_choice == '2':
                    if distribute_to_pod(filename): input("\nPress Enter..."); return True
                    else: input("Distribution failed. Press Enter..."); continue
                elif back_choice == '3': cprint("Local changes discarded. Returning...", TColors.YELLOW); return True
                else: continue
            elif aksi == 's':
                cprint("Done. File saved locally.", TColors.GREEN)
                confirm_back_after_save = input("\nBack to start file selection? (y/n): ").strip().lower()
                return confirm_back_after_save == 'y'
            elif aksi == 'd':
                if distribute_to_pod(filename):
                    confirm_back_after_dist = input("\nBack to start file selection? (y/n): ").strip().lower()
                    return confirm_back_after_dist == 'y'
                else: input("Distribution failed. Press Enter..."); continue

            edit_mode = ""; show_az = False
            if aksi == '1': edit_mode = "priority"
            elif aksi == '2': edit_mode = "toggle_status"; show_az = True
            elif aksi == '3': edit_mode = "priority_and_toggle"
            elif aksi == '4': edit_mode = "delete"
            else: cprint("Invalid choice.", TColors.RED); time.sleep(1); continue

            # --- Enter Directive Selection Loop ---
            current_loop_filter = initial_filter # Use initial filter from search (if any)
            initial_filter = None # Clear after first use

            while True:
                # --- OPTIMIZATION v27: Pass current_data ---
                selection_result = select_directives_from_file(current_data, show_az_options=show_az, initial_filter=current_loop_filter)
                current_loop_filter = None # Clear after first pass

                if not selection_result or selection_result[0] == 'back': break # Exit Selection loop -> Main Menu
                elif selection_result[0] in ['input_empty', 'invalid_selection']: continue # Stay in Selection loop
                elif selection_result[0] in ['set_all_active', 'set_all_passive']:
                    new_status = (selection_result[0] == 'set_all_passive'); status_str = "PASSIVE" if new_status else "ACTIVE"
                    cprint("\n--- CONFIRMATION ---", TColors.YELLOW, bold=True)
                    print("Set ALL directives in file to {}?".format(status_str))
                    confirm = input("Are you sure? (y/n): ").strip().lower()
                    if confirm == 'y':
                        # Get ALL IDs from the current in-memory data
                        all_ids = [d.get('id') for d in current_data.get("directives", []) if d.get('id') is not None]
                        if all_ids:
                            # Update current_data and save
                            update_json_file(filename, current_data, "set_all_status", None, new_status, ids_to_update=all_ids)
                        else: cprint("No directives to change.", TColors.YELLOW)
                    else: cprint("Cancelled.", TColors.YELLOW)
                    time.sleep(1.5); continue # Refresh list using updated current_data
                else: # User selected numbers/range
                    selected_ids = selection_result; action_successful = False
                    if edit_mode == "toggle_status":
                        action_successful = toggle_directives_status(filename, current_data, selected_ids)
                    elif edit_mode == "delete":
                        cprint("\n--- CONFIRM DELETE ---", TColors.RED, bold=True); print("Delete {} directive(s): {}".format(len(selected_ids), ", ".join(str(s) for s in selected_ids)))
                        confirm = input("Are you sure? (y/n): ").strip().lower()
                        if confirm == 'y': action_successful = delete_directives_from_file(filename, current_data, selected_ids)
                        else: cprint("Cancelled.", TColors.YELLOW)
                    elif edit_mode == "priority":
                        cprint("\n--- New Value ---", bold=True); new_prio = get_valid_input("New 'priority': ", "priority")
                        action_successful = update_json_file(filename, current_data, "priority", new_prio, None, ids_to_update=selected_ids)
                    elif edit_mode == "priority_and_toggle":
                        cprint("\n--- New Value ---", bold=True); new_prio = get_valid_input("New 'priority': ", "priority")
                        # Update priority modifies current_data, saves
                        if update_json_file(filename, current_data, "priority", new_prio, None, ids_to_update=selected_ids):
                            # Toggle status modifies current_data again, saves again
                            action_successful = toggle_directives_status(filename, current_data, selected_ids)
                        else: cprint("Prio update failed, toggle skipped.", TColors.RED)

                    if selected_ids: time.sleep(1.5) # Pause
                    if action_successful is not False: continue # Refresh list using modified current_data
                    else: cprint("Action failed.", TColors.RED); input("Press Enter..."); break # Exit inner loop
            # --- End Directive Selection Loop ---
        # --- End Main Menu Loop ---
        return False # Default exit if S or D


def show_upload_instructions(filename):
    """Displays final manual upload command."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    current_dir = os.getcwd() # Should be LOCAL_DIR
    relative_path = os.path.join(os.path.basename(LOCAL_DIR), filename)

    clear_screen(); cprint("--- DONE (Saved Locally) ---", TColors.GREEN, bold=True)
    cprint("File {} edited.".format(os.path.join(LOCAL_DIR, filename)), bold=True)
    print("\nTo upload manually, run this command from the script directory ({}):".format(script_dir))
    cprint("\n  kubectl cp {} {}:{}{}".format(relative_path, POD_NAME, REMOTE_PATH, filename), TColors.YELLOW)
    print("")
    os.chdir(script_dir) # Go back


def main():
    """Main execution function."""
    original_dir = os.getcwd()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        check_deps()
        while True:
            selected_file, initial_filter = setup_and_select_file() # cds into LOCAL_DIR
            if selected_file:
                structure, data = check_file_structure(selected_file) # Read from LOCAL_DIR
                if not structure: cprint("Failed to read/parse.", TColors.RED); input("Press Enter..."); continue

                # Pass initial 'data' and 'initial_filter'
                should_continue = run_edit_session(selected_file, structure, data, initial_filter)

                if not DISTRIBUTED_SUCCESSFULLY and not should_continue: show_upload_instructions(selected_file); break
                elif DISTRIBUTED_SUCCESSFULLY and not should_continue: os.chdir(script_dir); break
                elif should_continue: continue
                else: os.chdir(script_dir); break
            else: print("No file selected. Exiting."); os.chdir(original_dir); break
    except KeyboardInterrupt: print("\n\nCancelled."); os.chdir(original_dir); sys.exit(0)
    except Exception as e:
        cprint("\nUnexpected error: {}".format(e), TColors.RED, bold=True); import traceback; traceback.print_exc()
        try: os.chdir(original_dir)
        except OSError: pass
        sys.exit(1)

if __name__ == "__main__":
    main()