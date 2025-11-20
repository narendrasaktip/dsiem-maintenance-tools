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
import requests # <-- Added
import base64   # <-- Added
import io       # <-- Added

try:
    import configparser
except ImportError:
    import ConfigParser as configparser

# --- Penyesuaian Kompatibilitas Py2/Py3 ---
try:
    JSONDecodeError = json.JSONDecodeError
except AttributeError:
    JSONDecodeError = ValueError
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError
try:
    input = raw_input # Py2 compatibility
except NameError:
    pass # Py3 already has input
# ---

CONFIG_FILE = 'config.ini'

# --- State Variables ---
CURRENT_FILE_INFO = {
    "remote_path": None,
    "sha": None,
    "content_str": None,
    "filename": None,
    "modified": False
}
# ---

def load_config():
    config = configparser.ConfigParser()
    defaults = {
        'GitHub': {'RemotePath': '', 'FilePattern': 'directives_*.json'},
        'Display': {'ItemsPerPage': '100'}
    }
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, CONFIG_FILE)

    if not os.path.exists(config_path):
        if hasattr(config, 'read_dict'): config.read_dict(defaults)
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
        except: return defaults.get(section, {}).get(key, '')

    github_repo = os.getenv("GITHUB_REPO")
    github_token = os.getenv("GITHUB_TOKEN")
    github_branch = os.getenv("GITHUB_BRANCH")

    if not all([github_repo, github_token, github_branch]):
        print_error("FATAL: GITHUB_REPO, GITHUB_TOKEN, and GITHUB_BRANCH must be set.")
        print("       Please run 'source config.sh' first.")
        sys.exit(1)

    return {
        'github_repo': github_repo, 'github_token': github_token, 'github_branch': github_branch,
        'remote_path': get_conf('GitHub', 'RemotePath'),
        'file_pattern': get_conf('GitHub', 'FilePattern'),
        'items_per_page': get_conf('Display', 'ItemsPerPage')
    }

CONFIG = load_config()
GITHUB_REPO = CONFIG['github_repo']
GITHUB_TOKEN = CONFIG['github_token']
GITHUB_BRANCH = CONFIG['github_branch']
REMOTE_PATH = CONFIG['remote_path']
FILE_PATTERN = CONFIG['file_pattern']
ITEMS_PER_PAGE = CONFIG['items_per_page']

DISTRIBUTED_SUCCESSFULLY = False

class TColors:
    BOLD='\033[1m'; GREEN='\033[92m'; YELLOW='\033[93m'; RED='\033[91m'
    CYAN='\033[96m'; BLUE='\033[94m'; MAGENTA='\033[95m'; WHITE='\033[97m'
    RESET='\033[0m'; DIM='\033[2m'; UNDERLINE='\033[4m'
    if not os.popen('tput sgr0 2>/dev/null').read():
        BOLD=GREEN=YELLOW=RED=CYAN=BLUE=MAGENTA=WHITE=RESET=DIM=UNDERLINE=""

# --- Helper Functions ---
def print_info_box(title, items, icon="ℹ"):
    max_content_length = len(title) + len(icon) + 2
    for key, value in items: max_content_length = max(max_content_length, len("  {}: {}".format(key, str(value))))
    box_width = max_content_length + 4; inner_width = box_width - 2
    print("\n{}┌{}┐{}".format(TColors.CYAN, "─" * inner_width, TColors.RESET))
    title_text = " {} {} ".format(icon, title); padding = inner_width - len(title_text)
    print("{}│{}{}{} {} │{}".format(TColors.CYAN, TColors.RESET + TColors.BOLD, title_text, TColors.RESET, " " * padding, TColors.CYAN + TColors.RESET))
    print("{}├{}┤{}".format(TColors.CYAN, "─" * inner_width, TColors.RESET))
    for key, value in items:
        line = "  {}{}: {}{}".format(TColors.YELLOW, key, TColors.WHITE, value)
        visible_text = "  {}: {}".format(key, str(value)); padding = inner_width - len(visible_text)
        print("{}│{}{}{}│{}".format(TColors.CYAN, TColors.RESET, line, " " * max(0, padding), TColors.CYAN + TColors.RESET))
    print("{}└{}┘{}".format(TColors.CYAN, "─" * inner_width, TColors.RESET))
def print_separator(char="═", width=80, color=None): pre = color if color else ""; print("{}{}{}".format(pre, char * width, TColors.RESET))
def print_header(text, icon=""): clear_screen(); print("\n"); print_separator("═", 80, TColors.CYAN); centered = "{} {} {}".format(icon, text, icon).center(80); print("{}{}{}".format(TColors.BOLD + TColors.CYAN, centered, TColors.RESET)); print_separator("═", 80, TColors.CYAN); print("")
def print_panel(title, content, color=TColors.BLUE, width=78):
    print("\n{}{}┌─[ {} ]{}".format(color, TColors.BOLD, title, "─" * (width - len(title) - 5)))
    for line in content.split('\n'): print("{}│{} {}".format(color, TColors.RESET, line))
    print("{}└{}{}".format(color, "─" * (width-2), TColors.RESET))
def print_menu(title, options, show_back=True, show_quit=True):
    print("\n{}{}{}".format(TColors.BOLD + TColors.CYAN, "╭─ " + title + " " + "─" * (75 - len(title)), TColors.RESET))
    for i, (key, desc, color) in enumerate(options): icon = "▸"; print("{}│{} [{}{}{}] {}{}".format(TColors.CYAN, TColors.RESET, color + TColors.BOLD, key, TColors.RESET, desc, TColors.RESET))
    if show_back or show_quit: print("{}├{}{}".format(TColors.CYAN, "─" * 77, TColors.RESET))
    if show_back: print("{}│{} [{}B{}] {}Back{}".format(TColors.CYAN, TColors.RESET, TColors.YELLOW + TColors.BOLD, TColors.RESET, TColors.YELLOW, TColors.RESET))
    if show_quit: print("{}│{} [{}Q{}] {}Quit{}".format(TColors.CYAN, TColors.RESET, TColors.RED + TColors.BOLD, TColors.RESET, TColors.RED, TColors.RESET))
    print("{}╰{}{}".format(TColors.CYAN, "─" * 77, TColors.RESET))
def print_success(text): print("\n{}✓{} {}{}".format(TColors.GREEN + TColors.BOLD, TColors.RESET, TColors.GREEN, text + TColors.RESET))
def print_error(text): print("\n{}✗{} {}{}".format(TColors.RED + TColors.BOLD, TColors.RESET, TColors.RED, text + TColors.RESET))
def print_warning(text): print("\n{}⚠{} {}{}".format(TColors.YELLOW + TColors.BOLD, TColors.RESET, TColors.YELLOW, text + TColors.RESET))
def print_loading(text): print("\n{}⟳{} {}{}".format(TColors.CYAN + TColors.BOLD, TColors.RESET, TColors.CYAN, text + TColors.RESET))
def clear_screen(): os.system('clear' if os.name != 'nt' else 'cls')
# --- End Helper Functions ---

# --- GitHub API Functions ---
def gh_headers():
    return { "Accept": "application/vnd.github+json", "Authorization": "Bearer {}".format(GITHUB_TOKEN), "X-GitHub-Api-Version": "2022-11-28" }
def gh_api_get(path):
    url = "https://api.github.com/repos/{}/contents/{}".format(GITHUB_REPO, path.replace("\\", "/").lstrip('/'))
    try:
        r = requests.get(url, headers=gh_headers(), params={"ref": GITHUB_BRANCH}, timeout=60)
        if r.status_code == 404: return None
        r.raise_for_status(); return r.json()
    except requests.exceptions.RequestException as e: print_error("GitHub API GET Error for '{}': {}".format(path, e)); return None
def gh_put_file(path, content_bytes, message, sha):
    url = "https://api.github.com/repos/{}/contents/{}".format(GITHUB_REPO, path.replace("\\", "/").lstrip('/'))
    payload = { "message": message, "content": base64.b64encode(content_bytes).decode("ascii"), "branch": GITHUB_BRANCH, "sha": sha }
    try:
        r = requests.put(url, headers=gh_headers(), data=json.dumps(payload), timeout=60)
        r.raise_for_status(); print_success("Changes pushed to GitHub: {}".format(path)); return r.json()
    except requests.exceptions.RequestException as e: err_msg = e.response.text if e.response else str(e); print_error("GitHub API PUT Error for '{}': {}".format(path, err_msg)); return None
# --- End GitHub API Functions ---

# --- Recursive File Finder ---
def find_directive_files_recursively(current_repo_path=""):
    found_files = []
    items = gh_api_get(current_repo_path)
    if items is None: return []
    if not isinstance(items, list):
        if isinstance(items, dict) and items.get('type') == 'file' and re.match(FILE_PATTERN.replace('*','.*'), items.get('name', '')): return [items]
        else: print_warning("Expected list at path '{}'".format(current_repo_path)); return []
    for item in items:
        if item.get('type') == 'file' and re.match(FILE_PATTERN.replace('*','.*'), item.get('name', '')): found_files.append(item)
    for item in items:
        if item.get('type') == 'dir': found_files.extend(find_directive_files_recursively(item['path']))
    return found_files
# --- End Recursive File Finder ---

def parse_selection(input_str, max_total_items):
    indices = set(); input_str = input_str.strip().lower(); parts = re.split(r'[\s,]+', input_str)
    for part in parts:
        if not part: continue
        match = re.match(r'^(\d+)-(\d+)$', part)
        if match:
            start_disp, end_disp = int(match.group(1)), int(match.group(2))
            if start_disp > end_disp: start_disp, end_disp = end_disp, start_disp
            start_idx, end_idx = start_disp - 1, end_disp - 1
            if 0 <= start_idx < max_total_items and 0 <= end_idx < max_total_items: indices.update(range(start_idx, end_idx + 1))
            else: print_warning("Range '{}-{}' out of bounds (max {}).".format(start_disp, end_disp, max_total_items))
        elif part.isdigit():
            i_disp = int(part); i_idx = i_disp - 1
            if 0 <= i_idx < max_total_items: indices.add(i_idx)
            else: print_warning("Choice '{}' out of bounds (max {}).".format(i_disp, max_total_items))
        else:
            if part not in ['n', 'p', 'a', 'z', 'b', 'f', 'c']: print_warning("Input '{}' invalid.".format(part))
    return sorted(list(indices))

def get_status_filter():
    while True:
        print_header("STATUS FILTER", "🔍")
        print_menu("Select Directive Status", [("1", "Active", TColors.GREEN), ("2", "Passive", TColors.YELLOW), ("3", "Both", TColors.CYAN)], show_back=False, show_quit=False)
        choice = input("\n{}▸{} Select status [1, 2, 3]: ".format(TColors.BOLD, TColors.RESET)).strip()
        if choice == '1': return False
        elif choice == '2': return True
        elif choice == '3': return None
        else: print_error("Invalid choice.")

def check_directive_status(directive_data, desired_disabled_status):
    if desired_disabled_status is None: return True
    return directive_data.get('disabled', False) == desired_disabled_status

# --- Modified File Search (Recursive) ---
def search_and_select_file_from_github():
    while True:
        print_header("SEARCH BY DIRECTIVE NAME (GitHub)", "🔍")
        print("{}Enter directive name (or part) to search recursively from repo path '{}'{}".format(TColors.CYAN, REMOTE_PATH or "[repo root]", TColors.RESET))
        search_term_input = input("\n{}▸{} Search term (or B to Back): ".format(TColors.BOLD, TColors.RESET)).strip()
        if not search_term_input: print_error("Search term empty."); time.sleep(1.5); continue
        if search_term_input.lower() == 'b': return None, None
        desired_status = get_status_filter()
        status_desc = "any status"
        if desired_status is False: status_desc = "Active"
        elif desired_status is True: status_desc = "Passive"
        print_loading("Recursively searching GitHub repo from '{}' for '{}' files...".format(REMOTE_PATH or "[root]", FILE_PATTERN))
        all_directive_files_meta = find_directive_files_recursively(REMOTE_PATH)
        if not all_directive_files_meta: print_error("No directive files ({}) found recursively from '{}'.".format(FILE_PATTERN, REMOTE_PATH or "[root]")); input("\n{}Press Enter...{}".format(TColors.DIM, TColors.RESET)); continue
        print_loading("Checking content of {} files for '{}' (Status: {})...".format(len(all_directive_files_meta), search_term_input, status_desc))
        matching_files = []
        for file_meta in all_directive_files_meta:
            filename = file_meta['name']; file_remote_path = file_meta['path']
            print_loading("  Checking {}...".format(filename))
            content_meta = gh_api_get(file_remote_path)
            if not content_meta or 'content' not in content_meta: print_warning("    Could not read content of {}. Skipping.".format(filename)); continue
            try:
                file_content_bytes = base64.b64decode(content_meta['content'])
                file_content_str = file_content_bytes.decode('utf-8')
                data = json.loads(file_content_str, object_pairs_hook=collections.OrderedDict)
                found_in_file = False; directives_list = []; structure = "unknown"
                if "directives" in data and isinstance(data.get("directives"), list): directives_list = data.get("directives", []); structure = "array"
                elif isinstance(data.get('id'), int): directives_list = [data]; structure = "single_object"
                for directive in directives_list:
                    if check_directive_status(directive, desired_status) and search_term_input.lower() in directive.get('name', '').lower(): found_in_file = True; break
                if found_in_file: matching_files.append({"name": filename, "path": file_remote_path, "sha": content_meta['sha'], "content": file_content_str, "structure": structure})
            except (TypeError, base64.binascii.Error, UnicodeDecodeError, ValueError, JSONDecodeError) as e: print_warning("    File {} invalid (decode/JSON error: {}). Skipping.".format(filename, e)); continue
        if not matching_files: print_error("No file found containing '{}' (Status: {}).".format(search_term_input, status_desc)); input("\n{}Press Enter...{}".format(TColors.DIM, TColors.RESET)); continue
        elif len(matching_files) == 1:
            selected_file_info = matching_files[0]; print_success("Found unique match: {}".format(selected_file_info['path'])) # Show full path
            CURRENT_FILE_INFO.update({"remote_path": selected_file_info['path'], "sha": selected_file_info['sha'], "content_str": selected_file_info['content'], "filename": selected_file_info['name'], "modified": False})
            return selected_file_info['name'], search_term_input
        else:
            print_header("MULTIPLE MATCHING FILES FOUND", "📋")
            matching_files.sort(key=lambda x: x['path']) # Sort by full path
            print("\n{}Found {} files containing '{}':{}".format(TColors.GREEN + TColors.BOLD, len(matching_files), search_term_input, TColors.RESET))
            for i, finfo in enumerate(matching_files, 1): print("  {}[{:2d}]{} {}".format(TColors.CYAN, i, TColors.RESET, finfo['path'])) # Display full path
            print("\n{}{}{}".format(TColors.CYAN, "─" * 78, TColors.RESET))
            while True:
                try:
                    choice = input("\n{}▸{} Select file number (or B to Back): ".format(TColors.BOLD, TColors.RESET)).strip()
                    if choice.lower() == 'b': return None, None
                    choice_idx = int(choice) - 1
                    if 0 <= choice_idx < len(matching_files):
                        selected_file_info = matching_files[choice_idx]; print_success("Selected: {}".format(selected_file_info['path']))
                        CURRENT_FILE_INFO.update({"remote_path": selected_file_info['path'], "sha": selected_file_info['sha'], "content_str": selected_file_info['content'], "filename": selected_file_info['name'], "modified": False})
                        return selected_file_info['name'], search_term_input
                    else: print_error("Invalid choice.")
                except ValueError: print_error("Invalid input.")
# --- End Modified File Search ---

# --- Modified Setup (Recursive List) ---
def setup_and_select_file_from_github():
    while True:
        print_header(" DIRECTIVE CONFIGURATION EDITOR (GitHub)", "⚙️")
        print_info_box("GitHub Configuration", [("Repository", GITHUB_REPO), ("Branch", GITHUB_BRANCH), ("Search Path", REMOTE_PATH or "[repo root]"), ("File Pattern", FILE_PATTERN)], "📋")
        print("\n{}💡 Note: Changes saved temporarily until Pushed.{}".format(TColors.YELLOW, TColors.RESET))
        print_menu("Select File Method", [("1", "Select from list (recursive)", TColors.CYAN), ("2", "Search by name (recursive)", TColors.GREEN)], show_back=False)
        method_choice = input("\n{}▸{} Select method [1, 2, Q]: ".format(TColors.BOLD, TColors.RESET)).strip().lower()
        initial_filter = None
        if method_choice == '1': # Select from list
            print_loading("Recursively searching for '{}' files from repo path '{}'...".format(FILE_PATTERN, REMOTE_PATH or "[root]"))
            # --- Use recursive search ---
            all_directive_files_meta = find_directive_files_recursively(REMOTE_PATH)
            # --- End Use recursive search ---

            if not all_directive_files_meta:
                print_error("No directive files ({}) found recursively from '{}'.".format(FILE_PATTERN, REMOTE_PATH or "[root]"))
                input("\n{}Press Enter...{}".format(TColors.DIM, TColors.RESET)); continue

            # --- SORT BY FULL PATH ---
            all_directive_files_meta.sort(key=lambda x: x.get('path', '').lower()) # Sort case-insensitively by path
            # --- END SORT BY PATH ---

            # Create map and display list AFTER sorting
            file_path_map = {f_meta['path']: f_meta for f_meta in all_directive_files_meta}
            display_paths = [f_meta['path'] for f_meta in all_directive_files_meta] # Get paths in sorted order

            print_header("SELECT FILE FROM GITHUB (Recursive Search)", "📄")
            print("\n{}Found {} files:{}".format(TColors.GREEN + TColors.BOLD, len(display_paths), TColors.RESET))
            for i, file_path in enumerate(display_paths, 1): # Display sorted full path
                print("  {}[{:2d}]{} {}".format(TColors.CYAN, i, TColors.RESET, file_path))
            print("\n{}{}{}".format(TColors.CYAN, "─" * 78, TColors.RESET))

            while True: # Inner loop for selection
                try:
                    choice = input("\n{}▸{} Select file number (or B to Back): ".format(TColors.BOLD, TColors.RESET)).strip()
                    if choice.lower() == 'b': break # Break inner loop

                    choice_idx = int(choice) - 1
                    if 0 <= choice_idx < len(display_paths):
                        selected_full_path = display_paths[choice_idx] # Get the selected full path from the sorted list
                        selected_meta = file_path_map[selected_full_path] # Get its metadata from the map
                        selected_filename = selected_meta['name']

                        print_success("Selected: {}".format(selected_full_path))
                        print_loading("Downloading {} content from GitHub...".format(selected_filename))

                        # Use selected_full_path to get content
                        content_meta = gh_api_get(selected_full_path)

                        if content_meta and 'content' in content_meta and 'sha' in content_meta:
                            try:
                                file_content_bytes = base64.b64decode(content_meta['content'])
                                file_content_str = file_content_bytes.decode('utf-8')
                                CURRENT_FILE_INFO.update({
                                    "remote_path": selected_full_path,
                                    "sha": content_meta['sha'],
                                    "content_str": file_content_str,
                                    "filename": selected_filename,
                                    "modified": False
                                })
                                print_success("Content downloaded.")
                                return selected_filename, initial_filter # Return filename and filter
                            except (TypeError, base64.binascii.Error, UnicodeDecodeError) as e: print_error("Failed decode {}: {}".format(selected_filename, e))
                        else: print_error("Failed download content for {}.".format(selected_filename))
                        input("\n{}Press Enter...{}".format(TColors.DIM, TColors.RESET)); break # Break inner loop on error
                    else: print_error("Invalid choice.")
                except ValueError: print_error("Invalid input.")
            continue
        elif method_choice == '2':
            selected_file, initial_filter = search_and_select_file_from_github()
            if selected_file: return selected_file, initial_filter
            else: continue
        elif method_choice == 'q': print("\n{}Exiting...{}".format(TColors.YELLOW, TColors.RESET)); sys.exit(0)
        else: print_error("Invalid selection."); time.sleep(1.5); continue
# --- End Modified Setup ---

def check_file_structure_from_string(content_str):
    try: data = json.loads(content_str, object_pairs_hook=collections.OrderedDict)
    except (ValueError, JSONDecodeError) as e: print_error("Content is not valid JSON: {}".format(e)); return None, None
    if "directives" in data and isinstance(data.get("directives"), list): return "array", data
    elif isinstance(data.get('id'), int): return "single_object", data
    else: print_error("JSON structure not recognized."); return None, None

def get_valid_input(prompt, validation_type):
    while True:
        val = input("{}{}".format(TColors.BOLD + "▸ " + TColors.RESET, prompt)).strip().lower()
        if validation_type == "priority":
            if val.isdigit(): return int(val)
            else: print_error("Must be a number.")
        elif validation_type == "disabled":
            if val in ['true', 'false']: return val == 'true'
            else: print_error("Must be 'true' or 'false'.")

# --- Modified Update/Delete/Toggle Functions (operate on CURRENT_FILE_INFO) ---
def update_current_file_data(mode, new_prio, new_dis, ids_to_update=None):
    """Modifies CURRENT_FILE_INFO content_str based on mode and selection."""
    print_loading("Updating directives in memory...")

    try:
        # Load the current data from the string state
        data = json.loads(CURRENT_FILE_INFO['content_str'], object_pairs_hook=collections.OrderedDict)
        structure = "array" if "directives" in data else "single_object"
    except (ValueError, JSONDecodeError):
        print_error("Cannot parse current file content as JSON.")
        return False, 0 # Failed, 0 updated

    updated_items_count = 0
    updated_items_details = []

    # --- Apply modifications to the 'data' object ---
    if structure == "single_object":
        item_updated = False
        # Apply changes directly to 'data'
        if mode in ["priority", "both"] and new_prio is not None: data['priority'] = new_prio; item_updated = True
        if mode in ["disabled", "both", "set_all_status"] and new_dis is not None: data['disabled'] = new_dis; item_updated = True
        if item_updated:
            updated_items_count = 1
            updated_items_details.append(data)
    elif structure == "array" and ids_to_update is not None:
        ids_set = set(int(i) for i in ids_to_update)
        for directive in data.get("directives", []):
            dir_id = directive.get('id')
            item_updated = False
            if dir_id is not None and int(dir_id) in ids_set:
                # Apply changes directly to the 'directive' dict within 'data'
                if mode in ["priority", "both"] and new_prio is not None: directive['priority'] = new_prio; item_updated = True
                if mode in ["both", "set_all_status"] and new_dis is not None: directive['disabled'] = new_dis; item_updated = True
                if item_updated:
                    updated_items_count += 1
                    updated_items_details.append(directive)
    # --- End applying modifications ---

    if updated_items_count > 0:
        try:
            # --- [CORRECTED SERIALIZATION BLOCK] ---
            # 1. Use json.dumps directly to get the string
            #    Py2 -> str (bytes, utf-8)
            #    Py3 -> str (unicode)
            json_string = json.dumps(data, indent=4, ensure_ascii=False)

            # 2. Check if we are in Python 2 and need to decode
            try:
                unicode # Attempt to access 'unicode', fails in Py3
                # If we are in Py2 and the result is bytes (str), decode to unicode
                if isinstance(json_string, str):
                    json_string = json_string.decode('utf-8')
            except NameError:
                # This is Py3, json_string is already unicode (str)
                pass

            # 3. Store the guaranteed unicode string back into the state
            CURRENT_FILE_INFO['content_str'] = json_string
            CURRENT_FILE_INFO['modified'] = True
            # --- [END CORRECTED SERIALIZATION BLOCK] ---

            print_success("Update SUCCESSFUL for {} item(s) in memory.".format(updated_items_count))
            # Display update details (unchanged)
            if mode != "toggle_status" and updated_items_details:
                print("\n{}Check new values:{}".format(TColors.BOLD+TColors.CYAN, TColors.RESET))
                for item in updated_items_details:
                    print_data = {"id": item.get('id'), "name": item.get('name'), "priority": item.get('priority'), "disabled": item.get('disabled')}
                    print("{}{}{}".format(TColors.DIM, json.dumps(print_data, indent=2, sort_keys=True), TColors.RESET))
            return True, updated_items_count
        except Exception as e:
            # Catch potential errors during dumps or decode
            print_error("Failed to serialize or process updated JSON: {}".format(e))
            return False, 0
    else:
        # No items matched, but the process didn't fail
        print_warning("No items matched the criteria for update.")
        return True, 0 # Considered success

def delete_directives_from_current_file(ids_to_delete):
    print_loading("Deleting directives in memory...")
    try: data = json.loads(CURRENT_FILE_INFO['content_str'], object_pairs_hook=collections.OrderedDict); structure = "array" if "directives" in data else "single_object"
    except (ValueError, JSONDecodeError): print_error("Cannot parse current file content."); return False
    if structure != "array": print_error("Delete only supported for array structure."); return False
    ids_set = set(int(i) for i in ids_to_delete); original_directives = data.get("directives", []); original_count = len(original_directives)
    new_directives_list = [d for d in original_directives if int(d.get('id', 0)) not in ids_set]; data['directives'] = new_directives_list
    new_count = len(new_directives_list); deleted_count = original_count - new_count
    if deleted_count > 0:
        try:
            output = io.StringIO(); json.dump(data, output, indent=4, ensure_ascii=False); new_content_str = output.getvalue(); output.close()
            try: unicode; CURRENT_FILE_INFO['content_str'] = new_content_str.decode('utf-8') if isinstance(new_content_str, str) else new_content_str
            except NameError: CURRENT_FILE_INFO['content_str'] = new_content_str
            CURRENT_FILE_INFO['modified'] = True; print_success("Deletion SUCCESSFUL. {} directive(s) marked for removal.".format(deleted_count)); return True
        except Exception as e: print_error("Failed to serialize JSON after delete: {}".format(e)); return False
    else: print_warning("No matching directives found to delete."); return True

def toggle_directives_status_in_current_file(ids_to_toggle):
    """SWAP/TOGGLE disabled status in CURRENT_FILE_INFO content_str."""
    print_loading("Toggling status (swap) in memory...")

    try:
        # Load the current data from the string state
        data = json.loads(CURRENT_FILE_INFO['content_str'], object_pairs_hook=collections.OrderedDict)
        structure = "array" if "directives" in data else "single_object"
    except (ValueError, JSONDecodeError):
        print_error("Cannot parse current file content as JSON.")
        return False, 0 # Failed, 0 toggled

    if structure != "array":
        print_error("Toggle operation only supported for files with 'directives' array.")
        return False, 0 # Failed, 0 toggled

    toggled_summary = []
    ids_set = set(int(i) for i in ids_to_toggle)
    items_toggled_count = 0

    # --- Apply modifications directly to the 'data' object ---
    for directive in data.get("directives", []):
        dir_id = directive.get('id')
        if dir_id is not None and int(dir_id) in ids_set:
            current_status = directive.get('disabled', False)
            new_status = not current_status
            directive['disabled'] = new_status # Modify the dictionary directly
            items_toggled_count += 1
            status_str = "{}PASSIVE{}".format(TColors.YELLOW, TColors.RESET) if new_status else "{}ACTIVE{}".format(TColors.GREEN, TColors.RESET)
            toggled_summary.append("  → Status for '{}' changed to {}".format(directive.get('name', 'N/A'), status_str))
    # --- End applying modifications ---

    if items_toggled_count > 0:
        try:
            # --- [CORRECTED SERIALIZATION BLOCK] ---
            # 1. Use json.dumps directly to get the string
            #    Py2 -> str (bytes, utf-8)
            #    Py3 -> str (unicode)
            json_string = json.dumps(data, indent=4, ensure_ascii=False)

            # 2. Check if we are in Python 2 and need to decode
            try:
                unicode # Attempt to access 'unicode', fails in Py3
                # If we are in Py2 and the result is bytes (str), decode to unicode
                if isinstance(json_string, str):
                    json_string = json_string.decode('utf-8')
            except NameError:
                # This is Py3, json_string is already unicode (str)
                pass

            # 3. Store the guaranteed unicode string back into the state
            CURRENT_FILE_INFO['content_str'] = json_string
            CURRENT_FILE_INFO['modified'] = True
            # --- [END CORRECTED SERIALIZATION BLOCK] ---

            print_success("Update SUCCESSFUL for {} item(s) in memory.".format(items_toggled_count))
            # Display toggle details (unchanged)
            if toggled_summary:
                print("\n{}Change Details:{}".format(TColors.BOLD + TColors.CYAN, TColors.RESET))
                print("\n".join(toggled_summary))
            return True, items_toggled_count
        except Exception as e:
            # Catch potential errors during dumps or decode
            print_error("Failed to serialize JSON after toggle: {}".format(e))
            return False, 0
    else:
        # No items matched, but the process didn't fail
        print_warning("No matching directives found to toggle.")
        return True, 0 # Considered success
# --- End Modified Update Functions ---

def select_directives_from_data(data, show_az_options=False, initial_filter=None):
    # (Function body is largely unchanged, displays directives from 'data' argument)
    current_page = 1; search_term = initial_filter if initial_filter else ""
    while True:
        print_header("SELECT DIRECTIVE(S) from {}".format(CURRENT_FILE_INFO['filename']), "📋")
        if search_term: print("{}🔍 Filter active: '{}'{}".format(TColors.CYAN+TColors.BOLD, search_term, TColors.RESET))
        structure = "array" if "directives" in data else "single_object"; all_directives = []
        if structure == "array": all_directives = data.get("directives", [])
        elif structure == "single_object": all_directives = [data]
        if not all_directives: print_warning("No directives in current content."); return ['back']
        if search_term: filtered_directives = [d for d in all_directives if search_term.lower() in d.get('name', '').lower()]
        else: filtered_directives = list(all_directives)
        if not filtered_directives: print_warning("No directives match filter '{}'.".format(search_term)); total_items=0; display_directives_list=[]; max_num_on_page=0; total_pages=1
        else:
            sorted_directives=sorted(filtered_directives, key=lambda d: d.get('name','').lower());enabled_directives=[d for d in sorted_directives if not d.get('disabled')];disabled_directives=[d for d in sorted_directives if d.get('disabled')];display_directives_list=enabled_directives+disabled_directives
            total_items=len(display_directives_list);total_pages=max(1,(total_items+ITEMS_PER_PAGE-1)//ITEMS_PER_PAGE);current_page=max(1,min(current_page,total_pages));start_index=(current_page-1)*ITEMS_PER_PAGE;end_index=start_index+ITEMS_PER_PAGE;page_items=display_directives_list[start_index:end_index]
            print("\n{}Page {} of {} {} Total: {} directives{}".format(TColors.BOLD+TColors.CYAN, current_page, total_pages, TColors.RESET+TColors.DIM+"│", total_items, TColors.RESET))
            enabled_on_page=[d for d in page_items if not d.get('disabled')];disabled_on_page=[d for d in page_items if d.get('disabled')]
            if enabled_on_page:
                print("\n{}╭─ ACTIVE {}".format(TColors.GREEN+TColors.BOLD,"─"*(65)+"╮"+TColors.RESET))
                for idx, d in enumerate(enabled_on_page):
                    global_index=display_directives_list.index(d) if d in display_directives_list else -1;display_number=global_index+1 if global_index != -1 else '?';id_str="ID:{}".format(str(d.get('id','N/A')));prio_str="P:{}".format(str(d.get('priority','N/A')));name_str=d.get('name','No Name')
                    print("{}│{} {}[{:3d}]{} {}[{}]{} {}[{}]{} {}".format(TColors.GREEN,TColors.RESET,TColors.BOLD+TColors.WHITE,display_number,TColors.RESET,TColors.CYAN,id_str,TColors.RESET,TColors.YELLOW,prio_str,TColors.RESET,name_str))
                print("{}╰{}╯{}".format(TColors.GREEN,"─"*76,TColors.RESET))
            if disabled_on_page:
                print("\n{}╭─ INACTIVE {}".format(TColors.RED+TColors.BOLD,"─"*(63)+"╮"+TColors.RESET))
                for idx, d in enumerate(disabled_on_page):
                    global_index=display_directives_list.index(d) if d in display_directives_list else -1;display_number=global_index+1 if global_index != -1 else '?';id_str="ID:{}".format(str(d.get('id','N/A')));prio_str="P:{}".format(str(d.get('priority','N/A')));name_str=d.get('name','No Name')
                    print("{}│{} {}[{:3d}]{} {}[{}]{} {}[{}]{} {}{}".format(TColors.YELLOW,TColors.RESET,TColors.BOLD+TColors.WHITE,display_number,TColors.RESET,TColors.CYAN,id_str,TColors.RESET,TColors.YELLOW,prio_str,TColors.RESET,TColors.DIM,name_str+TColors.RESET))
                print("{}╰{}╯{}".format(TColors.RED,"─"*76,TColors.RESET))
            max_num_on_page=len(page_items)
        print("\n{}{}{}".format(TColors.CYAN,"═"*78,TColors.RESET))
        prompt_parts=["Select directive(s)"];example_start=1;example_end=total_items;prompt_parts.append("Examples: {}, {}-{}".format(example_start,example_start,example_end if example_end>=example_start else example_start))
        options=[]
        if total_pages > 1:
            if current_page > 1: options.append("{}P{}=Prev".format(TColors.YELLOW,TColors.RESET))
            if current_page < total_pages: options.append("{}N{}=Next".format(TColors.YELLOW,TColors.RESET))
        options.append("{}F{}=Filter".format(TColors.CYAN,TColors.RESET));
        if search_term: options.append("{}C{}=Clear Filter".format(TColors.CYAN,TColors.RESET))
        if show_az_options: options.append("{}A{}=All Active".format(TColors.GREEN,TColors.RESET)); options.append("{}Z{}=All Passive".format(TColors.YELLOW,TColors.RESET))
        options.append("{}B{}=Back".format(TColors.RED,TColors.RESET))
        print("{}Options: {}{}".format(TColors.DIM," │ ".join(options),TColors.RESET))
        print("{}{}{}".format(TColors.CYAN,"═"*78,TColors.RESET))
        selection_string=input("\n{}▸{} Your choice: ".format(TColors.BOLD,TColors.RESET));choice_lower=selection_string.strip().lower()
        if choice_lower == 'b': print("\n{}Returning...{}".format(TColors.YELLOW,TColors.RESET)); return ['back']
        elif choice_lower == 'n' and current_page < total_pages: current_page += 1; continue
        elif choice_lower == 'p' and current_page > 1: current_page -= 1; continue
        elif choice_lower == 'f': new_filter = input("{}▸{} Enter filter term: ".format(TColors.BOLD, TColors.RESET)).strip(); search_term = new_filter; current_page = 1; continue
        elif choice_lower == 'c' and search_term: search_term = ""; current_page = 1; print_success("Filter cleared."); time.sleep(1); continue
        elif show_az_options and choice_lower == 'a': return ['set_all_active']
        elif show_az_options and choice_lower == 'z': return ['set_all_passive']
        elif not selection_string.strip(): print_error("Input cannot be empty."); time.sleep(1.5); continue
        parsed_indices_actual=parse_selection(selection_string,total_items)
        selected_ids=[display_directives_list[idx].get('id') for idx in parsed_indices_actual if idx < len(display_directives_list) and display_directives_list[idx] is not None and display_directives_list[idx].get('id') is not None]
        if not selected_ids: print_error("No valid directives selected."); time.sleep(1.5); continue
        print_success("You selected {} directive(s).".format(len(selected_ids)))
        return selected_ids

# --- Modified distribute_to_github ---
def distribute_to_github():
    global DISTRIBUTED_SUCCESSFULLY
    if not CURRENT_FILE_INFO['modified']: print_warning("No changes to push."); return True
    remote_path = CURRENT_FILE_INFO['remote_path']; content_str = CURRENT_FILE_INFO['content_str']
    current_sha = CURRENT_FILE_INFO['sha']; filename = CURRENT_FILE_INFO['filename']
    if not all([remote_path, content_str, current_sha, filename]): print_error("Cannot distribute: File info incomplete."); return False
    print_header("DISTRIBUTE TO GITHUB", "📤")
    print_loading("Uploading changes for {} to GitHub...".format(filename))
    try: content_bytes = content_str.encode('utf-8')
    except Exception as e: print_error("Failed to encode content: {}".format(e)); return False
    commit_message = "[directive-updater] Update {}".format(filename)
    result = gh_put_file(remote_path, content_bytes, commit_message, current_sha)
    if result:
        new_sha = result.get('content', {}).get('sha')
        if new_sha: CURRENT_FILE_INFO['sha'] = new_sha; CURRENT_FILE_INFO['modified'] = False; print_success("Push successful. SHA updated.")
        else: print_warning("Push ok but failed get new SHA."); CURRENT_FILE_INFO['modified'] = False
        DISTRIBUTED_SUCCESSFULLY = True; return True
    else: print_error("Failed to upload to GitHub."); DISTRIBUTED_SUCCESSFULLY = False; return False
# --- End modified distribute_to_github ---

# --- Modified run_edit_session ---
def run_edit_session(initial_filter=None):
    global DISTRIBUTED_SUCCESSFULLY
    DISTRIBUTED_SUCCESSFULLY = False
    filename = CURRENT_FILE_INFO['filename']; content_str = CURRENT_FILE_INFO['content_str']
    structure, data = check_file_structure_from_string(content_str)
    if not structure: print_error("Cannot proceed."); input("\n{}Press Enter...{}".format(TColors.DIM, TColors.RESET)); return True
    if structure == "single_object":
        print_header("EDIT FILE (Single Object) - {}".format(filename), "✏️")
        current_prio = data.get('priority', 'N/A'); current_dis = data.get('disabled', 'N/A')
        print("\n{}Current:{} Prio={}, Disabled={}".format(TColors.BOLD, TColors.RESET, current_prio, current_dis))
        print("\n{}New Values:{}".format(TColors.BOLD, TColors.RESET))
        new_prio = get_valid_input("New 'priority': ", "priority"); new_dis = get_valid_input("New 'disabled' (true/false): ", "disabled")
        success, _ = update_current_file_data("both", new_prio, new_dis)
        if success:
            confirm_dist = input("\n{}▸{} Push to GitHub? (y/n): ".format(TColors.BOLD, TColors.RESET)).strip().lower()
            if confirm_dist == 'y':
                if distribute_to_github(): confirm_back = input("\n{}▸{} Back to file selection? (y/n): ".format(TColors.BOLD, TColors.RESET)).strip().lower(); return confirm_back == 'y'
                else: input("\n{}Push failed. Press Enter...{}".format(TColors.DIM, TColors.RESET)); return False # Stay
            else: confirm_back = input("\n{}▸{} Not pushed. Back to file selection? (y/n): ".format(TColors.BOLD, TColors.RESET)).strip().lower(); return confirm_back == 'y'
        else: input("\n{}Update failed. Press Enter...{}".format(TColors.DIM, TColors.RESET)); return False # Stay
    else:  # Array structure
        while True:
            print_header("EDIT MENU - {}".format(filename), "✏️")
            print_menu("Select Action", [("1", "Priority", TColors.CYAN), ("2", "Status (Toggle)", TColors.YELLOW), ("3", "Priority & Toggle", TColors.MAGENTA), ("4", "DELETE", TColors.RED)], show_back=False, show_quit=False)
            print("\n{}{}{}".format(TColors.CYAN, "─" * 78, TColors.RESET))
            print("{}│{} [{}P{}] {}Push Changes to GitHub & Exit/Back{}".format(TColors.CYAN, TColors.RESET, TColors.GREEN+TColors.BOLD, TColors.RESET, TColors.GREEN+TColors.BOLD, TColors.RESET))
            print("{}│{} [{}B{}] {}Back (Discard Unpushed Changes){}".format(TColors.CYAN, TColors.RESET, TColors.YELLOW+TColors.BOLD, TColors.RESET, TColors.YELLOW, TColors.RESET))
            print("{}╰{}{}".format(TColors.CYAN, "─" * 77, TColors.RESET))
            aksi = input("\n{}▸{} Select Action [1-4, B, P]: ".format(TColors.BOLD, TColors.RESET)).strip().lower()
            if aksi == 'b':
                if CURRENT_FILE_INFO['modified']:
                    confirm = input("{}Discard unpushed changes and go back? (y/n): {}".format(TColors.YELLOW, TColors.RESET)).strip().lower()
                    if confirm == 'y': return True
                    else: continue
                else: return True
            elif aksi == 'p':
                if CURRENT_FILE_INFO['modified']:
                    if distribute_to_github(): confirm_back = input("\n{}▸{} Back to file selection? (y/n): ".format(TColors.BOLD, TColors.RESET)).strip().lower(); return confirm_back == 'y'
                    else: input("\n{}Push failed. Press Enter...{}".format(TColors.DIM, TColors.RESET)); continue
                else: print_warning("No changes to push."); confirm_back = input("\n{}▸{} Back to file selection anyway? (y/n): ".format(TColors.BOLD, TColors.RESET)).strip().lower(); return confirm_back == 'y'
            edit_mode = ""; show_az = False
            if aksi == '1': edit_mode = "priority"
            elif aksi == '2': edit_mode = "toggle_status"; show_az = True
            elif aksi == '3': edit_mode = "priority_and_toggle"
            elif aksi == '4': edit_mode = "delete"
            else: print_error("Invalid choice."); time.sleep(1); continue
            current_loop_filter = initial_filter; initial_filter = None
            while True: # Directive selection loop
                try: current_data_for_select = json.loads(CURRENT_FILE_INFO['content_str'], object_pairs_hook=collections.OrderedDict)
                except (ValueError, JSONDecodeError): print_error("Cannot parse current data."); break
                selection_result = select_directives_from_data(current_data_for_select, show_az_options=show_az, initial_filter=current_loop_filter)
                current_loop_filter = None
                if not selection_result or selection_result[0] == 'back': break
                if selection_result[0] in ['set_all_active', 'set_all_passive']:
                    new_status = (selection_result[0] == 'set_all_passive'); status_str = "PASSIVE" if new_status else "ACTIVE"
                    print_header("CONFIRMATION", "⚠️"); print("\n{}Set ALL {} directives to {}?{}".format(TColors.YELLOW+TColors.BOLD, len(current_data_for_select.get("directives",[])), status_str, TColors.RESET))
                    confirm = input("\n{}▸{} Are you sure? (y/n): ".format(TColors.BOLD, TColors.RESET)).strip().lower()
                    if confirm == 'y':
                        all_ids = [d.get('id') for d in current_data_for_select.get("directives", []) if d.get('id') is not None]
                        if all_ids: update_current_file_data("set_all_status", None, new_status, ids_to_update=all_ids)
                        else: print_warning("No directives to change.")
                    else: print_warning("Cancelled.")
                    time.sleep(1.5); continue
                else:
                    selected_ids = selection_result; action_successful = False
                    if edit_mode == "toggle_status": action_successful, _ = toggle_directives_status_in_current_file(selected_ids)
                    elif edit_mode == "delete":
                        print_header("CONFIRM DELETE", "⚠️"); print("\n{}Delete {} directive(s): {}{}".format(TColors.RED+TColors.BOLD, len(selected_ids), ", ".join(str(s) for s in selected_ids), TColors.RESET))
                        confirm = input("\n{}▸{} Are you sure? (y/n): ".format(TColors.BOLD, TColors.RESET)).strip().lower()
                        if confirm == 'y': action_successful = delete_directives_from_current_file(selected_ids)
                        else: print_warning("Cancelled."); action_successful = True # Treat cancel as success
                    elif edit_mode == "priority":
                        print("\n{}Enter New Value:{}".format(TColors.BOLD+TColors.CYAN, TColors.RESET)); new_prio = get_valid_input("New 'priority': ", "priority"); action_successful, _ = update_current_file_data("priority", new_prio, None, ids_to_update=selected_ids)
                    elif edit_mode == "priority_and_toggle":
                        print("\n{}Enter New Value:{}".format(TColors.BOLD+TColors.CYAN, TColors.RESET)); new_prio = get_valid_input("New 'priority': ", "priority"); prio_ok, _ = update_current_file_data("priority", new_prio, None, ids_to_update=selected_ids)
                        if prio_ok: toggle_ok, _ = toggle_directives_status_in_current_file(selected_ids); action_successful = toggle_ok
                        else: print_error("Priority update failed, toggle skipped."); action_successful = False
                    if selected_ids: time.sleep(1.5)
                    if action_successful: continue
                    else: print_error("Action failed."); input("\n{}Press Enter...{}".format(TColors.DIM, TColors.RESET)); break
            # --- End Directive Selection Loop ---
        # --- End Array structure main loop ---
    return False # Default: Stay

# --- Main Execution ---
def main():
    original_dir = os.getcwd(); script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir) # Change to script dir for config.ini
    try:
        while True:
            selected_file, initial_filter = setup_and_select_file_from_github()
            if selected_file:
                go_back_to_selection = run_edit_session(initial_filter)
                if not go_back_to_selection: break
            else: break
    except KeyboardInterrupt: print("\n\n{}Cancelled by user.{}".format(TColors.YELLOW, TColors.RESET))
    except Exception as e: print_error("Unexpected error: {}".format(e)); traceback.print_exc()
    finally: print("\n{}Exiting script...{}".format(TColors.CYAN, TColors.RESET)); os.chdir(original_dir); sys.exit(0)

if __name__ == "__main__":
    main()