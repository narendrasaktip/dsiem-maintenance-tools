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
import shutil       # <-- NEW v22: For backup

# --- NEW v22: Config File Handling ---
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
        'Kubernetes': {
            'PodName': 'dsiem-frontend-0',
            'RemotePath': '/dsiem/configs/',
            'Namespace': '' # Optional namespace
        },
        'Paths': {
            'LocalDir': 'dsiem_configs',
            'FilePattern': 'directives_*.json'
        },
        'Display': {
            'ItemsPerPage': '20'
        }
    }

    # Create default config if it doesn't exist
    if not os.path.exists(CONFIG_FILE):
        print("[INFO] Config file '{}' not found. Creating default.".format(CONFIG_FILE))
        try:
            # Use defaults to create sections and write
            config.read_dict(defaults) # read_dict is Py3 only, handle Py2 below
            with open(CONFIG_FILE, 'w') as configfile:
                config.write(configfile)
        except AttributeError: # Python 2 doesn't have read_dict
             config = configparser.ConfigParser() # Start fresh for Py2 write
             for section, options in defaults.items():
                 config.add_section(section)
                 for key, value in options.items():
                     config.set(section, key, value)
             with open(CONFIG_FILE, 'w') as configfile:
                 config.write(configfile)

    # Read the actual config file
    config.read(CONFIG_FILE)

    # Helper to get config value with fallback
    def get_conf(section, key):
        try:
             # Handle Py2 specific methods if needed, though get should be universal
             val = config.get(section, key)
             # Try converting ItemsPerPage to int
             if section == 'Display' and key == 'ItemsPerPage':
                 try:
                     return int(val)
                 except ValueError:
                     print("[WARN] Invalid 'ItemsPerPage' in config. Using default 20.")
                     return 20 # Fallback integer
             return val
        except (configparser.NoSectionError, configparser.NoOptionError):
            print("[WARN] Missing '{}/{}' in config. Using default.".format(section, key))
            # Get default value correctly for Py2/Py3
            try: # Py3 style access
                default_val = defaults[section][key]
            except KeyError:
                default_val = '' # Should not happen with well-defined defaults
            # Handle ItemsPerPage default int conversion
            if section == 'Display' and key == 'ItemsPerPage':
                 try: return int(default_val)
                 except ValueError: return 20
            return default_val
        except Exception as e:
            print("[ERROR] Error reading config '{}/{}': {}".format(section, key, e))
            # Get default value correctly for Py2/Py3
            try: default_val = defaults[section][key]
            except KeyError: default_val = ''
            if section == 'Display' and key == 'ItemsPerPage':
                 try: return int(default_val)
                 except ValueError: return 20
            return default_val


    return {
        'pod_name': get_conf('Kubernetes', 'PodName'),
        'remote_path': get_conf('Kubernetes', 'RemotePath'),
        'namespace': get_conf('Kubernetes', 'Namespace'), # Load namespace
        'local_dir': get_conf('Paths', 'LocalDir'),
        'file_pattern': get_conf('Paths', 'FilePattern'),
        'items_per_page': get_conf('Display', 'ItemsPerPage')
    }

# Load config globally at the start
CONFIG = load_config()
POD_NAME = CONFIG['pod_name']
REMOTE_PATH = CONFIG['remote_path']
NAMESPACE = CONFIG['namespace'] # Store namespace globally
LOCAL_DIR = CONFIG['local_dir']
FILE_PATTERN = CONFIG['file_pattern']
ITEMS_PER_PAGE = CONFIG['items_per_page']
# ------------------------------

# --- Global Variables ---
DISTRIBUTED_SUCCESSFULLY = False # Flag to track upload status
# --------------------

# Handle input() difference between Py2 and Py3
try:
    # Py2: replace input() with raw_input()
    input = raw_input
except NameError:
    # Py3: input() is already correct
    pass

# --- Display Settings (GUI) ---
class TColors:
    BOLD = '\033[1m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    CYAN = '\033[96m'
    RESET = '\033[0m'

    # Disable colors if tput is unavailable or fails
    if not os.popen('tput sgr0 2>/dev/null').read():
        BOLD = GREEN = YELLOW = RED = CYAN = RESET = ""

def cprint(text, color=None, bold=False):
    """Helper function for colored/bold printing."""
    pre = ""
    if bold:
        pre += TColors.BOLD
    if color:
        pre += color
    print("{}{}{}".format(pre, text, TColors.RESET))

def clear_screen():
    """Clears the terminal screen."""
    os.system('clear')
# --------------------

def check_deps():
    """Checks if kubectl is installed."""
    try:
        # Use Popen (compatible) and discard output
        with open(os.devnull, 'w') as FNULL:
            # NEW v22: Include namespace if defined
            cmd = ["kubectl"]
            if NAMESPACE: cmd.extend(["-n", NAMESPACE])
            cmd.extend(["version", "--client"])
            subprocess.Popen(cmd, stdout=FNULL, stderr=FNULL).wait()
    except OSError as e: # Catches FileNotFoundError
        cprint("ERROR: 'kubectl' not found. Please install 'kubectl' first.", TColors.RED, bold=True)
        sys.exit(1)

def run_command(cmd_list):
    """Runs a shell command and returns its output (as string). Includes namespace."""
    try:
        # NEW v22: Prepend namespace to kubectl commands
        full_cmd = list(cmd_list) # Make a copy
        if cmd_list[0] == "kubectl" and NAMESPACE:
            # Insert namespace after 'kubectl'
            full_cmd.insert(1, "-n")
            full_cmd.insert(2, NAMESPACE)

        output = subprocess.check_output(full_cmd, stderr=subprocess.STDOUT)
        return output.decode('utf-8').strip()
    except subprocess.CalledProcessError as e:
        cprint("Error running command: {}".format(' '.join(full_cmd)), TColors.RED, bold=True)
        cprint("Output: {}".format(e.output.decode('utf-8')), TColors.RED)
        return None
    except OSError as e: # Catches FileNotFoundError
        cprint("Error: Command '{}' not found. (OSError)".format(full_cmd[0]), TColors.RED, bold=True)
        return None

def parse_selection(input_str, max_items, current_page=1, items_per_page=20):
    """
    Parses input string (e.g., "1, 3-5") into a list of ACTUAL indices (0-based).
    Accounts for pagination.
    """
    indices = set()
    input_str = input_str.strip().lower()

    parts = re.split(r'[\s,]+', input_str)
    for part in parts:
        if not part:
            continue

        match = re.match(r'^(\d+)-(\d+)$', part)
        if match:
            start_disp, end_disp = int(match.group(1)), int(match.group(2))
            if start_disp > end_disp: start_disp, end_disp = end_disp, start_disp
            
            # Convert displayed range to actual index range
            start_idx = (current_page - 1) * items_per_page + (start_disp - 1)
            end_idx   = (current_page - 1) * items_per_page + (end_disp - 1)

            # Check if actual indices are valid
            if 0 <= start_idx < max_items and 0 <= end_idx < max_items:
                 for i_idx in range(start_idx, end_idx + 1):
                      indices.add(i_idx)
            else:
                 cprint("Warning: Range '{}-{}' on this page refers to items outside the total list (1-{}), ignored.".format(start_disp, end_disp, max_items), TColors.YELLOW)


        elif part.isdigit():
            i_disp = int(part)
            # Convert displayed number to actual index
            i_idx = (current_page - 1) * items_per_page + (i_disp - 1)

            if 0 <= i_idx < max_items:
                indices.add(i_idx)
            else:
                cprint("Warning: Choice '{}' on this page refers to an item outside the total list (1-{}), ignored.".format(i_disp, max_items), TColors.YELLOW)
        else:
            # Handle N, P, A, Z, B outside this function
            if part not in ['n', 'p', 'a', 'z', 'b']:
                 cprint("Warning: Input '{}' is not valid, ignored.".format(part), TColors.YELLOW)

    return sorted(list(indices))

def create_backup(filename):
    """Creates a backup copy of the file."""
    backup_name = filename + ".bak"
    try:
        if os.path.exists(filename):
            shutil.copy2(filename, backup_name) # copy2 preserves metadata
            cprint("Backup created: {}".format(backup_name), TColors.CYAN)
            return True
    except Exception as e:
        cprint("ERROR: Failed to create backup for {}: {}".format(filename, e), TColors.RED)
        return False
    return True # No error if file didn't exist

def setup_and_select_file():
    """Step 1: Show intro, create dir, and select file."""
    clear_screen()
    cprint("--- Directive Configuration Editor (Python 2/3 Version) ---", TColors.GREEN, bold=True)
    print("This script works in the local directory {}{}/{}".format(TColors.BOLD, './', LOCAL_DIR, TColors.RESET))
    print("No changes are made in the pod until you run the `kubectl cp` command at the end.")
    print("")

    # Create LocalDir relative to script's location if it doesn't exist
    script_dir = os.path.dirname(os.path.abspath(__file__))
    local_dir_path = os.path.join(script_dir, LOCAL_DIR)

    if not os.path.isdir(local_dir_path):
        try:
            os.makedirs(local_dir_path)
        except OSError as e:
            cprint("Failed to create directory {}: {}".format(local_dir_path, e), TColors.RED)
            sys.exit(1)

    try:
        os.chdir(local_dir_path) # Change into the local dir
    except OSError as e:
        cprint("Failed to change into directory {}: {}".format(local_dir_path, e), TColors.RED)
        sys.exit(1)


    cprint("--- STEP 1: SELECT FILE ---", bold=True)
    print("Fetching file list from pod...")

    cmd = ["kubectl", "exec", POD_NAME, "--", "find", REMOTE_PATH, "-maxdepth", "1", "-name", FILE_PATTERN]
    file_list_raw = run_command(cmd)

    if file_list_raw is None: # Check if command failed
        os.chdir("..") # Go back before exiting
        sys.exit(1)
    if not file_list_raw.strip(): # Check if output is empty after strip
        cprint("ERROR: No files matching pattern '{}' found in {}:{}".format(FILE_PATTERN, POD_NAME, REMOTE_PATH), TColors.RED, bold=True)
        os.chdir("..")
        sys.exit(1)


    all_lines = file_list_raw.split('\n')
    valid_paths = [
        f for f in all_lines
        if f.startswith(REMOTE_PATH) and f.endswith('.json')
    ]

    if not valid_paths:
        cprint("ERROR: No .json files found (maybe kubectl error?). Output:\n{}".format(file_list_raw), TColors.RED, bold=True)
        os.chdir("..")
        sys.exit(1)

    file_names = [os.path.basename(f) for f in valid_paths]
    file_names.sort()

    # Display 'select' menu
    for i, filename in enumerate(file_names, 1):
        print("  {:2d}) {}".format(i, filename)) # Format neatly (2 digits)
    print("   B) Back")

    while True:
        try:
            choice = input("{}{}{}".format(TColors.BOLD, "Select the file to edit (type number): ", TColors.RESET)).strip()
            if choice.lower() == 'b':
                print("Operation cancelled.")
                os.chdir("..")
                sys.exit(0)

            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(file_names):
                selected_file = file_names[choice_idx]
                cprint("You selected file: {}".format(selected_file), TColors.GREEN)

                # Copy the file
                print("\nCopying {} from pod to local...".format(selected_file))
                cp_cmd = ["kubectl", "cp", "{}:{}{}".format(POD_NAME, REMOTE_PATH, selected_file), "./{}".format(selected_file)]
                if run_command(cp_cmd) is None: # Error
                    cprint("Failed to copy file.", TColors.RED, bold=True)
                    os.chdir("..")
                    sys.exit(1)

                print("File successfully copied to {}{}{}".format(TColors.BOLD, local_dir_path, os.path.sep, selected_file, TColors.RESET))
                return selected_file
            else:
                cprint("Invalid choice. Please enter a number between 1 and {}.".format(len(file_names)), TColors.RED)
        except ValueError:
            cprint("Invalid input. Please enter a number.", TColors.RED)

def check_file_structure(filename):
    """
    Reads the JSON file and determines its structure.
    USES OrderedDict TO PRESERVE KEY ORDER.
    """
    try:
        with open(filename, 'r') as f:
            # IMPORTANT: object_pairs_hook=collections.OrderedDict
            data = json.load(f, object_pairs_hook=collections.OrderedDict)

        if "directives" in data and isinstance(data.get("directives"), list):
            return "array", data
        else:
            return "single_object", data
    except (ValueError, json.JSONDecodeError): # ValueError for Py2
        cprint("ERROR: File {} is not valid JSON.".format(filename), TColors.RED, bold=True)
        return None, None
    except IOError as e:
        cprint("ERROR: Failed to read file {}: {}".format(filename, e), TColors.RED, bold=True)
        return None, None

def get_valid_input(prompt, validation_type):
    """Helper to request valid input."""
    while True:
        val = input(prompt).strip().lower()
        if validation_type == "priority":
            if val.isdigit():
                return int(val)
            else:
                cprint("Error: Must be a number.", TColors.RED)
        elif validation_type == "disabled":
            if val in ['true', 'false']:
                return val == 'true'
            else:
                cprint("Error: Must be 'true' or 'false'.", TColors.RED)

def update_json_file(filename, data, mode, new_prio, new_dis, ids_to_update=None):
    """
    Function to modify JSON data and save it to the file.
    Will preserve key order because data is an OrderedDict.
    Creates backup before saving.
    """
    # --- NEW v22: Backup before update ---
    if not create_backup(filename):
        cprint("Update cancelled due to backup failure.", TColors.RED)
        return False # Indicate failure

    cprint("\nUpdating file {} (locally)...".format(filename), TColors.YELLOW)
    updated_items = []
    update_successful = False # Flag

    try:
        if ids_to_update is None:
            # --- Single Object Mode ---
            if mode == "priority" or mode == "both":
                if new_prio is not None: data['priority'] = new_prio
            if mode == "disabled" or mode == "both":
                 if new_dis is not None: data['disabled'] = new_dis
            updated_items.append(data)

        else:
            # --- Array Mode ---
            ids_set = set(int(i) for i in ids_to_update)
            for directive in data.get("directives", []):
                dir_id = directive.get('id')
                if dir_id is not None and int(dir_id) in ids_set:
                    item_updated = False
                    if mode in ["priority", "both"]:
                        if new_prio is not None:
                             directive['priority'] = new_prio
                             item_updated = True
                    if mode in ["both", "set_all_status"]:
                         if new_dis is not None:
                             directive['disabled'] = new_dis
                             item_updated = True
                    if item_updated:
                        updated_items.append(directive)

        # Write back to the file
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        update_successful = True # Mark as successful only after writing

        if updated_items:
            cprint("Update SUCCESSFUL for {} item(s).".format(len(updated_items)), TColors.GREEN, bold=True)
            if mode != "toggle_status": # Toggle has its own summary
                cprint("Check new values:", bold=True)
                for item in updated_items:
                    print_data = {
                        "id": item.get('id'), "name": item.get('name'),
                        "priority": item.get('priority'), "disabled": item.get('disabled')
                    }
                    print(json.dumps(print_data, indent=2, sort_keys=True))
        else:
             cprint("No items were updated.", TColors.YELLOW)

    except Exception as e:
        cprint("ERROR: Failed to update JSON file: {}".format(e), TColors.RED, bold=True)
        update_successful = False

    return update_successful # Return status

def delete_directives_from_file(filename, data, ids_to_delete):
    """
    Function to delete directives from the file. Creates backup.
    """
     # --- NEW v22: Backup before delete ---
    if not create_backup(filename):
        cprint("Deletion cancelled due to backup failure.", TColors.RED)
        return False # Indicate failure

    cprint("\nDeleting directives from {}...".format(filename), TColors.RED)
    delete_successful = False

    try:
        ids_set = set(int(i) for i in ids_to_delete)
        original_count = len(data.get("directives", []))

        new_directives_list = [
            d for d in data.get("directives", [])
            if int(d.get('id', 0)) not in ids_set
        ]

        data['directives'] = new_directives_list
        new_count = len(new_directives_list)

        # Write back to the file
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        delete_successful = True

        cprint("Deletion SUCCESSFUL. {} directive(s) removed.".format(original_count - new_count), TColors.GREEN, bold=True)

    except Exception as e:
        cprint("ERROR: Failed to delete directives: {}".format(e), TColors.RED, bold=True)
        delete_successful = False

    return delete_successful

def toggle_directives_status(filename, data, ids_to_toggle):
    """
    Function to SWAP/TOGGLE the disabled status (True/False). Creates backup.
    """
     # --- NEW v22: Backup before toggle ---
    if not create_backup(filename):
        cprint("Toggle cancelled due to backup failure.", TColors.RED)
        return False # Indicate failure

    cprint("\nToggling status (swap) for {} (locally)...".format(filename), TColors.YELLOW)

    toggled_summary = []
    ids_set = set(int(i) for i in ids_to_toggle)
    items_toggled_count = 0
    toggle_successful = False

    try:
        for directive in data.get("directives", []):
            dir_id = directive.get('id')
            if dir_id is not None and int(dir_id) in ids_set:
                current_status = directive.get('disabled', False)
                new_status = not current_status # Flip it
                directive['disabled'] = new_status
                items_toggled_count += 1

                status_str = "--- [ Passive ] ---" if new_status else "+++ [ Active ] +++"
                toggled_summary.append("-> Status for '{}' changed to {}.".format(directive.get('name', 'N/A'), status_str))

        # Save the file after toggling all selected items
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        toggle_successful = True

        cprint("Update SUCCESSFUL for {} item(s).".format(items_toggled_count), TColors.GREEN, bold=True)

        # Print summary
        if toggled_summary:
            cprint("Change Details:", bold=True)
            for line in toggled_summary:
                print(line)

    except Exception as e:
        cprint("ERROR: Failed to update JSON file (during toggle): {}".format(e), TColors.RED, bold=True)
        toggle_successful = False

    return toggle_successful

# --- NEW v22: Added pagination logic ---
def select_directives_from_file(data, show_az_options=False):
    """
    Step 2.1: Display directive list with pagination, filtering, A/Z options.
    """
    current_page = 1
    search_term = ""

    while True: # Loop for pagination and filtering
        clear_screen()
        cprint("--- SELECT DIRECTIVE(S) ---", bold=True)
        if search_term:
            cprint("Filter active: '{}'".format(search_term), TColors.CYAN)

        all_directives = data.get("directives", [])
        if not all_directives:
            cprint("This file contains no directives.", TColors.YELLOW)
            return ['back']

        # 1. Filter (if search_term is set)
        if search_term:
            filtered_directives = [
                d for d in all_directives
                if search_term.lower() in d.get('name', '').lower()
            ]
        else:
            filtered_directives = list(all_directives) # Make a copy

        if not filtered_directives:
            cprint("No directives match the filter '{}'.".format(search_term), TColors.YELLOW)
        else:
            # 2. Sort the filtered list
            sorted_directives = sorted(
                filtered_directives,
                key=lambda d: d.get('name', '').lower()
            )

            # 3. Grouping
            enabled_directives = [d for d in sorted_directives if not d.get('disabled')]
            disabled_directives = [d for d in sorted_directives if d.get('disabled')]
            display_directives_list = enabled_directives + disabled_directives # This is the final list to display

            # 4. Pagination Calculations
            total_items = len(display_directives_list)
            total_pages = (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
            if total_pages == 0: total_pages = 1 # At least one page
            current_page = max(1, min(current_page, total_pages)) # Keep page in bounds
            start_index = (current_page - 1) * ITEMS_PER_PAGE
            end_index = start_index + ITEMS_PER_PAGE
            page_items = display_directives_list[start_index:end_index]
            directive_ids_on_page = [d.get('id') for d in page_items] # IDs just for this page

            # --- Display Menu for the current page ---
            i = 1 # Display counter for the current page
            cprint("\nPage {} of {}".format(current_page, total_pages), bold=True)

            # Display items grouped by status *within the page*
            enabled_on_page = [d for d in page_items if not d.get('disabled')]
            disabled_on_page = [d for d in page_items if d.get('disabled')]

            if enabled_on_page:
                 cprint("\n--- ACTIVE (Disabled: False) ---", TColors.GREEN, bold=True)
                 for d in enabled_on_page:
                     id_str = "[ID: {}]".format(str(d.get('id', 'N/A')))
                     prio_str = "[Priority: {}]".format(str(d.get('priority', 'N/A')))
                     name_str = d.get('name', 'No Name')
                     print("  {:3d}) {} {} {}".format(i, id_str, prio_str, name_str))
                     i += 1

            if disabled_on_page:
                 cprint("\n--- INACTIVE (Disabled: True) ---", TColors.YELLOW, bold=True)
                 for d in disabled_on_page:
                     id_str = "[ID: {}]".format(str(d.get('id', 'N/A')))
                     prio_str = "[Priority: {}]".format(str(d.get('priority', 'N/A')))
                     name_str = d.get('name', 'No Name')
                     print("  {:3d}) {} {} {}".format(i, id_str, prio_str, name_str))
                     i += 1
            
            # Use i-1 as the max number for selection on *this page*
            max_num_on_page = i - 1 

        # --- Display Action Options ---
        print("\n" + "-"*70) # Separator Line
        prompt = "Select (e.g., 1, 3-5), "
        options = []
        if total_pages > 1:
            if current_page > 1: options.append("P=Prev")
            if current_page < total_pages: options.append("N=Next")
        options.append("F=Filter")
        if show_az_options:
            options.append("A=All Active")
            options.append("Z=All Passive")
            prompt += " A, Z,"
        options.append("B=Back")
        prompt += " {})".format(", ".join(options))

        # Minta input
        selection_string = input("{}{}{}".format(TColors.BOLD, prompt + ": ", TColors.RESET))
        choice_lower = selection_string.strip().lower()

        # Handle Navigation/Action commands
        if choice_lower == 'b':
            print("Returning to main menu.")
            return ['back']
        elif choice_lower == 'n' and current_page < total_pages:
            current_page += 1
            continue # Redraw next page
        elif choice_lower == 'p' and current_page > 1:
            current_page -= 1
            continue # Redraw previous page
        elif choice_lower == 'f':
            new_filter = input("Enter filter term (leave empty to clear): ").strip()
            search_term = new_filter
            current_page = 1 # Reset to page 1 when filter changes
            continue # Redraw with filter
        elif show_az_options and choice_lower == 'a':
            return ['set_all_active']
        elif show_az_options and choice_lower == 'z':
            return ['set_all_passive']
        elif not selection_string.strip():
            cprint("Input cannot be empty.", TColors.RED)
            time.sleep(1.5)
            continue # Stay on current page

        # If it wasn't a command, try parsing as numbers/range
        # --- PERUBAHAN v22: Parse relative to the current page ---
        # The 'max_items' here refers to the number of items *displayed on the current page*
        # The 'total_items' refers to the total number of items *in the filtered list*
        parsed_indices_actual = parse_selection(selection_string, total_items, current_page, ITEMS_PER_PAGE)

        # Map actual indices back to the original full list IDs if needed,
        # but easier: just get the IDs directly from the actual indices
        selected_ids = [display_directives_list[idx].get('id') for idx in parsed_indices_actual if idx < total_items]


        if not selected_ids:
            cprint("No valid directives selected on this page.", TColors.RED)
            time.sleep(1.5)
            continue # Stay on current page

        cprint("You selected {} directive(s) based on current view.".format(len(selected_ids)), TColors.GREEN)
        return selected_ids # Success, return LIST OF IDs

def restart_pods(filename):
    """Restarts the frontend and relevant backend pods."""
    cprint("\n--- RESTARTING PODS ---", TColors.YELLOW, bold=True)

    # 1. Restart Frontend (always)
    cprint("Restarting {}...".format(POD_NAME), TColors.YELLOW)
    fe_output = run_command(["kubectl", "delete", "pod", POD_NAME])
    print(fe_output)

    # 2. Find and restart Backend
    match = re.search(r'directives_(dsiem-backend-\d+)_', filename)
    if match:
        backend_pod_name = match.group(1)
        cprint("Restarting {}...".format(backend_pod_name), TColors.YELLOW)
        be_output = run_command(["kubectl", "delete", "pod", backend_pod_name])
        print(be_output)
    else:
        cprint("No specific backend pod found in filename. Only frontend restarted.", TColors.YELLOW)

def distribute_to_pod(filename):
    """Uploads the file to the pod and triggers restarts."""
    global DISTRIBUTED_SUCCESSFULLY
    clear_screen()
    cprint("--- STEP 3: DISTRIBUTE TO POD ---", TColors.GREEN, bold=True)
    cprint("Uploading {} to pod...".format(filename), TColors.YELLOW)

    # CWD is currently inside LOCAL_DIR, so './filename' path is correct
    cp_cmd = ["kubectl", "cp", "./{}".format(filename), "{}:{}{}".format(POD_NAME, REMOTE_PATH, filename)]
    upload_output = run_command(cp_cmd)

    if upload_output is None:
        cprint("ERROR: Failed to upload file to pod. Distribution cancelled.", TColors.RED, bold=True)
        return

    print(upload_output)
    cprint("Upload SUCCESSFUL.", TColors.GREEN)

    confirm = input("\nRestart pods to apply changes? (y/n): ").strip().lower()
    if confirm == 'y':
        restart_pods(filename)
    else:
        cprint("Pods not restarted. Changes might not take effect immediately.", TColors.YELLOW)

    DISTRIBUTED_SUCCESSFULLY = True
    input("\nPress Enter to exit...")

def run_edit_session(filename, structure, data):
    """Step 2: Main editing process."""

    if structure == "single_object":
        # --- Single Object File Flow ---
        clear_screen()
        cprint("--- STEP 2: EDIT FILE (Single Object) ---", bold=True)
        cprint("This file ({}) contains a single object.".format(filename))

        current_prio = data.get('priority', 'N/A')
        current_dis = data.get('disabled', 'N/A')
        print("Current values: {0}Priority={1}{2}, {0}Disabled={3}{2}".format(
            TColors.BOLD, current_prio, TColors.RESET, current_dis
        ))

        new_prio = get_valid_input("Enter new 'priority' value: ", "priority")
        new_dis = get_valid_input("Enter new 'disabled' value (true/false): ", "disabled")

        if update_json_file(filename, data, "both", new_prio, new_dis): # Check if update succeeded
            print("")
            confirm_dist = input("Distribute this file to the pod now? (y/n): ").strip().lower()
            if confirm_dist == 'y':
                distribute_to_pod(filename)

    else:
        # --- Array File Flow (Looping Menu) ---
        while True: # Main Menu Loop
            clear_screen()
            # Re-read file each loop iteration for fresh data
            structure, data = check_file_structure(filename)
            if not structure: # Handle read error during loop
                 cprint("Error reading file during refresh. Returning to start.", TColors.RED)
                 time.sleep(2)
                 break

            cprint("--- STEP 2: EDIT MENU FOR FILE ({}) ---".format(filename), bold=True)
            print("What would you like to change?")

            cprint("  1) Change Priority Only", TColors.CYAN)
            cprint("  2) Toggle Status (Active/Passive)", TColors.CYAN)
            cprint("  3) Change Priority & Toggle Status", TColors.CYAN)
            cprint("  4) DELETE Directive(s)", TColors.RED, bold=True)
            print("")
            cprint("  S) Done (Save Locally)", bold=True)
            cprint("  D) Distribute to Pod (Upload & Restart)", TColors.GREEN, bold=True)

            aksi = input("{}{}{}".format(TColors.BOLD, "Select Action [1, 2, 3, 4, S, D]: ", TColors.RESET)).strip().lower()

            edit_mode = ""
            if aksi == '1': edit_mode = "priority"
            elif aksi == '2': edit_mode = "toggle_status"
            elif aksi == '3': edit_mode = "priority_and_toggle"
            elif aksi == '4': edit_mode = "delete"
            elif aksi == 's':
                cprint("Done. File saved locally.", TColors.GREEN)
                break # Exit Main Menu loop
            elif aksi == 'd':
                distribute_to_pod(filename)
                break # Exit Main Menu loop after distribution
            else:
                cprint("Invalid choice.", TColors.RED); input("Press Enter...")
                continue

            # --- Determine if A/Z options should be shown ---
            show_az = (edit_mode == "toggle_status")

            while True: # Directive Selection / Action Loop
                # 1. Re-read data for refresh
                structure, data = check_file_structure(filename)
                if not structure: # Handle read error during inner loop
                    cprint("Error reading file during refresh. Returning to main menu.", TColors.RED)
                    time.sleep(2)
                    break # Break inner loop, outer loop will re-check

                # 2. Display directive list & get selection
                selection_result = select_directives_from_file(data, show_az_options=show_az)

                # 3. Handle selection result
                if not selection_result or selection_result[0] == 'back':
                    break # Exit Selection loop -> return to Main Menu
                elif selection_result[0] in ['input_empty', 'invalid_selection']:
                    continue # Stay in Selection loop, re-display menu
                elif selection_result[0] in ['set_all_active', 'set_all_passive']:
                    # Handle A/Z
                    new_status = (selection_result[0] == 'set_all_passive')
                    status_str = "PASSIVE (Disabled: True)" if new_status else "ACTIVE (Disabled: False)"
                    cprint("\n--- CONFIRMATION ---", TColors.YELLOW, bold=True)
                    print("You are about to change ALL directives in this file to {}.".format(status_str))
                    confirm = input("Are you sure? (y/n): ").strip().lower()
                    if confirm == 'y':
                        all_ids = [d.get('id') for d in data.get("directives", []) if d.get('id') is not None]
                        if all_ids:
                            # Use mode 'set_all_status' for clarity in update function
                            update_json_file(filename, data, "set_all_status", None, new_status, ids_to_update=all_ids)
                        else:
                            cprint("No directives to change.", TColors.YELLOW)
                    else:
                        cprint("Operation cancelled.", TColors.YELLOW)
                    print("")
                    time.sleep(1.5) # Brief pause
                    continue # Stay in Selection loop, refresh list
                else:
                    # User selected numbers/range
                    selected_ids = selection_result

                    # --- 4. Execute Action based on 'edit_mode' ---
                    action_successful = False # Flag to track if action completed
                    if edit_mode == "toggle_status":
                        action_successful = toggle_directives_status(filename, data, selected_ids)
                    elif edit_mode == "delete":
                        print("") # Add spacing before confirm
                        cprint("--- CONFIRM DELETE ---", TColors.RED, bold=True)
                        print("You are about to delete the following {} directive(s):".format(len(selected_ids)))
                        print(", ".join(str(s) for s in selected_ids))
                        confirm = input("Are you sure? (y/n): ").strip().lower()
                        if confirm == 'y':
                            action_successful = delete_directives_from_file(filename, data, selected_ids)
                        else:
                            cprint("Deletion cancelled.", TColors.YELLOW)
                    elif edit_mode == "priority":
                        print("") # Add spacing before input
                        cprint("--- Enter New Value for {} Selected Directive(s) ---".format(len(selected_ids)), bold=True)
                        new_prio = get_valid_input("Enter new 'priority' value: ", "priority")
                        action_successful = update_json_file(filename, data, "priority", new_prio, None, ids_to_update=selected_ids)
                    elif edit_mode == "priority_and_toggle":
                        print("") # Add spacing before input
                        cprint("--- Enter New Value for {} Selected Directive(s) ---".format(len(selected_ids)), bold=True)
                        new_prio = get_valid_input("Enter new 'priority' value: ", "priority")
                        # Update priority first, check success
                        if update_json_file(filename, data, "priority", new_prio, None, ids_to_update=selected_ids):
                            # Re-read data before toggling
                            structure, data = check_file_structure(filename)
                            if structure: # Check read success
                                action_successful = toggle_directives_status(filename, data, selected_ids)
                            else:
                                cprint("Error reading file after priority update, toggle skipped.", TColors.RED)

                    # 5. Brief pause only if action was attempted
                    if selected_ids: # Ensure selected_ids is not empty before pausing
                        time.sleep(1.5)

                    # Continue loop to refresh list ONLY IF the action didn't fail badly
                    if action_successful is not False: # Checks for True or None (e.g., delete cancelled)
                        continue # Stay in Directive Selection Loop, refresh list
                    else:
                        cprint("Action failed, returning to main menu.", TColors.RED)
                        input("Press Enter...")
                        break # Exit inner loop on severe failure

            # --- End Directive Selection Loop ---
        # --- End Main Menu Loop ---


def show_upload_instructions(filename):
    """Step 3: Display final upload command."""
    clear_screen()
    cprint("--- DONE (Saved Locally) ---", TColors.GREEN, bold=True)
    print("File {}{}/{}{}{} has been edited.".format(TColors.BOLD, LOCAL_DIR, os.path.sep, filename, TColors.RESET))
    print("")
    print("To upload this file back to the pod manually, run this command:")
    print("")
    cprint("  kubectl cp ./{}/{} {}:{}{}".format(LOCAL_DIR, filename, POD_NAME, REMOTE_PATH, filename), TColors.YELLOW)
    print("")

    # Go back to the original directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir) # Go back to script's dir

def main():
    """Main execution function."""
    # Store the starting directory
    original_dir = os.getcwd()
    script_dir = os.path.dirname(os.path.abspath(__file__)) # Get script dir

    try:
        check_deps()
        selected_file = setup_and_select_file() # This function now cds into LOCAL_DIR

        structure, data = check_file_structure(selected_file) # Read file from LOCAL_DIR
        if not structure:
            os.chdir(original_dir) # Failed to read file
            sys.exit(1)

        run_edit_session(selected_file, structure, data) # Edit happens in LOCAL_DIR

        # Only show manual instructions if NOT distributed
        if not DISTRIBUTED_SUCCESSFULLY:
            # show_upload_instructions changes back to script_dir
            show_upload_instructions(selected_file)
        else:
            # If distributed, make sure to cd back
            os.chdir(script_dir)


    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        os.chdir(original_dir) # Ensure return to start dir on cancel
        sys.exit(0)
    except Exception as e:
        cprint("\nAn unexpected error occurred: {}".format(e), TColors.RED, bold=True)
        import traceback
        traceback.print_exc() # Show detailed error for debugging
        os.chdir(original_dir)
        sys.exit(1)

if __name__ == "__main__":
    main()
