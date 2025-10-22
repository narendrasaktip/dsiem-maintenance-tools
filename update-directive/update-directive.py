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
import time         # <-- Only for brief pauses after errors/refresh

# --- Configuration ---
POD_NAME = "dsiem-frontend-0"
REMOTE_PATH = "/dsiem/configs/"
LOCAL_DIR = "dsiem_configs_edited"
FILE_PATTERN = "directives_*.json"
# --------------------

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

    # Replace f-string with .format() for compatibility
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
            subprocess.Popen(["kubectl", "version", "--client"],
                             stdout=FNULL, stderr=FNULL).wait()
    except OSError as e: # Catches FileNotFoundError
        cprint("ERROR: 'kubectl' not found. Please install 'kubectl' first.", TColors.RED, bold=True)
        sys.exit(1)

def run_command(cmd_list):
    """Runs a shell command and returns its output (as string)."""
    try:
        # check_output (stderr=STDOUT) captures both stderr and stdout
        output = subprocess.check_output(cmd_list, stderr=subprocess.STDOUT)
        return output.decode('utf-8').strip()
    except subprocess.CalledProcessError as e:
        # If 'find' fails (e.g., wrong path), it's also an error here
        cprint("Error running command: {}".format(' '.join(cmd_list)), TColors.RED, bold=True)
        # e.output is bytes, needs decoding
        cprint("Output: {}".format(e.output.decode('utf-8')), TColors.RED)
        return None
    except OSError as e: # Catches FileNotFoundError
        cprint("Error: Command '{}' not found. (OSError)".format(cmd_list[0]), TColors.RED, bold=True)
        return None

def parse_selection(input_str, max_items):
    """
    Parses input string (e.g., "1, 3-5") into a list of indices (0-based).
    """
    indices = set()
    input_str = input_str.strip().lower()

    parts = re.split(r'[\s,]+', input_str)
    for part in parts:
        if not part:
            continue

        match = re.match(r'^(\d+)-(\d+)$', part)
        if match:
            start, end = int(match.group(1)), int(match.group(2))
            if start > end:
                start, end = end, start
            for i in range(start, end + 1):
                if 1 <= i <= max_items:
                    indices.add(i - 1)
                else:
                    cprint("Warning: Choice '{}' is out of range (1-{}), ignored.".format(i, max_items), TColors.YELLOW)

        elif part.isdigit():
            i = int(part)
            if 1 <= i <= max_items:
                indices.add(i - 1)
            else:
                cprint("Warning: Choice '{}' is out of range (1-{}), ignored.".format(i, max_items), TColors.YELLOW)
        else:
            cprint("Warning: Input '{}' is not valid, ignored.".format(part), TColors.YELLOW)

    return sorted(list(indices))

def setup_and_select_file():
    """Step 1: Show intro, create dir, and select file."""
    clear_screen()
    cprint("--- Directive Configuration Editor (Python 2/3 Version) ---", TColors.GREEN, bold=True)
    print("This script works in the local directory {}{}/{}".format(TColors.BOLD, './', LOCAL_DIR, TColors.RESET))
    print("No changes are made in the pod until you run the `kubectl cp` command at the end.")
    print("")

    if not os.path.isdir(LOCAL_DIR):
        try:
            os.makedirs(LOCAL_DIR)
        except OSError as e:
            cprint("Failed to create directory {}: {}".format(LOCAL_DIR, e), TColors.RED)
            sys.exit(1)

    os.chdir(LOCAL_DIR)

    cprint("--- STEP 1: SELECT FILE ---", bold=True)
    print("Fetching file list from pod...")

    cmd = ["kubectl", "exec", POD_NAME, "--", "find", REMOTE_PATH, "-maxdepth", "1", "-name", FILE_PATTERN]
    file_list_raw = run_command(cmd)

    if not file_list_raw:
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

                print("File successfully copied to {}{}/{}{}".format(TColors.BOLD, LOCAL_DIR, os.path.sep, selected_file, TColors.RESET))
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
    """
    cprint("\nUpdating file {} (locally)...".format(filename), TColors.YELLOW)

    updated_items = []

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
            # Ensure safe ID comparison (int vs int)
            ids_set = set(int(i) for i in ids_to_update)

            # data['directives'] is a list of OrderedDicts
            for directive in data.get("directives", []):
                # Add None check in case id is missing
                dir_id = directive.get('id')
                if dir_id is not None and int(dir_id) in ids_set:
                    item_updated = False
                    if mode == "priority" or mode == "both":
                        if new_prio is not None: # Only update if a new value was provided
                             directive['priority'] = new_prio
                             item_updated = True
                    if mode == "both" or mode == "set_all_status":
                         if new_dis is not None: # Only update if a new value (True/False) was provided
                             directive['disabled'] = new_dis
                             item_updated = True

                    if item_updated:
                        updated_items.append(directive)

        # Write back to the file. json.dump respects OrderedDict
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)

        if updated_items: # Only print if something was updated
            cprint("Update SUCCESSFUL for {} item(s).".format(len(updated_items)), TColors.GREEN, bold=True)
            if mode != "toggle_status":
                cprint("Check new values:", bold=True)
                for item in updated_items:
                    print_data = {
                        "id": item.get('id'),
                        "name": item.get('name'),
                        "priority": item.get('priority'),
                        "disabled": item.get('disabled')
                    }
                    print(json.dumps(print_data, indent=2, sort_keys=True))
        else:
             cprint("No items were updated.", TColors.YELLOW)


    except Exception as e:
        cprint("ERROR: Failed to update JSON file: {}".format(e), TColors.RED, bold=True)

def delete_directives_from_file(filename, data, ids_to_delete):
    """
    Function to delete directives from the file.
    """
    cprint("\nDeleting directives from {}...".format(filename), TColors.RED)

    try:
        ids_set = set(int(i) for i in ids_to_delete)
        original_count = len(data.get("directives", []))

        # Create a new list, filtering out those in the set
        new_directives_list = [
            d for d in data.get("directives", [])
            if int(d.get('id', 0)) not in ids_set
        ]

        data['directives'] = new_directives_list
        new_count = len(new_directives_list)

        # Write back to the file
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)

        cprint("Deletion SUCCESSFUL. {} directive(s) removed.".format(original_count - new_count), TColors.GREEN, bold=True)

    except Exception as e:
        cprint("ERROR: Failed to delete directives: {}".format(e), TColors.RED, bold=True)

def toggle_directives_status(filename, data, ids_to_toggle):
    """
    Function to SWAP/TOGGLE the disabled status (True/False).
    Based on the logic from the 02.manage_plugins.py script.
    """
    cprint("\nToggling status (swap) for {} (locally)...".format(filename), TColors.YELLOW)

    toggled_summary = []
    ids_set = set(int(i) for i in ids_to_toggle)
    items_toggled_count = 0

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

        cprint("Update SUCCESSFUL for {} item(s).".format(items_toggled_count), TColors.GREEN, bold=True)

        # Print summary
        if toggled_summary:
            cprint("Change Details:", bold=True)
            for line in toggled_summary:
                print(line)

    except Exception as e:
        cprint("ERROR: Failed to update JSON file (during toggle): {}".format(e), TColors.RED, bold=True)

# --- PERUBAHAN v19: Added 'show_az_options' argument ---
def select_directives_from_file(data, show_az_options=False):
    """
    Step 2.1: Display directive list, request selection.
    A/Z options only appear if show_az_options=True.
    """
    clear_screen()
    cprint("--- SELECT DIRECTIVE(S) ---", bold=True)

    all_directives = data.get("directives", [])
    if not all_directives:
        cprint("This file contains no directives.", TColors.YELLOW)
        return ['back']

    # 1. Sort: Order by name (alphabetical)
    sorted_directives = sorted(
        all_directives,
        key=lambda d: d.get('name', '').lower()
    )

    # 2. Grouping: Separate based on disabled status
    enabled_directives = [d for d in sorted_directives if not d.get('disabled')]
    disabled_directives = [d for d in sorted_directives if d.get('disabled')]

    # Combine lists for index mapping
    display_directives_list = enabled_directives + disabled_directives
    directive_ids = [d.get('id') for d in display_directives_list]

    # --- Display Menu ---
    i = 1 # Manual counter

    if enabled_directives:
        cprint("\n--- ACTIVE (Disabled: False) ---", TColors.GREEN, bold=True)
        for d in enabled_directives:
            # --- PERUBAHAN v21: REMOVE .ljust(10) ---
            id_str = "[ID: {}]".format(str(d.get('id', 'N/A')))
            prio_str = "[Priority: {}]".format(str(d.get('priority', 'N/A')))
            name_str = d.get('name', 'No Name')
            print("  {:3d}) {} {} {}".format(i, id_str, prio_str, name_str))
            i += 1

    if disabled_directives:
        cprint("\n--- INACTIVE (Disabled: True) ---", TColors.YELLOW, bold=True)
        for d in disabled_directives:
             # --- PERUBAHAN v21: REMOVE .ljust(10) ---
            id_str = "[ID: {}]".format(str(d.get('id', 'N/A')))
            prio_str = "[Priority: {}]".format(str(d.get('priority', 'N/A')))
            name_str = d.get('name', 'No Name')
            print("  {:3d}) {} {} {}".format(i, id_str, prio_str, name_str))
            i += 1

    # --- PERUBAHAN v19: Conditional A/Z options ---
    print("")
    prompt = "Select directive(s) (e.g., 1, 3, 5-7"
    if show_az_options:
        cprint("   A) Set ALL Active (Disable: False)", TColors.GREEN)
        cprint("   Z) Set ALL Passive (Disable: True)", TColors.YELLOW)
        prompt += ", A, Z" # Add A/Z to input hint
    cprint("   B) Back (Return to Main Menu)", bold=True)
    print("")

    # Request input
    prompt += ", or B): "
    selection_string = input("{}{}{}".format(TColors.BOLD, prompt, TColors.RESET))

    choice_lower = selection_string.strip().lower()

    if choice_lower == 'b':
        print("Returning to main menu.")
        return ['back'] # Return special list for back

    # --- PERUBAHAN v19: Handle A and Z only if displayed ---
    elif show_az_options and choice_lower == 'a':
        return ['set_all_active'] # Return special list
    elif show_az_options and choice_lower == 'z':
        return ['set_all_passive'] # Return special list

    elif not selection_string.strip():
        cprint("Input cannot be empty. Please make a selection or type 'B' to go back.", TColors.RED)
        time.sleep(1.5)
        # Return error code for empty input
        return ['input_empty']

    # Call parsing function for numbers/ranges
    parsed_indices = parse_selection(selection_string, len(display_directives_list))

    # Convert parsed indices to Directive IDs
    selected_ids = [directive_ids[idx] for idx in parsed_indices]

    if not selected_ids:
        cprint("No valid directives selected.", TColors.RED)
        time.sleep(1.5)
        # Return error code for invalid selection
        return ['invalid_selection']

    cprint("You selected {} directive(s).".format(len(selected_ids)), TColors.GREEN)
    return selected_ids # Success, return LIST OF IDs

def restart_pods(filename):
    """Restarts the frontend and relevant backend pods."""
    cprint("\n--- RESTARTING PODS ---", TColors.YELLOW, bold=True)

    # 1. Restart Frontend (always)
    cprint("Restarting {}...".format(POD_NAME), TColors.YELLOW)
    # Using delete pod is more universal than rollout restart
    fe_output = run_command(["kubectl", "delete", "pod", POD_NAME])
    print(fe_output)

    # 2. Find and restart Backend
    # Regex pattern: directives_(dsiem-backend-X)_...json
    match = re.search(r'directives_(dsiem-backend-\d+)_', filename)
    if match:
        backend_pod_name = match.group(1) # Result: "dsiem-backend-0"
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

    # Confirm restart
    confirm = input("\nRestart pods to apply changes? (y/n): ").strip().lower()
    if confirm == 'y':
        restart_pods(filename)
    else:
        cprint("Pods not restarted. Changes might not take effect immediately.", TColors.YELLOW)

    DISTRIBUTED_SUCCESSFULLY = True # Set flag to avoid showing manual instructions
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

        update_json_file(filename, data, "both", new_prio, new_dis)

        # Single objects don't have a menu loop, so ask about distribution here
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

            cprint("--- STEP 2: EDIT MENU FOR FILE ({}) ---".format(filename), bold=True)
            print("What would you like to change?")

            cprint("  1) Change Priority Only", TColors.CYAN)
            cprint("  2) Toggle Status (Active/Passive)", TColors.CYAN)
            cprint("  3) Change Priority & Toggle Status", TColors.CYAN) # <-- Menu 3 Name v18
            cprint("  4) DELETE Directive(s)", TColors.RED, bold=True)
            print("")
            cprint("  S) Done (Save Locally)", bold=True)
            cprint("  D) Distribute to Pod (Upload & Restart)", TColors.GREEN, bold=True)

            aksi = input("{}{}{}".format(TColors.BOLD, "Select Action [1, 2, 3, 4, S, D]: ", TColors.RESET)).strip().lower()

            edit_mode = ""
            if aksi == '1': edit_mode = "priority"
            elif aksi == '2': edit_mode = "toggle_status"
            elif aksi == '3': edit_mode = "priority_and_toggle" # <-- Mode v18
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

            # --- PERUBAHAN v19: Determine if A/Z options should be shown ---
            show_az = (edit_mode == "toggle_status")

            while True: # Directive Selection / Action Loop
                # 1. Re-read data for refresh
                structure, data = check_file_structure(filename)

                # 2. Display directive list & get selection
                # --- PERUBAHAN v19: Send 'show_az' ---
                selection_result = select_directives_from_file(data, show_az_options=show_az)

                # 3. Handle selection result
                if not selection_result or selection_result[0] == 'back':
                    break # Exit Selection loop -> return to Main Menu
                elif selection_result[0] in ['input_empty', 'invalid_selection']:
                    continue # Stay in Selection loop, re-display menu (error message already shown)
                elif selection_result[0] in ['set_all_active', 'set_all_passive']:
                    # Handle A/Z (Can only happen if show_az=True)
                    new_status = (selection_result[0] == 'set_all_passive')
                    status_str = "PASSIVE (Disabled: True)" if new_status else "ACTIVE (Disabled: False)"
                    cprint("\n--- CONFIRMATION ---", TColors.YELLOW, bold=True)
                    print("You are about to change ALL directives in this file to {}.".format(status_str))
                    confirm = input("Are you sure? (y/n): ").strip().lower()
                    if confirm == 'y':
                        all_ids = [d.get('id') for d in data.get("directives", []) if d.get('id') is not None]
                        if all_ids:
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

                    # --- 4. Execute Action based on 'edit_mode' from Main Menu ---
                    if edit_mode == "toggle_status":
                        toggle_directives_status(filename, data, selected_ids)
                    elif edit_mode == "delete":
                        print("") # Add spacing before confirm
                        cprint("--- CONFIRM DELETE ---", TColors.RED, bold=True)
                        print("You are about to delete the following {} directive(s):".format(len(selected_ids)))
                        print(", ".join(str(s) for s in selected_ids))
                        confirm = input("Are you sure? (y/n): ").strip().lower()
                        if confirm == 'y':
                            delete_directives_from_file(filename, data, selected_ids)
                        else:
                            cprint("Deletion cancelled.", TColors.YELLOW)
                    elif edit_mode == "priority":
                        print("") # Add spacing before input
                        cprint("--- Enter New Value for {} Selected Directive(s) ---".format(len(selected_ids)), bold=True)
                        new_prio = get_valid_input("Enter new 'priority' value: ", "priority")
                        update_json_file(filename, data, "priority", new_prio, None, ids_to_update=selected_ids)
                    elif edit_mode == "priority_and_toggle": # <-- Logic v18
                        print("") # Add spacing before input
                        cprint("--- Enter New Value for {} Selected Directive(s) ---".format(len(selected_ids)), bold=True)
                        new_prio = get_valid_input("Enter new 'priority' value: ", "priority")
                        # Update priority first
                        update_json_file(filename, data, "priority", new_prio, None, ids_to_update=selected_ids)
                        # Re-read data before toggling
                        structure, data = check_file_structure(filename)
                        # Then toggle status
                        toggle_directives_status(filename, data, selected_ids)

                    # 5. Brief pause after action completes
                    time.sleep(1.5)
                    continue # Stay in Directive Selection Loop, refresh list
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
    # PERUBAHAN v17: Added LOCAL_DIR to source path
    cprint("  kubectl cp ./{}/{} {}:{}{}".format(LOCAL_DIR, filename, POD_NAME, REMOTE_PATH, filename), TColors.YELLOW)
    print("")

    # Go back to the original directory
    os.chdir("..")

def main():
    """Main execution function."""
    # Store the starting directory
    original_dir = os.getcwd()
    try:
        check_deps()
        selected_file = setup_and_select_file()

        structure, data = check_file_structure(selected_file)
        if not structure:
            os.chdir(original_dir) # Failed to read file
            sys.exit(1)

        run_edit_session(selected_file, structure, data)

        # Only show manual instructions if NOT distributed
        if not DISTRIBUTED_SUCCESSFULLY:
            show_upload_instructions(selected_file)
        else:
            os.chdir(original_dir) # Ensure we return to start dir

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