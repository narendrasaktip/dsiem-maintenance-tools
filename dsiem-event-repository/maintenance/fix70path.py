#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import requests
import base64
import re

# --- KONFIGURASI ---
GITHUB_REPO = os.getenv("GITHUB_REPO")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH") or "main"

WRONG_PATH = '/root/kubeappl/logstash/pipelines/dsiem-events/dsiem-plugin-json/'
CORRECT_PATH = '/etc/logstash/pipelines/dsiem-events/dsiem-plugin-json/'
FILE_PATTERN = r'70_.*\.conf$' 

def print_color(text, color="green", same_line=False):
    colors = {
        'green': '\033[92m', 'yellow': '\033[93m', 'red': '\033[91m', 
        'cyan': '\033[96m', 'blue': '\033[94m', 'reset': '\033[0m'
    }
    end_char = "\r" if same_line else "\n"
    sys.stdout.write("{}{}{}{}".format(colors.get(color, ""), text, colors['reset'], end_char))
    sys.stdout.flush()

def gh_headers():
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": "Bearer {}".format(GITHUB_TOKEN),
        "X-GitHub-Api-Version": "2022-11-28"
    }

def find_files_recursively(path=""):
    """Mencari file secara rekursif dengan PROGRESS BAR realtime"""
    display_path = path if path else "[ROOT]"
    print_color("[SCAN] Entering directory: {}".format(display_path), "blue")
    
    url = "https://api.github.com/repos/{}/contents/{}".format(GITHUB_REPO, path)
    try:
        r = requests.get(url, headers=gh_headers(), params={"ref": GITHUB_BRANCH}, timeout=30)
        if r.status_code == 404: 
            print_color("[SKIP] Path not found: {}".format(display_path), "red")
            return []
        r.raise_for_status()
        items = r.json()
    except Exception as e:
        print_color("[ERR] Error listing path '{}': {}".format(display_path, e), "red")
        return []

    found = []
    if isinstance(items, list):
        for item in items:
            if item['type'] == 'file':
                # Cek apakah nama file sesuai pola 70_*.conf
                if re.match(FILE_PATTERN, item['name']):
                    print_color("[FOUND] Candidate found: {}".format(item['path']), "yellow")
                    found.append(item)
            elif item['type'] == 'dir':
                # Rekursif ke folder anak
                found.extend(find_files_recursively(item['path']))
    return found

def update_file(file_meta):
    path = file_meta['path']
    print_color("\n[CHECK] Processing: {}".format(path), "cyan")

    # 1. Download Content
    try:
        sys.stdout.write("        Downloading content... ")
        sys.stdout.flush()
        r = requests.get(file_meta['url'], headers=gh_headers(), timeout=30)
        r.raise_for_status()
        data = r.json()
        content_b64 = data['content']
        sha = data['sha']
        content_str = base64.b64decode(content_b64).decode('utf-8')
        print_color("OK", "green")
    except Exception as e:
        print_color("FAIL ({})".format(e), "red")
        return

    # 2. Cek dan Replace
    if WRONG_PATH in content_str:
        print_color("        [MATCH] Found wrong path! Fixing...", "yellow")
        new_content_str = content_str.replace(WRONG_PATH, CORRECT_PATH)
        
        # 3. Push Changes
        url = "https://api.github.com/repos/{}/contents/{}".format(GITHUB_REPO, path)
        payload = {
            "message": "[Auto-Fix] Update dictionary_path to /etc/logstash/...",
            "content": base64.b64encode(new_content_str.encode('utf-8')).decode('ascii'),
            "branch": GITHUB_BRANCH,
            "sha": sha
        }
        
        try:
            sys.stdout.write("        Pushing to GitHub... ")
            sys.stdout.flush()
            p = requests.put(url, headers=gh_headers(), data=json.dumps(payload), timeout=30)
            p.raise_for_status()
            print_color("SUCCESS", "green")
        except Exception as e:
            print_color("FAIL ({})".format(e), "red")
    else:
        print_color("        [SKIP] Path already correct or not found.", "blue")

def main():
    if not all([GITHUB_REPO, GITHUB_TOKEN]):
        print_color("Error: Env var GITHUB_REPO dan GITHUB_TOKEN harus di-set.", "red")
        sys.exit(1)

    print_color("=== STARTING MASS FIX PATH 70_*.CONF ===", "green")
    print("Repo: {}".format(GITHUB_REPO))
    print("Target Replace: '{}' -> '{}'".format(WRONG_PATH, CORRECT_PATH))
    print("-" * 60)
    
    # Proses Scan akan langsung print output saat berjalan
    files = find_files_recursively("")
    
    print("-" * 60)
    print_color("Scan Complete. Found {} files to process.".format(len(files)), "green")
    print("-" * 60)

    # Proses Update
    for f in files:
        update_file(f)

    print_color("\n=== SELESAI ===", "green")

if __name__ == "__main__":
    main()