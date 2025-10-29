#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from __future__ import print_function

"""
auto_replace_reindexed.py (Python 2.7 + requests)

Tujuan:
- Untuk setiap index yang berakhiran '-reindex', jadikan dia sebagai "pengganti" nama asli:
  <BASE>-reindex  ==>  <BASE> (fisik)
- Langkah:
  - Hapus index <BASE> kalau masih ada (dan hapus alias bernama <BASE> kalau ada)
  - Buat index <BASE> meniru mapping/settings dari <BASE>-reindex
  - _reindex dari <BASE>-reindex ke <BASE>
  - Pindahkan semua alias yang nempel di <BASE>-reindex ke <BASE>
  - Hapus <BASE>-reindex (nama '-reindex' hilang)

Prasyarat:
  pip install requests
"""

import sys
import time
import json
import requests

# ===================== KONFIGURASI =====================
OS_HOST    = "http://opensearch:9200"         # <-- ganti
OS_USER    = "systemadm"                       # <-- ganti
OS_PASS    = "gungiov5ue9iez8Shi4O"            # <-- ganti
VERIFY_TLS = False

SUFFIX     = "-reindex"
SLICES     = 4
REFRESH    = True
CONFLICTS  = "proceed"
WAIT_POLL  = 2
TIMEOUT    = 180

DO_FORCEMERGE   = True
MAX_NUM_SEGMENTS= 1

# Safety:
FORCE_PROCEED   = True   # True: lanjut meski dest<count (biasanya tidak terjadi)
DRY_RUN         = False  # True: tampilkan rencana, tidak eksekusi
# =======================================================

# Matikan warning TLS jika VERIFY_TLS=False
try:
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except Exception:
    pass

# ---------------- HTTP helpers ----------------
def _url(path):
    if not path.startswith("/"):
        path = "/" + path
    return OS_HOST.rstrip("/") + path

def _req(method, path, **kwargs):
    headers = kwargs.pop("headers", {})
    headers.setdefault("Content-Type", "application/json")
    return requests.request(
        method=method,
        url=_url(path),
        auth=(OS_USER, OS_PASS),
        headers=headers,
        verify=VERIFY_TLS,
        timeout=TIMEOUT,
        **kwargs
    )

def _is_index(name):
    return _req("HEAD", name).status_code == 200

def _is_alias(name):
    r = _req("GET", "_alias/%s" % name)
    if r.status_code == 404:
        return False
    if r.status_code != 200:
        return False
    data = r.json()
    for idx, v in data.items():
        als = (v.get("aliases") or {})
        if name in als:
            return True
    return False

def _alias_backing_index(alias_name):
    r = _req("GET", "_alias/%s" % alias_name)
    if r.status_code != 200:
        return None
    data = r.json()
    for idx, v in data.items():
        if alias_name in (v.get("aliases") or {}):
            return idx
    return None

def _list_indices():
    r = _req("GET", "_cat/indices?format=json")
    if r.status_code != 200:
        raise RuntimeError("Gagal ambil daftar index: %s %s" % (r.status_code, r.text))
    return [row.get("index") for row in r.json() if row.get("index")]

def _count(index):
    r = _req("GET", "%s/_count" % index)
    if r.status_code != 200:
        raise RuntimeError("Gagal count %s: %s %s" % (index, r.status_code, r.text))
    return r.json().get("count", 0)

def _post_aliases(actions_body):
    if DRY_RUN:
        print("DRY-RUN _aliases payload:\n%s" % json.dumps(actions_body, indent=2))
        return 200, '{"dry_run":"ok"}'
    r = _req("POST", "_aliases", data=json.dumps(actions_body))
    return r.status_code, r.text

def _delete_index(name):
    if DRY_RUN:
        print("DRY-RUN: DELETE %s" % name)
        return 200, '{"dry_run":"ok"}'
    r = _req("DELETE", name)
    return r.status_code, r.text

def _get_index_def(idx):
    r = _req("GET", idx)
    if r.status_code != 200:
        raise RuntimeError("GET index %s gagal: %s %s" % (idx, r.status_code, r.text))
    return r.json()[idx]

def _create_index_like(src_idx, dst_idx):
    if _is_index(dst_idx):
        return
    sm = _get_index_def(src_idx)
    mappings = sm.get("mappings", {}) or {}
    settings = (sm.get("settings", {}) or {}).get("index", {}) or {}
    for k in ["uuid", "provided_name", "version", "creation_date"]:
        settings.pop(k, None)
    settings.setdefault("number_of_replicas", "1")
    body = {"settings": settings, "mappings": mappings}
    if DRY_RUN:
        print("DRY-RUN: CREATE %s like %s\n%s" % (dst_idx, src_idx, json.dumps(body, indent=2)))
        return
    r = _req("PUT", dst_idx, data=json.dumps(body))
    if r.status_code not in (200, 201):
        raise RuntimeError("Create %s gagal: %s %s" % (dst_idx, r.status_code, r.text))

def _start_reindex(src, dst):
    body = {"source": {"index": src}, "dest": {"index": dst}, "conflicts": CONFLICTS}
    params = {"wait_for_completion": "false", "slices": str(SLICES), "refresh": str(REFRESH).lower()}
    if DRY_RUN:
        print("DRY-RUN: _reindex %s -> %s" % (src, dst))
        return "dry-run-task-id"
    r = _req("POST", "_reindex", data=json.dumps(body), params=params)
    if r.status_code not in (200, 201):
        raise RuntimeError("Mulai reindex gagal: %s %s" % (r.status_code, r.text))
    task = r.json().get("task")
    if not task:
        raise RuntimeError("Task ID kosong")
    return task

def _wait_task(task_id):
    if task_id == "dry-run-task-id":
        return {"response": {"created": 0, "total": 0}}
    while True:
        r = _req("GET", "_tasks/%s" % task_id)
        if r.status_code != 200:
            raise RuntimeError("Cek task gagal: %s %s" % (r.status_code, r.text))
        data = r.json()
        if data.get("completed"):
            return data
        time.sleep(WAIT_POLL)

def _forcemerge(index):
    if DRY_RUN:
        print("DRY-RUN: forcemerge %s" % index)
        return
    _req("POST", "%s/_forcemerge" % index, params={"max_num_segments": str(MAX_NUM_SEGMENTS)})
    _req("POST", "%s/_refresh" % index)

def _aliases_of_index(index_name):
    r = _req("GET", "%s/_alias" % index_name)
    if r.status_code == 404:
        return []
    if r.status_code != 200:
        return []
    data = r.json()
    if index_name not in data:
        return []
    aliases = data[index_name].get("aliases", {}) or {}
    return sorted(list(aliases.keys()))

def _reassign_all_aliases(from_index, to_index):
    aliases = _aliases_of_index(from_index)
    if not aliases:
        return 200, '{"aliases":"none"}'
    acts = []
    for a in aliases:
        acts.append({"remove": {"index": from_index, "alias": a}})
        acts.append({"add":    {"index": to_index,   "alias": a}})
    return _post_aliases({"actions": acts})

# ---------------- Core ----------------
def _process_reindex_index(reidx_name):
    base = reidx_name[:-len(SUFFIX)]
    print("\n=== Finalize ===\nSRC(-reindex): %s\nDEST(final):   %s" % (reidx_name, base))

    # pastikan sumber ada
    if not _is_index(reidx_name):
        print("  - SKIP: %s tidak ditemukan." % reidx_name)
        return

    # kalau base adalah alias, hapus alias dulu (agar bisa bikin index fisik)
    if _is_alias(base):
        backing = _alias_backing_index(base)
        print("  - '%s' adalah ALIAS (-> %s). Hapus alias dulu." % (base, backing))
        code, text = _post_aliases({"actions": [{"remove": {"index": backing, "alias": base}}]})
        print("  - Hapus alias status: %s %s" % (code, text[:200]))
        if code != 200 and not DRY_RUN:
            print("  ! ERROR: gagal hapus alias '%s'." % base); return

    # kalau sudah ada index fisik bernama base → hapus agar tidak konflik
    if _is_index(base):
        print("  - Hapus index lama '%s'." % base)
        code, text = _delete_index(base)
        print("  - Delete status: %s %s" % (code, text[:200]))
        if code not in (200, 202) and not DRY_RUN:
            print("  ! ERROR: gagal hapus index lama. Stop."); return

    # create index base meniru mapping/settings reidx
    print("  - Create index '%s' meniru mapping %s" % (base, reidx_name))
    _create_index_like(reidx_name, base)

    # reindex data reidx -> base
    try:
        src_cnt = _count(reidx_name)
    except Exception as e:
        print("  ! Gagal count src: %s" % e); src_cnt = None

    print("  - Mulai reindex %s -> %s (docs src=%s)" % (reidx_name, base, src_cnt))
    task = _start_reindex(reidx_name, base)
    print("  - Task:", task)
    done = _wait_task(task)
    summary = done.get("response", {})
    print("  - Reindex selesai:", json.dumps(summary, indent=2))

    # verifikasi
    try:
        dst_cnt = _count(base)
    except Exception as e:
        print("  ! Gagal count dest: %s" % e); dst_cnt = None
    print("  - Docs dest=%s" % dst_cnt)
    if (src_cnt is not None and dst_cnt is not None and dst_cnt < src_cnt and not FORCE_PROCEED):
        print("  ! PERINGATAN: dest < src. Stop hapus -reindex."); return

    # forcemerge opsional
    if DO_FORCEMERGE:
        print("  - Forcemerge %s" % base)
        _forcemerge(base)

    # pindahkan semua alias dari reidx ke base
    print("  - Reassign alias dari %s ke %s" % (reidx_name, base))
    code, text = _reassign_all_aliases(reidx_name, base)
    print("  - Reassign alias status: %s %s" % (code, text[:200]))

    # hapus reidx (supaya '-reindex' hilang)
    print("  - Hapus index %s" % reidx_name)
    code, text = _delete_index(reidx_name)
    print("  - Delete -reindex status: %s %s" % (code, text[:200]))

def main():
    # cari kandidat -reindex
    try:
        all_idx = _list_indices()
    except Exception as e:
        print("ERROR list indices:", e); return 1

    candidates = sorted([i for i in all_idx if i.endswith(SUFFIX)])
    if not candidates:
        print("Tidak ada index berakhiran '%s'." % SUFFIX); return 0

    print("Ditemukan %d index '%s'." % (len(candidates), SUFFIX))
    for reidx in candidates:
        _process_reindex_index(reidx)

    print("\nSelesai.")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nDibatalkan.")
        sys.exit(130)