#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from __future__ import print_function

"""
reindex_batch.py  (Python 2.7 + requests)

Fitur:
- Reindex banyak index sekaligus.
- Sumber diambil dari:
  1) GLOB/PREFIX filter (contoh: "checkpoint-smartdefense-aggregated-*")
  2) atau rentang tanggal di suffix (YYYY.MM.DD) dengan prefix tetap.
- Untuk setiap source, dest otomatis: <source> + "-reindex"
- Clone settings+mappings, apply field overrides, (opsional) cleaning IP
- Verifikasi count, forcemerge, refresh
- (Opsional) swap alias read/write per index (jika diinginkan)

Prasyarat:
  pip install requests
"""

import os, sys, time, json, re, requests
from datetime import datetime, timedelta

# ===================== KONFIGURASI =====================
OS_HOST = os.environ.get("OS_HOST", "http://opensearch:9200")
OS_USER = os.environ.get("OS_USER", "systemadm")
OS_PASS = os.environ.get("OS_PASS", "gungiov5ue9iez8Shi4O")
VERIFY_TLS = False

# --- PILIH MODE SUMBER INDEX ---
MODE = "glob"  # "glob" atau "date_range"

# Mode "glob": cocok buat prefix/wildcard
GLOB_PATTERN = "checkpoint-smartdefense-aggregated-*"  # pakai '*' (glob sederhana)

# Mode "date_range": cocok buat nama: <PREFIX><YYYY.MM.DD>
DR_PREFIX   = "checkpoint-smartdefense-aggregated-"     # tanpa tanggal
DR_START    = "2025.09.04"  # inklusif
DR_END      = "2025.10.06"  # inklusif
DR_FMT      = "%Y.%m.%d"    # pattern tanggal di index

# Reindex options
SLICES     = 4
REFRESH    = True
CONFLICTS  = "proceed"
TIMEOUT_SEC= 180
POLL_SEC   = 2

# Forcemerge
DO_FORCEMERGE     = True
MAX_NUM_SEGMENTS  = 1

# Alias opsional (kalau mau pindahin per index—biasanya tidak perlu di tahap ini)
READ_ALIAS  = ""   # contoh "logs-read" atau "" untuk nonaktif
WRITE_ALIAS = ""   # contoh "logs-write" atau "" untuk nonaktif

# Field type overrides
# Gunakan tipe: "ip", "keyword", "integer", "long", "float", "double", "boolean", "date", dll.
FIELD_TYPE_OVERRIDES = {
    "device_ip": "ip",
    "src_ips": "ip",
    "dst_ips": "ip",
    "src_port": "integer",
    "dst_port": "integer",
    "Confidence": "integer",
    "sequencenum": "integer",
}
FIELD_DATE_FORMATS = {
    # "timestamp": "yyyy-MM-dd HH:mm:ss,SSS"
}

# Cleaning IP saat reindex (pisah koma/spasi; buang non-IPv4)
ENABLE_CLEAN_IPS = True

# Safety & behavior
SKIP_IF_DEST_EXISTS = True   # kalau dest sudah ada, lewati index itu
STOP_ON_ERROR       = False  # True = hentikan batch saat 1 index gagal
# =======================================================

# Matikan warning TLS jika VERIFY_TLS=False
try:
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except Exception:
    pass

# =============== HTTP helpers ===============
def _url(path):
    if not path.startswith("/"): path = "/" + path
    return OS_HOST.rstrip("/") + path

def _req(method, path, **kwargs):
    headers = kwargs.pop("headers", {})
    headers.setdefault("Content-Type", "application/json")
    return requests.request(
        method=method, url=_url(path),
        auth=(OS_USER, OS_PASS), headers=headers,
        verify=VERIFY_TLS, timeout=TIMEOUT_SEC, **kwargs
    )

def list_indices():
    r = _req("GET", "_cat/indices?format=json")
    if r.status_code != 200:
        raise RuntimeError("Gagal _cat/indices: %s %s" % (r.status_code, r.text))
    return [row.get("index") for row in r.json() if row.get("index")]

def index_exists(index):
    return _req("HEAD", index).status_code == 200

def get_index_def(index):
    r = _req("GET", index)
    if r.status_code != 200:
        raise RuntimeError("Gagal GET index '%s': %s %s" % (index, r.status_code, r.text))
    return r.json().get(index, {})

def count_docs(index):
    r = _req("GET", "%s/_count" % index)
    if r.status_code != 200:
        raise RuntimeError("Gagal count '%s': %s %s" % (index, r.status_code, r.text))
    return r.json().get("count", 0)

def refresh_index(index):
    _req("POST", "%s/_refresh" % index)

def forcemerge_index(index, max_segments=1):
    _req("POST", "%s/_forcemerge" % index, params={"max_num_segments": str(int(max_segments))})

# =============== Mapping helpers ===============
def _ensure_path_properties(mappings, field_path):
    parts = field_path.split(".")
    cur = mappings.setdefault("properties", {})
    for p in parts[:-1]:
        cur = cur.setdefault(p, {}).setdefault("properties", {})
    return cur, parts[-1]

def apply_field_overrides(mappings, overrides, date_formats=None):
    if not overrides: return mappings
    if date_formats is None: date_formats = {}
    mappings.setdefault("properties", {})
    for path, ftype in overrides.items():
        parent_props, leaf = _ensure_path_properties(mappings, path)
        field_obj = {"type": ftype}
        if ftype == "date" and path in date_formats:
            field_obj["format"] = date_formats[path]
        old = parent_props.get(leaf, {})
        if isinstance(old, dict):
            old.update(field_obj); parent_props[leaf] = old
        else:
            parent_props[leaf] = field_obj
    return mappings

def create_index_from_src(src, dst):
    if index_exists(dst): return
    sm = get_index_def(src)
    mappings = sm.get("mappings", {}) or {}
    settings = (sm.get("settings", {}) or {}).get("index", {}) or {}
    for k in ["provided_name","uuid","version","creation_date"]:
        settings.pop(k, None)
    settings.setdefault("number_of_replicas", "1")
    mappings = apply_field_overrides(mappings, FIELD_TYPE_OVERRIDES, FIELD_DATE_FORMATS)
    body = {"settings": settings, "mappings": mappings}
    r = _req("PUT", dst, data=json.dumps(body))
    if r.status_code not in (200,201):
        raise RuntimeError("Gagal create '%s': %s %s" % (dst, r.status_code, r.text))

# =============== Reindex helpers ===============
def build_reindex_script(overrides, enable_clean_ips):
    if not enable_clean_ips: return None
    ip_fields = [k for k, v in overrides.items() if v == "ip"]
    if not ip_fields: return None
    src = r"""
      def ipFields = params.ip_fields;
      def ipv4 = /^(?:\d{1,3}\.){3}\d{1,3}$/;
      def delim = /[,;\s]+/;

      for (def f : ipFields) {
        def v = ctx._source[f];
        if (v == null) continue;

        def list = (v instanceof List) ? v : [v];
        def out = new ArrayList();

        for (def item : list) {
          if (item == null) continue;
          def s = item.toString();
          String[] toks = delim.split(s);
          for (int i = 0; i < toks.length; i++) {
            def tok = toks[i];
            if (tok != null && tok.length() > 0 && ipv4.matcher(tok).matches()) {
              out.add(tok);
            }
          }
        }

        if (out.size() == 0) {
          ctx._source.remove(f);
        } else if (out.size() == 1) {
          ctx._source[f] = out.get(0);
        } else {
          ctx._source[f] = out;
        }
      }
    """
    return {"lang": "painless", "source": src, "params": {"ip_fields": ip_fields}}

def start_reindex(src, dst, slices, refresh, conflicts, script=None, pipeline=None):
    dest_obj = {"index": dst}
    if pipeline: dest_obj["pipeline"] = pipeline
    body = {"source":{"index":src}, "dest":dest_obj, "conflicts":conflicts}
    if script: body["script"] = script
    params = {"wait_for_completion":"false", "slices": str(int(slices)), "refresh": "true" if refresh else "false"}
    r = _req("POST", "_reindex", params=params, data=json.dumps(body))
    if r.status_code not in (200,201):
        raise RuntimeError("Gagal mulai reindex %s -> %s: %s %s" % (src, dst, r.status_code, r.text))
    task = r.json().get("task")
    if not task: raise RuntimeError("Task ID kosong")
    return task

def wait_task(task):
    while True:
        r = _req("GET", "_tasks/%s" % task)
        if r.status_code != 200:
            raise RuntimeError("Gagal cek task: %s %s" % (r.status_code, r.text))
        data = r.json()
        if data.get("completed"): return data
        time.sleep(POLL_SEC)

# =============== Alias (opsional) ===============
def update_aliases(actions):
    r = _req("POST", "_aliases", data=json.dumps({"actions": actions}))
    if r.status_code != 200:
        raise RuntimeError("Gagal update aliases: %s %s" % (r.status_code, r.text))

def swap_aliases_atomic(src, dst, read_alias, write_alias):
    actions = []
    if read_alias:
        actions += [{"remove":{"index":src,"alias":read_alias}}, {"add":{"index":dst,"alias":read_alias}}]
    if write_alias:
        actions += [{"remove":{"index":src,"alias":write_alias}}, {"add":{"index":dst,"alias":write_alias}}]
    if actions:
        update_aliases(actions)

# =============== Selection helpers ===============
def glob_to_regex(glob_pattern):
    # sangat sederhana: * -> .*
    pattern = "^" + re.escape(glob_pattern).replace("\\*", ".*") + "$"
    return re.compile(pattern)

def enumerate_date_range(prefix, start_str, end_str, fmt):
    start = datetime.strptime(start_str, fmt)
    end   = datetime.strptime(end_str, fmt)
    if end < start: start, end = end, start
    cur = start
    out = []
    while cur <= end:
        out.append(prefix + cur.strftime(fmt))
        cur += timedelta(days=1)
    return out

# =============== main ===============
def main():
    # 1) ambil semua index yang ada
    try:
        all_idx = list_indices()
    except Exception as e:
        sys.stderr.write("ERROR list indices: %s\n" % e)
        return 1

    # 2) pilih sources sesuai MODE
    if MODE == "glob":
        rx = glob_to_regex(GLOB_PATTERN)
        sources = sorted([i for i in all_idx if rx.match(i)])
    elif MODE == "date_range":
        candidates = enumerate_date_range(DR_PREFIX, DR_START, DR_END, DR_FMT)
        # pilih hanya yang ada
        have = set(all_idx)
        sources = [i for i in candidates if i in have]
    else:
        sys.stderr.write("MODE tidak dikenal: %s\n" % MODE); return 1

    if not sources:
        print("Tidak ada index yang cocok dengan seleksi (%s)." % MODE)
        return 0

    print("Ditemukan %d index sumber." % len(sources))

    # 3) proses satu per satu
    reindex_script = build_reindex_script(FIELD_TYPE_OVERRIDES, ENABLE_CLEAN_IPS)

    failures = 0
    for src in sources:
        dst = src + "-reindex"
        print("\n=== Reindex pair ===\nSRC: %s\nDST: %s" % (src, dst))

        try:
            if not index_exists(src):
                print("  - SKIP: source tidak ada."); continue
            if SKIP_IF_DEST_EXISTS and index_exists(dst):
                print("  - SKIP: dest sudah ada (idempotent)."); continue

            # create dest from src (clone + overrides)
            create_index_from_src(src, dst)

            # counts (pre)
            try: src_before = count_docs(src)
            except: src_before = None
            try: dst_before = count_docs(dst)
            except: dst_before = None
            print("  - Docs source=%s dest(sebelum)=%s" % (src_before, dst_before))

            # start reindex
            task = start_reindex(src, dst, SLICES, REFRESH, CONFLICTS, script=reindex_script)
            print("  - Task:", task)

            # wait
            done = wait_task(task)
            summary = done.get("response") or done.get("task", {})
            print("  - Reindex selesai:")
            try: print(json.dumps(summary, indent=2))
            except: print(summary)

            # verify
            try:
                src_after = count_docs(src)
                dst_after = count_docs(dst)
            except Exception as e:
                src_after = dst_after = None
                print("  ! Gagal verifikasi: %s" % e)
            print("  - Docs source(akhir)=%s dest(akhir)=%s" % (src_after, dst_after))
            if src_after is not None and dst_after is not None and dst_after < src_after:
                print("  ! PERINGATAN: dest < source (periksa cleaning/konflik)")

            # forcemerge + refresh
            if DO_FORCEMERGE:
                try:
                    forcemerge_index(dst, MAX_NUM_SEGMENTS)
                    print("  - Forcemerge OK")
                except Exception as e:
                    print("  ! Forcemerge gagal (non-fatal): %s" % e)
            try: refresh_index(dst)
            except Exception as e: print("  ! Refresh gagal (non-fatal): %s" % e)

            # (opsional) swap alias read/write
            if READ_ALIAS or WRITE_ALIAS:
                try:
                    swap_aliases_atomic(src, dst, READ_ALIAS, WRITE_ALIAS)
                    print("  - Alias dipindahkan (jika di-set).")
                except Exception as e:
                    print("  ! Swap alias gagal (non-fatal): %s" % e)

        except Exception as e:
            failures += 1
            print("  ! ERROR pada pair %s -> %s: %s" % (src, dst, e))
            if STOP_ON_ERROR:
                print("\nBatch dihentikan karena STOP_ON_ERROR=True.")
                break

    print("\nSelesai. Gagal: %d dari %d index." % (failures, len(sources)))
    return 0 if failures == 0 else 2

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.stderr.write("\nDibatalkan oleh user.\n")
        sys.exit(130)
