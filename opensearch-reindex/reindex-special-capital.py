#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from __future__ import print_function

"""
reindex_batch_annotated.py 

Script Reindex OpenSearch/Elasticsearch dengan anotasi lengkap.
Digunakan untuk:
1. Memperbaiki tipe data (misal: Object menjadi Text).
2. Membersihkan field yang namanya mengandung titik (dot notation conflict).
3. Membersihkan data IP Address (opsional).
4. Menampilkan progress bar real-time.

Cara pakai:
- Edit bagian [USER CONFIG] sesuai kebutuhan.
- Jalankan: python reindex_batch.py
"""

import os, sys, time, json, re, requests
from datetime import datetime, timedelta

# ==============================================================================
# BAGIAN 1: KONEKSI & AUTENTIKASI
# ==============================================================================
# [USER CONFIG] Ganti URL dan Credential di sini
# Bisa via Environment Variable atau hardcode langsung.
OS_HOST = os.environ.get("OS_HOST", "http://opensearch:9200")
OS_USER = os.environ.get("OS_USER", "systemadm") 
OS_PASS = os.environ.get("OS_PASS", "p@f2!vaG2M-IIS]TnKnK?mT<") 
VERIFY_TLS = False # Set True jika menggunakan HTTPS dengan sertifikat valid

# ==============================================================================
# BAGIAN 2: SELEKSI INDEX (SUMBER)
# ==============================================================================
# [USER CONFIG] Pilih salah satu mode: "glob" atau "date_range"
MODE = "date_range"  

# --- OPSI A: Mode GLOB ---
# Cocok untuk pola nama bebas. Contoh: "app-logs-*" atau "syslog-2025*"
GLOB_PATTERN = "microservices-*"  

# --- OPSI B: Mode DATE RANGE ---
# Cocok untuk index harian dengan pola: prefix + tanggal
# Contoh: microservices-2025.09.21
DR_PREFIX     = "microservices-"      
DR_START      = "2025.09.21"  # Tanggal Awal (Inklusif)
DR_END        = "2025.09.21"  # Tanggal Akhir (Inklusif)
DR_FMT        = "%Y.%m.%d"    # Format tanggal pada nama index (misal: %Y-%m-%d atau %Y.%m.%d)

# ==============================================================================
# BAGIAN 3: KONFIGURASI PERFORMA REINDEX
# ==============================================================================
# [USER CONFIG] Tuning performa
SLICES     = 4         # Jumlah parallel workers. Saran: Jumlah Shard atau Jumlah CPU.
REFRESH    = True      # True = Index langsung bisa disearch setelah selesai (sedikit lebih lambat).
CONFLICTS  = "proceed" # "proceed" = Jangan berhenti jika ada version conflict (data duplikat).
TIMEOUT_SEC= 180       # Timeout koneksi HTTP (detik).
POLL_SEC   = 2         # Seberapa sering script mengecek status task (detik).

# [USER CONFIG] Forcemerge
# Berguna untuk menghemat disk space setelah reindex selesai.
DO_FORCEMERGE     = True 
MAX_NUM_SEGMENTS  = 1    # 1 segment = performa search paling cepat.

# [USER CONFIG] Alias (Opsional)
# Jika diisi, script akan memindahkan alias dari index lama ke index baru.
READ_ALIAS  = ""   # Contoh: "logs-read"
WRITE_ALIAS = ""   # Contoh: "logs-write"

# ==============================================================================
# BAGIAN 4: MAPPING & DATA TYPE OVERRIDES (PENTING!)
# ==============================================================================
# [USER CONFIG] Daftar field yang ingin DIPAKSA tipe datanya.
#
# Masalah Umum: "Mapper Parsing Exception"
# Solusi: Tulis path field di sini dan tentukan tipenya.
# Tipe valid: "text", "keyword", "integer", "long", "float", "boolean", "ip", "date"
#
# NOTE: Script otomatis akan menghapus "properties" dari mapping lama 
# jika Anda mengubah tipe Object menjadi tipe primitif (text/int) di sini.
FIELD_TYPE_OVERRIDES = {
    "protoPayload.request.metadata.labels.app": "text",
    "protoPayload.request.spec.ports.targetPort": "integer",
    "protoPayload.request.spec.selector.matchLabels.app": "text",
    "protoPayload.request.spec.template.metadata.labels.app": "text",
    "protoPayload.request.spec.template.spec.containers.livenessProbe.httpGet.port": "integer",
    "protoPayload.response.metadata.labels.app": "text",
    "protoPayload.response.spec.ports.targetPort": "integer",
    "protoPayload.response.spec.selector.app": "text",
    "protoPayload.response.spec.selector.matchLabels.app": "text",
    "protoPayload.response.spec.template.metadata.labels.app": "text",
    "protoPayload.response.spec.template.spec.containers.livenessProbe.httpGet.port": "integer"
}

FIELD_DATE_FORMATS = {
    # Contoh jika ada field date custom:
    # "timestamp_field": "yyyy-MM-dd HH:mm:ss"
}

# [USER CONFIG] Fitur Cleaning IP
# Jika True, field yang ditandai sebagai "ip" di atas akan dicek regex IPv4.
# Jika bukan IP valid, data field itu akan dibuang (agar tidak error saat masukin ke index).
ENABLE_CLEAN_IPS = True

# ==============================================================================
# BAGIAN 5: SAFETY & LOGIC
# ==============================================================================
SKIP_IF_DEST_EXISTS = True   # Aman: Skip jika index tujuan sudah ada.
STOP_ON_ERROR       = False  # Aman: Jika satu index gagal, lanjut ke index berikutnya.

# ------------------------------------------------------------------------------
# KODE PROGRAM (Hanya ubah jika Anda paham Python/OpenSearch API)
# ------------------------------------------------------------------------------

# Matikan warning TLS (Self-signed cert)
try:
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except Exception:
    pass

# --- HTTP Helper Functions ---
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
    print("[INFO] Mengambil daftar index...")
    r = _req("GET", "_cat/indices?format=json")
    if r.status_code != 200:
        raise RuntimeError("Gagal _cat/indices: %s" % r.text)
    return [row.get("index") for row in r.json() if row.get("index")]

def index_exists(index):
    return _req("HEAD", index).status_code == 200

def get_index_def(index):
    r = _req("GET", index)
    return r.json().get(index, {})

def count_docs(index):
    r = _req("GET", "%s/_count" % index)
    return r.json().get("count", 0)

def refresh_index(index):
    print("  - Refreshing index...")
    _req("POST", "%s/_refresh" % index)

def forcemerge_index(index, max_segments=1):
    print("  - Memulai Forcemerge (max_segments=%s)..." % max_segments)
    _req("POST", "%s/_forcemerge" % index, params={"max_num_segments": str(int(max_segments))})

# --- Mapping Logic ---
def _ensure_path_properties(mappings, field_path):
    # Fungsi helper untuk membuat struktur nested dict (properties) berdasarkan string path
    parts = field_path.split(".")
    cur = mappings.setdefault("properties", {})
    for p in parts[:-1]:
        cur = cur.setdefault(p, {}).setdefault("properties", {})
    return cur, parts[-1]

def apply_field_overrides(mappings, overrides, date_formats=None):
    """
    Fungsi ini memodifikasi JSON Mapping dari index lama.
    1. Menerapkan tipe data baru sesuai USER CONFIG.
    2. [PENTING] Menghapus key 'properties' jika tipe diubah jadi text/integer
       untuk mencegah error 'Mapper Parsing Exception'.
    """
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
            # [LOGIC] Bersihkan properties sisa (Fix Conflict Object vs Text)
            if "properties" in old: del old["properties"]
            if "dynamic" in old: del old["dynamic"]
            old.update(field_obj)
            parent_props[leaf] = old
        else:
            parent_props[leaf] = field_obj
    return mappings

def create_index_from_src(src, dst):
    if index_exists(dst): return
    print("  - Membuat index tujuan dengan mapping baru...")
    sm = get_index_def(src)
    mappings = sm.get("mappings", {}) or {}
    settings = (sm.get("settings", {}) or {}).get("index", {}) or {}
    
    # Hapus metadata internal yang tidak boleh dicopy
    for k in ["provided_name","uuid","version","creation_date"]:
        settings.pop(k, None)
    
    # [USER CONFIG] Default Replicas
    settings.setdefault("number_of_replicas", "1")
    
    # [FIX] NAIKKAN LIMIT FIELD AGAR TIDAK ERROR "Limit exceeded"
    # Kita set ke 10.000 (aman untuk sementara)
    settings["mapping"] = {"total_fields": {"limit": 10000}}
    
    # Apply perubahan mapping
    mappings = apply_field_overrides(mappings, FIELD_TYPE_OVERRIDES, FIELD_DATE_FORMATS)
    
    body = {"settings": settings, "mappings": mappings}
    r = _req("PUT", dst, data=json.dumps(body))
    if r.status_code not in (200,201):
        raise RuntimeError("Gagal create index: %s" % r.text)

def build_reindex_script(overrides, enable_clean_ips):
    """
    [UPDATED] Menambahkan sanitasi untuk path response.spec.template.metadata.labels
    """
    ip_fields = [k for k, v in overrides.items() if v == "ip"] if enable_clean_ips else []
    
    src = r"""
      // Helper: Rename keys with dots to underscores
      void sanitizeMap(def map) {
        if (map == null || !(map instanceof Map)) return;
        def keysToRename = new ArrayList();
        for (def key : map.keySet()) { if (key.contains(".")) keysToRename.add(key); }
        for (def key : keysToRename) {
          def newKey = key.replace(".", "_").replace("/", "_");
          map[newKey] = map.remove(key);
        }
      }

      if (ctx._source.protoPayload != null) {
        def pp = ctx._source.protoPayload;
        
        // 1. Sanitasi Request Labels
        if (pp.request != null) {
            if (pp.request.metadata != null) sanitizeMap(pp.request.metadata.labels);
            if (pp.request.spec != null) {
                if (pp.request.spec.selector != null) sanitizeMap(pp.request.spec.selector.matchLabels);
                if (pp.request.spec.template != null && pp.request.spec.template.metadata != null) {
                    sanitizeMap(pp.request.spec.template.metadata.labels);
                }
            }
        }
        
        // 2. Sanitasi Response Labels
        if (pp.response != null) {
             if (pp.response.metadata != null) sanitizeMap(pp.response.metadata.labels);
             if (pp.response.spec != null) {
                if (pp.response.spec.selector != null) sanitizeMap(pp.response.spec.selector.matchLabels);
                
                // [FIX] Bagian ini ditambahkan karena error terakhir berasal dari sini
                if (pp.response.spec.template != null && pp.response.spec.template.metadata != null) {
                    sanitizeMap(pp.response.spec.template.metadata.labels);
                }
             }
        }
      }

      // Logika Cleaning IP
      def ipFields = params.ip_fields;
      if (ipFields != null && ipFields.size() > 0) {
          def ipv4 = /^(?:\d{1,3}\.){3}\d{1,3}$/; 
          def delim = /[,;\s]+/;
          for (def f : ipFields) {
            if (!ctx._source.containsKey(f)) continue;
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
                if (tok != null && tok.length() > 0 && ipv4.matcher(tok).matches()) out.add(tok);
              }
            }
            if (out.size() == 0) ctx._source.remove(f);
            else if (out.size() == 1) ctx._source[f] = out.get(0);
            else ctx._source[f] = out;
          }
      }
    """
    return {"lang": "painless", "source": src, "params": {"ip_fields": ip_fields}}

def start_reindex(src, dst, slices, refresh, conflicts, script=None):
    print("  - Mengirim perintah reindex ke server...")
    dest_obj = {"index": dst}
    body = {"source":{"index":src}, "dest":dest_obj, "conflicts":conflicts}
    if script: body["script"] = script
    params = {"wait_for_completion":"false", "slices": str(int(slices)), "refresh": "true" if refresh else "false"}
    
    r = _req("POST", "_reindex", params=params, data=json.dumps(body))
    if r.status_code not in (200,201):
        raise RuntimeError("Gagal mulai reindex: %s" % r.text)
    task = r.json().get("task")
    if not task: raise RuntimeError("Task ID kosong")
    return task

def wait_task(task):
    # [LOGIC] Fungsi Monitoring Progress Bar
    start_time = time.time()
    print("  - Menunggu proses reindex selesai...")
    
    while True:
        r = _req("GET", "_tasks/%s" % task)
        if r.status_code != 200:
            raise RuntimeError("Gagal cek task: %s" % r.text)
        
        data = r.json()
        if data.get("completed"): 
            sys.stdout.write("\n") 
            return data
        
        task_info = data.get("task", {})
        status = task_info.get("status", {})
        total_ops = status.get("total", 0)
        current_ops = status.get("created", 0) + status.get("updated", 0) + status.get("deleted", 0)
        elapsed = time.time() - start_time
        percent = (float(current_ops) / float(total_ops) * 100.0) if total_ops > 0 else 0.0
        
        # Menulis ulang baris yang sama (\r)
        msg = "\r    > Progress: %d/%d docs (%.2f%%) | Elapsed: %ds" % (current_ops, total_ops, percent, int(elapsed))
        sys.stdout.write(msg)
        sys.stdout.flush()
        time.sleep(POLL_SEC)

def update_aliases(actions):
    r = _req("POST", "_aliases", data=json.dumps({"actions": actions}))
    if r.status_code != 200: raise RuntimeError("Gagal update aliases: %s" % r.text)

def swap_aliases_atomic(src, dst, read_alias, write_alias):
    print("  - Swapping aliases...")
    actions = []
    if read_alias:
        actions += [{"remove":{"index":src,"alias":read_alias}}, {"add":{"index":dst,"alias":read_alias}}]
    if write_alias:
        actions += [{"remove":{"index":src,"alias":write_alias}}, {"add":{"index":dst,"alias":write_alias}}]
    if actions: update_aliases(actions)

# --- Main Execution Flow ---
def glob_to_regex(glob_pattern):
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

def main():
    # 1. Ambil daftar index
    try:
        all_idx = list_indices()
    except Exception as e:
        sys.stderr.write("ERROR list indices: %s\n" % e)
        return 1

    # 2. Filter index berdasarkan MODE
    if MODE == "glob":
        rx = glob_to_regex(GLOB_PATTERN)
        sources = sorted([i for i in all_idx if rx.match(i)])
    elif MODE == "date_range":
        candidates = enumerate_date_range(DR_PREFIX, DR_START, DR_END, DR_FMT)
        have = set(all_idx)
        sources = [i for i in candidates if i in have]
    else:
        sys.stderr.write("MODE tidak dikenal: %s\n" % MODE); return 1

    if not sources:
        print("Tidak ada index yang cocok dengan seleksi (%s)." % MODE)
        return 0

    print("[INFO] Ditemukan %d index sumber." % len(sources))
    
    # Siapkan script painless
    reindex_script = build_reindex_script(FIELD_TYPE_OVERRIDES, ENABLE_CLEAN_IPS)
    failures = 0
    
    # 3. Loop setiap index
    for src in sources:
        dst = src + "-reindex"
        print("\n" + "="*50)
        print("PROSES: %s -> %s" % (src, dst))
        print("="*50)

        try:
            if not index_exists(src):
                print("  - SKIP: source tidak ada."); continue
            if SKIP_IF_DEST_EXISTS and index_exists(dst):
                print("  - SKIP: dest sudah ada (idempotent)."); continue

            # Step A: Create Index Tujuan
            create_index_from_src(src, dst)

            try: src_docs = count_docs(src)
            except: src_docs = 0
            print("  - Total Dokumen Source: %d" % src_docs)

            # Step B: Mulai Reindex
            task_id = start_reindex(src, dst, SLICES, REFRESH, CONFLICTS, script=reindex_script)
            print("  - Task ID: %s" % task_id)

            # Step C: Tunggu & Monitor
            done = wait_task(task_id)
            
            # Cek Error
            summary = done.get("response") or done.get("task", {})
            failures_list = summary.get("failures", [])
            if len(failures_list) > 0:
                print("\n  ! ADA FAILURES DI DALAM TASK:")
                print(json.dumps(failures_list[:2], indent=2)) 
            
            # Step D: Verifikasi Jumlah Data
            try:
                dst_docs = count_docs(dst)
            except Exception as e:
                dst_docs = 0
                print("  ! Gagal verifikasi: %s" % e)
            
            print("  - Hasil Akhir: Source=%d, Dest=%d" % (src_docs, dst_docs))
            
            # Step E: Optimization
            if DO_FORCEMERGE:
                try: forcemerge_index(dst, MAX_NUM_SEGMENTS); print("  - Forcemerge selesai.")
                except Exception as e: print("  ! Forcemerge gagal (non-fatal): %s" % e)
            
            try: refresh_index(dst)
            except: pass

            # Step F: Alias Swap (Opsional)
            if READ_ALIAS or WRITE_ALIAS:
                try:
                    swap_aliases_atomic(src, dst, READ_ALIAS, WRITE_ALIAS)
                    print("  - Alias berhasil dipindahkan.")
                except Exception as e:
                    print("  ! Swap alias gagal: %s" % e)

        except Exception as e:
            failures += 1
            print("\n  ! ERROR FATAL pada pair ini: %s" % e)
            if STOP_ON_ERROR:
                print("\nBatch dihentikan paksa."); break

    print("\n" + "="*50)
    print("BATCH SELESAI. Gagal: %d dari %d index." % (failures, len(sources)))
    return 0 if failures == 0 else 2

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.stderr.write("\n[ABORT] Dibatalkan oleh user.\n")
        sys.exit(130)
