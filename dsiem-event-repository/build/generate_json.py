# -*- coding: utf-8 -*-
import openpyxl  # Library untuk baca Excel
import json
import sys       # Untuk keluar program jika error
import csv       # Untuk membaca CSV sebagai fallback
# --- Tambahkan import yang diperlukan ---
from collections import OrderedDict # Untuk menjaga urutan field saat dump
import io # Untuk file handling unicode

# --- Definisi fungsi save_json_utf8 ---
def save_json_utf8(path, obj):
    """
    Python2/3-safe JSON writer: dumps with ensure_ascii=False and writes unicode.
    """
    import io as _io
    import json as _json
    try:
        # Check if obj is OrderedDict for direct dump without sorting keys
        if isinstance(obj, OrderedDict):
            data = _json.dumps(obj, ensure_ascii=False, indent=2)
        else:
            # Fallback ke sort_keys jika bukan OrderedDict
            data = _json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception as dump_err: # Tangkap error saat dump
        print(f"[ERROR] Gagal saat serialisasi JSON ke {path}: {dump_err}")
        raise dump_err # Hentikan proses jika dump gagal

    try:
        unicode  # noqa: F821 (py3 ignores)
        if isinstance(data, str): # Jika bytes di Py2
            data = data.decode("utf-8")
    except NameError:
        pass # Py3 sudah str (unicode)

    try:
        with _io.open(path, "w", encoding="utf-8") as f:
            f.write(data)
            f.write(u"\n") # Tambah newline di akhir
    except IOError as io_err:
        print(f"[ERROR] Gagal membuka/menulis file {path}: {io_err}")
        raise io_err # Hentikan jika gagal tulis
    except Exception as e:
        print(f"[ERROR] Kesalahan tak terduga saat menyimpan JSON ke {path}: {e}")
        raise e # Hentikan jika error lain
# --- Akhir definisi fungsi ---


def create_rule_template(data):
    """Membuat satu blok JSON, debug filter kedua."""

    # Fungsi helper kecil untuk mengambil nilai atau string kosong
    def get_str_or_empty(key):
        value = data.get(key)
        if value is None: return ""
        # Cek jika value adalah tipe numerik 0, kembalikan 0 (biarkan JSON handle)
        if isinstance(value, (int, float)) and value == 0:
             pass # Jangan ubah jadi string ""
        elif not value: # Jika Falsy lain (selain 0), seperti string kosong atau boolean False
             return ""
        # Jika bukan None atau Falsy (selain 0), konversi ke string (handle unicode Py2)
        try: unicode; return unicode(value)
        except NameError: return str(value)

    try:
        # --- Validasi Data Krusial ---
        plugin_id_raw = data.get('plugin_id_new')
        if plugin_id_raw is None:
             # print(f"[WARN] Baris dilewati: 'plugin_id_new' kosong (Title: {data.get('Title', 'N/A')})") # Kurangi verbosity
             return None
        plugin_id = int(plugin_id_raw)

        title = get_str_or_empty('Title')
        if not title:
            # print("[WARN] Baris dilewati: 'Title' kosong.") # Kurangi verbosity
            return None

        category = data.get('CATEGORY')
        if category is None:
             # print(f"[WARN] Baris dilewati: Kolom wajib 'CATEGORY' kosong (Title: {title})") # Kurangi verbosity
             return None
        # Tambahkan validasi lain jika perlu

    except (ValueError, TypeError):
        # print(f"[WARN] Baris dilewati: 'plugin_id_new' ('{plugin_id_raw}') bukan angka (Title: {data.get('Title', 'N/A')})") # Kurangi verbosity
        return None
    except KeyError as e:
        print(f"[ERROR] Kolom {e} tidak ada? Dilewati.")
        return None

    # --- Logika Filters dengan DEBUG ---
    filters_list = []
    # Proses filter pertama
    filter1_field = get_str_or_empty('filters_field')
    filter1_value = get_str_or_empty('filter_value')
    # Hanya tambahkan jika KEDUA field dan value ADA
    if filter1_field and filter1_value:
        filters_list.append(OrderedDict([
            ("field", filter1_field),
            ("value", filter1_value),
            ("op", "term") # Asumsi tetap term
        ]))

    # Proses filter kedua (jika ada kolomnya di Excel)
    filter2_field = get_str_or_empty('filters_field_2') # Baca kolom filter 2
    filter2_value = get_str_or_empty('filter_value_2') # Baca kolom filter 2

    # --- PRINT DEBUG DI SINI ---
    print(f"    [DEBUG] Filter 2 - Field: '{filter2_field}' (Type: {type(filter2_field)}), Value: '{filter2_value}' (Type: {type(filter2_value)})")
    # --- AKHIR PRINT DEBUG ---

    # Hanya tambahkan jika KEDUA field dan value ADA
    if filter2_field and filter2_value:
        print("      [DEBUG] Menambahkan Filter 2!") # Tambahkan ini juga
        filters_list.append(OrderedDict([
            ("field", filter2_field),
            ("value", filter2_value),
            ("op", "term") # Asumsi tetap term
        ]))
    else:
        # Cetak hanya jika salah satu ADA tapi yang lain TIDAK, atau keduanya kosong
        if filter2_field or filter2_value:
             print("      [DEBUG] Kondisi Filter 2 TIDAK terpenuhi (salah satu kosong).")
        # else: # Keduanya kosong, tidak perlu print apa2
        #    print("      [DEBUG] Kolom Filter 2 kosong.")


    # --- Konstruksi Template (tetap sama) ---
    template = OrderedDict([
        ("creator", OrderedDict([
            ("device_name", get_str_or_empty('device_name')),
            ("index_pattern", get_str_or_empty('index_pattern')),
            ("field_name", get_str_or_empty('field_name')),
            ("size", 100),
            ("module_slug", get_str_or_empty('module_slug')),
            ("submodule_slug", get_str_or_empty('submodule_slug')),
            ("filters", filters_list) # Gunakan list yang sudah dibuat
        ])),
        ("plugin", OrderedDict([("plugin_id_new", plugin_id)])),
        ("directive", OrderedDict([
            ("HEADER", title), ("CATEGORY", get_str_or_empty('CATEGORY')), ("KINGDOM", get_str_or_empty('KINGDOM')),
            ("generate_directive", "y"), ("template_id", get_str_or_empty('template_id')), ("DISABLED", "y")
        ])),
        ("mappings", OrderedDict([
            ("sensor_mode", get_str_or_empty('sensor_mode')), ("sensor_value", get_str_or_empty('sensor_value')),
            ("product_mode", get_str_or_empty('product_mode')), ("product_value", get_str_or_empty('product_value')),
            ("src_ips_mode", "f"), ("src_ips_value", "src_ips"), ("dst_ips_mode", "f"), ("dst_ips_value", "dst_ips"),
            ("src_port_mode", "f"), ("src_port_value", "src_port"), ("dst_port_mode", "f"), ("dst_port_value", "dst_port"),
            ("custom_label1", get_str_or_empty('custom_label1')), ("custom_data1", get_str_or_empty('custom_data1')),
            ("custom_label2", get_str_or_empty('custom_label2')), ("custom_data2", get_str_or_empty('custom_data2')),
            ("custom_label3", get_str_or_empty('custom_label3')), ("custom_data3", get_str_or_empty('custom_data3')),
            ("timestamp_field", get_str_or_empty('timestamp_field') or "timestamp")
        ])),
        ("distribution", OrderedDict([
            ("distribute_70", "n"), ("distribute_json", "n"), ("distribute_directive", "n"), ("restart", "n")
        ]))
    ])
    return title, template

# --- Main Script ---
excel_file_path = 'data.xlsx' # Nama file input Anda
json_file_path = 'plugin_presets.json' # Nama file output JSON
final_json = OrderedDict() # Gunakan OrderedDict untuk urutan preset

print(f"Membaca data dari {excel_file_path}...")
workbook = None # Inisialisasi workbook
headers = []
data_rows = []

try:
    # --- Coba baca sebagai Excel ---
    try:
        # Muat workbook dan sheet aktif
        # data_only=True membaca nilai sel, bukan formula
        # read_only=True bisa mempercepat pembacaan file besar
        workbook = openpyxl.load_workbook(excel_file_path, data_only=True, read_only=True)
        sheet = workbook.active
        print("[INFO] Membaca sebagai file Excel (.xlsx)")
        # Baca headers (baris pertama) dan bersihkan (hapus spasi ekstra)
        headers = [str(cell.value).strip() for cell in sheet[1] if cell.value is not None]
        # Dapatkan iterator baris data
        data_rows_iter = sheet.iter_rows(min_row=2, values_only=True)
        # Konversi iterator ke list agar bisa digunakan ulang jika perlu
        data_rows = list(data_rows_iter)
        # Penting: Tutup workbook jika read_only=True
        if workbook is not None:
             try: workbook.close()
             except Exception: pass # Abaikan error saat close jika read_only

    # --- Fallback ke CSV jika gagal sebagai Excel ---
    except Exception as ex_err: # Tangkap error jika BUKAN file Excel valid
        print(f"[WARN] Gagal membuka sebagai Excel: {ex_err}. Mencoba membaca sebagai CSV...")
        import csv
        headers = [] # Reset headers
        data_rows = [] # Reset data_rows
        try:
            # Gunakan 'utf-8-sig' untuk handle BOM jika ada
            with io.open(excel_file_path, 'r', encoding='utf-8-sig', newline='') as csvfile:
                # Coba deteksi delimiter secara otomatis
                try:
                    # Baca sampel yang lebih besar untuk deteksi yang lebih baik
                    sample = csvfile.read(1024 * 20) # Tingkatkan ukuran sampel
                    dialect = csv.Sniffer().sniff(sample)
                    csvfile.seek(0) # Kembali ke awal file
                    print(f"[INFO] Delimiter CSV terdeteksi: '{dialect.delimiter}'")
                except csv.Error:
                    dialect = None # Gagal deteksi, gunakan koma default
                    csvfile.seek(0)
                    print("[WARN] Gagal deteksi delimiter CSV, menggunakan ',' default.")

                # Buat reader dengan atau tanpa dialect
                reader = csv.reader(csvfile, dialect=dialect) if dialect else csv.reader(csvfile)

                # Baca header, pastikan tidak kosong
                try:
                    first_row = next(reader)
                    headers = [h.strip() for h in first_row if h is not None] # Handle None dan strip
                except StopIteration:
                    print("Error: File CSV kosong.")
                    sys.exit(1)

                # Baca baris data
                for row in reader:
                    # Hanya tambahkan baris jika tidak kosong (punya setidaknya satu nilai non-kosong)
                    if any(field.strip() for field in row if field is not None):
                        data_rows.append(row)

            if headers: print("[INFO] Berhasil membaca sebagai file CSV.")
            else: raise ValueError("Header CSV tidak terbaca.") # Picu error jika header masih kosong

        except StopIteration: # Tangkap jika file CSV hanya berisi header
            print("Error: File CSV hanya berisi header atau kosong.")
            sys.exit(1)
        except Exception as csv_err:
            print(f"Error: Gagal membaca file '{excel_file_path}' sebagai Excel maupun CSV.")
            print(f"-> Detail: {csv_err}")
            sys.exit(1)

    # --- Validasi Header ---
    if not headers:
        print("Error: Header kosong atau tidak terbaca setelah mencoba Excel dan CSV.")
        sys.exit(1)

    print(f"Header ditemukan: {headers}")
    # Verifikasi header filter kedua (opsional tapi baik)
    if 'filters_field_2' not in headers or 'filter_value_2' not in headers:
         print("[INFO] Kolom 'filters_field_2' / 'filter_value_2' tidak ada di header. Filter kedua akan diabaikan.")

    # --- Proses Baris Data ---
    print("Memulai pemrosesan baris data...")
    processed_count = 0
    skipped_count = 0
    # Iterasi melalui baris data yang sudah dibaca (dari Excel atau CSV)
    for row_index, row in enumerate(data_rows, start=2): # Start index from 2 (Excel row number)
        # Penanganan baris kosong yang mungkin lolos
        is_empty_row = all(v is None or (isinstance(v, str) and not v.strip()) for v in row)
        if is_empty_row:
             continue # Lewati baris kosong

        # Buat dictionary (pastikan jumlah value cocok header)
        row_values = row[:len(headers)] # Ambil nilai sesuai jumlah header
        # Pad dengan None jika baris lebih pendek dari header
        row_values += (None,) * (len(headers) - len(row_values))
        row_data_dict = dict(zip(headers, row_values))
        current_title = row_data_dict.get('Title', f'Baris {row_index}') # Ambil title untuk logging

        # Panggil template creator dengan penanganan error per baris
        try:
            result = create_rule_template(row_data_dict)
            if result:
                title, rule_data = result
                if title in final_json:
                    print(f"[WARN] Duplikasi Title '{title}' pada baris {row_index}. Timpa.")
                final_json[title] = rule_data
                print(f"  -> Baris {row_index}: OK ('{title}')")
                processed_count += 1
            else:
                skipped_count += 1 # Pesan skip sudah dicetak di dalam create_rule_template
        except Exception as process_err:
             print(f"[ERROR] Error tak terduga saat memproses baris {row_index} (Title: {current_title}): {process_err}")
             skipped_count += 1
             import traceback; traceback.print_exc() # Cetak traceback untuk debug

    # --- Simpan Hasil ---
    if not final_json:
        print("\nTidak ada data yang berhasil diproses. File JSON tidak dibuat.")
    else:
        print(f"\nTotal diproses: {processed_count}, Dilewati: {skipped_count}")
        print(f"Menyimpan hasil ke {json_file_path}...")
        try:
            # Gunakan save_json_utf8 yang sudah ditambahkan
            save_json_utf8(json_file_path, final_json)
            print(f"Selesai! 🔥 File JSON dibuat di: {json_file_path}")
        except Exception as write_err:
             print(f"\nError saat menyimpan file JSON: {write_err}")

# --- Penanganan Error Global ---
except FileNotFoundError:
    print(f"Error: File '{excel_file_path}' tidak ditemukan.")
    print("Pastikan file Excel/CSV ada di direktori yang sama dengan skrip ini.")
except ImportError as imp_err:
    # Cek library mana yang error
    if 'openpyxl' in str(imp_err):
         print("Error: Library 'openpyxl' belum ter-install. Install: pip install openpyxl")
    else:
         print(f"Error import library yang diperlukan: {imp_err}")
except Exception as e:
    print(f"\nTerjadi error tidak terduga di luar loop pemrosesan:")
    print(f"-> {e}")
    import traceback
    traceback.print_exc()