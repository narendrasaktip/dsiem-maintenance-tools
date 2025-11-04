#!/bin/bash

# --- Konfigurasi Awal ---
NAMESPACE="default" # GANTI INI jika pod-nya tidak ada di namespace 'default'
POD_TARGET_DIR="/var/ossec/etc/lists/defenxor/"
RESTART_SCRIPT_PATH="/var/ossec/bin/wazuh-daemon-control.sh"
LOCAL_RESTART_SCRIPT="wazuh-daemon-control.sh"
RESTART_CMD="bash ${RESTART_SCRIPT_PATH} restart wazuh-analysisd"
SEARCH_ANCHOR_FILE="administrative-ports" # File yg dicari
CONFIG_MAP_NAME="update-config-map.sh" # Nama script yg dicari

# --- Variabel Global (Akan diisi oleh user) ---
LOCAL_LISTS_DIR=""
CONFIG_MAP_SCRIPT_PATH=""
POD_NAME=""
FILES_TO_SYNC=()

# --- Fungsi UI Helper (Sama seperti v10) ---
print_header() {
    local text="$1"
    local width=70
    echo -e "\n\033[1;36m╔$(printf '═%.0s' $(seq 1 $((width-2))))╗\033[0m"
    printf "\033[1;36m║\033[0m\033[1;97m%-$((width-2))s\033[0m\033[1;36m║\033[0m\n" " $text"
    echo -e "\033[1;36m╚$(printf '═%.0s' $(seq 1 $((width-2))))╝\033[0m"
}
print_line() {
    echo -e "\033[2;37m$(printf '─%.0s' $(seq 1 70))\033[0m"
}
print_success() {
    echo -e "\033[1;32m✓\033[0m $1"
}
print_error() {
    echo -e "\033[1;31m✗\033[0m $1"
}
print_warning() {
    echo -e "\033[1;33m⚠\033[0m $1"
}
print_info() {
    echo -e "\033[1;34mℹ\033[0m $1"
}
# --- Akhir Fungsi UI Helper ---


# --- Fungsi Baru: Menandai file untuk sinkronisasi ---
add_to_sync_list() {
    local filename=$1
    local found=0
    for item in "${FILES_TO_SYNC[@]}"; do
        if [[ "$item" == "$filename" ]]; then
            found=1
            break
        fi
    done
    if [[ $found -eq 0 ]]; then
        FILES_TO_SYNC+=("$filename")
        print_info "File '$filename' ditandai untuk sinkronisasi."
    fi
}

# --- Fungsi DIPERBARUI: Step 1 & 3 - PENCARIAN OTOMATIS GANDA ---
ask_for_local_paths() {
    print_header "Step 1 & 3: Konfigurasi Path Lokal"
    
    # 1. Minta path ke direktori list LOKAL (PENCARIAN OTOMATIS)
    print_info "Mencari direktori list lokal (mencari '$SEARCH_ANCHOR_FILE')..."
    
    SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
    SEARCH_PATHS=("$PWD" "$SCRIPT_DIR")
    
    if [[ "$HOME" != "$PWD" && "$HOME" != "$SCRIPT_DIR" ]]; then
        SEARCH_PATHS+=("$HOME")
    fi
    
    local found_paths=()
    for path in "${SEARCH_PATHS[@]}"; do
        if [ -d "$path" ]; then
            print_info "  → Mencari di $path (max 10 level)..."
            
            temp_file=$(mktemp)
            if [ -z "$temp_file" ]; then
                print_error "Gagal membuat file sementara."
                exit 1
            fi
            
            find "$path" -maxdepth 10 -type f -name "$SEARCH_ANCHOR_FILE" 2>/dev/null > "$temp_file"
            
            while IFS= read -r line; do
                if [ -n "$line" ]; then
                    found_paths+=("$(dirname "$line")")
                fi
            done < "$temp_file"
            
            rm "$temp_file"
        fi
    done

    # Dapatkan path unik
    local unique_paths=()
    if [ ${#found_paths[@]} -gt 0 ]; then
        temp_file2=$(mktemp)
        if [ -z "$temp_file2" ]; then
            print_error "Gagal membuat file sementara (2)."
            exit 1
        fi
        
        printf "%s\n" "${found_paths[@]}" | sort -u > "$temp_file2"
        
        while IFS= read -r line; do
            if [ -n "$line" ]; then
                unique_paths+=("$line")
            fi
        done < "$temp_file2"
        rm "$temp_file2"
    fi

    # Tampilkan menu pilihan berdasarkan hasil pencarian
    if [ ${#unique_paths[@]} -eq 0 ]; then
        print_warning "Direktori tidak ditemukan otomatis."
        while true; do
            printf "%b" "\033[1;33m→\033[0m Masukkan path direktori list LOKAL: "
            read -e LOCAL_LISTS_DIR
            LOCAL_LISTS_DIR=${LOCAL_LISTS_DIR%/}
            if [ -d "$LOCAL_LISTS_DIR" ]; then
                print_success "Direktori ditemukan: $LOCAL_LISTS_DIR"
                break
            else
                print_error "Direktori tidak ditemukan. Coba lagi."
            fi
        done
    elif [ ${#unique_paths[@]} -eq 1 ]; then
        LOCAL_LISTS_DIR=${unique_paths[0]}
        print_success "Ditemukan 1 lokasi: $LOCAL_LISTS_DIR"
    else
        print_success "Ditemukan ${#unique_paths[@]} lokasi!"
        echo ""
        local i=1
        for path in "${unique_paths[@]}"; do
            printf "  \033[1;33m%2d)\033[0m %s\n" "$i" "$path"
            i=$((i+1))
        done
        printf "  \033[1;33m%2d)\033[0m Masukkan path manual\n" "$i"
        echo ""
        
        while true; do
            printf "%b" "\033[1;36m→\033[0m Pilih path direktori: "
            read choice
            
            if [[ "$choice" -ge 1 && "$choice" -lt "$i" ]]; then
                LOCAL_LISTS_DIR=${unique_paths[$((choice-1))]}
                print_success "Menggunakan path: $LOCAL_LISTS_DIR"
                break
            # --- PERBAIKAN: Hapus '{' dan ganti dengan 'then' ---
            elif [ "$choice" -eq "$i" ]; then
                printf "%b" "\033[1;33m→\033[0m Masukkan path direktori list LOKAL: "
                read -e LOCAL_LISTS_DIR
                LOCAL_LISTS_DIR=${LOCAL_LISTS_DIR%/}
                if [ -d "$LOCAL_LISTS_DIR" ]; then
                    print_success "Direktori ditemukan: $LOCAL_LISTS_DIR"
                    break
                else
                    print_error "Direktori tidak ditemukan. Coba lagi."
                fi
            # --- PERBAIKAN: Hapus '}' ---
            else
                print_error "Pilihan tidak valid."
            fi
        done
    fi
    
    echo ""
    # Otomatis mencari config map script
    print_info "Mencari '$CONFIG_MAP_NAME' berdasarkan path list..."
    
    PROPOSED_BASE_DIR=$(dirname "$LOCAL_LISTS_DIR")
    PROPOSED_BASE_DIR=$(dirname "$PROPOSED_BASE_DIR")
    PROPOSED_BASE_DIR=$(dirname "$PROPOSED_BASE_DIR")
    PROPOSED_SCRIPT_PATH="$PROPOSED_BASE_DIR/$CONFIG_MAP_NAME"
    
    print_info "  → Mengecek di: $PROPOSED_SCRIPT_PATH"
    
    if [ -f "$PROPOSED_SCRIPT_PATH" ]; then
        print_success "Script ditemukan otomatis!"
        CONFIG_MAP_SCRIPT_PATH="$PROPOSED_SCRIPT_PATH"
    else
        print_warning "Script tidak ditemukan otomatis."
        while true; do
            printf "%b" "\033[1;33m→\033[0m Masukkan path ke '$CONFIG_MAP_NAME': "
            read -e CONFIG_MAP_SCRIPT_PATH
            
            if [ -f "$CONFIG_MAP_SCRIPT_PATH" ]; then
                print_success "Script ditemukan: $CONFIG_MAP_SCRIPT_PATH"
                break
            else
                print_error "Script tidak ditemukan. Coba lagi."
            fi
        done
    fi
    print_line
}

# --- Fungsi Baru: Step 5 - Mencari Pod Wazuh ---
find_wazuh_pod() {
    print_header "Step 5: Penemuan Pod Wazuh"
    local label_selectors=("app.kubernetes.io/component=manager" "app=wazuh-manager" "app=wazuh" "component=wazuh-manager")
    
    for label in "${label_selectors[@]}"; do
        print_info "Mencari pod dengan label: \033[1m$label\033[0m"
        POD_NAME=$(kubectl get pods -n $NAMESPACE -l $label --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
        if [ -n "$POD_NAME" ]; then
            print_success "Pod Manager ditemukan: \033[1;32m$POD_NAME\033[0m"
            print_line
            return 0
        fi
        print_warning "Label '$label' tidak menemukan pod aktif."
    done

    print_error "Gagal menemukan pod manager secara otomatis."
    echo ""
    while true; do
        printf "%b" "\033[1;33m→\033[0m Masukkan NAMA AWALAN pod (cth: wazuh-manager-): "
        read POD_PREFIX
        if [ -z "$POD_PREFIX" ]; then
            print_error "Nama awalan tidak boleh kosong."
            continue
        fi
        
        POD_NAME=$(kubectl get pods -n $NAMESPACE --field-selector=status.phase=Running -o name | grep "pod/$POD_PREFIX" | head -n 1 | cut -d'/' -f2)
        
        if [ -n "$POD_NAME" ]; then
            print_success "Pod ditemukan: \033[1;32m$POD_NAME\033[0m"
            print_line
            return 0
        else
            print_error "Tidak ada pod aktif dengan awalan '$POD_PREFIX'. Coba lagi."
        fi
    done
}

# --- Fungsi DIPERBARUI: Step 4 - Menjalankan Script ConfigMap ---
run_configmap_script() {
    print_header "Step 3: Menjalankan Update Config Map"
    
    local SCRIPT_DIR
    SCRIPT_DIR=$(dirname "$CONFIG_MAP_SCRIPT_PATH")
    local SCRIPT_NAME
    SCRIPT_NAME=$(basename "$CONFIG_MAP_SCRIPT_PATH")

    print_info "Berpindah ke direktori: $SCRIPT_DIR"
    print_info "Menjalankan: bash $SCRIPT_NAME"
    
    if (cd "$SCRIPT_DIR" && bash "$SCRIPT_NAME"); then
        print_success "Script '$CONFIG_MAP_NAME' berhasil dijalankan."
    else
        print_error "Script '$CONFIG_MAP_NAME' gagal."
        printf "%b" "\033[1;31m→\033[0m Script gagal. Tetap lanjutkan copy ke pod? (y/n): "
        read confirm_continue
        if [[ "$confirm_continue" != "y" && "$confirm_continue" != "Y" ]]; then
            print_error "Aksi dibatalkan oleh user."
            exit 1
        fi
        print_warning "Melanjutkan proses copy..."
    fi
    print_line
}

# --- Fungsi Baru: Step 5 - Menyalin File ke Pod ---
copy_files_to_pod() {
    print_header "Step 4: Sinkronisasi File ke Pod"
    
    if [ ${#FILES_TO_SYNC[@]} -eq 0 ]; then
        print_warning "Tidak ada file yang diubah. Tidak ada yang disinkronisasi."
        print_line
        return 0
    fi

    print_info "Menyalin \033[1m${#FILES_TO_SYNC[@]}\033[0m file yang telah diubah ke pod \033[1m$POD_NAME\033[0m..."
    echo ""
    
    local all_success=1
    for filename in "${FILES_TO_SYNC[@]}"; do
        local_file_path="$LOCAL_LISTS_DIR/$filename"
        pod_target_path="$POD_NAME:$POD_TARGET_DIR/$filename"
        
        if [ ! -f "$local_file_path" ]; then
            print_error "File lokal $filename tidak ditemukan. Dilewati."
            all_success=0
            continue
        fi

        print_info "Menyalin $filename..."
        if kubectl cp "$local_file_path" "$pod_target_path" -n $NAMESPACE; then
            print_success "  ✓ Sukses: $filename"
        else
            print_error "  ✗ Gagal: $filename"
            all_success=0
        fi
    done
    
    echo ""
    if [ $all_success -eq 1 ]; then
        print_success "Semua file berhasil disalin."
    else
        print_error "Beberapa file gagal disalin."
    fi
    print_line
}

# --- Fungsi Modifikasi: Step 2 - Editor File LOKAL ---
run_local_editor() {
    print_info "Mencari file di direktori lokal: $LOCAL_LISTS_DIR"
    
    FILE_LIST=$(find "$LOCAL_LISTS_DIR" -maxdepth 1 -type f ! -name "*.cdb" -printf "%f\n" | sort)
    if [ -z "$FILE_LIST" ]; then
        print_error "Tidak menemukan file list (non-.cdb) di $LOCAL_LISTS_DIR"
        exit 1
    fi
    
    temp_file3=$(mktemp)
    if [ -z "$temp_file3" ]; then
        print_error "Gagal membuat file sementara (3)."
        exit 1
    fi
    echo "$FILE_LIST" > "$temp_file3"
    
    local options=()
    while IFS= read -r line; do
        if [ -n "$line" ]; then
            options+=("$line")
        fi
    done < "$temp_file3"
    rm "$temp_file3"

    print_success "Ditemukan ${#options[@]} file."
    printf "%b" "\n\033[1;32m→\033[0m Tekan ENTER untuk masuk ke menu editor..."
    read

    while true; do
        clear
        print_header "Step 2: Editor File LOKAL"
        echo ""
        printf "  \033[2;37mEditing directory: \033[1;36m${LOCAL_LISTS_DIR}\033[0m\n"
        echo "" ; print_line ; echo ""

        i=1
        for file in "${options[@]}"; do
            printf "  \033[1;33m%2d)\033[0m %s\n" "$i" "$file"
            i=$((i+1))
        done
        
        echo "" ; print_line
        printf "  \033[1;31m99)\033[0m \033[1mSelesai Mengedit (Lanjut ke Step 3)\033[0m\n"
        echo "" ; print_line ; echo ""
        
        printf "%b" "\033[1;36m→\033[0m Pilih file (ketik angka): "
        read choice
        clear

        case $choice in
            99)
                print_success "Selesai mengedit file lokal."
                break
                ;;
            *)
                if [[ "$choice" -ge 1 && "$choice" -le "${#options[@]}" ]]; then
                    FILENAME=${options[$((choice-1))]}
                    FULL_PATH="$LOCAL_LISTS_DIR/$FILENAME"
                    
                    while true; do
                        clear
                        print_header "ACTION MENU (LOKAL): $FILENAME"
                        echo -e "\n  \033[1;32m1)\033[0m Tambah Data (Append)"
                        echo -e "  \033[1;31m2)\033[0m Hapus Data (Delete)"
                        echo -e "  \033[1;37m3)\033[0m Kembali ke Menu Utama\n"
                        print_line ; echo ""
                        printf "%b" "\033[1;36m→\033[0m Pilih Aksi: "
                        read action_choice
                        clear

                        case $action_choice in
                            1) # --- MODE TAMBAH (LOKAL) ---
                                print_header "APPEND MODE (LOKAL): $FILENAME"
                                echo ""
                                print_info "Membaca file lokal: $FULL_PATH"
                                CURRENT_CONTENT=$(cat "$FULL_PATH" 2>/dev/null || true)
                                
                                echo ""
                                echo -e "\033[1;36m┌─ Current Content (Last 10 Lines) $(printf '─%.0s' $(seq 1 28))┐\033[0m"
                                if [ -z "$CURRENT_CONTENT" ]; then
                                    echo -e "\033[1;36m│\033[0m \033[2;37m[ File kosong ]\033[0m"
                                else
                                    echo "$CURRENT_CONTENT" | tail -n 10 | while IFS= read -r line; do
                                        printf "\033[1;36m│\033[0m %s\n" "$line"
                                    done
                                fi
                                echo -e "\033[1;36m└$(printf '─%.0s' $(seq 1 67))┘\033[0m" ; echo ""

                                # Tampilkan Contoh/Hint
                                echo -e "\033[1;35m┌─ Format Examples $(printf '─%.0s' $(seq 1 50))┐\033[0m"
                                case "$FILENAME" in
                                    "administrative-ports"|"common-ports")
                                        echo -e "\033[1;35m│\033[0m  \033[2m→\033[0m 22"; echo -e "\033[1;35m│\033[0m  \033[2m→\033[0m 3389" ;;
                                    "domain-controller-hostnames")
                                        echo -e "\033[1;35m│\033[0m  \033[2m→\033[0m DC-01"; echo -e "\033[1;35m│\033[0m  \033[2m→\033[0m server-utama.local" ;;
                                    "domain-controller-ips"|"pam-ips")
                                        echo -e "\033[1;35m│\033[0m  \033[2m→\033[0m 192.168.1.10"; echo -e "\033[1;35m│\033[0m  \033[2m→\033[0m 10.0.0.5" ;;
                                    "high-privilege-users")
                                        echo -e "\033[1;35m│\033[0m  \033[2m→\033[0m administrator"; echo -e "\033[1;35m│\033[0m  \033[2m→\033[0m root" ;;
                                    "malicious-domains")
                                        echo -e "\033[1;35m│\033[0m  \033[2m→\033[0m domain-jahat.com"; echo -e "\033[1;35m│\033[0m  \033[2m→\033[0m 123.45.67.89" ;;
                                    "malicious-hashes")
                                        echo -e "\033[1;35m│\033[0m  \033[2m→\033[0m e1b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                                        echo -e "\033[1;35m│\033[0m  \033[2m→\033[0m 5d41402abc4b2a76b9719d911017c592" ;;
                                    "bash_profile")
                                        echo -e "\033[1;35m│\033[0m  \033[2m→\033[0m export CUSTOM_VAR=\"my_value\""; echo -e "\033[1;35m│\033[0m  \033[2;33m⚠ Shell profile, bukan list biasa\033[0m" ;;
                                    *)
                                        echo -e "\033[1;35m│\033[0m  \033[2;37mMasukkan data sebagai teks biasa\033[0m" ;;
                                esac
                                echo -e "\033[1;35m└$(printf '─%.0s' $(seq 1 67))┘\033[0m"
                                echo ""
                                
                                echo -e "\033[1;32m┌─ Input Data (Append) $(printf '─%.0s' $(seq 1 48))┐\033[0m"
                                echo -e "\033[1;32m│\033[0m  Masukkan data baru (baris per baris) untuk DITAMBAHKAN"
                                echo -e "\033[1;32m│\033[0m  Setiap baris akan diakhiri dengan ':'"
                                echo -e "\033[1;32m│\033[0m  Tekan ENTER di baris kosong untuk selesai"
                                echo -e "\033[1;32m└$(printf '─%.0s' $(seq 1 67))┘\033[0m" ; echo ""
                                
                                INPUT_DATA=""
                                LINE_COUNT=0
                                while true; do
                                    printf "%b" "\033[1;32m▸\033[0m "
                                    read LINE
                                    if [ -z "$LINE" ]; then break; else
                                        INPUT_DATA+="${LINE}:"$'\n'
                                        LINE_COUNT=$((LINE_COUNT+1))
                                    fi
                                done

                                if [ -z "$INPUT_DATA" ]; then
                                    print_warning "Tidak ada data baru. File tidak diubah"
                                else
                                    echo ""
                                    print_info "Menambahkan $LINE_COUNT baris ke file LOKAL..."
                                    if printf "%s" "$INPUT_DATA" >> "$FULL_PATH"; then
                                        print_success "Berhasil menambahkan data ke $FILENAME (Lokal)"
                                        add_to_sync_list "$FILENAME"
                                    else
                                        print_error "Gagal menambahkan data ke $FILENAME (Lokal)"
                                    fi
                                fi
                                echo "" ; print_line
                                printf "%b" "\n\033[1;32m→\033[0m Tekan ENTER untuk kembali..."
                                read
                                break
                                ;;
                            
                            2) # --- MODE HAPUS (LOKAL) ---
                                print_header "DELETE MODE (LOKAL): $FILENAME"
                                NUMBERED_CONTENT=$(cat "$FULL_PATH" 2>/dev/null | cat -n)
                                
                                if [ -z "$NUMBERED_CONTENT" ]; then
                                    print_warning "File kosong, tidak ada yang bisa dihapus."
                                    echo "" ; print_line
                                    printf "%b" "\n\033[1;33m→\033[0m Tekan ENTER untuk kembali..."
                                    read
                                    continue
                                fi
                                
                                echo ""
                                echo -e "\033[1;36m┌─ Current Content (with line numbers) $(printf '─%.0s' $(seq 1 29))┐\033[0m"
                                echo "$NUMBERED_CONTENT" | while IFS= read -r line; do
                                    printf "\033[1;36m│\033[0m %s\n" "$line"
                                done
                                echo -e "\033[1;36m└$(printf '─%.0s' $(seq 1 67))┘\033[0m" ; echo ""

                                printf "%b" "\033[1;31m→\033[0m Masukkan NOMOR baris yg dihapus (cth: 2 5 8): "
                                read lines_input
                                lines_to_delete=($lines_input) # Word splitting

                                if [ ${#lines_to_delete[@]} -eq 0 ]; then
                                    print_warning "Tidak ada nomor dipilih. Aksi dibatalkan."
                                    echo "" ; print_line
                                    printf "%b" "\n\033[1;33m→\033[0m Tekan ENTER untuk kembali..."
                                    read
                                    continue
                                fi

                                local pattern
                                for num in "${lines_to_delete[@]}"; do
                                    pattern+="|^\\s*$num\\s"
                                done
                                pattern=${pattern:1}
                                
                                NEW_CONTENT=$(echo "$NUMBERED_CONTENT" | grep -vE "$pattern" | cut -f2-)
                                
                                clear
                                print_header "KONFIRMASI PENGHAPUSAN (LOKAL): $FILENAME"
                                echo "" ; print_warning "Aksi ini akan MENIMPA (OVERWRITE) file LOKAL!"
                                print_info "Konten LOKAL BARU akan menjadi:"
                                echo -e "\033[1;36m┌─ New Content Preview $(printf '─%.0s' $(seq 1 44))┐\033[0m"
                                if [ -z "$NEW_CONTENT" ]; then
                                    echo -e "\033[1;36m│\033[0m \033[2;37m[ File akan menjadi kosong ]\033[0m"
                                else
                                    echo "$NEW_CONTENT" | while IFS= read -r line; do
                                        printf "\033[1;36m│\033[0m %s\n" "$line"
                                    done
                                fi
                                echo -e "\033[1;36m└$(printf '─%.0s' $(seq 1 67))┘\033[0m" ; echo "" ; print_line ; echo ""
                                
                                printf "%b" "\033[1;31m→\033[0m Lanjutkan menyimpan ke file LOKAL? (y/n): "
                                read confirm_delete
                                
                                if [[ "$confirm_delete" == "y" || "$confirm_delete" == "Y" ]]; then
                                    echo ""
                                    print_info "Menyimpan ke file LOKAL... (OVERWRITE)"
                                    if printf "%s" "$NEW_CONTENT" > "$FULL_PATH"; then
                                        print_success "File lokal berhasil diupdate."
                                        add_to_sync_list "$FILENAME"
                                    else
                                        print_error "Gagal menyimpan file lokal."
                                    fi
                                else
                                    echo ""
                                    print_warning "Penghapusan dibatalkan."
                                fi

                                echo "" ; print_line
                                printf "%b" "\n\033[1;32m→\033[0m Tekan ENTER untuk kembali..."
                                read
                                break
                                ;;

                            3) # --- KEMBALI KE MENU FILE ---
                                break
                                ;;
                            *)
                                print_error "Pilihan tidak valid."
                                printf "%b" "\n\033[1;33m→\033[0m Tekan ENTER untuk coba lagi..."
                                read
                                ;;
                        esac
                    done
                else
                    print_error "Pilihan tidak valid"
                    printf "%b" "\n\033[1;33m→\033[0m Tekan ENTER untuk coba lagi..."
                    read
                fi
                ;;
        esac
    done
}

# --- Fungsi Identik: Step 6 - Cek Script Restart ---
check_and_deploy_restart_script() {
    print_header "Step 6: Validasi Script Restart di Pod"
    print_info "Mengecek ketersediaan script di pod \033[1m$POD_NAME\033[0m..."
    
    kubectl exec $POD_NAME --namespace=${NAMESPACE} -- test -f ${RESTART_SCRIPT_PATH}
    if [ $? -eq 0 ]; then
        print_success "Script restart sudah tersedia di pod."
    else
        print_warning "Script restart tidak ditemukan di pod."
        print_info "Mencari file lokal '${LOCAL_RESTART_SCRIPT}'..."
        
        if [ -f "${LOCAL_RESTART_SCRIPT}" ]; then
            print_success "File lokal ditemukan. Meng-upload..."
            kubectl cp "${LOCAL_RESTART_SCRIPT}" "${POD_NAME}:${RESTART_SCRIPT_PATH}" --namespace=${NAMESPACE}
            if [ $? -ne 0 ]; then
                print_error "FATAL: Gagal meng-upload script ke pod." ; exit 1 ; fi
            
            print_info "Memberikan izin eksekusi..."
            kubectl exec ${POD_NAME} --namespace=${NAMESPACE} -- chmod +x ${RESTART_SCRIPT_PATH}
            if [ $? -ne 0 ]; then
                print_error "FATAL: Gagal chmod +x script di pod." ; exit 1 ; fi
            
            print_success "Script restart berhasil di-deploy."
        else
            print_error "FATAL: Script restart lokal '${LOCAL_RESTART_SCRIPT}' tidak ditemukan."
            exit 1
        fi
    fi
    print_line
}

# --- Fungsi Identik: Step 7 - Konfirmasi Restart ---
run_restart_confirmation() {
    print_header "Step 7: Konfirmasi Restart Service"
    echo ""
    printf "%b" "\033[1;33m→\033[0m Restart wazuh-analysisd sekarang? (y/n): "
    read confirm

    if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
        echo ""
        print_info "Me-restart wazuh-analysisd di pod \033[1m$POD_NAME\033[0m..."
        kubectl exec ${POD_NAME} --namespace=${NAMESPACE} -- ${RESTART_CMD}
        
        if [ $? -eq 0 ]; then
            echo "" ; print_success "Service berhasil di-restart."
        else
            echo "" ; print_error "Gagal me-restart service."
        fi
    else
        echo "" ; print_warning "Restart dibatalkan."
    fi
}

# --- ALUR UTAMA SCRIPT (MAIN) ---
clear
ask_for_local_paths
run_local_editor

if [ ${#FILES_TO_SYNC[@]} -eq 0 ]; then
    print_warning "Tidak ada file yang diubah. Script selesai."
    echo ""
    exit 0
fi

find_wazuh_pod
run_configmap_script
copy_files_to_pod
check_and_deploy_restart_script
run_restart_confirmation

echo ""
print_header "SEMUA LANGKAH SELESAI"
echo ""