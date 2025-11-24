#!/bin/bash

# --- KONFIGURASI WARNA ---
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 1. CARI LOKASI ROOT GIT
if git rev-parse --git-dir > /dev/null 2>&1; then
    GIT_ROOT=$(git rev-parse --show-toplevel)
    cd "$GIT_ROOT" || exit
else
    echo -e "${RED}[ERROR] Folder ini bukan bagian dari Git Repository.${NC}"
    exit 1
fi

# 2. DAFTAR FILE YANG DI-EXCLUDE
EXCLUDE_FILES=(
    "dsiem-event-repository/customer.json"
    "dsiem-event-repository/active_plugins.json"
    "dsiem-event-repository/last_selection.json"
)

# 3. FETCH DATA TERBARU
# Kita redirect output ke null biar ga berisik
git fetch --all > /dev/null 2>&1

LOCAL_HASH=$(git rev-parse HEAD)
REMOTE_HASH=$(git rev-parse origin/main)
# -uno: Abaikan file asing (baba.py)
PHYSICAL_STATE=$(git status --porcelain -uno)

if [ "$LOCAL_HASH" = "$REMOTE_HASH" ]; then
    if [ -z "$PHYSICAL_STATE" ]; then
        echo -e "${GREEN}[INFO] Repository sudah paling update & Bersih.${NC}"
        exit 0
    else
        echo -e "${CYAN}[INFO] Commit ID sama, tapi file Git ada yang berubah lokal.${NC}"
        echo -e "${YELLOW}Memaksa sinkronisasi ulang (Reset)...${NC}"
    fi
else
    echo -e "${CYAN}[UPDATE TERSEDIA] Server tertinggal dari GitHub.${NC}"
    echo -e "${YELLOW}=== 📋 RINCIAN PERUBAHAN ===${NC}"
    git log HEAD..origin/main --oneline --format=" - %s (%h)"
    echo -e "${YELLOW}============================${NC}"
fi

# 4. PROSES BACKUP (SILENT)
echo -e "${YELLOW}[BACKUP] Mengamankan file konfigurasi lokal...${NC}"
for file in "${EXCLUDE_FILES[@]}"; do
    if [ -f "$file" ]; then
        cp "$file" "$file.protected.bak"
    fi
done

# 5. FORCE UPDATE (HARD RESET)
echo -e "${RED}[ACTION] Menimpa SELURUH repo dengan versi GitHub...${NC}"
# Redirect output HEAD is now at... ke null biar bersih
git reset --hard origin/main > /dev/null 2>&1

if [ $? -ne 0 ]; then
    echo -e "${RED}[ERROR] Update gagal. Cek permission.${NC}"
    exit 1
fi

# 6. RESTORE CONFIG (SILENT)
echo -e "${GREEN}[RESTORE] Mengembalikan konfigurasi lokal...${NC}"
for file in "${EXCLUDE_FILES[@]}"; do
    if [ -f "$file.protected.bak" ]; then
        mv "$file.protected.bak" "$file"
        git update-index --skip-worktree "$file" 2>/dev/null
    fi
done

# 7. Update Permission (SILENT)
# Redirect error ke null
find . -name "*.py" -exec chmod +x {} \; 2>/dev/null
find . -name "*.sh" -exec chmod +x {} \; 2>/dev/null

echo -e "${GREEN}[DONE] Update Selesai.${NC}"
