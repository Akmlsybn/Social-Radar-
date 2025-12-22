import pandas as pd
import os
import io
import shutil
import json
import duckdb

# ==============================
# KONFIGURASI PATH
# ==============================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RAW_SOURCE = BASE_DIR
LAKE_BRONZE = os.path.join(BASE_DIR, 'datalake', 'bronze')
LAKE_SILVER = os.path.join(BASE_DIR, 'datalake', 'silver')
LAKE_GOLD   = os.path.join(BASE_DIR, 'datalake', 'gold')

os.makedirs(LAKE_BRONZE, exist_ok=True)
os.makedirs(LAKE_SILVER, exist_ok=True)
os.makedirs(LAKE_GOLD, exist_ok=True)

# ==============================
# HELPER
# ==============================
def clean_csv_quotes(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    cleaned = []
    for line in lines:
        s = line.strip()
        if s.startswith('"') and s.endswith('"'):
            s = s[1:-1].replace('""', '"')
        cleaned.append(s)

    return io.StringIO("\n".join(cleaned))

# ==============================
# MAIN PIPELINE
# ==============================
def run_elt():
    print("🚀 MEMULAI ELT PIPELINE")

    # -------------------------------------------------
    # 1. EXTRACT → BRONZE
    # -------------------------------------------------
    raw_files = ['hasil_survey.csv', 'social_time_rules.csv', 'lokasi_bjm.json']
    for f in raw_files:
        src = os.path.join(RAW_SOURCE, f)
        dst = os.path.join(LAKE_BRONZE, f)
        if os.path.exists(src):
            shutil.copy(src, dst)
            print(f"✅ [BRONZE] {f}")

    # -------------------------------------------------
    # 2. BRONZE → SILVER (SURVEY)
    # -------------------------------------------------
    print("⚙️ [SILVER] Survey")

    survey_path = os.path.join(LAKE_BRONZE, 'hasil_survey.csv')
    csv_io = clean_csv_quotes(survey_path)
    df_raw = pd.read_csv(csv_io)

    df_raw.columns = [c.lower().strip().replace(" ", "_") for c in df_raw.columns]

    arch_map = {
        'Religius': ('relig_fisik_cowo', 'relig_lokasi'),
        'Intellectual': ('intel_fisik_cowo', 'intel_lokasi'),
        'Creative': ('creative_fisik_cowo', 'creative_lokasi'),
        'Social': ('social_fisik_cowo', 'social_lokasi'),
        'Sporty': ('sporty_fisik_cowo', 'sporty_lokasi'),
        'Techie': ('techie_fisik_cowo', 'techie_lokasi'),
        'Active': ('active_fisik_cowo', 'active_lokasi'),
    }

    rows = []
    for arch, (fisik_col, lokasi_col) in arch_map.items():
        if fisik_col not in df_raw.columns:
            continue

        temp = df_raw[['timestamp', 'gender', fisik_col, lokasi_col]].copy()
        temp.rename(columns={
            fisik_col: 'ciri_fisik',
            lokasi_col: 'habitat'
        }, inplace=True)

        temp['archetype'] = arch
        temp.dropna(subset=['ciri_fisik'], inplace=True)
        rows.append(temp)

    df_silver = pd.concat(rows, ignore_index=True)
    df_silver.to_parquet(os.path.join(LAKE_SILVER, 'survey_data.parquet'), index=False)
    print("✅ survey_data.parquet")

    # -------------------------------------------------
    # 3. BRONZE → SILVER (RULES)
    # -------------------------------------------------
    print("⚙️ [SILVER] Rules")

    rules_path = os.path.join(LAKE_BRONZE, 'social_time_rules.csv')
    csv_rules = clean_csv_quotes(rules_path)
    df_rules = pd.read_csv(csv_rules)

    df_rules.columns = [c.lower().strip().replace(" ", "_") for c in df_rules.columns]
    df_rules.to_parquet(os.path.join(LAKE_SILVER, 'rules_data.parquet'), index=False)
    print("✅ rules_data.parquet")

    # -------------------------------------------------
    # 4. BRONZE → SILVER (LOKASI JSON – DIPERLUAS)
    # -------------------------------------------------
    print("⚙️ [SILVER] Lokasi")

    loc_path = os.path.join(LAKE_BRONZE, 'lokasi_bjm.json')
    with open(loc_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    rows = []
    for el in data.get("elements", []):
        tags = el.get("tags", {})
        name = tags.get("name")
        if not name:
            continue

        # 🔑 KUNCI: ambil kategori SELENGKAP MUNGKIN
        if tags.get("amenity"):
            kategori = tags["amenity"]
        elif tags.get("leisure"):
            kategori = tags["leisure"]
        elif tags.get("shop"):
            kategori = tags["shop"]
        elif tags.get("tourism"):
            kategori = tags["tourism"]
        elif tags.get("building"):
            kategori = tags["building"]
        else:
            kategori = "other"

        lat = el.get("lat") or el.get("center", {}).get("lat")
        lon = el.get("lon") or el.get("center", {}).get("lon")

        if lat and lon:
            rows.append({
                "nama_tempat": name,
                "kategori": kategori,
                "lat": lat,
                "lon": lon
            })

    df_loc = pd.DataFrame(rows)
    df_loc.to_parquet(os.path.join(LAKE_SILVER, 'locations.parquet'), index=False)
    print(f"✅ locations.parquet ({len(df_loc)} rows)")

    # -------------------------------------------------
    # 5. SILVER → GOLD (FEATURES)
    # -------------------------------------------------
    print("🏆 [GOLD] Features")

    # 👇 INI YANG TADI HILANG: Logic pembuatan df_gold_loc
    df_gold_loc = (
        df_loc
        .groupby(['kategori', 'nama_tempat', 'lat', 'lon'])
        .size()
        .reset_index(name='score')
        .sort_values('score', ascending=False)
        .head(300)
    )

    df_feat = (
        df_silver
        .groupby('archetype')
        .size()
        .reset_index(name='jumlah')
        .sort_values('jumlah', ascending=False)
    )

    df_feat.to_parquet(os.path.join(LAKE_GOLD, 'gold_features.parquet'), index=False)
    print("✅ gold_features.parquet")

    # -------------------------------------------------
    # 6. SILVER → GOLD (LOCATIONS – LENGKAP)
    # -------------------------------------------------
    print("🏆 [GOLD] Locations")

    df_gold_loc.to_parquet(os.path.join(LAKE_GOLD, 'gold_locations.parquet'), index=False)
    print("✅ gold_locations.parquet")

    # ========================================================
    # [BARU] 7. PUBLISH KE SQL DATA WAREHOUSE (DUCKDB)
    # ========================================================
    print("💾 [SQL] Memuat data ke DuckDB (Serving Layer)...")
    
    # Nama file database (akan muncul di folder project)
    db_path = os.path.join(BASE_DIR, 'social_radar_olap.duckdb')
    
    # Koneksi (akan membuat file jika belum ada)
    con = duckdb.connect(db_path)
    
    # LOAD: Masukkan DataFrame Gold ke tabel SQL
    # 'CREATE OR REPLACE' penting agar pipeline bisa dijalankan berulang kali tanpa error
    # Pastikan variabel df_feat dan df_gold_loc masih dikenali di sini (mereka ada di dalam fungsi run_elt yang sama)
    con.execute("CREATE OR REPLACE TABLE features AS SELECT * FROM df_feat")
    con.execute("CREATE OR REPLACE TABLE locations AS SELECT * FROM df_gold_loc")
    
    rules_parquet = os.path.join(LAKE_SILVER, 'rules_data.parquet')
    con.execute(f"CREATE OR REPLACE TABLE rules AS SELECT * FROM '{rules_parquet}'")

    # Verifikasi sederhana
    row_count = con.execute("SELECT COUNT(*) FROM locations").fetchone()[0]
    rule_count = con.execute("SELECT COUNT(*) FROM rules").fetchone()[0]
    print(f"✅ Database SQL Updated. Locs: {row_count}, Rules: {rule_count}")

    
    con.close()

    print("🎉 ELT SELESAI – DATA SIAP DI-QUERY VIA SQL")

# ==============================
if __name__ == "__main__":
    run_elt()
