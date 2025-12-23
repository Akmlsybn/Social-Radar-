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

# Pastikan folder ada
os.makedirs(LAKE_BRONZE, exist_ok=True)
os.makedirs(LAKE_SILVER, exist_ok=True)
os.makedirs(LAKE_GOLD, exist_ok=True)

# Path Database Final (Disimpan di Gold Layer agar rapi)
DB_PATH = os.path.join(LAKE_GOLD, 'social_radar_olap.duckdb')

# ==============================
# HELPER
# ==============================
def clean_csv_quotes(file_path):
    """Membersihkan CSV dari quote yang rusak"""
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
# PERBAIKAN: Nama fungsi diubah menjadi run_elt_pipeline (bukan run_elt)
def run_elt_pipeline():
    print("🚀 MEMULAI ELT PIPELINE")

    # -------------------------------------------------
    # 1. EXTRACT → BRONZE (Raw Ingestion)
    # -------------------------------------------------
    raw_files = ['hasil_survey.csv', 'social_time_rules.csv', 'lokasi_bjm.json']
    for f in raw_files:
        src = os.path.join(RAW_SOURCE, f)
        dst = os.path.join(LAKE_BRONZE, f)
        if os.path.exists(src):
            shutil.copy(src, dst)
            print(f"✅ [BRONZE] {f} tersimpan.")

    # -------------------------------------------------
    # 2. BRONZE → SILVER (Survey Transformation)
    # -------------------------------------------------
    print("⚙️ [SILVER] Processing Survey...")
    survey_path = os.path.join(LAKE_BRONZE, 'hasil_survey.csv')
    
    if os.path.exists(survey_path):
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
            if fisik_col in df_raw.columns:
                temp = df_raw[['timestamp', 'gender', fisik_col, lokasi_col]].copy()
                temp.rename(columns={fisik_col: 'ciri_fisik', lokasi_col: 'habitat'}, inplace=True)
                temp['archetype'] = arch
                temp.dropna(subset=['ciri_fisik'], inplace=True)
                rows.append(temp)

        if rows:
            df_silver = pd.concat(rows, ignore_index=True)
            df_silver.to_parquet(os.path.join(LAKE_SILVER, 'survey_data.parquet'), index=False)
            print("   ↳ survey_data.parquet created.")

    # -------------------------------------------------
    # 3. BRONZE → SILVER (Rules Standardization)
    # -------------------------------------------------
    print("⚙️ [SILVER] Processing Rules...")
    rules_path = os.path.join(LAKE_BRONZE, 'social_time_rules.csv')
    if os.path.exists(rules_path):
        csv_rules = clean_csv_quotes(rules_path)
        df_rules = pd.read_csv(csv_rules)
        df_rules.columns = [c.lower().strip().replace(" ", "_") for c in df_rules.columns]
        df_rules.to_parquet(os.path.join(LAKE_SILVER, 'rules_data.parquet'), index=False)
        print("   ↳ rules_data.parquet created.")

    # -------------------------------------------------
    # 4. BRONZE → SILVER (JSON Parsing)
    # -------------------------------------------------
    print("⚙️ [SILVER] Processing Locations (JSON)...")
    loc_path = os.path.join(LAKE_BRONZE, 'lokasi_bjm.json')
    if os.path.exists(loc_path):
        with open(loc_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        rows = []
        for el in data.get("elements", []):
            tags = el.get("tags", {})
            name = tags.get("name")
            if not name: continue

            # Mapping Kategori Prioritas
            kategori = "other"
            for key in ["amenity", "leisure", "shop", "tourism", "building"]:
                if tags.get(key):
                    kategori = tags[key]
                    break

            lat = el.get("lat") or el.get("center", {}).get("lat")
            lon = el.get("lon") or el.get("center", {}).get("lon")

            if lat and lon:
                rows.append({"nama_tempat": name, "kategori": kategori, "lat": lat, "lon": lon})

        df_loc = pd.DataFrame(rows)
        df_loc.to_parquet(os.path.join(LAKE_SILVER, 'locations.parquet'), index=False)
        print(f"   ↳ locations.parquet created ({len(df_loc)} rows).")

    # -------------------------------------------------
    # 5. SILVER → GOLD (Pandas Aggregation)
    # -------------------------------------------------
    print("🏆 [GOLD] Aggregating Features & Locations...")
    
    # Gold Locations (Top 300)
    df_gold_loc = (
        df_loc.groupby(['kategori', 'nama_tempat', 'lat', 'lon'])
        .size().reset_index(name='score')
        .sort_values('score', ascending=False)
        .head(300)
    )
    df_gold_loc.to_parquet(os.path.join(LAKE_GOLD, 'gold_locations.parquet'), index=False)

    # Gold Features (Stats)
    df_feat = (
        df_silver.groupby('archetype')
        .size().reset_index(name='jumlah')
        .sort_values('jumlah', ascending=False)
    )
    df_feat.to_parquet(os.path.join(LAKE_GOLD, 'gold_features.parquet'), index=False)

    # ========================================================
    # 6. SERVING LAYER (DUCKDB SQL AUTOMATION)
    # ========================================================
    print(f"💾 [SQL] Membangun Data Warehouse di: {DB_PATH}")
    
    # 1. BUKA KONEKSI DATABASE
    con = duckdb.connect(DB_PATH)
    
    # 2. LOAD DATAFRAME KE TABEL SQL
    # Kita load dulu agar tabelnya eksis saat dikuery nanti
    print("   ↳ Loading Tables: gold_features, gold_locations, gold_rules")
    con.execute("CREATE OR REPLACE TABLE gold_features AS SELECT * FROM df_feat")
    con.execute("CREATE OR REPLACE TABLE gold_locations AS SELECT * FROM df_gold_loc")
    
    # Load rules dari parquet file yang tadi dibuat
    rules_parquet = os.path.join(LAKE_SILVER, 'rules_data.parquet')
    con.execute(f"CREATE OR REPLACE TABLE gold_rules AS SELECT * FROM '{rules_parquet}'")

    # 3. BUAT TABEL REKOMENDASI FINAL (Pre-Computed)
    print("   ↳ Creating Table: gold_daily_recommendations (FINAL ANSWER)")
    
    # Query ini menggunakan tabel 'gold_features' dan 'gold_locations' yang barusan kita buat
    con.execute("""
        CREATE OR REPLACE TABLE gold_daily_recommendations AS
        WITH Ranked AS (
            SELECT 
                t1.archetype,
                t2.nama_tempat,
                t2.lat,
                t2.lon,
                t2.kategori,
                t2.score,
                -- Random() membuat rekomendasi berubah tiap kali pipeline jalan
                ROW_NUMBER() OVER (PARTITION BY t1.archetype ORDER BY t2.score DESC, random()) as rank_urutan
            FROM gold_features t1
            JOIN gold_locations t2 ON 
                -- MAPPING LOGIKA BISNIS
                (t1.archetype = 'Sporty'           AND t2.kategori IN ('gym', 'park', 'stadium', 'sports_centre')) OR
                (t1.archetype = 'Religius'         AND t2.kategori IN ('place_of_worship', 'mosque')) OR
                (t1.archetype = 'Intellectual'     AND t2.kategori IN ('library', 'university', 'book_store', 'school')) OR
                (t1.archetype = 'Social Butterfly' AND t2.kategori IN ('cafe', 'food_court', 'restaurant', 'fast_food')) OR
                (t1.archetype = 'Healing'          AND t2.kategori IN ('park', 'garden', 'river_bank', 'viewpoint')) OR
                (t1.archetype = 'Techie'           AND t2.kategori IN ('electronics', 'computer_shop', 'cafe')) OR
                (t1.archetype = 'Creative'         AND t2.kategori IN ('arts_centre', 'gallery', 'museum', 'cafe')) OR
                (t1.archetype = 'Active'           AND t2.kategori IN ('park', 'playground', 'recreation_ground'))
        )
        -- Ambil HANYA Juara 1 untuk setiap tipe
        SELECT * FROM Ranked WHERE rank_urutan <= 10
    """)

    # 4. VERIFIKASI & TUTUP
    count_rec = con.execute("SELECT COUNT(*) FROM gold_daily_recommendations").fetchone()[0]
    con.close()

    print(f"✅ ELT Selesai! {count_rec} rekomendasi final siap disajikan di App.")

# ==============================
if __name__ == "__main__":
    run_elt_pipeline()