import pandas as pd
import os
import io
import shutil
import json
import duckdb
from datetime import datetime
import pytz 

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

DB_PATH = os.path.join(LAKE_GOLD, 'social_radar_olap.duckdb')

# ==============================
# HELPER: CSV CLEANER
# ==============================
def clean_csv_quotes(file_path):
    """
    Mencoba membaca file dengan berbagai encoding agar tidak crash 
    saat file diedit di Excel (Windows).
    """
    # Cara 1: Coba UTF-8 (Standar Modern)
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        # Cara 2: Jika gagal, coba Latin-1 (Standar Windows/Excel)
        print(f"⚠️ Warning: Encoding file {os.path.basename(file_path)} bukan UTF-8. Mencoba Latin-1...")
        try:
            with open(file_path, 'r', encoding='latin-1') as f:
                lines = f.readlines()
        except Exception as e:
            print(f"❌ Gagal total membaca file. Error: {e}")
            return io.StringIO("") 

    cleaned = []
    for line in lines:
        s = line.strip()
        # Perbaikan quote ganda dari Excel
        if s.startswith('"') and s.endswith('"'):
            s = s[1:-1].replace('""', '"')
        cleaned.append(s)
    
    return io.StringIO("\n".join(cleaned))

# ==============================
# HELPER: TIME RULES ENGINE (LOGIKA BARU)
# ==============================
def get_allowed_categories_by_time(con):
    """
    Membaca gold_rules dan mengembalikan list kategori teknis
    yang DIPERBOLEHKAN berdasarkan jam sekarang (WITA).
    """
    # 1. Tentukan Waktu Sekarang (WITA)
    tz = pytz.timezone('Asia/Makassar')
    now = datetime.now(tz)
    current_hour = now.hour
    
    # Mapping Hari Python (0=Senin) ke String CSV
    day_map = {0: 'Senin', 1: 'Selasa', 2: 'Rabu', 3: 'Kamis', 4: 'Jumat', 5: 'Sabtu', 6: 'Minggu'}
    current_day = day_map[now.weekday()]
    
    print(f"⏰ Context Waktu: {current_day}, Jam {current_hour}:00 WITA")
    
    # 2. Ambil Rules dari Database
    # Mencari fase waktu yang cocok dengan jam sekarang
    try:
        query = f"""
            SELECT rekomendasi_prioritas 
            FROM gold_rules 
            WHERE day_category = '{current_day}' 
              AND {current_hour} >= start_hour 
              AND {current_hour} < end_hour
            LIMIT 1
        """
        result = con.execute(query).fetchone()
        
        if not result:
            print("⚠️ Tidak ada rule waktu cocok. Default: Buka Semua.")
            return [] # Empty list = No filter

        # Contoh output raw dari CSV: "Kampus, Perpustakaan, Toko Buku"
        raw_string = result[0]
        # Bersihkan string dari quote aneh jika ada
        raw_list = raw_string.replace('"', '').split(',')
        raw_list = [x.strip() for x in raw_list]
        print(f"📜 Rule Aktif (Bahasa Manusia): {raw_list}")
        
    except Exception as e:
        print(f"⚠️ Error membaca rules: {e}")
        return []

    # 3. KAMUS PENERJEMAH (Bahasa Aturan -> Bahasa Teknis Database)
    # Ini memetakan kata di CSV (misal "Taman Kota") ke kategori OSM (misal "park")
    dictionary_map = {
        "Kampus":       ["university", "school", "college"],
        "Perpustakaan": ["library"],
        "Toko Buku":    ["book_store"],
        "Museum":       ["museum", "arts_centre", "gallery"],
        "Cafe":         ["cafe", "coffee_shop", "restaurant", "fast_food", "food_court"],
        "Mall":         ["mall", "department_store", "shop", "electronics", "clothes"],
        "Taman Kota":   ["park", "garden", "playground", "recreation_ground", "viewpoint", "river_bank"],
        "Tempat Ibadah":["place_of_worship", "mosque"],
        "Gym":          ["gym", "sports_centre", "stadium"],
        "Art Gallery":  ["arts_centre", "gallery"],
        "Thrift Shop":  ["shop", "clothes"],
        "Car Free Day": ["park", "street"],
        "Hotel":        ["hotel"]
    }

    allowed_technical_cats = []
    
    for item in raw_list:
        # Cari padanan kata di kamus
        if item in dictionary_map:
            allowed_technical_cats.extend(dictionary_map[item])
    
    # Hapus duplikat
    allowed_technical_cats = list(set(allowed_technical_cats))
    
    print(f"✅ Filter Kategori Teknis: {allowed_technical_cats}")
    return allowed_technical_cats

# ==============================
# MAIN PIPELINE
# ==============================
def run_elt_pipeline():
    print("🚀 MEMULAI ELT PIPELINE")

    # 1. EXTRACT
    raw_files = ['hasil_survey.csv', 'social_time_rules.csv', 'lokasi_bjm.json']
    for f in raw_files:
        src = os.path.join(RAW_SOURCE, f)
        dst = os.path.join(LAKE_BRONZE, f)
        if os.path.exists(src):
            shutil.copy(src, dst)

    # 2. TRANSFORM SURVEY
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

    # 3. TRANSFORM RULES
    print("⚙️ [SILVER] Processing Rules...")
    rules_path = os.path.join(LAKE_BRONZE, 'social_time_rules.csv')
    if os.path.exists(rules_path):
        csv_rules = clean_csv_quotes(rules_path)
        df_rules = pd.read_csv(csv_rules)
        df_rules.columns = [c.lower().strip().replace(" ", "_") for c in df_rules.columns]
        df_rules.to_parquet(os.path.join(LAKE_SILVER, 'rules_data.parquet'), index=False)

    # 4. TRANSFORM LOCATIONS
    print("⚙️ [SILVER] Processing Locations...")
    loc_path = os.path.join(LAKE_BRONZE, 'lokasi_bjm.json')
    if os.path.exists(loc_path):
        with open(loc_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        rows = []
        for el in data.get("elements", []):
            tags = el.get("tags", {})
            name = tags.get("name")
            if not name: continue
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

    # 5. AGGREGATION (GOLD)
    print("🏆 [GOLD] Aggregating...")
    df_gold_loc = df_loc.groupby(['kategori', 'nama_tempat', 'lat', 'lon']).size().reset_index(name='score').sort_values('score', ascending=False).head(300)
    df_gold_loc.to_parquet(os.path.join(LAKE_GOLD, 'gold_locations.parquet'), index=False)

    df_feat = (df_silver.groupby('archetype').size().reset_index(name='jumlah').sort_values('jumlah', ascending=False))
    df_feat.to_parquet(os.path.join(LAKE_GOLD, 'gold_features.parquet'), index=False)

    # ========================================================
    # 6. SERVING LAYER (THE BRAIN)
    # ========================================================
    print(f"💾 [SQL] Building Data Warehouse di: {DB_PATH}")
    con = duckdb.connect(DB_PATH)
    
    # Load Tables
    con.execute("CREATE OR REPLACE TABLE gold_features AS SELECT * FROM df_feat")
    con.execute("CREATE OR REPLACE TABLE gold_locations AS SELECT * FROM df_gold_loc")
    
    rules_parquet = os.path.join(LAKE_SILVER, 'rules_data.parquet')
    con.execute(f"CREATE OR REPLACE TABLE gold_rules AS SELECT * FROM '{rules_parquet}'")

    # --------------------------------------------------------
    # LANGKAH KUNCI: AMBIL FILTER WAKTU DARI RULES
    # --------------------------------------------------------
    allowed_cats = get_allowed_categories_by_time(con)
    
    # Siapkan string SQL untuk filter (misal: "'cafe', 'park'")
    time_filter_sql = ""
    if allowed_cats:
        # Format list python menjadi string SQL: 'item1', 'item2'
        allowed_sql_str = ", ".join([f"'{x}'" for x in allowed_cats])
        # INJEKSI FILTER: Tempat yang dipilih HARUS ada dalam daftar yang dibolehkan
        time_filter_sql = f"AND t2.kategori IN ({allowed_sql_str})"
        print("🔒 Applying Time-Based Filtering (Context Awareness Active)")
    else:
        print("🔓 No Time Restrictions Applied (Mungkin subuh/tidak ada rule)")

    # --------------------------------------------------------
    # QUERY FINAL DENGAN INJEKSI FILTER WAKTU
    # --------------------------------------------------------
    # Perhatikan bagian {time_filter_sql} di bawah
    query = f"""
        CREATE OR REPLACE TABLE gold_daily_recommendations AS
        WITH Ranked AS (
            SELECT 
                t1.archetype,
                t2.nama_tempat,
                t2.lat,
                t2.lon,
                t2.kategori,
                t2.score,
                ROW_NUMBER() OVER (PARTITION BY t1.archetype ORDER BY t2.score DESC, random()) as rank_urutan
            FROM gold_features t1
            JOIN gold_locations t2 ON 
                (
                    -- LOGIKA PREFERENSI (User Matching)
                    (t1.archetype = 'Sporty'           AND t2.kategori IN ('gym', 'park', 'stadium', 'sports_centre')) OR
                    (t1.archetype = 'Religius'         AND t2.kategori IN ('place_of_worship', 'mosque')) OR
                    (t1.archetype = 'Intellectual'     AND t2.kategori IN ('library', 'university', 'book_store', 'school')) OR
                    (t1.archetype = 'Social Butterfly' AND t2.kategori IN ('cafe', 'food_court', 'restaurant', 'fast_food')) OR
                    (t1.archetype = 'Healing'          AND t2.kategori IN ('park', 'garden', 'river_bank', 'viewpoint')) OR
                    (t1.archetype = 'Techie'           AND t2.kategori IN ('electronics', 'computer_shop', 'cafe', 'coworking_space')) OR
                    (t1.archetype = 'Creative'         AND t2.kategori IN ('arts_centre', 'gallery', 'museum', 'cafe')) OR
                    (t1.archetype = 'Active'           AND t2.kategori IN ('park', 'playground', 'recreation_ground'))
                )
                -- INI DIA RAEDAHNYA:
                {time_filter_sql}
        )
        SELECT * FROM Ranked WHERE rank_urutan <= 10
    """
    
    con.execute(query)

    # Verifikasi
    count_rec = con.execute("SELECT COUNT(*) FROM gold_daily_recommendations").fetchone()[0]
    con.close()
    print(f"✅ ELT Selesai! {count_rec} rekomendasi Context-Aware siap disajikan.")

if __name__ == "__main__":
    run_elt_pipeline()