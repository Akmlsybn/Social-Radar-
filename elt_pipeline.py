import pandas as pd
import os
import io
import shutil
import requests
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

os.makedirs(LAKE_BRONZE, exist_ok=True)
os.makedirs(LAKE_SILVER, exist_ok=True)
os.makedirs(LAKE_GOLD, exist_ok=True)

DB_PATH = os.path.join(LAKE_GOLD, 'social_radar_olap.duckdb')

# ==============================
# HELPER: CSV CLEANER (ANTI-CRASH)
# ==============================
def clean_csv_quotes(file_path):
    """ Mencoba membaca file dengan berbagai encoding agar tidak crash. """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except UnicodeDecodeError:
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
        if s.startswith('"') and s.endswith('"'):
            s = s[1:-1].replace('""', '"')
        cleaned.append(s)
    
    return io.StringIO("\n".join(cleaned))

# ==============================
# HELPER: TIME RULES ENGINE
# ==============================
# ==============================
# HELPER: TIME RULES ENGINE (VERSI BULLETPROOF / KEBAL TYPO)
# ==============================
def get_allowed_categories_by_time(con):
    tz = pytz.timezone('Asia/Makassar')
    now = datetime.now(tz)
    
    current_date_str = now.strftime("%Y-%m-%d")
    current_hour = now.hour
    
    # 1. Cek Hari Normal
    day_map = {0: 'Senin', 1: 'Selasa', 2: 'Rabu', 3: 'Kamis', 4: 'Jumat', 5: 'Sabtu', 6: 'Minggu'}
    real_day = day_map[now.weekday()]
    category_to_use = real_day 
    status_day = "Normal Day"

    print(f"\n⏰ [TIME CHECK] Real Time: {real_day}, {current_date_str} @ {current_hour}:00 WITA")

    # 2. Cek Holiday (Override ke Minggu)
    try:
        tbl_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'gold_holidays'").fetchone()[0]
        if tbl_exists > 0:
            res_holiday = con.execute(f"SELECT name FROM gold_holidays WHERE CAST(date AS VARCHAR) = '{current_date_str}'").fetchone()
            if res_holiday:
                holiday_name = res_holiday[0]
                status_day = f"Holiday ({holiday_name})"
                category_to_use = 'Minggu'
                print(f"🎉 HOLIDAY DETECTED: {holiday_name}! (Mode Liburan Aktif)")
    except Exception as e:
        print(f"⚠️ Gagal cek hari libur: {e}")

    # 3. Ambil Rule dari Database
    try:
        query = f"""
            SELECT rekomendasi_prioritas 
            FROM gold_rules 
            WHERE day_category = '{category_to_use}' 
              AND {current_hour} >= CAST(start_hour AS INTEGER) 
              AND {current_hour} < CAST(end_hour AS INTEGER)
            LIMIT 1
        """
        result = con.execute(query).fetchone()
        
        if not result:
            print("⚠️ Tidak ada rule waktu cocok (Mungkin tutup/istirahat).")
            return [] 

        # Bersihkan string dari CSV (Hapus kutip, bagi koma, hapus spasi kiri kanan)
        raw_list = [x.strip().replace('"', '') for x in result[0].split(',')]
        print(f"📜 Aturan Mentah DB: {raw_list}")
        
    except Exception as e:
        print(f"❌ Error Query Rules: {e}")
        return []

    # 4. Dictionary Mapping (NORMALISASI HURUF KECIL)
    # Kita ubah semua key jadi lowercase agar pencocokan tidak gagal karena huruf besar/kecil
    dictionary_map = {
        "kampus":       ["university", "school", "college"],
        "perpustakaan": ["library"],
        "toko buku":    ["book_store"],
        "museum":       ["museum", "arts_centre", "gallery"],
        "cafe":         ["cafe", "coffee_shop", "restaurant", "fast_food", "food_court"],
        "restoran":     ["restaurant", "fast_food", "food_court"],
        "mall":         ["mall", "department_store", "shop", "electronics", "clothes"],
        "taman kota":   ["park", "garden", "playground", "recreation_ground", "viewpoint", "river_bank"],
        "tempat ibadah":["place_of_worship", "mosque"],
        "gym":          ["gym", "sports_centre", "stadium"],
        "art gallery":  ["arts_centre", "gallery"],
        "thrift shop":  ["shop", "clothes"],
        "car free day": ["park", "street"],
        "hotel":        ["hotel"],
        "rumah":        ["residential"],
        "kost":         ["residential"]
    }
    
    allowed_technical_cats = []
    
    print("🔎 Mencocokkan Aturan vs Kamus:")
    for item in raw_list:
        # KUNCI PERBAIKAN: Ubah item dari CSV jadi huruf kecil bersih
        clean_item = item.lower().strip()
        
        if clean_item in dictionary_map:
            mapped = dictionary_map[clean_item]
            allowed_technical_cats.extend(mapped)
            print(f"   ✅ '{item}' -> {mapped}")
        else:
            print(f"   ❌ '{item}' (Clean: '{clean_item}') TIDAK DIKENALI di Kamus! Cek ejaan CSV.")
    
    unique_cats = list(set(allowed_technical_cats))
    print(f"🔓 Total Kategori Diizinkan: {len(unique_cats)} tipe teknis.")
    return unique_cats

# ==============================
# Extraxt Lokasi dari API
# ==============================
def extract_lokasi_api():
    """
    Mengambil data lokasi langsung dari URL (API)
    bukan dari file lokal.
    """
    # Contoh URL (Ganti dengan URL API asli atau Link Raw GitHub file json Anda)
    api_url = "https://raw.githubusercontent.com/rizkiiirr/Social-Radar/refs/heads/main/lokasi_bjm.json" 
    
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status() # Cek error HTTP
        
        data = response.json()
        print(f"[API] Sukses menarik data dari internet.")
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Gagal menarik API Lokasi: {e}")
        return None
    
# ==============================
# MAIN PIPELINE
# ==============================
def run_elt_pipeline():
    print("🚀 MEMULAI ELT PIPELINE")

    # 1. EXTRACT
    # A. Copy CSV Manual (Survey & Rules)
    raw_files = ['hasil_survey.csv', 'social_time_rules.csv']
    for f in raw_files:
        src = os.path.join(RAW_SOURCE, f)
        dst = os.path.join(LAKE_BRONZE, f)
        if os.path.exists(src): 
            shutil.copy(src, dst)
            # B. Tarik API Lokasi (AUTO DOWNLOAD)
    json_data = extract_lokasi_api() # Panggil fungsi API
    
    tgt_path = os.path.join(LAKE_BRONZE, 'lokasi_bjm.json')

    if json_data:
        # Jika API sukses, simpan hasilnya ke Bronze (Menimpa file lama)
        with open(tgt_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f)
        print("   -> Lokasi: Updated from API ✅")
    else:
        # Fallback: Jika API gagal, cek apakah ada file manual/lama
        if os.path.exists(os.path.join(RAW_SOURCE, 'lokasi_bjm.json')):
            shutil.copy(os.path.join(RAW_SOURCE, 'lokasi_bjm.json'), tgt_path)
            print("   -> Lokasi: API Failed, using Local Backup ⚠️")
        else:
            print("   -> Lokasi: DATA MISSING ❌")

    # 2. TRANSFORM SURVEY
    print("⚙️ [SILVER] Survey...")
    survey_path = os.path.join(LAKE_BRONZE, 'hasil_survey.csv')
    df_silver = pd.DataFrame() # Inisialisasi kosong agar aman

    if os.path.exists(survey_path):
        df_raw = pd.read_csv(clean_csv_quotes(survey_path))
        df_raw.columns = [c.lower().strip().replace(" ", "_") for c in df_raw.columns]
        
        arch_map = {
            'Religius': ('relig_fisik_cowo', 'relig_lokasi'),
            'Intellectual': ('intel_fisik_cowo', 'intel_lokasi'),
            'Creative': ('creative_fisik_cowo', 'creative_lokasi'),
            'Social Butterfly': ('social_fisik_cowo', 'social_lokasi'), # Perbaikan Nama Key
            'Sporty': ('sporty_fisik_cowo', 'sporty_lokasi'),
            'Techie': ('techie_fisik_cowo', 'techie_lokasi'),
            'Active': ('active_fisik_cowo', 'active_lokasi'),
            'Healing': ('active_fisik_cowo', 'active_lokasi') # Fallback jika Healing tidak ada kolomnya
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
    print("⚙️ [SILVER] Rules...")
    rules_path = os.path.join(LAKE_BRONZE, 'social_time_rules.csv')
    if os.path.exists(rules_path):
        df_rules = pd.read_csv(clean_csv_quotes(rules_path))
        df_rules.columns = [c.lower().strip().replace(" ", "_") for c in df_rules.columns]
        df_rules.to_parquet(os.path.join(LAKE_SILVER, 'rules_data.parquet'), index=False)

    # 4. TRANSFORM LOCATIONS
    print("⚙️ [SILVER] Locations...")
    loc_path = os.path.join(LAKE_BRONZE, 'lokasi_bjm.json')
    if os.path.exists(loc_path):
        with open(loc_path, 'r', encoding='utf-8') as f: data = json.load(f)
        rows = []
        for el in data.get("elements", []):
            tags = el.get("tags", {})
            name = tags.get("name")
            if not name: continue
            kategori = "other"
            for key in ["amenity", "leisure", "shop", "tourism", "building"]:
                if tags.get(key): kategori = tags[key]; break
            lat = el.get("lat") or el.get("center", {}).get("lat")
            lon = el.get("lon") or el.get("center", {}).get("lon")
            if lat and lon: rows.append({"nama_tempat": name, "kategori": kategori, "lat": lat, "lon": lon})
        df_loc = pd.DataFrame(rows)
        df_loc.to_parquet(os.path.join(LAKE_SILVER, 'locations.parquet'), index=False)

    # 4.5. TRANSFORM HOLIDAYS (NoSQL Source)
    print("⚙️ [SILVER] Holidays (NoSQL)...")
    holiday_path = os.path.join(BASE_DIR, 'holidays.json') # Ambil dari root folder
    
    if os.path.exists(holiday_path):
        try:
            # Baca sebagai JSON murni
            with open(holiday_path, 'r') as f:
                holiday_data = json.load(f)
            
            # Convert ke DataFrame
            df_holidays = pd.DataFrame(holiday_data)
            
            # Pastikan format tanggal konsisten
            df_holidays['date'] = pd.to_datetime(df_holidays['date']).dt.date
            
            # Simpan ke Silver (Parquet)
            df_holidays.to_parquet(os.path.join(LAKE_SILVER, 'holidays.parquet'), index=False)
            print(f"   -> Holidays loaded: {len(df_holidays)} events found.")
            
        except Exception as e:
            print(f"⚠️ Gagal memproses Holidays: {e}")
    else:
        print("⚠️ File holidays.json tidak ditemukan.")

    # 5. AGGREGATION (REVISI: FORCE ALL ARCHETYPES)
    print("🏆 [GOLD] Aggregating & Strategy Planning...")
    
    # Simpan Lokasi Terpopuler (Global Top)
    if not df_loc.empty:
        df_gold_loc = df_loc.groupby(['kategori', 'nama_tempat', 'lat', 'lon']).size().reset_index(name='score').sort_values('score', ascending=False).head(300)
        df_gold_loc.to_parquet(os.path.join(LAKE_GOLD, 'gold_locations.parquet'), index=False)
    else:
        df_gold_loc = pd.DataFrame(columns=['kategori', 'nama_tempat', 'lat', 'lon', 'score'])

    # Hitung Survey Asli (Tanpa Dummy)
    if not df_silver.empty:
        df_feat = (df_silver.groupby('archetype').size().reset_index(name='jumlah').sort_values('jumlah', ascending=False))
    else:
        df_feat = pd.DataFrame(columns=['archetype', 'jumlah'])
    
    # Simpan Data Features Asli
    df_feat.to_parquet(os.path.join(LAKE_GOLD, 'gold_features.parquet'), index=False)

    # --- PISAHKAN ARCHETYPE: ADA DATA VS KOSONG ---
    all_archs = ['Active', 'Creative', 'Healing', 'Intellectual', 'Religius', 'Social Butterfly', 'Sporty', 'Techie']
    existing_archs = df_feat['archetype'].tolist() if not df_feat.empty else []
    
    missing_archs = [arch for arch in all_archs if arch not in existing_archs]
    
    print(f"   -> Personalized Strategy untuk: {existing_archs}")
    print(f"   -> Global Top Strategy untuk: {missing_archs}")

    # =========================================
    # 6. SERVING (DUCKDB)
    # =========================================
    print(f"💾 [SQL] Building Data Warehouse...")
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE OR REPLACE TABLE gold_features AS SELECT * FROM df_feat")
    con.execute("CREATE OR REPLACE TABLE gold_locations AS SELECT * FROM df_gold_loc")
    con.execute(f"CREATE OR REPLACE TABLE gold_rules AS SELECT * FROM '{os.path.join(LAKE_SILVER, 'rules_data.parquet')}'")
    con.execute(f"CREATE OR REPLACE TABLE gold_holidays AS SELECT * FROM '{os.path.join(LAKE_SILVER, 'holidays.parquet')}'")

    # Ambil Filter Waktu
    allowed_cats = get_allowed_categories_by_time(con)
    time_filter_sql = ""
    if allowed_cats:
        allowed_sql_str = ", ".join([f"'{x}'" for x in allowed_cats])
        time_filter_sql = f"AND t2.kategori IN ({allowed_sql_str})"
    else:
        time_filter_sql = "AND 1=0"

    # Ambil Context Cuaca
    cuaca_main = "Clear"
    indoor_cats = "'mall', 'cafe', 'library', 'museum', 'book_store', 'restaurant', 'fast_food', 'food_court', 'shop', 'electronics', 'clothes', 'gym', 'mosque', 'place_of_worship'"
    try:
        res = con.execute("SELECT main FROM context_weather LIMIT 1").fetchone()
        if res: cuaca_main = res[0]
    except: pass

    # ========================================================
    # QUERY REKOMENDASI CERDAS (HYBRID)
    # ========================================================
    # Kita menggunakan UNION ALL untuk menggabungkan dua logika:
    # 1. Logic A: Untuk Archetype yang ADA data surveynya (Personalized by Category)
    # 2. Logic B: Untuk Archetype yang KOSONG (Fallback ke Top Global Places)
    
    # Siapkan list missing untuk di-inject ke SQL
    missing_sql_list = ", ".join([f"'{x}'" for x in missing_archs])
    
    query = f"""
        CREATE OR REPLACE TABLE gold_daily_recommendations AS
        WITH Combined AS (
            -- LOGIC A: PERSONALIZED (Untuk yang punya data survey)
            SELECT 
                t1.archetype, 
                t2.nama_tempat, t2.lat, t2.lon, t2.kategori, t2.score,
                'Personalized' as metode
            FROM gold_features t1
            JOIN gold_locations t2 ON 
                (
                    (t1.archetype = 'Sporty' AND t2.kategori IN ('gym', 'park', 'stadium', 'sports_centre')) OR
                    (t1.archetype = 'Religius' AND t2.kategori IN ('place_of_worship', 'mosque')) OR
                    (t1.archetype = 'Intellectual' AND t2.kategori IN ('library', 'university', 'book_store', 'school')) OR
                    (t1.archetype = 'Social Butterfly' AND t2.kategori IN ('cafe', 'food_court', 'restaurant', 'fast_food')) OR
                    (t1.archetype = 'Healing' AND t2.kategori IN ('park', 'garden', 'river_bank', 'viewpoint')) OR
                    (t1.archetype = 'Techie' AND t2.kategori IN ('electronics', 'computer_shop', 'cafe', 'coworking_space')) OR
                    (t1.archetype = 'Creative' AND t2.kategori IN ('arts_centre', 'gallery', 'museum', 'cafe')) OR
                    (t1.archetype = 'Active' AND t2.kategori IN ('park', 'playground', 'recreation_ground'))
                )
            WHERE 1=1 {time_filter_sql}

            UNION ALL

            -- LOGIC B: FALLBACK / COLD START (Untuk yang datanya kosong)
            -- Kita ambil Top 10 Lokasi apapun kategorinya, lalu kita tempelkan label archetype yang hilang
            SELECT 
                m.arch_name as archetype,
                t2.nama_tempat, t2.lat, t2.lon, t2.kategori, t2.score,
                'Global Top (Fallback)' as metode
            FROM (SELECT unnest([{missing_sql_list}]) as arch_name) m -- List missing archs
            CROSS JOIN (
                SELECT * FROM gold_locations 
                ORDER BY score DESC 
                LIMIT 20 -- Ambil top 20 global sebagai kandidat
            ) t2
            WHERE 1=1 -- Disini kita TIDAK memfilter kategori (biar user tetap dapat rekomendasi tempat bagus)
        ),
        Finalized AS (
            SELECT *,
                -- LOGIKA STRATEGI & WARNA (Sama seperti sebelumnya)
                CASE 
                    WHEN '{cuaca_main}' LIKE '%Rain%' AND kategori NOT IN ({indoor_cats}) 
                    THEN '**Strategi 💘:** Cuaca hujan. Bawa payung atau cari opsi indoor.'
                    ELSE '**Strategi 💘:** Cuaca mendukung. Segera meluncur!'
                END as pesan_strategi,
                
                CASE 
                    WHEN '{cuaca_main}' LIKE '%Rain%' AND kategori NOT IN ({indoor_cats}) 
                    THEN '#9d174d' ELSE '#f9a8d4' 
                END as warna_border,

                ROW_NUMBER() OVER (PARTITION BY archetype ORDER BY score DESC, random()) as rank_urutan
            FROM Combined
            -- Pastikan jika missing list kosong, query tidak error (sudah ditangani di Python flow tapi SQL butuh validasi)
            WHERE archetype IS NOT NULL
        )
        SELECT * FROM Finalized WHERE rank_urutan <= 10 AND archetype != 'IGNORE_ME'
    """
    
# Eksekusi Query hanya jika ada data lokasi
    if not df_gold_loc.empty:
        
        # JANGAN LAKUKAN SPLIT QUERY! 
        # Kita ingin logika 'Finalized' (Warna & Strategi) tetap jalan meskipun missing_archs kosong.
        
        # HACK: Jika list missing kosong, ganti dengan dummy agar SQL tidak error
        if not missing_archs:
             query = query.replace(f"unnest([{missing_sql_list}])", "unnest(['IGNORE_ME'])")

        con.execute(query)
        count = con.execute("SELECT COUNT(*) FROM gold_daily_recommendations").fetchone()[0]
        print(f"✅ ELT Selesai! {count} rekomendasi siap saji tersimpan (Hybrid Strategy).")
    else:
        print("❌ Data Lokasi Kosong. Pipeline finish without result.")

        # HACK KECIL: Jika missing_archs kosong, SQL "unnest([])" akan error.
       # ... (bagian eksekusi query)
    if not df_gold_loc.empty:
        if not missing_archs:
             query = query.replace(f"unnest([{missing_sql_list}])", "unnest(['IGNORE_ME'])")

        con.execute(query)
        count = con.execute("SELECT COUNT(*) FROM gold_daily_recommendations").fetchone()[0]
        print(f"✅ ELT Selesai! {count} rekomendasi siap saji tersimpan (Hybrid Strategy).")
    else:
        print("❌ Data Lokasi Kosong. Pipeline finish without result.")

    con.close()
    # Tidak ada print lagi disini

if __name__ == "__main__":
    run_elt_pipeline()