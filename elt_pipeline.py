import pandas as pd
import os
import io
import shutil
import requests
import sqlite3
import json
import duckdb
from datetime import datetime
import pytz
from minio import Minio

# --- KONFIGURASI MINIO ---
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY", "")
BUCKET_NAME = "datalake"

# Folder Kerja Sementara (Ephemeral)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP_DIR = "/tmp/social_radar"
os.makedirs(TEMP_DIR, exist_ok=True)

# Inisialisasi MinIO Client
client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

# --- FUNGSI BANTU MINIO ---
def upload_file(folder, filename, file_path):
    """Upload file lokal ke MinIO bucket"""
    try:
        if not client.bucket_exists(BUCKET_NAME): client.make_bucket(BUCKET_NAME)
        object_name = f"{folder}/{filename}"
        client.fput_object(BUCKET_NAME, object_name, file_path)
        print(f"   ☁️ [MINIO] Uploaded: {object_name}")
    except Exception as e: print(f"   ❌ Error Upload MinIO: {e}")

# --- FUNGSI LOGIKA 
def clean_csv_quotes(file_path):
    """ Mencoba membaca file dengan berbagai encoding agar tidak crash. """
    try:
        with open(file_path, 'r', encoding='utf-8') as f: lines = f.readlines()
    except UnicodeDecodeError:
        print(f"Warning: Encoding file {os.path.basename(file_path)} bukan UTF-8. Mencoba Latin-1...")
        try:
            with open(file_path, 'r', encoding='latin-1') as f: lines = f.readlines()
        except Exception as e:
            return io.StringIO("") 

    cleaned = []
    for line in lines:
        s = line.strip()
        if s.startswith('"') and s.endswith('"'):
            s = s[1:-1].replace('""', '"')
        cleaned.append(s)
    
    return io.StringIO("\n".join(cleaned))

def get_allowed_categories_by_time(con):
    tz = pytz.timezone('Asia/Makassar')
    now = datetime.now(tz)
    current_date_str = now.strftime("%Y-%m-%d")
    current_hour = now.hour
    
    day_map = {0: 'Senin', 1: 'Selasa', 2: 'Rabu', 3: 'Kamis', 4: 'Jumat', 5: 'Sabtu', 6: 'Minggu'}
    real_day = day_map[now.weekday()]
    category_to_use = real_day 
    print(f"\n[TIME CHECK] Real Time: {real_day}, {current_date_str} @ {current_hour}:00 WITA")

    try:
        tbl_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'gold_holidays'").fetchone()[0]
        if tbl_exists > 0:
            res_holiday = con.execute(f"SELECT name FROM gold_holidays WHERE CAST(date AS VARCHAR) = '{current_date_str}'").fetchone()
            if res_holiday:
                category_to_use = 'Minggu'
                print(f"HOLIDAY DETECTED: {res_holiday[0]}! (Mode Liburan Aktif)")
    except Exception as e: print(f"Gagal cek hari libur: {e}")

    # Ambil Rule
    try:
        query = f"""
            SELECT rekomendasi_prioritas FROM gold_rules 
            WHERE day_category = '{category_to_use}' 
            AND {current_hour} >= CAST(start_hour AS INTEGER) 
            AND {current_hour} < CAST(end_hour AS INTEGER) LIMIT 1
        """
        result = con.execute(query).fetchone()
        if not result: return [] 
        raw_list = [x.strip().replace('"', '') for x in result[0].split(',')]
    except Exception as e: return []

    # Dictionary Mapping 
    dictionary_map = {
        "kampus": ["university", "school", "college"], "perpustakaan": ["library"], "toko buku": ["book_store"],
        "museum": ["museum", "arts_centre", "gallery"], "cafe": ["cafe", "coffee_shop", "restaurant", "fast_food", "food_court"],
        "restoran": ["restaurant", "fast_food", "food_court"], "mall": ["mall", "department_store", "shop", "electronics", "clothes"],
        "taman kota": ["park", "garden", "playground", "recreation_ground", "viewpoint", "river_bank"],
        "tempat ibadah":["place_of_worship", "mosque"], "gym": ["gym", "sports_centre", "stadium"],
        "art gallery": ["arts_centre", "gallery"], "thrift shop": ["shop", "clothes"],
        "car free day": ["park", "street"], "hotel": ["hotel"], "rumah": ["residential"], "kost": ["residential"]
    }
    
    allowed_technical_cats = []
    for item in raw_list:
        clean_item = item.lower().strip()
        if clean_item in dictionary_map:
            allowed_technical_cats.extend(dictionary_map[clean_item])
    
    return list(set(allowed_technical_cats))

def extract_lokasi_api():
    try:
        api_url = "https://raw.githubusercontent.com/rizkiiirr/Social-Radar/refs/heads/main/lokasi_bjm.json" 
        response = requests.get(api_url, timeout=10)
        return response.json() if response.status_code == 200 else None
    except: return None

# Extract Cuaca 
def extract_weather_data():
    data = {"main": "Clouds", "temp": 29.5, "description": "berawan (default)"}
    try:
        if OPENWEATHER_API_KEY:
            url = f"https://api.openweathermap.org/data/2.5/weather?q=Banjarmasin&appid={OPENWEATHER_API_KEY}&units=metric"
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                d = r.json()
                data = {"main": d['weather'][0]['main'], "temp": d['main']['temp'], "description": d['weather'][0]['description']}
    except: pass
    return data

def run_elt_pipeline():
    print("🚀 MEMULAI ELT PIPELINE (LAKEHOUSE MODE)")

    # 0. WEATHER (Bronze -> Silver -> MinIO)
    w_data = extract_weather_data()
    pd.DataFrame([w_data]).to_parquet(os.path.join(TEMP_DIR, 'context_weather.parquet'), index=False)
    upload_file("gold", "context_weather.parquet", os.path.join(TEMP_DIR, 'context_weather.parquet'))

    # 1. EXTRACT (BRONZE)
    print("[BRONZE] Extracting Data...")
    SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQn2iBR8DjQEgmZeA4ieEFLr1876iA5fi0F1p5hcNqYNuYEa9Qe6YlUoYRLPubzJ0D1jyD1P8on29jY/pub?output=csv" 
    
    p_survey = os.path.join(TEMP_DIR, 'hasil_survey.csv')
    p_rules = os.path.join(TEMP_DIR, 'social_time_rules.csv')
    p_loc = os.path.join(TEMP_DIR, 'lokasi_bjm.json')

    try:
        with open(p_survey, 'wb') as f: f.write(requests.get(SHEET_URL).content)
        upload_file("bronze", "hasil_survey.csv", p_survey)
    except: 
        if os.path.exists('hasil_survey.csv'): shutil.copy('hasil_survey.csv', p_survey)

    if os.path.exists('social_time_rules.csv'):
        shutil.copy('social_time_rules.csv', p_rules)
        upload_file("bronze", "social_time_rules.csv", p_rules)

    json_data = extract_lokasi_api()
    if json_data:
        with open(p_loc, 'w', encoding='utf-8') as f: json.dump(json_data, f)
        upload_file("bronze", "lokasi_bjm.json", p_loc)
    elif os.path.exists('lokasi_bjm.json'):
        shutil.copy('lokasi_bjm.json', p_loc)

    # 2. TRANSFORM (SILVER)
    print("[SILVER] Transforming...")
    
    # Survey
    df_silver = pd.DataFrame()
    if os.path.exists(p_survey):
        df_raw = pd.read_csv(clean_csv_quotes(p_survey))
        df_raw.columns = [c.lower().strip().replace(" ", "_") for c in df_raw.columns]
        arch_map = {
            'Religius': ('relig_fisik_cowo', 'relig_lokasi'), 'Intellectual': ('intel_fisik_cowo', 'intel_lokasi'),
            'Creative': ('creative_fisik_cowo', 'creative_lokasi'), 'Social Butterfly': ('social_fisik_cowo', 'social_lokasi'), 
            'Sporty': ('sporty_fisik_cowo', 'sporty_lokasi'), 'Techie': ('techie_fisik_cowo', 'techie_lokasi'),
            'Active': ('active_fisik_cowo', 'active_lokasi'), 'Healing': ('active_fisik_cowo', 'active_lokasi') 
        }
        rows = []
        for arch, (f_col, l_col) in arch_map.items():
            if f_col in df_raw.columns:
                temp = df_raw[['timestamp', 'gender', f_col, l_col]].copy()
                temp.rename(columns={f_col:'ciri_fisik', l_col:'habitat'}, inplace=True)
                temp['archetype'] = arch; temp.dropna(subset=['ciri_fisik'], inplace=True)
                rows.append(temp)
        if rows:
            df_silver = pd.concat(rows, ignore_index=True)
            path_svy_silver = os.path.join(TEMP_DIR, 'survey_data.parquet')
            df_silver.to_parquet(path_svy_silver, index=False)
            upload_file("silver", "survey_data.parquet", path_svy_silver)

    # Rules
    if os.path.exists(p_rules):
        df_rules = pd.read_csv(clean_csv_quotes(p_rules))
        df_rules.columns = [c.lower().strip().replace(" ", "_") for c in df_rules.columns]
        path_rules_silver = os.path.join(TEMP_DIR, 'rules_data.parquet')
        df_rules.to_parquet(path_rules_silver, index=False)
        upload_file("silver", "rules_data.parquet", path_rules_silver)

    # Locations
    df_loc = pd.DataFrame()
    if os.path.exists(p_loc):
        with open(p_loc, 'r') as f: data = json.load(f)
        rows = []
        for el in data.get("elements", []):
            tags = el.get("tags", {})
            name = tags.get("name")
            if not name: continue
            cat = "other"
            for k in ["amenity", "leisure", "shop", "tourism", "building"]: 
                if tags.get(k): cat = tags[k]; break
            lat = el.get("lat") or el.get("center", {}).get("lat")
            lon = el.get("lon") or el.get("center", {}).get("lon")
            if lat and lon: rows.append({"nama_tempat": name, "kategori": cat, "lat": lat, "lon": lon})
        df_loc = pd.DataFrame(rows)
        path_loc_silver = os.path.join(TEMP_DIR, 'locations.parquet')
        df_loc.to_parquet(path_loc_silver, index=False)
        upload_file("silver", "locations.parquet", path_loc_silver)

    # Holidays (SQLite Local)
    if os.path.exists('holidays.db'):
        try:
            con_sql = sqlite3.connect('holidays.db')
            df_hol = pd.read_sql_query("SELECT date, name FROM holidays", con_sql)
            con_sql.close()
            df_hol['date'] = pd.to_datetime(df_hol['date']).dt.date
            path_hol_silver = os.path.join(TEMP_DIR, 'holidays.parquet')
            df_hol.to_parquet(path_hol_silver, index=False)
            upload_file("silver", "holidays.parquet", path_hol_silver)
        except: pass

    # GOLD
    print("🏆 [GOLD] Aggregating...")
    if not df_loc.empty:
        df_gold_loc = df_loc.groupby(['kategori', 'nama_tempat', 'lat', 'lon']).size().reset_index(name='score').sort_values('score', ascending=False).head(300)
        path_gold_loc = os.path.join(TEMP_DIR, 'gold_locations.parquet')
        df_gold_loc.to_parquet(path_gold_loc, index=False)
        upload_file("gold", "gold_locations.parquet", path_gold_loc)
    else:
        df_gold_loc = pd.DataFrame(columns=['kategori', 'nama_tempat', 'lat', 'lon', 'score'])

    if not df_silver.empty:
        df_feat = (df_silver.groupby('archetype').size().reset_index(name='jumlah').sort_values('jumlah', ascending=False))
        path_gold_feat = os.path.join(TEMP_DIR, 'gold_features.parquet')
        df_feat.to_parquet(path_gold_feat, index=False)
        upload_file("gold", "gold_features.parquet", path_gold_feat)
    else:
        df_feat = pd.DataFrame(columns=['archetype', 'jumlah'])

    all_archs = ['Active', 'Creative', 'Healing', 'Intellectual', 'Religius', 'Social Butterfly', 'Sporty', 'Techie']
    existing_archs = df_feat['archetype'].tolist() if not df_feat.empty else []
    missing_archs = [a for a in all_archs if a not in existing_archs]

    print(f"💾 [SQL] Building Data Lakehouse Table (In-Memory)...")
    
    con = duckdb.connect(":memory:")
    
    con.execute("CREATE TABLE gold_features AS SELECT * FROM df_feat")
    con.execute("CREATE TABLE gold_locations AS SELECT * FROM df_gold_loc")
    
    if os.path.exists(os.path.join(TEMP_DIR, 'rules_data.parquet')):
        con.execute(f"CREATE TABLE gold_rules AS SELECT * FROM '{os.path.join(TEMP_DIR, 'rules_data.parquet')}'")
    
    con.execute(f"CREATE TABLE context_weather AS SELECT * FROM '{os.path.join(TEMP_DIR, 'context_weather.parquet')}'")
    
    if os.path.exists(os.path.join(TEMP_DIR, 'holidays.parquet')):
        con.execute(f"CREATE TABLE gold_holidays AS SELECT * FROM '{os.path.join(TEMP_DIR, 'holidays.parquet')}'")
    else:
        con.execute("CREATE TABLE gold_holidays (date DATE, name VARCHAR)")

    allowed_cats = get_allowed_categories_by_time(con)
    time_filter_sql = ""
    if allowed_cats:
        allowed_sql_str = ", ".join([f"'{x}'" for x in allowed_cats])
        time_filter_sql = f"AND t2.kategori IN ({allowed_sql_str})"
    else:
        time_filter_sql = "AND 1=0" 

    cuaca_main = w_data['main'] 
    indoor_cats = "'mall', 'cafe', 'library', 'museum', 'book_store', 'restaurant', 'fast_food', 'food_court', 'shop', 'electronics', 'clothes', 'gym', 'mosque', 'place_of_worship'"

    missing_sql_list = ", ".join([f"'{x}'" for x in missing_archs])
    
    query = f"""
        CREATE TABLE final_recs AS
        WITH Combined AS (
            -- LOGIC A: PERSONALIZED
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

            -- LOGIC B: FALLBACK
            SELECT 
                m.arch_name as archetype,
                t2.nama_tempat, t2.lat, t2.lon, t2.kategori, t2.score,
                'Global Top (Fallback)' as metode
            FROM (SELECT unnest([{missing_sql_list}]) as arch_name) m
            CROSS JOIN (
                SELECT * FROM gold_locations 
                ORDER BY score DESC 
                LIMIT 20 
            ) t2
            WHERE 1=1
        ),
        Finalized AS (
            SELECT *,
                CASE 
                    WHEN '{cuaca_main}' LIKE '%Rain%' AND kategori NOT IN ({indoor_cats}) 
                    THEN 'Strategi : Cuaca hujan. Bawa payung atau cari opsi indoor.'
                    ELSE 'Strategi : Cuaca mendukung. Segera meluncur!'
                END as pesan_strategi,
                
                CASE 
                    WHEN '{cuaca_main}' LIKE '%Rain%' AND kategori NOT IN ({indoor_cats}) 
                    THEN '#9d174d' ELSE '#f9a8d4' 
                END as warna_border,

                ROW_NUMBER() OVER (PARTITION BY archetype ORDER BY score DESC, random()) as rank_urutan
            FROM Combined
            WHERE archetype IS NOT NULL
        )
        SELECT * FROM Finalized WHERE rank_urutan <= 10 AND archetype != 'IGNORE_ME'
    """
    
    if not df_gold_loc.empty:
        # HACK 
        if not missing_archs:
             query = query.replace(f"unnest([{missing_sql_list}])", "unnest(['IGNORE_ME'])")

        con.execute(query)
        
        path_final = os.path.join(TEMP_DIR, 'recommendations.parquet')
        con.execute(f"COPY final_recs TO '{path_final}' (FORMAT PARQUET)")
        
        upload_file("gold", "recommendations.parquet", path_final)
        
        count = con.execute("SELECT COUNT(*) FROM final_recs").fetchone()[0]
        print(f"✅ ELT SUCCESS! {count} rekomendasi tersimpan di MinIO (Lakehouse Format).")
    else:
        print("❌ Data Lokasi Kosong. Pipeline finish without result.")

    con.close()

if __name__ == "__main__":
    run_elt_pipeline()