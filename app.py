import streamlit as st
import pandas as pd
import requests
import pytz
import os
import duckdb
from datetime import datetime
from dotenv import load_dotenv

# ==========================================
# 1. KONFIGURASI DASAR
# ==========================================
st.set_page_config(
    page_title="Social Radar Banjarmasin",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
.rec-card {
    padding: 20px;
    border-radius: 10px;
    border-left: 6px solid #000000;
    box-shadow: 2px 2px 10px rgba(0,0,0,0.1);
    margin-bottom: 20px;
}
</style>
""", unsafe_allow_html=True)

load_dotenv()
API_KEY_CUACA = os.getenv("API_KEY_CUACA")

if not API_KEY_CUACA:
    st.error("API Key Cuaca belum disetting (.env)")
    st.stop()

KOTA = "Banjarmasin"

# ==========================================
# 2. KONEKSI KE DATA WAREHOUSE (DUCKDB)
# ==========================================
# Pastikan file ini ada (hasil run elt_pipeline.py)
DB_PATH = "social_radar_olap.duckdb"

if not os.path.exists(DB_PATH):
    st.error("⚠️ Database SQL tidak ditemukan. Harap jalankan 'elt_pipeline.py' terlebih dahulu!")
    st.stop()

# Fungsi helper untuk menjalankan Query SQL
def query_db(query):
    # read_only=True agar aman saat diakses
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(query).df()
    con.close()
    return df

# Load Opsi Archetype untuk Sidebar
try:
    # Kita ambil daftar archetype yang tersedia
    df_features = query_db("SELECT * FROM features ORDER BY jumlah DESC")
    opsi_archetype = df_features['archetype'].tolist()
    # Tambahkan opsi manual "General" jika belum ada
    if "General" not in opsi_archetype:
        opsi_archetype.append("General")
except Exception as e:
    st.error(f"Gagal koneksi database: {e}")
    st.stop()

# ==========================================
# 3. FUNGSI LOGIKA UTAMA
# ==========================================
def get_cuaca():
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={KOTA}&appid={API_KEY_CUACA}&units=metric&lang=id"
        d = requests.get(url, timeout=3).json()
        return d['weather'][0]['main'], d['weather'][0]['description'], d['main']['temp']
    except:
        return "Unknown", "Offline", 30

def get_time_context():
    try:
        # 1. Setup Waktu (WITA)
        tz = pytz.timezone("Asia/Makassar")
        now = datetime.now(tz)
        current_hour = now.hour         # Format Int (misal: 14)
        current_day_int = now.weekday() # Format Int 0-6 (0=Senin)

        # 2. Mapping Hari Python ke CSV
        days_map = {
            0: 'Senin', 1: 'Selasa', 2: 'Rabu', 3: 'Kamis',
            4: 'Jumat', 5: 'Sabtu', 6: 'Minggu'
        }
        current_day_str = days_map.get(current_day_int, 'Senin')

        # 3. Query Cerdas ke DuckDB (Cek Tabel Rules)
        query = f"""
            SELECT phase_name 
            FROM rules 
            WHERE day_category = '{current_day_str}'
              AND {current_hour} >= start_hour 
              AND {current_hour} < end_hour
        """
        
        df_rule = query_db(query)

        if not df_rule.empty:
            phase = df_rule.iloc[0]['phase_name']
            return f"{current_day_str} - {phase}"
        else:
            return f"{current_day_str} (Santai)"

    except Exception as e:
        return "Mode Offline"

def cari_target(selected_arch):
    # 1. Ambil Skor Archetype (jika ada)
    if selected_arch == "General":
        skor = 0
    else:
        df_skor = query_db(f"SELECT jumlah FROM features WHERE archetype = '{selected_arch}'")
        skor = int(df_skor.iloc[0]['jumlah']) if not df_skor.empty else 0

    # 2. Mapping Kategori ke Tempat
    kategori_map = {
        'Religius': ['place_of_worship', 'mosque', 'religious_school'],
        'Intellectual': ['university', 'college', 'school', 'library', 'books'],
        'Creative': ['arts_centre', 'gallery', 'cafe'],
        'Social': ['cafe', 'restaurant', 'mall', 'community_centre'],
        'Sporty': ['gym', 'fitness_centre', 'park', 'stadium'],
        'Techie': ['electronics', 'computer', 'coworking'],
        'Active': ['park', 'gym', 'fitness_centre', 'outdoor'],
        'General': [] # Kosongkan, nanti dihandle SQL khusus
    }

    allowed_kat = kategori_map.get(selected_arch, [])

    # 3. FILTER LOKASI MENGGUNAKAN SQL
    if selected_arch == 'General':
        # Ambil Top 100 Lokasi apapun kategorinya
        sql_query = "SELECT * FROM locations ORDER BY score DESC LIMIT 100"
    
    elif not allowed_kat:
        # Fallback: Cari nama kategori mirip string
        sql_query = f"""
            SELECT * FROM locations 
            WHERE kategori ILIKE '%{selected_arch}%'
            ORDER BY score DESC
        """
    else:
        # Filter IN (...)
        kat_tuple = str(tuple(allowed_kat)).replace(",)", ")")
        sql_query = f"""
            SELECT * FROM locations 
            WHERE kategori IN {kat_tuple}
            ORDER BY score DESC
        """

    df_loc_filt = query_db(sql_query)

    if df_loc_filt.empty:
        return None

    # 🎯 AMBIL 1 SAMPLE (Weighted Random)
    # Lokasi dengan skor tinggi punya peluang lebih besar muncul
    loc = df_loc_filt.sample(
        n=1,
        weights=df_loc_filt['score'],
        random_state=None
    ).iloc[0]

    return {
        "Profil": f"Tipe {selected_arch}",
        "Skor": skor,
        "Lokasi": loc['nama_tempat'],
        "Lat": loc['lat'],
        "Lon": loc['lon']
    }

# ==========================================
# 4. SIDEBAR
# ==========================================
with st.sidebar:
    st.title("🎛️ Control Panel")
    st.markdown("---")

    # Dropdown mengambil data dari DuckDB
    selected_arch = st.selectbox("Pilih Archetype:", opsi_archetype)

    st.markdown("---")
    btn_scan = st.button("📡 SCAN TARGET", use_container_width=True, type="primary")

# ==========================================
# 5. DASHBOARD UTAMA
# ==========================================
st.title("📡 Social Radar: Banjarmasin Intelligence")
st.markdown("Sistem pendukung keputusan berbasis **Data Lakehouse (DuckDB SQL)**")
st.divider()

# Ambil Data Real-time
cuaca_main, cuaca_desc, suhu = get_cuaca()
fase_waktu = get_time_context()

if not btn_scan:
    c1, c2, c3 = st.columns(3)
    c1.metric("🌤️ Cuaca", f"{suhu}°C", cuaca_desc.title())
    c2.metric("🕒 Fase Waktu", fase_waktu) # <-- INI AKAN BERUBAH SESUAI JAM & HARI
    c3.metric("📍 Kota", KOTA)
    st.info("👈 Pilih archetype lalu klik SCAN TARGET.")
else:
    # 1️⃣ Ambil hasil rekomendasi
    res = cari_target(selected_arch)

    # 2️⃣ Cek Validasi
    if res is None:
        st.error("❌ Tidak ditemukan lokasi yang sesuai dengan kriteria ini.")
        st.stop()

    # 3️⃣ Logic Prescriptive (Strategi)
    if "Rain" in cuaca_main:
        strategi = "Hindari area terbuka, pilih lokasi indoor."
    elif "Clear" in cuaca_main or "Clouds" in cuaca_main:
        strategi = "Cuaca mendukung untuk aktivitas sosial."
    else:
        strategi = "Cuaca relatif aman."

    # 4️⃣ Tampilkan Hasil
    c1, c2, c3,= st.columns(3)
    c1.metric("Target", res["Profil"])
    c2.metric("Cuaca", cuaca_main)
    c3.metric("Waktu", fase_waktu)

    st.markdown(f"""
    <div class="rec-card">
        <h2>REKOMENDASI: {res["Lokasi"].upper()}</h2>
        <p><strong>Strategi:</strong> {strategi}</p>
        <p><strong>Alasan:</strong> Sesuai archetype <b>{selected_arch}</b> dan fase waktu <b>{fase_waktu}</b>.</p>
    </div>
    """, unsafe_allow_html=True)

    st.subheader("📍 Peta Lokasi")
    st.map(pd.DataFrame({
        "lat": [res["Lat"]],
        "lon": [res["Lon"]]
    }))

    st.link_button(
        "🚀 Buka di Google Maps",
        f"https://www.google.com/maps?q={res['Lat']},{res['Lon']}",
        use_container_width=True
    )