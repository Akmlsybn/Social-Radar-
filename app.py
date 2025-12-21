import streamlit as st
import pandas as pd
import requests
import pytz
import os
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
# 2. LOAD GOLD LAYER (SINGLE SOURCE OF TRUTH)
# ==========================================
GOLD_FEATURES = "datalake/gold/gold_features.parquet"
GOLD_LOCATIONS = "datalake/gold/gold_locations.parquet"

if not os.path.exists(GOLD_FEATURES) or not os.path.exists(GOLD_LOCATIONS):
    st.error("Gold Layer belum tersedia. Jalankan elt_pipeline.py terlebih dahulu.")
    st.stop()

df_features = pd.read_parquet(GOLD_FEATURES)
df_locations = pd.read_parquet(GOLD_LOCATIONS)

# ==========================================
# 3. FUNGSI PENDUKUNG
# ==========================================
def get_cuaca():
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={KOTA}&appid={API_KEY_CUACA}&units=metric&lang=id"
        d = requests.get(url, timeout=3).json()
        return d['weather'][0]['main'], d['weather'][0]['description'], d['main']['temp']
    except:
        return "Unknown", "Offline", 30

def get_time_context():
    tz = pytz.timezone("Asia/Makassar")
    now = datetime.now(tz)
    hour = now.hour

    if 6 <= hour < 10:
        return "Pagi Aktif"
    elif 10 <= hour < 15:
        return "Siang Santai"
    elif 15 <= hour < 18:
        return "Sore Sosial"
    else:
        return "Malam Nongkrong"

def cari_target(selected_arch):
    row_feat = df_features[df_features['archetype'] == selected_arch]
    skor = int(row_feat.iloc[0]['jumlah']) if not row_feat.empty else 0

    kategori_map = {
    'Religius': [
        'place_of_worship',
        'mosque',
        'religious_school'
    ],
    'Intellectual': [
        'university',
        'college',
        'school',
        'library',
        'books'
    ],
    'Creative': [
        'arts_centre',
        'gallery',
        'cafe'
    ],
    'Social': [
        'cafe',
        'restaurant',
        'mall',
        'community_centre'
    ],
    'Sporty': [
        'gym',
        'fitness_centre',
        'park',
        'stadium'
    ],
    'Techie': [
        'electronics',
        'computer',
        'coworking'
    ],
    'Active': [
        'park',
        'gym',
        'fitness_centre',
        'outdoor'
    ],
    'General': df_locations['kategori'].unique().tolist()
}

    allowed_kat = kategori_map.get(selected_arch, [])

    df_loc_filt = df_locations[df_locations['kategori'].isin(allowed_kat)]

# fallback TERKONTROL (masih masuk akal secara akademik)
    if df_loc_filt.empty:
        df_loc_filt = df_locations[df_locations['kategori'].str.contains(
        selected_arch.lower(), case=False, na=False
    )]

    if df_loc_filt.empty:
        return None

    # 🎯 AMBIL YANG PALING RELEVAN
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

    opsi = df_features['archetype'].tolist()
    selected_arch = st.selectbox("Pilih Archetype:", opsi)

    st.markdown("---")
    btn_scan = st.button("📡 SCAN TARGET", use_container_width=True, type="primary")

# ==========================================
# 5. DASHBOARD
# ==========================================
st.title("📡 Social Radar: Banjarmasin Intelligence")
st.markdown("Sistem pendukung keputusan berbasis **Data Lakehouse (Medallion Architecture)**")
st.divider()

cuaca_main, cuaca_desc, suhu = get_cuaca()
fase_waktu = get_time_context()

if not btn_scan:
    c1, c2, c3 = st.columns(3)
    c1.metric("🌤️ Cuaca", f"{suhu}°C", cuaca_desc.title())
    c2.metric("🕒 Fase Waktu", fase_waktu)
    c3.metric("📍 Kota", KOTA)
    st.info("👈 Pilih archetype lalu klik SCAN TARGET.")
else:
    # 1️⃣ Ambil hasil
    res = cari_target(selected_arch)

    # 2️⃣ WAJIB cek None DI SINI
    if res is None:
        st.error("❌ Tidak ditemukan lokasi yang sesuai dengan archetype ini.")
        st.stop()

    # 3️⃣ Baru logic preskriptif
    if "Rain" in cuaca_main:
        strategi = "Hindari area terbuka, pilih lokasi indoor."
    elif "Clear" in cuaca_main:
        strategi = "Cuaca cerah, cocok untuk aktivitas sosial."
    else:
        strategi = "Cuaca relatif aman."

    # 4️⃣ Tampilkan hasil
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Target", res["Profil"])
    c2.metric("Akurasi", f"{res['Skor']} poin")
    c3.metric("Cuaca", cuaca_main)
    c4.metric("Waktu", fase_waktu)

    st.markdown(f"""
    <div class="rec-card">
        <h2>REKOMENDASI: {res["Lokasi"].upper()}</h2>
        <p><strong>Strategi:</strong> {strategi}</p>
        <p><strong>Alasan:</strong> Sesuai archetype <b>{selected_arch}</b> dan konteks waktu/cuaca.</p>
    </div>
    """, unsafe_allow_html=True)

    st.subheader("📍 Lokasi Rekomendasi")
    st.map(pd.DataFrame({
        "lat": [res["Lat"]],
        "lon": [res["Lon"]]
    }))

    st.link_button(
        "🚀 Buka di Google Maps",
        f"https://www.google.com/maps?q={res['Lat']},{res['Lon']}",
        use_container_width=True
    )
