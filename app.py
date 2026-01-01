import streamlit as st
import pandas as pd
import os
import time
from minio import Minio

# --- 1. KONFIGURASI DAN KONEKSI MINIO ---
st.set_page_config(
    page_title="Temu Loka - Social Radar Dashboard",
    layout="wide"
)

# Konfigurasi MinIO (Sesuai Docker Compose)
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "datalake"

# Path File Sementara di Container App
FILE_RECS = "/tmp/recommendations.parquet"
FILE_WEATHER = "/tmp/context_weather.parquet"

def sync_data_from_lake():
    """
    Fungsi ini bertugas mengambil data matang (Gold Layer) 
    dari Data Lake (MinIO) ke lokal container agar bisa dibaca Pandas.
    """
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )
        # Cek apakah bucket ada
        if not client.bucket_exists(BUCKET_NAME):
            return False, "Menunggu Pipeline..."
            
        # Download File Parquet
        client.fget_object(BUCKET_NAME, "gold/recommendations.parquet", FILE_RECS)
        client.fget_object(BUCKET_NAME, "gold/context_weather.parquet", FILE_WEATHER)
        return True, "Data Terupdate"
    except Exception as e:
        return False, f"Menunggu Koneksi... ({str(e)})"

# Sync data saat aplikasi dimuat ulang
db_status, db_msg = sync_data_from_lake()

# --- 2. STYLE UI (DIPERTAHANKAN 100% SESUAI PERMINTAAN) ---
st.markdown("""
<style>
/* Import Font Elegan untuk Judul */
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@600;700&family=Lato:wght@400;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Lato', sans-serif;
}

/* Judul Utama Dashboard */
h1 {
    font-family: 'Playfair Display', serif !important;
    color: #f9a8d4 !important; 
}

/* Kartu Rekomendasi */
.rec-card {
    padding: 24px;
    border-radius: 20px;
    background-color: #fff1f2;
    box-shadow: 0 10px 20px rgba(244, 63, 94, 0.15); 
    margin-bottom: 24px;
    border: 1px solid #fecdd3;
    transition: transform 0.3s ease;
}

.rec-card:hover {
    transform: scale(1.02);
}

.rec-card h3 {
    margin-top: 0;
    font-family: 'Playfair Display', serif;
    color: #881337;
    font-size: 1.6rem;
}

.rec-card p {
    color: #4c0519;
}

/* Badge Kategori */
.badge-cat {
    background-color: #fbcfe8;
    color: #9d174d;
    padding: 6px 15px;
    border-radius: 50px;
    font-weight: 600;
    font-size: 0.85em;
    border: 1px solid #f9a8d4;
    letter-spacing: 0.5px;
}

/* Badge Rekomendasi */
.badge-top {
    background-color: #fffbe0;
    color: #b45309;
    padding: 6px 15px;
    border-radius: 50px;
    font-weight: 600;
    font-size: 0.85em;
    border: 1px solid #fcd34d;
    margin-left: 8px;
}

hr { border-top: 1px solid #fecdd3 !important; }

div[data-testid="stLinkButton"] > a {
    background-color: #be123c !important;
    color: white !important;
    border: none !important;
    border-radius: 50px !important;
    font-weight: 700 !important;
    text-decoration: none !important;
    box-shadow: 0 4px 14px 0 rgba(190, 18, 60, 0.39) !important;
    transition: all 0.2s ease-in-out !important;
    text-align: center !important;
    display: flex !important;
    justify-content: center !important;
    align-items: center !important;
    padding: 10px 20px !important;
}

div[data-testid="stLinkButton"] > a:hover {
    background-color: #9f1239 !important;
    transform: translateY(-2px) !important;
    box-shadow: 0 6px 20px rgba(190, 18, 60, 0.23) !important;
    color: #fff !important;
}

div[data-testid="stLinkButton"] > a:active {
    transform: translateY(1px) !important;
}
</style>
""", unsafe_allow_html=True)

# --- 3. HELPER FUNCTIONS (BACA PARQUET) ---
ALL_POSSIBLE_ARCHETYPES = [
    "Active", "Creative", "Healing", "Intellectual", 
    "Religius", "Social Butterfly", "Sporty", "Techie"
]

def load_data_recs():
    """Membaca file Parquet Rekomendasi"""
    try:
        return pd.read_parquet(FILE_RECS)
    except:
        return pd.DataFrame()

def load_data_weather():
    """Membaca file Parquet Cuaca"""
    try:
        df = pd.read_parquet(FILE_WEATHER)
        return df.iloc[0]['main'], df.iloc[0]['description'], df.iloc[0]['temp']
    except:
        return "Unknown", "Offline", 0

# --- 4. LOGIKA DATA & STATE ---
# Load Dataframe ke Memory
df_recs = load_data_recs()
cuaca_main, cuaca_desc, suhu = load_data_weather()

# Ambil Opsi Archetype yang Tersedia di Data
if not df_recs.empty:
    available_archs = sorted(df_recs['archetype'].unique().tolist())
    # Hapus 'Global' dari dropdown pilihan user, biar 'Global' cuma jadi fallback
    if 'Global' in available_archs: available_archs.remove('Global')
    # Filter opsional: hanya tampilkan yang ada di list default
    opsi_archetype = [a for a in ALL_POSSIBLE_ARCHETYPES if a in available_archs]
    if not opsi_archetype: opsi_archetype = ALL_POSSIBLE_ARCHETYPES
else:
    opsi_archetype = sorted(ALL_POSSIBLE_ARCHETYPES)

# Session State
if 'selected_arch_state' not in st.session_state:
    st.session_state.selected_arch_state = "Sporty"

def update_selection():
    st.session_state.selected_arch_state = st.session_state.arch_selector

# Validasi pilihan
if st.session_state.selected_arch_state not in opsi_archetype and opsi_archetype:
    st.session_state.selected_arch_state = opsi_archetype[0]

# --- 5. SIDEBAR ---
with st.sidebar:
    st.header("Pusat Komando")
    
    # Indikator Status Danau Data
    if db_status:
        st.success(f"🟢 Lakehouse Connected")
    else:
        st.warning(f"🟡 {db_msg}")
        
    st.write("### Pilih Tipe Wanita Mu Hari Ini")
    
    if opsi_archetype:
        idx = 0
        if st.session_state.selected_arch_state in opsi_archetype:
            idx = opsi_archetype.index(st.session_state.selected_arch_state)
            
        st.selectbox(
            "Archetype:", 
            options=opsi_archetype,
            key='arch_selector',
            index=idx,
            on_change=update_selection
        )
    else:
        st.error("Data Archetype belum tersedia.")

    # Tombol Refresh Manual
    if st.button("🔄 Segarkan Data"):
        sync_data_from_lake()
        st.rerun()

    selected_arch = st.session_state.arch_selector if 'arch_selector' in st.session_state else "Sporty"
    st.markdown("---")
    st.info("**Status:** Saat ini kamu perlu mencari pasangan hidup‼️")

# --- 6. DASHBOARD UTAMA ---
st.title("Temu Loka Dashboard")
st.markdown("### Rekomendasi Tempat untuk Menemukan Pasangan di Kota Banjarmasin")
st.divider()

# Metric Cuaca
c1, c2, c3 = st.columns(3)
c1.metric("🌡️ Suhu Lokasi", f"{suhu}°C", cuaca_main)
c2.metric("🌤️ Cuaca Saat Ini", cuaca_desc.title())

if "Rain" in cuaca_main:
    c3.metric("💈 Pilih Area", "INDOOR MODE", "Waspada", delta_color="inverse")
else:
    c3.metric("💈 Pilih Area", "OUTDOOR MODE", "Aman", delta_color="normal")

# Menampilkan Data
try:
    if df_recs.empty:
        st.warning("⚠️ Data Kosong. Pipeline ELT sedang bekerja, silakan tunggu sebentar dan refresh.")
    else:
        # FILTERING DATA (PANDAS)
        # 1. Coba ambil data sesuai Archetype pilihan
        result = df_recs[df_recs['archetype'] == selected_arch]
        
        is_fallback = False
        # 2. Jika kosong, Fallback ke Global
        if result.empty:
            is_fallback = True
            result = df_recs[df_recs['archetype'] == 'Global']

        # Ambil sampel acak (shuffle) max 5
        if not result.empty:
            result = result.sample(frac=1).head(5) # Shuffle dan ambil 5
            
            hero = result.iloc[0]

            # Jika Fallback, kasih notifikasi halus
            if is_fallback:
                st.info(f"💡 Belum ada rekomendasi spesifik untuk **{selected_arch}**. Menampilkan **Rekomendasi Terpopuler** di Banjarmasin.")

            # KARTU UTAMA (HERO)
            st.markdown(f"""
            <div class="rec-card" style="border-left: 6px solid {hero['warna_border']};">
                <h3>💈 Pilihan Utama: {hero['nama_tempat']}</h3>
                <div style="margin-bottom: 15px;">
                    <span class="badge-cat">📍 {hero['kategori']}</span>
                    <span class="badge-top">{'📌 Rekomendasi Teratas' if not is_fallback else '⭐ Paling Populer'}</span>
                </div>
                <hr style="margin: 10px 0;">
                <p style="font-size: 1.05em; line-height: 1.5;">{hero['pesan_strategi']}</p>
            </div>
            """, unsafe_allow_html=True)
            
            c_map, c_btn = st.columns([3, 1])
            with c_map:
                st.map(pd.DataFrame({'lat': [hero['lat']], 'lon': [hero['lon']]}))
            with c_btn:
                st.write("") 
                st.write("") 
                # URL Format Sesuai Permintaan
                gmaps_url = f"https://www.google.com/maps?q={hero['lat']},{hero['lon']}"            
                st.link_button("Buka Maps", gmaps_url, use_container_width=True)

            # LIST OPSI LAINNYA
            alternatives = result.iloc[1:]
            
            if not alternatives.empty:
                st.markdown("---")
                st.subheader("💁‍♀️ Opsi Menarik Lainnya")
                st.caption("Kurang sreg dengan pilihan di atas? Coba cek tempat ini:")
                
                for index, row in alternatives.iterrows():
                    with st.expander(f"📍 {row['nama_tempat']} ({row['kategori']})"):
                        # Link Maps Kecil
                        alt_url = f"https://www.google.com/maps?q={row['lat']},{row['lon']}"
                        st.markdown(f"[📍 Buka di Google Maps]({alt_url})")

        else:
            st.error(f"Maaf, tidak ditemukan data apapun (termasuk Global). Sistem Data Lake mungkin sedang kosong.")

except Exception as e:
    st.error(f"Terjadi kesalahan pada tampilan: {e}")