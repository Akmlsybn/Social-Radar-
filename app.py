import streamlit as st
import duckdb
import pandas as pd
import os
import time

st.set_page_config(
    page_title="Temu Loka - Social Radar Dashboard",
    layout="wide"
)

st.markdown("""
<style>
/* Import Font Elegan untuk Judul */
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@600;700&family=Lato:wght@400;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Lato', sans-serif; /* Font body modern */
}

/* Judul Utama Dashboard */
h1 {
    font-family: 'Playfair Display', serif !important;
    color: #f9a8d4 !important; /* Warna Merah Hati Gelap */
}

/* Kartu Rekomendasi */
.rec-card {
    padding: 24px;
    border-radius: 20px; /* Sangat membulat (soft) */
    background-color: #fff1f2; /* Latar belakang pink sangat muda (Rose Water) */
    /* Shadow pink lembut */
    box-shadow: 0 10px 20px rgba(244, 63, 94, 0.15); 
    margin-bottom: 24px;
    border: 1px solid #fecdd3; /* Border pink muda */
    transition: transform 0.3s ease;
}

.rec-card:hover {
    transform: scale(1.02); /* Efek zoom sedikit saat disentuh */
}

.rec-card h3 {
    margin-top: 0;
    font-family: 'Playfair Display', serif; /* Font Romantis */
    color: #881337; /* Rose Red */
    font-size: 1.6rem;
}

.rec-card p {
    color: #4c0519; /* Coklat kemerahan */
}

/* Badge Kategori - Pink Elegan */
.badge-cat {
    background-color: #fbcfe8; /* Pink Pastel */
    color: #9d174d; /* Teks Merah Hati */
    padding: 6px 15px;
    border-radius: 50px; /* Bentuk Pill */
    font-weight: 600;
    font-size: 0.85em;
    border: 1px solid #f9a8d4;
    letter-spacing: 0.5px;
}

/* Badge Rekomendasi - Emas (Rose Gold) */
.badge-top {
    background-color: #fffbe0; /* Kuning Cream */
    color: #b45309; /* Warna Emas/Tembaga */
    padding: 6px 15px;
    border-radius: 50px;
    font-weight: 600;
    font-size: 0.85em;
    border: 1px solid #fcd34d;
    margin-left: 8px;
}

/* Garis Pemisah */
hr {
    border-top: 1px solid #fecdd3 !important;
}
            div[data-testid="stLinkButton"] > a {
    background-color: #be123c !important; /* Warna Rose Red (Merah Mawar) */
    color: white !important; /* Teks Putih */
    border: none !important;
    border-radius: 50px !important; /* Membulat seperti pil */
    font-weight: 700 !important;
    text-decoration: none !important;
    box-shadow: 0 4px 14px 0 rgba(190, 18, 60, 0.39) !important; /* Shadow pink */
    transition: all 0.2s ease-in-out !important;
    text-align: center !important;
    display: flex !important;
    justify-content: center !important;
    align-items: center !important;
    padding: 10px 20px !important;
}

/* Efek saat mouse diarahkan (Hover) */
div[data-testid="stLinkButton"] > a:hover {
    background-color: #9f1239 !important; /* Warna lebih gelap dikit */
    transform: translateY(-2px) !important; /* Tombol naik sedikit */
    box-shadow: 0 6px 20px rgba(190, 18, 60, 0.23) !important;
    color: #fff !important;
}

/* Efek saat diklik (Active) */
div[data-testid="stLinkButton"] > a:active {
    transform: translateY(1px) !important;
}
</style>
""", unsafe_allow_html=True)

DB_PATH = "datalake/gold/social_radar_olap.duckdb"
ALL_POSSIBLE_ARCHETYPES = [
    "Active", "Creative", "Healing", "Intellectual", 
    "Religius", "Social Butterfly", "Sporty", "Techie"
]

# 2. LOAD DATA
@st.cache_data(ttl=60)
def get_archetype_options():
    for attempt in range(3):
        try:
            if not os.path.exists(DB_PATH): return sorted(ALL_POSSIBLE_ARCHETYPES)
            con = duckdb.connect(DB_PATH, read_only=True)
            df = con.execute("SELECT DISTINCT archetype FROM gold_daily_recommendations ORDER BY archetype ASC").df()
            con.close()
            if df.empty: return sorted(ALL_POSSIBLE_ARCHETYPES)
            return df['archetype'].tolist()
        except RuntimeError: time.sleep(0.1)
        except Exception: break
    return sorted(ALL_POSSIBLE_ARCHETYPES)

def get_weather_context():
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        df = con.execute("SELECT * FROM context_weather LIMIT 1").df()
        con.close()
        if not df.empty:
            return df.iloc[0]['main'], df.iloc[0]['description'], df.iloc[0]['temp']
    except: pass
    return "Unknown", "Offline", 0

# 3. LOGIKA SESSION STATE
if 'selected_arch_state' not in st.session_state:
    st.session_state.selected_arch_state = "Sporty"

def update_selection():
    st.session_state.selected_arch_state = st.session_state.arch_selector

opsi_archetype = get_archetype_options()
cuaca_main, cuaca_desc, suhu = get_weather_context()

if st.session_state.selected_arch_state not in opsi_archetype:
    st.session_state.selected_arch_state = opsi_archetype[0]

# 4. SIDEBAR
with st.sidebar:
    st.header("Pusat Komando")
    st.write("### Pilih Tipe Wanita Mu Hari Ini")
    
    st.selectbox(
        "Archetype:", 
        options=opsi_archetype,
        key='arch_selector',
        index=opsi_archetype.index(st.session_state.selected_arch_state),
        on_change=update_selection
    )
    selected_arch = st.session_state.arch_selector
    
    st.markdown("---")
    st.info("**Status:** Saat ini kamu perlu mencari pasangan hidup‼️")

# 5. DASHBOARD UTAMA
st.title("Temu Loka Dashboard")
st.markdown("### Rekomendasi Tempat untuk Menemukan Pasangan di Kota Banjarmasin")
st.divider()

c1, c2, c3 = st.columns(3)
c1.metric("🌡️ Suhu Lokasi", f"{suhu}°C", cuaca_main)
c2.metric("🌤️ Cuaca Saat Ini", cuaca_desc.title())

if "Rain" in cuaca_main:
    c3.metric("💈 Pilih Area", "INDOOR MODE", "Waspada", delta_color="inverse")
else:
    c3.metric("💈 Pilih Area", "OUTDOOR MODE", "Aman", delta_color="normal")

try:
    con = duckdb.connect(DB_PATH, read_only=True)

    query = f"""
        SELECT *
        FROM gold_daily_recommendations 
        WHERE archetype = '{selected_arch}'
        ORDER BY random() 
        LIMIT 5
    """
    
    result = con.execute(query).df()
    con.close()
    
    if not result.empty:
        hero = result.iloc[0]

        st.markdown(f"""
        <div class="rec-card" style="border-left: 6px solid {hero['warna_border']};">
            <h3>💈 Pilihan Utama: {hero['nama_tempat']}</h3>
            <div style="margin-bottom: 15px;">
                <span class="badge-cat">📍 {hero['kategori']}</span>
                <span class="badge-top">📌 Rekomendasi Teratas</span>
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
            gmaps_url = f"https://www.google.com/maps?q={hero['lat']},{hero['lon']}"            
            st.link_button("Buka Maps", gmaps_url, use_container_width=True)

        #  2. LIST SECTION
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
        st.warning(f"Sedang memproses data untuk **{selected_arch}**...")
        st.caption("Scheduler sedang menyiapkan data terbaru.")

except Exception as e:
    st.error(f"Error Database: {e}")