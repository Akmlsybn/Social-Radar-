import duckdb
import os

# Pastikan path ini sesuai
db_path = os.path.join('datalake', 'gold', 'social_radar_olap.duckdb')

try:
    con = duckdb.connect(db_path, read_only=True)
    
    print(f"Membuka database di: {db_path}")
    
    # Ambil info kolom
    columns_info = con.execute("PRAGMA table_info('gold_daily_recommendations')").fetchall()
    
    # List semua nama kolom
    col_names = [col[1] for col in columns_info]
    
    print("\n📋 DAFTAR KOLOM YANG DITEMUKAN:")
    print(col_names)
    
    print("\n🕵️‍♀️ HASIL DIAGNOSA:")
    if 'warna_border' in col_names:
        print("✅ SUKSES! Kolom 'warna_border' DITEMUKAN.")
        print("   Silakan jalankan app.py sekarang.")
    else:
        print("❌ GAGAL! Kolom 'warna_border' TIDAK ADA.")
        print("   Cek apakah kode elt_pipeline.py sudah di-SAVE?")

except Exception as e:
    print(f"Error: {e}")