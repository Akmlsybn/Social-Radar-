import sqlite3
import json
import os

def init_database():
    print("⚙️ Inisialisasi Database SQL (SQLite)...")
    
    # Nama file database yang akan dibuat
    db_file = "holidays.db"
    
    # Hapus file lama jika ada (biar fresh)
    if os.path.exists(db_file):
        os.remove(db_file)
        
    # 1. Koneksi ke SQLite
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # 2. Buat Tabel
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS holidays (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            name TEXT NOT NULL
        )
    ''')
    
    # 3. Data Liburan (Pindahkan dari holidays.json ke sini)
    # Anda bisa copy-paste isi holidays.json Anda ke variabel ini
    data_liburan = [
        {"date": "2024-01-01", "name": "Tahun Baru Masehi"},
        {"date": "2024-02-08", "name": "Isra Mi'raj"},
        {"date": "2024-02-10", "name": "Tahun Baru Imlek"},
        {"date": "2024-03-11", "name": "Hari Suci Nyepi"},
        {"date": "2024-03-29", "name": "Wafat Isa Al Masih"},
        {"date": "2024-04-10", "name": "Hari Raya Idul Fitri"},
        {"date": "2024-04-11", "name": "Cuti Bersama Idul Fitri"},
        {"date": "2024-05-01", "name": "Hari Buruh Internasional"},
        {"date": "2024-05-09", "name": "Kenaikan Isa Al Masih"},
        {"date": "2024-05-23", "name": "Hari Raya Waisak"},
        {"date": "2024-06-01", "name": "Hari Lahir Pancasila"},
        {"date": "2024-06-17", "name": "Hari Raya Idul Adha"},
        {"date": "2024-07-07", "name": "Tahun Baru Islam"},
        {"date": "2024-08-17", "name": "Hari Kemerdekaan RI"},
        {"date": "2024-09-16", "name": "Maulid Nabi Muhammad SAW"},
        {"date": "2024-12-25", "name": "Hari Raya Natal"}
    ]
    
    # 4. Insert Data ke Tabel
    count = 0
    for item in data_liburan:
        cursor.execute("INSERT INTO holidays (date, name) VALUES (?, ?)", (item['date'], item['name']))
        count += 1
        
    conn.commit()
    conn.close()
    
    print(f"✅ Sukses! Database '{db_file}' berhasil dibuat dengan {count} data.")

if __name__ == "__main__":
    init_database()