import sqlite3
import json
import os

def init_database():
    print("Inisialisasi Database SQL (SQLite)...")
    
    db_file = "holidays.db"
    
    if os.path.exists(db_file):
        os.remove(db_file)
        
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS holidays (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            name TEXT NOT NULL
        )
    ''')
    
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
    
    count = 0
    for item in data_liburan:
        cursor.execute("INSERT INTO holidays (date, name) VALUES (?, ?)", (item['date'], item['name']))
        count += 1
        
    conn.commit()
    conn.close()
    
    print(f"Sukses! Database '{db_file}' berhasil dibuat dengan {count} data.")

if __name__ == "__main__":
    init_database()