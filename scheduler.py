import time
import duckdb
import requests
import os
import pytz
from datetime import datetime
from dotenv import load_dotenv
from elt_pipeline import run_elt_pipeline 

load_dotenv()
API_KEY_CUACA = os.getenv("API_KEY_CUACA")
KOTA = "Banjarmasin"

DB_PATH = "datalake/gold/social_radar_olap.duckdb"

def update_context_data():
    """
    Fungsi ini mengambil data cuaca dan menyimpannya ke tabel context_weather
    """
    print("☁️ Mengambil Data Cuaca Real-time...")
    
    main, desc, temp = "Unknown", "Offline", 0
    
    try:
        if not API_KEY_CUACA:
            print(" Warning: API_KEY_CUACA tidak ditemukan di .env")
        else:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={KOTA}&appid={API_KEY_CUACA}&units=metric&lang=id"
            resp = requests.get(url, timeout=10) 
            
            if resp.status_code == 200:
                data = resp.json()
                main = data['weather'][0]['main']
                desc = data['weather'][0]['description']
                temp = data['main']['temp']
                print(f"Dapat Data API: {main}, {desc}, {temp}C")
            else:
                print(f"Gagal Request API: {resp.status_code} - {resp.text}")
                
    except Exception as e:
        print(f"Error Koneksi API: {e}")

    try:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        
        con = duckdb.connect(DB_PATH)
        
        tz = pytz.timezone("Asia/Makassar")
        updated_at = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

        con.execute(f"""
            CREATE OR REPLACE TABLE context_weather AS 
            SELECT 
                '{main}' as main,
                '{desc}' as description,
                {temp} as temp,
                '{updated_at}' as updated_at
        """)
        con.close()
        print(f"Context Tersimpan di Database: {updated_at}")
        
    except Exception as e:
        print(f"Error Database Context: {e}")

def job_runner():
    print("SCHEDULER: SOCIAL RADAR SYSTEM")
    while True:
        try:
            print("\n [SCHEDULER] Memulai Siklus Baru...")
            
            run_elt_pipeline()
            
            update_context_data()
            
            print("Siklus Selesai. Menunggu 30 Menit...")
            
            time.sleep(1800) 
            
        except KeyboardInterrupt:
            print("Scheduler Dihentikan.")
            break
        except Exception as e:
            print(f"Critical Error di Scheduler: {e}")
            time.sleep(60)

if __name__ == "__main__":
    job_runner()