import time
import os
from elt_pipeline import run_elt_pipeline


def job_runner():
    print("⏰ [SCHEDULER] Sistem Penjadwalan Aktif (MinIO Lakehouse Mode)")
    
    while True:
        try:
            print("\n🚀 [JOB START] Memulai Siklus ELT Pipeline...")
            
            run_elt_pipeline()
            
            print("✅ [JOB DONE] Siklus Selesai. Data di MinIO sudah terupdate.")
            print("💤 Istirahat 30 Menit...")
            
            # Tunggu 30 Menit (1800 detik)
            time.sleep(1800)
            
        except KeyboardInterrupt:
            print("🛑 Scheduler Dihentikan Manual.")
            break
        except Exception as e:
            print(f"❌ [CRITICAL ERROR] Scheduler Gagal: {e}")
            time.sleep(60)

if __name__ == "__main__":
    if not os.environ.get("MINIO_ENDPOINT"):
        print("⚠️ Warning: Env MINIO_ENDPOINT tidak terdeteksi (Mungkin aman jika pakai default code)")
        
    job_runner()