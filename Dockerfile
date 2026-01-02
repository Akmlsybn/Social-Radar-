# Gunakan image Python yang ringan
FROM python:3.9-slim

# Set folder kerja di dalam container
WORKDIR /app

# Install dependencies sistem dasar
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy file requirements dan install library
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy seluruh file proyek ke dalam container
COPY . .

# Buka port untuk Streamlit
EXPOSE 8501

# Command default (ini hanya fallback, karena akan di-override oleh docker-compose)
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]