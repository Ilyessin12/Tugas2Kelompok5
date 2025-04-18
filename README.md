# Stock Market Data Analysis Project

## Introductory
Project ini ditujukan untuk memenuhi Tugas 2 dari Mata Kuliah Big Data dengan Anggota kelompok sebagai berikut:
- Varrell Rizky Irvanni Mahkota (2306245)   
- Muhammad Hafidh Fadhilah (2305672)
- Muhammad Bintang Eighista Dwiputra (2304137)
- Muhammad Ichsan Khairullah (2306924)
- Lyan Nazhabil Dzuquwwa (2308428)

## Description
Project ini adalah kelanjutan dari Tugas 1 yang berupa pipeline scraping dan ingestion data keuangan. Pada Tugas 2 ini, fokus utama adalah pada transformasi data dan visualisasi untuk memudahkan analisis terhadap data yang telah dikumpulkan sebelumnya. Project terdiri dari dua komponen utama:

1. **Transformasi Data Laporan Keuangan**:
   - Menggunakan PySpark untuk memproses dan mentransformasi data laporan keuangan dari MongoDB
   - Menggabungkan dan merestrukturisasi data untuk memudahkan analisis 
   - Menyimpan hasil transformasi ke dalam koleksi MongoDB baru untuk digunakan pada tahap visualisasi

2. **Visualisasi Data Saham**:
   - Web application untuk menampilkan data historis saham emiten di IDX
   - Antarmuka interaktif untuk memilih saham, periode waktu, dan jenis grafik
   - Visualisasi menggunakan Plotly.js dengan berbagai jenis chart (Candlestick, OHLC, Line Chart)

## Komponen Transformasi Data
- Menggunakan PySpark untuk memproses data dalam skala besar
- Memformat dan menyeleksi kolom-kolom finansial penting seperti:
  - SalesAndRevenue
  - GrossProfit
  - ProfitFromOperation
  - ProfitLoss
  - CashAndCashEquivalents
  - Assets
  - ShortTermBankLoans dan LongTermBankLoans
  - Equity dan cash flow metrics
- Data hasil transformasi disimpan kembali ke MongoDB dalam format yang terstruktur untuk query yang lebih efisien

## Komponen Visualisasi 
- **Frontend**: Aplikasi web interaktif dengan fitur:
  - Pemilihan emiten dari daftar lengkap saham IDX
  - Opsi periode waktu: Daily, Weekly, Monthly, Quarterly, Yearly, 1Y, 3Y, 5Y, atau All Data
  - Jenis chart: Candlestick, OHLC, dan Line chart dengan volume
  - Error handling dan loading indicators
  - Responsive layout

- **Backend API**: 
  - REST API yang mengambil data dari MongoDB
  - Endpoint `/api/stock/:symbol` untuk mengakses data historis saham
  - Parameter query untuk memfilter berdasarkan periode waktu

## Teknologi yang Digunakan
- **Transformasi Data**:
  - PySpark: Untuk pemrosesan data skala besar
  - MongoDB Spark Connector: Untuk integrasi antara Spark dan MongoDB
  - Python: Sebagai bahasa pemrograman utama

- **Visualisasi**:
  - HTML/CSS/JavaScript: Untuk antarmuka web
  - Plotly.js: Untuk pembuatan chart interaktif
  - Flask (backend): Untuk API yang menyediakan data

- **Database**:
  - MongoDB Atlas: Sebagai penyimpanan utama untuk data mentah dan data yang telah ditransformasi

## Setup dan Penggunaan

### Prasyarat
- Python 3.8+ 
- MongoDB Atlas account atau MongoDB server lokal
- Java 8+ (untuk PySpark)

### 1. Konfigurasi Environment
Buat file .env di root folder project:
```
MONGODB_CONNECTION_STRING=your_mongodb_connection_string
MONGODB_DATABASE_NAME=your_database_name
COLLECTION_YFINANCE_DATA=yfinance_data
COLLECTION_FINANCIAL_REPORTS=financial_reports
```

### 2. Instalasi Dependencies
```bash
pip install pyspark pymongo python-dotenv plotly pandas jupyter
```

Untuk JAR files yang dibutuhkan PySpark:
```bash
# Download JAR files untuk MongoDB Spark Connector
wget https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/3.0.1/mongo-spark-connector_2.12-3.0.1.jar
wget https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/3.12.10/mongo-java-driver-3.12.10.jar
```

### 3. Menjalankan Transformasi Data
1. Buka notebook transformasi_laporan.ipynb di Jupyter
2. Sesuaikan variabel environment jika diperlukan
3. Jalankan semua cell untuk memproses dan mentransformasi data laporan keuangan
4. Verifikasi data hasil transformasi di MongoDB

### 4. Menjalankan Visualisasi
1. Jalankan API backend:
```bash
cd Plotting_Data/API
pip install flask flask-cors
python app.py
```

2. Buka file index.html di browser atau jalankan web server sederhana:
```bash
cd Plotting_Data/Website
python -m http.server 8000
```
3. Akses web app melalui `http://localhost:8000`

## Kontribusi
- **Transformasi Data**: [Anggota 1, Anggota 2]
- **Visualisasi Data**: [Anggota 3, Anggota 4]
- **Integrasi dan Dokumentasi**: [Anggota 5]

## Catatan
- Aplikasi memerlukan koneksi internet untuk memuat library Plotly dari CDN
- Data saham yang ditampilkan adalah data historis, bukan real-time
- Pastikan MongoDB telah diisi dengan data dari hasil scraping pada Tugas 1