## 🚀 Quick Start (Installation Guide)

### 1. Clone the Repository

```bash
git clone https://github.com/ApiwitJoey/DSDE_SING_WATER_2025.git
cd DSDE_SING_WATER_2025
```

### 2. Download & Extract Required Data
ดาวน์โหลดไฟล์ข้อมูลจาก Google Drive: https://drive.google.com/file/d/1CYyVEVQ5HUcjUEIKS61rtwLBpqPk-dEI/view?usp=sharing


### 3. Create .env File

```bash
echo "AIRFLOW_UID=50000" > .env
```

### 4. Start with Docker

```bash
docker compose up -d --build
```

### 5. Access the Services

🧩 Airflow: http://localhost:8080
📊 Visualization App: http://localhost:8501