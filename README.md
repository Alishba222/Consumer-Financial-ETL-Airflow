# 📊 Consumer Financial Complaints ETL Pipeline (Apache Airflow)

![Python](https://img.shields.io/badge/Python-3.12-blue?style=for-the-badge&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.x-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?style=for-the-badge&logo=mysql&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Analysis-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Google Sheets](https://img.shields.io/badge/Google%20Sheets-API-34A853?style=for-the-badge&logo=google-sheets&logoColor=white)

An end-to-end automated **Data Engineering ETL Pipeline** built using **Apache Airflow** and **Docker**. This pipeline extracts real-world financial complaint records from the public **CFPB (Consumer Financial Protection Bureau) API**, stages raw data in a local **MySQL** database, performs analytical transformations using **Pandas**, validates file availability via Airflow sensors, exports clean datasets to **Google Sheets**, and dispatches automated **SMTP email notifications**.

---

## 🏗️ Pipeline Architecture & Workflow

```mermaid
graph LR
    A[🌐 CFPB Public API] -->|extract.py| B(🐬 MySQL Staging)
    B -->|transform.py| C[📄 Transformed CSV]
    C -->|FileSensor| D{✅ File Verified?}
    D -->|load.py| E[📊 Live Google Sheet]
    E -->|EmailOperator| F[📧 SMTP Email Alert]

🔄 Sequential DAG Tasks (consumer_financial_etl)
📥 extract_data_from_source (PythonOperator)

Fetches US state codes dynamically and queries the CFPB API for a 30-day rolling window across all states.

Consolidates API JSON responses into a structured Pandas DataFrame.

🐬 dump_data_to_mysql (PythonOperator)

Connects to the host MySQL database (cde_assignment) via SQLAlchemy (host.docker.internal).

Loads raw extracted records into the raw_consumer_complaints staging table.

⚙️ transform_data (PythonOperator)

Reads raw data from MySQL, formats timestamps into Month Year periods, and handles missing categorical values ("N/A").

Aggregates unique complaint counts (complaint_id) grouped by product, issue, company, state, and response metrics.

Exports the cleaned dataset to the mounted volume (DATA/consumer_complaints_transformed.csv).

🔍 check_consumer_financial_csv (FileSensor)

Acts as a quality gate, poking the DATA/ directory every 10 seconds to verify the physical existence of the CSV before downstream execution.

☁️ dump_googlesheet (PythonOperator)

Authenticates with Google Drive & Sheets APIs using a Service Account (credentials.json).

Clears existing worksheet rows and uploads the newly transformed dataset to a live Google Sheet.

📧 send_googlesheet_url_via_email (EmailOperator)

Triggers an automated HTML email notification containing the clickable URL of the updated Google Sheet upon successful completion.

📂 Project Structure
Plaintext
airflow/
├── dags/
│   ├── etl_dag.py          # Main Airflow DAG orchestration & task dependencies
│   ├── extract.py          # CFPB API extraction logic
│   ├── transform.py        # Pandas data cleaning, aggregation & CSV export
│   └── load.py             # MySQL staging & Google Sheets API upload logic
├── DATA/
│   └── consumer_complaints_transformed.csv  # Generated output dataset (gitignored)
├── config/                 # Airflow configuration files
├── docker-compose.yaml     # Multi-container Airflow setup (Webserver, Scheduler, Worker)
├── .gitignore              # Excludes credentials, logs, and local data files
└── README.md               # Project documentation
🛠️ Tech Stack
Orchestration: Apache Airflow (PythonOperator, FileSensor, EmailOperator)

Containerization: Docker & Docker Compose

Programming & Data Processing: Python 3.12, Pandas, Requests

Database & ORM: MySQL, SQLAlchemy, PyMySQL

Cloud & Notifications: Google Sheets API (gspread, google-auth), SMTP

🚀 Getting Started
1️⃣ Prerequisites
Docker Desktop installed and running

Local MySQL Server with a database named cde_assignment

A Google Cloud Service Account JSON key (credentials.json) with Google Sheets & Drive APIs enabled

2️⃣ Setup & Configuration
Clone the repository:

Bash
git clone [https://github.com/Alishba222/consumer-financial-etl-airflow.git](https://github.com/Alishba222/consumer-financial-etl-airflow.git)
cd consumer-financial-etl-airflow
Add Credentials:

Place your Google Service Account credentials.json inside the root directory (airflow/) and dags/ folder.

Share your target Google Sheet (CFPB_Complaints_Transformed) with the service account email (Editor access).

Configure Database & SMTP:

Update MySQL credentials in dags/load.py and dags/transform.py.

Configure Airflow SMTP connection (smtp_default) in docker-compose.yaml or the Airflow Admin UI.

3️⃣ Run the Pipeline
Start the Docker containers:

Bash
docker compose up -d
Access the Airflow Web UI at http://localhost:8085, unpause the consumer_financial_etl DAG, and trigger a run!

🔒 Security Note
Sensitive files including credentials.json, Airflow runtime logs/, and raw/transformed .csv datasets are excluded from version control via .gitignore.


---

### PowerShell se Direct `README.md` Create aur Push Karne Ke Liye:

VS Code terminal mein yeh commands run kar dein:

```powershell
git add README.md
git commit -m "Add polished README with badges and architecture diagram"
git push