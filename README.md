# Rapid ETL Pipeline (Apache Airflow + PostgreSQL)

**Course:** DATA 430 – Data Engineering Practicum · **Project 1** (Individual Sprint)
**Author:** Ahmed · **Orchestration tool:** Option A – Apache Airflow

---

## 1. Project Overview

This project is a working proof-of-concept ETL (Extract, Transform, Load) pipeline. It takes synthetic customer, product, and order data stored in **three file formats (CSV, JSON, YAML)** and loads it into a **PostgreSQL** database, orchestrated by **Apache Airflow**. Every component runs inside **Docker containers** started with a single `docker compose up` command.

**What the pipeline does, end to end:**

1. **Generates** 3,000 synthetic records (1,000 customers, 1,000 products, 1,000 orders) with the Python Faker library and writes them to 9 files.
2. **Validates** that all 9 files exist and can be read.
3. **Sets up** the database tables (idempotent `CREATE TABLE IF NOT EXISTS`) and clears data from the previous run.
4. **Extracts** the CSV, JSON, and YAML files in parallel with pandas, checking that every required column is present.
5. **Loads** customers, then products, then orders into PostgreSQL using bulk inserts inside transactions (customers first, because orders reference them through a foreign key).
6. **Validates** the loaded data: row counts match, and every order points to a real customer.
7. **Completes** the run by archiving a copy of the files and writing a `SUCCESS` row to the `etl_audit` table.

If any step fails, Airflow retries it once. If it fails again, a `FAILED` row is written to `etl_audit`.

### Technology stack

| Component | Technology | Version |
|---|---|---|
| Orchestration | Apache Airflow (official Docker image) | 3.x (meets the 2.0+ requirement) |
| Database | PostgreSQL | 16 (meets the 12+ requirement) |
| Data processing | pandas, PyYAML, psycopg2 | Included in the Airflow image |
| Data generation | Faker | Latest |
| Containers | Docker Desktop + Docker Compose | Latest |
| Language | Python | 3.12 (data generation), 3.13 (Airflow image) |
| Version control | Git + GitHub | — |

---

## 2. Repository Structure

```
etl-project/
├── dags/
│   └── etl_pipeline.py        # Main Airflow DAG (10 tasks)
├── scripts/
│   └── generate_data.py       # Faker script: creates all 9 data files
├── sql/
│   └── schema.sql             # Table DDL + sample queries
├── data/                      # The 9 generated data files
│   ├── customers.csv / .json / .yaml
│   ├── products.csv  / .json / .yaml
│   └── orders.csv    / .json / .yaml
├── docs/
│   ├── technical_documentation.md
│   └── user_guide.md
├── config/  logs/  plugins/   # Airflow working folders
├── docker-compose.yaml        # Airflow + PostgreSQL services
├── requirements.txt           # Python dependencies for data generation
├── .env.example               # Template for environment variables
├── .gitignore                 # Excludes .env, logs, staging, archive
└── README.md
```

---

## 3. Prerequisites

- **Docker Desktop** (with WSL 2 on Windows), with at least 4 GB of memory available to Docker
- **Git**
- A web browser
- Python is optional on your computer. Data generation runs inside a Docker container.

---

## 4. Setup Instructions

> All commands below are for **Windows PowerShell** (or the VS Code terminal), run from the project folder. Mac/Linux commands are the same unless noted.

### Step 1: Clone the repository
```
git clone https://github.com/<Ahmed786110-a11y>/etl-project.git
cd etl-project
```

### Step 2: Create the `.env` file
Copy the template and fill in your own values:
```
Copy-Item .env.example .env
```
Open `.env` and set a database user, password, and Fernet key. To generate a Fernet key:
```
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```
`.env` is listed in `.gitignore` and is never committed.

### Step 3: Generate the data (9 files)
```
docker run --rm -v "${PWD}:/app" -w /app python:3.12-slim sh -c "pip install -r requirements.txt && python scripts/generate_data.py"
```
Expected output: `Done: 9 files created`.

### Step 4: Initialise Airflow (first time only)
```
docker compose up airflow-init
```
Wait for `airflow-init-1 exited with code 0`.

### Step 5: Start all services
```
docker compose up -d
```
Wait about 2 minutes, then check that all services are healthy:
```
docker compose ps
```

---

## 5. How to Run the Pipeline

1. Open **http://localhost:8080** and log in (`airflow` / `airflow`).
2. Find **`etl_pipeline`** in the DAG list and switch its toggle **on**.
3. Click **▶ Trigger** to run it manually. It also runs automatically **every day at 06:00 UTC**.
4. Open the **Graph** view and watch all 10 tasks turn green (about 30 seconds).

### Check the results
```
docker compose exec etl-postgres psql -U etl_user -d etl_db -c "SELECT * FROM etl_audit;"
```
A successful run shows `SUCCESS` with `3000` records processed. Press `q` if the output ends with `(END)`.

### Stop the services
```
docker compose down          # stop (keeps data)
docker compose down -v       # stop and delete all data
```

---

## 6. Pipeline Task Flow

```
file_validation >> database_setup >> [extract_csv, extract_json, extract_yaml]
  >> load_customers >> load_products >> load_orders >> data_validation >> completion
```

| Task | What it does |
|---|---|
| `file_validation` | Checks that all 9 files exist, are not empty, and are readable |
| `database_setup` | Runs `schema.sql`, empties the tables for a full refresh, clears staging |
| `extract_csv` / `_json` / `_yaml` | Read each format with pandas, check columns, save to staging (run in parallel) |
| `load_customers` | Merges the 3 formats, removes duplicates, bulk-inserts into `customers` |
| `load_products` | Same for `products` |
| `load_orders` | Same for `orders` (after customers, because of the foreign key) |
| `data_validation` | Verifies row counts and that no order references a missing customer |
| `completion` | Archives a copy of the files and writes `SUCCESS` to `etl_audit` |

---

## 7. Further Documentation

- **[Technical Documentation](docs/technical_documentation.md):** architecture, database schema, data flow, design decisions
- **[User Guide](docs/user_guide.md):** step-by-step installation, running the pipeline, troubleshooting
