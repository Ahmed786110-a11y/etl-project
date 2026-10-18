# User Guide: Rapid ETL Pipeline

This guide walks you through installing, running, and troubleshooting the ETL pipeline from scratch. No prior experience with Airflow or Docker is needed.

Commands are shown for **Windows PowerShell**. On Mac or Linux they are the same unless noted.

---

## Part 1: Installation

### 1.1 Install the required software

| Software | Where to get it | Notes |
|---|---|---|
| **Docker Desktop** | docker.com/products/docker-desktop | On Windows, accept the **WSL 2** option and restart when asked |
| **Git** | git-scm.com | Accept all default options |
| **VS Code** (recommended) | code.visualstudio.com | Install the Python and Docker extensions |

Open Docker Desktop and wait until the bottom-left corner says **Engine running**. Then confirm everything is installed:

```
git --version
docker --version
docker compose version
docker run hello-world
```

The last command should print **"Hello from Docker!"**

### 1.2 Get the project

```
cd Documents
git clone https://github.com/<Ahmed786110-a11y>/etl-project.git
cd etl-project
code .
```

### 1.3 Create your settings file

The pipeline reads passwords from a file named `.env`, which is never uploaded to GitHub. Create it from the template:

```
Copy-Item .env.example .env
```

On Mac/Linux, use `cp .env.example .env`.

Open `.env` in VS Code and fill in the values:

```
AIRFLOW_UID=50000
ETL_DB_USER=etl_user
ETL_DB_PASSWORD=choose_a_password
ETL_DB_NAME=etl_db
FERNET_KEY=<paste a generated key>
```

To generate a Fernet key (needs Python on your computer):

```
python -m pip install cryptography
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

If you don't have Python, you can leave `FERNET_KEY=` blank for a local demo. Airflow will show a warning but still work.

---

## Part 2: Running the Pipeline

### 2.1 Generate the data

This creates the 9 data files in `data/`. It runs inside a temporary Docker container, so you don't need Python installed.

```
docker run --rm -v "${PWD}:/app" -w /app python:3.12-slim sh -c "pip install -r requirements.txt && python scripts/generate_data.py"
```

The first run takes 1–3 minutes to download what it needs. Expected output:

```
  customers: 1000 records -> customers.csv, customers.json, customers.yaml
  products: 1000 records -> products.csv, products.json, products.yaml
  orders: 1000 records -> orders.csv, orders.json, orders.yaml
Done: 9 files created
```

### 2.2 Set up Airflow (first time only)

```
docker compose up airflow-init
```

The first time, this downloads several GB of images (5–20 minutes). It's finished when you see:

```
airflow-init-1 exited with code 0
```

### 2.3 Start all services

```
docker compose up -d
```

Wait 2 minutes, then check:

```
docker compose ps
```

Every Airflow service should show **`Up (healthy)`**, and `etl-postgres` should show **`Up`**.

### 2.4 Open Airflow

1. Go to **http://localhost:8080**
2. Log in with username **`airflow`** and password **`airflow`**

### 2.5 Run the pipeline

1. Find **`etl_pipeline`** in the DAG list. It can take up to a minute to appear after startup.
2. Click the **toggle** to turn it on. Once it's on, it also runs automatically every day at 06:00 UTC.
3. Click the **▶ (Trigger)** button and confirm.
4. Click the DAG name, then the latest run, and open the **Graph** view.
5. Watch the 10 tasks turn **green** in order. A full run takes about 30 seconds.

| Colour | Meaning |
|---|---|
| Light green | Running |
| Dark green | Succeeded |
| Yellow / orange | Retrying, or waiting on a failed task |
| Red | Failed (see Part 3) |

### 2.6 Check the results

Run these in the VS Code terminal.

**Pipeline history:**
```
docker compose exec etl-postgres psql -U etl_user -d etl_db -c "SELECT * FROM etl_audit;"
```

**Row counts** (each should be 1000):
```
docker compose exec etl-postgres psql -U etl_user -d etl_db -c "SELECT 'customers', COUNT(*) FROM customers UNION ALL SELECT 'products', COUNT(*) FROM products UNION ALL SELECT 'orders', COUNT(*) FROM orders;"
```

**Look at some data:**
```
docker compose exec etl-postgres psql -U etl_user -d etl_db -c "SELECT * FROM orders LIMIT 5;"
```

If the output ends with **`(END)`**, press **`q`** to return to the prompt.

You can also connect a database tool such as DBeaver or pgAdmin to **localhost : 5433**, using the user, password, and database name from `.env`.

### 2.7 Stop and restart

| Action | Command |
|---|---|
| Stop everything (keeps data) | `docker compose down` |
| Start again | `docker compose up -d` |
| Wipe everything and start fresh | `docker compose down -v`, then repeat from step 2.2 |

---

## Part 3: Troubleshooting

### How to read an error

1. In the Airflow UI, open the failed run.
2. Click the **first red task**.
3. Open the **Logs** tab and scroll to the lines marked **`ERROR`**.

The pipeline also writes a **`FAILED`** row to `etl_audit` whenever a run fails.

### Common problems

| Problem | Likely cause | Fix |
|---|---|---|
| `docker` is not recognized | Docker Desktop isn't installed or isn't running | Open Docker Desktop and wait for **Engine running** |
| localhost:8080 won't load | Airflow is still starting | Wait 2 minutes, then run `docker compose ps` |
| Services keep restarting | Docker doesn't have enough memory | Give Docker at least 4 GB in Docker Desktop settings, or close other programs |
| `Port is already allocated` | Another program is using port 8080 or 5433 | Close that program, or change the first number in `ports:` in `docker-compose.yaml` |
| `etl_pipeline` doesn't appear | The DAG file has an error, or it hasn't been picked up yet | Wait 1 minute, then look for a red **import error** banner at the top of the DAG list |
| **file_validation** fails: `missing: X` | Data files weren't generated | Run the data generation command in step 2.1 again |
| **database_setup** fails: `could not translate host name` | Typo in `ETL_DB_HOST` in `docker-compose.yaml` | It must be exactly `etl-postgres`. Fix it, then run `docker compose up -d --force-recreate` |
| `Missing environment variable` | The `ETL_DB_*` lines are missing from `docker-compose.yaml` or `.env` | Check both files, then run `docker compose up -d --force-recreate` |
| `password authentication failed` | The password in `.env` was changed after the database was created | Run `docker compose down -v` (this deletes data), then start again |
| Data generation fails with `Microsoft Visual C++ 14.0 or greater is required` | Running the generator with a very new Python on Windows | Use the Docker command in step 2.1 instead of running Python directly |
| `(END)` at the bottom of the terminal | The output is shown in a page viewer | Press **`q`** |

### Checking settings inside the containers

To confirm Airflow can see the database settings:

```
docker compose exec airflow-worker env | findstr ETL
```

On Mac/Linux, use `grep` instead of `findstr`. This should list `ETL_DB_HOST`, `ETL_DB_USER`, `ETL_DB_PASSWORD`, and `ETL_DB_NAME`.

### Viewing service logs

```
docker compose logs airflow-scheduler --tail 50
docker compose logs etl-postgres --tail 50
```
