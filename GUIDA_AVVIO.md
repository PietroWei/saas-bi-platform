# Guida avvio SaaS BI Platform (passo per passo)

Guida in italiano per avviare il progetto **pezzo per pezzo** su Windows 11.
Pensata per chi parte da zero: spiega *cosa installare*, *perché*, e *come lanciare ogni servizio* singolarmente prima di mettere su tutto lo stack.

The whole guide is written in Italian (UI choice), but the platform itself runs in English.

---

## 0. Cosa ti serve (prerequisiti)

| Strumento | Stato sulla tua macchina | Serve per |
|---|---|---|
| Python 3.12 (Anaconda) | gia' installato | sviluppo locale, generare le chiavi Airflow |
| conda | gia' installato | creare l'ambiente `saas_bi` |
| git | gia' installato | versionare il codice |
| **Docker Desktop** | **DA INSTALLARE** | far girare tutto lo stack containerizzato |

### 0.1 Installare Docker Desktop

1. Scarica l'installer da https://www.docker.com/products/docker-desktop/ "Download for Windows".
2. Esegui `Docker Desktop Installer.exe`. Lascia spuntata l'opzione **"Use WSL 2 instead of Hyper-V"** (raccomandata su Win 11).
3. Al termine, **riavvia il PC** se richiesto.
4. Avvia **Docker Desktop** dal menu Start e aspetta che l'icona della balena in basso a destra diventi **verde/stabile**.
5. Verifica da PowerShell:
   ```powershell
   docker --version
   docker compose version
   docker info
   ```
   Se `docker info` risponde senza errori sei pronto.

> Se `docker info` restituisce "Cannot connect to the Docker daemon", significa che Docker Desktop non e' ancora partito: aprilo e aspetta.

---

## 1. Configurare il file `.env`

Il repo contiene `.env.example` come template. Il `.env` vero non e' versionato (e' nel `.gitignore`).

### 1.1 Copia il template

```powershell
Copy-Item .env.example .env
```

### 1.2 Genera le due chiavi Airflow

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python -c "import secrets; print(secrets.token_hex(32))"
```

Se il primo comando da' `ModuleNotFoundError: cryptography`, installalo:
```powershell
pip install cryptography
```

Copia i due output in `.env` al posto di `AIRFLOW_FERNET_KEY` e `AIRFLOW_SECRET_KEY`.

### 1.3 Imposta `DATABASE_URL` (Supabase)

I DAG nuovi (`reddit_dag`, `appstore_dag`, `web_signals_dag`) scrivono sullo schema `raw` di **Supabase**, non sul Postgres locale.

In `.env` aggiungi (sostituendo eventuali placeholder):
```
DATABASE_URL=postgresql://postgres:LA_TUA_PASSWORD@db.IL_TUO_REF.supabase.co:5432/postgres
```

La connection string si copia da Supabase: **Project Settings, Database, Connection string, URI**.

### 1.4 Crea le tabelle raw su Supabase

Apri Supabase, **SQL Editor, New query**, e lancia in ordine:

1. `database/init/01_create_schemas.sql` (crea `raw`, `staging`, `marts`)
2. `database/migrations/002_new_raw_tables.sql` (crea `reddit_mentions`, `app_reviews`, `app_ratings`, `web_signals`)

---

## 2. Avvio pezzo per pezzo

Lo stack e' composto da 6 servizi Docker. Dipendenze:

```
postgres -> airflow-init -> airflow-webserver + airflow-scheduler
postgres -> dbt-runner
postgres -> fastapi-backend -> streamlit-frontend
```

### 2.1 Step 1, Database Postgres locale (per Airflow + dev)

```powershell
docker compose up -d --build postgres
docker compose ps
docker compose logs postgres --tail 30
```

Cerca la riga `database system is ready to accept connections`. Lo stato deve diventare `healthy` entro 30-60 secondi.

> Nota: i DAG scrivono i dati ingest su **Supabase** (tramite `DATABASE_URL`).
> Il Postgres locale serve principalmente al metadato di Airflow.

### 2.2 Step 2, Airflow (init + webserver + scheduler)

```powershell
docker compose up -d --build airflow-init
docker compose logs -f airflow-init
```

Aspetta che `airflow-init` finisca con exit code 0. Poi:

```powershell
docker compose up -d airflow-webserver airflow-scheduler
docker compose logs -f airflow-webserver
```

Apri http://localhost:8080, login `admin` / `admin` (o quello che hai messo in `.env`).

> La prima build scarica l'immagine Airflow (~1 GB) e installa i `requirements.txt` con `praw`, `textblob`, `app-store-scraper`, `google-play-scraper`, `pytrends`. Puo' impiegare 3-5 minuti.

### 2.3 Step 3, dbt runner

```powershell
docker compose up -d --build dbt-runner
docker compose run --rm dbt-runner dbt --version
docker compose run --rm dbt-runner dbt debug
```

`dbt debug` conferma la connessione. Se dice `All checks passed!` sei a posto.

> Non eseguire `dbt run` ora: le tabelle `raw.*` su Supabase sono ancora vuote. Prima fai partire i DAG (step 6).

### 2.4 Step 4, FastAPI backend

```powershell
docker compose up -d --build fastapi-backend
docker compose logs -f fastapi-backend
```

Aspetta `Uvicorn running on http://0.0.0.0:8000`. Apri http://localhost:8000/docs e prova `/health`.

### 2.5 Step 5, Streamlit frontend

```powershell
docker compose up -d --build streamlit-frontend
docker compose logs -f streamlit-frontend
```

Apri http://localhost:8501.

### 2.6 Step 6, Popolare i dati (DAG + dbt)

1. Su Airflow (http://localhost:8080), **unpause** e triggera in quest'ordine:
   - `reddit_dag`        (giornaliero, post Reddit + sentiment TextBlob)
   - `appstore_dag`      (giornaliero, recensioni iOS + Android e rating aggregati)
   - `web_signals_dag`   (settimanale, Google Trends + proxy HN)

2. Quando i DAG sono verdi:
   ```powershell
   docker compose run --rm dbt-runner dbt build
   ```

3. Ricarica http://localhost:8501.

---

## 3. Comandi di servizio utili

### Stato di tutti i container
```powershell
docker compose ps
```

### Log di un singolo servizio
```powershell
docker compose logs -f fastapi-backend
docker compose logs -f airflow-scheduler
```

### Entrare in un container per debug
```powershell
docker compose exec postgres psql -U PietroWei -d bi_platform
docker compose exec fastapi-backend bash
```

### Fermare tutto (dati preservati)
```powershell
docker compose stop
```

### Fermare e rimuovere container (dati preservati nel volume)
```powershell
docker compose down
```

### RESET TOTALE (cancella anche i dati Postgres locali)
```powershell
docker compose down -v
```

> Le tabelle su Supabase non vengono toccate da `down -v`. Per resettarle, droppa e ricrea via SQL Editor.

### Rebuild di un singolo servizio
```powershell
docker compose up -d --build fastapi-backend
```

---

## 4. Sviluppo locale senza Docker (opzionale)

```powershell
conda env create -f environment.yml
conda activate saas_bi
```

Poi, con Postgres gia' su via Docker:
```powershell
# API da locale
uvicorn backend.main:app --reload --port 8000

# Streamlit da locale
streamlit run frontend/app/main.py

# Validare la sintassi di un DAG
python airflow/dags/reddit_dag.py
python airflow/dags/appstore_dag.py
python airflow/dags/web_signals_dag.py
```

---

## 5. Troubleshooting

| Sintomo | Soluzione |
|---|---|
| `docker: command not found` o `Cannot connect to the Docker daemon` | Docker Desktop non e' avviato. Aprilo e aspetta. |
| `AIRFLOW_FERNET_KEY` mancante al boot | Non hai generato la chiave: torna allo step 1.2. |
| Porta gia' in uso (es. 8080 / 5432 / 8000) | Cambia la parte sinistra del mapping in `docker-compose.yml`. |
| Streamlit mostra grafici vuoti | Step 2.6 non fatto: triggera i DAG e lancia `dbt build`. |
| `reddit_dag` ritorna 0 righe per una company | Reddit ha rate-limited o e' temporaneamente irragiungibile. Il DAG usa `SAMPLE_DATA` come fallback. |
| `appstore_dag` salta Murex/Bloomberg/Slack | Atteso: niente app consumer, viene loggato e si va avanti. |
| `web_signals_dag` Google Trends 429 | pytrends fa rate-limit pesante. Il DAG cade nel fallback bundled. |
| `dbt build` fallisce con "relation does not exist" | I DAG non hanno ancora popolato Supabase: aspetta che siano verdi. |
| Errori line-endings nei Dockerfile | `git config --global core.autocrlf input`. |

---

## 6. Riassunto flusso di avvio "da zero"

```powershell
# 0. Installa Docker Desktop e avvialo

# 1. Configura le variabili d'ambiente
Copy-Item .env.example .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python -c "import secrets; print(secrets.token_hex(32))"
# (modifica .env: incolla i due valori e DATABASE_URL di Supabase)

# 1.b Crea le tabelle raw su Supabase
#  Esegui in SQL Editor:
#  - database/init/01_create_schemas.sql
#  - database/migrations/002_new_raw_tables.sql

# 2. Avvio graduale
docker compose up -d --build postgres
docker compose up -d --build airflow-init
docker compose up -d airflow-webserver airflow-scheduler
docker compose up -d --build dbt-runner
docker compose up -d --build fastapi-backend
docker compose up -d --build streamlit-frontend

# 3. Popola i dati
#  Airflow UI http://localhost:8080 : unpausa e triggera reddit_dag, appstore_dag, web_signals_dag
docker compose run --rm dbt-runner dbt build

# 4. Controlla
#  http://localhost:8080  Airflow
#  http://localhost:8000/docs  FastAPI Swagger
#  http://localhost:8501  Streamlit
```
