# Guida avvio SaaS BI Platform (passo per passo)

Guida in italiano per avviare il progetto **pezzo per pezzo** su Windows 11.
Pensata per chi parte da zero: spiega *cosa installare*, *perché*, e *come lanciare ogni servizio* singolarmente prima di mettere su tutto lo stack.

---

## 0. Cosa ti serve (prerequisiti)

| Strumento | Stato sulla tua macchina | Serve per |
|---|---|---|
| Python 3.12 (Anaconda) | ✅ già installato | sviluppo locale, generare le chiavi Airflow |
| conda | ✅ già installato | creare l'ambiente `saas_bi` |
| git | ✅ già installato | versionare il codice |
| **Docker Desktop** | ❌ **DA INSTALLARE** | far girare tutto lo stack containerizzato |

### 0.1 Installare Docker Desktop (unica cosa che manca)

1. Scarica l'installer da https://www.docker.com/products/docker-desktop/ → "Download for Windows".
2. Esegui `Docker Desktop Installer.exe`. Lascia spuntata l'opzione **"Use WSL 2 instead of Hyper-V"** (raccomandata su Win 11).
3. Al termine, **riavvia il PC** se richiesto.
4. Avvia **Docker Desktop** dal menu Start e aspetta che l'icona della balena in basso a destra diventi **verde/stabile**. Serve che il demone sia attivo prima di qualsiasi `docker compose`.
5. Verifica da PowerShell:
   ```powershell
   docker --version
   docker compose version
   docker info
   ```
   Se `docker info` risponde senza errori → sei pronto.

> ⚠️ Se `docker info` dà errore "Cannot connect to the Docker daemon", significa che Docker Desktop non è ancora partito: apri l'app e aspetta.

---

## 1. Configurare il file `.env`

Il repo contiene `.env.example` come template. Il `.env` vero **NON** è versionato (è nel `.gitignore`).

### 1.1 Copia il template

Da PowerShell nella cartella del progetto:
```powershell
Copy-Item .env.example .env
```

### 1.2 Genera le due chiavi Airflow

Airflow ha bisogno di due segreti: una **Fernet key** (cripta le connessioni) e una **secret key** (firma i cookie della web UI).

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python -c "import secrets; print(secrets.token_hex(32))"
```

Se il primo comando dà `ModuleNotFoundError: cryptography`, installalo:
```powershell
pip install cryptography
```

Copia i due output e incollali in `.env` al posto di:
- `AIRFLOW_FERNET_KEY=replace_with_fernet_key_generated_locally`
- `AIRFLOW_SECRET_KEY=replace_with_random_32_byte_hex`

### 1.3 Cambia la password Postgres (consigliato)

In `.env`, sostituisci:
```
POSTGRES_PASSWORD=change_me_strong_password
```
con una password a tuo piacere (niente spazi, niente caratteri esotici).

### 1.4 (Opzionale) Token GitHub

Se vuoi far girare il DAG `github_activity_dag` senza limiti di rate (passi da 60 a 5000 req/h):
1. Vai su https://github.com/settings/tokens
2. "Generate new token (classic)" → scope `public_repo`
3. Incolla in `.env` alla riga `GITHUB_TOKEN=`

Senza token il DAG funziona comunque, solo più lento.

---

## 2. Avvio pezzo per pezzo

Lo stack è composto da 6 servizi Docker. La dipendenza è:

```
postgres → airflow-init → airflow-webserver + airflow-scheduler
postgres → dbt-runner
postgres → fastapi-backend → streamlit-frontend
```

L'idea: invece di `docker compose up --build` che tira su tutto insieme, li facciamo partire uno alla volta per capire cosa fa ciascuno e vedere errori isolati.

### 2.1 Step 1 — Database Postgres

```powershell
docker compose up -d --build postgres
```

- `-d` = detached (gira in background)
- `--build` = (ri)costruisce l'immagine se serve

Verifica che sia su e sano:
```powershell
docker compose ps
docker compose logs postgres --tail 30
```
Cerca la riga `database system is ready to accept connections`. Lo stato nella colonna `STATUS` deve diventare `healthy` entro 30–60 secondi.

Cosa è successo: è stato creato il container `bi_postgres`, sono stati eseguiti gli script in `database/init/` (schemi `raw` e `marts` + tabelle raw), ed è stato creato anche il DB `airflow` per i metadati.

**Test rapido** (entra nel DB):
```powershell
docker compose exec postgres psql -U PietroWei -d bi_platform -c "\dn" 
```
Devi vedere gli schemi `raw` e `marts`.

### 2.2 Step 2 — Airflow (init + webserver + scheduler)

Airflow ha 3 servizi:
- `airflow-init`: one-shot, crea le tabelle di metadati e l'utente admin
- `airflow-webserver`: la UI su :8080
- `airflow-scheduler`: esegue i DAG

Partono in sequenza grazie ai `depends_on` del compose.

```powershell
docker compose up -d --build airflow-init
docker compose logs -f airflow-init
```
Aspetta che `airflow-init` finisca con exit code 0 (il log smetterà di aggiornarsi e nel `ps` lo vedrai `Exited (0)`). Poi:

```powershell
docker compose up -d airflow-webserver airflow-scheduler
docker compose logs -f airflow-webserver
```

Apri http://localhost:8080 → login `admin` / `admin` (o quello che hai messo in `.env`).

> La prima build scarica l'immagine Airflow (~1 GB) e installa i `requirements.txt` → può impiegare 3–5 minuti. È normale.

### 2.3 Step 3 — dbt runner

```powershell
docker compose up -d --build dbt-runner
```

Questo container resta **idle** (fa `tail -f /dev/null`) apposta: lo usi solo per eseguire comandi dbt on-demand. Verifica che parta:
```powershell
docker compose run --rm dbt-runner dbt --version
docker compose run --rm dbt-runner dbt debug
```
`dbt debug` ti conferma che si collega a Postgres. Se risponde `All checks passed!` sei a posto.

> ⚠️ Non eseguire `dbt run` ora: le tabelle `raw.*` sono ancora vuote. Prima devi far girare i DAG (step 5).

### 2.4 Step 4 — FastAPI backend

```powershell
docker compose up -d --build fastapi-backend
docker compose logs -f fastapi-backend
```

Aspetta la riga `Uvicorn running on http://0.0.0.0:8000`.

Apri http://localhost:8000/docs → Swagger UI con tutti gli endpoint. Prova `/health`: deve rispondere `{"status":"ok"}` (o simile).

> Le rotte `/companies/...` ora rispondono vuoto o con 404 perché i mart sono vuoti. Normale.

### 2.5 Step 5 — Streamlit frontend

```powershell
docker compose up -d --build streamlit-frontend
docker compose logs -f streamlit-frontend
```

Apri http://localhost:8501. Vedrai l'app con grafici vuoti — popoliamo i dati nello step successivo.

### 2.6 Step 6 — Popolare i dati (DAG + dbt)

1. Vai su Airflow (http://localhost:8080), **unpause** e triggera in quest'ordine:
   - `g2_reviews_dag`
   - `crunchbase_dag`
   - `github_activity_dag`
2. Quando sono verdi, lancia la trasformazione:
   ```powershell
   docker compose run --rm dbt-runner dbt build
   ```
   `dbt build` = `run` (materializza i modelli) + `test` (esegue i test sulle colonne). Output atteso: tutti i modelli in `staging` e `marts` completati con `OK`.
3. Ricarica http://localhost:8501 → i dashboard mostrano i dati.

---

## 3. Comandi di servizio utili

### Stato di tutti i container
```powershell
docker compose ps
```

### Log di un singolo servizio (`-f` = follow live)
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

### RESET TOTALE (⚠️ cancella anche i dati Postgres)
```powershell
docker compose down -v
```

### Rebuild di un singolo servizio dopo modifiche al Dockerfile o requirements
```powershell
docker compose up -d --build fastapi-backend
```

---

## 4. Sviluppo locale senza Docker (opzionale)

Utile per iterare velocemente su codice Python senza ricostruire l'immagine.

```powershell
conda env create -f environment.yml
conda activate saas_bi
```

Poi, con Postgres già su via Docker:
```powershell
# API da locale (ricaricamento automatico)
uvicorn backend.main:app --reload --port 8000

# Streamlit da locale
streamlit run frontend/app/main.py

# Validare la sintassi di un DAG
python airflow/dags/g2_reviews_dag.py
```

---

## 5. Troubleshooting

| Sintomo | Soluzione |
|---|---|
| `docker: command not found` / `Cannot connect to the Docker daemon` | Docker Desktop non è avviato. Aprilo e aspetta la balena verde. |
| `AIRFLOW_FERNET_KEY` mancante al boot | Non hai generato la chiave: torna allo step 1.2. |
| Porta già in uso (es. 8080 / 5432 / 8000) | Cambia la parte sinistra del mapping in `docker-compose.yml` (es. `"8001:8000"`). |
| Streamlit mostra grafici vuoti | Step 2.6 non fatto: triggera i DAG e lancia `dbt build`. |
| G2 scraper ritorna 0 righe | G2 fa rate-limit. Il DAG logga un warning ed esce pulito — riprova più tardi. |
| `dbt build` fallisce con "relation does not exist" | I DAG non sono ancora finiti: aspetta che siano verdi su Airflow prima di lanciare dbt. |
| Windows: errori "line endings" o simili nei Dockerfile | In VS Code, forza CRLF→LF sui file del repo, oppure configura git con `git config --global core.autocrlf input`. |

---

## 6. Riassunto flusso di avvio "da zero"

```powershell
# 0. Installa Docker Desktop e avvialo

# 1. Configura le variabili d'ambiente
Copy-Item .env.example .env
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"  # → AIRFLOW_FERNET_KEY
python -c "import secrets; print(secrets.token_hex(32))"                                    # → AIRFLOW_SECRET_KEY
# (modifica .env a mano con i due valori)

# 2. Avvio graduale
docker compose up -d --build postgres
docker compose up -d --build airflow-init
docker compose up -d airflow-webserver airflow-scheduler
docker compose up -d --build dbt-runner
docker compose up -d --build fastapi-backend
docker compose up -d --build streamlit-frontend

# 3. Popola i dati
#  → Airflow UI http://localhost:8080 : unpausa e triggera i 3 DAG
docker compose run --rm dbt-runner dbt build

# 4. Controlla
#  http://localhost:8080  → Airflow
#  http://localhost:8000/docs → FastAPI Swagger
#  http://localhost:8501  → Streamlit
```

Fine. Quando Docker Desktop sarà installato, possiamo ripercorrere questa sequenza insieme, un comando alla volta.
