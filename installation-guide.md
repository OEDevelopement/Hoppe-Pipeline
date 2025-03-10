# Installation Guide für die Hoppe ETL Pipeline

Diese Anleitung beschreibt, wie Sie die Hoppe ETL Pipeline installieren und konfigurieren.

## Voraussetzungen

- Python 3.8 oder höher
- Pip (Python-Paketmanager)
- ODBC-Treiber für MS SQL Server (wenn Datenbankintegration verwendet wird)
- Zugang zur Hoppe API mit einem gültigen API-Schlüssel

## Schritte zur Installation

### 1. Repository klonen oder Dateien herunterladen

Laden Sie alle Dateien der Pipeline in ein lokales Verzeichnis herunter.

### 2. Python-Umgebung einrichten (empfohlen)

Es wird empfohlen, eine virtuelle Python-Umgebung zu verwenden, um die Abhängigkeiten zu isolieren:

```bash
# Virtuelle Umgebung erstellen
python -m venv hoppe-env

# Umgebung aktivieren
# Unter Windows:
hoppe-env\Scripts\activate
# Unter macOS/Linux:
source hoppe-env/bin/activate
```

### 3. Abhängigkeiten installieren

```bash
pip install -r requirements.txt
```

### 4. Konfiguration einrichten

Erstellen Sie eine `.env`-Datei im Hauptverzeichnis der Pipeline basierend auf dem bereitgestellten Beispiel:

```bash
cp .env.example .env
```

Öffnen Sie die `.env`-Datei und passen Sie die Konfigurationswerte an:

```
# Hoppe API Konfiguration
HOPPE_API_KEY=your_api_key_here
HOPPE_BASE_URL=https://api.hoppe-sts.com/

# Dateipfade für die verschiedenen Datenebenen
RAW_PATH=./data/raw_data
TRANSFORMED_PATH=./data/transformed_data
GAPS_PATH=./data/gaps_data

# Datenbankverbindung
# Für Windows-Authentifizierung
MSSQL_SERVER=server_name
MSSQL_DATABASE=database_name
# Oder als Connection-String
MSSQL_CONNECTION_STRING=mssql+pyodbc://server_name/database_name?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes

# Pipeline-Konfiguration
MAX_WORKERS=8
RETRY_ATTEMPTS=5
TIMEOUT=45
DAYS_TO_KEEP=90
HISTORY_DAYS=5
```

### 5. Datenbankverbindung einrichten (optional)

Wenn Sie die Datenbankintegration verwenden möchten, stellen Sie sicher, dass der ODBC-Treiber für MS SQL Server installiert ist:

#### Windows

1. Laden Sie den Microsoft ODBC-Treiber für SQL Server von der Microsoft-Website herunter
2. Führen Sie das Installationsprogramm aus
3. Überprüfen Sie die Installation mit `odbcad32.exe`

#### Linux

```bash
# Ubuntu/Debian
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql17
```

### 6. Verzeichnisstruktur erstellen

Die Pipeline erstellt die erforderlichen Verzeichnisse automatisch beim ersten Ausführen, aber Sie können sie auch manuell erstellen:

```bash
mkdir -p data/raw_data
mkdir -p data/transformed_data
mkdir -p data/gaps_data
```

## Ausführung der Pipeline

Nach der Installation können Sie die Pipeline wie folgt ausführen:

```bash
# Vollständige Pipeline
python main.py --mode all

# Nur Timeseries-Daten aktualisieren (für 15-Minuten-Intervalle)
python main.py --mode timeseries

# Nur Fleet- und Signal-Daten aktualisieren (für tägliche Updates)
python main.py --mode fleet
```

## Integration mit Microsoft Fabric oder Azure Synapse

Wenn Sie die Pipeline in Microsoft Fabric oder Azure Synapse verwenden möchten:

1. Importieren Sie das `fabric_notebook.ipynb` in Ihre Fabric- oder Synapse-Umgebung
2. Konfigurieren Sie die erforderlichen Geheimnisse in Fabric/Synapse
3. Passen Sie die Datei-Pfade an die Lakehouse-Struktur an
4. Planen Sie die Ausführung entsprechend Ihren Anforderungen

## Fehlerbehebung

### Probleme mit der Datenbankverbindung

- Überprüfen Sie, ob der ODBC-Treiber korrekt installiert ist
- Testen Sie die Verbindung mit einem einfachen SQL-Client
- Überprüfen Sie die Firewall-Einstellungen für den Datenbankserver

### API-Verbindungsprobleme

- Stellen Sie sicher, dass Ihr API-Schlüssel gültig ist
- Überprüfen Sie Ihre Internetverbindung
- Erhöhen Sie den Timeout-Wert in der `.env`-Datei

### Leistungsprobleme

- Reduzieren Sie den `MAX_WORKERS`-Wert, wenn Speicher- oder CPU-Probleme auftreten
- Passen Sie `BATCH_SIZE` für große Datensätze an

## Nächste Schritte

Nach erfolgreicher Installation können Sie:

1. Die Pipeline in einem Scheduler (z.B. cron) einrichten
2. Überwachung mit Logging-Tools implementieren
3. Die Daten in Business Intelligence-Tools einbinden

Für weitere Unterstützung oder Fragen wenden Sie sich an Ihr Entwicklungsteam.