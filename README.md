# Hoppe API ETL Pipeline

Diese ETL-Pipeline ermöglicht das Abrufen, Transformieren und Speichern von Daten aus der Hoppe-STS-API. Sie ist darauf ausgelegt, sowohl als eigenständiges Python-Skript als auch in Microsoft Fabric oder Azure Synapse zu laufen.

## Funktionalitäten

- **API-Integration**: Robustes Abrufen von Daten mit Retry-Logik
- **Datenverarbeitung**: Transformation von Fleet-, Signal- und Timeseries-Daten
- **Null-Wert-Behandlung**: Erkennung und Speicherung von Datenlücken
- **Historische Daten**: Integration mit historischen Daten der letzten Tage
- **Datenbankintegration**: Flexibles Schema für MSSQL-Datenbank mit automatischer Spaltenanpassung
- **Parallelverarbeitung**: Effiziente Verarbeitung mehrerer Schiffe
- **Datenverwaltung**: Automatische Bereinigung alter Daten
- **Metadaten-Tracking**: Zeitstempel für alle verarbeiteten Daten

## Projektstruktur

```
hoppe-etl/
├── enhanced-etl-pipeline.py    # Hauptpipeline-Skript
├── fabric_notebook.ipynb       # Microsoft Fabric Notebook
├── .env                        # Konfigurationsdatei
├── requirements.txt            # Python-Abhängigkeiten
├── README.md                   # Diese Datei
└── data/                       # Datenverzeichnis
    ├── raw_data/               # Rohdaten
    ├── transformed_data/       # Transformierte Daten
    └── gaps_data/              # Lückendaten
```

## Voraussetzungen

- Python 3.8+
- Die folgenden Python-Pakete (siehe `requirements.txt`):
  - polars
  - pandas
  - requests
  - python-dotenv
  - sqlalchemy
  - pyodbc
  - pytz

## Installation

1. Repository klonen oder Dateien herunterladen
2. Python-Abhängigkeiten installieren:
   ```bash
   pip install -r requirements.txt
   ```
3. `.env`-Datei mit Ihren Konfigurationseinstellungen erstellen (siehe `.env.example`)

## Verwendung

### Als eigenständiges Skript

```bash
# Vollständige Pipeline ausführen
python enhanced-etl-pipeline.py --mode all

# Nur Timeseries-Daten verarbeiten (für 15-Minuten-Intervalle)
python enhanced-etl-pipeline.py --mode timeseries

# Nur Fleet- und Signal-Daten verarbeiten (für tägliche Aktualisierungen)
python enhanced-etl-pipeline.py --mode fleet
```

### In Microsoft Fabric

1. Das Notebook `fabric_notebook.ipynb` in Microsoft Fabric importieren
2. Die erforderlichen Geheimnisse in Fabric Secrets einrichten:
   - `HOPPE_API_KEY`
   - `MSSQL_CONNECTION_STRING` (optional)
3. Notebook-Zellen anpassen und ausführen
4. In Fabric einen geplanten Job einrichten:
   - Tägliche Ausführung für die vollständige Pipeline (`mode="all"`)
   - 15-Minuten-Intervalle für die Timeseries-Pipeline (`mode="timeseries"`)

## Datenbankschema

Die Pipeline erstellt und aktualisiert automatisch die folgenden Tabellen in der SQL-Datenbank:

- **TimeSeries_Staging**: Temporäre Tabelle für eingehende Daten
- **TimeSeries_pivot**: Haupttabelle mit pivotierten Zeitreihendaten
  - Enthält eine Spalte für jeden Signal-Typ
  - Primärschlüssel: (`imo`, `signal_timestamp`)
- **TimeSeries_Gaps**: Tabelle zur Speicherung von Datenlücken
  - Enthält (`imo`, `signal`, `gap_start`, `gap_end`, `loaddate`)
  - Ermöglicht die Analyse von Zeiträumen ohne Signale

## Konfigurationsoptionen (.env)

| Parameter | Beschreibung | Standardwert |
|-----------|--------------|--------------|
| HOPPE_API_KEY | API-Schlüssel für die Hoppe-API | (erforderlich) |
| HOPPE_BASE_URL | Basis-URL der API | https://api.hoppe-sts.com/ |
| RAW_PATH | Pfad für Rohdaten | ./data/raw_data |
| TRANSFORMED_PATH | Pfad für transformierte Daten | ./data/transformed_data |
| GAPS_PATH | Pfad für Lückendaten | ./data/gaps_data |
| MSSQL_CONNECTION_STRING | Datenbankverbindungs-String | (optional) |
| MAX_WORKERS | Anzahl paralleler Prozesse | 8 |
| RETRY_ATTEMPTS | Anzahl API-Wiederholungsversuche | 5 |
| TIMEOUT | API-Timeout in Sekunden | 45 |
| DAYS_TO_KEEP | Aufbewahrungsdauer für Daten in Tagen | 90 |
| HISTORY_DAYS | Anzahl der Tage für historische Daten | 5 |

## Anpassung und Erweiterung

Die Pipeline ist modular aufgebaut und kann leicht erweitert werden:

- **Neue Datenquellen**: Erweitern Sie `APIClient` für zusätzliche API-Endpunkte
- **Zusätzliche Transformationen**: Ergänzen Sie `DataProcessor` mit neuen Transformationsmethoden
- **Alternative Speicherlösungen**: Implementieren Sie zusätzliche Methoden in `DataStorage`
- **Erweiterte Datenbank-Integration**: Passen Sie die `write_ts_to_msdb`-Methode an Ihre Bedürfnisse an

## Fehlerbehebung

- **API-Verbindungsprobleme**: Überprüfen Sie API-Schlüssel und Netzwerkverbindung
- **Datenbankfehler**: Stellen Sie sicher, dass die Datenbankverbindung korrekt konfiguriert ist
- **Speicherprobleme**: Reduzieren Sie `MAX_WORKERS` oder teilen Sie die Verarbeitung in kleinere Batches auf

## Weitere Hinweise

Die Pipeline berücksichtigt die Besonderheiten der Hoppe-API:

- Sensordaten werden an Bord gepuffert, bevor sie an Land übertragen werden
- Daten können nach Verbindungsabbrüchen "anti-chronologisch" übertragen werden
- Zeitstempel beziehen sich immer auf den Zeitpunkt der Messung

## Beiträge

Verbesserungsvorschläge und Pull Requests sind willkommen!