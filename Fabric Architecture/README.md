# ETL-Pipeline zum Import von Sensor-Daten

## Aufgabenstellung
Firma: Peter Döhle Schiffahrts-KG

Daten: Signaldaten von 12+ Schiffen
- Treibstoff Emissionen, Tankfüllstand, Geschwindigkeit, Gewicht,...
- im 15 min / 1h Takt via Rest API verfügbar

ursprüngliches Ziel: Skalierbare ETL-Strecke für den poc von Microsoft Fabric

geändertes Ziel: Vergleich verschiedener ETL-Ansätze anhand festgelegter Kriterien


## API-Endpoints

Response: JSON-Format
- /fleet : Alle Schiffe inkl. technischer Informationen
- /fleet/{imo}/signals: Auflistung der für das Schiff verfügbaren Sensordaten inkl. Erklärung
- /fleet/{imo}/timeseries?{optionaleParameter}: Zeitreihe der Sensordaten des Schiffes in 5-min Abständen

Zielbild: 
Zeitreihe mit verständlichen Signalnamen pivotisiert und vollständig historisiert

## Herausforderungen

- Teils starke Verschachtelung
- Schlechte Verbindung des Schiffes 
    - Fehlende Daten als NULL gespeichert
- Fehlende Daten teils nachträglich überschrieben 
    - überlappendes Laden notwendig
- Veränderung der Anzahl der Signale 
- Pivotierung führt zu sehr vielen Spalten

## Hoppe: Pipeline-Ablauf

### Voraussetzungen
- IMO-Liste erstellen
- Signals-Liste anlegen

### ShipData Pipeline
*Läuft maximal einmal täglich*

1. ShipData API abrufen
2. JSON-Daten speichern
3. IMO-Nummern extrahieren und unter `data/latest/` speichern
4. Relevante Daten aus JSON extrahieren
5. Daten im PARQUET-Format speichern

### Signals Pipeline
*Läuft maximal einmal täglich*

1. ShipData API abrufen
2. IMO-Nummern aus gespeicherter Datei laden
3. Für jedes Schiff (parallelisierte Verarbeitung):
   1. Signals API für das spezifische Schiff abrufen
   2. JSON-Daten speichern
   3. Relevante Daten aus JSON extrahieren
   4. Daten im PARQUET-Format speichern
   5. Die `signal_mapping`-Datei in `data/latest/` aktualisieren (enthält imo, signal_id & friendly_name)

### Timeseries Pipeline
*Läuft mehrmals täglich (ca. 1x pro Stunde)*

1. ShipData API abrufen
2. IMO-Nummern aus gespeicherter Datei laden
3. `signal_mapping` Datei laden
4. Aktuelle Daily Summary in `daily_df` laden und mit Tag "today" versehen
5. Falls noch keine Daily Summary Datei vorhanden ist (`daily_df` leer):
   - `ref_data` Datei aktualisieren, sodass nur die letzten X Tage enthalten sind
   - Ältesten Tag entfernen & letzten Tag hinzufügen
6. Reference Data in `hist_df` laden und mit Tag "old" versehen
7. Leeren DataFrame `current_df` erstellen
8. Für jedes Schiff (parallelisierte Verarbeitung):
   1. Timeseries API abrufen (mit `to_date = run_timestamp` um Abweichungen durch Abfragezeit zu vermeiden)
   2. JSON-Daten speichern
   3. Relevante Daten aus JSON extrahieren
   4. Friendly-Name aus `signal_mapping` hinzufügen
   5. Daten im PARQUET-Format speichern
   6. Daten zum Gesamt-DataFrame `current_df` hinzufügen
9. Daily Summary erstellen/ergänzen:
   - `hist_df`, `daily_df` und `current_df` zusammenführen
   - Nur eindeutige Einträge behalten, die zuerst im DataFrame erscheinen und den Tag "new" oder "today" haben
   - Ergebnis im `summary_df` speichern
10. Daily Summary (`summary_df`) als PARQUET speichern:
    - Falls noch keine Datei zum aktuellen Tag existiert, neue Datei anlegen
    - Ansonsten vorhandene Datei überschreiben
    - Idee: Eine Datei, in der alle neuen Einträge des Tages gespeichert sind im Verzeichnis data/daily_summary/jahrmonattag.parquet