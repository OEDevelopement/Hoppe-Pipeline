#!/usr/bin/env python3
"""
Skript zum Extrahieren, Zusammenführen und Pivotisieren aller verfügbaren Timeseries-Dateien.
Das Skript:
1. Durchsucht alle Timeseries-Dateien in einem angegebenen Verzeichnis
2. Liest und kombiniert die Daten
3. Entfernt Nullwerte und Duplikate
4. Pivotisiert die Daten (Signal wird zu Spalten)
5. Speichert die pivotisierte Tabelle als Parquet-Datei
"""

import os
import argparse
import logging
from pathlib import Path
from datetime import datetime
import polars as pl
import pyarrow as pa

# Logging einrichten
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def find_timeseries_files(base_path: str, max_days: int = None) -> list:
    """
    Findet alle Timeseries-Parquet-Dateien im angegebenen Verzeichnis
    
    Args:
        base_path: Basispfad zum Durchsuchen
        max_days: Optional, maximale Anzahl der zu berücksichtigenden Tage (neueste zuerst)
        
    Returns:
        Liste von Pfaden zu Timeseries-Dateien
    """
    logger.info(f"Durchsuche Verzeichnis {base_path} nach Timeseries-Dateien")
    
    base_dir = Path(base_path)
    if not base_dir.exists() or not base_dir.is_dir():
        logger.error(f"Verzeichnis {base_path} existiert nicht oder ist kein Verzeichnis")
        return []
    
    # Finde alle Jahr-Verzeichnisse, sortiere absteigend für neueste zuerst
    years = sorted([d for d in base_dir.glob("*") if d.is_dir() and d.name.isdigit()], 
                   key=lambda x: x.name, reverse=True)
    
    all_files = []
    days_processed = 0
    
    # Durchlaufe Jahre, Monate, Tage
    for year_dir in years:
        months = sorted([d for d in year_dir.glob("*") if d.is_dir() and d.name.isdigit()], 
                        key=lambda x: x.name, reverse=True)
        
        for month_dir in months:
            days = sorted([d for d in month_dir.glob("*") if d.is_dir() and d.name.isdigit()], 
                          key=lambda x: x.name, reverse=True)
            
            for day_dir in days:
                if max_days is not None and days_processed >= max_days:
                    break
                
                # Finde alle Stunden und Minuten für diesen Tag
                ts_files = []
                hour_dirs = sorted([d for d in day_dir.glob("*") if d.is_dir()], 
                                   key=lambda x: x.name, reverse=True)
                
                for hour_dir in hour_dirs:
                    minute_dirs = sorted([d for d in hour_dir.glob("*") if d.is_dir()], 
                                         key=lambda x: x.name, reverse=True)
                    
                    for minute_dir in minute_dirs:
                        # Finde alle Timeseries-Dateien in diesem Minuten-Verzeichnis
                        files = list(minute_dir.glob("Timeseries_*.parquet"))
                        if files:
                            ts_files.extend(files)
                            # Nur der letzte Run des Tages
                            break
                    
                    if ts_files:
                        # Nur die letzte Stunde des Tages
                        break
                
                all_files.extend(ts_files)
                days_processed += 1
                
                if max_days is not None and days_processed >= max_days:
                    logger.info(f"Maximale Anzahl von Tagen ({max_days}) erreicht")
                    break
            
            if max_days is not None and days_processed >= max_days:
                break
        
        if max_days is not None and days_processed >= max_days:
            break
    
    logger.info(f"Gefunden: {len(all_files)} Timeseries-Dateien aus {days_processed} Tagen")
    return all_files

def load_and_combine_timeseries(file_paths: list) -> pl.DataFrame:
    """
    Lädt und kombiniert alle Timeseries-Dateien
    
    Args:
        file_paths: Liste der zu ladenden Dateipfade
        
    Returns:
        Polars DataFrame mit kombinierten Daten
    """
    logger.info(f"Lade {len(file_paths)} Timeseries-Dateien")
    
    all_dfs = []
    total_rows = 0
    
    for idx, file_path in enumerate(file_paths):
        try:
            if idx % 10 == 0:
                logger.info(f"Verarbeite Datei {idx + 1} von {len(file_paths)}")
            
            df = pl.read_parquet(file_path)
            rows = len(df)
            total_rows += rows
            
            if rows > 0:
                all_dfs.append(df)
        except Exception as e:
            logger.error(f"Fehler beim Lesen von {file_path}: {str(e)}")
    
    if not all_dfs:
        logger.warning("Keine Daten geladen")
        return pl.DataFrame()
    
    # Kombiniere alle DataFrames
    combined_df = pl.concat(all_dfs)
    logger.info(f"Insgesamt {total_rows} Zeilen geladen, {len(combined_df)} vor Deduplizierung")
    
    return combined_df

def clean_and_deduplicate(df: pl.DataFrame) -> pl.DataFrame:
    """
    Entfernt Nullwerte und Duplikate aus dem DataFrame
    
    Args:
        df: Zu bereinigender DataFrame
        
    Returns:
        Bereinigter DataFrame
    """
    if len(df) == 0:
        return df
    
    # Entferne Zeilen mit Nullwerten
    no_nulls = df.filter(pl.col("signal_value").is_not_null())
    logger.info(f"Zeilen nach Entfernen von Nullwerten: {len(no_nulls)}")
    
    # Sortiere nach Ladedatum (absteigend) und entferne Duplikate
    deduplicated = (
        no_nulls
        .sort(by=["loaddate"], descending=True)
        .unique(subset=["imo", "signal", "signal_timestamp"], keep="first")
    )
    
    logger.info(f"Zeilen nach Deduplizierung: {len(deduplicated)}")
    return deduplicated

def pivot_timeseries(df: pl.DataFrame, max_signals: int = None) -> pl.DataFrame:
    """
    Pivotisiert die Timeseries-Daten: Signal wird zu Spalten
    
    Args:
        df: Zu pivotisierender DataFrame
        max_signals: Optional, maximale Anzahl von Signalen zu verarbeiten
                    (für Speicher- und Leistungsoptimierung)
        
    Returns:
        Pivotisierter DataFrame
    """
    if len(df) == 0:
        return df
    
    logger.info("Pivotisiere Daten")
    
    # Identifiziere eindeutige Signale
    unique_signals = df.select("signal").unique()
    signal_count = len(unique_signals)
    
    logger.info(f"Gefunden: {signal_count} eindeutige Signale")
    
    if max_signals is not None and signal_count > max_signals:
        logger.warning(f"Zu viele Signale ({signal_count}), begrenze auf {max_signals}")
        
        # Verwende die häufigsten Signale, wenn zu viele vorhanden sind
        signal_counts = df.group_by("signal").count().sort(by="count", descending=True)
        top_signals = signal_counts.head(max_signals).select("signal")
        
        # Filtere DataFrame auf die häufigsten Signale
        df = df.join(top_signals, on="signal")
        logger.info(f"DataFrame auf {len(df)} Zeilen mit Top-{max_signals} Signalen reduziert")
    
    try:
        # Pivotisiere den DataFrame
        pivoted = df.pivot(
            values="signal_value",
            index=["imo", "signal_timestamp", "loaddate"],
            columns="signal"
        )
        
        logger.info(f"Pivotisierter DataFrame hat {len(pivoted)} Zeilen und {len(pivoted.columns)} Spalten")
        return pivoted
    except Exception as e:
        logger.error(f"Fehler bei der Pivotisierung: {str(e)}")
        
        # Alternative Methode mit expliziter Speicherverwaltung
        logger.info("Versuche alternative Pivotisierungsmethode...")
        
        # Gruppieren nach imo, timestamp, loaddate
        pivoted_dfs = []
        
        for sig in df.select("signal").unique().to_series():
            try:
                # Filtern für dieses Signal
                signal_df = df.filter(pl.col("signal") == sig)
                
                # Umbenennen der signal_value-Spalte zum Signalnamen
                renamed = signal_df.select(
                    ["imo", "signal_timestamp", "loaddate", 
                     pl.col("signal_value").alias(sig)]
                )
                
                pivoted_dfs.append(renamed)
            except Exception as sub_e:
                logger.error(f"Fehler bei der Verarbeitung von Signal {sig}: {str(sub_e)}")
        
        if not pivoted_dfs:
            logger.error("Keine Daten nach alternativer Pivotisierung")
            return pl.DataFrame()
        
        # Join all signal dataframes
        result = pivoted_dfs[0]
        for idx, signal_df in enumerate(pivoted_dfs[1:], 1):
            if idx % 10 == 0:
                logger.info(f"Verbinde Signal {idx} von {len(pivoted_dfs) - 1}")
                
            try:
                result = result.join(
                    signal_df, 
                    on=["imo", "signal_timestamp", "loaddate"], 
                    how="outer"
                )
            except Exception as join_e:
                logger.error(f"Fehler beim Verbinden von Signal {idx}: {str(join_e)}")
        
        logger.info(f"Alternative Pivotisierung: {len(result)} Zeilen, {len(result.columns)} Spalten")
        return result

def main():
    parser = argparse.ArgumentParser(description="Extrahiert, kombiniert und pivotisiert Timeseries-Daten")
    parser.add_argument(
        "--base-path", 
        type=str, 
        default="./data/transformed_data",
        help="Basispfad zum Durchsuchen nach Timeseries-Dateien"
    )
    parser.add_argument(
        "--output", 
        type=str, 
        default="./pivoted_timeseries.parquet",
        help="Pfad für die Ausgabedatei"
    )
    parser.add_argument(
        "--max-days", 
        type=int, 
        default=None,
        help="Maximale Anzahl der zu berücksichtigenden Tage (neueste zuerst)"
    )
    parser.add_argument(
        "--max-signals", 
        type=int, 
        default=None,
        help="Maximale Anzahl der zu verarbeitenden Signale (häufigste zuerst)"
    )
    
    args = parser.parse_args()
    
    # Starte Zeitmessung
    start_time = datetime.now()
    logger.info(f"Starte Verarbeitung um {start_time}")
    
    # Finde Dateien
    ts_files = find_timeseries_files(args.base_path, args.max_days)
    
    if not ts_files:
        logger.error("Keine Timeseries-Dateien gefunden")
        return
    
    # Lade und kombiniere Daten
    combined_df = load_and_combine_timeseries(ts_files)
    
    if len(combined_df) == 0:
        logger.error("Keine Daten geladen")
        return
    
    # Bereinige Daten
    clean_df = clean_and_deduplicate(combined_df)
    
    if len(clean_df) == 0:
        logger.error("Keine Daten nach Bereinigung")
        return
    
    # Pivotisiere Daten
    pivoted_df = pivot_timeseries(clean_df, args.max_signals)
    
    if len(pivoted_df) == 0:
        logger.error("Keine Daten nach Pivotisierung")
        return
    
    # Speichere Ergebnis
    try:
        # Erstelle das Ausgabeverzeichnis, falls es nicht existiert
        output_dir = os.path.dirname(args.output)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            
        pivoted_df.write_parquet(args.output, compression="snappy")
        logger.info(f"Pivotisierte Daten gespeichert nach {args.output}")
        
        # Berechne Statistiken
        stats = {
            "Anzahl der Zeilen": len(pivoted_df),
            "Anzahl der Spalten": len(pivoted_df.columns),
            "Eindeutige IMOs": len(pivoted_df.select("imo").unique()),
            "Zeitraum": f"{pivoted_df.select('signal_timestamp').min()[0]} bis {pivoted_df.select('signal_timestamp').max()[0]}"
        }
        
        logger.info("Statistiken der pivotisierten Daten:")
        for key, value in stats.items():
            logger.info(f"  {key}: {value}")
        
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Daten: {str(e)}")
    
    # Beende Zeitmessung
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Verarbeitung abgeschlossen nach {duration}, Endzeit: {end_time}")

if __name__ == "__main__":
    main()
