import logging
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Set

logger = logging.getLogger("hoppe_etl_pipeline")

class DataProcessor:
    """Processes raw API data into analytics-ready format"""

    @staticmethod
    def get_imo_numbers(data: List[dict]) -> List[str]:
        """Extracts IMO numbers from ship data"""
        return [ship['imo'] for ship in data if ship.get('active', True)]

    @staticmethod
    def transform_signals(signals: pl.DataFrame, run_timestamp: str) -> pl.DataFrame:
        """Transforms signals data"""
        if len(signals) == 0:
            return signals
            
        signals = (
            signals.lazy()
            .unnest("signals")
            .unpivot(index="imo", variable_name="signal")
            .unnest("value")
        )

        # Unnest remaining structs
        for column, dtype in signals.collect_schema().items():
            if dtype == pl.Struct:
                signals = signals.unnest(column)

        # Handle null columns
        for column, dtype in signals.collect_schema().items():
            if dtype == pl.Null:
                signals = signals.with_columns(pl.col(column).cast(pl.String))
        
        # Add loaddate
        signals = signals.with_columns(
            pl.lit(run_timestamp).alias("loaddate")
        )
                
        return signals.collect()
    
    @staticmethod
    def transform_timeseries(timeseries: pl.DataFrame, imo: str, run_timestamp: str) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """
        Transforms time series data and extracts gaps data
        
        Returns:
            Tuple of (transformed_data, gaps_data)
        """
        if len(timeseries) == 0:
            return timeseries, pl.DataFrame()
        
        # Initial transformation
        transformed = (
            timeseries.lazy()
            .drop("timestamp")
            .unpivot(variable_name="signal")
            .unnest("value")
            .unpivot(
                index="signal",
                variable_name="signal_timestamp",
                value_name="signal_value",
            )
            .with_columns(
                pl.lit(imo).alias("imo"),
                pl.lit(run_timestamp).alias("loaddate")
            )
        )
        
        # Identifiziere Lücken (NULL-Werte)
        gaps = (
            transformed
            .filter(pl.col("signal_value").is_null())
            .select(["imo", "signal", "signal_timestamp", "loaddate"])
            .with_columns(
                pl.col("signal_timestamp").alias("gap_start")
            )
        )
        
        # Entferne NULL-Werte aus dem Hauptdatensatz
        data = transformed.filter(pl.col("signal_value").is_not_null())
        
        return data.collect(), gaps.collect()
    
    @staticmethod
    def process_gaps(gaps_df: pl.DataFrame) -> pl.DataFrame:
        """
        Verarbeitet Lücken-Daten, um zusammenhängende Zeiträume zu identifizieren
        """
        if len(gaps_df) == 0:
            return pl.DataFrame()
            
        # Gruppiere nach IMO und Signal, sortiere nach Zeitstempel
        result = []
        
        # Konvertiere zu Pandas für einfachere Gruppierung und Verarbeitung
        # (In einer produktiven Umgebung kann dies für große Datensätze 
        # effizienter mit Polars-nativen Funktionen implementiert werden)
        gaps_pd = gaps_df.to_pandas()
        
        for (imo, signal), group in gaps_pd.groupby(['imo', 'signal']):
            group = group.sort_values('gap_start')
            
            # Parse timestamps to datetime
            group['gap_start'] = pd.to_datetime(group['gap_start'])
            
            current_start = group['gap_start'].iloc[0]
            prev_time = current_start
            
            for idx, row in group.iloc[1:].iterrows():
                curr_time = row['gap_start']
                
                # Wenn mehr als 15 Minuten zwischen den Zeitstempeln liegen, 
                # betrachte es als neue Lücke
                if (curr_time - prev_time) > timedelta(minutes=15):
                    result.append({
                        'imo': imo,
                        'signal': signal,
                        'gap_start': current_start.isoformat(),
                        'gap_end': prev_time.isoformat(),
                        'loaddate': row['loaddate']
                    })
                    current_start = curr_time
                
                prev_time = curr_time
            
            # Füge die letzte Lücke hinzu
            result.append({
                'imo': imo,
                'signal': signal,
                'gap_start': current_start.isoformat(),
                'gap_end': prev_time.isoformat(),
                'loaddate': group['loaddate'].iloc[-1]
            })
        
        return pl.DataFrame(result)
    
    @staticmethod
    def transform_ships(ships: pl.DataFrame, run_timestamp: str) -> Tuple[pl.DataFrame, Dict[str, pl.DataFrame]]:
        """Transforms ship data and extracts nested tables"""
        ships = ships.lazy().unnest("data")
        
        # Extract nested tables
        tables = {}
        for column, dtype in ships.collect_schema().items():
            if dtype == pl.List(pl.Struct):
                tables[column] = (
                    ships.select("imo", column)
                    .explode(column)
                    .unnest(column)
                    .with_columns(
                        pl.lit(run_timestamp).alias("loaddate")
                    )
                    .collect()
                )
            elif dtype == pl.List:
                tables[column] = (
                    ships.select("imo", column)
                    .explode(column)
                    .with_columns(
                        pl.lit(run_timestamp).alias("loaddate")
                    )
                    .collect()
                )

        # Keep only non-list columns in main table
        ships = ships.select(
            pl.exclude([col for col, dtype in ships.collect_schema().items() if dtype == pl.List])
        ).with_columns(
            pl.lit(run_timestamp).alias("loaddate")
        ).collect()

        return ships, tables
    
    @staticmethod
    def enrich_timeseries_with_friendly_names(timeseries_df: pl.DataFrame, signals_df: pl.DataFrame) -> pl.DataFrame:
        """
        Fügt friendly_name aus der Signaldatei zu den Timeseries-Daten hinzu
        """
        if len(timeseries_df) == 0 or len(signals_df) == 0:
            return timeseries_df
            
        # Extrahiere Signal-Mapping (signal -> friendly_name)
        signal_mapping = (
            signals_df
            .filter(pl.col("friendly_name").is_not_null())
            .select(["signal", "friendly_name"])
            .unique()
        )
        
        # Join mit Timeseries-Daten
        return timeseries_df.join(
            signal_mapping,
            on="signal",
            how="left"
        )
