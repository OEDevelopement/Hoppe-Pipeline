import polars as pl
from typing import List, Dict, Tuple


class Data_Processor:
    #logger = logging.getLogger("Data_Processor") ???

    @staticmethod
    def get_imo_numbers(data: List[dict]) -> List[str]:
        return [ship['imo'] for ship in data if ship.get('active', True)]
    
    @staticmethod
    def transform_shipdata(shipdata: pl.DataFrame, run_timestamp: str) -> Tuple[pl.DataFrame, Dict[str, pl.DataFrame]]:
        shipdata = shipdata.unnest("data")
        
        # Verschachtelte Tabellen extrahiren
        tables = {}
        for column, dtype in shipdata.collect_schema().items():
            if dtype == pl.List(pl.Struct):
                tables[column] = (
                    shipdata.select("imo", column)
                    .explode(column)
                    .unnest(column)
                    .with_columns(
                        pl.lit(run_timestamp).alias("loaddate")
                    )
                    
                )
            elif dtype == pl.List:
                tables[column] = (
                    shipdata.select("imo", column)
                    .explode(column)
                    .with_columns(
                        pl.lit(run_timestamp).alias("loaddate")
                    )
                    
                )

        # Schiffsdaten ohne Verschachtelung extrahieren
        shipdata = shipdata.select(
            pl.exclude([col for col, dtype in shipdata.collect_schema().items() if dtype == pl.List])
        ).with_columns(
            pl.lit(run_timestamp).alias("loaddate")
        )

        return shipdata, tables

    
    
    @staticmethod
    def transform_signals(signals: pl.DataFrame, run_timestamp: str) -> pl.DataFrame:
        if len(signals) == 0:
            return signals

        # Initiale Transformation    
        signals = (
            signals.unnest("signals")
            .unpivot(index="imo", variable_name="signal")
            .unnest("value")
        )

        # Verbleibende Verschachtelungen plätten
        for column, dtype in signals.collect_schema().items():
            if dtype == pl.Struct:
                signals = signals.unnest(column)

        # Null-Werte
        for column, dtype in signals.collect_schema().items():
            if dtype == pl.Null:
                signals = signals.with_columns(pl.col(column).cast(pl.String))
        
        # Das Lade-Datum hinzufügen
        signals = signals.with_columns(
            pl.lit(run_timestamp).alias("loaddate")
        )
                
        return signals
    

    @staticmethod
    def transform_timeseries(timeseries: pl.DataFrame, imo: str, run_timestamp: str) -> Tuple[pl.DataFrame, pl.DataFrame]:
        
        if len(timeseries) == 0:
            # Hier müssen wir sicherstellen, dass beide zurückgegebenen DataFrames dieselbe Struktur haben
            empty_df = pl.DataFrame({
                "signal": [], 
                "signal_timestamp": [], 
                "signal_value": [], 
                "imo": [], 
                "loaddate": []
            })
            return empty_df, empty_df.clone()
        
        # Optimierte Transformation
        transformed = (
            timeseries.drop("timestamp")
            .unpivot(
                index=[],
                variable_name="signal"
            )
            .unnest("value")
            .unpivot(
                index=["signal"],
                variable_name="signal_timestamp",
                value_name="signal_value"
            )
            .with_columns([
                pl.lit(imo).alias("imo"),
                pl.lit(run_timestamp).alias("loaddate")
            ])
        )
        
        # Lücken (NULL-Werte) identifizieren
        gaps = (
            transformed
            .filter(pl.col("signal_value").is_null())
            .select("imo", "signal", "signal_timestamp", "loaddate")
            .with_columns([
                pl.col("signal_timestamp").alias("gap_start")
            ])
        )
        
        # NULL-Werte aus dem Hauptdatensatz entfernen
        data = transformed.filter(pl.col("signal_value").is_not_null())
        
        return data, gaps
    
    @staticmethod
    def process_gaps(gaps_df: pl.DataFrame) -> pl.DataFrame:
        
        if len(gaps_df) == 0:
            return pl.DataFrame()
        
        # Stelle sicher, dass gap_start ein Datetime-Objekt ist
        if gaps_df["gap_start"].dtype != pl.Datetime:
            gaps_df = gaps_df.with_columns(
                pl.col("gap_start").cast(pl.Datetime)
            )
            
        result = []
        
        # Gruppieren nach IMO und Signal, sortiere nach Zeitstempel
        for (imo, signal), group in gaps_df.sort("gap_start").group_by(["imo", "signal"]):
            group_df = group.sort("gap_start")
            
            if len(group_df) <= 1:
                # Bei nur einem Eintrag
                result.append({
                    "imo": imo,
                    "signal": signal,
                    "gap_start": group_df["gap_start"][0],
                    "gap_end": group_df["gap_start"][0],
                    "loaddate": group_df["loaddate"][0]
                })
                continue
                
            current_start = group_df["gap_start"][0]
            prev_time = current_start
            
            for i in range(1, len(group_df)):
                curr_time = group_df["gap_start"][i]
                
                # Prüfe, ob mehr als 5 Minuten zwischen zwei Einträgen liegen
                max_sec = 5*60
                # Hier kommt es zum Fehler, wenn prev_time und curr_time Strings sind
                time_diff_seconds = (curr_time - prev_time).total_seconds()
                
                if time_diff_seconds > max_sec:
                    result.append({
                        "imo": imo,
                        "signal": signal,
                        "gap_start": current_start,
                        "gap_end": prev_time,
                        "loaddate": group_df["loaddate"][i]
                    })
                    current_start = curr_time  # Starte neue Lücke
                
                prev_time = curr_time  # Aktualisiere den vorherigen Zeitstempel
            
            # Letzte Lücke hinzufügen
            result.append({
                "imo": imo,
                "signal": signal,
                "gap_start": current_start,
                "gap_end": prev_time,
                "loaddate": group_df["loaddate"][-1]
            })
        
        return pl.DataFrame(result)
    
    @staticmethod
    def enrich_timeseries_with_friendly_names(timeseries_df: pl.DataFrame, signals_df: pl.DataFrame) -> pl.DataFrame:

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
    
    @staticmethod
    def update_daily_timeseries_summary(hist_df: pl.DataFrame, daily_df: pl.DataFrame, current_df: pl.DataFrame) -> pl.DataFrame:
        
        combined_df = pl.concat([hist_df, daily_df, current_df])
        # summary_df = combined_df.unique(subset=["imo", "signal_timestamp", "friendly_name"], keep="first").filter(pl.col("tag")=="new"| pl.col("tag")=="today")
        summary_df = combined_df.unique(subset=["imo", "signal_timestamp", "friendly_name"], keep="first").filter(pl.col("tag").is_in(["new", "today"]))

        return summary_df
