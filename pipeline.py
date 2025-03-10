import logging
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import polars as pl
import sqlalchemy as sa

from config import Config
from api_client import APIClient
from data_processor import DataProcessor
from data_storage import DataStorage

logger = logging.getLogger("hoppe_etl_pipeline")

class Pipeline:
    """Main data pipeline class"""
    
    def __init__(self, config: Config, api_key: str):
        self.config = config
        self.api_client = APIClient(config.base_url, api_key, timeout=config.timeout)
        self.processor = DataProcessor()
        self.storage = DataStorage(config)
        
    def process_ship(self, imo: str, run_timestamp: str) -> None:
        """Processes data for a single ship"""
        try:
            # Get and process signals
            _, signals = self.api_client.get_data(f"fleet/{imo}/signals")
            if signals:
                signals_df = pl.DataFrame(signals)
                self.storage.write_file(
                    signals,
                    f"Signals_{imo}",
                    f"{self.config.raw_path}/{run_timestamp}",
                    'json'
                )
                signals_transformed = self.processor.transform_signals(signals_df, run_timestamp)
                self.storage.write_file(
                    signals_transformed,
                    f"Signals_{imo}",
                    f"{self.config.transformed_path}/{run_timestamp}",
                    'parquet'
                )

            # Get and process timeseries
            _, timeseries = self.api_client.get_data(f"fleet/{imo}/timeseries")
            if timeseries:
                ts_df = pl.DataFrame(timeseries)
                self.storage.write_file(
                    timeseries,
                    f"Timeseries_{imo}",
                    f"{self.config.raw_path}/{run_timestamp}",
                    'json'
                )
                # Transformieren und Lücken extrahieren
                ts_transformed, gaps = self.processor.transform_timeseries(ts_df, imo, run_timestamp)
                
                self.storage.write_file(
                    ts_transformed,
                    f"Timeseries_{imo}",
                    f"{self.config.transformed_path}/{run_timestamp}",
                    'parquet'
                )
                
                # Prozessiere und speichere Lücken-Daten, falls vorhanden
                if len(gaps) > 0:
                    processed_gaps = self.processor.process_gaps(gaps)
                    if len(processed_gaps) > 0:
                        self.storage.write_file(
                            processed_gaps,
                            f"Gaps_{imo}",
                            f"{self.config.gaps_path}/{run_timestamp}",
                            'parquet'
                        )
                
        except Exception as e:
            logger.error(f"Failed to process ship {imo}: {str(e)}")
            raise
            
    def run(self, mode: str = "all") -> str:
        """
        Runs the complete data pipeline
        
        Args:
            mode (str): 'all' for complete pipeline, 'timeseries' for only timeseries data,
                        'fleet' for only fleet and signals data
                        
        Returns:
            str: The run timestamp in the format 'YYYY/MM/DD/HH/MM'
        """
        run_start = datetime.now(timezone.utc)
        run_timestamp = run_start.strftime('%Y/%m/%d/%H/%M')
        
        try:
            # Initialize directories
            os.makedirs(f"{self.config.raw_path}/{run_timestamp}", exist_ok=True)
            os.makedirs(f"{self.config.transformed_path}/{run_timestamp}", exist_ok=True)
            os.makedirs(f"{self.config.gaps_path}/{run_timestamp}", exist_ok=True)
            
            # Get ship data
            if mode in ["all", "fleet"]:
                _, ships = self.api_client.get_data("fleet")
                if not ships:
                    raise ValueError("Failed to get ship data")
                    
                # Process ships
                imo_numbers = self.processor.get_imo_numbers(ships)
                
                # Store raw ship data
                self.storage.write_file(
                    ships,
                    'ShipData',
                    f"{self.config.raw_path}/{run_timestamp}",
                    'json'
                )
                
                # Transform and store ship data
                ships_df = pl.DataFrame(ships)
                ships_transformed, tables = self.processor.transform_ships(ships_df, run_timestamp)
                self.storage.write_file(
                    ships_transformed,
                    'ShipData',
                    f"{self.config.transformed_path}/{run_timestamp}",
                    'parquet'
                )
                
                # Process nested tables
                for name, table in tables.items():
                    self.storage.write_file(
                        table,
                        f"ShipData_{name}",
                        f"{self.config.transformed_path}/{run_timestamp}",
                        'parquet'
                    )
                
                # Process signals for all ships
                for imo in imo_numbers:
                    _, signals = self.api_client.get_data(f"fleet/{imo}/signals")
                    if signals:
                        signals_df = pl.DataFrame(signals)
                        self.storage.write_file(
                            signals,
                            f"Signals_{imo}",
                            f"{self.config.raw_path}/{run_timestamp}",
                            'json'
                        )
                        signals_transformed = self.processor.transform_signals(signals_df, run_timestamp)
                        self.storage.write_file(
                            signals_transformed,
                            f"Signals_{imo}",
                            f"{self.config.transformed_path}/{run_timestamp}",
                            'parquet'
                        )
            
            # Process timeseries data
            if mode in ["all", "timeseries"]:
                # Get ship data if not already loaded
                if mode == "timeseries":
                    _, ships = self.api_client.get_data("fleet")
                    if not ships:
                        raise ValueError("Failed to get ship data")
                    imo_numbers = self.processor.get_imo_numbers(ships)
                
                # Process individual ships in parallel
                with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                    # Lambda function to process only timeseries
                    def process_ship_timeseries(imo):
                        try:
                            # Get and process timeseries
                            _, timeseries = self.api_client.get_data(f"fleet/{imo}/timeseries")
                            if timeseries:
                                ts_df = pl.DataFrame(timeseries)
                                self.storage.write_file(
                                    timeseries,
                                    f"Timeseries_{imo}",
                                    f"{self.config.raw_path}/{run_timestamp}",
                                    'json'
                                )
                                # Transformieren und Lücken extrahieren
                                ts_transformed, gaps = self.processor.transform_timeseries(ts_df, imo, run_timestamp)
                                
                                # Combine with historical data (last 5 days)
                                historical_data = self.load_historical_timeseries(imo, self.config.history_days)
                                if len(historical_data) > 0:
                                    # Combine historical and new data
                                    combined = pl.concat([historical_data, ts_transformed])
                                    # Deduplicate
                                    ts_transformed = (
                                        combined
                                        .sort(by=["loaddate"], descending=True)
                                        .unique(subset=["imo", "signal", "signal_timestamp"], keep="first")
                                    )
                                
                                self.storage.write_file(
                                    ts_transformed,
                                    f"Timeseries_{imo}",
                                    f"{self.config.transformed_path}/{run_timestamp}",
                                    'parquet'
                                )
                                
                                # Prozessiere und speichere Lücken-Daten, falls vorhanden
                                if len(gaps) > 0:
                                    processed_gaps = self.processor.process_gaps(gaps)
                                    if len(processed_gaps) > 0:
                                        self.storage.write_file(
                                            processed_gaps,
                                            f"Gaps_{imo}",
                                            f"{self.config.gaps_path}/{run_timestamp}",
                                            'parquet'
                                        )
                        except Exception as e:
                            logger.error(f"Failed to process timeseries for ship {imo}: {str(e)}")
                    
                    # Execute in parallel
                    executor.map(process_ship_timeseries, imo_numbers)
            
            # Clean up old data
            self.storage.cleanup_old_data(self.config.raw_path, self.config.days_to_keep)
            self.storage.cleanup_old_data(self.config.transformed_path, self.config.days_to_keep)
            self.storage.cleanup_old_data(self.config.gaps_path, self.config.days_to_keep)
            
            logger.info(f"Pipeline run completed successfully in {datetime.now(timezone.utc) - run_start}")
            return run_timestamp
                
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            runtime = datetime.now(timezone.utc) - run_start
            logger.info(f"Pipeline completed in {runtime}")
            
    def run_timeseries_only(self) -> str:
        """Convenience method to run only the timeseries part of the pipeline"""
        return self.run(mode="timeseries")
        
    def run_fleet_only(self) -> str:
        """Convenience method to run only the fleet part of the pipeline"""
        return self.run(mode="fleet")
    
    def load_historical_timeseries(self, imo: str, days: int = 5) -> pl.DataFrame:
        """
        Lädt historische Timeseries-Daten für ein Schiff aus den letzten n Tagen
        """
        try:
            today = datetime.now()
            
            # Erstelle eine Liste von Pfaden für die letzten n Tage
            paths = []
            for i in range(days):
                check_date = today - timedelta(days=i)
                date_path = check_date.strftime('%Y/%m/%d')
                
                # Alle Unterordner des Tages durchsuchen (Stunden/Minuten)
                full_path = Path(f"{self.config.transformed_path}/{date_path}")
                
                if full_path.exists():
                    # Finde die letzten Runs des Tages
                    for hour_dir in sorted(full_path.glob("*"), reverse=True):
                        if hour_dir.is_dir():
                            for minute_dir in sorted(hour_dir.glob("*"), reverse=True):
                                if minute_dir.is_dir():
                                    file_path = minute_dir / f"Timeseries_{imo}.parquet"
                                    if file_path.exists():
                                        paths.append(str(file_path))
                                        # Nimm nur den letzten Run des Tages
                                        break
                            # Nimm nur die letzte Stunde des Tages
                            break
            
            # Load and combine data
            dfs = []
            for path in paths:
                try:
                    df = pl.read_parquet(path)
                    if len(df) > 0:
                        dfs.append(df)
                except Exception as e:
                    logger.warning(f"Failed to read {path}: {str(e)}")
            
            if not dfs:
                logger.info(f"No historical timeseries data found for ship {imo}")
                return pl.DataFrame()
        except Exception as e:
            logger.error(f"Failed to load historical timeseries for ship {imo}: {str(e)}")
            return pl.DataFrame()
        
    def get_all_signals(self, run_timestamp: str) -> pl.DataFrame:
        """
        Sammelt alle Signaldefinitionen aus den aktuellen Daten
        """
        try:
            signals_path = Path(f"{self.config.transformed_path}/{run_timestamp}")
            all_signals = []
            
            for file in signals_path.glob("Signals_*.parquet"):
                try:
                    df = pl.read_parquet(file)
                    if len(df) > 0:
                        # Extrahiere nur die relevanten Spalten für das Mapping
                        signals_mapping = df.select([
                            "signal", "friendly_name", "unit", "object_code", "name_code", "group_name", "sub_group"
                        ])
                        all_signals.append(signals_mapping)
                except Exception as e:
                    logger.warning(f"Failed to read {file}: {str(e)}")
            
            if not all_signals:
                logger.warning("No signal definitions found")
                return pl.DataFrame()
                
            # Combine all signal definitions and deduplicate
            return pl.concat(all_signals).unique(subset=["signal"], keep="first")
            
        except Exception as e:
            logger.error(f"Failed to collect signal definitions: {str(e)}")
            return pl.DataFrame()
        
    def process_and_store_to_db(self, engine: sa.Engine, run_timestamp: str) -> None:
        """
        Processes data and stores it to the SQL database
        
        This method:
        1. Loads timeseries data from the current run
        2. Enriches it with friendly names
        3. Writes the combined data to the SQL database using the specialized method
        4. Writes gaps data to a separate table
        """
        try:
            # 1. Load timeseries data from current run
            transformed_path = Path(f"{self.config.transformed_path}/{run_timestamp}")
            
            # Find all timeseries parquet files
            timeseries_files = list(transformed_path.glob("Timeseries_*.parquet"))
            if not timeseries_files:
                logger.warning(f"No timeseries files found in {run_timestamp}")
                return
            
            # 2. Load and combine all timeseries data
            ts_dfs = []
            for file in timeseries_files:
                try:
                    df = pl.read_parquet(file)
                    if len(df) > 0:
                        ts_dfs.append(df)
                except Exception as e:
                    logger.warning(f"Failed to read {file}: {str(e)}")
            
            if not ts_dfs:
                logger.warning("No data loaded from timeseries files")
                return
            
            combined_ts = pl.concat(ts_dfs)
            logger.info(f"Combined {len(ts_dfs)} timeseries files with {len(combined_ts)} rows total")
            
            # 3. Get all signal definitions for enrichment
            all_signals = self.get_all_signals(run_timestamp)
            
            # 4. Enrich timeseries data with friendly names
            if len(all_signals) > 0:
                combined_ts = self.processor.enrich_timeseries_with_friendly_names(combined_ts, all_signals)
                logger.info("Enriched timeseries data with friendly names")
            
            # 5. Write to database using the specialized method
            if len(combined_ts) > 0:
                self.storage.write_ts_to_msdb(combined_ts, engine)
                logger.info("Successfully processed and stored timeseries data to database")
            
            # 6. Process and store gaps data
            gaps_path = Path(f"{self.config.gaps_path}/{run_timestamp}")
            if gaps_path.exists():
                gaps_files = list(gaps_path.glob("Gaps_*.parquet"))
                gaps_dfs = []
                
                for file in gaps_files:
                    try:
                        df = pl.read_parquet(file)
                        if len(df) > 0:
                            gaps_dfs.append(df)
                    except Exception as e:
                        logger.warning(f"Failed to read gaps file {file}: {str(e)}")
                
                if gaps_dfs:
                    combined_gaps = pl.concat(gaps_dfs)
                    logger.info(f"Combined {len(gaps_dfs)} gaps files with {len(combined_gaps)} rows total")
                    
                    # Write gaps data to a separate table
                    self.storage.write_to_db(combined_gaps, engine, "TimeSeries_Gaps", if_exists="append")
                    logger.info("Successfully stored gaps data to database")
            
        except Exception as e:
            logger.error(f"Failed to process and store data to database: {str(e)}")
            raise
                
            # Combine all dataframes and keep only the latest value for each combination
            combined = pl.concat(dfs)
            
            # Dedupliziere die Daten - behalte nur den neuesten Eintrag für jede Kombination von imo, signal und signal_timestamp
            deduplicated = (
                combined
                .sort(by=["loaddate"], descending=True)
                .unique(subset=["imo", "signal", "signal_timestamp"], keep="first")
            )
            
            return deduplicated
            
        except Exception as e:
            logger.error(f"Failed to load historical timeseries for ship {imo}: {str(e)}")
            return pl.DataFrame()