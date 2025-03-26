import os
import logging
from datetime import datetime, timedelta
from typing import List
from concurrent.futures import ThreadPoolExecutor
from api_client import API_Client
from data_storage import Data_Storage
from data_processor import Data_Processor
from config import Config
import polars as pl

class Pipeline:
    def __init__(self, config: Config, api_key: str, verify_ssl: bool = True):
        self.config = config
        self.api_client = API_Client(config.base_url, api_key)
        self.processor = Data_Processor()
        self.storage = Data_Storage(config)
        self.logger = logging.getLogger('Pipeline')
    
    def process_shipdata(self, run_timestamp: str):
        try:

            self.logger.info("Processing ship data")

            # Get Shipdata
            response, shipdata = self.api_client.get_data("fleet")

            # Get & Store IMO Numbers
            imo_numbers = self.processor.get_imo_numbers(shipdata)
            self.storage.write_file(
                imo_numbers,
                'imos',
                f"./data/latest",
                'parquet'
            )

            self.logger.info(f"Found {len(imo_numbers)} active ships")

            if not shipdata:
                self.logger.error(f"No ship data received - {response}")
                raise ValueError(f"No ship data received - {response}")
            
            # Store raw ship data
            self.storage.write_file(
                    shipdata,
                    'ShipData',
                    f"{self.config.raw_path}/{run_timestamp}",
                    'json'
                )
            
            # Transform and store ship data
            ships_df = pl.DataFrame(shipdata)
            if ships_df.columns == ['detail']:
                self.logger.info("Request Error: see json file for details")
            
            else:
                ships_transformed, tables = self.processor.transform_shipdata(ships_df, run_timestamp)
            
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
            
        except Exception as e:
            self.logger.error(f"Failed to process ship data: {str(e)}")


    def process_signals(self, run_timestamp: str, imo_numbers:List[str], current_signals_df: pl.DataFrame):
        try:
            
            for imo in imo_numbers:

                self.logger.info(f"Processing signals data for {imo}")

                response, signals = self.api_client.get_data(f"fleet/{imo}/signals")

                if not signals:
                    self.logger.error(f"No signals data received - {response}")
                    ValueError(f"No signals data received - {response}")

                # Store raw signals data
                self.storage.write_file(
                        signals,
                        f'Signals_{imo}',
                        f"{self.config.raw_path}/{run_timestamp}",
                        'json'
                    )
                
                # Transform and store signals data
                signals_df = pl.DataFrame(signals)
                if signals_df.columns == ['detail']:
                    self.logger.info("Request Error: see json file for details")
                else:
                    signals_transformed = self.processor.transform_signals(signals_df, run_timestamp)
                    self.storage.write_file(
                            signals_transformed,
                            f'Signals_{imo}',
                            f"{self.config.transformed_path}/{run_timestamp}",
                            'parquet'
                        )
                    self.logger.info(f"Signals data processed for {imo}")
                
                    # Update Signals Mapping

                    new_signal_mapping = signals_transformed.select(["imo", "signal", "friendly_name"]).unique()

                    if current_signals_df is None:
                        self.storage.write_file(
                            new_signal_mapping,
                            'signal_mapping',
                            f"./data/latest",
                            'parquet'
                        )
                        self.logger.info(f"Signal Mapping updated for the first time")
                    else:        
                        # Add new Signals to existing Signal Mapping
                        
                        updated_signals = pl.concat([current_signals_df, new_signal_mapping]).unique(subset=["imo", "signal", "friendly_name"], keep="first")
                        
                        self.storage.write_file(
                                updated_signals,
                                "signal_mapping",
                                f"./data/latest",
                                "parquet")
                        self.logger.info(f"Signal Mapping updated")    
                
        except Exception as e:
            self.logger.error(f"Failed to process signals data: {str(e)}")

    def process_timeseries(self, run_timestamp: str, imo_numbers: List[str], signal_mapping: pl.DataFrame, current_df: pl.DataFrame, run_start) -> pl.DataFrame:
        try:
            self.logger.info(f"Starting parallel processing of timeseries data for {len(imo_numbers)} ships")
            result_df = current_df
            
            def process_per_imo(imo):
                try:
                    self.logger.info(f"Processing timeseries data for {imo}")
                    response, timeseries = self.api_client.get_data(f"fleet/{imo}/timeseries")
                    
                    # Store raw timeseries data
                    self.storage.write_file(
                        timeseries,
                        f'Timeseries_{imo}',
                        f"{self.config.raw_path}/{run_timestamp}",
                        'json'
                    )
                    
                    # Transform and store timeseries data
                    timeseries_df = pl.DataFrame(timeseries)
                    
                    if timeseries_df.columns == ['detail']:
                        self.logger.info(f"Request Error for {imo}: see json file for details")
                        return pl.DataFrame()
                    
                    if not timeseries:
                        self.logger.error(f"No timeseries data received for {imo} - {response}")
                        timeseries_transformed = pl.DataFrame({
                            "signal": [], "signal_timestamp": [], "signal_value": [], 
                            "imo": [], "loaddate": [], "friendly_name": []
                        })
                        gaps = pl.DataFrame()
                    else:
                        timeseries_transformed, gaps = self.processor.transform_timeseries(timeseries_df, imo, run_timestamp)
                    
                    # Enrich with friendly names
                    timeseries_transformed = self.processor.enrich_timeseries_with_friendly_names(
                        timeseries_transformed, signal_mapping
                    )
                    
                    self.storage.write_file(
                        timeseries_transformed,
                        f"Timeseries_{imo}",
                        f"{self.config.transformed_path}/{run_timestamp}",
                        'parquet'
                    )
                    
                    # Process gaps
                    gaps_df = self.processor.process_gaps(pl.DataFrame(gaps))
                    self.storage.write_file(
                        gaps_df,
                        f"Gaps_{imo}",
                        f"{self.config.gaps_path}/{run_timestamp}",
                        'parquet'
                    )
                    
                    return timeseries_transformed
                    
                except Exception as e:
                    self.logger.error(f"Failed to process timeseries for {imo}: {str(e)}")
                    return pl.DataFrame()
            
            # Process parallelized with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                results = list(executor.map(process_per_imo, imo_numbers))
            
            # Alle Ergebnisse kombinieren
            valid_results = [df for df in results if not df.is_empty()]
            if valid_results:
                result_df = pl.concat([result_df] + valid_results)
                
            return result_df
        
        except Exception as e:
            self.logger.error(f"Failed to process timeseries data: {str(e)}")
            return current_df
        
    def process_signals_in_batches(self, run_timestamp: str, imo_numbers: List[str], current_signals_df: pl.DataFrame):
        try:
            batch_size = min(self.config.batch_size, len(imo_numbers))
            total_batches = (len(imo_numbers) + batch_size - 1) // batch_size
            
            self.logger.info(f"Processing signals in {total_batches} batches with batch size {batch_size}")
            
            updated_signals = current_signals_df if current_signals_df is not None else pl.DataFrame()
            
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min(start_idx + batch_size, len(imo_numbers))
                batch_imos = imo_numbers[start_idx:end_idx]
                
                self.logger.info(f"Processing batch {batch_idx+1}/{total_batches} with {len(batch_imos)} ships")
                
                with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                    batch_results = list(executor.map(
                        lambda imo: self._process_single_ship_signals(imo, run_timestamp),
                        batch_imos
                    ))
                
                # Kombiniere alle Batch-Ergebnisse
                valid_results = [df for df in batch_results if not df.is_empty()]
                if valid_results:
                    batch_signals = pl.concat(valid_results)
                    
                    # Update signal mapping
                    new_signal_mapping = batch_signals.select(["imo", "signal", "friendly_name"]).unique()
                    
                    if updated_signals.is_empty():
                        updated_signals = new_signal_mapping
                    else:
                        updated_signals = pl.concat([updated_signals, new_signal_mapping]).unique(
                            subset=["imo", "signal", "friendly_name"], 
                            keep="first"
                        )
                    
            # Speichere das aktualisierte Signal-Mapping
            if not updated_signals.is_empty():
                self.storage.write_file(
                    updated_signals,
                    "signal_mapping",
                    f"./data/latest",
                    "parquet"
                )
                self.logger.info(f"Signal Mapping updated with {len(updated_signals)} records")
                
        except Exception as e:
            self.logger.error(f"Failed to process signals in batches: {str(e)}")
            
    def _process_single_ship_signals(self, imo: str, run_timestamp: str) -> pl.DataFrame:
        """Helper-Methode zur Verarbeitung von Signals für ein einzelnes Schiff"""
        try:
            self.logger.info(f"Processing signals data for {imo}")
            
            response, signals = self.api_client.get_data(f"fleet/{imo}/signals")
            
            if not signals:
                self.logger.error(f"No signals data received for {imo} - {response}")
                return pl.DataFrame()
                
            # Store raw signals data
            self.storage.write_file(
                signals,
                f'Signals_{imo}',
                f"{self.config.raw_path}/{run_timestamp}",
                'json'
            )
            
            # Transform and store signals data
            signals_df = pl.DataFrame(signals)
            if signals_df.columns == ['detail']:
                self.logger.info(f"Request Error for {imo}: see json file for details")
                return pl.DataFrame()
                
            signals_transformed = self.processor.transform_signals(signals_df, run_timestamp)
            
            self.storage.write_file(
                signals_transformed,
                f'Signals_{imo}',
                f"{self.config.transformed_path}/{run_timestamp}",
                'parquet'
            )
            
            self.logger.info(f"Signals data processed for {imo}")
            return signals_transformed
            
        except Exception as e:
            self.logger.error(f"Failed to process signals for {imo}: {str(e)}")
            return pl.DataFrame()


    def run(self, mode: str = "all"):

        try: 

            run_start = datetime.now()
            run_end = None
            run_timestamp = run_start.strftime('%Y/%m/%d/%H/%M')
            summary_filename = f"{run_start.strftime('%Y%m%d')}"
            self.logger.info(f"Starting pipeline run at {run_start}")
            cutoff_date_str = (run_start - timedelta(days=self.config.history_days)).strftime('%Y/%m/%d')

            # Initialize directories
            os.makedirs(f"{self.config.raw_path}/{run_timestamp}", exist_ok=True)
            os.makedirs(f"{self.config.transformed_path}/{run_timestamp}", exist_ok=True)
            os.makedirs(f"{self.config.gaps_path}/{run_timestamp}", exist_ok=True)
            os.makedirs(f"./data/latest", exist_ok=True)
            os.makedirs(f"./data/daily_summary", exist_ok=True)
            os.makedirs(f"./logs", exist_ok=True)

            empty_ts_schema = {"signal": pl.String, "signal_timestamp": pl.String, "signal_value": pl.Float64, "imo": pl.String, "loaddate": pl.String, "friendly_name": pl.String}
            empty_ts_schema_tags = {"signal": pl.String, "signal_timestamp": pl.String, "signal_value": pl.Float64, "imo": pl.String, "loaddate": pl.String, "friendly_name": pl.String, "tag": pl.String}
            current_signals_df = self.storage.read_file("signal_mapping", "./data/latest", "parquet")

            # Read daily and historical data
            daily_df = self.storage.read_file(summary_filename, "./data/daily_summary", "parquet")
            hist_df = self.storage.read_file("ref_data", "./data/latest", "parquet")
            if hist_df.is_empty(): hist_df = pl.DataFrame(schema=empty_ts_schema_tags)
            else: # Filter, wo die ersten 10 Zeichen (YYYY/MM/DD) größer als cutoff sind
                hist_df = hist_df.filter(pl.col("loaddate").str.slice(0, 10) > cutoff_date_str)

            if daily_df.is_empty(): # means it is a new day

                daily_df = pl.DataFrame(schema=empty_ts_schema_tags)
                
                last_day = (run_start - timedelta(days=1)).strftime('%Y%m%d')
                last_day_df = self.storage.read_file(last_day, "./data/daily_summary", "parquet")
                
                if last_day_df.is_empty(): 
                    last_day_df = pl.DataFrame(schema=empty_ts_schema_tags)
                else:
                    last_day_df = last_day_df.with_columns(pl.col("hist").alias("tag"))

                hist_df = pl.concat([hist_df, last_day_df])

                self.storage.write_file(hist_df, 
                                "ref_data", 
                                "./data/latest", 
                                "parquet")
                
            else: daily_df = daily_df.with_columns(pl.col("today").alias("tag"))

            current_df = pl.DataFrame(schema=empty_ts_schema)

            self.logger.info(f"Loaded {hist_df.shape[0]} historical records and {daily_df.shape[0]} daily records")
            
            self.logger.info(f"Processing data for mode: {mode}")

            if mode in ["all", "fleet"]:

                # Process Shipdata
                self.process_shipdata(run_timestamp)

                # Get IMO Numbers and turn them into list
                imo_numbers = self.storage.read_file(
                    'imos',
                    f"./data/latest",
                    'parquet'
                )            
                imo_numbers = imo_numbers.to_series(0).to_list()

                self.logger.info(f"Processing signals for {len(imo_numbers)} ships")

                # Process Signals
                self.process_signals_in_batches(run_timestamp, imo_numbers, current_signals_df)

                self.logger.info("All Signals processed")


            if mode in ["all", "timeseries"]:

                self.logger.info("Start to Process Timeseries Data")
                # Get IMO Numbers from file in data
                imo_numbers = self.storage.read_file(
                    'imos',
                    f"./data/latest",
                    'parquet'
                )
                imo_numbers = imo_numbers.to_series(0).to_list()

                # Get Signal-Mapping
                signal_mapping = self.storage.read_file(
                    'signal_mapping',
                    f"./data/latest",
                    'parquet'
                )                
                
                # Process Timeseries
                current_df = self.process_timeseries(run_timestamp, imo_numbers, signal_mapping, current_df, run_start)
                current_df = current_df.with_columns(pl.lit("new").alias("tag"))

                # Save Delta
                summary_df = self.processor.update_daily_timeseries_summary(hist_df, daily_df, current_df)
                self.storage.write_file(
                    summary_df.select(["imo", "signal_timestamp", "signal", "signal_value", "friendly_name", "loaddate"]),
                    summary_filename,
                    f"./data/daily_summary",
                    "parquet"
                )
            
            run_end = datetime.now()
        
        except Exception as e:
            run_end = datetime.now()
            self.logger.error(f"Pipeline run failed at {run_end}: {str(e)}") 
            raise

        

        return run_start, run_end