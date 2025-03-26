import os
import json
import logging
from pathlib import Path
from typing import List, Dict, Union, defaultdict
import polars as pl

class Data_Storage:
    
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger('Data Storage')
        
    # Schreiben von Files in lokale Ordner    
    def write_file(self, data: Union[List, Dict, pl.DataFrame], filename: str, path: str, postfix: str) -> None:
        os.makedirs(path, exist_ok=True)
        full_path = f"{path}/{filename}.{postfix}"
        
        try:
            # Schreibt json files
            if postfix == 'json':
                with open(full_path, 'w') as f:
                    json.dump(data, f)
                self.logger.info(f"Writting to {filename}.json file successfully")

            # Schreibt parquet files
            elif postfix == 'parquet':

                # Check auf richtiges Input-Format
                if not isinstance(data, pl.DataFrame):
                    if isinstance(data, list) or isinstance(data, dict):
                        data = pl.DataFrame(data)
                    else:
                        raise ValueError("Data must be DataFrame, List, or Dict for parquet format")
                    
                data.write_parquet(full_path, compression="snappy")
                self.logger.info(f"Writting to {filename}.parquet file successfully")

            else:
                raise ValueError(f"Unsupported format: {postfix}")
                
            self.logger.info(f"Data saved to {full_path}")
        except Exception as e:
            self.logger.error(f"Failed to write file {full_path}: {str(e)}")
            raise


    def read_file(self, filename: str, path: str, postfix: str) -> pl.DataFrame:
        full_path = f"{path}/{filename}.{postfix}"
        
        try:
            if not os.path.exists(full_path):
                self.logger.info(f"File {full_path} does not exist")
                return pl.DataFrame()  # Leeres DataFrame zurückgeben
            
            else:
                if postfix == 'json':
                    with open(full_path, 'r') as f:
                        data = json.load(f)
                        data = pl.DataFrame(data)
                    self.logger.info(f"Reading from {filename}.json file successfully")
                    return data

                elif postfix == 'parquet':
                    data = pl.read_parquet(full_path)
                    self.logger.info(f"Reading from {filename}.parquet file successfully")
                    return data

                else:
                    raise ValueError(f"Unsupported format: {postfix}")
            
        except Exception as e:
            self.logger.error(f"Failed to read file {full_path}: {str(e)}")
            raise

        
    def find_timeseries_files(self, base_path: str, max_days: int = None, pattern: str = "Timeseries_*.parquet") -> defaultdict:
        base_dir = Path(base_path)
        if not base_dir.exists() or not base_dir.is_dir():
            self.logger.error(f"Directory {base_path} does not exist or is no directory")
            return defaultdict(list)

        try:
            # Optimiertes Dateisystem-Scannen mit einmaliger Tiefensuche
            files_by_imo = defaultdict(list)
            
            # Metadaten für Logging
            days_found = set()
            months_found = set()
            years_found = set()
            
            # Schnellere Dateisuche mit glob statt rekursivem Durchsuchen
            year_dirs = sorted([d for d in base_dir.iterdir() if d.is_dir()], key=lambda x: x.name, reverse=True)
            
            for year_dir in year_dirs:
                years_found.add(year_dir.name)
                
                month_dirs = sorted([d for d in year_dir.iterdir() if d.is_dir()], key=lambda x: x.name, reverse=True)
                for month_dir in month_dirs:
                    months_found.add(f"{year_dir.name}/{month_dir.name}")
                    
                    day_dirs = sorted([d for d in month_dir.iterdir() if d.is_dir()], key=lambda x: x.name, reverse=True)
                    
                    # Begrenzung auf max_days
                    if max_days is not None and len(days_found) >= max_days:
                        break
                        
                    for day_dir in day_dirs:
                        # Begrenzung auf max_days
                        if max_days is not None and len(days_found) >= max_days:
                            break
                            
                        days_found.add(f"{year_dir.name}/{month_dir.name}/{day_dir.name}")
                        
                        # Direktes Sammeln aller Dateien für diesen Tag mit glob
                        for file in day_dir.glob(pattern):
                            imo = file.stem.split("_")[1]  # Extrahiert <imo> aus "Timeseries_<imo>.parquet"
                            files_by_imo[imo].append(file)
            
            self.logger.info(f"{sum(len(files) for files in files_by_imo.values())} files found: "
                            f"{len(files_by_imo)} different ships, {len(days_found)} days, "
                            f"{len(months_found)} months, {len(years_found)} years")
            
            return files_by_imo  # Dictionary mit Listen von Dateien nach IMO
            
        except Exception as e:
            self.logger.error(f"Failed to get historical Data: {str(e)}")
            raise
    
    def find_timeseries_summaries(self, base_path: str,  pattern:str = "*.parquet") -> list:
        base_dir = Path(base_path)
        if not base_dir.exists() or not base_dir.is_dir():
            self.logger.error(f"Directory {base_path} does not exist or is no directory")
            return defaultdict(list)

        # Alles Files mit dem pattern finden, pattern kann Datum begrenzen z.B. 2025*.parquet = alle Dateien aus 2025
        try:
            
            files =  []
            files_found = 0

            for file in base_dir.rglob(pattern):
                files.append(file)
                files_found +=1

        except Exception as e:
            self.logger.error(f"Failed to get historical Data: {str(e)}")
            raise

        self.logger.info(f"{files_found} summary files found")
    
        return files
        
