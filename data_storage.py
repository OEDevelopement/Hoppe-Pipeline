import logging
import os
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Union
import polars as pl
import sqlalchemy as sa
from sqlalchemy.sql import text

logger = logging.getLogger("hoppe_etl_pipeline")

class DataStorage:
    """Handles data storage operations"""
    
    def __init__(self, config):
        self.config = config

    def write_file(
        self, 
        data: Union[List, Dict, pl.DataFrame],
        filename: str,
        path: str,
        postfix: str
    ) -> None:
        """Writes data to file system"""
        os.makedirs(path, exist_ok=True)
        full_path = f"{path}/{filename}.{postfix}"
        
        try:
            if postfix == 'json':
                with open(full_path, 'w') as f:
                    json.dump(data, f)
            elif postfix == 'parquet':
                if not isinstance(data, pl.DataFrame):
                    if isinstance(data, list) or isinstance(data, dict):
                        data = pl.DataFrame(data)
                    else:
                        raise ValueError("Data must be DataFrame, List, or Dict for parquet format")
                data.write_parquet(full_path, compression="snappy")
            else:
                raise ValueError(f"Unsupported format: {postfix}")
                
            logger.info(f"Data saved to {full_path}")
        except Exception as e:
            logger.error(f"Failed to write file {full_path}: {str(e)}")
            raise

    def cleanup_old_data(self, base_path: str, days_to_keep: int = 90) -> None:
        """
        Löscht Daten, die älter als days_to_keep Tage sind
        """
        try:
            today = datetime.now()
            cutoff_date = today - timedelta(days=days_to_keep)
            
            # Wandle in Pfad-Format um (Jahr/Monat/Tag)
            cutoff_path = cutoff_date.strftime('%Y/%m/%d')
            base_path = Path(base_path)
            
            if not base_path.exists():
                return
                
            # Durchsuche alle Jahresordner
            for year_dir in base_path.glob("*"):
                if not year_dir.is_dir() or not year_dir.name.isdigit():
                    continue
                    
                year = int(year_dir.name)
                
                # Überspringe Ordner, die definitiv behalten werden sollen
                if year > cutoff_date.year:
                    continue
                
                # Behandle Jahre, die teilweise gelöscht werden müssen
                if year == cutoff_date.year:
                    for month_dir in year_dir.glob("*"):
                        if not month_dir.is_dir() or not month_dir.name.isdigit():
                            continue
                            
                        month = int(month_dir.name)
                        
                        # Überspringe Monate, die definitiv behalten werden sollen
                        if month > cutoff_date.month:
                            continue
                            
                        # Behandle Monate, die teilweise gelöscht werden müssen
                        if month == cutoff_date.month:
                            for day_dir in month_dir.glob("*"):
                                if not day_dir.is_dir() or not day_dir.name.isdigit():
                                    continue
                                    
                                day = int(day_dir.name)
                                
                                # Lösche Tage, die älter als der Cutoff sind
                                if day < cutoff_date.day:
                                    logger.info(f"Removing old data directory: {day_dir}")
                                    # In Produktion wäre hier tatsächliches Löschen (shutil.rmtree)
                                    # Für Sicherheit vorerst nur Logging
                                    # import shutil
                                    # shutil.rmtree(day_dir)
                        
                        # Lösche den gesamten Monat, wenn er älter als der Cutoff-Monat ist
                        elif month < cutoff_date.month:
                            logger.info(f"Removing old data directory: {month_dir}")
                            # import shutil
                            # shutil.rmtree(month_dir)
                
                # Lösche das gesamte Jahr, wenn es älter als das Cutoff-Jahr ist
                elif year < cutoff_date.year:
                    logger.info(f"Removing old data directory: {year_dir}")
                    # import shutil
                    # shutil.rmtree(year_dir)
                    
            logger.info(f"Cleanup of data older than {cutoff_date.strftime('%Y-%m-%d')} completed")
                
        except Exception as e:
            logger.error(f"Error during data cleanup: {str(e)}")

    def write_to_db(
        self,
        df: pl.DataFrame,
        engine: sa.Engine,
        table_name: str,
        if_exists: str = "replace",
        batch_size: int = 10000
    ) -> None:
        """Writes DataFrame to database in batches"""
        try:
            # Convert to pandas for writing to database in batches
            pdf = df.to_pandas()
            total_rows = len(pdf)
            
            if total_rows == 0:
                logger.warning(f"No data to write to table {table_name}")
                return
                
            logger.info(f"Writing {total_rows} rows to table {table_name}")
            
            # Write in batches to avoid memory issues
            for i in range(0, total_rows, batch_size):
                end = min(i + batch_size, total_rows)
                batch = pdf.iloc[i:end]
                
                # For first batch, replace or append based on if_exists parameter
                if i == 0:
                    batch.to_sql(
                        table_name,
                        engine,
                        if_exists=if_exists,
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                else:
                    # For subsequent batches, always append
                    batch.to_sql(
                        table_name,
                        engine,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                    
                logger.info(f"Wrote batch {i//batch_size + 1} of {(total_rows-1)//batch_size + 1} to table {table_name}")
                
            logger.info(f"Data successfully written to table {table_name}")
        except Exception as e:
            logger.error(f"Database write failed: {str(e)}")
            raise
            
    def write_ts_to_msdb(
        self,
        df: pl.DataFrame, 
        engine: sa.Engine,
        batch_size: int = 10000
    ) -> None:
        """
        Writes time series data to MSSQL database using a staging table approach.
        
        This specialized method:
        1. Writes the dataframe to a staging table
        2. Ensures the main table exists with the correct schema
        3. Adds any missing columns to the main table
        4. Merges data from staging to the main table using MERGE statement
        
        Parameters:
            df (pl.DataFrame): Time series data to write
            engine (sa.Engine): SQLAlchemy engine for database connection
            batch_size (int): Number of rows to write in each batch
        """
        try:
            # If dataframe is empty, nothing to do
            if len(df) == 0:
                logger.warning("No time series data to write to database")
                return
            
            # Step 1: Write DataFrame to staging table
            self.write_to_db(df, engine, "TimeSeries_Staging", if_exists="replace", batch_size=batch_size)
            logger.info("Data written to TimeSeries_Staging table")
            
            # Step 2: Ensure main pivot table exists
            create_pivot_table_sql = """
            IF OBJECT_ID('TimeSeries_pivot', 'U') IS NULL
            BEGIN
                CREATE TABLE TimeSeries_pivot (
                    imo NVARCHAR(255) NOT NULL,
                    signal_timestamp DATETIME NOT NULL,
                    loaddate NVARCHAR(255),
                    PRIMARY KEY (imo, signal_timestamp)
                );
            END
            """
            
            with engine.connect() as conn:
                conn.execute(text(create_pivot_table_sql))
                conn.commit()
                logger.info("Ensured TimeSeries_pivot table exists")
            
            # Step 3: Add missing columns to main table
            add_columns_sql = """
            DECLARE @column_name NVARCHAR(255)
            DECLARE @sql NVARCHAR(MAX)

            DECLARE column_cursor CURSOR FOR
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'TimeSeries_Staging'
            AND COLUMN_NAME NOT IN (SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TimeSeries_pivot')
            AND COLUMN_NAME NOT IN ('imo', 'signal_timestamp', 'loaddate')  -- Skip key columns and loaddate

            OPEN column_cursor
            FETCH NEXT FROM column_cursor INTO @column_name

            WHILE @@FETCH_STATUS = 0
            BEGIN
                SET @sql = 'ALTER TABLE TimeSeries_pivot ADD [' + @column_name + '] FLOAT NULL'
                EXEC sp_executesql @sql
                FETCH NEXT FROM column_cursor INTO @column_name
            END

            CLOSE column_cursor
            DEALLOCATE column_cursor
            """

            with engine.connect() as conn:
                conn.execute(text(add_columns_sql))
                conn.commit()
                logger.info("Added any missing columns to TimeSeries_pivot table")
            
            # Step 4: Pivotieren der Daten in der Staging-Tabelle
            # Dieses SQL transformiert die Daten von der Staging-Tabelle, wo sie im Format
            # imo, signal, signal_timestamp, signal_value sind, in das Pivot-Format
            pivot_staging_sql = """
            -- Create a temporary table to hold pivoted data
            IF OBJECT_ID('tempdb..#TempPivot', 'U') IS NOT NULL
                DROP TABLE #TempPivot;

            -- Get the list of unique signals for dynamic pivot
            DECLARE @columns NVARCHAR(MAX);
            DECLARE @sql NVARCHAR(MAX);

            -- Create a list of signals as columns for the PIVOT operation
            SELECT @columns = STRING_AGG(QUOTENAME(signal), ',')
            FROM (SELECT DISTINCT signal FROM TimeSeries_Staging) AS signals;

            -- Prepare the dynamic SQL for pivoting
            SET @sql = N'
            SELECT imo, signal_timestamp, loaddate, ' + @columns + '
            INTO #TempPivot
            FROM (
                SELECT imo, signal, signal_timestamp, signal_value, loaddate
                FROM TimeSeries_Staging
            ) AS src
            PIVOT (
                MAX(signal_value)
                FOR signal IN (' + @columns + ')
            ) AS pvt;
            ';

            -- Execute the dynamic SQL to create the temporary pivot table
            EXEC sp_executesql @sql;

            -- Create a MERGE statement to update the main table
            SET @sql = N'
            MERGE INTO TimeSeries_pivot AS target
            USING #TempPivot AS source
            ON target.imo = source.imo AND target.signal_timestamp = source.signal_timestamp
            WHEN MATCHED THEN
                UPDATE SET 
                    loaddate = source.loaddate' +
                    -- Add column updates from temp table, excluding key columns
                    (SELECT STRING_AGG(', ' + QUOTENAME(COLUMN_NAME) + ' = source.' + QUOTENAME(COLUMN_NAME), '')
                     FROM INFORMATION_SCHEMA.COLUMNS
                     WHERE TABLE_NAME = 'TimeSeries_pivot'
                     AND COLUMN_NAME NOT IN ('imo', 'signal_timestamp', 'loaddate')) + '
            WHEN NOT MATCHED THEN
                INSERT (imo, signal_timestamp, loaddate' +
                    -- Add columns from temp table, excluding key columns
                    (SELECT STRING_AGG(', ' + QUOTENAME(COLUMN_NAME), '')
                     FROM INFORMATION_SCHEMA.COLUMNS
                     WHERE TABLE_NAME = 'TimeSeries_pivot'
                     AND COLUMN_NAME NOT IN ('imo', 'signal_timestamp', 'loaddate')) + ')
                VALUES (source.imo, source.signal_timestamp, source.loaddate' +
                    -- Add values from temp table, excluding key columns
                    (SELECT STRING_AGG(', source.' + QUOTENAME(COLUMN_NAME), '')
                     FROM INFORMATION_SCHEMA.COLUMNS
                     WHERE TABLE_NAME = 'TimeSeries_pivot'
                     AND COLUMN_NAME NOT IN ('imo', 'signal_timestamp', 'loaddate')) + ');';

            -- Execute the merge
            EXEC sp_executesql @sql;

            -- Clean up
            DROP TABLE #TempPivot;
            """
            
            with engine.connect() as conn:
                conn.execute(text(pivot_staging_sql))
                conn.commit()
                logger.info("Data merged into TimeSeries_pivot table")
                
            # Optional: Schreibe Gaps-Daten in separate Tabelle
            gaps_table_sql = """
            IF OBJECT_ID('TimeSeries_Gaps', 'U') IS NULL
            BEGIN
                CREATE TABLE TimeSeries_Gaps (
                    imo NVARCHAR(255) NOT NULL,
                    signal NVARCHAR(255) NOT NULL,
                    gap_start DATETIME NOT NULL,
                    gap_end DATETIME NOT NULL,
                    loaddate NVARCHAR(255),
                    PRIMARY KEY (imo, signal, gap_start)
                );
            END
            """
            
            with engine.connect() as conn:
                conn.execute(text(gaps_table_sql))
                conn.commit()
                logger.info("Ensured TimeSeries_Gaps table exists")
                
        except Exception as e:
            logger.error(f"MSSQL database operation failed: {str(e)}")
            raise
