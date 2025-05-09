# Make sure these imports are at the top of your file
import os
import re
import sqlite3
import pandas as pd
import zipfile
import tempfile
import concurrent.futures
import time
import json
import duckdb

class lock_context:
    """Context manager for thread lock that does nothing if lock is None"""
    def __init__(self, lock):
        self.lock = lock
        
    def __enter__(self):
        if self.lock:
            self.lock.acquire()
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.lock:
            self.lock.release()

def db_execute(conn, query, params=None):
    """Execute database query with optional parameters"""
    cur = conn.cursor()
    if params:
        cur.execute(query, params)
    else:
        cur.execute(query)
    conn.commit()
    return cur

def setup_sqlite_optimizations(conn):
    """Apply performance optimizations to SQLite connection"""
    conn.execute('PRAGMA journal_mode = WAL')
    conn.execute('PRAGMA synchronous = NORMAL')
    conn.execute('PRAGMA cache_size = -32000')  # 32MB cache
    conn.execute('PRAGMA mmap_size = 30000000000')  # 30GB memory-mapped I/O
    conn.execute('PRAGMA temp_store = MEMORY')
    conn.execute('PRAGMA foreign_keys = ON')

def create_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS locations_metadata (
            id INTEGER PRIMARY KEY,
            station_name TEXT,
            state_province TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL,
            elevation REAL,
            timezone REAL,
            source_type TEXT,
            wmo_station_id TEXT,
            comment_1 TEXT,
            comment_2 TEXT,
            epw_file TEXT,
            catalog TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hourly_data (
            id INTEGER PRIMARY KEY,
            location_id SMALLINT,  
            hour_index SMALLINT, 
            Year SMALLINT,
            Month TINYINT, 
            Day TINYINT, 
            Hour TINYINT, 
            Minute TINYINT,
            DataFlags TEXT,
            DryBulb REAL, 
            DewPoint REAL, 
            RH REAL, 
            Pressure REAL,
            ExtraHorizontalRadiation REAL,
            ExtraDirNormalRadiation REAL,
            HorizontalIRSkyRadiation REAL,
            GlobalHorizontalRadiation REAL,
            DirectNormalRadiation REAL,
            DiffuseHorizontalRadiation REAL,
            GlobalHorizontalIlluminance REAL,
            DirectNormalIlluminance REAL,
            DiffuseHorizontalIlluminance REAL,
            ZenithLuminance REAL,
            WindDirection SMALLINT,
            WindSpeed REAL,
            TotalSkyCover TINYINT,
            OpaqueSkyCover TINYINT,
            Visibility REAL,
            CeilingHeight REAL,
            PresentWeatherObservation SMALLINT, 
            PresentWeatherCodes SMALLINT,  
            PrecipitableWater REAL,
            AerosolOpticalDepth REAL,
            SnowDepth REAL,
            DaysSinceLastSnowfall TINYINT,   
            Albedo REAL,
            LiquidPrecipitationDepth REAL,
            LiquidPrecipitationQuantity REAL,
            FOREIGN KEY(location_id) REFERENCES locations_metadata(id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS design_days (
            id INTEGER PRIMARY KEY,
            location_id INTEGER,
            name TEXT,
            month INTEGER,
            day INTEGER,
            max_dry_bulb REAL,
            humidity_value REAL,
            humidity_type TEXT,
            wind_speed REAL,
            wind_direction REAL,
            FOREIGN KEY(location_id) REFERENCES locations_metadata(id)
        )
    """)
    # Add indexes only for columns you'll be querying frequently
    cur.execute("CREATE INDEX IF NOT EXISTS idx_weather_location_date ON hourly_data(location_id, Year, Month, Day)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_weather_temp ON hourly_data(DryBulb)")
    
    # Add new tables for EPW metadata
    cur.execute("""
        CREATE TABLE IF NOT EXISTS design_conditions_data (
            id INTEGER PRIMARY KEY,
            location_id INTEGER,
            raw_data TEXT,
            FOREIGN KEY(location_id) REFERENCES locations_metadata(id)
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS typical_periods_data (
            id INTEGER PRIMARY KEY,
            location_id INTEGER,
            period_type TEXT,
            period_name TEXT,
            period_start_month INTEGER,
            period_start_day INTEGER,
            period_end_month INTEGER,
            period_end_day INTEGER,
            FOREIGN KEY(location_id) REFERENCES locations_metadata(id)
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ground_temperatures_data (
            id INTEGER PRIMARY KEY,
            location_id INTEGER,
            depth REAL,
            soil_conductivity REAL,
            soil_density REAL,
            soil_specific_heat REAL,
            january REAL,
            february REAL,
            march REAL,
            april REAL,
            may REAL,
            june REAL,
            july REAL,
            august REAL,
            september REAL,
            october REAL,
            november REAL,
            december REAL,
            FOREIGN KEY(location_id) REFERENCES locations_metadata(id)
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS holidays_dst_data (
            id INTEGER PRIMARY KEY,
            location_id INTEGER,
            uses_holidays TEXT,
            dst_start_day INTEGER,
            dst_end_day INTEGER,
            dst_indicator INTEGER,
            FOREIGN KEY(location_id) REFERENCES locations_metadata(id)
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS data_periods (
            id INTEGER PRIMARY KEY,
            location_id INTEGER,
            num_periods INTEGER,
            intervals_per_hour INTEGER,
            period_type TEXT,
            period_name TEXT,
            period_start_month INTEGER,
            period_start_day INTEGER,
            period_end_month INTEGER,
            period_end_day INTEGER,
            FOREIGN KEY(location_id) REFERENCES locations_metadata(id)
        )
    """)
    
    conn.commit()

def parse_epw_header(lines):
    fields = lines[0].split(',')
    
    # Find the data section start
    data_start_idx = None
    for i, line in enumerate(lines):
        if line.strip().startswith("DATA PERIODS,"):
            data_start_idx = i + 1
            break
    
    # If data_start_idx is still None, use a default value
    if data_start_idx is None:
        data_start_idx = 8  # Default assumption for EPW files
    
    return {
        "station_name": fields[1].strip(),  # Added strip() to remove whitespace
        "state_province": fields[2].strip(),
        "country": fields[3].strip(),
        "latitude": float(fields[6]) if fields[6].strip() else 0.0,
        "longitude": float(fields[7]) if fields[7].strip() else 0.0,
        "elevation": float(fields[9]) if fields[9].strip() else 0.0,
        "timezone": float(fields[8]) if fields[8].strip() else 0.0,
        "source_type": fields[4].strip(),
        "wmo_station_id": fields[5].strip(), 
        "comment_1": lines[5].strip().split(',', 1)[-1] if len(lines) > 5 else "",
        "comment_2": lines[6].strip().split(',', 1)[-1] if len(lines) > 6 else ""
    }, data_start_idx

def extract_epw_from_zip(zip_path, tmpdir):
    """Extract EPW file from a zip archive to a temporary directory"""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # List all files in the archive
            file_list = zip_ref.namelist()
            
            # Find EPW files
            epw_files = [f for f in file_list if f.lower().endswith('.epw')]
            
            if not epw_files:
                print(f"No EPW file found in {zip_path}")
                return None, None
                
            # Extract the first EPW file found
            epw_filename = epw_files[0]
            zip_ref.extract(epw_filename, tmpdir)
            
            epw_path = os.path.join(tmpdir, epw_filename)
            return epw_path, epw_filename
    except Exception as e:
        print(f"Error extracting EPW from {zip_path}: {e}")
        return None, None

def create_hourly_dataframe(data_lines):
    """Create a pandas DataFrame from EPW data lines"""
    # Column names for the DataFrame
    columns = [
        'Year', 'Month', 'Day', 'Hour', 'Minute', 'DataFlags',
        'DryBulb', 'DewPoint', 'RH', 'Pressure',
        'ExtraHorizontalRadiation', 'ExtraDirNormalRadiation', 'HorizontalIRSkyRadiation',
        'GlobalHorizontalRadiation', 'DirectNormalRadiation', 'DiffuseHorizontalRadiation',
        'GlobalHorizontalIlluminance', 'DirectNormalIlluminance', 'DiffuseHorizontalIlluminance',
        'ZenithLuminance', 'WindDirection', 'WindSpeed', 'TotalSkyCover', 'OpaqueSkyCover',
        'Visibility', 'CeilingHeight', 'PresentWeatherObservation', 'PresentWeatherCodes',
        'PrecipitableWater', 'AerosolOpticalDepth', 'SnowDepth', 'DaysSinceLastSnowfall',
        'Albedo', 'LiquidPrecipitationDepth', 'LiquidPrecipitationQuantity'
    ]
    
    # Parse data lines into rows
    rows = []
    for i, line in enumerate(data_lines):
        parts = line.strip().split(',')
        # Clean and convert data
        row = []
        for j, part in enumerate(parts):
            if j < len(columns):
                # Try to convert to numeric if appropriate
                if j not in [5]:  # Skip DataFlags column
                    try:
                        # Check for missing value markers
                        if part.strip() in ['*', '**', '***', '****', '*****', '******', '?', '??', 'undefined']:
                            value = None  # Use None for missing values
                        else:
                            value = float(part) if part.strip() else 0
                            # Convert to int if it's one of the integer columns
                            if j < 5 or j in [20, 22, 23, 26, 27, 31]:
                                value = int(value)
                    except ValueError:
                        value = None  # Use None for values that can't be converted
                else:
                    value = part
                row.append(value)
        
        # Add hour_index starting from 1
        row_dict = {col: val for col, val in zip(columns, row)}
        row_dict['hour_index'] = i + 1
        rows.append(row_dict)
    
    # Create DataFrame
    df = pd.DataFrame(rows)
    return df

def insert_location(conn, meta, epw_filename):
    # Extract catalog from epw_filename using regex to get only the final underscore-separated component
    catalog_pattern = r"((TMY|TRY|CWEC).*)(\.epw)$"
    catalog_match = re.search(catalog_pattern, epw_filename)
    if catalog_match:
        catalog = catalog_match.group(1)  
    else:
        catalog = "unknown"
        raise ValueError(f"Catalog not found in filename: {epw_filename}")
    
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO locations_metadata
        (station_name, state_province, country, latitude, longitude, elevation, 
        timezone, source_type, wmo_station_id, comment_1, comment_2, epw_file, catalog)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, tuple(meta.values()) + (epw_filename, catalog))
    conn.commit()
    return cur.lastrowid

def insert_hourly_data(conn, location_id, df):
    df["location_id"] = location_id
    df.to_sql("hourly_data", conn, if_exists="append", index=False)

def insert_design_days(conn, location_id, blocks):
    cur = conn.cursor()
    for b in blocks:
        cur.execute("""
            INSERT INTO design_days (location_id, name, month, day, max_dry_bulb,
            humidity_value, humidity_type, wind_speed, wind_direction)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (location_id, *b))
    conn.commit()

def extract_design_days(epw_df):
    df = epw_df.copy()
    df["Date"] = pd.to_datetime(df[["Year", "Month", "Day"]])
    daily = df.groupby("Date").agg({
        "DryBulb": ["min", "max", "mean"],
        "RH": "mean"
    })
    daily.columns = ["MinTemp", "MaxTemp", "MeanTemp", "MeanRH"]
    daily = daily.reset_index()

    def percentile_day(col, pct, asc=True):
        sorted_df = daily.sort_values(col, ascending=asc)
        idx = int(len(sorted_df) * (pct / 100))
        return sorted_df.iloc[idx]

    blocks = []
    heating_996 = percentile_day("MinTemp", 0.4, True)
    blocks.append(("Heating 99.6%", heating_996.Date.month, heating_996.Date.day, heating_996.MinTemp, 0.001, "HumidityRatio", 2.5, 270))
    heating_99 = percentile_day("MinTemp", 1.0, True)
    blocks.append(("Heating 99%", heating_99.Date.month, heating_99.Date.day, heating_99.MinTemp, 0.001, "HumidityRatio", 2.5, 270))
    cooling_04 = percentile_day("MaxTemp", 99.6, False)
    blocks.append(("Cooling 0.4%", cooling_04.Date.month, cooling_04.Date.day, cooling_04.MaxTemp, 21.0, "WetBulb", 2.5, 270))
    cooling_1 = percentile_day("MaxTemp", 99.0, False)
    blocks.append(("Cooling 1%", cooling_1.Date.month, cooling_1.Date.day, cooling_1.MaxTemp, 21.0, "WetBulb", 2.5, 270))
    daily["HumidIndex"] = daily["MaxTemp"] * (daily["MeanRH"] / 100)
    humid = daily.loc[daily.HumidIndex.idxmax()]
    blocks.append(("Cooling Humid", humid.Date.month, humid.Date.day, humid.MaxTemp, 24.0, "WetBulb", 2.5, 270))
    jan = daily[daily.Date.dt.month == 1].iloc[0]
    blocks.append(("Clear Winter", jan.Date.month, jan.Date.day, jan.MaxTemp, 0.001, "HumidityRatio", 2.5, 270))
    jul = daily[daily.Date.dt.month == 7].iloc[0]
    blocks.append(("Clear Summer", jul.Date.month, jul.Date.day, jul.MaxTemp, 21.0, "WetBulb", 2.5, 270))
    return blocks

def parse_typical_periods(line, conn, location_id):
    if not line.startswith("TYPICAL/EXTREME PERIODS"):
        return
    
    parts = line.split(',')
    if len(parts) < 3:
        return
    
    num_periods = int(parts[1])
    
    cur = conn.cursor()
    
    # The format is different than what you expected
    # Each period has 4 elements: name, type, start_date, end_date
    current_idx = 2  # Start after "TYPICAL/EXTREME PERIODS,6,"
    
    for i in range(num_periods):
        # Check if we have enough parts left
        if current_idx + 3 >= len(parts):
            break
            
        # Extract the 4 components for each period
        period_name = parts[current_idx].strip()
        period_type = parts[current_idx + 1].strip()
        start_date = parts[current_idx + 2].strip()
        end_date = parts[current_idx + 3].strip()
        
        # Move to next period
        current_idx += 4
        
        # Parse dates (format is "M/D")
        try:
            # Split the date strings and convert to integers
            start_parts = start_date.split('/')
            end_parts = end_date.split('/')
            
            if len(start_parts) == 2 and len(end_parts) == 2:
                start_month = int(start_parts[0])
                start_day = int(start_parts[1])
                end_month = int(end_parts[0])
                end_day = int(end_parts[1])
                
                # Insert into the database
                cur.execute("""
                    INSERT INTO typical_periods_data 
                    (location_id, period_type, period_name, 
                    period_start_month, period_start_day, period_end_month, period_end_day)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (location_id, period_type, period_name, 
                     start_month, start_day, end_month, end_day))
            else:
                print(f"Invalid date format for period '{period_name}': {start_date} - {end_date}")
        except Exception as e:
            print(f"Error parsing typical period '{period_name}' with dates {start_date} - {end_date}: {e}")
    
    conn.commit()

def parse_ground_temps(line, conn, location_id):
    if not line.startswith("GROUND TEMPERATURES"):
        return
    
    parts = line.split(',')
    if len(parts) < 3:
        return
    
    num_depths = int(parts[1])
    
    cur = conn.cursor()
    
    # Process each depth
    for i in range(num_depths):
        base_idx = 2 + (i * 16)
        
        if base_idx >= len(parts):
            break
            
        try:
            depth = float(parts[base_idx])
            soil_conductivity = float(parts[base_idx + 1]) if parts[base_idx + 1].strip() else 0.0
            soil_density = float(parts[base_idx + 2]) if parts[base_idx + 2].strip() else 0.0
            soil_specific_heat = float(parts[base_idx + 3]) if parts[base_idx + 3].strip() else 0.0
            
            # Get the 12 monthly values
            monthly_temps = []
            for j in range(12):
                monthly_temps.append(float(parts[base_idx + 4 + j]) if parts[base_idx + 4 + j].strip() else 0.0)
            
            cur.execute("""
                INSERT INTO ground_temperatures_data 
                (location_id, depth, soil_conductivity, soil_density, soil_specific_heat,
                january, february, march, april, may, june, july, august, 
                september, october, november, december)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (location_id, depth, soil_conductivity, soil_density, soil_specific_heat,
                *monthly_temps))
        except Exception as e:
            print(f"Error parsing ground temperature data for depth {parts[base_idx]}: {e}")
    
    conn.commit()

def parse_holidays_dst(line, conn, location_id):
    if not line.startswith("HOLIDAYS/DAYLIGHT SAVINGS"):
        return
    
    parts = line.split(',')
    if len(parts) < 5:
        return
    
    uses_holidays = parts[1]
    dst_start_day = int(parts[2]) if parts[2].strip() else 0
    dst_end_day = int(parts[3]) if parts[3].strip() else 0
    dst_indicator = int(parts[4]) if parts[4].strip() else 0
    
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO holidays_dst_data 
        (location_id, uses_holidays, dst_start_day, dst_end_day, dst_indicator)
        VALUES (?, ?, ?, ?, ?)
    """, (location_id, uses_holidays, dst_start_day, dst_end_day, dst_indicator))
    conn.commit()

def parse_data_periods(line, conn, location_id):
    if not line.startswith("DATA PERIODS"):
        return
    
    parts = line.split(',')
    if len(parts) < 6:
        print(f"Not enough parts in DATA PERIODS line: {len(parts)}")
        return
    
    try:
        num_periods = int(parts[1])
        intervals_per_hour = int(parts[2])
        period_type = parts[3].strip()
        day_of_week = parts[4].strip()  # This is the period name (day of week)
        
        # Parse start date (format is typically "M/ D" or "M/D")
        start_date = parts[5].strip()
        start_parts = start_date.split('/')
        start_month = int(start_parts[0])
        # Handle case where there might be a space after slash
        start_day = int(start_parts[1].strip())
        
        # Parse end date (format is typically "M/ D" or "M/D")
        end_date = parts[6].strip()
        end_parts = end_date.split('/')
        end_month = int(end_parts[0])
        # Handle case where there might be a space after slash
        end_day = int(end_parts[1].strip())
        
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO data_periods
            (location_id, num_periods, intervals_per_hour, period_type, period_name, 
             period_start_month, period_start_day, period_end_month, period_end_day)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (location_id, num_periods, intervals_per_hour, period_type, day_of_week, 
              start_month, start_day, end_month, end_day))
        conn.commit()
    except Exception as e:
        print(f"Error parsing DATA PERIODS line: {e}")
        print(f"Line content: {line}")
        print(f"Parts: {parts}")

def parse_epw_metadata(lines, conn, location_id):
    """Parse all EPW header metadata in a single function"""
    metadata_handlers = {
        "DESIGN CONDITIONS,": lambda line, conn, loc_id: parse_line_to_table(
            line, conn, loc_id, "design_conditions_data", 
            ["location_id", "raw_data"], 
            lambda parts: [loc_id, ','.join(parts[1:])]),
        "TYPICAL/EXTREME PERIODS,": parse_typical_periods,
        "GROUND TEMPERATURES,": parse_ground_temps,
        "HOLIDAYS/DAYLIGHT SAVINGS,": parse_holidays_dst,
        "DATA PERIODS,": parse_data_periods
    }
    
    for line in lines[:20]:  # Check first 20 lines
        for prefix, handler in metadata_handlers.items():
            if line.strip().startswith(prefix):
                handler(line, conn, location_id)
                break

def process_single_zip(zip_path, db_path, lock=None):
    try:
        with lock_context(lock):
            with sqlite3.connect(db_path) as conn:
                setup_sqlite_optimizations(conn)
                with tempfile.TemporaryDirectory() as tmpdir:
                    epw_path, epw_filename = extract_epw_from_zip(zip_path, tmpdir)
                    if not epw_path:
                        return False
                    
                    with open(epw_path, 'r') as f:
                        lines = f.readlines()
                    
                    # Process EPW file
                    header_dict, data_start_idx = parse_epw_header(lines)
                    location_id = insert_location(conn, header_dict, epw_filename)
                    parse_epw_metadata(lines, conn, location_id)
                    
                    # Process weather data
                    df = create_hourly_dataframe(lines[data_start_idx:])
                    insert_hourly_data(conn, location_id, df)
                    
                    # Generate and insert design days
                    design_day_blocks = extract_design_days(df)
                    insert_design_days(conn, location_id, design_day_blocks)
        return True
    except Exception as e:
        print(f"Error processing {zip_path}: {e}")
        return False

def process_zip_folder_to_db(zip_folder_path, db_path, use_parallel=True, max_workers=None):
    """Process all zip files in a folder and add their data to a database"""
    # Set default max_workers to the number of CPU cores minus 1
    if max_workers is None:
        max_workers = max(1, os.cpu_count() - 1)
    
    # Create the database and tables first
    conn = sqlite3.connect(db_path)
    print(f"Connected to database: {db_path}")
    create_tables(conn)
    print("Created tables in database")
    conn.close()
    
    # Get all zip files
    zip_files = [os.path.join(zip_folder_path, f) for f in os.listdir(zip_folder_path) 
                if f.lower().endswith(".zip")]
    
    if not zip_files:
        print(f"No zip files found in {zip_folder_path}")
        return
    
    print(f"Found {len(zip_files)} zip files to process")
    
    if use_parallel:
        # Using ThreadPoolExecutor for I/O bound tasks
        import threading
        lock = threading.Lock()
        
        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_single_zip, zip_path, db_path, lock) 
                      for zip_path in zip_files]
            
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                result = future.result()
                print(f"Completed {i+1}/{len(zip_files)} files")
        
        elapsed = time.time() - start_time
        print(f"Processed {len(zip_files)} files in {elapsed:.2f} seconds using {max_workers} workers")
    else:
        # Process sequentially
        start_time = time.time()
        success_count = 0
        for i, zip_path in enumerate(zip_files):
            if process_single_zip(zip_path, db_path):
                success_count += 1
            print(f"Completed {i+1}/{len(zip_files)} files")
        
        elapsed = time.time() - start_time
        print(f"Processed {success_count} files in {elapsed:.2f} seconds sequentially")

def process_multiple_zip_folders(zip_folder_paths, db_path, use_parallel=True, max_workers=None):
    """
    Process all zip files in multiple folders and add their data to a single database.

    Args:
        zip_folder_paths: List of folder paths containing zip files.
        db_path: Path to the SQLite database.
        use_parallel: Whether to process files in parallel.
        max_workers: Maximum number of worker threads.
    """
    # Set default max_workers to the number of CPU cores minus 1
    if max_workers is None:
        max_workers = max(1, os.cpu_count() - 1)

    # Create the database and tables if they don't exist
    conn = sqlite3.connect(db_path)
    print(f"Connected to database: {db_path}")
    create_tables(conn)
    print("Ensured tables exist in database")
    conn.close()

    # Collect all zip files from the provided folders
    zip_files = []
    for zip_folder_path in zip_folder_paths:
        if not os.path.exists(zip_folder_path):
            print(f"Folder does not exist: {zip_folder_path}")
            continue
        folder_zip_files = [os.path.join(zip_folder_path, f) for f in os.listdir(zip_folder_path) if f.lower().endswith(".zip")]
        zip_files.extend(folder_zip_files)

    if not zip_files:
        print("No zip files found in the provided folders")
        return

    print(f"Found {len(zip_files)} zip files to process from {len(zip_folder_paths)} folders")

    if use_parallel:
        # Using ThreadPoolExecutor for I/O bound tasks
        import threading
        lock = threading.Lock()

        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_single_zip, zip_path, db_path, lock) for zip_path in zip_files]

            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                result = future.result()
                print(f"Completed {i + 1}/{len(zip_files)} files")

        elapsed = time.time() - start_time
        print(f"Processed {len(zip_files)} files in {elapsed:.2f} seconds using {max_workers} workers")
    else:
        # Process sequentially
        start_time = time.time()
        success_count = 0
        for i, zip_path in enumerate(zip_files):
            if process_single_zip(zip_path, db_path):
                success_count += 1
            print(f"Completed {i + 1}/{len(zip_files)} files")

        elapsed = time.time() - start_time
        print(f"Processed {success_count} files in {elapsed:.2f} seconds sequentially")


def parse_line_to_table(line, conn, location_id, table_name, column_names, transform_func):
    """Generic function to parse a line and insert into a table"""
    parts = line.split(',')
    values = transform_func(parts)
    
    # Construct query with appropriate number of placeholders
    placeholders = ", ".join(["?" for _ in column_names])
    columns = ", ".join(column_names)
    
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    cur = conn.cursor()
    cur.execute(query, values)
    conn.commit()

def export_to_parquet_with_duckdb(sqlite_path, output_folder, compression='brotli'):
    """
    Export SQLite database to Parquet files using DuckDB, partitioned by catalog only.
    
    Args:
        sqlite_path: Path to the SQLite database.
        output_folder: Folder where Parquet files will be saved.
        compression: Compression algorithm to use (e.g., 'snappy', 'gzip', 'brotli').
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # Connect to DuckDB
    duck_conn = duckdb.connect(':memory:')
    
    # Attach SQLite database
    duck_conn.execute(f"ATTACH '{sqlite_path}' AS sqlite_db")
    
    # Get all locations with their catalog to establish partitions
    partitions = duck_conn.execute("""
        SELECT catalog, GROUP_CONCAT(id) AS location_ids
        FROM sqlite_db.locations_metadata
        GROUP BY catalog
    """).fetchall()
    
    # Process each partition
    for catalog, location_ids_str in partitions:
        # Create a folder structure for the partition - only by catalog
        partition_folder = os.path.join(
            output_folder,
            f"catalog={catalog or 'unknown'}"
        )
        if not os.path.exists(partition_folder):
            os.makedirs(partition_folder)
        
        print(f"Processing partition: catalog={catalog}")
        
        # Get tables from SQLite - using the main schema's sqlite_master table
        tables = duck_conn.execute("""
            SELECT name FROM main.sqlite_master 
            WHERE type='table' AND name NOT IN ('sqlite_sequence')
        """).fetchall()
        
        if not tables:
            # If the main.sqlite_master doesn't work, try directly querying the attached database schema
            tables = duck_conn.execute("""
                SELECT name FROM pragma_table_info('sqlite_db.sqlite_master')
                WHERE type='table' AND name NOT IN ('sqlite_sequence')
            """).fetchall()
            
            if not tables:
                # As a fallback, use a hardcoded list of tables we know exist
                tables = [('locations_metadata',), ('hourly_data',), 
                          ('design_days',), ('design_conditions_data',),
                          ('typical_periods_data',), ('ground_temperatures_data',),
                          ('holidays_dst_data',), ('data_periods',)]
                print("Using fallback table list")
        
        for table_name, in tables:
            # Check if table has location_id column using pragma_table_info directly
            try:
                has_location_id = duck_conn.execute(f"""
                    SELECT COUNT(*) FROM pragma_table_info('sqlite_db.{table_name}') 
                    WHERE name='location_id'
                """).fetchone()[0]
                
                if table_name == 'locations_metadata':
                    # Export locations_metadata filtered by IDs
                    duck_conn.execute(f"""
                        COPY (
                            SELECT * FROM sqlite_db.{table_name}
                            WHERE id IN ({location_ids_str})
                        ) TO '{os.path.join(partition_folder, f"{table_name}.parquet")}' 
                        (FORMAT 'PARQUET', COMPRESSION '{compression}')
                    """)
                    print(f"  - Exported {table_name} to partition")
                    
                elif has_location_id:
                    # Export all tables with location_id to a single file per table
                    duck_conn.execute(f"""
                        COPY (
                            SELECT * FROM sqlite_db.{table_name}
                            WHERE location_id IN ({location_ids_str})
                        ) TO '{os.path.join(partition_folder, f"{table_name}.parquet")}' 
                        (FORMAT 'PARQUET', COMPRESSION '{compression}')
                    """)
                    print(f"  - Exported {table_name} to partition")
                else:
                    # Tables without location_id are exported to the root folder (only once)
                    output_file = os.path.join(output_folder, f"{table_name}.parquet")
                    if not os.path.exists(output_file):
                        duck_conn.execute(f"""
                            COPY (SELECT * FROM sqlite_db.{table_name}) 
                            TO '{output_file}' 
                            (FORMAT 'PARQUET', COMPRESSION '{compression}')
                        """)
                        print(f"  - Exported {table_name} to root folder")
            except Exception as e:
                print(f"  - Error processing table {table_name}: {e}")
    
    duck_conn.close()
    print("Export to Parquet complete!")

def export_database_schema_to_json(sqlite_path, output_file="database_schema.json"):
    """
    Export the SQLite database schema to a JSON file.

    Args:
        sqlite_path: Path to the SQLite database.
        output_file: Path to the JSON file where the schema will be saved.
    """
    conn = sqlite3.connect(sqlite_path)
    cursor = conn.cursor()

    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]

    schema = {}

    for table in tables:
        # Get table schema using PRAGMA table_info
        cursor.execute(f"PRAGMA table_info('{table}')")
        columns = cursor.fetchall()

        # Add table schema to the dictionary
        schema[table] = [
            {
                "name": col[1],
                "type": col[2],
                "not_null": bool(col[3]),
                "default_value": col[4],
                "primary_key": bool(col[5])
            }
            for col in columns
        ]

    # Save schema to a JSON file
    with open(output_file, "w") as f:
        json.dump(schema, f, indent=4)

    print(f"Database schema exported to {output_file}")

    conn.close()
    


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Process EPW files into optimized SQLite database")
    parser.add_argument("--zip-folders", nargs="+", default=["C:\\Users\\plopez\\btap_weather\\historic", "C:\\Users\\plopez\\btap_weather\\future"],
                        help="List of folders containing EPW zip files")
    parser.add_argument("--db-path", default="C:\\Users\\plopez\\btap_weather\\weather.sqlite",
                        help="Output SQLite database path")
    parser.add_argument("--type-csv", default=None,
                        help="CSV file with column type definitions")
    parser.add_argument("--workers", type=int, default=20,
                        help="Number of worker threads (0 for sequential)")
    parser.add_argument("--log-level", default="INFO",
                        help="Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
    args = parser.parse_args()

    # Process ZIP folders and create the database
    process_multiple_zip_folders(args.zip_folders, args.db_path, use_parallel=args.workers > 0, max_workers=args.workers)
    print("Database created successfully!")

    # Export the database schema to JSON
    export_database_schema_to_json(args.db_path, os.path.join(os.path.dirname(args.db_path), "database_schema.json"))


    # Export to Parquet with compression
    export_to_parquet_with_duckdb(args.db_path, os.path.join(os.path.dirname(args.db_path), "parquet_files"), compression='brotli')


    print("Database creation complete!")


if __name__ == "__main__":
    main()

