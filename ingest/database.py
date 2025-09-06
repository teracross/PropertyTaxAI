import os
import re
import zipfile
import pandas as pd
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from sqlalchemy import create_engine, Engine, URL
from sqlalchemy.orm import Session

# Initialize the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARN)  # set the level to warn

# Configuration variables - TODO: migrate to config file
DB_HOST = 'localhost'
DB_NAME = 'database'
DB_USER = 'username'
DB_PASSWORD = 'password'
DB_PORT = 8080
ZIP_FOLDER = '/path/to/your/zip_folder'
YEAR_REGEX = r'\d{4}'
ACCOUNT_ID_REGEX = r'^(\w+)'

# Connect to the PostgreSQL database
engine = create_engine(URL.create(
    "postgresql+psycopg",
    host=str(DB_HOST), 
    port=DB_PORT,
    username=DB_USER,
    password=DB_PASSWORD, 
    database=DB_NAME
    ))

# semaphores for each table to prevent collision on table creates and writes
locks = {}

def getTableName(filename: str):
     return re.sub(r'\d+$', '', os.path.basename(filename))

# Extracts the CSV files from the zip file and returns list(str) of file names
def unzip(filePath: str):
    extracted = []
    with zipfile.ZipFile(filePath, 'r') as zf: 
         zf.extractall()
         extracted = zf.namelist()

    return extracted

# Function to a list of CSV-format files with ".txt" file extension.
# Ignores files with non ".txt" file extensions as well as subdirectories 
def process_csv(filePath: str, year: int, db_table_lock: threading.Semaphore):
    table_name = ""
    if os.path.basename(filePath).endswith('.txt'):
        table_name = getTableName(filePath)

    with db_table_lock:
        logger.debug("Acuiring DB table lock for ", table_name)

        with Session(engine) as session: 
            df = pd.read_csv(filePath, sep=" ", low_memory=False)
            df['year']=year
            df.to_sql(name=table_name, con=engine, if_exists="append", index=False)
                
# Function to process a single directory (year)
def process_directory(dirPath: str):
    # retrieve the year value from the folder name
    try: 
        year = int(os.path.basename(dirPath))
    except ValueError as e:
        logger.error("File structure improperly formatted:", str(e))
        exit(1)

    # retrieve list of zip files to process
    zip_files = [f for f in os.listdir(dirPath) if f.endswith('.zip')]
    if not zip_files:
        logger.warning("No .zip files found in {}".format(dirPath))

    # process csv files extracted from each zip file concurrently
    for zip in zip_files: 
        csv_file_names = unzip(os.path.join(dirPath, zip))
        locks = {getTableName(filename=c): threading.Semaphore(1) for c in csv_file_names}

        with ThreadPoolExecutor() as executor:
            futures = []
            for csv_file in csv_file_names:
                table_name = getTableName(csv_file)
                lock = locks[table_name]
                futures.append(
                    executor.submit(process_csv, os.path.join(dirPath, csv_file), year, lock)
                )
 
            for _ in as_completed(futures):
                pass

# Start processing the ZIP folder
# TODO: expand to handle multiple directories (multiple tax years)
process_directory(ZIP_FOLDER)