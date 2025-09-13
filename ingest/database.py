import os
import re
import zipfile
import pandas as pd
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from sqlalchemy import create_engine, Engine, URL, inspect
from sqlalchemy.orm import Session, sessionmaker, scoped_session

# Initialize the logger
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('ingest')
logger.setLevel(logging.DEBUG)  # TODO: reset the level to warn

# Configuration variables - TODO: migrate to config file
DB_HOST = 'localhost'
DB_NAME = 'testdb'
DB_USER = 'postgres'
DB_PASSWORD = 'local'
DB_PORT = 5432

# TODO: swap pathing to use Pathlib
ANNUAL_TAX_RECORDS_FOLDER = 'test/data/2025' 

# Connect to the PostgreSQL database
engine = create_engine(URL.create(
    "postgresql+psycopg",
    host=str(DB_HOST), 
    port=DB_PORT,
    username=DB_USER,
    password=DB_PASSWORD, 
    database=DB_NAME
    ))

# Create a scoped_session factory for multi-threading with SQL Alchemy
Session_factory = sessionmaker(bind=engine)
Scoped_Session = scoped_session(Session_factory)

# semaphores for each table to prevent collision on table creates and writes
locks = {}

def getTableName(file_path: str):
    filename = os.path.basename(file_path)
    file_basename, suffix = filename.split(".")
    cleaned_filename = re.sub(r'\d+$', '', file_basename)
    return cleaned_filename
    

# Extracts the CSV files from the zip file and returns list(str) of file names
def unzip(zipFilePath: str):
    extracted = []
    try:
        with zipfile.ZipFile(zipFilePath, 'r') as zf:
            zf.extractall()
            extracted = zf.namelist()
    except zipfile.BadZipFile:
        logger.error(f"Error: Could not extract files from {zipFilePath}. File may be corrupt.")

    return extracted


"""
Function to ingest data from a CSV file with a ".txt" extension into the database.

Ignores files with non ".txt" extensions and subdirectories.

Adds a "record_year" column and a composite index on "acct" and "records_year" for faster lookups and to prevent data collisions.

TODO: make "acct" and "records_year" values constants
"""
def load_data_from_csv(filePath: str, year: int, db_table_lock: threading.Semaphore):
    table_name = ""
    if filePath.endswith('.txt'):
        table_name = getTableName(filePath)

    with db_table_lock:
        logger.debug(f"Acuiring DB table lock for {table_name}")

        try: 
            with Scoped_Session() as session:               
                logger.debug(f"Writing to table: {table_name}")
                df = pd.read_csv(filePath, sep=' ', engine='python')
                df['records_year']=year
                df = df.set_index(['acct', 'records_year'])
                df.to_sql(name=table_name, con=session.connection(), if_exists="append", index=True, chunksize=10000)
                session.commit()
        except Exception as e:
            # Rollback the session for the current thread on error.
            logger.error(f"Thread {threading.current_thread().name}: Error writing to {table_name}: {e}")
            Scoped_Session.rollback()
        finally:
            # Crucial for multithreading: remove() closes the thread-local session.
            Scoped_Session.remove()

        
                
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
        locks = {getTableName(file_path=c): threading.Semaphore(1) for c in csv_file_names}

        with ThreadPoolExecutor() as executor:
            futures = []
            for csv_file in csv_file_names:
                table_name = getTableName(csv_file)
                lock = locks[table_name]
                futures.append(
                    executor.submit(load_data_from_csv, os.path.join(os.getcwd(), csv_file), year, lock)
                )
 
            for _ in as_completed(futures):
                pass

"""Removes all files ending in .txt in the same directory as this Python script."""
def remove_txt_files():
    for filename in os.listdir():
        if filename.endswith(".txt"):
            os.remove(filename)

# Start processing the ZIP folder
# TODO: expand to handle multiple directories (multiple tax years)
current_script_folder = os.path.dirname(os.getcwd()) 
process_directory(current_script_folder + '/' + ANNUAL_TAX_RECORDS_FOLDER)

# verify tables created
inspector = inspect(engine)
tables = inspector.get_table_names()
print("Tables in the database:", tables)

# cleanup generated text files
remove_txt_files()