import os
import re
import zipfile
import modin.pandas as md_pd
import pandas as pd
import logging
import pdfplumber
import sys
import csv

from pandas.io.parsers import TextFileReader
from sqlalchemy import create_engine, URL, inspect
from modin.db_conn import ModinDatabaseConnection
from distributed import Client
from typing import List

# Initialize the logger
logging.basicConfig(level=logging.WARN,
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
sqlalchemy_engine = create_engine(URL.create(
    "postgresql+psycopg",
    host=str(DB_HOST), 
    port=DB_PORT,
    username=DB_USER,
    password=DB_PASSWORD, 
    database=DB_NAME
    ))
engine = ModinDatabaseConnection(
    "sqlalchemy",
    host=str(DB_HOST), 
    dbname=DB_NAME,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD
)

# for parsing primary key values from pdataCodebook.pdf
filename_regex = r'Text file: [^\n]+'
file_pattern = re.compile(filename_regex)
primary_key_regex = 'Primary Key: '
primary_key_pattern = re.compile(primary_key_regex)
table_name_regex = 'Text file: '
table_name_pattern = re.compile(table_name_regex)
suggested_keys = {}

def get_table_name(file_path: str):
    filename = os.path.basename(file_path)
    file_basename, suffix = filename.split(".")
    return file_basename    

""" 
Extracts the CSV files from the zip file and returns list(str) of file names
"""
def unzip(zipFilePath: str):
    extracted = []
    try:
        with zipfile.ZipFile(zipFilePath, 'r') as zf:
            zf.extractall()
            extracted = zf.namelist()
    except zipfile.BadZipFile:
        logger.error(f"Error: Could not extract files from {zipFilePath}. File may be corrupt.")

    return extracted

# The COPY statement uses STDIN for the data source
def get_copy_statement(table_name: str):
    return f"""
        COPY {table_name} (id, name, value)
        FROM STDIN WITH (FORMAT CSV, DELIMITER ' ')
    """ 

def fix_invalid_formatting(data_frame: md_pd.DataFrame) -> md_pd.DataFrame: 
    for column_name in data_frame.columns.to_list():
    # Check if the column exists and has a string-like data type and contains the single quote character
        if data_frame[column_name].dtype == 'object' and data_frame[column_name].astype(str).str.contains("'").any():
            logger.debug(f"Quotes found in column '{column_name}'. Performing replacement.")
            data_frame[column_name] = data_frame[column_name].astype(str).str.replace('"', '"""')

    return data_frame

def prepare_dataframe_for_db(year: int, table_name: str, data_frame: md_pd.DataFrame) -> md_pd.DataFrame:
    data_frame['records_year'] = year
    index = []
    if (table_name in suggested_keys.keys()): 
        index = suggested_keys[table_name].copy()
        index.append('records_year')
    else:
        index = ['acct', 'records_year']
    data_frame.set_index(index, inplace=True)

    return data_frame

"""
Function to ingest data from a CSV file with a ".txt" extension into the database.

Ignores files with non ".txt" extensions and subdirectories.
Adds a "record_year" column and a composite index on "acct" and "records_year" for faster lookups and to prevent data collisions.

TODO: make "acct" and "records_year" values constants
"""
def load_data_from_csv(filePath: str, year: int):
    table_name = get_table_name(filePath) if filePath.endswith('.txt') else ''
    csv_data = None
    connection = sqlalchemy_engine.connect()

    try: 
        logger.info(f"Writing to table: {table_name}")
        csv_data = md_pd.read_csv(
            filePath, 
            sep='\x09', 
            engine='python', 
            encoding='MacRoman', 
            quoting=csv.QUOTE_NONE, 
            escapechar='\\', 
            on_bad_lines='warn'
        )

        df_chunks: List[md_pd.DataFrame] = []
        if isinstance(csv_data, TextFileReader): 
            for chunk in csv_data: 
                # logger.debug(f"Current Dataframe columns: {chunk.columns}")
                df_chunks.append(prepare_dataframe_for_db(year, table_name, chunk))
        else: 
            # logger.debug(f"Current Dataframe columns: {csv_data.columns}")
            df_chunks.append(prepare_dataframe_for_db(year, table_name, csv_data))

        for df in df_chunks: 
            df.to_sql(name=table_name, con=connection, if_exists="append", index=True, chunksize=10000)
    except Exception as e:
        logger.error("Error writing to table %s: %s", table_name, e, exc_info=True)
    finally:
        # Remove generated text file to save on memory - TODO: undo commenting-out below
        # if filePath.endswith(".txt"):
        #     logger.debug(f'Removing file {os.path.basename(filePath)} to save on memory.')
        #     os.remove(filePath)
        
        # ensure connection closed
        connection.close()
               
""" 
Function to process a single directory (year)
"""
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
        for csv_file in csv_file_names:
            load_data_from_csv(os.path.join(os.getcwd(), csv_file), year)

"""
Removes all files ending in .txt in the same directory as this Python script.
"""
def remove_txt_files():
    for filename in os.listdir():
        if filename.endswith(".txt"):
            os.remove(filename)

""" 
Get the primary keys for the tables
"""
def retieve_primary_keys(filePath: str): 
    pdf = None
    table_name = ''

    try:
        pdf = pdfplumber.open(filePath)
        pages = pdf.pages[2::1] # start from the 3rd page of the pdf

        for page in pages:
            page_lines = page.extract_text_lines(return_chars=False)

            for text_line in page_lines: 
                if file_pattern.match(text_line['text']): 
                    # regex match determines the start of a new table
                    # retrieve table name based on the file name provided and store info - also dilineates where a new table starts
                    table_name = table_name_pattern.sub('', text_line['text'])
                elif primary_key_pattern.match(text_line['text']): 
                    # convert comma-seperated string into list of keys
                    keys_str = primary_key_pattern.sub('', text_line['text'])

                    if not (',' in keys_str): 
                        # remove any unecessary text 
                        keys_str = keys_str.split(' ')[0]

                    suggested_keys[table_name] = keys_str.replace(' ', '').split(',')
    except Exception as e:
        logger.error('Error extracting primary keys from tables using pdfplumber', e)
    finally:
        if pdf is not None:
            pdf.close()


def main():
    client = Client()

    # configure max field size limit for reading csv file data
    field_size_limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(field_size_limit)
            break
        except OverflowError:
            field_size_limit = int(field_size_limit / 10)

    # TODO: expand to handle multiple directories (multiple tax years)
    current_script_folder = os.path.dirname(os.getcwd()) 
    test_data_filepath = current_script_folder + '/' + ANNUAL_TAX_RECORDS_FOLDER

    # retrieve the suggested primary keys for the tables 
    retieve_primary_keys(rf'{test_data_filepath}/pdataCodebook.pdf')
    logger.debug(f'Suggested primary keys: {suggested_keys}')

    if (len(sys.argv) > 1): 
        filepath = os.path.join(os.getcwd(), sys.argv[1])
        logger.debug(f'Filepath: {filepath}')
        load_data_from_csv(filepath, 2025)
    else: 
        # Start processing the ZIP folder
        process_directory(test_data_filepath)

        # cleanup generated text files
        remove_txt_files()
    
    # verify tables created
    inspector = inspect(sqlalchemy_engine)
    tables = inspector.get_table_names()
    print("Tables in the database:", tables)


if __name__ == "__main__":
    main()