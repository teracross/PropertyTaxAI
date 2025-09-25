import pdfplumber
import csv
import io
import re
import logging
from TableInfo import TableInfo
from typing import Tuple

from csv import reader

pdf_path = '/home/ed/PropertyTaxAI/test/data/2025/pdataCodebook.pdf'
tables_list = []
pdf = None

# Regexs used to determine table b box coordinates
filename_regex = r'Text file: [^\n]+'
file_pattern = re.compile(filename_regex)
table_name_regex = 'Text file: '
table_name_pattern = re.compile(table_name_regex)
primary_key_regex = 'Primary key: '
primary_key_pattern = re.compile(primary_key_regex)
table_start_regex = 'Column Name '
table_start_pattern = re.compile(table_start_regex)


# Initialize the logger
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('create')

"""
Attempt to parse pdf document using PDF plumber

Code saved for posterity

Realization: PDF Plumber doesn't have a good way of handling split, multi-page tables
"""
try:
    pdf = pdfplumber.open(pdf_path)
    for page_num, page in enumerate(pdf.pages, start=1):
        page_table_info_list = []

        if page_num == 3:
            text_lines = page.extract_text_lines(return_chars=False) #TODO: extract to method get_tables(text_lines: T_obj_list)
            line_iter = iter(text_lines)
            prev_line = None
            current_TableInfo = None

            for text_line in text_lines: 
                if file_pattern.match(text_line['text']): 

                    if prev_line != None and current_TableInfo != None: 
                    # if previous line exists, then it is also ending of the previous table
                        current_TableInfo.bottom = prev_line['bottom']
                        page_table_info_list.append(current_TableInfo)

                    # retrieve table name based on the file name provided and store info
                    current_TableInfo = TableInfo( 
                        table_name=table_name_pattern.sub('', text_line['text'])
                    )
                    prev_line = text_line
                elif primary_key_pattern.match(text_line['text']) and current_TableInfo != None: 
                    # convert comma-seperated string into list of keys
                    primary_keys = primary_key_pattern.sub('', text_line['text']).strip().split(',')
                    current_TableInfo.primary_key_fields = primary_keys
                elif table_name_pattern.match(text_line['text']) and current_TableInfo != None:
                    # determine starting coordinates of the table 
                    current_TableInfo.x0 = text_line['x0']
                    current_TableInfo.x1 = text_line['x1']
                    current_TableInfo.top = text_line['top']
                elif current_TableInfo != None : 
                    # find the largest x1 value from all the rows of the able for proper page cropping 
                    current_TableInfo.x1 = current_TableInfo.x1 if current_TableInfo.x1 > text_line['x1'] else text_line ['x1']

        for table_info in page_table_info_list: 
            cropped_page = page.crop( bbox=(table_info.x0, table_info.top, table_info.x1, table_info.bottom) )
            tables_list.append(cropped_page.find_table())

        page.close()

except Exception as e:
    logger.error('Error extracting tables with pdfplumber', e)
finally:
    if pdf is not None:
        pdf.close()

# try:
#     tables = camelot.io.read_pdf(pdf_path, pages='3-end', flavor='text')
#     logger.info(f"Number of tables found: {len(tables)}")
#     for table in tables: 
#         print(f'{table.df}\n\n')
# except Exception as e:
#     logger.error('Error extracting table with camelot', e)
# finally:
#     if pdf is not None:
#         pdf.close()