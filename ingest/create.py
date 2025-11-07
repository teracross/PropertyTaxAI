import pdfplumber
import csv
import io
import re
import logging
import camelot

from TableInfo import TableInfo
from typing import Tuple
from importlib import resources
from PdfplumberBBox import PdfPlumberBBox

from csv import reader

pdf_path = '/home/ed/PropertyTaxAI/test/data/2025/pdataCodebook.pdf'
resources.path('test', 'pdataCodebook.pdf')
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
Processing Logic: 
1) use PDF plumber to find all of the bbox values for the tables based regexes above
2) using PDF plumber bbox values parse the tables using bbox values by first cropping the page

Parse Logic: 
1) traverse each page line by line
2) if "table start" parse table name, set bool flag <code>table_ended = false</code>
3)  a) if "primary key" present parse keys - add "records_year" field - 

    

Note - PDF Plumber doesn't have a good way of handling split, multi-page tables
"""
try:
    pdf = pdfplumber.open(pdf_path)

    current_TableInfo = TableInfo()
    for page_num, page in enumerate(pdf.pages, start=1):
        page_table_info_list = []
        if page_num == 3:
            text_lines = page.extract_text_lines(return_chars=False) #TODO: extract to method get_tables(text_lines: T_obj_list)
            prev_line = None

            curr_pdfplumber_bbox = PdfPlumberBBox()

            # TODO: need to update logic as tables can also span multiple pages - table doesn't end until next file_pattern regex match is made
            for line_num, text_line in enumerate(text_lines): 
            
                if file_pattern.match(text_line['text']): 
                    # regex match determines the start of a new table
                    if prev_line != None: 
                    # if previous line exists, then it is also ending of the previous table
                        curr_pdfplumber_bbox.bottom = prev_line['bottom']
                        current_TableInfo.Pdfplumber_bboxes.append(curr_pdfplumber_bbox)
                        page_table_info_list.append(current_TableInfo)

                    # retrieve table name based on the file name provided and store info - also dilineates where a new table starts
                    current_TableInfo.table_name = table_name_pattern.sub('', text_line['text'])
                    curr_pdfplumber_bbox = PdfPlumberBBox()
                elif primary_key_pattern.match(text_line['text']): 
                    # convert comma-seperated string into list of keys
                    primary_keys = primary_key_pattern.sub('', text_line['text']).strip().split(',')
                    primary_keys.append('records_year')
                    current_TableInfo.primary_key_fields = primary_keys
                elif table_name_pattern.match(text_line['text']):
                    # determine starting coordinates of the table 
                    curr_pdfplumber_bbox.x0 = text_line['x0']
                    curr_pdfplumber_bbox.x1 = text_line['x1']
                    curr_pdfplumber_bbox.top = text_line['top']
                elif hasne : 
                    # current page has ended, new bbox needed to scope for following page
                else: 
                    # find the largest x1 value from all the rows of the able for proper page cropping 
                    curr_pdfplumber_bbox.x1 = curr_pdfplumber_bbox.x1 if curr_pdfplumber_bbox.x1 > text_line['x1'] else text_line ['x1']
                prev_line = text_line

        for table_info in page_table_info_list: 
            cropped_page = page.crop( bbox=(table_info.x0, table_info.top, table_info.x1, table_info.bottom) )
            tables_list.append(cropped_page.find_table())

        page.close()

except Exception as e:
    logger.error('Error extracting tables with pdfplumber', e)
finally:
    if pdf is not None:
        pdf.close()

try:
    tables = camelot.io.read_pdf(pdf_path, pages='3,4', flavor='stream', strip_text="\n", row_tol=10)
    logger.info(f"Number of tables found: {len(tables)}")
    for table in tables: 
        print(f'{table.df}\n\n')
        table.rows[0]
except Exception as e:
    logger.error('Error extracting table with camelot', e)
finally:
    if pdf is not None:
        pdf.close()