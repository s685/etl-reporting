"""
Excel Report Generator with Formatting
Reads data from Snowflake and writes to Excel with specific formatting.
This is proven working code for client reports.
"""

import argparse
import logging
import os
import sys
import yaml
import time
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, Alignment
from openpyxl.styles import PatternFill, Border, Side, Color
from datetime import datetime

# Use datamart_analytics framework
from datamart_analytics.connector.snowpark_connector import SnowparkConnector
from datamart_analytics.models.custom_models import SnowflakeCredentials, DatamartTable
from datamart_analytics.environment import environment_configuration
from datamart_analytics.tools.datamart_utils import create_target_credentials


# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

VALID_EXTENSIONS = ['.xlsx']


class FileWriter:
    """Class to write data to an Excel or CSV file."""
    
    def __init__(self, params):
        """
        parameters:
        output_path: str
            The path where the output file will be saved
        output_file: str
            The name of the output file
        max_column_width: int
            Maximum column width
        """
        self.output_path = params["output_path"]
        self.output_file = params["output_file"]
        self.max_column_width = params["max_column_width"]
        
        self.sheet_header_font = params["sheet_header_font"]
        self.table_header_font = params["table_header_font"]
        self.table_data_font = params["table_data_font"]
        self.border_to_row = params["border_to_row"]
        self.max_column_width = params["max_column_width"]
        self.carrier_name = params["carrier_name"]
        self.report_name = params["report_name"]
        self.report_start_dt = params["report_start_dt"]
        self.report_end_dt = params["report_end_dt"]
        self.report_run_dt = params["report_run_dt"]
        self.report_as_of_run_dt = params["report_as_of_run_dt"]
        self.header = params["header"]
        self.footer = params["footer"]
        self.border_to_row = params["border_to_row"]
        self.dollar_columns = params["dollar_columns"]
        self.specific_column_widths = params["specific_column_widths"]
        self.positive_dollar_format = "$(:,.2f)"
        self.negative_dollar_format = "($(:,.2f))"
        
    def write_to_excel(self, data, wb, current_page, total_pages):
        """
        Write data to an Excel file.
        
        parameters:
        data: pandas DataFrame
            The data to be written to the file
        wb: Workbook
        current_page: int
        total_pages: int
        """
        
        table_headers = data.columns
        last_column = data.shape[1]
        current_row = 1
        
        # add report header
        if self.header:
            self.add_header(wb, current_row, last_column, current_page, total_pages)
            current_row += 5
        
        # adding table headers
        for col, header in enumerate(table_headers, start=1):
            cell = wb.cell(row=current_row, column=col)
            cell.value = header
            cell.font = Font(name=name, size=size, bold=bold, color=color)
            cell.alignment = Alignment(horizontal='left', wrap_text=wrap_text)
            cell.fill = PatternFill(fill_type=fill_type, fgcolor=fill_color)
        
        # set column widths
        logging.info("Setting column widths")
        self.set_column_widths(wb, self.max_column_width)
        self.set_specific_column_widths(data, wb)
        
        data_rows = data.values.tolist()
        
        for row in data_rows:
            wb.append(row)
            current_row += 1
        
        def apply_dollar_format(self, data, wb):
            """Apply dollar format to the columns in the data frame if they are in the dollar_columns list"""
            if isinstance(data, pd.DataFrame):
                for column in self.dollar_columns:
                    logging.info(f"Applying dollar format to column {column}")
                    if column in data.columns:
                        clm_index = data.columns.get_loc(column) + 1  # Get the column index (1-based)
                        
        def apply_sorting(self, grouped_data, sorting_columns):
            if sorting_columns is not None:
                grouped_data = grouped_data.sort_values(by=sorting_columns, ascending=True)
            return grouped_data
        
        def apply_border(self, wb, current_row, last_column, border_to_row):
            """Add border to table headers"""
            if border_to_row['border to table headers']:
                self.apply_border_to_row(wb, current_row, last_column, border_to_row)
            return current_row+1
        
        def apply_border_to_row(self, wb, current_row, last_column, border_to_row):
            start_color = border_to_row['start_color']
            end_color = border_to_row['end_color']
            fill_type = border_to_row['fill_type']
            thin_border = Border(top=Side(style='thin'), bottom=Side(style='thin'))
            
            #current_row += 1
            wb.row_dimensions[current_row].height = 1
            for col_num in range(1, last_column+1):
                cell = wb.cell(row=current_row, column=col_num)
                cell.border = thin_border
                cell.fill = PatternFill(start_color=Color(rgb=start_color), end_color=Color(rgb=end_color), fill_type=fill_type)
            
            current_row += 1
            return current_row
        
        def add_header(self, wb, current_row, last_column, current_page, total_pages):
            # Add report header
            
            name, size, bold, color, alignment, wrap_text, fill_color, fill_type = self.set_cell_properties(self.sheet_header_font)
            
            for row in range(current_row, current_row + 3):
                cell = wb.cell(row=row, column=1)
                if row == current_row:
                    cell.value = self.carrier_name
                    wb.merge_cells(start_row=row, start_column=1, end_row=row, end_column=last_column//2)
                    
                # Add "timestamp" on the same row as carrier_name
                time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                time_info = f"Executed On:{time}"
                
                cell_offset = last_column // 2
                time_cell = wb.cell(row=row, column=cell_offset + 1)
                time_cell.value = time_info
                wb.merge_cells(start_row=row, start_column=cell_offset + 1, end_row=row, end_column=last_column)
                time_cell.font = Font(name=name, size=size, bold=bold, color=color)
                time_cell.alignment = Alignment(horizontal='right', wrap_text=wrap_text)
                time_cell.fill = PatternFill(fill_type=fill_type, fgcolor=fill_color)
            
            elif row == current_row + 1:
                cell.value = self.report_name
                wb.merge_cells(start_row=row, start_column=1, end_row=row, end_column=last_column)
                
                # Add "Page 1 of 1" on the same row as report_name
                page_info = f"Page {current_page} of {total_pages}"
                # page + 1
                cell_offset = last_column // 2
                page_cell = wb.cell(row=row, column=cell_offset + 1)
                page_cell.value = page_info
                wb.merge_cells(start_row=row, start_column=cell_offset + 1, end_row=row, end_column=last_column)
                page_cell.font = Font(name=name, size=size, bold=bold, color=color)
                page_cell.alignment = Alignment(horizontal='right', wrap_text=wrap_text)
                page_cell.fill = PatternFill(fill_type=fill_type, fgcolor=fill_color)
            
            else:
                if self.report_start_dt and self.report_end_dt:
                    
                    start_date = datetime.strptime(self.report_start_dt, '%Y-%m-%d %H:%M:%S').strftime("%m/%d/%Y")
                    end_date = datetime.strptime(self.report_end_dt, '%Y-%m-%d %H:%M:%S').strftime("%m/%d/%Y")
                    date_value = f"For the period from {start_date} to {end_date}"
                else:
                    report_date = datetime.strptime(self.report_run_dt, '%Y-%m-%d %H:%M:%S').strftime("%m/%d/%Y")
                    cell.value = f"Report as Date: {report_date}"
                    
                wb.merge_cells(start_row=row, start_column=1, end_row=row, end_column=last_column)
                cell.font = Font(name=name, size=size, bold=bold, color=color)
                cell.alignment = Alignment(horizontal='left', wrap_text=wrap_text)
                cell.fill = PatternFill(fill_type=fill_type, fgcolor=fill_color)
        
        def set_column_widths(self, wb, max_column_width):
            for col in range(1, wb.max_column + 1):
                col_index = get_column_letter(col)
                wb.column_dimensions[col_index].width = self.max_column_width
        
        def set_specific_column_widths(self, data, wb):
            # Set column widths based on the YAML configuration
            if isinstance(data, pd.DataFrame):
                if self.specific_column_widths is not None:
                    logging.info("Setting specific column widths")
                    for column in self.specific_column_widths:
                        logging.info(f"Setting column width for column {column}")
                        clm = column['column']
                        wdth = column['width']
                        wb.column_dimensions[clm].width = wdth
        
        def set_cell_properties(self, font):
            name = font['name']
            size = font['size']
            bold = font['bold']
            color = font['color']
            alignment = font['alignment']
            wrap_text = font['wrap_text']
            fill_color = font['fill_color']
            fill_type = font['fill_type']
            
            return name, size, bold, color, alignment, wrap_text, fill_color, fill_type


class DataPreprocessor:
    """Class to handle fetching and processing data from a database using SnowparkConnector."""
    
    def __init__(self, database, schema, warehouse, pre_sql_query):
        """
        parameters:
        database: str
            The name of the Snowflake database
        schema: str
            The name of the Snowflake schema
        warehouse: str
            The name of the Snowflake warehouse
        pre_sql_query: str
            The SQL query to set session variables
        """
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.pre_sql_query = pre_sql_query
        self.connector = None
    
    def get_snowflake_credentials(self):
        """
        Create SnowflakeCredentials from environment variables.
        
        returns:
        SnowflakeCredentials: Credentials object for Snowpark connection
        """
        return SnowflakeCredentials(
            user=environment_configuration.snowflake_user_target,
            password=environment_configuration.snowflake_password_target,
            account=environment_configuration.snowflake_account,
            warehouse=self.warehouse,
            database=self.database,
            table_schema=self.schema,
            role=environment_configuration.snowflake_role_target,
            authenticator=environment_configuration.snowflake_authenticator,
        )
    
    def connect_to_snowflake(self):
        """
        Establish connection to Snowflake using SnowparkConnector.
        
        returns:
        SnowparkConnector: Connected Snowpark connector (use with context manager)
        """
        credentials = self.get_snowflake_credentials()
        self.connector = SnowparkConnector(credentials)
        return self.connector
    
    def fetch_data(self, table, exclude_columns, filter_rows, sorting_columns):
        """
        Fetch data from Snowflake using SnowparkConnector.
        
        Must be called within a context manager (with statement).
        """
        
        # Execute pre_sql_query for session variables
        if self.pre_sql_query:
            for statement in self.pre_sql_query.split('\n'):
                if statement.strip():  # ensure the statement is not empty
                    self.connector.execute_query(statement.strip(), lazy=False)
                    logging.info(f"Executed statement: {statement.strip()}")
        
        # Build query
        columns = '*'
        if exclude_columns:
            exclude_str = ', '.join([f'"{col}"' for col in exclude_columns])
            columns = f'* EXCLUDE({exclude_str})'
        
        if filter_rows:
            query = f"SELECT {columns} FROM {table} WHERE {filter_rows}"
        else:
            query = f"SELECT {columns} FROM {table}"
        
        # Add sorting
        if sorting_columns:
            order_by_clause = ', '.join([f'"{col}"' if not col.startswith('*') and not col.endswith('*') else col for col in sorting_columns])
            query += f" ORDER BY {order_by_clause}"
        
        logging.info(f"Query statement: {query}")
        
        # Execute query and return pandas DataFrame
        df = self.connector.execute_query(query, lazy=False)
        
        # Convert Snowpark result to pandas DataFrame if needed
        if df is not None:
            if not isinstance(df, pd.DataFrame):
                # If it's a list of Row objects, convert to DataFrame
                result = pd.DataFrame([row.as_dict() for row in df])
            else:
                result = df
            logging.info(f"Data fetched from {table}")
            return result
        else:
            logging.warning(f"No data returned from {table}")
            return pd.DataFrame()


def validate_report_configextension(report):
    """validate the report configuration yml file extension"""
    base, ext = os.path.splitext(report)
    return f"{base}.yml" if not ext else report


def load_report_config(report):
    """load the report configuration yml file"""
    with open(report, 'r') as file:
        return yaml.safe_load(file)


def validate_report(report):
    """validate the report configuration yml file"""
    
    # Check if the report configuration file is empty
    if not report:
        logging.error(f"Error: {report} configuration file is empty.")
        sys.exit(1)
    
    # Check if the report configuration file has the required keys :
    # carrier_name and report_name,tables_list(for multiple worksheets), sheetnames(for multiple worksheets), header(for multiple worksheets), sorting_columns (for sorting the data)
    for key in ['carrier_name', 'report_name', 'tables', 'pre_sql_query']:
        if key not in report:
            logging.error(f"Error: {key} key is missing in the report configuration file.")
            sys.exit(1)


def parse_and_validate_args():
    """parse and validate command line arguments"""
    # ArgumentParser: Returns an ArgumentParser object
    # ArgumentParser is a function that returns a command line arguments
    parser = argparse.ArgumentParser("Required arguments: report_config file, database, schema, output_path, output_file")
    parser.add_argument("report", help="name of the report config file or extract to create, e.g., mcas.yml", type=str)
    parser.add_argument("database", help="The name of the Snowflake database, e.g., BUSINESS_VAULT", type=str)
    parser.add_argument("schema", help="The name of the Snowflake schema, e.g., BUSINESS_VAULT", type=str)
    parser.add_argument("output_path", help="The path where the output file will be saved, e.g., C:\\exports", type=str)
    parser.add_argument("output_file", help="The name of the output file, e.g., mcas.xlsx", type=str)
    parser.add_argument("carrier_name", help="The name of the carrier, e.g., ALLIANZ_ADMIN_GBP", type=str)
    
    # # add optional arguments : as_of_run_dt, report_start_date, report_end_date, report_run_dt
    parser.add_argument("--as_of_run_dt", help="The ASOF month for the extract, e.g., 12/31/2023", type=str)
    parser.add_argument("--report_start_dt", help="The start date for the report, e.g., 01/01/2023", type=str)
    parser.add_argument("--report_end_dt", help="The end date for the report, e.g., 01/31/2023", type=str)
    parser.add_argument("--report_run_dt", help="The run date for the report, e.g., 12/31/2023", type=str)
    
    args = parser.parse_args()
    
    # validate the arguments
    if not args.report:
        raise ValueError("Report name is required")
    
    if not args.database:
        raise ValueError("Database name is required")
    
    if not args.schema:
        raise ValueError("Schema name is required")
    
    if not args.output_path:
        raise ValueError("Output path is required")
    
    if not args.output_file:
        raise ValueError("Output file name is required")
    
    if not args.carrier_name:
        raise ValueError("Carrier name is required")
    
    if not os.path.isfile(args.report):
        logging.error(f"Error: {args.report} configuration file is not a valid file.")
        sys.exit(1)
    
    if not os.path.isdir(args.output_path):
        logging.error(f"Error: {args.output_path} is not a valid path.")
        sys.exit(1)
    
    ext = os.path.splitext(args.output_file)
    if ext not in VALID_EXTENSIONS:
        logging.WARN(f" WARNING: {args.output_file} does not have a standard file extension.")
    
    return args


def main():
    """main function"""
    
    # Assuming parse_and_validate_args() is a function that returns command line arguments
    args = parse_and_validate_args()
    
    print(args.report)
    # validate the report configuration yml file extension (.yml)
    report_validation = validate_report_configextension(args.report)
    
    # load the report configuration yml file
    report = load_report_config(report_validation)
    
    # validate the report configuration yml file keys
    validate_report(report)
    
    # read the configuration file mandatory keys
    carrier_name = report['carrier_name']
    report_name = report['report_name']
    tables_config = report['tables']
    # sheetnames = report['sheetnames']
    pre_sql_query = report['pre_sql_query'].format(carrier_name=args.carrier_name, as_of_run_dt=args.as_of_run_dt, report_start_dt=args.report_start_dt, report_end_dt=args.report_end_dt, report_start_date=args.report_start_date)
    
    
    # optional keys in config file
    data_columns, dollar_columns, grouping_column, column_widths, sheet_header_font, \
    table_header_font, table_data_font, border_to_row, max_column_width, as_of_run_dt, report_start_date, report_end_date, \
    excel_config = None, None, None, None, None, None, None, None, None, None, None, None, None
    
    params = {
        'output_path': args.output_path,
        'output_file': args.output_file,
        'max_column_width': max_column_width,
        'sheet_header_font': sheet_header_font,
        'table_header_font': table_header_font,
        'table_data_font': table_data_font,
        'carrier_name': carrier_name,
        'report_name': report_name,
        'report_start_dt': report_start_dt,
        'report_end_dt': report_end_dt,
        'report_run_dt': report_run_dt,
        'report_as_of_run_dt': report_as_of_run_dt,
        'header': header,
        'footer': footer,
        'border_to_row': border_to_row,
        'dollar_columns': dollar_columns,
        'specific_column_widths': specific_column_widths
    }
    
    # Fetch and validate the data from Snowflake using SnowparkConnector
    dp = DataPreprocessor(
        database=args.database, 
        schema=args.schema, 
        warehouse=args.warehouse if hasattr(args, 'warehouse') else environment_configuration.snowflake_role_target,
        pre_sql_query=pre_sql_query
    )
    
    # Use context manager for proper connection handling
    connector = dp.connect_to_snowflake()
    
    with connector:
        df = dp.fetch_data(table, exclude_columns, filter_rows, sorting_columns)
        
        current_page = current_page+1
        ws = wb.create_sheet(title=sheet_name)
        writer = FileWriter(params)
        writer.write_to_excel(df, ws, current_page, total_pages)
        
        # Apply dollar formatting to the worksheet
        if dollar_columns is not None:
            writer.apply_dollar_format(df, ws)
    
    wb.save(os.path.join(args.output_path, args.output_file))
    
    logging.info(f"Output will be saved to: {args.output_path}/{args.output_file}")


if __name__ == '__main__':
    """
    Execution Time For Generating the Feed: {hours} hr {minutes} min {seconds} sec {milliseconds} ms')
    """
    start_time = time.time()
    main()
    end_time = time.time()
    
    execution_time = end_time - start_time
    hours = int(execution_time // 3600)
    minutes = int((execution_time % 3600) // 60)
    seconds = int((execution_time % 60))
    milliseconds = int((execution_time % 1) * 1000)
    
    print(f"================================================================================")
    print(f'Execution Time For Generating the Feed: {hours} hr {minutes} min {seconds} sec {milliseconds} ms')
    print(f"================================================================================")
    
    logging.info(f'Execution Time For Generating the Feed: {hours} hr {minutes} min {seconds} sec {milliseconds} ms')
