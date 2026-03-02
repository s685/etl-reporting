# ********************************************************************************
# PURPOSE: Python script to generate an Excel workbook with multiple worksheets
# from a SINGLE Snowflake table using YAML configuration. Each unique value in
# a designated grouping_column becomes a separate worksheet. The grouping column
# value is used as the worksheet name and is excluded from the data columns.
# ********************************************************************************
# USAGE:
#   python yaml_multi_worksheet_template.py <config>.yml <database> <schema> \
#       <output_path> <output_file>.xlsx <carrier_name> \
#       --warehouse <wh> --report_start_dt "2023-01-01" --report_end_dt "2023-12-31"
# ********************************************************************************
# YAML REQUIRED KEYS:
#   carrier_name, report_name, table, grouping_column, pre_sql_query
# ********************************************************************************
# Dependencies:
#   Python 3.6+, snowflake-snowpark-python (via SnowparkConnector),
#   datamart_analytics (connector, models, tools), pandas, openpyxl, pyyaml
# ********************************************************************************

import argparse
import logging
import os
import re
import sys
import time
from datetime import datetime
from typing import cast

import pandas as pd
import yaml  # type: ignore[import-untyped]

from datamart_analytics.connector.snowpark_connector import SnowparkConnector
from datamart_analytics.models.custom_models import DatamartTable
from datamart_analytics.tools.datamart_utils import create_target_credentials

from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side, Color

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

VALID_EXTENSIONS = ['.xlsx']


def sanitize_sheet_name(name):
    """Sanitize a string for use as an Excel worksheet name.

    Excel rules:
      - Max 31 characters
      - Cannot contain: \\ / * ? : [ ]
      - Cannot be blank
    """
    if not name or str(name).strip() == '':
        return 'Sheet'
    name = str(name).strip()
    # Remove invalid characters
    name = re.sub(r'[\\/*?\[\]:]', '', name)
    # Truncate to 31 characters
    if len(name) > 31:
        name = name[:31]
    return name if name else 'Sheet'


class FileWriter:
    """Class to write data to an Excel worksheet with formatting support.

    parameters:
        output_path: str - Path where the output file will be saved
        output_file: str - Name of the output file
    """

    def __init__(self, params):
        self.output_path = params["output_path"]
        self.output_file = params["output_file"]
        self.max_column_width = params.get("max_column_width")
        self.sheet_header_font = params.get("sheet_header_font")
        self.table_header_font = params.get("table_header_font")
        self.table_data_font = params.get("table_data_font")
        self.border_to_row = params.get("border_to_row")
        self.carrier_name = params["carrier_name"]
        self.report_name = params["report_name"]
        self.report_start_dt = params.get("report_start_dt")
        self.report_end_dt = params.get("report_end_dt")
        self.report_run_dt = params.get("report_run_dt")
        self.report_as_of_run_dt = params.get("report_as_of_run_dt")
        self.header = params.get("header")
        self.footer = params.get("footer")
        self.dollar_columns = params.get("dollar_columns")
        self.specific_column_widths = params.get("specific_column_widths")
        self.positive_dollar_format = "${:,.2f}"
        self.negative_dollar_format = "(${:,.2f})"

    def write_to_excel(self, data, ws, current_page, total_pages):
        """Write data to an Excel worksheet.

        parameters:
            data: pandas DataFrame - The data to be written
            ws: openpyxl Worksheet - Target worksheet
            current_page: int - Current page/worksheet number
            total_pages: int - Total number of worksheets
        """
        table_headers = data.columns
        last_column = data.shape[1]
        current_row = 1

        # add report header
        if self.header:
            self.add_header(ws, current_row, last_column, current_page, total_pages)
            current_row += 5

        # write table headers
        name, size, bold, color, alignment, wrap_text, fill_color, fill_type = self.set_cell_properties(self.table_header_font)

        for col, header in enumerate(table_headers, start=1):
            cell = ws.cell(row=current_row, column=col)
            cell.value = header
            cell.font = Font(name=name, size=size, bold=bold, color=color)
            cell.alignment = Alignment(horizontal=alignment, wrap_text=wrap_text)
            cell.fill = PatternFill(fill_type=fill_type, fgColor=fill_color)

        # apply border to header row
        if self.border_to_row and self.border_to_row.get('border_to_table_headers'):
            self.apply_border_to_row(ws, current_row, last_column, self.border_to_row)

        # set column widths
        logging.info("Setting column widths")
        self.set_column_widths(ws, self.max_column_width)
        self.set_specific_column_widths(data, ws)

        # write data rows
        data_rows = data.values.tolist()

        for row in data_rows:
            current_row += 1
            for col_idx, val in enumerate(row, start=1):
                ws.cell(row=current_row, column=col_idx, value=val)

        # apply data font styling
        if self.table_data_font and not data.empty:
            d_name, d_size, d_bold, d_color, d_alignment, d_wrap_text, d_fill_color, d_fill_type = self.set_cell_properties(self.table_data_font)
            header_offset = 6 if self.header else 1
            data_start_row = header_offset + 1  # first data row (after table header row)
            for row_idx in range(data_start_row, data_start_row + len(data_rows)):
                for col_idx in range(1, last_column + 1):
                    cell = ws.cell(row=row_idx, column=col_idx)
                    cell.font = Font(name=d_name, size=d_size, bold=d_bold, color=d_color)
                    cell.alignment = Alignment(horizontal=d_alignment, wrap_text=d_wrap_text)
                    if d_fill_type and d_fill_type != 'none':
                        cell.fill = PatternFill(fill_type=d_fill_type, fgColor=d_fill_color)

    def apply_dollar_format(self, data, ws):
        """Apply dollar format to specified columns in the worksheet."""
        if self.dollar_columns and isinstance(data, pd.DataFrame):
            for column in self.dollar_columns:
                logging.info(f"Applying dollar format to column: {column}")
                if column in data.columns:
                    col_idx = cast(int, data.columns.get_loc(column)) + 1
                    column_letter = get_column_letter(col_idx)
                    for cell in ws[column_letter]:
                        cell.number_format = '$#,##0.00'

    def apply_border_to_row(self, ws, current_row, last_column, border_to_row):
        """Apply a thin border and fill to a row."""
        start_color = border_to_row.get('start_color', '00000000')
        end_color = border_to_row.get('end_color', '00000000')
        fill_type = border_to_row.get('fill_type', 'none')
        thin_border = Border(top=Side(style='thin'), bottom=Side(style='thin'))

        for col_num in range(1, last_column + 1):
            cell = ws.cell(row=current_row, column=col_num)
            cell.border = thin_border
            cell.fill = PatternFill(
                start_color=Color(rgb=start_color),
                end_color=Color(rgb=end_color),
                fill_type=fill_type
            )

    def add_header(self, ws, current_row, last_column, current_page, total_pages):
        """Add report header rows to the worksheet.

        Row 1: Carrier name (left) | Executed On: timestamp (right)
        Row 2: Report name (left)  | Page X of Y (right)
        Row 3: For Period: start_date To end_date
        """
        name, size, bold, color, alignment, wrap_text, fill_color, fill_type = self.set_cell_properties(self.sheet_header_font)

        for row in range(current_row, current_row + 3):
            cell = ws.cell(row=row, column=1)

            if row == current_row:
                # Row 1: Carrier name | Executed On
                cell.value = self.carrier_name
                ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=last_column // 2)
                cell.font = Font(name=name, size=size, bold=bold, color=color)
                cell.alignment = Alignment(horizontal=alignment, wrap_text=wrap_text)
                cell.fill = PatternFill(fill_type=fill_type, fgColor=fill_color)

                time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                time_info = f"Executed On: {time_str}"
                cell_offset = last_column // 2
                time_cell = ws.cell(row=row, column=cell_offset + 1)
                time_cell.value = time_info
                ws.merge_cells(start_row=row, start_column=cell_offset + 1, end_row=row, end_column=last_column)
                time_cell.font = Font(name=name, size=size, bold=bold, color=color)
                time_cell.alignment = Alignment(horizontal='right', wrap_text=wrap_text)
                time_cell.fill = PatternFill(fill_type=fill_type, fgColor=fill_color)

            elif row == current_row + 1:
                # Row 2: Report name | Page X of Y
                cell.value = self.report_name
                ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=last_column // 2)
                cell.font = Font(name=name, size=size, bold=bold, color=color)
                cell.alignment = Alignment(horizontal=alignment, wrap_text=wrap_text)
                cell.fill = PatternFill(fill_type=fill_type, fgColor=fill_color)

                page_info = f"Page {current_page} of {total_pages}"
                cell_offset = last_column // 2
                page_cell = ws.cell(row=row, column=cell_offset + 1)
                page_cell.value = page_info
                ws.merge_cells(start_row=row, start_column=cell_offset + 1, end_row=row, end_column=last_column)
                page_cell.font = Font(name=name, size=size, bold=bold, color=color)
                page_cell.alignment = Alignment(horizontal='right', wrap_text=wrap_text)
                page_cell.fill = PatternFill(fill_type=fill_type, fgColor=fill_color)

            else:
                # Row 3: For Period: start_date To end_date
                if self.report_start_dt and self.report_end_dt:
                    try:
                        start_date = datetime.strptime(self.report_start_dt, '%Y-%m-%d %H:%M:%S.%f').strftime("%m/%d/%Y")
                    except ValueError:
                        try:
                            start_date = datetime.strptime(self.report_start_dt, '%Y-%m-%d %H:%M:%S').strftime("%m/%d/%Y")
                        except ValueError:
                            start_date = self.report_start_dt
                    try:
                        end_date = datetime.strptime(self.report_end_dt, '%Y-%m-%d %H:%M:%S.%f').strftime("%m/%d/%Y")
                    except ValueError:
                        try:
                            end_date = datetime.strptime(self.report_end_dt, '%Y-%m-%d %H:%M:%S').strftime("%m/%d/%Y")
                        except ValueError:
                            end_date = self.report_end_dt
                    cell.value = f"For Period: {start_date} To {end_date}"
                else:
                    cell.value = f"For Period: {datetime.now().strftime('%m/%d/%Y')}"

                ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=last_column)
                cell.font = Font(name=name, size=size, bold=bold, color=color)
                cell.alignment = Alignment(horizontal=alignment, wrap_text=wrap_text)
                cell.fill = PatternFill(fill_type=fill_type, fgColor=fill_color)

    def set_column_widths(self, ws, max_column_width):
        """Set all column widths to max_column_width."""
        width = max_column_width if max_column_width else 15
        for col in range(1, ws.max_column + 1):
            col_index = get_column_letter(col)
            ws.column_dimensions[col_index].width = width

    def set_specific_column_widths(self, data, ws):
        """Set column widths based on the YAML configuration overrides."""
        if isinstance(data, pd.DataFrame) and self.specific_column_widths:
            logging.info("Setting specific column widths")
            for column in self.specific_column_widths:
                logging.info(f"Setting column width for column {column}")
                clmn = column['column']
                wdth = column['width']
                ws.column_dimensions[clmn].width = wdth

    def set_cell_properties(self, font):
        """Extract font/cell properties from a font configuration dict."""
        if not font or not isinstance(font, dict):
            return 'Calibri', 11, False, '000000', 'left', False, 'FFFFFF', 'solid'
        name = font.get('name', 'Calibri')
        size = font.get('size', 11)
        bold = font.get('bold', False)
        color = font.get('color', '000000')
        wrap_text = font.get('wrap_text', False)
        fill_color = font.get('fill_color', 'FFFFFF')
        fill_type = font.get('fill_type', 'solid')
        alignment = font.get('alignment', 'left')
        return name, size, bold, color, alignment, wrap_text, fill_color, fill_type


class Datapreprocessor:
    """Class to handle fetching and processing data from Snowflake.

    parameters:
        connector: SnowparkConnector - The Snowpark connector (use within context manager)
        database: str - The Snowflake database
        schema: str - The Snowflake schema
        pre_sql_query: str - SQL to set session variables
    """

    def __init__(self, connector, database, schema, pre_sql_query):
        self.connector = connector
        self.database = database
        self.schema = schema
        self.pre_sql_query = pre_sql_query

        logging.info("Using Snowpark connection")
        logging.info(f"Active Database.Schema is {self.database}.{self.schema}")

    def fetch_data(self, table, exclude_columns, filter_rows, sorting_columns):
        """Fetch data from the Snowflake database using SnowparkConnector."""

        # set session variables using pre_sql_query
        for statement in self.pre_sql_query.split('\n'):
            if statement.strip():
                self.connector.execute_query(statement, lazy=False)
                logging.info(f"Executed statement: {statement}")

        # build SELECT with optional EXCLUDE
        columns = ','.join(
            ['*'] if not exclude_columns
            else [f'* EXCLUDE("{col}")' for col in exclude_columns]
        )

        if filter_rows:
            query = f"SELECT {columns} FROM {table} WHERE {filter_rows}"
        else:
            query = f"SELECT {columns} FROM {table}"

        # sorting
        if sorting_columns:
            order_by_clause = ', '.join(
                [f'"{col}"' if not col.startswith('"') and not col.endswith('"') else col
                 for col in sorting_columns]
            )
            query += f" ORDER BY {order_by_clause}"

        logging.info(f"Query statement: {query}")
        result = self.connector.execute_query(query, lazy=False)

        if result is None or len(result) == 0:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame([row.as_dict() for row in result])

        logging.info(f"Data fetched from {table}")
        return df


def validate_report_configextension(report):
    """Validate the report configuration yml file extension."""
    base, ext = os.path.splitext(report)
    return f"{base}.yml" if not ext else report


def load_report_config(report):
    """Load the report configuration yml file."""
    with open(report, 'r') as file:
        return yaml.safe_load(file)


def validate_report(report):
    """Validate the report configuration yml file has required keys."""
    if not report:
        logging.error("Error: Report configuration file is empty.")
        sys.exit(1)

    if not isinstance(report, dict):
        logging.error("Error: Report configuration file is not a dictionary.")
        sys.exit(1)

    for key in ['carrier_name', 'report_name', 'table', 'grouping_column', 'pre_sql_query']:
        if key not in report:
            logging.error(f"Error: '{key}' key is missing in the report configuration file.")
            sys.exit(1)

    if report.get('header'):
        if not report.get('sheet_header_font'):
            logging.error("Error: sheet_header_font is required when header is True.")
            sys.exit(1)


def parse_and_validate_args():
    """Parse and validate command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate multi-worksheet Excel from a single table grouped by a column. "
                    "Required: report, database, schema, output_path, output_file, carrier_name"
    )
    parser.add_argument("report", help="Path to the YAML report config file, e.g., app_activity.yml", type=str)
    parser.add_argument("database", help="Snowflake database name, e.g., DEV_SNOWFLAKE_WAREHOUSE", type=str)
    parser.add_argument("schema", help="Snowflake schema name, e.g., BUSINESS_VAULT", type=str)
    parser.add_argument("output_path", help="Path where the output file will be saved, e.g., /workspace/", type=str)
    parser.add_argument("output_file", help="Name of the output file, e.g., report.xlsx", type=str)
    parser.add_argument("carrier_name", help="Carrier name, e.g., ALLIANZ_ADMIN_088", type=str)
    parser.add_argument(
        "--warehouse",
        help="Snowflake warehouse name (or set SNOWFLAKE_WAREHOUSE env var)",
        type=str,
        default=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )
    parser.add_argument("--as_of_run_dt", help="ASOF date, e.g., 12/31/2023", type=str)
    parser.add_argument("--report_start_dt", help="Report start date, e.g., 01/01/2023", type=str)
    parser.add_argument("--report_end_dt", help="Report end date, e.g., 12/31/2023", type=str)
    parser.add_argument("--report_run_dt", help="Report run date, e.g., 12/31/2023", type=str)

    args = parser.parse_args()

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
    if not args.warehouse:
        logging.error("Error: Warehouse is required. Set --warehouse or SNOWFLAKE_WAREHOUSE env var.")
        sys.exit(1)

    if not os.path.isfile(args.report):
        logging.error(f"Error: {args.report} configuration file is not a valid file.")
        sys.exit(1)

    if not os.path.isdir(args.output_path):
        logging.error(f"Error: {args.output_path} is not a valid path.")
        sys.exit(1)

    ext = os.path.splitext(args.output_file)[1]
    if ext not in VALID_EXTENSIONS:
        logging.warning(f"WARNING: {args.output_file} does not have a standard file extension.")

    return args


def main():
    """Main function."""
    args = parse_and_validate_args()
    print(args.report)

    # validate the report configuration yml file extension (.yml)
    report_validation = validate_report_configextension(args.report)

    # load the report configuration yml file
    report = load_report_config(report_validation)

    # validate the report configuration yml file keys
    validate_report(report)

    # ---- Read required configuration keys ----
    carrier_name = report['carrier_name']
    report_name = report['report_name']
    table = report['table']
    grouping_column = report['grouping_column']
    pre_sql_query = report['pre_sql_query'].format(
        carrier_name=args.carrier_name,
        as_of_run_dt=args.as_of_run_dt or '',
        report_start_dt=args.report_start_dt or '',
        report_end_dt=args.report_end_dt or '',
        report_run_dt=args.report_run_dt or '',
    )

    # ---- Read optional configuration keys ----
    exclude_columns = report.get('exclude_columns', None)
    filter_rows = report.get('filter_rows', None)
    sorting_columns = report.get('sorting_columns', None)
    dollar_columns = report.get('dollar_columns', None)
    specific_column_widths = report.get('specific_column_widths', None)
    sheet_header_font = report.get('sheet_header_font', None)
    table_header_font = report.get('table_header_font', None)
    table_data_font = report.get('table_data_font', None)
    border_to_row = report.get('border_to_row', None)
    max_column_width = report.get('max_column_width', None)
    header = report.get('header', None)
    footer = report.get('footer', None)
    null_group_sheet_name = report.get('null_group_sheet_name', 'Unassigned')

    # ---- Optional arguments from command line ----
    report_start_dt = args.report_start_dt if args.report_start_dt else None
    report_end_dt = args.report_end_dt if args.report_end_dt else None
    report_as_of_run_dt = args.as_of_run_dt if args.as_of_run_dt else None
    report_run_dt = args.report_run_dt if args.report_run_dt else None

    # ---- Create credentials and connect ----
    datamart_table = DatamartTable(
        name="datafeed",
        source_database=args.database,
        source_schema=args.schema,
        target_database=args.database,
        target_schema=args.schema,
        target_warehouse=args.warehouse,
        carrier_name=args.carrier_name,
    )
    credentials = create_target_credentials(datamart_table)

    with SnowparkConnector(credentials) as connector:
        dp = Datapreprocessor(
            connector=connector,
            database=args.database,
            schema=args.schema,
            pre_sql_query=pre_sql_query,
        )

        # Fetch all data from the single table
        df = dp.fetch_data(table, exclude_columns, filter_rows, sorting_columns)

        if df.empty:
            logging.warning("No data returned from the query. Creating empty workbook.")
            wb = Workbook()
            del wb['Sheet']
            ws = wb.create_sheet(title="No Data")
            ws.cell(row=1, column=1, value="No data is available during this period")
            wb.save(os.path.join(args.output_path, args.output_file))
            logging.info(f"Output saved to: {os.path.join(args.output_path, args.output_file)}")
            return

        # Validate grouping column exists in the data
        if grouping_column not in df.columns:
            logging.error(f"Error: grouping_column '{grouping_column}' not found in data columns: {list(df.columns)}")
            sys.exit(1)

        # Handle NULL values in grouping column - replace with configurable name
        null_count = df[grouping_column].isna().sum()
        if null_count > 0:
            logging.info(f"Found {null_count} rows with NULL in '{grouping_column}'. "
                         f"Creating worksheet named '{null_group_sheet_name}'.")
            df[grouping_column] = df[grouping_column].fillna(null_group_sheet_name)

        # Get unique grouping values (preserves order from SQL)
        unique_groups = df[grouping_column].unique().tolist()
        total_pages = len(unique_groups)
        logging.info(f"Found {total_pages} unique group(s) in '{grouping_column}': {unique_groups}")

        # ---- Create workbook and iterate through groups ----
        wb = Workbook()
        del wb['Sheet']

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
            'report_as_of_run_dt': report_as_of_run_dt,
            'report_run_dt': report_run_dt,
            'header': header,
            'footer': footer,
            'border_to_row': border_to_row,
            'dollar_columns': dollar_columns,
            'specific_column_widths': specific_column_widths,
        }

        writer = FileWriter(params)

        for current_page, group_value in enumerate(unique_groups, start=1):
            sheet_name = sanitize_sheet_name(group_value)
            logging.info(f"Processing worksheet {current_page}/{total_pages}: '{sheet_name}'")

            # Filter data for this group and drop the grouping column
            group_df = df[df[grouping_column] == group_value].drop(columns=[grouping_column])
            group_df = group_df.reset_index(drop=True)

            ws = wb.create_sheet(title=sheet_name)
            writer.write_to_excel(group_df, ws, current_page, total_pages)

            # Apply dollar formatting to the worksheet
            if dollar_columns:
                writer.apply_dollar_format(group_df, ws)

    # Save workbook
    output_filepath = os.path.join(args.output_path, args.output_file)
    wb.save(output_filepath)
    logging.info(f"Output saved to: {output_filepath}")


if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()

    execution_time = end_time - start_time
    hours = int(execution_time // 3600)
    minutes = int((execution_time % 3600) // 60)
    seconds = int(execution_time % 60)
    milliseconds = int((execution_time % 1) * 1000)

    print(f'***************************************************')
    print(f'Execution Time For Generating the Feed: {hours} hr {minutes} min {seconds} sec {milliseconds} ms'.center(100))
    print(f'***************************************************')

    logging.info(f'Execution Time For Generating the Feed: {hours} hr {minutes} min {seconds} sec {milliseconds} ms')
