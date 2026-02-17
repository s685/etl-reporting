# *****************************************************************************
# PURPOSE: Python script to generate an Excel file from a SQL query with
#          header or footer logic. Designed to handle grouped data.
#
# REPORTS:
#   - Refund reason report
#   - Writeoff
#   - Deposit Receipts Detail
#   - Deposit Receipts Summary
#   - Under Over tolerance
#   - Returned Checks EFT
#   - FMS Voided and stopped checks
#   - FMS Refunds Generated
#
# *****************************************************************************
# Create an Excel file with the following format:
# 1. Each column is a separate field in the Excel file
#    a. If a column is not a string, it's converted to a string
#    b. If a column is null, it's converted to an empty string
#    c. If a column is NaN, it's converted to an empty string
#    d. If the format is still not correct, convert in the SQL query
# 2. Columns are written in the order specified in the config file
#    a. Each row is written to a new line
#    b. Each line is terminated with a newline character
#
# *****************************************************************************
# NOTES AND RECOMMENDATIONS:
# 1. Headers are added via add_header(). Use UNION ALL in the query for footer.
# 2. The file is sorted by the SQL query, not by the script.
# 3. Delimiter for CSV (if added): comma by default, configurable in config.
#
# *****************************************************************************
# DEPENDENCIES:
#   Python 3.6+, snowflake-snowpark-python (via SnowparkConnector),
#   datamart_analytics (connector, models, tools), pandas, argparse,
#   pyarrow=10.0.1, openpyxl
#
# *****************************************************************************
# RUN COMMAND:
#   python excel_writer_header_footer.py args[0] args[1] args[2] args[3] args[4] args[5]
#   Arguments:
#     args[0] = Path to the report definition file
#     args[1] = Snowflake database name
#     args[2] = Snowflake schema name
#     args[3] = Path to the folder where the file will be saved
#     args[4] = Name of the output file
#     args[5] = Carrier name (e.g. Extract ASOF month)
#   Ex. python excel_writer_header_footer.py mcas.yml DEV_DB BUSINESS_VAULT d:/workspace/ mcas.xlsx ALLIANZ
#
# SUCCESS:
#   1. File is created in the specified folder location
#   2. Script logs: connection init, query, formatting, writing path, success
#   3. Script exits with status 0
# *****************************************************************************

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from typing import Any, Optional

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


class FileWriter:
    """Class to write data to an Excel file with header/footer and grouping support.

    parameters:
        output_path: str - The path where the output file will be saved
        output_file: str - The name of the output file
    """

    def __init__(self, params):
        self.output_path = params["output_path"]
        self.output_file = params["output_file"]
        self.sheetnames = params["sheetnames"]
        self.max_column_width = params.get("max_column_width")
        self.sorting_columns = params.get("sorting_columns")
        self.sheet_header_font = params.get("sheet_header_font")
        self.table_header_font = params.get("table_header_font")
        self.table_data_font = params.get("table_data_font")
        self.carrier_name = params["carrier_name"]
        self.report_name = params["report_name"]
        self.report_start_dt = params.get("report_start_dt")
        self.report_end_dt = params.get("report_end_dt")
        self.report_run_dt = params.get("report_run_dt")
        self.report_as_of_run_dt = params.get("report_as_of_run_dt")
        self.header = params.get("header")
        self.footer = params.get("footer")
        self.grouping_column = params.get("grouping_column")
        self.sub_grouping_column = params.get("sub_grouping_column")
        self.grp_sum = params.get("grp_sum")
        self.grp_cnt_label = params.get("grp_cnt_label")
        self.grp_sum_label = params.get("grp_sum_label")
        self.sub_group_cnt_label = params.get("sub_group_cnt_label")
        self.sub_group_sum_label = params.get("sub_group_sum_label")
        self.tot_cnt_label = params.get("tot_cnt_label")
        self.tot_sum_label = params.get("tot_sum_label")
        self.border_to_row = params.get("border_to_row")
        self.dollar_columns = params.get("dollar_columns")
        self.null_file_handling = params.get("null_file_handling", True)
        self.positive_dollar_format = "${:,.2f}"
        self.negative_dollar_format = "(${:,.2f})"

    def write_to_file(self, data):
        """Write data to an Excel file."""
        if self.grouping_column:
            return self.write_to_excel_with_grouping(data)
        return self.write_to_excel(data)

    def write_to_excel(self, data):
        """Write data to an Excel file without grouping."""
        wb = Workbook()
        del wb['Sheet']
        sheet_name = self.sheetnames[0] if isinstance(self.sheetnames, list) else self.sheetnames
        ws = wb.create_sheet(title=sheet_name)

        table_headers = data.columns
        last_column = data.shape[1]
        current_row = 1

        if self.header:
            self.add_header(ws, current_row, last_column)
            current_row += 5

        name, size, bold, color, alignment, wrap_text, fill_color, fill_type = self.set_cell_properties(
            self.table_header_font
        )
        for col, header in enumerate(table_headers, start=1):
            cell = ws.cell(row=current_row, column=col)
            cell.value = header
            cell.font = Font(name=name, size=size, bold=bold, color=color)
            cell.alignment = Alignment(horizontal=alignment, wrap_text=wrap_text)
            cell.fill = PatternFill(fill_type=fill_type, fgColor=fill_color)

        self.set_column_widths(ws, self.max_column_width)

        if data.empty:
            if self.null_file_handling:
                current_row += 1
                ws.cell(row=current_row, column=1, value="No data is available during this period")
        else:
            for row in data.values.tolist():
                current_row += 1
                for col, val in enumerate(row, start=1):
                    ws.cell(row=current_row, column=col, value=val)

        wb.save(os.path.join(self.output_path, self.output_file))
        return 'SUCCESS'

    def write_to_excel_with_grouping(self, data):
        """Write data to an Excel file with grouping."""
        wb = Workbook()
        del wb['Sheet']
        sheet_name = self.sheetnames[0] if isinstance(self.sheetnames, list) else self.sheetnames
        ws = wb.create_sheet(title=sheet_name)

        last_column = data.shape[1]
        current_row = 1
        font_settings = self.table_header_font

        if self.header:
            self.add_header(ws, current_row, last_column)
            current_row += 5

        for group in data[self.grouping_column].unique():
            current_row = self.apply_border(ws, current_row, last_column, self.border_to_row)
            self.write_group_name(ws, current_row, group, self.grouping_column, last_column)
            current_row = self.apply_border(ws, current_row + 1, last_column, self.border_to_row)

            group_data = data[data[self.grouping_column] == group].drop(columns=[self.grouping_column])
            sub_cols = self.sub_grouping_column if isinstance(self.sub_grouping_column, list) else [self.sub_grouping_column] if self.sub_grouping_column else []
            current_row = self.handle_sub_grouping(ws, group_data, sub_cols, current_row, last_column, font_settings)

            if self.grp_cnt_label is not None:
                self.write_group_count(ws, current_row, group_data, last_column, font_settings)
            if self.grp_sum_label is not None and self.grp_sum:
                self.write_group_sum(ws, current_row, group_data, last_column, font_settings)
            current_row += 2

        if self.tot_cnt_label is not None:
            current_row = self.write_total_count(ws, current_row, data, last_column, font_settings)
        if self.tot_sum_label is not None and self.grp_sum:
            current_row = self.write_total_sum(ws, current_row, data, last_column, font_settings)

        wb.save(os.path.join(self.output_path, self.output_file))
        return 'SUCCESS'

    def handle_sub_grouping(self, ws, group_data, sub_grouping_columns, current_row, last_column, font_settings):
        """Recursively handle sub-grouping of data."""
        if not sub_grouping_columns:
            group_data = self.apply_sorting(group_data, self.sorting_columns)
            group_data = self.apply_dollar_format(group_data)
            current_row = self.append_group_data(ws, group_data, current_row)
            current_row = self.apply_border(ws, current_row, last_column, self.border_to_row)
            return current_row

        sub_col = sub_grouping_columns[0]
        remaining = sub_grouping_columns[1:]

        for sub_group in group_data[sub_col].unique():
            current_row += 1
            self.write_group_name(ws, current_row, sub_group, sub_col, last_column)
            sub_group_data = group_data[group_data[sub_col] == sub_group].drop(columns=[sub_col])
            current_row = self.handle_sub_grouping(ws, sub_group_data, remaining, current_row + 1, last_column, font_settings)
            if self.sub_group_cnt_label is not None:
                self.write_group_count(ws, current_row, sub_group_data, last_column, font_settings)
            if self.sub_group_sum_label is not None and self.grp_sum:
                self.write_group_sum(ws, current_row, sub_group_data, last_column, font_settings)
            current_row += 2

        return current_row

    def append_group_data(self, ws, data, current_row):
        """Append grouped data rows to the worksheet."""
        for row in data.values.tolist():
            current_row += 1
            for col, val in enumerate(row, start=1):
                ws.cell(row=current_row, column=col, value=val)
        return current_row

    def write_group_name(self, ws, current_row, group, grouping_column, last_column):
        """Write the group name to the worksheet."""
        grp_nm_label = f"{grouping_column}: {group}"
        cell = ws.cell(row=current_row, column=1, value=grp_nm_label)
        name, size, bold, color, alignment, wrap_text, fill_color, fill_type = self.set_cell_properties(
            self.table_header_font
        )
        cell.font = Font(name=name, size=size, bold=bold, color=color)
        cell.alignment = Alignment(horizontal=alignment, wrap_text=wrap_text)
        ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=last_column)

    def write_group_count(self, ws, current_row, data, last_column, font_settings):
        """Write group count to the worksheet."""
        if self.grp_cnt_label is None:
            return
        cnt_label = f"{self.grp_cnt_label}"
        ws.cell(row=current_row, column=1, value=cnt_label)
        ws.cell(row=current_row, column=max(last_column // 2, 2), value=data.shape[0])
        ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=max(last_column // 2, 2))
        name, size, bold, color, alignment, wrap_text, fill_color, fill_type = self.set_cell_properties(font_settings)
        ws.cell(row=current_row, column=1).font = Font(name=name, size=size, bold=bold, color=color)
        ws.cell(row=current_row, column=max(last_column // 2, 2)).font = Font(name=name, size=size, bold=bold, color=color)
        ws.cell(row=current_row, column=1).alignment = Alignment(horizontal='center', wrap_text=True)
        ws.cell(row=current_row, column=max(last_column // 2, 2)).alignment = Alignment(horizontal='center', vertical='center')
        ws.row_dimensions[current_row].height = 30

    def write_group_sum(self, ws, current_row, data, last_column, font_settings):
        """Write group sum to the worksheet."""
        if self.grp_sum_label is None or self.grp_sum is None:
            return
        total_sum = data[self.grp_sum].sum()
        total_sum = self.apply_dollar_format(total_sum)
        sum_label = f"{self.grp_sum_label}"
        ws.cell(row=current_row, column=(last_column // 2) + 1, value=sum_label)
        ws.cell(row=current_row, column=last_column, value=total_sum)
        ws.merge_cells(start_row=current_row, start_column=(last_column // 2) + 1, end_row=current_row, end_column=last_column)
        name, size, bold, color, alignment, wrap_text, fill_color, fill_type = self.set_cell_properties(font_settings)
        ws.cell(row=current_row, column=(last_column // 2) + 1).font = Font(name=name, size=size, bold=bold, color=color)
        ws.cell(row=current_row, column=last_column).font = Font(name=name, size=size, bold=bold, color=color)
        ws.cell(row=current_row, column=(last_column // 2) + 1).alignment = Alignment(horizontal='right', wrap_text=True)
        ws.cell(row=current_row, column=last_column).alignment = Alignment(horizontal='right', vertical='center')

    def write_total_count(self, ws, current_row, data, last_column, font_settings):
        """Write total count to the worksheet."""
        if self.tot_cnt_label is None:
            return current_row
        tot_cnt_label = f"{self.tot_cnt_label}"
        ws.cell(row=current_row, column=1, value=tot_cnt_label)
        ws.cell(row=current_row, column=max(last_column // 2, 2), value=data.shape[0])
        ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=max(last_column // 2, 2))
        name, size, bold, color, alignment, wrap_text, fill_color, fill_type = self.set_cell_properties(font_settings)
        ws.cell(row=current_row, column=1).font = Font(name=name, size=size, bold=bold, color=color)
        ws.cell(row=current_row, column=max(last_column // 2, 2)).font = Font(name=name, size=size, bold=bold, color=color)
        ws.cell(row=current_row, column=1).alignment = Alignment(horizontal='center', wrap_text=True)
        ws.cell(row=current_row, column=max(last_column // 2, 2)).alignment = Alignment(horizontal='center', vertical='center')
        ws.row_dimensions[current_row].height = 30
        return current_row

    def write_total_sum(self, ws, current_row, data, last_column, font_settings):
        """Write total sum to the worksheet."""
        if self.tot_sum_label is None or self.grp_sum is None:
            return current_row
        tot_sum_label = f"{self.tot_sum_label}"
        total_sum = data[self.grp_sum].sum()
        total_sum = self.apply_dollar_format(total_sum)
        ws.cell(row=current_row, column=(last_column // 2) + 1, value=tot_sum_label)
        ws.cell(row=current_row, column=last_column, value=total_sum)
        ws.merge_cells(start_row=current_row, start_column=(last_column // 2) + 1, end_row=current_row, end_column=last_column)
        name, size, bold, color, alignment, wrap_text, fill_color, fill_type = self.set_cell_properties(font_settings)
        ws.cell(row=current_row, column=(last_column // 2) + 1).font = Font(name=name, size=size, bold=bold, color=color)
        ws.cell(row=current_row, column=last_column).font = Font(name=name, size=size, bold=bold, color=color)
        ws.cell(row=current_row, column=(last_column // 2) + 1).alignment = Alignment(horizontal='right', wrap_text=True)
        ws.cell(row=current_row, column=last_column).alignment = Alignment(horizontal='right', vertical='center')
        return current_row + 1

    def apply_dollar_format(self, data):
        """Apply dollar formatting to data (DataFrame or scalar)."""
        if isinstance(data, pd.DataFrame):
            if self.dollar_columns:
                for col in self.dollar_columns:
                    if col in data.columns:
                        data[col] = data[col].apply(
                            lambda x: self.positive_dollar_format.format(x) if x > 0 else self.negative_dollar_format.format(abs(x)) if pd.notna(x) else ''
                        )
            return data
        if isinstance(data, (int, float)):
            return self.positive_dollar_format.format(data) if data > 0 else self.negative_dollar_format.format(abs(data))
        return data

    def apply_sorting(self, grouped_data, sorting_columns):
        """Sort grouped data by specified columns."""
        if sorting_columns:
            grouped_data = grouped_data.sort_values(by=sorting_columns, ascending=True)
        return grouped_data

    def apply_border(self, ws, current_row, last_column, border_to_row):
        """Apply border to a row."""
        if not border_to_row:
            return current_row
        return self.apply_border_to_row(ws, current_row, last_column, border_to_row)

    def apply_border_to_row(self, ws, current_row, last_column, border_to_row):
        """Apply a thin border and fill to a row."""
        if not border_to_row:
            return current_row
        start_color = border_to_row.get('start_color', '00000000')
        end_color = border_to_row.get('end_color', '00000000')
        fill_type = border_to_row.get('fill_type', 'none')
        thin_border = Border(top=Side(style='thin'), bottom=Side(style='thin'))

        ws.row_dimensions[current_row].height = 1
        for col_num in range(1, last_column + 1):
            cell = ws.cell(row=current_row, column=col_num)
            cell.border = thin_border
            cell.fill = PatternFill(start_color=Color(rgb=start_color), end_color=Color(rgb=end_color), fill_type=fill_type)
        return current_row + 1

    def add_header(self, ws, current_row, last_column):
        """Add report header to the worksheet."""
        name, size, bold, color, alignment, wrap_text, fill_color, fill_type = self.set_cell_properties(
            self.sheet_header_font
        )
        total_pages = len(self.sheetnames) if isinstance(self.sheetnames, list) else 1

        for row in range(current_row, current_row + 3):
            cell = ws.cell(row=row, column=1)
            if row == current_row:
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
                cell.value = self.report_name
                ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=last_column // 2)
                cell.font = Font(name=name, size=size, bold=bold, color=color)
                cell.alignment = Alignment(horizontal=alignment, wrap_text=wrap_text)
                cell.fill = PatternFill(fill_type=fill_type, fgColor=fill_color)

                page_info = "Page 1 of 1"
                cell_offset = last_column // 2
                page_cell = ws.cell(row=row, column=cell_offset + 1)
                page_cell.value = page_info
                ws.merge_cells(start_row=row, start_column=cell_offset + 1, end_row=row, end_column=last_column)
                page_cell.font = Font(name=name, size=size, bold=bold, color=color)
                page_cell.alignment = Alignment(horizontal='right', wrap_text=wrap_text)
                page_cell.fill = PatternFill(fill_type=fill_type, fgColor=fill_color)

            else:
                if self.report_start_dt and self.report_end_dt:
                    try:
                        start_date = datetime.strptime(str(self.report_start_dt), '%Y-%m-%d %H:%M:%S.%f').strftime("%m/%d/%Y")
                    except ValueError:
                        start_date = datetime.strptime(str(self.report_start_dt), '%Y-%m-%d %H:%M:%S').strftime("%m/%d/%Y")
                    try:
                        end_date = datetime.strptime(str(self.report_end_dt), '%Y-%m-%d %H:%M:%S.%f').strftime("%m/%d/%Y")
                    except ValueError:
                        end_date = datetime.strptime(str(self.report_end_dt), '%Y-%m-%d %H:%M:%S').strftime("%m/%d/%Y")
                    cell.value = f"For Dates: {start_date} To {end_date}"
                elif self.report_as_of_run_dt:
                    try:
                        report_date = datetime.strptime(str(self.report_as_of_run_dt), '%Y-%m-%d %H:%M:%S.%f').strftime("%m/%d/%Y")
                    except ValueError:
                        try:
                            report_date = datetime.strptime(str(self.report_as_of_run_dt), '%Y-%m-%d %H:%M:%S').strftime("%m/%d/%Y")
                        except ValueError:
                            report_date = str(self.report_as_of_run_dt)
                    cell.value = f"Report as Date: {report_date}"
                else:
                    cell.value = f"Report as Date: {datetime.now().strftime('%m/%d/%Y')}"
                ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=last_column)
                cell.font = Font(name=name, size=size, bold=bold, color=color)
                cell.alignment = Alignment(horizontal=alignment, wrap_text=wrap_text)
                cell.fill = PatternFill(fill_type=fill_type, fgColor=fill_color)

    def set_column_widths(self, ws, max_column_width):
        """Set column widths in the worksheet."""
        width = max_column_width or self.max_column_width or 15
        for col in range(1, ws.max_column + 1):
            col_index = get_column_letter(col)
            ws.column_dimensions[col_index].width = width

    def set_cell_properties(self, font):
        """Extract font/cell properties from a font configuration dict."""
        if not font or not isinstance(font, dict):
            return 'Calibri', 11, False, '000000', 'general', False, '00000000', 'none'
        name = font.get('name', 'Calibri')
        size = font.get('size', 11)
        bold = font.get('bold', False)
        color = font.get('color', '000000')
        wrap_text = font.get('wrap_text', False)
        fill_color = font.get('fill_color', '00000000')
        fill_type = font.get('fill_type', 'none')
        alignment = font.get('alignment', 'general')
        return name, size, bold, color, alignment, wrap_text, fill_color, fill_type


class Datapreprocessor:
    """Fetch and process data from Snowflake using SnowparkConnector.

    parameters:
        connector: SnowparkConnector - The Snowpark connector (use within context manager)
        database: str - The Snowflake database
        schema: str - The Snowflake schema
        pre_sql_query: str - SQL to set session variables
        tables_list: list - List of tables (first used for single-sheet)
    """

    def __init__(
        self,
        connector: SnowparkConnector,
        database: str,
        schema: str,
        pre_sql_query: str,
        tables_list: list[str] | str,
    ) -> None:
        self.connector = connector
        self.database = database
        self.schema = schema
        self.pre_sql_query = pre_sql_query or ''
        tables = tables_list if isinstance(tables_list, list) else [tables_list]
        self.tables_list: list[str] = tables
        self.table: Optional[str] = tables[0] if tables else None

        logging.info("Using Snowpark connection")
        logging.info(f"Active Database.Schema is {self.database}.{self.schema}")

    def fetch_data(
        self,
        exclude_columns: Optional[list[str]] = None,
        filter_rows: Optional[str] = None,
        sorting_columns: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """Fetch data from the Snowflake database using SnowparkConnector."""
        exclude_columns = exclude_columns or []
        if not self.table:
            return pd.DataFrame()

        for statement in self.pre_sql_query.split('\n'):
            if statement.strip():
                self.connector.execute_query(statement, lazy=False)
                logging.info(f"Executed statement: {statement}")

        columns = ','.join(
            ['*'] if not exclude_columns else [f'* EXCLUDE("{col}")' for col in exclude_columns]
        )
        if filter_rows:
            query = f"SELECT {columns} FROM {self.table} WHERE {filter_rows}"
        else:
            query = f"SELECT {columns} FROM {self.table}"

        if sorting_columns:
            order_by_clause = ', '.join(
                f'"{col}"' if not (str(col).startswith('"') and str(col).endswith('"')) else col
                for col in sorting_columns
            )
            query += f" ORDER BY {order_by_clause}"

        logging.info(f"Query statement {query}")
        result = self.connector.execute_query(query, lazy=False)

        if result is None or len(result) == 0:
            return pd.DataFrame()
        df = pd.DataFrame([row.as_dict() for row in result])
        logging.info(f"Data fetched from {self.table}")
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
    """Validate the report configuration yml file."""
    if not report:
        logging.error("Error: Report configuration file is empty.")
        sys.exit(1)
    if not isinstance(report, dict):
        logging.error("Error: Report configuration file is not a dictionary.")
        sys.exit(1)
    for key in ['carrier_name', 'report_name', 'tables_list', 'sheetnames', 'pre_sql_query']:
        if key not in report:
            logging.error(f"Error: {key} key is missing in the report configuration file.")
            sys.exit(1)


def parse_and_validate_args() -> argparse.Namespace:
    """Parse and validate command line arguments."""
    parser = argparse.ArgumentParser(
        description="Required arguments: report, database, schema, output_path, output_file, carrier_name"
    )
    parser.add_argument("report", help="Report config file, e.g. mcas.yml", type=str)
    parser.add_argument("database", help="Snowflake database, e.g. DEV_SNOWFLAKE_WAREHOUSE", type=str)
    parser.add_argument("schema", help="Snowflake schema, e.g. BUSINESS_VAULT", type=str)
    parser.add_argument("output_path", help="Path where output will be saved, e.g. c:/workspace/", type=str)
    parser.add_argument("output_file", help="Output file name, e.g. mcas.xlsx", type=str)
    parser.add_argument("carrier_name", help="Carrier name, e.g. ALLIANZ_ADMIN_088", type=str)
    parser.add_argument(
        "--warehouse",
        help="Snowflake warehouse name (or set SNOWFLAKE_WAREHOUSE env var)",
        type=str,
        default=os.environ.get("SNOWFLAKE_WAREHOUSE"),
    )
    parser.add_argument("--as_of_run_dt", help="ASOF month, e.g. 12/31/2023", type=str, default=None)
    parser.add_argument("--report_start_dt", help="Start date, e.g. 01/01/2023", type=str, default=None)
    parser.add_argument("--report_end_dt", help="End date, e.g. 12/31/2023", type=str, default=None)
    parser.add_argument("--report_run_dt", help="Run date, e.g. 12/31/2023", type=str, default=None)

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

    _, ext = os.path.splitext(args.output_file)
    if ext.lower() not in VALID_EXTENSIONS:
        logging.warning(f"WARNING: {args.output_file} does not have a standard file extension.")

    return args


def main() -> None:
    """Main function."""
    args = parse_and_validate_args()
    print(args.report)

    report_validation = validate_report_configextension(args.report)
    report = load_report_config(report_validation)
    validate_report(report)

    carrier_name = report['carrier_name']
    report_name = report['report_name']
    tables_list = report['tables_list']
    sheetnames = report['sheetnames']
    pre_sql_query = report['pre_sql_query'].format(
        carrier_name=args.carrier_name,
        as_of_run_dt=args.as_of_run_dt or '',
        report_start_dt=args.report_start_dt or '',
        report_end_dt=args.report_end_dt or '',
        report_run_dt=args.report_run_dt or ''
    )

    header = report.get('header', None)
    footer = report.get('footer', None)
    sorting_columns: Optional[list[str]] = report.get('sorting_columns', None)
    exclude_columns: Optional[list[str]] = report.get('exclude_columns', None)
    filter_rows: Optional[str] = report.get('filter_rows', None)
    grouping_column = report.get('grouping_column', None)
    sub_grouping_column = report.get('sub_grouping_column', None)
    grp_sum = report.get('grp_sum', None)
    grp_cnt_label = report.get('grp_cnt_label', None)
    grp_sum_label = report.get('grp_sum_label', None)
    sub_group_cnt_label = report.get('sub_group_cnt_label', None)
    sub_group_sum_label = report.get('sub_group_sum_label', None)
    tot_cnt_label = report.get('tot_cnt_label', None)
    tot_sum_label = report.get('tot_sum_label', None)
    border_to_row = report.get('border_to_row', None)
    dollar_columns = report.get('dollar_columns', None)
    max_column_width = report.get('max_column_width', None)
    sheet_header_font = report.get('sheet_header_font', None)
    table_header_font = report.get('table_header_font', None)
    table_data_font = report.get('table_data_font', None)
    null_file_handling = report.get('null_file_handling', True)

    report_start_dt = args.report_start_dt if args.report_start_dt else None
    report_end_dt = args.report_end_dt if args.report_end_dt else None
    report_as_of_run_dt = args.as_of_run_dt if args.as_of_run_dt else None
    report_run_dt = args.report_run_dt if args.report_run_dt else None

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
            tables_list=tables_list,
        )
        df = dp.fetch_data(exclude_columns, filter_rows, sorting_columns)

        params: dict[str, Any] = {
            'output_path': args.output_path,
            'output_file': args.output_file,
            'sheetnames': sheetnames,
            'max_column_width': max_column_width,
            'sorting_columns': sorting_columns,
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
            'grouping_column': grouping_column,
            'sub_grouping_column': sub_grouping_column,
            'grp_sum': grp_sum,
            'grp_cnt_label': grp_cnt_label,
            'grp_sum_label': grp_sum_label,
            'sub_group_cnt_label': sub_group_cnt_label,
            'sub_group_sum_label': sub_group_sum_label,
            'tot_cnt_label': tot_cnt_label,
            'tot_sum_label': tot_sum_label,
            'border_to_row': border_to_row,
            'dollar_columns': dollar_columns,
            'null_file_handling': null_file_handling,
        }

        writer = FileWriter(params)
        writer.write_to_file(df)
        logging.info(f"Output will be saved to: {args.output_path}{args.output_file}")


if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    execution_time = end_time - start_time
    hours = int(execution_time // 3600)
    minutes = int((execution_time % 3600) // 60)
    seconds = int(execution_time % 60)
    milliseconds = int((execution_time % 1) * 1000)
    print("***********************************************************************************************")
    print(f'Execution Time For Generating the Feed: {hours} hr {minutes} min {seconds} sec {milliseconds} ms.')
    print("***********************************************************************************************")
    logging.info(f'Execution Time For Generating the Feed: {hours} hr {minutes} min {seconds} sec {milliseconds} ms')
