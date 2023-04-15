import pandas as pd

from utils import TableData


def get_tables_from_excel(input_excel_file):
    with pd.ExcelFile(input_excel_file) as excel_file:
        for sheet_name in excel_file.sheet_names:
            target_tag = sheet_name.replace('_KZ', '')
            if 'GBD_Water_Consumption' in input_excel_file:
                target_tag = 'Water_Consumption'
            elif 'GBD_Water_Level_IBB' in input_excel_file:
                target_tag = 'Water_Level'
            table = excel_file.parse(sheet_name)
            column_names = []
            has_useful_names = False
            for name in table.columns:
                if 'Unnamed' in name:
                    column_names.append('')
                else:
                    column_names.append(name)
                    has_useful_names = True

            if has_useful_names:
                table.loc[-1] = column_names
                table.index = table.index + 1
                table.sort_index(inplace=True)

            table_data = TableData()
            table_data.target_tag = target_tag
            table_data.table = table
            table_data.sheet_name = sheet_name

            yield table_data
