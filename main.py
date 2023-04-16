import numpy as np
import pandas as pd

from owlready2 import *

from utils import cast_to_number, \
    find_data_property_match, find_object_property_match,\
    find_object_match, find_class_match, \
    export_data_to_ontology, load_ontology
from parsers import get_tables_from_excel

ontology_data = None
terms = {}


def analyze_cells(table_data):
    class_column_idx = -1
    for col_idx, column in enumerate(table_data.table):
        for row_idx, cell_val in enumerate(table_data.table[column]):
            if cell_val is None or cell_val == "" or cell_val == "-":
                continue

            if type(cell_val) is datetime.datetime:
                cell_type = 'Date'
                table_data.col2predicate[col_idx] = cell_type
                table_data.row2date[row_idx] = cell_val
            else:
                cell_val_number = None
                if type(cell_val) is str:
                    cast_to_number(cell_val)
                try:
                    if class_column_idx == col_idx \
                            and col_idx in table_data.col2predicate \
                            and table_data.col2predicate[col_idx] == 'Observation_Post':
                        cell_val = str(cell_val)
                        raise ValueError('It must be a new object')
                    if cell_val_number is not None:
                        float_val = cell_val_number
                    else:
                        float_val = float(cell_val)
                    table_data.table.iloc[row_idx, col_idx] = float_val
                    cell_type = 'Number'
                except ValueError:
                    cell_val_lower = cell_val.lower().strip()
                    cell_type = 'Text'

                    founded_object = find_object_match(ontology_data, cell_val_lower)
                    if class_column_idx == col_idx \
                            and col_idx in table_data.col2predicate \
                            and table_data.col2predicate[col_idx] == 'Observation_Post':
                        post_name = f'Post_{cell_val_lower}'
                        post_object = find_object_match(ontology_data, post_name, max_dist=0)
                        if post_object is None:
                            created_post = ontology_data.ontology.Observation_Post(post_name)
                            ontology_data.name2object[created_post.name.lower()] = {
                                "individual": created_post,
                                "is_instance_of": "|"
                            }
                            ontology_data.object_names.append(created_post.name.lower())
                        else:
                            post_name = post_object
                        if row_idx not in table_data.row2object:
                            cell_type = ontology_data.name2object[post_name.lower()]['individual'].name
                            table_data.row2object[row_idx] = ontology_data.name2object[post_name.lower()]['individual']
                    elif founded_object is not None:
                        if row_idx not in table_data.row2object:
                            if class_column_idx == col_idx or class_column_idx == -1:
                                cell_type = ontology_data.name2object[founded_object]['individual'].name
                                table_data.row2object[row_idx] = ontology_data.name2object[founded_object]['individual']

                    cell_type_candidate = find_data_property_match(ontology_data, cell_val)
                    if cell_type_candidate is None:
                        cell_type_candidate = find_object_property_match(ontology_data, cell_val)
                    if cell_type_candidate is None:
                        cell_type_candidate = find_class_match(ontology_data, cell_val)
                    if cell_type_candidate is None:
                        cell_type_candidate = ontology_data.tag2property.get(cell_val, None)

                    if class_column_idx == -1 and cell_type_candidate in ontology_data.class_names:
                        cell_type = f'Class_{cell_type_candidate}'
                        class_column_idx = col_idx
                        # col2tag[col_idx] = cell_type

                        if cell_type_candidate == 'Observation_Post':
                            table_data.col2predicate[col_idx] = cell_type_candidate

                    elif cell_type_candidate is not None:
                        cell_type = cell_type_candidate
                        table_data.col2predicate[col_idx] = cell_type
                        # print(f'Cell value "{cell_val}" classified as {cell_type}')

            table_data.markup.iloc[row_idx, col_idx] = cell_type


def detect_year(table_data, row, row_idx, column_count):
    numbers_in_row = 0
    previous_number = None
    for col_idx, cell in enumerate(row):
        if table_data.markup.iloc[row_idx, col_idx] == 'Number':
            numbers_in_row += 1
            if previous_number is None:
                previous_number = round(cell)
            elif cell == previous_number + 1:
                previous_number = round(cell)
            else:
                return False
        elif table_data.markup.iloc[row_idx, col_idx] == 'Text':
            # print(f'Text value: "{cell}"')
            pass

    if numbers_in_row < column_count * 0.9:
        return False

    for col_idx, cell in enumerate(row):
        if table_data.markup.iloc[row_idx, col_idx] == 'Number':
            table_data.markup.iloc[row_idx, col_idx] = 'Year'
            table_data.col2predicate[col_idx] = round(cell)
    return True


def detect_date(table_data, row, row_idx):
    for col_idx, cell in enumerate(row):
        if table_data.markup.iloc[row_idx, col_idx] == 'Date':
            return True
    return False


def analyze_rows(table_data):
    row_count, column_count = table_data.table.shape
    has_year = False
    has_date = False
    for row_idx, row in table_data.table.iterrows():
        if detect_year(table_data, row, row_idx, column_count):
            has_year = True
        if detect_date(table_data, row, row_idx):
            has_date = True
    if not (has_year or has_date):
        table_data.target_tag = None


def analyze_file(input_file, output_folder):
    print(f'Handling: {input_file}')
    if os.path.splitext(input_file)[1] == '.xlsx':
        for table_data in get_tables_from_excel(input_file):
            print(f'Sheet: {table_data.sheet_name} handling started')
            analyze_table(table_data)
            markup_save_path = os.path.join(output_folder, table_data.sheet_name + '.xlsx')
            table_data.markup.to_excel(markup_save_path, columns=table_data.table.columns)
            print(f'Saved marked up table into {markup_save_path}')

            print(f'Sheet {table_data.sheet_name} handling ended.\n')
    else:
        raise ValueError(f'Unsupported file format: {os.path.splitext(input_file)[1]}')


def analyze_table(table_data):
    columns = list(table_data.table.columns)
    row_count, _ = table_data.table.shape

    table_data.table.replace({np.nan: None}, inplace=True)

    table_data.markup = pd.DataFrame('', index=np.arange(row_count), columns=columns)

    analyze_cells(table_data)
    analyze_rows(table_data)

    export_data_to_ontology(table_data, ontology_data)




def main():
    global ontology_data
    ontology_path = "ontologies/Kaz_Water_Ontology_modified_fixed.owl"

    # load_terms()
    ontology_data = load_ontology(ontology_path)

    # excel_files = ['Water_Basins_KZ.xlsx', 'Population_KZ.xlsx']
    # excel_files = ['Water_Basins_KZ.xlsx']
    excel_files = ['Water_Basins_KZ.xlsx', 'Population_KZ.xlsx', 'GBD_Water_Consumption', 'GBD_Water_Level_IBB']
    base_path = '/Users/titrom/Desktop/Диплом/Tables/PyTableMiner'
    tables_path = os.path.join(base_path, 'tables')
    processed_path = os.path.join(base_path, 'processed')

    if not os.path.exists(processed_path):
        os.mkdir(processed_path)

    for path in excel_files:
        target_path = os.path.join(tables_path, path)
        if os.path.isdir(target_path):
            files = [os.path.join(path, file) for file in os.listdir(target_path)]
            output_path = os.path.join(processed_path, path)
            if not os.path.exists(output_path):
                os.makedirs(output_path)
        else:
            files = [path]

        print(files)

        for file_idx, excel_file_path in enumerate(files):
            print(f'File {file_idx + 1}/{len(files)}')
            input_excel_file = os.path.join(tables_path, excel_file_path)
            analyze_file(input_excel_file, processed_path)
            if file_idx == 2:
                break

    ontology_data.save2newfile()


if __name__ == '__main__':
    main()

