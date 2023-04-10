import csv
import os
import time
import numpy as np
import pandas as pd
import requests

from nltk.stem import snowball
from owlready2 import *

from Levenshtein import distance as levenshtein_distance


TARGET_TAG = None
IGNORED_TAGS = ["Urban_Basins_Population", "Rural_Basins_Population"]

onto = None
object_names = []
data_properties = []
object_properties = []
class_names = []

name2data_property = {}
name2object_property = {}
name2object = {}

row2object = {}
row2predicate = {}
row2date = {}

col2object = {}
col2predicate = {}
col2date = {}

terms = {}
tag2property = {
    'Square (sq km)': 'Square',
    'Square, km²': 'Square',
    'Surface area': 'Lake_Square',
    'Water_resources_KZ (cubic meter)': 'Water_resources_KZ',
    'Regions_KZ': 'Located_in',
    'Regions': 'Located_in',
    'Basins_Population': 'Basins_Population',
    'Urban_Basins_Population': 'Urban_Basins_Population',
    'Rural_Basins_Population': 'Rural_Basins_Population',
    'Rivers_of_Basins': 'Basin_has_River',
    'River_length_in_KZ (km)': 'River_length_in_KZ',
    'Rivers_length, km': 'River_length_in_KZ',
    'Lakes_KZ': 'Lake',
    'Average_annual_water_consumption, m3/s': 'Average_annual_water_consumption',
    'Water_and_energy_resources, Power, thousand kW': 'Water_and_energy_resources_Power',
    'Water and energy resources, Energy, million kWh/year': 'Water_and_energy_resources_Energy',
    'River_fall, m': 'River_Fall',
    'Код поста': 'Observation_Post',
    'Дата' : 'Date',
    'Значение': 'Value'
}

inverse_property_map = {
    'Located_in': 'Contains',
    'Contains': 'Located_in',
    'River_in_Basin': 'Basin_has_River',
    'Basin_has_River': 'River_in_Basin',
}

property2class = {
    'Located_in' : 'Region'
}


def cast_to_number(value):
    if type(value) in [int, float]:
        value_number = value
    elif type(value) is str:
        try:
            value_number = float(value)
        except Exception:
            value_number = None
    else:
        raise TypeError(f'Unexpected type {type(value)} in cast_to_number method')

    value_str = str(value)
    if 'млн' in value_str or 'km' in value_str:
        value_str = value_str\
            .replace('млн', '') \
            .replace('km', '') \
            .replace(' ', '') \
            .replace(',', '.') \
            .strip()
        try:
            value_number = float(value_str) * 1000000
        except Exception:
            pass
    if 'square kilometre' in value_str:
        value_str = value_str[0:value_str.index('square kilometre')]
        value_str = value_str.replace(',', '.').strip()
        try:
            value_number = float(value_str)
        except Exception:
            pass

    return value_number


def set_property(subject, predicate, object, year=None, date=None):
    try:
        if date is not None:
            timestamp = onto.Timestamp(f'~{subject.name}_'
                                       f'{predicate}_'
                                       f'{date.year}_'
                                       f'{date.month}_'
                                       f'{date.day}',
                                       namespace=onto,
                                       Timestamp_Year=int(date.year),
                                       Timestamp_Month=int(date.month),
                                       Timestamp_Day=int(date.day),
                                       Timestamp_Value=float(object))
            if subject.__getattr__(predicate) is None:
                subject.__setattr__(predicate, [timestamp])
            else:
                subject.__getattr__(predicate).append(timestamp)
        else:
            if year is None:
                if subject.__getattr__(predicate) is None:
                    subject.__setattr__(predicate, [object])
                else:
                    subject.__getattr__(predicate).append(object)
            else:
                timestamp = onto.Timestamp(f'~year_{year}_'
                                           f'{subject.name}_'
                                           f'{predicate}_'
                                           f'{object}',
                                           namespace=onto,
                                           Timestamp_Year=year,
                                           Timestamp_Value=object)
                if subject.__getattr__(predicate) is None:
                    subject.__setattr__(predicate, [timestamp])
                else:
                    subject.__getattr__(predicate).append(timestamp)
    except Exception as e:
        print('e', e)


def analyze_cells(table, markup):
    class_column_idx = -1
    for col_idx, column in enumerate(table):
        for row_idx, cell_val in enumerate(table[column]):
            if cell_val is None or cell_val == "" or cell_val == "-":
                continue

            if type(cell_val) is datetime.datetime:
                cell_type = 'Date'
                col2predicate[col_idx] = cell_type
                row2date[row_idx] = cell_val
            else:
                cell_val_number = None
                if type(cell_val) is str:
                    cast_to_number(cell_val)
                try:
                    if class_column_idx == col_idx \
                            and col_idx in col2predicate \
                            and col2predicate[col_idx] == 'Observation_Post':
                        cell_val = str(cell_val)
                        raise ValueError('It must be a new object')
                    if cell_val_number is not None:
                        float_val = cell_val_number
                    else:
                        float_val = float(cell_val)
                    table.iloc[row_idx, col_idx] = float_val
                    cell_type = 'Number'
                except ValueError:
                    cell_val_lower = cell_val.lower().strip()
                    cell_type = 'Text'

                    founded_object = find_object_match(cell_val_lower)
                    if class_column_idx == col_idx \
                            and col_idx in col2predicate \
                            and col2predicate[col_idx] == 'Observation_Post':
                        post_name = f'Post_{cell_val_lower}'
                        post_object = find_object_match(post_name, max_dist=0)
                        if post_object is None:
                            created_post = onto.Observation_Post(post_name)
                            name2object[created_post.name.lower()] = {
                                "individual": created_post,
                                "is_instance_of": "|"
                            }
                            object_names.append(created_post.name.lower())
                        else:
                            post_name = post_object
                        if row_idx not in row2object:
                            cell_type = name2object[post_name.lower()]['individual'].name
                            row2object[row_idx] = name2object[post_name.lower()]['individual']
                    elif founded_object is not None:
                        if row_idx not in row2object:
                            if class_column_idx == col_idx or class_column_idx == -1:
                                cell_type = name2object[founded_object]['individual'].name
                                row2object[row_idx] = name2object[founded_object]['individual']

                    cell_type_candidate = find_data_property_match(cell_val)
                    if cell_type_candidate is None:
                        cell_type_candidate = find_object_property_match(cell_val)
                    if cell_type_candidate is None:
                        cell_type_candidate = find_class_match(cell_val)
                    if cell_type_candidate is None:
                        cell_type_candidate = tag2property.get(cell_val, None)

                    if class_column_idx == -1 and cell_type_candidate in class_names:
                        cell_type = f'Class_{cell_type_candidate}'
                        class_column_idx = col_idx
                        # col2tag[col_idx] = cell_type

                        if cell_type_candidate == 'Observation_Post':
                            col2predicate[col_idx] = cell_type_candidate

                    elif cell_type_candidate is not None:
                        cell_type = cell_type_candidate
                        col2predicate[col_idx] = cell_type
                        # print(f'Cell value "{cell_val}" classified as {cell_type}')

            markup.iloc[row_idx, col_idx] = cell_type


def detect_year(markup, row, row_idx, column_count):
    numbers_in_row = 0
    previous_number = None
    for col_idx, cell in enumerate(row):
        if markup.iloc[row_idx, col_idx] == 'Number':
            numbers_in_row += 1
            if previous_number is None:
                previous_number = round(cell)
            elif cell == previous_number + 1:
                previous_number = round(cell)
            else:
                return False
        elif markup.iloc[row_idx, col_idx] == 'Text':
            # print(f'Text value: "{cell}"')
            pass

    if numbers_in_row < column_count * 0.9:
        return False

    for col_idx, cell in enumerate(row):
        if markup.iloc[row_idx, col_idx] == 'Number':
            markup.iloc[row_idx, col_idx] = 'Year'
            col2predicate[col_idx] = round(cell)
    return True


def detect_date(markup, row, row_idx):
    for col_idx, cell in enumerate(row):
        if markup.iloc[row_idx, col_idx] == 'Date':
            return True
    return False


def analyze_rows(table, markup):
    global TARGET_TAG
    row_count, column_count = table.shape
    has_year = False
    has_date = False
    for row_idx, row in table.iterrows():
        if detect_year(markup, row, row_idx, column_count):
            has_year = True
        if detect_date(markup, row, row_idx):
            has_date = True
    if not (has_year or has_date):
        TARGET_TAG = None


def find_best_property(property_list, tag, max_dist=0.3):
    tag = tag.replace('_GBD', '')
    best_match = None
    best_match_dist = 4
    for prop in property_list:
        dist = levenshtein_distance(prop, tag)
        if dist < best_match_dist:
            best_match_dist = dist
            best_match = prop

    if best_match_dist > len(tag) * max_dist:
        return None
    return best_match


def find_data_property_match(tag, individual=None, max_dist=0.3):
    specific_class_tag = None
    if individual is not None:
        class_name = individual.is_a[0].name
        specific_class_tag = find_best_property(data_properties, f'{class_name}_{tag}')
    return specific_class_tag if specific_class_tag else find_best_property(data_properties, f'{tag}', max_dist)


def find_object_property_match(tag, individual=None, max_dist=0.3):
    specific_class_tag = None
    if individual is not None:
        class_name = individual.is_a[0].name
        specific_class_tag = find_best_property(object_properties, f'{class_name}_{tag}')
    return specific_class_tag if specific_class_tag else find_best_property(object_properties, f'{tag}', max_dist)


def find_object_match(tag, property_name=None, max_dist=0.3):
    specific_class_tag = None
    if property_name is not None and property_name in property2class:
        property_class = property2class[property_name].lower()
        specific_class_tag = find_best_property(object_names, f'{tag}_{property_class}')
    return specific_class_tag if specific_class_tag else find_best_property(object_names, f'{tag}', max_dist)


def find_class_match(tag):
    return find_best_property(class_names, f'{tag}')


def write_table_to_ontology(table, markup):
    for col_idx, column in enumerate(table):
        if col_idx not in col2predicate:
            continue
        if 'Observation_Post' == col2predicate[col_idx]:
            continue
        for row_idx, cell_val in enumerate(table[column]):
            if row_idx not in row2object:
                continue
            if markup.iloc[row_idx, col_idx] == 'Date':
                continue
            # try:
            value = table.iloc[row_idx, col_idx]
            # except Exception:
            #     value = table.iloc[row_idx, col_idx]

            if value is None:
                continue

            individual = row2object[row_idx]

            if TARGET_TAG:
                year = col2predicate[col_idx]
                try:
                    float(year)
                except Exception:
                    year = None
                try:
                    float(value)
                except Exception:
                    continue

                data_property_name = find_data_property_match(TARGET_TAG, individual=individual)
                object_property_name = find_object_property_match(TARGET_TAG, individual=individual)

                if data_property_name is None and object_property_name is None:
                    if TARGET_TAG not in IGNORED_TAGS:
                        print(f'    Property "{TARGET_TAG}" of "{individual.name}" was not found')
                else:
                    if data_property_name:
                        prop_range = name2data_property[data_property_name].range
                        if prop_range is not None:
                            if len(prop_range) == 1:
                                val_type = prop_range[0]
                                try:
                                    if val_type in [float, int]:
                                        value_casted = cast_to_number(value)
                                        if value_casted is not None:
                                            value = value_casted
                                    val_modified_type = val_type(value)
                                    value = val_modified_type
                                    set_property(individual, data_property_name, value, year=year)
                                    markup.iloc[row_idx, col_idx] = f'{year}_{value}'
                                except Exception:
                                    print(f'    Unable to cast value "{value}" to type {val_type} for data property {data_property_name}')
                            else:
                                raise ValueError(f'    prop_range has length {len(prop_range)} - {prop_range} for property {data_property_name}')
                        else:
                            # set_property(individual, data_property_name, value)
                            raise ValueError(f'    Property not set')
                    elif object_property_name:
                        prop_range = name2object_property[object_property_name].range
                        if prop_range is not None:
                            if len(prop_range) == 1:
                                val_type = prop_range[0]
                                try:
                                    if type(individual) is onto.Observation_Post and object_property_name in ['Water_Consumption', 'Water_Level']:
                                        row_date = None
                                        if row_idx in row2date:
                                            row_date = row2date[row_idx]
                                        set_property(individual, object_property_name, value, date=row_date)
                                        markup.iloc[row_idx, col_idx] = f'Date_{value}'
                                    else:
                                        set_property(individual, object_property_name, value, year=year)
                                        markup.iloc[row_idx, col_idx] = f'{year}_{value}'
                                except Exception:
                                    print(f'    Unable to cast value "{value}" to type {val_type} for object property {object_property_name}')
                            else:
                                raise ValueError(f'    prop_range has length {len(prop_range)} - {prop_range}')

                        # raise NotImplementedError('object_property_name handling not implemented')

                # if individual.__getattr__(TARGET_TAG) is None:
                #     try:
                #         individual.__setattr__(TARGET_TAG, [f'{year}_{value}'])
                #     except Exception:
                #         individual.__setattr__(TARGET_TAG, f'{year}_{value}')
                # else:
                #     try:
                #         individual.__getattr__(TARGET_TAG).append(f'{year}_{value}')
                #     except Exception:
                #         str_prop = individual.__getattr__(TARGET_TAG)
                #         individual.__setattr__(TARGET_TAG, str_prop + f'{year}_{value}|')
            else:
                tag = col2predicate[col_idx]
                # print(f'Property {tag}, '
                #       f'Val {value}, '
                #       f'Individual: {individual}')

                if tag in tag2property:
                    tag = tag2property[tag]
                data_property_name = find_data_property_match(tag, individual=individual)
                object_property_name = find_object_property_match(tag, individual=individual)

                if data_property_name is None and object_property_name is None:
                    if tag not in IGNORED_TAGS:
                        print(f'    Property "{tag}" of "{individual.name}" was not found')
                else:
                    if data_property_name:
                        prop_range = name2data_property[data_property_name].range
                        if prop_range is not None:
                            if len(prop_range) == 1:
                                val_type = prop_range[0]
                                try:
                                    if val_type in [float, int]:
                                        value_casted = cast_to_number(value)
                                        if value_casted is not None:
                                            value = value_casted
                                    else:
                                        print(val_type)
                                    val_modified_type = val_type(value)
                                    value = val_modified_type
                                    set_property(individual, data_property_name, value)
                                    markup.iloc[row_idx, col_idx] = f'{value}'
                                except Exception:
                                    print(f'    Unable to cast value "{value}" to type {val_type}')
                            else:
                                raise ValueError(f'    prop_range has length {len(prop_range)} - {data_property_name}')
                        else:
                            set_property(individual, data_property_name, value)
                            markup.iloc[row_idx, col_idx] = f'{value}'
                    elif object_property_name:
                        value = value.replace(',', '|').replace('/', '|')
                        values = value.split('|')
                        markup.iloc[row_idx, col_idx] = f'{value}'
                        for splitted_value in values:
                            splitted_value_clean = splitted_value.lower().strip()

                            founded_object = find_object_match(splitted_value_clean, object_property_name)
                            if founded_object:
                                second_object = name2object[founded_object]
                                second_object_ind = second_object['individual']
                                set_property(individual, object_property_name, second_object_ind)
                                # print(f'{individual.name} property: {object_property_name}, {second_object_ind.name}')

                                # TODO: Fix support for inverse properties
                                # inverse_property = name2object_property[object_property_name].inverse_property
                                inverse_property = inverse_property_map.get(object_property_name, None)

                                if inverse_property is not None:
                                    # set_property(second_object_ind, inverse_property.name, individual)
                                    # print(f'{second_object_ind.name} inverse property: {inverse_property}, {individual.name}')
                                    set_property(second_object_ind, inverse_property, individual)

                                    # print(f'{second_object_ind.name} inverse property: {inverse_property}, {individual.name}')
                            else:
                                print(f'    Object "{splitted_value}" of '
                                      f'property "{object_property_name}" for {individual.name} was not found')


def analyze_excel(table, excel_out):
    columns = list(table.columns)
    row_count, column_count = table.shape

    table.replace({np.nan: None}, inplace=True)

    markup = pd.DataFrame('', index=np.arange(row_count), columns=columns)

    analyze_cells(table, markup)
    analyze_rows(table, markup)
    table.columns = columns

    # if TARGET_TAG

    write_table_to_ontology(table, markup)

    markup.to_excel(excel_out, columns=columns)
    print(f'Saved marked up table into {excel_out}')


def load_ontology():
    global onto
    onto = get_ontology(ONTOLOGY_PATH)
    onto.load()

    print('onto.individuals()', len(list(onto.individuals())))
    for individual in onto.individuals():
        object_name = individual.name.lower()
        if object_name[0] == '~':
            continue
        is_instance_of_str = []
        for x in individual.is_instance_of:
            try:
                is_instance_of_str.append(str(x.name))
            except Exception:
                pass

        # print(individual.is_instance_of, individual.name)
        name2object[object_name] = {
            "individual": individual,
            "is_instance_of": "|".join(is_instance_of_str)
        }
        object_names.append(object_name)

    print('onto.object_properties()', len(list(onto.object_properties())))
    for prop in onto.object_properties():
        object_properties.append(prop.name)
        name2object_property[prop.name] = prop

    print('onto.properties()', len(list(onto.properties())))
    for prop in onto.data_properties():
        data_properties.append(prop.name)
        name2data_property[prop.name] = prop

    print('onto.classes()', len(list(onto.classes())))
    for cls in onto.classes():
        class_names.append(cls.name)


def load_terms():
    global terms
    with open(TERMS_PATH) as csvfile_reader:
        csv_reader = csv.reader(csvfile_reader, delimiter=';')
        for line_idx, csv_line in enumerate(csv_reader):
            rus_term, eng_term, rus_short, eng_short = csv_line
            terms[eng_term.lower()] = {
                "rus_term": rus_term,
                "eng_term": eng_term,
                "rus_short": rus_short,
                "eng_short": eng_short
            }


def main():
    global name2object
    global row2object
    global col2predicate
    global TARGET_TAG
    global ONTOLOGY_PATH
    ONTOLOGY_PATH = "ontologies/Kaz_Water_Ontology_modified_fixed.owl"
    # ONTOLOGY_PATH = "ontologies/Kaz_Water_Ontology_modified_fixed_new.owl"

    # load_terms()
    load_ontology()

    # onto.save(ONTOLOGY_PATH.replace('.owl', '_fixed.owl'))


    # return
    # EXCEL_FILES = ['Water_Basins_KZ.xlsx', 'Population_KZ.xlsx']
    # EXCEL_FILES = ['Water_Basins_KZ.xlsx']
    EXCEL_FILES = ['Water_Basins_KZ.xlsx', 'Population_KZ.xlsx', 'GBD_Water_Consumption', 'GBD_Water_Level_IBB']
    # EXCEL_FILES = ['GBD_Water_Level_IBB']

    BASE_PATH = '/Users/titrom/Desktop/Диплом/Tables/PyTableMiner'
    TABLES_PATH = os.path.join(BASE_PATH, 'tables')
    PROCESSED_PATH = os.path.join(BASE_PATH, 'processed')
    # INPUT_EXCEL_FILE = os.path.join(TABLES_PATH, EXCEL_FILE)
    # OUTPUT_EXCEL_FILE = os.path.join(PROCESSED_PATH, EXCEL_FILE)

    if not os.path.exists(PROCESSED_PATH):
        os.mkdir(PROCESSED_PATH)

    for path in EXCEL_FILES:
        target_path = os.path.join(TABLES_PATH, path)
        if os.path.isdir(target_path):
            files = [os.path.join(path, file) for file in os.listdir(target_path)]
            output_path = os.path.join(PROCESSED_PATH, path)
            if not os.path.exists(output_path):
                os.makedirs(output_path)
        else:
            files = [path]

        print(files)

        for file_idx, excel_file_path in enumerate(files):
            input_excel_file = os.path.join(TABLES_PATH, excel_file_path)
            output_excel_file = os.path.join(PROCESSED_PATH, excel_file_path)

            print(f'File {file_idx+1}/{len(files)}')
            with pd.ExcelFile(input_excel_file) as excel_file:
                for sheet_name in excel_file.sheet_names:
                    print(f'Sheet: {sheet_name} handling started')
                    TARGET_TAG = sheet_name.replace('_KZ', '')
                    if 'GBD_Water_Consumption' in input_excel_file:
                        TARGET_TAG = 'Water_Consumption'
                    elif 'GBD_Water_Level_IBB' in input_excel_file:
                        TARGET_TAG = 'Water_Level'
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


                    row2object = {}
                    col2predicate = {}


                    analyze_excel(table, output_excel_file.replace('.xlsx', '_') + sheet_name + '.xlsx')

                    print(f'Sheet {sheet_name} handling ended.\n')
            # break

    onto.save(ONTOLOGY_PATH.replace('.owl', '_new.owl'))


if __name__ == '__main__':
    main()

