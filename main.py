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

ONTOLOGY_PATH = "ontologies/Kaz_Water_Ontology_modified.owl"
onto = None
data_properties = []
object_properties = []
individual_names = []

name2data_property = {}
name2object_property = {}
name2individual = {}

row2individual = {}
col2tag = {}

terms = {}
tag2property = {
    'Square (sq km)': 'Square',
    'Square, km²': 'Square',
    'Water_resources_KZ (cubic meter)': 'Water_resources_KZ',
    'Regions_KZ': 'Located_in_Regions_KZ',
    'Regions': 'Located_in_Regions_KZ',
    'Basins_Population': 'Basins_Population',
    'Urban_Basins_Population': 'Urban_Basins_Population',
    'Rural_Basins_Population': 'Rural_Basins_Population',
    'Rivers_of_Basins': 'Rivers_of_Basins',
    'River_length_in_KZ (km)': 'River_length_in_KZ',
    'Rivers_length, km': 'River_length_in_KZ',
    'Lakes_KZ' : 'has_Lakes',
}


def set_property(subject, predicate, object):
    if subject.__getattr__(predicate) is None:
        subject.__setattr__(predicate, [object])
    else:
        subject.__getattr__(predicate).append(object)


def analyze_cells(table, markup):
    for col_idx, column in enumerate(table):
        for row_idx, cell_val in enumerate(table[column]):
            if cell_val is None or cell_val == "":
                continue
            cell_val_number = None
            if type(cell_val) is str:
                if 'млн' in cell_val:
                    cell_val_number_str = cell_val.replace('млн', '') \
                                          .replace(' ', '') \
                                          .replace(',', '.') \
                                          .strip()
                    try:
                        cell_val_number = float(cell_val_number_str) * 1000000
                    except Exception:
                        pass
                if 'square kilometres' in cell_val:
                    cell_val_number_str = cell_val[0:cell_val.index('square kilometres')]
                    cell_val_number_str = cell_val_number_str.replace(',', '.').strip()
                    try:
                        cell_val_number = float(cell_val_number_str)
                    except Exception:
                        pass
            try:
                if cell_val_number is not None:
                    float_val = cell_val_number
                else:
                    float_val = float(cell_val)
                table.iloc[row_idx, col_idx] = float_val
                cell_type = 'Number'
            except ValueError:
                cell_val_lower = cell_val.lower()
                if cell_val_lower in name2individual:
                    cell_type = name2individual[cell_val_lower]['individual'].name
                    print(cell_val_lower, cell_type)
                    # print(f'Value "{cell_val_lower}" classifies as "{cell_type}"')
                    if row_idx not in row2individual:
                        row2individual[row_idx] = name2individual[cell_val_lower]['individual']
                else:
                    cell_type_best = find_best_data_property(cell_val)
                    if cell_type_best is None:
                        cell_type_best = find_best_object_property(cell_val)
                    cell_type_candidate = tag2property.get(cell_val, cell_type_best)

                    cell_type = 'Text'
                    if cell_type_candidate is not None:
                        cell_type = cell_type_candidate
                        col2tag[col_idx] = cell_type
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
            col2tag[col_idx] = round(cell)
    return True


def analyze_rows(table, markup):
    row_count, column_count = table.shape
    for row_idx, row in table.iterrows():
        if detect_year(markup, row, row_idx, column_count):
            continue


def find_best_property(property_list, tag):
    best_match = None
    best_match_dist = 4
    for prop in property_list:
        dist = levenshtein_distance(prop, tag)
        # print(prop, tag, dist)
        if dist < best_match_dist:
            best_match_dist = dist
            best_match = prop

    if best_match_dist > len(tag) * 0.5:
        return None
    return best_match


def find_best_data_property(tag):
    return find_best_property(data_properties, tag)


def find_best_object_property(tag):
    return find_best_property(object_properties, tag)


def find_best_individual(tag):
    return find_best_property(individual_names, tag)


def write_table_to_ontology(table):
    for col_idx, column in enumerate(table):
        if col_idx not in col2tag:
            continue
        for row_idx, cell_val in enumerate(table[column]):
            if row_idx not in row2individual:
                continue
            try:
                value = round(table.iloc[row_idx, col_idx])
            except Exception:
                value = table.iloc[row_idx, col_idx]

            individual = row2individual[row_idx]

            if TARGET_TAG:
                year = col2tag[col_idx]
                print(f'Property {TARGET_TAG}, '
                      f'Val {value}, '
                      f'Year: {year}, '
                      f'Individual: {individual}')
                if individual.__getattr__(TARGET_TAG) is None:
                    try:
                        individual.__setattr__(TARGET_TAG, [f'{year}_{value}'])
                    except Exception:
                        individual.__setattr__(TARGET_TAG, f'{year}_{value}')
                else:
                    try:
                        individual.__getattr__(TARGET_TAG).append(f'{year}_{value}')
                    except Exception:
                        str_prop = individual.__getattr__(TARGET_TAG)
                        individual.__setattr__(TARGET_TAG, str_prop + f'{year}_{value}|')
            else:
                tag = col2tag[col_idx]
                # print(f'Property {tag}, '
                #       f'Val {value}, '
                #       f'Individual: {individual}')

                data_property_name = find_best_data_property(tag)
                object_property_name = find_best_object_property(tag)

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
                                    val_modified_type = val_type(value)
                                    value = val_modified_type
                                except Exception:
                                    print(f'    Unable to cast value "{value}" to type {val_type}')
                            else:
                                raise ValueError(f'    prop_range has length {len(prop_range)} - {prop_range}')
                        set_property(individual, data_property_name, value)
                    elif object_property_name:
                        value = value.replace(',', '|')
                        values = value.split('|')
                        for splitted_value in values:
                            splitted_value_clean = splitted_value.lower().strip()

                            founded_object = find_best_individual(splitted_value_clean)
                            if founded_object:
                                second_object = name2individual[founded_object]
                                second_object_ind = second_object['individual']
                                set_property(individual, object_property_name, second_object_ind)
                                print(f'{individual.name} property: {object_property_name}, {second_object_ind.name}')
                            else:
                                print(f'    Object "{splitted_value}" of '
                                      f'property "{object_property_name}" was not found')


def analyze_excel(table, excel_out):
    columns = list(table.columns)
    row_count, column_count = table.shape

    table.replace({np.nan: None}, inplace=True)

    markup = pd.DataFrame('', index=np.arange(row_count), columns=columns)

    analyze_cells(table, markup)
    analyze_rows(table, markup)
    table.columns = columns

    write_table_to_ontology(table)

    markup.to_excel(excel_out, columns=columns)
    print(f'Saved marked up table into {excel_out}')


def load_ontology():
    global onto
    onto = get_ontology(ONTOLOGY_PATH)
    onto.load()

    print('onto.individuals()', len(list(onto.individuals())))
    for individual in onto.individuals():
        is_instance_of_str = []
        for x in individual.is_instance_of:
            try:
                is_instance_of_str.append(str(x.name))
            except Exception:
                pass

        # print(individual.is_instance_of, individual.name)
        name2individual[individual.name.lower()] = {
            "individual": individual,
            "is_instance_of": "|".join(is_instance_of_str)
        }
        individual_names.append(individual.name.lower())

    print('onto.object_properties()', len(list(onto.object_properties())))
    for prop in onto.object_properties():
        object_properties.append(prop.name)
        name2object_property[prop.name] = prop

    print('onto.properties()', len(list(onto.properties())))
    for prop in onto.data_properties():
        data_properties.append(prop.name)
        name2data_property[prop.name] = prop


    # print(dir(onto))
    # for cls in onto.classes():
    #     print(cls, cls.__module__, dir(cls))


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
    global name2individual
    global row2individual
    global col2tag
    global TARGET_TAG

    GET_TAG_FROM_SHEET_NAME = False
    # load_terms()
    load_ontology()



    # return
    EXCEL_FILE = 'Water_Basins_KZ_updated.xlsx'
    BASE_PATH = '/Users/titrom/Desktop/Диплом/Tables/PyTableMiner'
    TABLES_PATH = os.path.join(BASE_PATH, 'tables')
    PROCESSED_PATH = os.path.join(BASE_PATH, 'processed')
    INPUT_EXCEL_FILE = os.path.join(TABLES_PATH, EXCEL_FILE)
    OUTPUT_EXCEL_FILE = os.path.join(PROCESSED_PATH, EXCEL_FILE)

    if not os.path.exists(PROCESSED_PATH):
        os.mkdir(PROCESSED_PATH)

    excel_file = pd.ExcelFile(INPUT_EXCEL_FILE)

    for sheet_name in excel_file.sheet_names:
        print(f'Sheet: {sheet_name} handling started')
        # break
        TARGET_TAG = sheet_name.replace('_KZ', '').replace('population', 'Population')
        if 'female' in TARGET_TAG or 'Female' in TARGET_TAG:
            TARGET_TAG = TARGET_TAG.replace('female', 'Female')
        elif 'male' in TARGET_TAG:
            TARGET_TAG = TARGET_TAG.replace('male', 'Male')
        table = excel_file.parse(sheet_name)

        if not GET_TAG_FROM_SHEET_NAME:
            TARGET_TAG = None

        row2individual = {}
        col2tag = {}

        analyze_excel(table, OUTPUT_EXCEL_FILE.replace('.xlsx', '_') + sheet_name + '.xlsx')

        print(f'Sheet {sheet_name} handling ended.\n')

    onto.save(ONTOLOGY_PATH.replace('.owl', '_new.owl'))


if __name__ == '__main__':
    main()

