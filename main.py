import csv
import os
import time
import numpy as np
import pandas as pd
import requests

from nltk.stem import snowball
from owlready2 import *

from Levenshtein import distance as levenshtein_distance


ONTOLOGY_PATH = "ontologies/Kaz_Water_Ontology.owl"
# ONTOLOGY_PATH = "ontologies/Water_Ontology_KZ_modified.owl"
onto = None
onto_properties = []
property_name2property = {}

terms = {}

value2individual = {}
row2individual = {}
col2tag = {}

TARGET_TAG = None



tag2property = {
    'Square (sq km)': 'Square',
    'Square, km²': 'Square',
    'Water_resources_KZ (cubic meter)': 'Water_resources_KZ',
    'Regions_KZ': 'Regions_KZ_prop',
    'Regions': 'Regions_KZ_prop',
    'Basins_Population': 'Basins_Population',
    'Urban_Basins_Population': 'Urban_Basins_Population',
    'Rural_Basins_Population': 'Rural_Basins_Population',
    'Rivers_of_Basins': 'Rivers_of_Basins',
    'River_length_in_KZ (km)': 'River_length_in_KZ',
    'Rivers_length, km': 'River_length_in_KZ',
    'Lakes_KZ' : 'has_Lakes',
}


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
                if cell_val_lower in value2individual:
                    # cell_type = value2individual[cell_val_lower]['is_instance_of']
                    cell_type = value2individual[cell_val_lower]['individual'].name
                    # cell_individual = value2individual[cell_val_lower]['individual']
                    print(f'Value "{cell_val_lower}" classifies as "{cell_type}"')
                    row2individual[row_idx] = value2individual[cell_val_lower]['individual']
                    # markup.iloc[row_idx, col_idx] = cell_type
                else:
                    # stemmed_val = stemmer.stem(cell_val)
                    # stemmed_words.add(stemmed_val)

                    cell_type_best = find_best_matched_property(cell_val)
                    cell_type_candidate = tag2property.get(cell_val, cell_type_best)

                    cell_type = 'Text'
                    if cell_type_candidate is not None:
                        cell_type = cell_type_candidate
                        col2tag[col_idx] = cell_type
                        print(f'Cell value "{cell_val}" classified as {cell_type}')

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


# def init_ontology():
#     for file in os.listdir(ONTOLOGY_PATH):
#         with open(os.path.join(ONTOLOGY_PATH, file)) as ontology_file:
#             for term in ontology_file:
#                 print(term)

def find_best_matched_property(tag):
    best_match = None
    best_match_dist = 4
    for prop in onto_properties:
        dist = levenshtein_distance(prop, tag)
        # print(prop, tag, dist)
        if dist < best_match_dist:
            best_match_dist = dist
            best_match = prop

    if best_match_dist > len(tag) * 0.5:
        return None
    return best_match



def write_table_to_ontology(table, markup):
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
                print(f'Property {tag}, '
                      f'Val {value}, '
                      f'Individual: {individual}')

                prop_name = find_best_matched_property(tag)
                if prop_name is None:
                    print(f'    Property "{tag}" was not found')
                else:
                    prop_range = property_name2property[prop_name].range
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
                    if individual.__getattr__(prop_name) is None:
                        # try:
                        individual.__setattr__(prop_name, [f'{value}'])
                        # except Exception:
                        #     individual.__setattr__(tag, f'{value}')
                    else:
                        # try:
                        individual.__getattr__(prop_name).append(value)
                        # except Exception:
                        #     str_prop = individual.__getattr__(tag)
                        #     # print(type(str_prop), dir(str_prop))
                            # str_prop.insert(f'{value}|')
                            # individual.__setattr__(tag, str_prop)


def analyze_excel(table, excel_out):
    global stemmed_words
    global stemmer

    columns = list(table.columns)
    row_count, column_count = table.shape

    table.replace({np.nan: None}, inplace=True)

    stemmer = snowball.SnowballStemmer('russian')
    stemmed_words = set()

    markup = pd.DataFrame('', index=np.arange(row_count), columns=columns)

    analyze_cells(table, markup)
    analyze_rows(table, markup)
    table.columns = columns

    write_table_to_ontology(table, markup)

    markup.to_excel(excel_out, columns=columns)
    print(f'Saved marked up table into {excel_out}')


def load_ontology():
    global onto
    onto = get_ontology(ONTOLOGY_PATH)
    onto.load()

    for individual in onto.individuals():
        is_instance_of_str = []
        for x in individual.is_instance_of:
            try:
                is_instance_of_str.append(str(x.name))
            except Exception:
                pass

        print(individual.is_instance_of, individual.name)
        value2individual[individual.name.lower()] = {
            "individual": individual,
            "is_instance_of": "|".join(is_instance_of_str)
        }

    for prop in onto.properties():
        onto_properties.append(prop._name)
        property_name2property[prop._name] = prop


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
    global value2individual
    global row2individual
    global col2tag
    global TARGET_TAG

    GET_TAG_FROM_SHEET_NAME = False
    # load_terms()
    load_ontology()

    print(onto)
    # print(dir(onto))
    print('onto.object_properties()', len(list(onto.object_properties())))
    for prop in onto.object_properties():
        print(prop)
    print('onto.individuals()', len(list(onto.individuals())))
    # for ind in onto.individuals():
    #     print(ind)
    print('onto.object_properties()', len(list(onto.object_properties())))


    # return
    EXCEL_FILE = 'Water_Basins_KZ.xlsx'
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

        # if TARGET_TAG in ['Urban_Male_Population',
        #                        'Rural_Male_Population',
        #                        'Urban_Female_Population',
        #                        'Rural_Female_Population']:
        #     continue

        if not GET_TAG_FROM_SHEET_NAME:
            TARGET_TAG = None

        row2individual = {}
        col2tag = {}

        analyze_excel(table, OUTPUT_EXCEL_FILE.replace('.xlsx', '_') + sheet_name + '.xlsx')

        print(f'Sheet {sheet_name} handling ended.\n')
        # break
    #
    # print(value2individual)
    #
    onto.save(ONTOLOGY_PATH.replace('.owl', '_new.owl'))


if __name__ == '__main__':
    main()

