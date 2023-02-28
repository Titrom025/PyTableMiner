import csv
import os
import time
import numpy as np
import pandas as pd
import requests

from nltk.stem import snowball
from owlready2 import *


SOLR_PATH = '/Users/titrom/Desktop/Tables/QTM-master/solr'
CORES = ['trait_descriptors', 'trait_values', 'trait_properties', 'sgn_tomato_genes', 'sgn_tomato_markers']

TERMS_PATH = "terms.csv"
ONTOLOGY_PATH = "Population_KZ.owl"
ontology = {}
terms = {}
onto = None
value2individual = {}
row2individual = {}
col2year = {}

TARGET_PROPERTY = 'Population'

def control_solr(command):
    solr_command = f'{SOLR_PATH}/run.sh {command} 8983 {SOLR_PATH}/core/data'
    print(solr_command)
    os.system(solr_command)
    time.sleep(3)


def make_api_call(post_data, core):
    url = f'http://localhost:8983/solr/{core}/tag?fl=uuid,code,prefterm,' \
          f'term&overlaps=LONGEST_DOMINANT_RIGHT&matchText=true&tagsLimit=5000&wt=json'

    x = requests.post(url, data=post_data)
    if x.status_code == 200:
        return x.json()
    return {''}


def get_term(text):
    terms_str = ''
    for core in CORES:
        json = make_api_call(text, core)
        terms = []

        if 'response' not in json:
            continue
        if 'docs' not in json['response']:
            continue

        for doc in json['response']['docs']:
            terms.append(doc['term'])

        for term in terms:
            if term in terms_str:
                continue
            if len(terms_str):
                terms_str += ' | '
            terms_str += f'{term}'

    return terms_str


def solr_main():
    control_solr('start')

    with open('table.csv') as csvfile_reader:
        with open('table_processed.csv', 'w') as csvfile_writer:
            csv_reader = csv.reader(csvfile_reader, delimiter=';')
            csv_writer = csv.writer(csvfile_writer, delimiter=';')
            for line_idx, csv_line in enumerate(csv_reader):
                print(f'Line {line_idx}')
                csv_output_row = []
                for cell in csv_line:
                    term = ''
                    if cell != '':
                        term = get_term(cell)
                    csv_output_row.append(term)
                csv_writer.writerow(csv_output_row)


def analyze_cells(table, markup):
    for col_idx, column in enumerate(table):
        for row_idx, cell_val in enumerate(table[column]):
            if cell_val is None:
                continue
            try:
                float(cell_val)
                cell_type = 'Number'
            except ValueError:
                cell_val = cell_val.lower()
                if cell_val in value2individual:
                    cell_type = value2individual[cell_val]['is_instance_of']
                    print(f'Value "{cell_val}" classifies as "{cell_type}"')
                    row2individual[row_idx] = value2individual[cell_val]['individual']
                    print(row2individual[row_idx])
                else:
                    stemmed_val = stemmer.stem(cell_val)
                    stemmed_words.add(stemmed_val)
                    cell_type = 'Text'

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
            pass

    if numbers_in_row < column_count * 0.9:
        return False

    for col_idx, cell in enumerate(row):
        if markup.iloc[row_idx, col_idx] == 'Number':
            markup.iloc[row_idx, col_idx] = 'Year'
            col2year[col_idx] = round(cell)
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

def write_table_to_ontology(table, markup):
    for col_idx, column in enumerate(table):
        if col_idx not in col2year:
            continue
        for row_idx, cell_val in enumerate(table[column]):
            if row_idx not in row2individual:
                continue
            if markup.iloc[row_idx, col_idx] == 'Number':
                value = round(table.iloc[row_idx, col_idx])
                individual = row2individual[row_idx]
                year = col2year[col_idx]
                print(f'Property {TARGET_PROPERTY}, '
                      f'Val {value}, '
                      f'Year: {year}, '
                      f'Individual: {individual}')
                if individual.__getattr__(TARGET_PROPERTY) is None:
                    try:
                        individual.__setattr__(TARGET_PROPERTY, [f'{year}_{value}'])
                    except Exception:
                        individual.__setattr__(TARGET_PROPERTY, f'{year}_{value}')
                else:
                    try:
                        individual.__getattr__(TARGET_PROPERTY).append(f'{year}_{value}')
                    except:
                        str_prop = individual.__getattr__(TARGET_PROPERTY)
                        individual.__setattr__(TARGET_PROPERTY, str_prop + f'{year}_{value}|')


def analyze_excel(table, excel_out):
    global stemmed_words
    global stemmer

    # init_ontology()

    # table = pd.read_excel(path)
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
    # print(stemmed_words)
    print(f'Saved marked up table into {excel_out}')


def load_ontology():
    global onto
    # onto_path.append("/Users/titrom/Desktop/Диплом/Tables/")
    onto = get_ontology(ONTOLOGY_PATH)
    onto.load()

    # with onto:
    #     class Population_KZ(DataProperty):
    #         range = [int]

    for individual in onto.individuals():
        print(individual.is_instance_of, individual.name)
        is_instance_of_str = [str(x.name) for x in individual.is_instance_of]
        value2individual[individual.name.lower()] = {
            "individual": individual,
            "is_instance_of": "|".join(is_instance_of_str)
        }

        # individual.__setattr__(TARGET_PROPERTY, [123])

        # for prop in individual.get_properties():
        #     if prop.namespace.name == 'population_kz':
        #         prop_value = individual.__getattr__(prop.name)[0]
        #         print(f'   - {prop} {prop_value}')
        #         if prop.name == 'Label_ru':
        #             is_instance_of_str = [str(x.name) for x in individual.is_instance_of]
        #             value2individual[prop_value.lower()] = {
        #                 "individual": individual,
        #                 "is_instance_of": "|".join(is_instance_of_str)
        #             }
        #             print(f'Term "{prop_value.lower()}" in term list exists: '
        #                   f'{prop_value.lower() in terms}')



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


if __name__ == '__main__':
    load_terms()
    load_ontology()
    EXCEL_FILE = 'Population_KZ.xlsx'
    BASE_PATH = '/Users/titrom/Desktop/Диплом/Tables/'
    INPUT_EXCEL_FILE = BASE_PATH + EXCEL_FILE
    OUTPUT_EXCEL_FILE = BASE_PATH + 'processed/' + EXCEL_FILE

    if not os.path.exists(BASE_PATH + 'processed/'):
        os.mkdir(BASE_PATH + 'processed/')

    excel_file = pd.ExcelFile(INPUT_EXCEL_FILE)

    for sheet_name in excel_file.sheet_names:
        TARGET_PROPERTY = sheet_name.replace('_KZ', '').replace('population', 'Population')
        if 'female' in TARGET_PROPERTY or 'Female' in TARGET_PROPERTY:
            TARGET_PROPERTY = TARGET_PROPERTY.replace('female', 'Female')
        elif 'male' in TARGET_PROPERTY:
            TARGET_PROPERTY = TARGET_PROPERTY.replace('male', 'Male')
        table = excel_file.parse(sheet_name)

        if TARGET_PROPERTY in ['Urban_Male_Population',
                               'Rural_Male_Population',
                               'Urban_Female_Population',
                               'Rural_Female_Population']:
            continue

        # try:
        analyze_excel(table, OUTPUT_EXCEL_FILE.replace('.xlsx', '_') + sheet_name + '.xlsx')
        # except Exception as e:
            # print(e)

        # break
    #
    # print(value2individual)
    #
    onto.save(ONTOLOGY_PATH.replace('.owl', '_new.owl'))