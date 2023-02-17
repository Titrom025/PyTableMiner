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
ONTOLOGY_PATH = 'Ontologies'
ontology = {}
terms = {}
onto = None
value2individual = {}

INPUT_EXCEL_FILE = '1.xlsx'
OUTPUT_EXCEL_FILE = '1_processed.xlsx'


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


def analyze_excel():
    global stemmed_words
    global stemmer

    # init_ontology()

    table = pd.read_excel(INPUT_EXCEL_FILE)
    columns = list(table.columns)
    row_count, column_count = table.shape

    table.replace({np.nan: None}, inplace=True)

    stemmer = snowball.SnowballStemmer('russian')
    stemmed_words = set()

    markup = pd.DataFrame('', index=np.arange(row_count), columns=columns)

    analyze_cells(table, markup)
    analyze_rows(table, markup)
    table.columns = columns

    markup.to_excel(OUTPUT_EXCEL_FILE, columns=columns)
    # print(stemmed_words)
    print(f'Saved marked up table into {INPUT_EXCEL_FILE}')


def load_ontology():
    global onto
    # onto_path.append("/Users/titrom/Desktop/Диплом/Tables/Ontology/population_kz-ontologies-owl-REVISION-HEAD/")
    onto = get_ontology("Population_KZ_1702.owl")
    onto.load()

    for individual in onto.individuals():
        print(individual.is_instance_of, individual.name)
        for prop in individual.get_properties():
            if prop.namespace.name == 'population_kz':
                prop_value = individual.__getattr__(prop.name)[0]
                print(f'   - {prop} {prop_value}')
                if prop.name == 'Label_ru':
                    is_instance_of_str = [str(x.name) for x in individual.is_instance_of]
                    value2individual[prop_value.lower()] = {
                        "individual": individual,
                        "is_instance_of": "|".join(is_instance_of_str)
                    }
                    print(f'Term "{prop_value.lower()}" in term list exists: '
                          f'{prop_value.lower() in terms}')


    # print(dir(onto))
    onto.save("Population_KZ_new.owl")


def load_terms():
    global terms
    with open(TERMS_PATH) as csvfile_reader:
        csv_reader = csv.reader(csvfile_reader, delimiter=';')
        for line_idx, csv_line in enumerate(csv_reader):
            rus_term, eng_term, rus_short, eng_short = csv_line
            terms[rus_term.lower()] = {
                "rus_term": rus_term,
                "eng_term": eng_term,
                "rus_short": rus_short,
                "eng_short": eng_short
            }


if __name__ == '__main__':
    load_terms()
    load_ontology()
    analyze_excel()
