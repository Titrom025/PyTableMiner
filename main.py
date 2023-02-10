import csv
import os
import time
import numpy as np
import pandas as pd
import requests

from nltk.stem import snowball

SOLR_PATH = '/Users/titrom/Desktop/Tables/QTM-master/solr'
CORES = ['trait_descriptors', 'trait_values', 'trait_properties', 'sgn_tomato_genes', 'sgn_tomato_markers']

TERMS_PATH = "terms.csv"
ONTOLOGY_PATH = 'Ontologies'
ontology = {}
ENTITIES = {}

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
        for row_idx, val in enumerate(table[column]):
            if val is None:
                continue
            try:
                float(val)
                cell_type = 'Number'
            except ValueError:
                stemmed_val = stemmer.stem(val)
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
                previous_number = cell
            elif cell == previous_number + 1:
                previous_number = cell
            else:
                return False
        elif markup.iloc[row_idx, col_idx] == 'Text':
            pass

    if numbers_in_row > column_count * 0.9:
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


def init_ontology():
    for file in os.listdir(ONTOLOGY_PATH):
        with open(os.path.join(ONTOLOGY_PATH, file)) as ontology_file:
            for term in ontology_file:
                print(term)


def excel_main():
    global stemmed_words
    global stemmer

    init_ontology()

    table = pd.read_excel('1.xlsx')
    columns = list(table.columns)
    row_count, column_count = table.shape

    table.replace({np.nan: None}, inplace=True)

    stemmer = snowball.SnowballStemmer('russian')
    stemmed_words = set()

    markup = pd.DataFrame('-', index=np.arange(row_count), columns=columns)

    analyze_cells(table, markup)
    analyze_rows(table, markup)
    # columns[0] = 'region'
    table.columns = columns

    markup.to_excel('1_processed.xlsx', columns=columns)
    print(stemmed_words)


from owlready2 import *


def rdf_main():
    onto_path.append("/Users/titrom/Desktop/Диплом/Tables/Ontology/population_kz-ontologies-owl-REVISION-HEAD/")
    onto = get_ontology("Population_KZ.owl")
    onto.load()
    # print(dir(onto))
    # print(list(onto.classes()))
    # print(list(onto.individuals()))
    # print(dir(onto.Almaty_Region), onto.Almaty_Region.get_properties())
    for individual in onto.individuals():
        print(dir(individual))
        print(individual.is_instance_of, individual.name, individual.name.lower() in ENTITIES, individual)


def parse_entities():
    global ENTITIES
    with open(TERMS_PATH) as csvfile_reader:
        csv_reader = csv.reader(csvfile_reader, delimiter=';')
        for line_idx, csv_line in enumerate(csv_reader):
            rus_term, eng_term, rus_short, eng_short = csv_line
            # print(rus_term, eng_term, rus_short, eng_short)
            ENTITIES[eng_term.lower()] = {
                "rus_term": rus_term,
                "eng_term": eng_term,
                "rus_short": rus_short,
                "eng_short": eng_short
            }


if __name__ == '__main__':
    # excel_main()
    parse_entities()
    rdf_main()
