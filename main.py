import csv
import os
import time
import requests


SOLR_PATH = '/Users/titrom/Desktop/Tables/QTM-master/solr'
CORES = ['trait_descriptors', 'trait_values', 'trait_properties', 'sgn_tomato_genes', 'sgn_tomato_markers']


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


if __name__ == '__main__':
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
