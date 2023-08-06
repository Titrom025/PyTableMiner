import os.path

from flask import Flask, render_template, request
import csv
import hashlib

from tableminer import make_resuest

DATA_FOLDER = 'request_results/'
app = Flask(__name__)

# Function to read data from CSV
def read_csv_data(filepath):
    if not os.path.exists(filepath):
        return []
    data = []
    with open(filepath, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=';',
                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            data.append(row)
    return data


def create_data_folder():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)


def get_hashed_filename(text):
    return hashlib.md5(text.encode()).hexdigest() + '.csv'

# Home page route
@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        new_text = request.form.get('input_text')
        if new_text:
            create_data_folder()
            hashed_filename = get_hashed_filename(new_text)
            filepath = os.path.join(DATA_FOLDER, hashed_filename)
            try:
                make_resuest(new_text, filepath)
                data = read_csv_data(filepath)
            except Exception as e:
                data = [[f'Exception was occured while processing request: {e})']]
        else:
            data = []
        return render_template('index.html', data=data)
    else:
        data = []
        return render_template('index.html', data=data)

if __name__ == '__main__':
    app.run(debug=True)
