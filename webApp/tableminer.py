import csv
from owlready2 import *

go = get_ontology("ontologies/Kaz_Water_Ontology_1506_filled.owl").load()

r = """
PREFIX : <http://www.semanticweb.org/асель/ontologies/2023/1/water_resources#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT DISTINCT ?obj ?wpi
Where {
	    ?cls rdfs:subClassOf :Water_Objects .
	    ?obj rdf:type ?cls .
		?obj :WPI ?wpi .
		FILTER  ( ?wpi > 0 ) .
		FILTER  ( ?wpi < 5 )
	}
order by DESC(?wpi)
"""

def make_resuest(text_request, results_file):
    try:
        res = default_world.sparql(text_request)
    except Exception as e:
        print(f'Exception was occured while processing request: {text_request}\n{e}')
        raise e


    with open(results_file, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=';',
                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        print(dir(res))
        for r in res:
            r_clean = [str(x).replace("Kaz_Water_Ontology_1506_filled.", "") for x in r]
            writer.writerow(r_clean)
