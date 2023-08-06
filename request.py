import csv
from owlready2 import *

# go = get_ontology("ontologies/Kaz_Water_Ontology_population_water_level_cons.owl").load()
go = get_ontology("ontologies/Kaz_Water_Ontology_1506_filled.owl").load()

# res = default_world.sparql("""
# PREFIX : <http://www.semanticweb.org/асель/ontologies/2023/1/water_resources#>
# PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
# PREFIX owl: <http://www.w3.org/2002/07/owl#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
# SELECT ?y ?m ?d ?v
# 	{
# 	    :Post_14276 :Water_Level ?timestamp .
# 	    ?timestamp :Timestamp_Value ?v .
# 		?timestamp :Timestamp_Year ?y .
# 		?timestamp :Timestamp_Month ?m .
# 		?timestamp :Timestamp_Day ?d
# 	}
# """)

# 1.csv
# res = default_world.sparql("""
# PREFIX : <http://www.semanticweb.org/асель/ontologies/2023/1/water_resources#>
# PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
# PREFIX owl: <http://www.w3.org/2002/07/owl#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
# SELECT ?cls ?obj
# Where {
# 	    ?cls rdfs:subClassOf :Water_Objects .
# 		?obj rdf:type ?cls .
# 	}
# order by asc(UCASE(str(?obj)))
# """)

# # 2.csv
# res = default_world.sparql("""
# PREFIX : <http://www.semanticweb.org/асель/ontologies/2023/1/water_resources#>
# PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
# PREFIX owl: <http://www.w3.org/2002/07/owl#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
# SELECT DISTINCT ?cls ?obj ?wpi_val
# Where {
# 	    ?cls rdfs:subClassOf :Water_Objects .
# 		?obj rdf:type ?cls .
# 		?obj :WPI ?wpi_val
# 	}
# order by asc(UCASE(str(?obj)))
# """)

# 3.csv
# res = default_world.sparql("""
# PREFIX : <http://www.semanticweb.org/асель/ontologies/2023/1/water_resources#>
# PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
# PREFIX owl: <http://www.w3.org/2002/07/owl#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
# SELECT DISTINCT ?cls ?obj ?region ?population_Year ?population_Value
# Where {
# 	    ?cls rdfs:subClassOf :Water_Objects .
# 		?obj rdf:type ?cls .
# 		?obj :Located_in ?region .
# 		?region :Population ?population .
# 		?population :Timestamp_Year ?population_Year .
# 		?population :Timestamp_Value ?population_Value
# 	}
# order by asc(UCASE(str(?obj)))
# """)


# # 4.csv
# res = default_world.sparql("""
# PREFIX : <http://www.semanticweb.org/асель/ontologies/2023/1/water_resources#>
# PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
# PREFIX owl: <http://www.w3.org/2002/07/owl#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
# SELECT DISTINCT ?cls ?obj ?power ?energy ?resources
# Where {
# 	    ?cls rdfs:subClassOf :Water_Objects .
# 	    ?obj rdf:type ?cls .
# 		OPTIONAL { ?obj :Water_and_energy_resources_Power ?power } .
# 		OPTIONAL { ?obj :Water_and_energy_resources_Energy ?energy } .
# 		OPTIONAL { ?obj :Water_resources_KZ ?resources } .
# 	}
# order by asc(UCASE(str(?obj)))
# """)
#
# with open('4.csv', 'w') as csvfile:
#     writer = csv.writer(csvfile, delimiter=';',
#             quotechar='|', quoting=csv.QUOTE_MINIMAL)
#     for r in res:
#         r_clean = [str(x).replace("Kaz_Water_Ontology_population_water_level_cons.", "") for x in r]
#         print(r_clean)
#         writer.writerow(r_clean)


# 1505_1.csv
res = default_world.sparql("""
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
""")

with open('1505_1.csv', 'w') as csvfile:
    writer = csv.writer(csvfile, delimiter=';',
            quotechar='|', quoting=csv.QUOTE_MINIMAL)
    print('Object', 'WPI')
    for r in res:
        r_clean = [str(x).replace("Kaz_Water_Ontology_1506_filled.", "") for x in r]
        print(r_clean)
        writer.writerow(r_clean)


# 1505_2.csv
# res = default_world.sparql("""
# PREFIX : <http://www.semanticweb.org/асель/ontologies/2023/1/water_resources#>
# PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
# PREFIX owl: <http://www.w3.org/2002/07/owl#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
# SELECT  ?obj (sample(?wpi) as ?wpi_) (sample(?location) as ?loc) (sample(?diseases) as ?dis)
# Where {
# 	    ?cls rdfs:subClassOf :Water_Objects .
# 	    ?obj rdf:type ?cls .
# 		?obj :WPI ?wpi .
# 		?obj :Located_in ?location .
# 		?location :Circ_Sys_Diseases ?diseases
# 	}
# GROUP BY ?obj
# order by DESC(?dis)
# """)
# res = default_world.sparql("""
# PREFIX : <http://www.semanticweb.org/асель/ontologies/2023/1/water_resources#>
# PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
# PREFIX owl: <http://www.w3.org/2002/07/owl#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
# PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
# SELECT  ?obj ?wpi ?location ?diseases
# Where {
# 	    ?cls rdfs:subClassOf :Water_Objects .
# 	    ?obj rdf:type ?cls .
# 		?obj :WPI ?wpi .
# 		?obj :Located_in ?location .
# 		?location :Circ_Sys_Diseases ?diseases
# 	}
# order by DESC(?diseases)
# """)
# with open('1505_1.csv', 'w') as csvfile:
#     writer = csv.writer(csvfile, delimiter=';',
#             quotechar='|', quoting=csv.QUOTE_MINIMAL)
#     print('Object', 'WPI', 'Region', 'Circ_Sys_Diseases')
#     for r in res:
#         r_clean = [str(x).replace("Kaz_Water_Ontology_1506_filled.", "") for x in r]
#         print(r_clean)
#         writer.writerow(r_clean)