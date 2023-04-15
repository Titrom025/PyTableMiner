from owlready2 import get_ontology


class OntologyData:
    def __init__(self, ontology_path):
        self.ontology_path = ontology_path
        self.ontology = get_ontology(self.ontology_path)
        self.ontology.load()

        self.object_names = []
        self.data_properties = []
        self.object_properties = []
        self.class_names = []

        self.name2data_property = {}
        self.name2object_property = {}
        self.name2object = {}

        self.tag2property = {
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
            'Дата': 'Date',
            'Значение': 'Value'
        }

        self.inverse_property_map = {
            'Located_in': 'Contains',
            'Contains': 'Located_in',
            'River_in_Basin': 'Basin_has_River',
            'Basin_has_River': 'River_in_Basin',
        }

        self.property2class = {
            'Located_in': 'Region'
        }

        self.tags2ignore = ["Urban_Basins_Population", "Rural_Basins_Population"]

    def save2newfile(self):
        self.ontology.save(self.ontology_path.replace('.owl', '_new.owl'))
