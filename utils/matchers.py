from Levenshtein import distance as levenshtein_distance


def find_best_property(property_list, tag, max_dist=0.3):
    tag = tag.replace('_GBD', '')
    best_match = None
    best_match_dist = 4
    for prop in property_list:
        dist = levenshtein_distance(prop, tag)
        if dist < best_match_dist:
            best_match_dist = dist
            best_match = prop

    if best_match_dist > len(tag) * max_dist:
        return None
    return best_match


def find_data_property_match(ontology_data, tag, individual=None, max_dist=0.3):
    specific_class_tag = None
    if individual is not None:
        class_name = individual.is_a[0].name
        specific_class_tag = find_best_property(ontology_data.data_properties, f'{class_name}_{tag}')
    return specific_class_tag if specific_class_tag else find_best_property(ontology_data.data_properties, f'{tag}', max_dist)


def find_object_property_match(ontology_data, tag, individual=None, max_dist=0.3):
    specific_class_tag = None
    if individual is not None:
        class_name = individual.is_a[0].name
        specific_class_tag = find_best_property(ontology_data.object_properties, f'{class_name}_{tag}')
    return specific_class_tag if specific_class_tag else find_best_property(ontology_data.object_properties, f'{tag}', max_dist)


def find_object_match(ontology_data, tag, property_name=None, max_dist=0.3):
    specific_class_tag = None
    if property_name is not None and property_name in ontology_data.property2class:
        property_class = ontology_data.property2class[property_name].lower()
        specific_class_tag = find_best_property(ontology_data.object_names, f'{tag}_{property_class}')
    return specific_class_tag if specific_class_tag else find_best_property(ontology_data.object_names, f'{tag}', max_dist)


def find_class_match(ontology_data, tag):
    return find_best_property(ontology_data.class_names, f'{tag}')