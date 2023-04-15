from utils import find_data_property_match, find_object_property_match,\
    find_object_match, find_class_match, find_best_property, cast_to_number, OntologyData


def set_property(ontology_data, subject, predicate, object, year=None, date=None):
    try:
        if date is not None:
            timestamp = ontology_data.ontology.Timestamp(f'~{subject.name}_'
                                       f'{predicate}_'
                                       f'{date.year}_'
                                       f'{date.month}_'
                                       f'{date.day}',
                                       namespace=ontology_data.ontology,
                                       Timestamp_Year=int(date.year),
                                       Timestamp_Month=int(date.month),
                                       Timestamp_Day=int(date.day),
                                       Timestamp_Value=float(object))
            if subject.__getattr__(predicate) is None:
                subject.__setattr__(predicate, [timestamp])
            else:
                subject.__getattr__(predicate).append(timestamp)
        else:
            if year is None:
                if subject.__getattr__(predicate) is None:
                    subject.__setattr__(predicate, [object])
                else:
                    subject.__getattr__(predicate).append(object)
            else:
                timestamp = ontology_data.ontology.Timestamp(f'~year_{year}_'
                                           f'{subject.name}_'
                                           f'{predicate}_'
                                           f'{object}',
                                           namespace=ontology_data.ontology,
                                           Timestamp_Year=year,
                                           Timestamp_Value=object)
                if subject.__getattr__(predicate) is None:
                    subject.__setattr__(predicate, [timestamp])
                else:
                    subject.__getattr__(predicate).append(timestamp)
    except Exception as e:
        print('e', e)


def load_ontology(ontology_path):
    ontology_data = OntologyData(ontology_path)

    print('ontology_data.ontology.individuals()', len(list(ontology_data.ontology.individuals())))
    for individual in ontology_data.ontology.individuals():
        object_name = individual.name.lower()
        if object_name[0] == '~':
            continue
        is_instance_of_str = []
        for x in individual.is_instance_of:
            try:
                is_instance_of_str.append(str(x.name))
            except Exception:
                pass

        ontology_data.name2object[object_name] = {
            "individual": individual,
            "is_instance_of": "|".join(is_instance_of_str)
        }
        ontology_data.object_names.append(object_name)

    print('ontology_data.ontology.object_properties()', len(list(ontology_data.ontology.object_properties())))
    for prop in ontology_data.ontology.object_properties():
        ontology_data.object_properties.append(prop.name)
        ontology_data.name2object_property[prop.name] = prop

    print('ontology_data.ontology.properties()', len(list(ontology_data.ontology.properties())))
    for prop in ontology_data.ontology.data_properties():
        ontology_data.data_properties.append(prop.name)
        ontology_data.name2data_property[prop.name] = prop

    print('ontology_data.ontology.classes()', len(list(ontology_data.ontology.classes())))
    for cls in ontology_data.ontology.classes():
        ontology_data.class_names.append(cls.name)

    return ontology_data


def export_data_to_ontology(table_data, ontology_data):
    for col_idx, column in enumerate(table_data.table):
        if col_idx not in table_data.col2predicate:
            continue
        if 'Observation_Post' == table_data.col2predicate[col_idx]:
            continue
        for row_idx, cell_val in enumerate(table_data.table[column]):
            if row_idx not in table_data.row2object:
                continue
            if table_data.markup.iloc[row_idx, col_idx] == 'Date':
                continue

            value = table_data.table.iloc[row_idx, col_idx]

            if value is None:
                continue

            individual = table_data.row2object[row_idx]

            if table_data.target_tag:
                year = table_data.col2predicate[col_idx]
                try:
                    float(year)
                except Exception:
                    year = None
                try:
                    float(value)
                except Exception:
                    continue

                data_property_name = find_data_property_match(ontology_data, table_data.target_tag, individual=individual)
                object_property_name = find_object_property_match(ontology_data, table_data.target_tag, individual=individual)

                if data_property_name is None and object_property_name is None:
                    if table_data.target_tag not in IGNORED_TAGS:
                        print(f'    Property "{table_data.target_tag}" of "{individual.name}" was not found')
                else:
                    if data_property_name:
                        prop_range = ontology_data.name2data_property[data_property_name].range
                        if prop_range is not None:
                            if len(prop_range) == 1:
                                val_type = prop_range[0]
                                try:
                                    if val_type in [float, int]:
                                        value_casted = cast_to_number(value)
                                        if value_casted is not None:
                                            value = value_casted
                                    val_modified_type = val_type(value)
                                    value = val_modified_type
                                    set_property(ontology_data, individual, data_property_name, value, year=year)
                                    table_data.markup.iloc[row_idx, col_idx] = f'{year}_{value}'
                                except Exception:
                                    print(f'    Unable to cast value "{value}" to type {val_type} for data property {data_property_name}')
                            else:
                                raise ValueError(f'    prop_range has length {len(prop_range)} - {prop_range} for property {data_property_name}')
                        else:
                            # set_property(ontology_data, individual, data_property_name, value)
                            raise ValueError(f'    Property not set')
                    elif object_property_name:
                        prop_range = ontology_data.name2object_property[object_property_name].range
                        if prop_range is not None:
                            if len(prop_range) == 1:
                                val_type = prop_range[0]
                                try:
                                    if type(individual) is ontology_data.ontology.Observation_Post and object_property_name in ['Water_Consumption', 'Water_Level']:
                                        row_date = None
                                        if row_idx in table_data.row2date:
                                            row_date = table_data.row2date[row_idx]
                                        set_property(ontology_data, individual, object_property_name, value, date=row_date)
                                        table_data.markup.iloc[row_idx, col_idx] = f'Date_{value}'
                                    else:
                                        set_property(ontology_data, individual, object_property_name, value, year=year)
                                        table_data.markup.iloc[row_idx, col_idx] = f'{year}_{value}'
                                except Exception:
                                    print(f'    Unable to cast value "{value}" to type {val_type} for object property {object_property_name}')
                            else:
                                raise ValueError(f'    prop_range has length {len(prop_range)} - {prop_range}')

                        # raise NotImplementedError('object_property_name handling not implemented')
            else:
                tag = table_data.col2predicate[col_idx]

                if tag in ontology_data.tag2property:
                    tag = ontology_data.tag2property[tag]
                data_property_name = find_data_property_match(ontology_data, tag, individual=individual)
                object_property_name = find_object_property_match(ontology_data,tag, individual=individual)

                if data_property_name is None and object_property_name is None:
                    if tag not in ontology_data.tags2ignore:
                        print(f'    Property "{tag}" of "{individual.name}" was not found')
                else:
                    if data_property_name:
                        prop_range = ontology_data.name2data_property[data_property_name].range
                        if prop_range is not None:
                            if len(prop_range) == 1:
                                val_type = prop_range[0]
                                try:
                                    if val_type in [float, int]:
                                        value_casted = cast_to_number(value)
                                        if value_casted is not None:
                                            value = value_casted
                                    else:
                                        print(val_type)
                                    val_modified_type = val_type(value)
                                    value = val_modified_type
                                    set_property(ontology_data, individual, data_property_name, value)
                                    table_data.markup.iloc[row_idx, col_idx] = f'{value}'
                                except Exception:
                                    print(f'    Unable to cast value "{value}" to type {val_type}')
                            else:
                                raise ValueError(f'    prop_range has length {len(prop_range)} - {data_property_name}')
                        else:
                            set_property(ontology_data, individual, data_property_name, value)
                            table_data.markup.iloc[row_idx, col_idx] = f'{value}'
                    elif object_property_name:
                        value = value.replace(',', '|').replace('/', '|')
                        values = value.split('|')
                        table_data.markup.iloc[row_idx, col_idx] = f'{value}'
                        for splitted_value in values:
                            splitted_value_clean = splitted_value.lower().strip()

                            founded_object = find_object_match(ontology_data, splitted_value_clean,
                                                               property_name=object_property_name)
                            if founded_object:
                                second_object = ontology_data.name2object[founded_object]
                                second_object_ind = second_object['individual']
                                set_property(ontology_data, individual, object_property_name, second_object_ind)
                                # print(f'{individual.name} property: {object_property_name}, {second_object_ind.name}')

                                # TODO: Fix support for inverse properties
                                # inverse_property = ontology_data.name2object_property[object_property_name].inverse_property
                                inverse_property = ontology_data.inverse_property_map.get(object_property_name, None)

                                if inverse_property is not None:
                                    # set_property(ontology_data, second_object_ind, inverse_property.name, individual)
                                    # print(f'{second_object_ind.name} inverse property: {inverse_property}, {individual.name}')
                                    set_property(ontology_data, second_object_ind, inverse_property, individual)

                                    # print(f'{second_object_ind.name} inverse property: {inverse_property}, {individual.name}')
                            else:
                                print(f'    Object "{splitted_value}" of '
                                      f'property "{object_property_name}" for {individual.name} was not found')
