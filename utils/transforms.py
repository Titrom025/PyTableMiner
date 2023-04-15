def cast_to_number(value):
    if type(value) in [int, float]:
        value_number = value
    elif type(value) is str:
        try:
            value_number = float(value)
        except Exception:
            value_number = None
    else:
        raise TypeError(f'Unexpected type {type(value)} in cast_to_number method')

    value_str = str(value)
    if 'млн' in value_str or 'km' in value_str:
        value_str = value_str\
            .replace('млн', '') \
            .replace('km', '') \
            .replace(' ', '') \
            .replace(',', '.') \
            .strip()
        try:
            value_number = float(value_str) * 1000000
        except Exception:
            pass
    if 'square kilometre' in value_str:
        value_str = value_str[0:value_str.index('square kilometre')]
        value_str = value_str.replace(',', '.').strip()
        try:
            value_number = float(value_str)
        except Exception:
            pass

    return value_number
