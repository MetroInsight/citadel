import enum

from . import schema

def schema_converter(metadata):
    """ 
    Input: metadata (dict)
    Output: Normalized metadata (dict)
    """
    norm_metadata = dict()
    try:
        for tag, data_type in schema.items():
            if isinstance(data_type, enum.EnumMeta):
                schema_value = data_type[metadata[tag]].name
            else:
                schema_value = data_type(metadata[tag])
            norm_metadata[tag] = schema_value
            del metadata[tag]
    except KeyError as e:
        raise e
    for tag, value in metadata.items():
        norm_metadata[tag] = value

    #schema_validation(norm_metadata) # Jason: Do we need this? Maybe not.
    return norm_metadata

# Define Exceptions
# TODO: names should be normalized across different implementations later.
