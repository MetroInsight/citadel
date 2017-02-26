from .. import config

METADATA_DB_TYPE = config.METADATA_DB_TYPE

if METADATA_DB_TYPE=="mongodb":
    from metadata_interface_mongodb import *
else:
    print("Unknown metadata db type: {0}".format(metadata_db_type))
    assert(False)
