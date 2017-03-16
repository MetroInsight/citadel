import os
from os import listdir
from os.path import isfile, join
import pdb

import rdflib
from rdflib import Graph

# TODO: Following list needs to be modeled based on QUDT later.
custom_units = ['MBTU-PER_HOUR',
                'Gallons-PER-HOUR',
                'MBTU',
                'KiloW-HR',
                'KW-reactive',
                'PowerFactor',
                'PercentReletiveHumidity',
                'Pounds-force-per-square-inch',
                'Inches-of-water',
                'MIN',
                'GAL-PER-MIN',
                'No-units',
                'Parts-per-million',
                'Percent',
                'Revolutions-per-minute-BACNet',
                'MegaW-HR',
                'MI-PER-SEC',
                'W-PER-M2',
                'PPM'
                ]

g = Graph()

metaschema_dir = 'metaschema/'
for f in listdir(metaschema_dir):
    filename = join(metaschema_dir, f)
    if isfile(filename) and 'QUDT' in f:
        if filename[-4:]=='.ttl':
            g.parse(filename, format='turtle')


q = """
select distinct ?unit where{
    {?unit a <http://qudt.org/schema/qudt/Unit> . }
    UNION
    {?unit a <http://qudt.org/schema/qudt/DerivedUnit> . }
    }
"""

res = g.query(q)

unit_list = list()

for row in res:
    uri = str(row[0])
    if uri == 'http://qudt.org/vocab/unit/DEG_C':
        pdb.set_trace()

    unit_str = uri.split('/')[-1]
    unit_list.append(unit_str)

unit_list += custom_units

with open('unit.py', 'w') as fp:
    fp.writelines([
        'from enum import Enum\n',
        'Unit = Enum("Unit", [\n']
        + ["    '{0}',\n".format(unit) for unit in unit_list]
        + ['    ])\n']
        )
