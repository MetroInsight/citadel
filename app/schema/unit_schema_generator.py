import os
from os import listdir
from os.path import isfile, join
import pdb

import rdflib
from rdflib import Graph

g = Graph()

metaschema_dir = 'metaschema/'
for f in listdir(metaschema_dir):
    filename = join(metaschema_dir, f)
    if isfile(filename) and 'QUDT' in f:
        g.parse(filename, format='turtle')


q = """
select distinct ?unit where{
    ?unit a <http://qudt.org/schema/qudt/Unit>
    }
"""

res = g.query(q)

unit_list = list()

for row in res:
    uri = row[0].decode()
    unit_str = uri.split('/')[-1]
    unit_list.append(unit_str)

with open('unit.py', 'w') as fp:
    fp.writelines([
        'from enum import Enum\n',
        'Unit = Enum("Unit", {0})\n'.format(str(unit_list))
        ])
