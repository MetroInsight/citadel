from enum import Enum

PointType = Enum("PointType", [\
        'temperature',
        'waterflow',
        'airflow',
        'command',
        'electrical energy',
        'thermal energy',
        'electrical power',
        'thermal power',
        'time',
        'angle',
        'relative_humidity',
        'speed',
        'radiation',
        ]
        )
