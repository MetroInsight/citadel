from enum import Enum

PointType = Enum("PointType", [\
        'temperature',
        'waterflow',
        'airflow',
        'command',
        'electrical_energy',
        'thermal_energy',
        'electrical_power',
        'time',
        'angle',
        'relative_humidity',
        'speed',
        'radiation',
        ]
        )
