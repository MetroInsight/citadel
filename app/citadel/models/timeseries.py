from .. import timeseriesdb


def read_ts_data(uuid, start_time=None, end_time=None):
    query_string = 'select * from "{0}" '.format(uuid)
    if not start_time:
        query_string += "order by time desc limit 1"
    elif not end_time:
        end_time_str = arrow.get(end_time).format(influxdb_time_format)
        query_string += "where time >= {0}".format(str(start_time)+'s')
    else:
#            start_time_str = arrow.get(start_time).format(influxdb_time_format)
#            end_time_str = arrow.get(end_time).format(influxdb_time_format)
        query_string += "where time >= {0} and time < {1}"\
                        .format(str(start_time)+'s', str(end_time)+'s')
    data = timeseriesdb.query(query_string, epoch='s')
    points = dict([(point['time'],point['value']) for point in data.get_points()])
    return points


def write_ts_data(uuid, data):
    points = [{
        'measurement': uuid,
        'time': int(float(t)),
        'fields': {
            'value': v
            }
        } for t,v in data['samples'].items()]
    return timeseriesdb.write_points(points, time_precision='s')


def delete_ts_data(uuid, start_time, end_time):
    delete_query = 'delete from "{0}" where time >= {1} and time < {2}'\
                    .format(uuid, str(start_time)+'s', str(end_time)+'s')
    timeseriesdb.query(delete_query)

def delete_point(uuid):
    drop_measurement_query = 'drop measurement "{0}"'.format(uuid)
    timeseriesdb.query(drop_measurement_query)

