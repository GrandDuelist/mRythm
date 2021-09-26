import time
def time_to_slot(time_str):
    '''
    :param time_str: example, 2013-10-20T17:07:31.000Z
    :return:
    '''
    datetime_object = time.strptime(time_str, '%Y-%m-%dT%H:%M:%S.000Z')
    number_slot = datetime_object.tm_hour * 12 + datetime_object.tm_min/5
    return number_slot

def extract_user_slot_location_telecom(line):
    '''
    460036745603797,2013-10-20T17:07:31.000Z,114.1202083,22.6095139,,6,1100,2,0,1x
    :param line:
    :return:
    '''
    attrs = line.split(",")
    user_id = attrs[0]
    time_slot = time_to_slot(attrs[1])
    location = attrs[2]+","+attrs[3]
    value = (user_id, str(time_slot)+","+location)
    return value

def extract_user_slot_location_unicom(line):
    '''
    460036745603797,2013-10-20T17:07:31.000Z,114.1202083,22.6095139,,6,1100,2,0,1x
    :param line:
    :return:
    '''
    attrs = line.split(",")
    user_id = attrs[0]
    time_slot = time_to_slot(attrs[2])
    location = attrs[3]+","+attrs[4]
    value = (user_id,str(time_slot)+","+location)
    return value


def line_to_points(line):
    key_value = line.split(":")
    in_key = key_value[0]
    in_value = key_value[1]
    points = in_value.split('|')
    return points
