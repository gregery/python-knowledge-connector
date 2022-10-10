

def parse_uuid_array(uuid_string):
    num_ids = int(len(uuid_string)/16)
    uuid_list = []
    for index in range(num_ids):
        uuid_list.append(uuid_string[index*16:(index*16)+16])
    return uuid_list

