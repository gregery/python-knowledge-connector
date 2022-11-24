import csv
import argparse
import knowledge_finder
import entity_mapper
import work_item_processor
import concurrent.futures
import uuid_utils


def applyEntityToEditFrame(edit_frame, entity):
    add_entity = edit_frame.adds.entities['person_resolved_entity'].namedObjectAdds.add()
    for key,value in entity.items():
        add_entity.properties[key].primitive_value.string_value = value

def applyRelToEditFrame(edit_frame, rel, uuid_cache):
    if rel[0] not in uuid_cache or rel[1] not in uuid_cache:
        return

    add_rel = edit_frame.adds.relationships["related_to"].namedObjectAdds.add()
    add_rel.properties["originGlobalID"].primitive_value.uuid_value = uuid_cache[rel[0]]
    add_rel.properties["destinationGlobalID"].primitive_value.uuid_value = uuid_cache[rel[1]]
    add_rel.properties["match_level_code" ].primitive_value.string_value = rel[2]
    add_rel.properties["match_key"].primitive_value.string_value = rel[3]

    add_rel = edit_frame.adds.relationships["related_to"].namedObjectAdds.add()
    add_rel.properties["originGlobalID"].primitive_value.uuid_value = uuid_cache[rel[1]]
    add_rel.properties["destinationGlobalID"].primitive_value.uuid_value = uuid_cache[rel[0]]
    add_rel.properties["match_level_code" ].primitive_value.string_value = rel[2]
    add_rel.properties["match_key"].primitive_value.string_value = rel[3]



def processBatch(config, entity_collection):
    #init rest services
    conn = knowledge_server.KnowledgeConnection(config)
    kapi = knowledge_server.KnowledgeAPI(conn)
    szfunc = knowledge_server.SenzingKnowledgeFunctions(kapi)
    #init work item processor
    wiproc = work_item_processor.WorkItemProcessor(kapi, szfunc)

def processCSV(config, mapping_filename, input_file, batch_size, thread_count):
    uuid_cache = {}

    #initialize senzing
    import senzing_server
    senzing_handle = senzing_server.SenzingServer(config)
    #init mapper
    mapper = entity_mapper.EntityMapper(mapping_filename)
    entity_collection = []

    with open(input_file) as csvfile:
        reader = csv.DictReader(csvfile)
        bulk_collected_count = 0

        print(F'processing csv file {input_file}')
        for row in reader:
            if 'ENTITY_ID' in row:
                res_ent = senzing_handle.getEntityByEntityID(int(row['ENTITY_ID']))
            elif 'DATA_SOURCE' in row and 'RECORD_ID' in row:
                res_ent = senzing_handle.getEntityByRecordID(row['DATA_SOURCE'], row['RECORD_ID'])
            else:
                raise Exception(F'unrecognized csv data {row}\nMust have ENTITY_ID or (DATA_SOURCE,RECORD_ID)')
            #map entity and collect result
            entity_collection.append(mapper.mapEntityAndRecords(res_ent))
            bulk_collected_count += 1

        print(F'extracted {bulk_collected_count} entities')

    #init rest services
    conn = knowledge_server.KnowledgeConnection(config)
    kapi = knowledge_server.KnowledgeAPI(conn)
    szfunc = knowledge_server.SenzingKnowledgeFunctions(kapi)

    ###add resolved entities
    entities_added = 0
    edit_frame = szfunc.getEditFrame()
    entity_list = []
    for entity_item in entity_collection:
        applyEntityToEditFrame(edit_frame, entity_item['ENTITY_ATTRIBUTES'])
        entity_list.append(entity_item['ENTITY_ATTRIBUTES']['entity_id'])
        entities_added += 1
        if entities_added == 10:
            break
    body = szfunc.applyEditFrame(edit_frame)
    #preserve the global ids -- we need them for relationships later
    uuid_list = uuid_utils.parse_uuid_array(body.entity_add_results['person_resolved_entity'].globalid_array)
    for idx in range(len(entity_list))
        uuid_cache[entity_list[idx]] = uuid_list[idx]
    entity_list.clear()
    uuid_list.clear()

    print(F'added {entities_added} entities')


    ###link resolved entities to each other

    #first look up the res_ents we need but are uncached
    to_lookup = set([])
    for entity_item in entity_collection:
        for rel_item in entity_item["ENTITY_RELS"]:
            if rel_item[0] not in uuid_cache:
                to_lookup.add(rel_item[0])
            if rel_item[1] not in uuid_cache:
                to_lookup.add(rel_item[1])
    #now finish populating the cache                
    gid_associations = szfunc.queryGraphForGIDByEntityID(to_lookup, 'person_resolved_entity')
    for assoc in gid_associations:
        uuid_cache[assoc[0]] = assoc[1]

    #now add the relationships
    edit_frame = szfunc.getEditFrame()
    for entity_item in entity_collection:
        for rel_item in entity_item["ENTITY_RELS"]:
            applyRelToEditFrame(edit_frame, rel_item, uuid_cache)
    szfunc.applyEditFrame(edit_frame)
    


    #link resolved entities to their records




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Sync entities between Senzing and ArcGIS Knowledge')  
    parser.add_argument('-c', '--config', type=str, default='knowledge_config.json')
    parser.add_argument('-m', '--mapping', type=str, default='entity_feature_mapping.json')
    parser.add_argument('-i', '--input', type=str, default='input.csv')
    parser.add_argument('-s', '--size', type=int, default=100, help='Knowledge API call batch size')
    parser.add_argument('-t', '--threads', type=int, default=4)
    args = parser.parse_args()

    #add knowledge protobuf python lib to the pythonpath
    knowledge_finder.findKnowledge(args.config)
    import knowledge_server

    processCSV(args.config, args.mapping, args.input, args.size, args.threads)

