import csv
import argparse
import knowledge_finder
import entity_mapper
import work_item_processor


def processCSV(config, mapping_filename, input_file, batch_size):
    #initialize senzing
    import senzing_server
    senzing_handle = senzing_server.SenzingServer(config)
    #init mapper
    mapper = entity_mapper.EntityMapper(mapping_filename)
    #init rest services
    conn = knowledge_server.KnowledgeConnection(config)
    kapi = knowledge_server.KnowledgeAPI(conn)
    szfunc = knowledge_server.SenzingKnowledgeFunctions(kapi)
    #init work item processor
    wiproc = work_item_processor.WorkItemProcessor(kapi, szfunc)

    uuid_cache = {}

    with open(input_file) as csvfile:
        reader = csv.DictReader(csvfile)
        entity_collection = []
        bulk_collected_count = 0
        bulk_processed_total = 0
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
            if bulk_collected_count == batch_size:
                wiproc.bulkProcessEntities(entity_collection, mapper.getResolvedEntityType(), uuid_cache)
                entity_collection.clear()
                bulk_processed_total += bulk_collected_count
                bulk_collected_count = 0
                print(F'processed {bulk_processed_total}')
        #process the last batch
        if entity_collection:
            wiproc.bulkProcessEntities(entity_collection, mapper.getResolvedEntityType(), uuid_cache)
            print(F'processed {bulk_processed_total + bulk_collected_count}')
        print('done!')





if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Sync entities between Senzing and ArcGIS Knowledge')  
    parser.add_argument('-c', '--config', type=str, default='knowledge_config.json')
    parser.add_argument('-m', '--mapping', type=str, default='entity_feature_mapping.json')
    parser.add_argument('-i', '--input', type=str, default='input.csv')
    parser.add_argument('-s', '--size', type=int, default=10)
    args = parser.parse_args()

    #add knowledge protobuf python lib to the pythonpath
    knowledge_finder.findKnowledge(args.config)
    import knowledge_server

    processCSV(args.config, args.mapping, args.input, args.size)

