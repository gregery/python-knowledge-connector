import argparse
import knowledge_finder

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Sync entities between Senzing and ArcGIS Knowledge')  
    parser.add_argument('-c', '--config', type=str, default='knowledge_config.json')
    parser.add_argument('-s', '--size', type=int, default=1000)
    args = parser.parse_args()

    #add knowledge protobuf python lib to the pythonpath
    knowledge_finder.findKnowledge(args.config)
    import knowledge_server
    #init rest services
    conn = knowledge_server.KnowledgeConnection(args.config)
    kapi = knowledge_server.KnowledgeAPI(conn)
    szfunc = knowledge_server.SenzingKnowledgeFunctions(kapi)

    import senzing_server
    senzing_handle = senzing_server.SenzingServer(args.config)

    senzing_handle.exportRecords()

    edit_frame = szfunc.getEditFrame()
    total_count = 0
    record_count = 0
    while True:
        item = senzing_handle.getNextRecord()
        if item is None:
            break
        #we have an item, let's process it
        add_entity = edit_frame.adds.entities['person'].namedObjectAdds.add()
        add_entity.properties['data_source'].primitive_value.string_value = item[0]
        add_entity.properties['record_id'].primitive_value.string_value = item[1]
        add_entity.properties['source_record_id'].primitive_value.string_value = item[0] + '|' + item[1]
        record_count += 1
        total_count += 1
        if record_count >= args.size:
            #print(edit_frame)
            print(F'adding {len(edit_frame.adds.entities["person"].namedObjectAdds)}')
            add_response = szfunc.applyEditFrame(edit_frame)
            print('response')
            print(add_response)
            print (F'loaded {record_count}...')
            edit_frame = szfunc.getEditFrame()
            record_count = 0
            print (F'total loaded {total_count}...')

    if record_count > 0:
        szfunc.applyEditFrame(edit_frame)
        edit_frame = None
        print (F'loaded {record_count}...')
        print (F'total loaded {total_count}...')


    senzing_handle.closeExportRecords()
