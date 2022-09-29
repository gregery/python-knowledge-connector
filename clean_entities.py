import argparse
import knowledge_finder

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Sync entities between Senzing and ArcGIS Knowledge')  
    parser.add_argument('-c', '--config', type=str, default='knowledge_config.json')
    parser.add_argument('-t', '--type', type=str, required=True)
    args = parser.parse_args()

    #add knowledge protobuf python lib to the pythonpath
    knowledge_finder.findKnowledge(args.config)

    import knowledge_server
    conn = knowledge_server.KnowledgeConnection(args.config)
    kapi = knowledge_server.KnowledgeAPI(conn)
    szfunc = knowledge_server.SenzingKnowledgeFunctions(kapi)

    print(F'deleting all entities of type {args.type}')
    szfunc.deleteAllEntitiesByType(args.type, True)

  
