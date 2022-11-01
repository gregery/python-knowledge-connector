import argparse
import knowledge_finder

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ArcGIS Knowledge Data Model Utility for Senzing')
    parser.add_argument('-c', '--config', type=str, default='knowledge_config.json')
    parser.add_argument('-e', '--entity', type=str, help='entity type name', required=True)
    args = parser.parse_args()

    #add knowledge protobuf python lib to the pythonpath
    knowledge_finder.findKnowledge(args.config)

    import knowledge_server

    conn = knowledge_server.KnowledgeConnection(args.config)
    kapi = knowledge_server.KnowledgeAPI(conn)

    retval = kapi.deleteEntityType(args.entity)
    print(retval)
