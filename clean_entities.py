import knowledge_server

if __name__ == "__main__":
    conn = knowledge_server.KnowledgeConnection('knowledge_config.json')
    kapi = knowledge_server.KnowledgeAPI(conn)
    szfunc = knowledge_server.SenzingKnowledgeFunctions(kapi)

    print('deleting all resolved entities')
    szfunc.deleteAllEntitiesByType('senzing_resolved_entity', True)

    print('deleting all records')
    szfunc.deleteAllEntitiesByType('senzing_record', True)
  