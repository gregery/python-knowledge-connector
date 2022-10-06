import json

from rabbit_mq_connection import RabbitMQConnection
import knowledge_server
import senzing_server
import entity_mapper
import work_item_processor

def callback_closure(config_filename, mapping_filename):
    def callback(ch, method, properties, body):
        #extract the entity ID
        affected_entities = []
        message = json.loads(body)
        for affected in message['AFFECTED_ENTITIES']:
            affected_entities.append(affected['ENTITY_ID'])

        #process the affected entities
        for entity_id in affected_entities:
            print(F'processing entity: {entity_id}')
            res_ent = senzing_handle.getEntityByEntityID(int(entity_id))
            if res_ent is not None:
                #we have a real entity, sync it
                wiproc.processEntity(mapper.mapEntityAndRecords(res_ent), mapper.getResolvedEntityType())
            else:
                #this entity no longer exists in senzing, so delete it from knowledge
                szfunc.deleteEntityByEntityID(entity_id)
            print('processed entity')
        ch.basic_ack(delivery_tag = method.delivery_tag)

    mapper = entity_mapper.EntityMapper(mapping_filename)
    #setup the Knowledge connection
    conn = knowledge_server.KnowledgeConnection(config_filename)
    kapi = knowledge_server.KnowledgeAPI(conn)
    szfunc = knowledge_server.SenzingKnowledgeFunctions(kapi)
    wiproc = work_item_processor.WorkItemProcessor(kapi, szfunc)

    #setup the G2 connection
    senzing_handle = senzing_server.SenzingServer(config_filename)
    return callback

def do_event_loop(config_filename, mapping_filename):
    callback = callback_closure(config_filename, mapping_filename)
    conn = RabbitMQConnection(config_filename)
    conn.connect()
    conn.run(callback)
