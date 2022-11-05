import argparse
import knowledge_finder



def create(args, kapi):
    index_collection = data_model.DataModelIndexCollection(args.model)
    for key,index in index_collection.index_collection.items():
        print(F'Adding index {index.index_name} for type_name {index.type_name}')
        retval = kapi.addIndex(index)
        print(retval)

def delete(args, kapi):
    index_collection = data_model.DataModelIndexCollection(args.model)
    for key,index in index_collection.index_collection.items():
        print(F'deleting {index.type_name=} {index.index_name=}...')
        retval = kapi.deleteIndex(index.type_name, index.index_name)
        print(retval)

if __name__ == "__main__":
    action_choices = ['create', 'delete', 'recreate', 'print']
    action_choices_help = 'create - create index types\n'\
                          'delete - delete existing index types\n'\
                          'recreate - delete existing index types and then re-create them\n'\
                          'print - print to screen existing indexes'

    parser = argparse.ArgumentParser(description='ArcGIS Knowledge Data Model Utility for Senzing', formatter_class = argparse.RawTextHelpFormatter)
    parser.add_argument('action', type=str, choices=action_choices, help=action_choices_help)
    parser.add_argument('-c', '--config', type=str, default='knowledge_config.json')
    parser.add_argument('-m', '--model', type=str, default='entity_data_model_indexes.csv')
    args = parser.parse_args()

    #add knowledge protobuf python lib to the pythonpath
    knowledge_finder.findKnowledge(args.config)

    import knowledge_server
    import data_model

    conn = knowledge_server.KnowledgeConnection(args.config)
    kapi = knowledge_server.KnowledgeAPI(conn)
    szfunc = knowledge_server.SenzingKnowledgeFunctions(kapi)

    if args.action == 'delete' or args.action == 'recreate':
        delete(args, kapi)

    if args.action == 'create' or args.action == 'recreate':
        create(args, kapi)

    if args.action == 'print':
        data_model = kapi.getDataModel()
        system_created_indexes = ['esri__oid_idx', 'esri__globalid_idx', 'esri__geometry_geo_idx']
        for entity_type in data_model.data_model.entity_types:
            for index_type in entity_type.entity.field_indexes:
                if index_type.name in system_created_indexes:
                    continue
                print(F'{entity_type.entity.name=}')
                print(F'{index_type}')
                print('')

