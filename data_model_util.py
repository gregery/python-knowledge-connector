import argparse
import knowledge_server
import data_model

if __name__ == "__main__":
    action_choices = ['create', 'delete', 'recreate', 'print']
    action_choices_help = 'create - create Senzing data model types\n'\
                          'delete - delete existing Senzing data model types\n'\
                          'recreate - delete existing Senzing data model types and then re-create them\n'\
                          'print - print to screen existing data model types'

    parser = argparse.ArgumentParser(description='ArcGIS Knowledge Data Model Utility for Senzing')
    parser.add_argument('action', type=str, choices=action_choices, help=action_choices_help)
    parser.add_argument('-c', '--config', type=str, default='knowledge_config.json')
    parser.add_argument('-m', '--model', type=str, default='entity_data_model.csv')
    parser.add_argument('-a', '--mapping', type=str, default='entity_feature_mapping.json')
    args = parser.parse_args()

    conn = knowledge_server.KnowledgeConnection(args.config)
    kapi = knowledge_server.KnowledgeAPI(conn)
    szfunc = knowledge_server.SenzingKnowledgeFunctions(kapi)

    if args.action == 'create':
        data_model.create_data_model(args.model, args.config, args.mapping)
    elif args.action == 'delete':
        data_model.delete_data_model(args.model, args.config)
    elif args.action == 'recreate':
        data_model.delete_data_model(args.model, args.config)
        data_model.create_data_model(args.model, args.config, args.mapping)
    elif args.action == 'print':
        data_model = kapi.GetDataModel()
        print(data_model)
