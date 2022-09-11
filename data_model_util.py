import argparse
import knowledge_server

if __name__ == "__main__":
    action_choices = ['create', 'delete', 'recreate', 'print']
    action_choices_help = 'create - create Senzing data model types\n'\
                          'delete - delete existing Senzing data model types\n'\
                          'recreate - delete existing Senzing data model types and then re-create them\n'\
                          'print - print to screen existing data model types'

    parser = argparse.ArgumentParser(description='ArcGIS Knowledge Data Model Utility for Senzing')
    parser.add_argument('action', type=str, choices=action_choices, help=action_choices_help)
    args = parser.parse_args()

    conn = knowledge_server.KnowledgeConnection('knowledge_config.json')
    kapi = knowledge_server.KnowledgeAPI(conn)
    szfunc = knowledge_server.SenzingKnowledgeFunctions(kapi)

    if args.action == 'create':
        szfunc.BuildDataModel()
    elif args.action == 'delete':
        szfunc.DeleteDataModel()
    elif args.action == 'recreate':
        szfunc.RebuildDataModel()
    elif args.action == 'print':
        data_model = kapi.GetDataModel()
        print(data_model)
