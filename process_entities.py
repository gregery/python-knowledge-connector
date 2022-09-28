import argparse
import knowledge_finder

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Sync entities between Senzing and ArcGIS Knowledge')  
    parser.add_argument('-c', '--config', type=str, default='knowledge_config.json')
    parser.add_argument('-m', '--mapping', type=str, default='entity_feature_mapping.json')
    args = parser.parse_args()

    #add knowledge protobuf python lib to the pythonpath
    knowledge_finder.findKnowledge(args.config)

    import senzing_knowedge_event_loop
    senzing_knowedge_event_loop.do_event_loop(args.config, args.mapping)
