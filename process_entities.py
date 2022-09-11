import argparse
import senzing_knowedge_event_loop

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Sync entities between Senzing and ArcGIS Knowledge')  
    parser.add_argument('-c', '--config', type=str, default='knowledge_config.json')
    args = parser.parse_args()

    senzing_knowedge_event_loop.do_event_loop(args.config)
