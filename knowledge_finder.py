import json
import sys
import os

def findKnowledge(config_filename):
    with open(config_filename, mode='rt', encoding='utf-8') as config_file:
        knowledge_dir = json.load(config_file)["knowledge_protobuf_python_dir"]
        #check this path has what we expect
        if not os.path.exists(os.path.join(knowledge_dir, 'esriPBuffer/graph/ApplyEditsRequest_pb2.py')):
            raise Exception(F'knowledge protobuf python library was not found here {knowledge_dir}')
    sys.path.insert(0, knowledge_dir)