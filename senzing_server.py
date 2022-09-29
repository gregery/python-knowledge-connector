import json

import senzing_module_config
#make sure we can get the ini before we import the rest of the senzing python lib
senzing_module_config.getJsonConfig()

from senzing import G2Engine, G2Exception, G2EngineFlags, G2Diagnostic

class SenzingServer:
    def __init__(self, config_filename):
        #parse the config file

        with open(config_filename, mode='rt', encoding='utf-8') as config_file:
            config = json.load(config_file)
            self.config_params = {}
            if 'senzing_config' in config:
                self.config_params = config['senzing_config']

        #initialize the engine
        self.g2_engine = G2Engine()
        if 'VERBOSE_LOGGING' in self.config_params:
            verbose_logging=self.config_params['VERBOSE_LOGGING']
        else:
            verbose_logging = False
        return_code = self.g2_engine.init('senzing-knowledge-connector-engine',
                                          senzing_module_config.getJsonConfig(),
#                                          json.dumps(self.config_params),
                                          verbose_logging)
        self.g2_diagnostic = G2Diagnostic()
        self.g2_diagnostic.init('senzing-knowledge-connector-diagnostic',
                                json.dumps(self.config_params),
                                verbose_logging)

    def getEntityByEntityID(self, entity_id):
        response_bytearray = bytearray()
        try:
            return_code = self.g2_engine.getEntityByEntityID(entity_id, response_bytearray)
        except G2Exception as ex:
            #if this entity doesn't exist, return None
            if ex.args[1].startswith('0037E'):
                return None
            else:
                raise
        return json.loads(response_bytearray.decode())

    def getRecord(self, datasource_code, record_id):
        response_bytearray = bytearray()
        return_code = self.g2_engine.getRecord(datasource_code, record_id, response_bytearray)
        return json.loads(response_bytearray.decode())

    def getEntityRecordFeatures(self, entity_id):
        response_bytearray = bytearray()
        self.g2_diagnostic.getEntityDetails(entity_id, True, response_bytearray)
        return json.loads(response_bytearray.decode())
