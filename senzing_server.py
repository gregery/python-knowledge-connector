import json
import pprint

import senzing_module_config
#make sure we can get the ini before we import the rest of the senzing python lib
senzing_module_config.getJsonConfig()

from senzing import G2Engine, G2Exception, G2EngineFlags, G2Diagnostic

class SenzingServer:
    def __init__(self, config_filename):
        self.export_handle = None
        self.record_export_items = []
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

    def getEntityByRecordID(self, data_source, record_id):
        response_bytearray = bytearray()
        try:
            return_code = self.g2_engine.getEntityByRecordID(data_source, record_id, response_bytearray)
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


    #these functions are for record export only
    def exportRecords(self):
        self.export_handle = self.g2_engine.exportJSONEntityReport(G2EngineFlags.G2_EXPORT_INCLUDE_ALL_ENTITIES | G2EngineFlags.G2_ENTITY_INCLUDE_RECORD_DATA)

    def getNextRecord(self):
        if self.record_export_items:
            return self.record_export_items.pop()

        self.record_export_items = []
        response_bytearray = bytearray()
        self.g2_engine.fetchNext(self.export_handle, response_bytearray)
        if not response_bytearray:
            return None
        response_dict = json.loads(response_bytearray)
        for record in response_dict['RESOLVED_ENTITY']['RECORDS']:
            self.record_export_items.append((record['DATA_SOURCE'],record['RECORD_ID']))

        return self.getNextRecord()



    def closeExportRecords(self):
        self.g2_engine.closeExport(self.export_handle)
