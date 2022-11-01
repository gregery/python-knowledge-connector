import json

import senzing_module_config
#make sure we can get the ini before we import the rest of the senzing python lib
senzing_module_config.getJsonConfig()

from senzing import G2Engine, G2Exception, G2EngineFlags, G2Diagnostic

class SenzingServer:
    def __init__(self, config_filename):
        self.export_handle = None
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
        headers = 'DATA_SOURCE,RECORD_ID'
        self.export_handle = self.g2_engine.exportCSVEntityReport(headers, )

    def getNextRecord(self):
        response_bytearray = bytearray()
        self.g2_engine.fetchNext(self.export_handle, response_bytearray)
        if not response_bytearray:
            return None
        val = response_bytearray.decode().strip()
        val = val.split(',')
        try:
            val[0] = int(val[0])
        except ValueError:
            #skip the header
            if 'DATA_SOURCE' in val[0]:
                return self.getNextRecord()

        #remove quotes
        val[0] = val[0].strip('"')
        val[1] = val[1].strip('"')
        #create the json
        return val

    def closeExportRecords(self):
        self.g2_engine.closeExport(self.export_handle)
