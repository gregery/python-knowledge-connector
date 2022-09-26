#import pprint
import json

class EntityMapper:
    def __init__(self, mapping_config_file):
        with open(mapping_config_file, mode='rt', encoding='utf-8') as config_file:
            config_params = json.load(config_file)
            self.validateConfigItem(config_params)
            self.mapping = config_params
            self.feature_mapping = config_params['FEATURE_MAPPING']
            self.role_mapping = config_params['ROLE_MAPPING']


    def mapEntityAndRecords(self, entity_doc):
        #pprint.pprint(entity_doc)
        entity_attributes = {}
        #find the entity_id
        entity_attributes['entity_id'] = str(entity_doc['RESOLVED_ENTITY']['ENTITY_ID'])

        #find the mappable features for the resolved entity
        for ftype, fval in entity_doc['RESOLVED_ENTITY']['FEATURES'].items():
            if ftype in self.feature_mapping:
                #map the feature
                attribute_type = self.feature_mapping[ftype]
                attribute_value = fval[0]['FEAT_DESC']
                entity_attributes[attribute_type] = attribute_value

        #find roles
        for record in entity_doc['RESOLVED_ENTITY']['RECORDS']:
            if record['DATA_SOURCE'] in self.role_mapping:
                entity_attributes[self.role_mapping[record['DATA_SOURCE']]] = 'YES'

        #(record entity_id, resolved_entity_id, match_key)
        record_rels = []
        #extract the records for this entity
        for record in entity_doc['RESOLVED_ENTITY']['RECORDS']:
            #(record entity_id, resolved_entity_id, match_key)
            record_rels.append((record['DATA_SOURCE'],
                                record['RECORD_ID'],
                                entity_attributes['entity_id'],
                                record['MATCH_KEY']))

        #(ENTITY_ID, ENTITY_ID, match_description, matck_key)
        entity_rels = []
        #extract relationships
        for rel in entity_doc['RELATED_ENTITIES']:
            entity_rels.append((entity_attributes['entity_id'],
                                rel['ENTITY_ID'],
                                rel['MATCH_LEVEL_CODE'],
                                rel['MATCH_KEY']))
        return {"ENTITY_ATTRIBUTES" : entity_attributes, "RECORD_RELS" : record_rels, "ENTITY_RELS" : entity_rels}

    def validateConfigItem(self, config_item):
        if not isinstance(config_item, dict):
            raise Exception(F'Invalid mapping config item.  Item is not an object {config_item}')
        if 'KNOWLEDGE_ENTITY_TYPE' not in config_item:
            raise Exception(F'Invalid mapping config item.  Missing required field KNOWLEDGE_ENTITY_TYPE {config_item}')
        if 'FEATURE_MAPPING' not in config_item:
            raise Exception(F'Invalid mapping config item.  Missing required field FEATURE_MAPPING {config_item}')
        feature_mapping = config_item['FEATURE_MAPPING']
        if not isinstance(feature_mapping, dict):
            raise Exception(F'Invalid mapping config item.  FEATURE_MAPPING is not an object {feature_mapping}')
        role_mapping = config_item['ROLE_MAPPING']
        if not isinstance(role_mapping, dict):
            raise Exception(F'Invalid mapping config item.  ROLE_MAPPING is not an object {role_mapping}')
