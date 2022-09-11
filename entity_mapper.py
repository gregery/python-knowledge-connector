class EntityMapper:
    def __init__(self):
        self.feature_mapping = {
            'NAME':'name',
            'ADDRESS':'address',
        }
        #map data source "roles" to knowledge attributes
        #example -- record from mother data source gets IsMother attribute
        self.role_mapping = {
        }

    def mapEntityAndRecords(self, entity_doc):
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

        records = []
        #(record entity_id, resolved_entity_id, match_key)
        record_rels = []
        #extract the records for this entity
        for record in entity_doc['RESOLVED_ENTITY']['RECORDS']:
            record_extract = {}
            record_extract['entity_id'] = str(record['INTERNAL_ID'])
            record_extract['data_source'] = record['DATA_SOURCE']
            record_extract['record_id'] = record['RECORD_ID']
            record_extract['record_description'] = record['ENTITY_DESC']
            #record_extract['match_key'] = record['MATCH_KEY']
            records.append(record_extract)
            #(record entity_id, resolved_entity_id, match_key)
            record_rels.append((record_extract['entity_id'],
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

        return (entity_attributes, records, record_rels, entity_rels)
