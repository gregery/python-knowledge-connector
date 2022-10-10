import uuid_utils

class WorkItemProcessor():
    def __init__(self, knowledge_api_functions, senzing_knowledge_function):
        self.kapi = knowledge_api_functions
        self.skFunc = senzing_knowledge_function

    def bulkProcessEntities(self, mapped_data_collection, entity_type, uuid_cache):
        #bulk add the entities
        edit_frame = self.skFunc.getEditFrame()
        for mapped_item in mapped_data_collection:
            self.addEntityToFrame(edit_frame, entity_type, mapped_item['ENTITY_ATTRIBUTES'])
        edit_retval = self.skFunc.applyEditFrame(edit_frame)
        uuid_list = uuid_utils.parse_uuid_array(edit_retval.entity_add_results[entity_type].globalid_array)
        #add newly added entities to the cache
        for index in range(len(mapped_data_collection)):
            uuid_cache[mapped_data_collection[index]['ENTITY_ATTRIBUTES']['entity_id']] = uuid_list[index]

        #bulk add all the relationships
        edit_frame = self.skFunc.getEditFrame()
        for mapped_item in mapped_data_collection:
            #add the entity relationships
            for entity_rel in mapped_item['ENTITY_RELS']:
                self.addEntityRelationshipToFrame(edit_frame, entity_type, *entity_rel, uuid_cache)
            #add the record links
            for record_rel in mapped_item['RECORD_RELS']:
                self.addRecordRelationshipToFrame(edit_frame, entity_type, *record_rel, uuid_cache)
        return self.skFunc.applyEditFrame(edit_frame)


    def processEntity(self, mapped_data, entity_type):
        uuid_cache = {}
        # sync the resolved entity
        # first delete the entity if it currently exists
        entity_attributes = mapped_data['ENTITY_ATTRIBUTES']
        self.skFunc.deleteEntityByEntityID(entity_attributes["entity_id"], entity_type)
        edit_frame = self.skFunc.getEditFrame()
        self.addEntityToFrame(edit_frame, entity_type, entity_attributes)
        edit_retval = self.skFunc.applyEditFrame(edit_frame)
        uuid_cache[entity_attributes['entity_id']] = edit_retval.entity_add_results[entity_type].globalid_array

        # batch up all the relationships
        edit_frame = self.skFunc.getEditFrame()
        for entity_rel in mapped_data['ENTITY_RELS']:
            self.addEntityRelationshipToFrame(edit_frame, entity_type, *entity_rel, uuid_cache)

        # add link from record to entity
        for record_rel in mapped_data['RECORD_RELS']:
            self.addRecordRelationshipToFrame(edit_frame, entity_type, *record_rel, uuid_cache)

        #execute update
        return self.skFunc.applyEditFrame(edit_frame)

    def addEntityToFrame(self, edit_frame, entity_type, entity_attributes):
        add_entity = edit_frame.adds.entities[entity_type].namedObjectAdds.add()
        for key, value in entity_attributes.items():
            add_entity.properties[key].primitive_value.string_value = value

    def addEntityRelationshipToFrame(self, edit_frame, entity_type, from_entity_id, to_entity_id, match_level_code, match_key, uuid_cache):
        if from_entity_id in uuid_cache:
            from_uuid = uuid_cache[from_entity_id]
        else:
            (header, body) = self.kapi.queryGraphForEntityByEntityID(from_entity_id, entity_type)
            # if lookup failed, entity might not be loaded yet -- will get picked up on the other side
            if body is None:
                return False
            from_uuid = body.rows[0].values[0].entity_value.properties["globalid"].primitive_value.uuid_value
            #add this lookup to the cache
            uuid_cache[from_entity_id] = from_uuid
            #print(F'{from_entity_id=} {from_uuid=}')

        if not isinstance(to_entity_id, str):
            to_entity_id = str(to_entity_id)

        if to_entity_id in uuid_cache:
            to_uuid = uuid_cache[to_entity_id]
        else:
            (header, body) = self.kapi.queryGraphForEntityByEntityID(to_entity_id, entity_type)
            # if lookup failed, entity might not be loaded yet -- will get picked up on the other side
            if body is None:
                return False
            to_uuid = body.rows[0].values[0].entity_value.properties["globalid"].primitive_value.uuid_value
            #add this lookup to the cache
            uuid_cache[to_entity_id] = to_uuid
        
        add_rel = edit_frame.adds.relationships["related_to"].namedObjectAdds.add()
        add_rel.properties["originGlobalID"].primitive_value.uuid_value = from_uuid
        add_rel.properties["destinationGlobalID"].primitive_value.uuid_value = to_uuid
        add_rel.properties["match_level_code" ].primitive_value.string_value = match_level_code
        add_rel.properties["match_key"].primitive_value.string_value = match_key

        # relationships are directional in knowledge, so add the opposite direction as well
        add_rel = edit_frame.adds.relationships["related_to"].namedObjectAdds.add()
        add_rel.properties["originGlobalID"].primitive_value.uuid_value = to_uuid
        add_rel.properties["destinationGlobalID"].primitive_value.uuid_value = from_uuid
        add_rel.properties["match_level_code" ].primitive_value.string_value = match_level_code
        add_rel.properties["match_key"].primitive_value.string_value = match_key

    def addRecordRelationshipToFrame(self, edit_frame, resolved_entity_type, record_data_source, record_record_id, entity_id, match_key, uuid_cache):
        (header, body) = self.kapi.queryGraphForRecord(record_data_source, record_record_id)
        if body is None:
            print(f'WARNING: record has not been loaded DATA_SOURCE:{record_data_source} RECORD_ID:{record_record_id}')
            return None
        from_uuid = body.rows[0].values[0] .entity_value.properties["globalid"] .primitive_value.uuid_value

        if entity_id in uuid_cache:
            to_uuid = uuid_cache[entity_id]
        else:
            (header, body) = self.kapi.queryGraphForEntityByEntityID(entity_id, resolved_entity_type)
            to_uuid = body.rows[0].values[0].entity_value.properties["globalid"] .primitive_value.uuid_value
            print('cache miss {entity_id=}')

        add_rel = edit_frame.adds.relationships["resolved_to"].namedObjectAdds.add()
        add_rel.properties["originGlobalID"].primitive_value.uuid_value = from_uuid
        add_rel.properties["destinationGlobalID"].primitive_value.uuid_value = to_uuid
        add_rel.properties["match_key"].primitive_value.string_value = match_key
