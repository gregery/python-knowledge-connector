import csv
import esriPBuffer.graph.DataModelTypes_pb2
import esriPBuffer.graph.AddNamedTypesRequest_pb2
import esriPBuffer.graph.AddNamedTypesResponse_pb2
import esriPBuffer.graph.AddIndexesRequest_pb2
import knowledge_server
import entity_mapper

import pprint

class DataModelAttribute():
    required_fields = ['PROPERTY_NAME',
                       'PROPERTY_ALIAS',
                       'FIELD_TYPE',
                       'NOT_NULLABLE',
                       'NOT_EDITABLE',
                       'NOT_VISIBLE',
                       'REQUIRED',
                       'IS_SYSTEM_MAINTAINED',
                       'SEARCHABLE'
                      ]
    def __init__(self, row_dict):
        for field in self.required_fields:
            if field not in row_dict:
                raise Exception(F'row missing required field {field}')
        self.name = row_dict['PROPERTY_NAME']
        self.alias = row_dict['PROPERTY_ALIAS']
        self.field_type = self.validateType(row_dict['FIELD_TYPE'])
        self.not_nullable = self.validateBoolean(row_dict['NOT_NULLABLE'])
        self.not_editable = self.validateBoolean(row_dict['NOT_EDITABLE'])
        self.not_visible = self.validateBoolean(row_dict['NOT_VISIBLE'])
        self.required = self.validateBoolean(row_dict['REQUIRED'])
        self.is_system_maintained = self.validateBoolean(row_dict['IS_SYSTEM_MAINTAINED'])
        self.searchable = self.validateBoolean(row_dict['SEARCHABLE'])

    def __repr__(self):
        return f'{self.name=} {self.alias=} {self.field_type=} {self.not_nullable=} {self.not_editable=} {self.not_visible=} {self.required=} {self.is_system_maintained=} {self.searchable=}'

    def validateType(self, field_type):
        if field_type.upper() not in ['STRING']:
            raise Exception(F'unknown field type {field_type}')
        return field_type.upper()

    def validateBoolean(self, item):
        if item.upper()[0] not in ['T','F']:
            raise Exception(F'unknown boolean type {item}')
        if item.upper()[0] == 'T':
            return True
        return False

    def mapDataType(self, data_type):
        if data_type == 'STRING':
            return esriPBuffer.EsriExtendedTypes.EsriExtendedTypes_pb2.esriFieldTypeString

    def transposeItemToProtobuf(self, attr):
        attr.name = self.name
        attr.alias = self.alias
        attr.fieldType = self.mapDataType(self.field_type)
        attr.not_nullable = self.not_nullable
        attr.not_editable = self.not_editable
        attr.not_visible = self.not_visible
        attr.required = self.required
        attr.isSystemMaintained = self.is_system_maintained
        attr.searchable = self.searchable

class DataModelEntityType():
    def __init__(self, entity_type_name):
        self.entity_type_name = entity_type_name
        self.attributes = []
    
    def __repr__(self):
        return f'entity_type_name: {self.entity_type_name} attributes: {self.attributes}'

    def addAttribute(self, attribute):
        if not isinstance(attribute, DataModelAttribute):
            raise Exception(f'expecting type DataModelAttribute, found {type(attribute)}')
        self.attributes.append(attribute)

    def transposeEntityToProtobuf(self, ent):
        #set the name
        ent.entity.name = self.entity_type_name
        ent.entity.alias = self.entity_type_name

        #add the attributes
        for new_attr in self.attributes:
            attr = ent.entity.properties.add()
            new_attr.transposeItemToProtobuf(attr)

class DataModeEntityCollection():
    def __init__(self, schema_config_file):
        self.entity_collection = {}
        with open(schema_config_file, mode='rt', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                type_name = row['ENTITY_TYPE_NAME']
                #if this is the first time seeing this entity type, add it now
                if type_name not in self.entity_collection:
                    self.entity_collection[type_name] = DataModelEntityType(type_name)
                #add the attribute to the entity type
                self.entity_collection[type_name].addAttribute(DataModelAttribute(row))

    def transposeModelToProtobuf(self, add_request):
        for entity_type in self.entity_collection.values():
            newType = add_request.entity_types.add()
            entity_type.transposeEntityToProtobuf(newType)

    def getEntityTypes(self):
        return self.entity_collection.keys()

class DataModelIndex():
    def __init__(self, row_dict):
        self.type_name = row_dict['ENTITY_TYPE_NAME']
        self.index_name = row_dict['INDEX_NAME']
        self.field_name = [row_dict['FIELD_NAME'],]
        self.is_ascending = self.validateBoolean(row_dict['IS_ASCENDING'])
        self.is_unique = self.validateBoolean(row_dict['IS_UNIQUE'])
        self.description = row_dict['DESCRIPTION']

    def validateBoolean(self, item):
        if item.upper()[0] not in ['T','F']:
            raise Exception(F'unknown boolean type {item}')
        if item.upper()[0] == 'T':
            return True
        return False

    def addField(self, row_dict):
        self.field_name.append(row_dict['FIELD_NAME'])

    def transposeIndexToProtobuf(self, add_request):
        index_item = add_request.field_indexes.add()
        index_item.name = self.index_name
        index_item.fields = ','.join(self.field_name)
        index_item.isAscending = self.is_ascending
        index_item.isUnique = self.is_unique
        index_item.description = self.description

    def __repr__(self):
        return F'{self.type_name=} {self.index_name=} {self.field_name=} {self.is_ascending=} {self.is_unique=} {self.description=}'

class DataModelIndexCollection():
    def __init__(self, index_file):
        self.index_collection = {}
        with open(index_file, mode='rt', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                type_name = row['ENTITY_TYPE_NAME']
                index_name = row['INDEX_NAME']
                key = (type_name,index_name)
                if key not in self.index_collection:
                    self.index_collection[key] = DataModelIndex(row)
                else:
                    self.index_collection[key].addField(row)




def create_data_model(data_model_file, knowledge_config_file, mapping_config_file):
    #parse data model and transpose into protobuf
    data_model = DataModeEntityCollection(data_model_file)
    add_request = esriPBuffer.graph.AddNamedTypesRequest_pb2.GraphNamedObjectTypeAddsRequest()
    data_model.transposeModelToProtobuf(add_request)

    #setup REST API 
    conn = knowledge_server.KnowledgeConnection(knowledge_config_file)
    kapi = knowledge_server.KnowledgeAPI(conn)
    szfunc = knowledge_server.SenzingKnowledgeFunctions(kapi)

    mapper = entity_mapper.EntityMapper(mapping_config_file)

    print("adding entity types")
    add_response = kapi.addNamedType(add_request)
    print(add_response)

    print("adding relationship types")
    add_response = szfunc.addRecordToEntityRelType(mapper.getResolvedEntityType())
    print(add_response)
    add_response = szfunc.addEntityToEntityRelType(mapper.getResolvedEntityType())
    print(add_response)

def delete_data_model(data_model_file, knowledge_config_file):
    data_model = DataModeEntityCollection(data_model_file)

    conn = knowledge_server.KnowledgeConnection(knowledge_config_file)
    kapi = knowledge_server.KnowledgeAPI(conn)

    # delete entity types
    print('deleting entity types')
    for entity_type_name in data_model.getEntityTypes():
        response = kapi.deleteEntityType(entity_type_name)
        print(response)

    # delete relationship types
    print('deleting entity types')
    response = kapi.deleteRelType("resolved_to")
    print(response)
    response = kapi.deleteRelType("related_to")
    print(response)

