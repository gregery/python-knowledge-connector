import json
import requests
import gzip

import esriPBuffer.graph.QueryDataModelResponse_pb2
import esriPBuffer.graph.AddNamedTypesRequest_pb2
import esriPBuffer.graph.AddNamedTypesResponse_pb2
import esriPBuffer.graph.DeleteNamedTypeResponse_pb2
import esriPBuffer.graph.DataModelTypes_pb2
import esriPBuffer.graph.ApplyEditsRequest_pb2
import esriPBuffer.graph.ApplyEditsResponse_pb2
import esriPBuffer.EsriExtendedTypes.EsriExtendedTypes_pb2
import esriPBuffer.graph.QueryResponse_pb2

from google.protobuf.internal.encoder import _VarintBytes
from google.protobuf.internal.decoder import _DecodeVarint


class DataModel:
    def __init__(self, model_):
        self.data_model = model_

    def getEntityTypes(self):
        entity_types = []
        for entity_type in self.data_model.entity_types:
            entity_types.append(entity_type.entity.name)
        return entity_types

    def getRelationshipTypes(self):
        relationship_types = []
        for rel_type in self.data_model.relationship_types:
            relationship_types.append(rel_type.relationship.name)
        return relationship_types

    def __str__(self):
        return str(self.data_model)


class KnowledgeConnection:
    def __init__(self, config_filename):
        required_keys = [
            "host",
            "username",
            "password",
            "instance",
            "dbname",
            "verify_ssl",
        ]
        self.auth_token = ""
        with open(config_filename, mode='rt', encoding='utf-8') as config_file:
            self.config_params = json.load(config_file)["arcgis_config"]
            for required_key in required_keys:
                if required_key not in self.config_params:
                    raise Exception("config is missing required key: " + required_key)
        # if we are not verifying ssl cert and authorities, let's suppress the warnings
        if self.getVerifySSL() is False:
            requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
        self.session = requests.Session()

    def getHost(self):
        return self.config_params["host"]

    def getUsername(self):
        return self.config_params["username"]

    def getPassword(self):
        return self.config_params["password"]

    def getInstance(self):
        return self.config_params["instance"]

    def getDBName(self):
        return self.config_params["dbname"]

    def getAuthToken(self):
        if self.auth_token == "":
            self.login()
        return self.auth_token

    def getVerifySSL(self):
        return self.config_params["verify_ssl"]

    def login(self):
        # query info for the token url
        infoUrl = f"https://{self.getHost()}/{self.getInstance()}/rest/info"
        params = {"f": "json"}
        rdata = self.session.get(infoUrl, params=params, verify=self.getVerifySSL()).json()
        tokenUrl = rdata["authInfo"]["tokenServicesUrl"]

        refer_url = f"https://{self.getHost()}/{self.getInstance()}"
        # query for a token
        tokenRequestParams = {
            "f": "json",
            "username": self.getUsername(),
            "password": self.getPassword(),
            "client": "referer",
            "referer": refer_url,
            "expiration": 525600,
        }
        tokenResponse = requests.post(tokenUrl, data=tokenRequestParams, verify=self.getVerifySSL())
        token = tokenResponse.json()["token"]
        self.auth_token = token


class KnowledgeAPI:
    def __init__(self, knowledge_connection):
        if type(knowledge_connection) is not KnowledgeConnection:
            raise Exception( "KnowledgeAPI must be constructed with a valid KnowledgeConnection object")
        self.kconn = knowledge_connection

    def getDataModel(self):
        url = f"https://{self.kconn.getHost()}/{self.kconn.getInstance()}/rest/services/Hosted/{self.kconn.getDBName()}/KnowledgeGraphServer/dataModel/queryDataModel"
        params = {"f": "pbf", "token": self.kconn.getAuthToken()}
        r = self.kconn.session.get(url, params=params, verify=self.kconn.getVerifySSL())
        query_data_model_response = esriPBuffer.graph.QueryDataModelResponse_pb2.GraphDataModel()
        query_data_model_response.ParseFromString(r.content)
        model = DataModel(query_data_model_response)
        return model

    ####
    # Data Model functions
    ###
    def addNamedType(self, new_type):
        if (isinstance(new_type, esriPBuffer.graph.AddNamedTypesRequest_pb2.GraphNamedObjectTypeAddsRequest)):
            raise Exception( "new entity type must be a protobuf of type esriPBuffer.graph.AddNamedTypesRequest_pb2.GraphNamedObjectTypeAddsRequest")

        url = f"https://{self.kconn.getHost()}/{self.kconn.getInstance()}/rest/services/Hosted/{self.kconn.getDBName()}/KnowledgeGraphServer/dataModel/edit/namedTypes/add"
        params = {"f": "pbf", "token": self.kconn.getAuthToken()}
        raw_response = self.kconn.session.post(
            url,
            params=params,
            headers={"Content-Type": "application/octet-stream"},
            data=new_type.SerializeToString(),
            verify=self.kconn.getVerifySSL(),
        )
        add_response = esriPBuffer.graph.AddNamedTypesResponse_pb2.GraphNamedObjectTypeAddsResponse()
        add_response.ParseFromString(raw_response.content)
        return add_response

    def deleteNamedType(self, type_name):
        url = f"https://{self.kconn.getHost()}/{self.kconn.getInstance()}/rest/services/Hosted/{self.kconn.getDBName()}/KnowledgeGraphServer/dataModel/edit/namedTypes/{type_name}/delete"
        params = {"f": "pbf", "token": self.kconn.getAuthToken()}
        r = self.kconn.session.post(
            url,
            params=params,
            headers={"Content-Type": "application/octet-stream"},
            verify=self.kconn.getVerifySSL(),
        )
        delete_response = (esriPBuffer.graph.DeleteNamedTypeResponse_pb2.GraphNamedObjectTypeDeleteResponse())
        delete_response.ParseFromString(r.content)

    def deleteEntityType(self, entity_type_name):
        self.deleteNamedType(entity_type_name)

    def deleteRelType(self, rel_type_name):
        self.deleteNamedType(rel_type_name)

    ####
    ## Graph functions
    ####
    def applyGraphEdits(self, edit_header, edit_frame):
        url = F"https://{self.kconn.getHost()}/{self.kconn.getInstance()}/rest/services/Hosted/{self.kconn.getDBName()}/KnowledgeGraphServer/graph/applyedits"
        params = {"f": "pbf", "token": self.kconn.getAuthToken()}
        raw_header = edit_header.SerializeToString()
        compressed_frame = gzip.compress(edit_frame.SerializeToString())
        data_string = (
            _VarintBytes(len(raw_header))
            + raw_header
            + _VarintBytes(len(compressed_frame))
            + compressed_frame
        )
        raw_response = self.kconn.session.post(
            url,
            params=params,
            headers={"Content-Type": "application/octet-stream"},
            data=data_string,
            verify=self.kconn.getVerifySSL(),
        )
        edit_response = esriPBuffer.graph.ApplyEditsResponse_pb2.GraphApplyEditsResult()
        # print(raw_response)
        # print(raw_response.content)
        edit_response.ParseFromString(raw_response.content)
        # print(type(edit_response))
        # print(edit_response)
        # print(edit_response.content)

        return edit_response

    def parseGraphQueryResponse(self, raw_response):
        message_length = len(raw_response.content)
        read_pos = 0
        header_length, read_pos = _DecodeVarint(raw_response.content, read_pos)

        # print("msg len: " + str(message_length))
        # print("header len: " + str(header_length))
        query_header = esriPBuffer.graph.QueryResponse_pb2.GraphQueryResultHeader()
        query_header.ParseFromString(
            raw_response.content[read_pos : header_length + read_pos]
        )
        # if the query returned nothing, there will be no body, so check for that
        if (header_length + read_pos) >= message_length:
            return (query_header, None)
        # print('header: ' + str(query_header))
        # print(query_header.error)
        frame_length, read_pos = _DecodeVarint( raw_response.content, read_pos + header_length)
        query_frame = esriPBuffer.graph.QueryResponse_pb2.GraphQueryResultFrame()
        query_frame.ParseFromString( raw_response.content[read_pos : frame_length + read_pos])
        return (query_header, query_frame)

    def queryGraphForEntityByEntityID(self, entity_id):
        url = F"https://{self.kconn.getHost()}/{self.kconn.getInstance()}/rest/services/Hosted/{self.kconn.getDBName()}/KnowledgeGraphServer/graph/query"
        cquery = F"MATCH (entity:senzing_resolved_entity) WHERE entity.entity_id = '{entity_id}' RETURN entity"
        params = {
            "f": "pbf",
            "token": self.kconn.getAuthToken(),
            "openCypherQuery": cquery,
        }
        raw_response = self.kconn.session.get(
            url, params=params, verify=self.kconn.getVerifySSL()
        )
        return self.parseGraphQueryResponse(raw_response)

    def queryGraphForRelationshipsByEntityID(self, entity_id):
        url = F"https://{self.kconn.getHost()}/{self.kconn.getInstance()}/rest/services/Hosted/{self.kconn.getDBName()}/KnowledgeGraphServer/graph/query"
        cquery = F"MATCH (entity:senzing_resolved_entity)-[r1]-() WHERE entity.entity_id = '{entity_id}' RETURN r1"
        params = {
            "f": "pbf",
            "token": self.kconn.getAuthToken(),
            "openCypherQuery": cquery,
        }
        raw_response = self.kconn.session.get( url, params=params, verify=self.kconn.getVerifySSL())
        return self.parseGraphQueryResponse(raw_response)

    def queryGraphForEntitiesByType(self, entity_type):
        url = F"https://{self.kconn.getHost()}/{self.kconn.getInstance()}/rest/services/Hosted/{self.kconn.getDBName()}/KnowledgeGraphServer/graph/query"
        cquery = F"MATCH (entity:{entity_type}) RETURN entity"
        params = {
            "f": "pbf",
            "token": self.kconn.getAuthToken(),
            "openCypherQuery": cquery,
        }
        raw_response = self.kconn.session.get(url, params=params, verify=self.kconn.getVerifySSL())
        return self.parseGraphQueryResponse(raw_response)

    def queryGraphForRecord(self, entity_id):
        url = F"https://{self.kconn.getHost()}/{self.kconn.getInstance()}/rest/services/Hosted/{self.kconn.getDBName()}/KnowledgeGraphServer/graph/query"
        cquery = F"MATCH (record:senzing_record) WHERE record.entity_id = '{entity_id}' RETURN record"
        params = {
            "f": "pbf",
            "token": self.kconn.getAuthToken(),
            "openCypherQuery": cquery,
        }
        raw_response = self.kconn.session.get(
            url, params=params, verify=self.kconn.getVerifySSL()
        )
        return self.parseGraphQueryResponse(raw_response)

class SenzingKnowledgeFunctions:
    def __init__(self, knowledge_api):
        if type(knowledge_api) is not KnowledgeAPI:
            raise Exception(
                "SenzingKnowledgeFunctions must be constructed with a valid KnowledgeAPI object"
            )
        self.kapi = knowledge_api

    def buildDataModel(self):
        # add entity types
        self.addRecordType()
        self.addResolvedEntityType()

        # add relationship types
        self.addRecordToEntityRelType()
        self.addEntityToEntityRelType()

    def deleteDataModel(self):
        # delete entity types
        self.kapi.deleteEntityType("senzing_record")
        self.kapi.deleteEntityType("senzing_resolved_entity")

        # delete relationship types
        self.kapi.deleteRelType("resolved_to")
        self.kapi.deleteRelType("related_to")

    def rebuildDataModel(self):
        self.deleteDataModel()
        self.buildDataModel()

    def addRecordType(self):
        add_request = (
            esriPBuffer.graph.AddNamedTypesRequest_pb2.GraphNamedObjectTypeAddsRequest()
        )
        newType = add_request.entity_types.add()
        newType.entity.name = "senzing_record"
        newType.entity.alias = "senzing_record"

        szAttr = newType.entity.properties.add()
        szAttr.name = "entity_id"
        szAttr.alias = "entity_id"
        szAttr.fieldType = (
            esriPBuffer.EsriExtendedTypes.EsriExtendedTypes_pb2.esriFieldTypeString
        )
        szAttr.not_nullable = True
        szAttr.not_editable = True
        szAttr.not_visible = False
        szAttr.required = True
        szAttr.isSystemMaintained = False
        szAttr.searchable = True

        szAttr = newType.entity.properties.add()
        szAttr.name = "record_id"
        szAttr.alias = "record_id"
        szAttr.fieldType = (
            esriPBuffer.EsriExtendedTypes.EsriExtendedTypes_pb2.esriFieldTypeString
        )

        szAttr = newType.entity.properties.add()
        szAttr.name = "data_source"
        szAttr.alias = "data_source"
        szAttr.fieldType = (
            esriPBuffer.EsriExtendedTypes.EsriExtendedTypes_pb2.esriFieldTypeString
        )

        szAttr = newType.entity.properties.add()
        szAttr.name = "record_description"
        szAttr.alias = "record_description"
        szAttr.fieldType = (
            esriPBuffer.EsriExtendedTypes.EsriExtendedTypes_pb2.esriFieldTypeString
        )

        field_index = newType.entity.field_indexes.add()
        field_index.name = "entity_id_idx"
        field_index.fields = "entity_id"
        field_index.isAscending = True
        field_index.isUnique = True
        field_index.description = "entity_id index"

        self.kapi.AddNamedType(add_request)

    def addResolvedEntityType(self):
        add_request = (
            esriPBuffer.graph.AddNamedTypesRequest_pb2.GraphNamedObjectTypeAddsRequest()
        )
        newType = add_request.entity_types.add()
        newType.entity.name = "senzing_resolved_entity"
        newType.entity.alias = "senzing_resolved_entity"

        szAttr = newType.entity.properties.add()
        szAttr.name = "entity_id"
        szAttr.alias = "entity_id"
        szAttr.fieldType = (
            esriPBuffer.EsriExtendedTypes.EsriExtendedTypes_pb2.esriFieldTypeString
        )
        szAttr.not_nullable = True
        szAttr.not_editable = True
        szAttr.not_visible = False
        szAttr.required = True
        szAttr.isSystemMaintained = False
        # szAttr.domain =
        szAttr.searchable = True

        szAttr = newType.entity.properties.add()
        szAttr.name = "name"
        szAttr.alias = "name"
        szAttr.fieldType = (
            esriPBuffer.EsriExtendedTypes.EsriExtendedTypes_pb2.esriFieldTypeString
        )

        szAttr = newType.entity.properties.add()
        szAttr.name = "address"
        szAttr.alias = "address"
        szAttr.fieldType = (
            esriPBuffer.EsriExtendedTypes.EsriExtendedTypes_pb2.esriFieldTypeString
        )

        field_index = newType.entity.field_indexes.add()
        field_index.name = "entity_id_idx"
        field_index.fields = "entity_id"
        field_index.isAscending = True
        field_index.isUnique = True
        field_index.description = "entity_id index"

        self.kapi.AddNamedType(add_request)

    def addRecordToEntityRelType(self):
        add_request = (
            esriPBuffer.graph.AddNamedTypesRequest_pb2.GraphNamedObjectTypeAddsRequest()
        )
        newType = add_request.relationship_types.add()
        newType.origin_entity_types.append("senzing_record")
        newType.dest_entity_types.append("senzing_resolved_entity")

        newType.strict_origin = True
        newType.strict_dest = True
        newType.relationship.name = "resolved_to"
        newType.relationship.alias = "resolved_to"
        newType.relationship.role = (
            esriPBuffer.graph.DataModelTypes_pb2.esriGraphNamedObjectRegular
        )

        szAttr = newType.relationship.properties.add()
        szAttr.name = "match_key"
        szAttr.alias = "match_key"
        szAttr.fieldType = (
            esriPBuffer.EsriExtendedTypes.EsriExtendedTypes_pb2.esriFieldTypeString
        )

        self.kapi.AddNamedType(add_request)

    def addEntityToEntityRelType(self):
        add_request = (
            esriPBuffer.graph.AddNamedTypesRequest_pb2.GraphNamedObjectTypeAddsRequest()
        )
        newType = add_request.relationship_types.add()
        newType.origin_entity_types.append("senzing_resolved_entity")
        newType.dest_entity_types.append("senzing_resolved_entity")

        newType.strict_origin = True
        newType.strict_dest = True
        newType.relationship.name = "related_to"
        newType.relationship.alias = "related_to"
        newType.relationship.role = (
            esriPBuffer.graph.DataModelTypes_pb2.esriGraphNamedObjectRegular
        )

        szAttr = newType.relationship.properties.add()
        szAttr.name = "match_level_code"
        szAttr.alias = "match_level_code"
        szAttr.fieldType = (
            esriPBuffer.EsriExtendedTypes.EsriExtendedTypes_pb2.esriFieldTypeString
        )

        szAttr = newType.relationship.properties.add()
        szAttr.name = "match_key"
        szAttr.alias = "match_key"
        szAttr.fieldType = (
            esriPBuffer.EsriExtendedTypes.EsriExtendedTypes_pb2.esriFieldTypeString
        )

        self.kapi.AddNamedType(add_request)

    def deleteEntityByEntityID(self, entity_id):
        # get the object_oid for this entity_id
        (header, body) = self.kapi.queryGraphForEntityByEntityID(entity_id)
        # if response is null, this entity doesn't exist, so we are done
        if body is None:
            return False

        # extract the object id
        oid = (
            body.rows[0]
            .values[0]
            .entity_value.properties["objectid"]
            .primitive_value.sint64_value
        )
        # now delete the entity
        self.deleteEntitiesByObjectID("senzing_resolved_entity", oid, True)
        return True

    def deleteNamedTypeByObjectID( self, named_type, object_id, cascade_delete, isEntity):
        edit_header = esriPBuffer.graph.ApplyEditsRequest_pb2.GraphApplyEditsHeader()
        # we are using objectid not globalid
        edit_header.useGlobalIDs = False
        edit_header.cascade_delete = cascade_delete

        edit_frame = esriPBuffer.graph.ApplyEditsRequest_pb2.GraphApplyEditsFrame()
        # if this object_id is just a single object_id, turn it into a list as required by the api
        if type(object_id) not in (list, tuple):
            object_id = [ object_id, ] 
            if isEntity is True:
                edit_frame.deletes.deleted_entity_ids[named_type].oid_array.value.extend(object_id)
            else:
                edit_frame.deletes.deleted_relationship_ids[named_type].oid_array.value.extend(object_id)

        return self.kapi.applyGraphEdits(edit_header, edit_frame)

    def deleteEntitiesByObjectID(self, entity_type, object_id, cascade_delete):
        return self.deleteNamedTypeByObjectID( entity_type, object_id, cascade_delete, True)

    def deleteRelationshipsByObjectID(self, relationship_type, object_id):
        return self.deleteNamedTypeByObjectID( relationship_type, object_id, False, False)

    def deleteAllEntitiesByType(self, entity_type, cascade_delete):
        (header, body) = self.kapi.queryGraphForEntitiesByType(entity_type)
        # if we get no results, nothing to delete so quit
        if body is None:
            return
        # accumulate the object ids of the entities we are deleting
        object_ids = []
        for row in body.rows:
            object_ids.append(row.values[0].entity_value.properties["objectid"].primitive_value.sint64_value)
        # delete the entities
        self.deleteEntitiesByObjectID(entity_type, object_ids, True)

    def deleteNamedTypeByGlobalID(self, entity_type, global_id, cascade_delete):
        edit_header = esriPBuffer.graph.ApplyEditsRequest_pb2.GraphApplyEditsHeader()
        # we are using globalID, not objectID
        edit_header.useGlobalIDs = True
        edit_header.cascade_delete = cascade_delete

        edit_frame = esriPBuffer.graph.ApplyEditsRequest_pb2.GraphApplyEditsFrame()
        edit_frame.deletes.deleted_entity_ids[entity_type].globalid_array = global_id
        return self.kapi.ApplyGraphEdits(edit_header, edit_frame)

    def addRelationshipBetweenResolvedEntities(self, from_entity_id, to_entity_id, match_level_code, match_key):
        edit_header = esriPBuffer.graph.ApplyEditsRequest_pb2.GraphApplyEditsHeader()
        edit_header.useGlobalIDs = True
        edit_header.cascade_delete = True

        (header, body) = self.kapi.queryGraphForEntityByEntityID(from_entity_id)
        from_uuid = (
            body.rows[0]
            .values[0]
            .entity_value.properties["globalid"]
            .primitive_value.uuid_value
        )
        (header, body) = self.kapi.queryGraphForEntityByEntityID(to_entity_id)
        # if lookup failed, entity might not be loaded yet -- will get picked up on the other side
        if body is None:
            return False
        to_uuid = body.rows[0].values[0].entity_value.properties["globalid"].primitive_value.uuid_value

        edit_frame = esriPBuffer.graph.ApplyEditsRequest_pb2.GraphApplyEditsFrame()

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

        self.kapi.applyGraphEdits(edit_header, edit_frame)
        return True

    def addRelationshipBetweenRecordAndResolvedEntity(self, record_entity_id, entity_id, match_key):
        edit_header = esriPBuffer.graph.ApplyEditsRequest_pb2.GraphApplyEditsHeader()
        edit_header.useGlobalIDs = True
        edit_header.cascade_delete = True

        (header, body) = self.kapi.queryGraphForRecord(record_entity_id)
        from_uuid = body.rows[0] .values[0] .entity_value.properties["globalid"] .primitive_value.uuid_value

        (header, body) = self.kapi.queryGraphForEntityByEntityID(entity_id)
        to_uuid = body.rows[0] .values[0] .entity_value.properties["globalid"] .primitive_value.uuid_value

        edit_frame = esriPBuffer.graph.ApplyEditsRequest_pb2.GraphApplyEditsFrame()
        add_rel = edit_frame.adds.relationships["resolved_to"].namedObjectAdds.add()
        add_rel.properties["originGlobalID"].primitive_value.uuid_value = from_uuid
        add_rel.properties["destinationGlobalID"].primitive_value.uuid_value = to_uuid
        add_rel.properties["match_key"].primitive_value.string_value = match_key

        self.kapi.applyGraphEdits(edit_header, edit_frame)

    def addEntity(self, type_name, entity_attributes):
        edit_header = esriPBuffer.graph.ApplyEditsRequest_pb2.GraphApplyEditsHeader()
        edit_header.useGlobalIDs = True

        edit_frame = esriPBuffer.graph.ApplyEditsRequest_pb2.GraphApplyEditsFrame()

        add_entity = edit_frame.adds.entities[type_name].namedObjectAdds.add()
        for key, value in entity_attributes.items():
            add_entity.properties[key].primitive_value.string_value = value
        self.kapi.applyGraphEdits(edit_header, edit_frame)

    def addResolvedEntity(self, entity_attributes):
        self.addEntity("senzing_resolved_entity", entity_attributes)

    def addRecord(self, record_attributes):
        self.addEntity("senzing_record", record_attributes)

    def processEntity(self, entity_attributes, records, record_rels, entity_rels):
        # sync the resolved entity
        # first delete the entity if it currently exists
        self.deleteEntityByEntityID(entity_attributes["entity_id"])
        self.addResolvedEntity(entity_attributes)

        # add relationships
        for entity_rel in entity_rels:
            self.addRelationshipBetweenResolvedEntities(*entity_rel)

        # sync the records
        for record in records:
            self.addRecord(record)

        # add link from record to entity
        for record_rel in record_rels:
            self.addRelationshipBetweenRecordAndResolvedEntity(
                record_rel[0], record_rel[1], record_rel[2]
            )
