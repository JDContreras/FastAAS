# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 14:47:34 2022

@author: jamuj
"""
import json
import asyncio
import redis
from redis.commands.graph.node import Node
from redis.commands.graph.edge import Edge
#conection to database on redislab
redislab_url = 'redis://redis-11621.c73.us-east-1-2.ec2.cloud.redislabs.com' #redis://[[default]:[kW5cZ20lifbjiXU88gKb6qVlNh3OlxkK]]@redis-11621.c73.us-east-1-2.ec2.cloud.redislabs.com:11621/0 --> 'rediss://:password@hostname:port/0'
db = redis.from_url(url=redislab_url,
                          password="kW5cZ20lifbjiXU88gKb6qVlNh3OlxkK",
                          port= 11621,
                          username = "default")
gdb = db.graph("graph1")

#node names
content = "content" #objects in dataSpecificationContent
dsc = "dataSpecificationContent"
eds = "embeddedDataSpecification" # each object in embeddedDataSpecifications list
idf = "identification"
cd = "conceptDescription"
sm = "submodel"
sme = "submodelElement"
modelTypesNames = ["Property","File","SubmodelElementCollection","ConceptDescription" , "Submodel", "Asset", "AssetAdministrationShell"]
dataObjectTypeNames = ["string","","float","langString" , "anyURI"] #todo: chnage "" for null o NaN

modelTypesDir = {}
all_keys = ['assetAdministrationShells', 'assets', 'submodels', 'conceptDescriptions']
ass_properties = ["conceptDictionaries","identification","idShort", "modelType"]
ass_keys = []
f = open('05_Bosch.json')
data = json.load(f)


        
def val_query(q,p):
        res = gdb.query(q,p)
        return res.result_set[0][0]
        

def create_modelTypes_node(modelTypesNames):
    for j in modelTypesNames:
        #modelTypesDir[j] = Node(label="modelType", properties = {"name":j})
        q = "MERGE (:modelType {name:$n})"
        p = {"n":j}
        gdb.query(q,p)

def create_dataObjectType_node(dataObjectTypeNames):
    for j in dataObjectTypeNames:
        #modelTypesDir[j] = Node(label="modelType", properties = {"name":j})
        q = "MERGE (:dataObjectType {name:$n})"
        p = {"n":j}
        gdb.query(q,p)
    
def add_modelType(parent_label, parent_id, modelTypesName):
    q = """MATCH (a:%s) WHERE ID(a)=$id  
        MATCH (b:modelType {name:$name}) 
        CREATE (a)-[:has_modelType]->(b)""" % parent_label
    p = {"id":parent_id, "name":modelTypesName}
    gdb.query(q,p)

def add_idf(parent_label, parent_id, _idType, _id):   
    q = """MATCH (a:%s) WHERE ID(a)= $_parent_id 
        CREATE (a)-[:has_identification]->(:identification {idType:$idType,id:$id})""" %parent_label
    p = {"label":parent_label,"_parent_id": parent_id, "idType":_idType, "id":_id}
    gdb.query(q,p)

def add_values(parent_id, values):
    for m in values:
        if isinstance(m["value"],list):
            #print(count)
            
            q = """MATCH (b:submodelElement) WHERE ID(b)=%s 
            CREATE (b)-[:has_value]->(a:value {idShort:$_idShort, category:$_category, kind: $_kind})
            RETURN ID(a)"""%parent_id
            p = {"_idShort":m["idShort"], "_category":m["category"], "_kind": m["kind"] }
            #print(p)
            value_id = val_query(q,p)
            print("--")
            
            q = """MATCH (a:value) WHERE ID(a) = %s 
                MATCH (b:conceptDescription)-[:has_identification]->(:identification {idType:$_idType,id:$_id})
                MATCH (c:modelType {name:$_name})
                MATCH (d:dataObjectType {name:$_name2})
                CREATE (a)-[:semanticId]->(b)
                CREATE (a)-[:has_modelType]->(c)
                CREATE (a)-[:valueType]->(d)"""%value_id
                #todo: add value descriptions when exist "_value": m["value"]
            try:
                p = {"_idType":m["semanticId"]["keys"][0]["idType"], "_id":m["semanticId"]["keys"][0]["value"], "_name": m["modelType"]["name"], "_name2": m["valueType"]["dataObjectType"]["name"]}
            except:
                p = {"_idType":m["semanticId"]["keys"][0]["idType"], "_id":m["semanticId"]["keys"][0]["value"], "_name": m["modelType"]["name"], "_name2": ""}
            gdb.query(q,p)
            for h in m["value"]:
                q = """MATCH (b:value) WHERE ID(b)=%s 
                CREATE (b)-[:has_value]->(a:value {idShort:$_idShort, category:$_category, kind: $_kind})
                RETURN ID(a)"""%value_id
                p = {"_idShort":h["idShort"], "_category":h["category"], "_kind": h["kind"] }
                #print(p)
                value_id2 = val_query(q,p)
                print("--")
                
                q = """MATCH (a:value) WHERE ID(a) = %s 
                    MATCH (b:conceptDescription)-[:has_identification]->(:identification {idType:$_idType,id:$_id})
                    MATCH (c:modelType {name:$_name})
                    MATCH (d:dataObjectType {name:$_name2})
                    CREATE (a)-[:semanticId]->(b)
                    CREATE (a)-[:has_modelType]->(c)
                    CREATE (a)-[:valueType]->(d)"""%value_id2
                try:
                    p = {"_idType":h["semanticId"]["keys"][0]["idType"], "_id":h["semanticId"]["keys"][0]["value"], "_name": h["modelType"]["name"], "_name2": h["valueType"]["dataObjectType"]["name"]}
                except:
                    p = {"_idType":h["semanticId"]["keys"][0]["idType"], "_id":h["semanticId"]["keys"][0]["value"], "_name": h["modelType"]["name"], "_name2": ""}
                gdb.query(q,p)
            print("value: " + str(m["value"])+" added")
        else:

            q = """MATCH (b:submodelElement) WHERE ID(b)=%s 
            CREATE (b)-[:has_value]->(a:value {idShort:$_idShort, category:$_category, value:$_value, kind: $_kind})
            RETURN ID(a)"""%parent_id
            p = {"_idShort":m["idShort"], "_category":m["category"], "_value": m["value"], "_kind": m["kind"] }
            print(p)
            value_id = val_query(q,p)
            
            print("--")
            q = """MATCH (a:value) WHERE ID(a) = %s 
                MATCH (b:conceptDescription)-[:has_identification]->(:identification {idType:$_idType,id:$_id})
                MATCH (c:modelType {name:$_name})
                MATCH (d:dataObjectType {name:$_name2})
                CREATE (a)-[:semanticId]->(b)
                CREATE (a)-[:has_modelType]->(c)
                CREATE (a)-[:valueType]->(d)"""%value_id
                #todo: add value descriptions when exist
            try:
                p = {"_idType":m["semanticId"]["keys"][0]["idType"], "_id":m["semanticId"]["keys"][0]["value"], "_name": m["modelType"]["name"], "_name2": m["valueType"]["dataObjectType"]["name"]}
            except:
                p = {"_idType":m["semanticId"]["keys"][0]["idType"], "_id":m["semanticId"]["keys"][0]["value"], "_name": m["modelType"]["name"], "_name2": ""}
            gdb.query(q,p)
            print("value: " + str(m["value"])+" added")
        
        


def create_conceptDescriptions(data):
    for i in data["conceptDescriptions"]:
        #create the main node
        idshort = i["idShort"]
        q = "CREATE (a:conceptDescription {idShort:$_idShort}) RETURN ID(a)"
        p = {"_idShort":idshort}
        cd_id = val_query(q,p)
        print("###")
        print(cd_id)
        add_idf(cd, cd_id, i["identification"]["idType"],i["identification"]["id"])
        #print(i["modelType"]["name"])
        add_modelType(cd,cd_id,i["modelType"]["name"])
        index1 = 0
        for j in i["embeddedDataSpecifications"]:
            ds = j["dataSpecificationContent"]
            q = """MATCH (a:conceptDescription) WHERE ID(a) = %s
                CREATE (a)-[:embeddedDataSpecification {index:$_index}]->(b:dataSpecificationContent {unit: $_unit, dataType: $_dataType})
                RETURN ID(b)""" %cd_id
            p = {"_idShort": idshort, "_index":index1, "_unit": ds["unit"], "_dataType": ds["dataType"]}
            ds_id = val_query(q,p)
            for f in j["dataSpecificationContent"]["preferredName"]:
            
                q = """MATCH (c:dataSpecificationContent) WHERE ID(c) = %s
                    MERGE (d:content {lenguage:$_lenguage, text:$_text})
                    CREATE (c)-[:preferredName]->(d)"""%ds_id
                p = {"_idShort":idshort,  "_index": index1, "_lenguage": f["language"], "_text":f["text"]}
                gdb.query(q,p)
                
            for f in j["dataSpecificationContent"]["shortName"]:
                
                q = """MATCH (c:dataSpecificationContent) WHERE ID(c) = %s
                    MERGE (d:content {lenguage:$_lenguage, text:$_text})
                    CREATE (c)-[:shortName]->(d)"""%ds_id
                p = {"_idShort":idshort,  "_index": index1, "_lenguage": f["language"], "_text":f["text"]}
                gdb.query(q,p)
            
            index1 += 1
  


def create_submodel(data):
    for i in data["submodels"]:
        #create the main node
        idshort = i["idShort"]
        q = "CREATE (a:%s {idShort:$_idShort, kind:$_kind}) RETURN ID(a)" % sm
        p = {"_idShort":idshort, "_kind": i["kind"]}
        model_id = val_query(q,p)
        add_idf(sm, model_id, i["identification"]["idType"],i["identification"]["id"])
        #print(i["modelType"]["name"])
        add_modelType(sm,model_id,i["modelType"]["name"])
        
        for j in i["submodelElements"]:
            idshort2 = j["idShort"]
            _idType = j["semanticId"]["keys"][0]["idType"]
            _id = j["semanticId"]["keys"][0]["value"]
            _value = j["value"]
            
            if isinstance(_value,list):
                print("list")
                q = """MATCH (h:submodel) WHERE ID(h)=%s
                    MATCH (a:conceptDescription)-[:has_identification]->(:identification {idType:$_idType,id:$_id}) 
                    CREATE (h)-[:has_submodelElement]->(b:submodelElement {idShort:$_idShort, kind:$_kind, category: $_category})-[:semanticId]->(a)
                    RETURN ID(b) LIMIT 1""" %model_id
                p = {"_idShort":idshort2, "_idType":_idType, "_id":_id, "_kind": j["kind"], "_category":j["category"]}
                val_id = val_query(q,p)
                add_values(val_id,_value)
                
            else:
                print("value: " + str(_value)+" added") 
                q = """MATCH (h:submodel) WHERE ID(h)=%s
                    MATCH (f:conceptDescription)-[:has_identification]->(:identification {idType:$_idType,id:$_id}) 
                    CREATE (h)-[:has_submodelElement]->(b:submodelElement {idShort:$_idShort, kind:$_kind, value:$_value, category: $_category})-[:semanticId]->(f)""" % model_id
                try:
                    p = {"_idShort":idshort2, "_idType":_idType, "_id":_id, "_kind": j["kind"],"_value":_value, "_category":j["category"]}
                except KeyError as e:
                    print(e.args[0])
                    p = {"_idShort":idshort2, "_idType":_idType, "_id":_id, "_kind": j["kind"],"_value":_value, "_category":""}
                gdb.query(q,p)
            #todo: iterate in the keys when there are more than 1 keys in semanticId
    print("fin")
    


def create_asset(data):
    for i in data["assets"]:
        q = "CREATE (a:asset {idShort:$_idShort, kind:$_kind}) RETURN ID(a)" 
        p = {"_idShort":i["idShort"], "_kind": i["kind"]}
        model_id = val_query(q,p)
        add_idf("asset", model_id, i["identification"]["idType"],i["identification"]["id"])
        add_modelType("asset",model_id,i["modelType"]["name"])
        
def create_aas(data):
    for i in data["assetAdministrationShells"]:
        q = "CREATE (a:assetAdministrationShell {idShort:$_idShort}) RETURN ID(a)" 
        p = {"_idShort":i["idShort"]}
        aas_id = val_query(q,p)
        add_idf("asset", aas_id, i["identification"]["idType"],i["identification"]["id"])
        add_modelType("assetAdministrationShell",aas_id,i["modelType"]["name"])
        q = """MATCH (a:assetAdministrationShell) WHERE ID(a)=%s
            MATCH (b:asset)-[:has_identification]->(:identification {idType:$_idType,id:$_id}) 
            CREATE (b)-[:has_aas]->(a)""" %aas_id
        p= {"_idType": i["asset"]["keys"][0]["idType"], "_id":i["asset"]["keys"][0]["value"] }
        gdb.query(q,p)
        for n in i["submodels"]:
            q = """MATCH (a:assetAdministrationShell) WHERE ID(a)=%s
                MATCH (b:submodel)-[:has_identification]->(:identification {idType:$_idType,id:$_id}) 
                CREATE (a)-[:has_submodel]->(b)""" %aas_id
            p= {"_idType": n["keys"][0]["idType"], "_id":n["keys"][0]["value"] }
            gdb.query(q,p)

        
#create_modelTypes_node(modelTypesNames)
#create_dataObjectType_node(dataObjectTypeNames)
#create_conceptDescriptions(data)
#create_submodel(data)

#create_asset(data)
create_aas(data)
    
    
    
    