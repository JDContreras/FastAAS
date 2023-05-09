# -*- coding: utf-8 -*-
"""
Created on Tue Apr 26 20:21:17 2022

@author: pcJuanDavid
"""

import json
import asyncio
import redis
from redis.commands.graph.node import Node
from redis.commands.graph.edge import Edge
from redis.commands.json.path import Path
import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import NumericFilter, Query
#conection to database on redislab
redislab_url = 'redis://redis-11621.c73.us-east-1-2.ec2.cloud.redislabs.com' #redis://[[default]:[kW5cZ20lifbjiXU88gKb6qVlNh3OlxkK]]@redis-11621.c73.us-east-1-2.ec2.cloud.redislabs.com:11621/0 --> 'rediss://:password@hostname:port/0'
db = redis.from_url(url=redislab_url,
                          password="kW5cZ20lifbjiXU88gKb6qVlNh3OlxkK",
                          port= 11621,
                          username = "default")

f = open('05_Bosch.json')
data = json.load(f)

aas = data["assetAdministrationShells"]
asset = data["assets"]
submodel = data["submodels"]
conceptDescriptions = data["conceptDescriptions"]

db.json().set("aas", ".", {"assetAdministrationShells": aas, "assets": asset, "submodels":submodel,"conceptDescriptions": conceptDescriptions})
#db.ft().create_index(TextField("$.user.name", as_name="name") )
#db.json().get('aas', Path(".submodels[?(submodels.*.semanticId.keys[0].value == 'https://www.hsu-hh.de/aut/aas/nameplate')]"))
#db.json().get('aas', Path(".submodels[?(.*.semanticId.keys[0].value == 'https://www.hsu-hh.de/aut/aas/nameplate')]"))
#db.json().get('aas', Path(".submodels[?(.*.identification.id == 'http://boschrexroth.com/shells/R036447000/1005625831070001/submodels/technicalSpecification/')]"))

def submodel_by_id(aas,_id):
    path = ".submodels[?(@.*.identification.id == '{a}')]".format(a = _id)
    return db.json().get(aas, path)

def submodelElement_by_id(aas,_id):
    path = ".submodels[?(@.*.identification.id == '{a}')]".format(a = _id)
    return db.json().get(aas, path)