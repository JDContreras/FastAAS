# -*- coding: utf-8 -*-
"""
Created on Sun Apr 17 21:05:44 2022

@author: jamuj
"""
import aioredis
import httpx
from aioredis.exceptions import ResponseError
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import FastAPI
from pydantic import BaseSettings
import asyncio
import aioredis
#application for rest
app = FastAPI()
#conection to database on redislab
redislab_url = 'redis://redis-11621.c73.us-east-1-2.ec2.cloud.redislabs.com' #redis://[[default]:[kW5cZ20lifbjiXU88gKb6qVlNh3OlxkK]]@redis-11621.c73.us-east-1-2.ec2.cloud.redislabs.com:11621/0 --> 'rediss://:password@hostname:port/0'
redis = aioredis.from_url(url=redislab_url,
                          password="kW5cZ20lifbjiXU88gKb6qVlNh3OlxkK",
                          port= 11621,
                          username = "default")

@app.get("/items/{item_id}")
async def read_item(item_id):
    return {"item_id": item_id}
