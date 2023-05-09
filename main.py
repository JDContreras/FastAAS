# -*- coding: utf-8 -*-
"""
Created on Sun Jul 10 21:01:50 2022

@author: jamuj
"""

from fastapi import FastAPI
from base64 import urlsafe_b64encode, urlsafe_b64decode, b64decode
import json

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}

#GetAllAssetAdministrationShellIdsByAssetLink
@app.get("/lookup/shells")
async def lookup_aas(assetids: str = "eyJzZXJpYWwiOiIxMjM0In0"):
    x = urlsafe_b64decode(assetids+"==")
    y = json.loads(x)
    return y