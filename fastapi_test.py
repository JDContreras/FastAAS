# -*- coding: utf-8 -*-
"""
Created on Sun Jul 10 21:01:50 2022

@author: jamuj
"""

from fastapi import FastAPI
from base64 import urlsafe_b64encode, urlsafe_b64decode

def base64UrlDecode(base64Url):
    padding = b'=' * (4 - (len(base64Url) % 4))
 
    return urlsafe_b64decode(base64Url + padding)

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}

#GetAllAssetAdministrationShellIdsByAssetLink
@app.get("/lookup/shells")
async def lookup_aas(assetids: str = "nn"):
    print(assetids)
    return {"AAShellID": base64UrlDecode(assetids)}