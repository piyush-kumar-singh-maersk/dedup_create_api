from fastapi import FastAPI, Path
from typing import Optional
from pydantic import BaseModel
import pandas as pd
import numpy as np
from geopy.distance import lonlat, distance
from fuzzywuzzy import process, fuzz
import json
from fastapi.responses import JSONResponse
from fastapi import Request
import uvicorn
import httpx
import requests
from starlette.requests import Request
import time
app = FastAPI()

@app.post('/bulk-dedup-create-record')
async def get_body(request: Request):
    JScontent = await request.json()    
    # jsonfile = {}
    jsonfile = []
    counter = 1
    for temp1 in JScontent["DATA"]:
        res = requests.post('http://localhost:8000/dedup-create-record/', 
                        headers = {'Content-type': 'application/json'}, 
                        json = JScontent["DATA"][temp1])
        temp = {}
        if (res.status_code == 200):
            #STATUS
            status_key = 'Status'
            status_value = res.status_code
            temp[status_key] = status_value
            #JSON
            json_key = 'Response'
            json_value = res.json()
            temp[json_key] = json_value
        else:
            #STATUS
            status_key = 'Status'
            status_value = res.status_code
            temp[status_key] = status_value
            #JSON
            json_key = 'Response'
            json_value = None
            temp[json_key] = json_value
        # final_key = 'RESPONSE ' + str(counter)
        final_key = None
        final_value = temp
        jsonfile.append(final_value)
        counter += 1
    return jsonfile

# if __name__ == "__main__":
#     uvicorn.run(app, host="127.0.0.1", port=8001)