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
import requests
import csv
app = FastAPI()

#############################################################
# DATABASE
def read_db():
    print("DB READ")
    df_smds_data = pd.read_csv("smds_data.csv")
    df_smds_data = df_smds_data.applymap(str)

    df_unloc_data = pd.read_csv("unloc_data.csv")
    df_unloc_data = df_unloc_data.applymap(str)
    return df_smds_data, df_unloc_data

def write_csv_data(df_smds_data):
    #Writing the csv data
    df_smds_data.to_csv('smds_data.csv', index=False)
#############################################################

df_smds_data, df_unloc_data = read_db()

#############################################################
# MODEL
class City(BaseModel):
    RKST: Optional[str] = None
    CITY_NAME: Optional[str] = None
    ALT_CITY_NAME: Optional[str] = None
    RW_DESC: Optional[str] = None
    COUNTRY_CODE: Optional[str] = None
    LAT: Optional[str] = None
    LONG: Optional[str] = None
#############################################################

#############################################################
# MAIN CODE FOR API
@app.post("/dedup-create-record")
def read_root(city_input : City):
    #RKST
    if city_input.RKST is not None:
        value_RKST = city_input.RKST.lower()
    #CITY
    if city_input.CITY_NAME is not None and city_input.CITY_NAME != '':
        value_CITY_NAME = city_input.CITY_NAME.lower()
    else:
        return {"Message" : "INVALID CITY NAME",
                "ValidationStatus" : "FAILED",
                "InputData" : city_input,
                "MatchData" : None}
    #ALT-CITY
    if city_input.ALT_CITY_NAME is not None:
        value_ALT_CITY_NAME = city_input.ALT_CITY_NAME.lower()
    #STATE
    if (city_input.COUNTRY_CODE.lower() in ['ar', 'au', 'at', 'be', 'br', 'ca', 'co', 'cn', 'fr', 'de', 
                                            'in', 'it', 'jp', 'my', 'mx', 'nl', 'za', 'ch', 'us', 'vn']):
        if city_input.RW_DESC is not None and city_input.RW_DESC != '':
            value_RW_DESC = city_input.RW_DESC.lower()
        else:
            return {"Message" : "REGION NAME MANDATORY",
                    "ValidationStatus" : "FAILED",
                    "InputData" : city_input,
                    "MatchData" : None}
    else:
        if city_input.RW_DESC is not None and city_input.RW_DESC != '':
            value_RW_DESC = city_input.RW_DESC.lower()
    #COUNTRY CODE
    if city_input.COUNTRY_CODE is not None and city_input.COUNTRY_CODE != '':
        value_COUNTRY_CODE = city_input.COUNTRY_CODE.lower()
    else:
        return {"Message" : "INVALID COUNTRY",
                "ValidationStatus" : "FAILED",
                "InputData" : city_input,
                "MatchData" : None}
    #LAT
    if city_input.LAT is not None:
        value_LAT = city_input.LAT.lower()
    #LONG
    if city_input.LONG is not None:
        value_LONG = city_input.LONG.lower()

    ## CITY - CITY ITERATION ##########################
    if (city_input.CITY_NAME is None and city_input.CITY_NAME == ''):
        return {"Message" : "INVALID CITY NAME",
                "ValidationStatus" : "FAILED",
                "Data" : city_input,
                "MatchData" : None}
    else:
        if (value_COUNTRY_CODE in ['ar', 'au', 'at', 'be', 'br', 'ca', 'co', 'cn', 'fr', 'de', 'in', 'it', 
                                   'jp', 'my', 'mx', 'nl', 'za', 'ch', 'us', 'vn']):
            temp_df = df_smds_data.loc[(df_smds_data['RW_DESC'] == value_RW_DESC) & 
                                       (df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE)]
        else:
            temp_df = df_smds_data.loc[(df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE)]

        temp_df = temp_df.reset_index()
        temp_df = temp_df.astype(str)
        temp_df = temp_df.applymap(lambda s: s.lower() if type(s) == str else s)
        len_temp_data = len(temp_df)

        curr_city_info = value_CITY_NAME

        high_score = 0
        high_score_index = 0
        high_score_address = ''
        high_score_alt_city = ''
        high_score_lat = ''
        high_score_long = ''

        for j in range(0, len_temp_data):
            all_city_info = str(temp_df['CITY_NAME'][j])
            if (curr_city_info == all_city_info):
                return {"Message" : "DUPLICATE - CITY NAME MATCH - MATCHED WITH RKST = " + str(temp_df['RKST'][j]).upper(), 
                        "ValidationStatus" : "FAILED",
                        "InputData" : city_input,
                        "MatchData" : {"DUP_MATCH_RKST" : temp_df['RKST'][j],
                                "DUP_MATCH_UNLOC" : str(temp_df['UNLOC_CODE'][j]).replace("nan", "null"),
                                "DUP_MATCH_CITY" : str(all_city_info),
                                "DUP_MATCH_ALT_CITY" : str(temp_df['ALIAS_CITY'][j]).replace("nan", "null"),
                                "DUP_MATCH_SCORE" : str(100),
                                "DUP_MATCH_LAT" : str(temp_df['LAT'][j]),
                                "DUP_MATCH_LONG" : str(temp_df['LONG'][j])}}
        
        for j in range(0, len_temp_data):
            all_city_info = str(temp_df['CITY_NAME'][j])
            if (fuzz.token_set_ratio(curr_city_info, all_city_info) == 100 and curr_city_info != all_city_info):
                curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info) - 3
            else:
                curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info)
                
            if curr_score >= 95 and curr_score > high_score:
                high_score = curr_score
                high_score_index = j
                high_score_address = str(temp_df['CITY_NAME'][j])
                high_score_alt_city = str(temp_df['ALIAS_CITY'][j])
                high_score_lat = str(temp_df['LAT'][j])
                high_score_long = str(temp_df['LONG'][j])
        
        if high_score >= 95:
            DUP_MATCH_RKST = str(temp_df['RKST'][high_score_index])
            DUP_MATCH_UNLOC = str(temp_df['UNLOC_CODE'][high_score_index])
            DUP_MATCH_CITY = high_score_address
            DUP_MATCH_ALT_CITY = high_score_alt_city
            DUP_MATCH_SCORE = str(high_score)
            DUP_MATCH_LAT = high_score_lat
            DUP_MATCH_LONG = high_score_long
        else:
            DUP_MATCH_RKST = 'null'
            DUP_MATCH_UNLOC = 'null'
            DUP_MATCH_CITY = 'null'
            DUP_MATCH_ALT_CITY = 'null'
            DUP_MATCH_SCORE = 'null'
            DUP_MATCH_LAT = 'null'
            DUP_MATCH_LONG = 'null'

        if DUP_MATCH_CITY != 'null':
            return {"Message" : "DUPLICATE - CITY NAME MATCH - MATCHED WITH RKST = " + str(DUP_MATCH_RKST).upper(),
                    "ValidationStatus" : "FAILED",
                    "InputData" : city_input,
                    "MatchData" : {"DUP_MATCH_RKST" : DUP_MATCH_RKST, 
                                   "DUP_MATCH_UNLOC" : DUP_MATCH_UNLOC.replace("nan", "null"),
                                   "DUP_MATCH_CITY" : DUP_MATCH_CITY, 
                                   "DUP_MATCH_ALT_CITY" : DUP_MATCH_ALT_CITY.replace("nan", "null"),
                                   "DUP_MATCH_SCORE" : DUP_MATCH_SCORE, 
                                   "DUP_MATCH_LAT" : DUP_MATCH_LAT,
                                   "DUP_MATCH_LONG" : DUP_MATCH_LONG}}

    ## CITY - ALT CITY MATCH ITERATION ################
    if (city_input.CITY_NAME is None and city_input.CITY_NAME == ''):
        return {"Message" : "INVALID CITY NAME",
                "ValidationStatus" : "FAILED",
                "InputData" : city_input,
                "MatchData" : None}
    else:
        if (value_COUNTRY_CODE in ['ar', 'au', 'at', 'be', 'br', 'ca', 'co', 'cn', 'fr', 'de', 'in', 'it', 
                                'jp', 'my', 'mx', 'nl', 'za', 'ch', 'us', 'vn']):
            temp_df = df_smds_data.loc[(df_smds_data['RW_DESC'] == value_RW_DESC) & 
                                    (df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE)]
        else:
            temp_df = df_smds_data.loc[(df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE)]
        temp_df = temp_df.reset_index()
        temp_df = temp_df.astype(str)
        temp_df = temp_df.applymap(lambda s: s.lower() if type(s) == str else s)
        len_temp_data = len(temp_df)

        curr_city_info = value_CITY_NAME

        high_score = 0
        high_score_index = 0
        high_score_address = ''
        high_score_alt_city = ''
        high_score_lat = ''
        high_score_long = ''

        for j in range(0, len_temp_data):
            all_city_info = str(temp_df['ALIAS_CITY'][j])
            if (curr_city_info == all_city_info and temp_df['ALIAS_CITY'][j] != 'nan'):
                return {"Message" : "DUPLICATE - ALT CITY NAME MATCH - MATCHED WITH RKST = " + str(temp_df['RKST'][j]).upper(),
                        "ValidationStatus" : "FAILED",
                        "InputData" : city_input,
                        "MatchData" : {"DUP_MATCH_RKST" : temp_df['RKST'][j],
                                       "DUP_MATCH_UNLOC" : temp_df['UNLOC_CODE'][j].replace("nan", "null"),
                                       "DUP_MATCH_CITY" : str(temp_df['CITY_NAME'][j]),
                                       "DUP_MATCH_ALT_CITY" : str(all_city_info).replace("nan", "null"),
                                       "DUP_MATCH_SCORE" : str(100),
                                       "DUP_MATCH_LAT" : str(temp_df['LAT'][j]),
                                       "DUP_MATCH_LONG" : str(temp_df['LONG'][j])}}
        for j in range(0, len_temp_data):
            all_city_info = str(temp_df['ALIAS_CITY'][j])
            if (fuzz.token_set_ratio(curr_city_info, all_city_info) == 100 and 
                curr_city_info != all_city_info):
                curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info) - 3
            else:
                curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info)

            if curr_score >= 95 and curr_score > high_score and temp_df['ALIAS_CITY'][j] != 'nan':
                high_score = curr_score
                high_score_index = j
                high_score_address = str(temp_df['CITY_NAME'][j])
                high_score_alt_city = str(temp_df['ALIAS_CITY'][j])
                high_score_lat = str(temp_df['LAT'][j])
                high_score_long = str(temp_df['LONG'][j])
        
        if high_score >= 95:
            DUP_MATCH_RKST = str(temp_df['RKST'][high_score_index])
            DUP_MATCH_UNLOC = str(temp_df['UNLOC_CODE'][high_score_index])
            DUP_MATCH_CITY = high_score_address
            DUP_MATCH_ALT_CITY = high_score_alt_city
            DUP_MATCH_SCORE = str(high_score)
            DUP_MATCH_LAT = high_score_lat
            DUP_MATCH_LONG = high_score_long
        else:
            DUP_MATCH_RKST = 'null'
            DUP_MATCH_UNLOC = 'null'
            DUP_MATCH_CITY = 'null'
            DUP_MATCH_ALT_CITY = 'null'
            DUP_MATCH_SCORE = 'null'
            DUP_MATCH_LAT = 'null'
            DUP_MATCH_LONG = 'null'

        if DUP_MATCH_CITY != 'null':
            return {"Message" : "DUPLICATE - ALT CITY NAME MATCH - MATCHED WITH RKST = " + str(DUP_MATCH_RKST).upper(),
                    "ValidationStatus" : "FAILED",
                    "InputData" : city_input,
                    "MatchData" : {"DUP_MATCH_RKST" : DUP_MATCH_RKST, 
                                   "DUP_MATCH_UNLOC" : DUP_MATCH_UNLOC.replace("nan", "null"), 
                                   "DUP_MATCH_CITY" : DUP_MATCH_CITY, 
                                   "DUP_MATCH_ALT_CITY" : DUP_MATCH_ALT_CITY.replace("nan", "null"),
                                   "DUP_MATCH_SCORE" : DUP_MATCH_SCORE, 
                                   "DUP_MATCH_LAT" : DUP_MATCH_LAT,
                                   "DUP_MATCH_LONG" : DUP_MATCH_LONG}}

    ## ALT CITY - CITY & ALT CITY MATCH ITERATION #####
    if (city_input.ALT_CITY_NAME is not None and city_input.ALT_CITY_NAME != ''):
        if (value_COUNTRY_CODE in ['ar', 'au', 'at', 'be', 'br', 'ca', 'co', 'cn', 'fr', 'de', 'in', 'it', 
                                   'jp', 'my', 'mx', 'nl', 'za', 'ch', 'us', 'vn']):
            temp_df = df_smds_data.loc[(df_smds_data['RW_DESC'] == value_RW_DESC) & 
                                       (df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE)]
        else:
            temp_df = df_smds_data.loc[(df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE)]
        temp_df = temp_df.reset_index()
        temp_df = temp_df.astype(str)
        temp_df = temp_df.applymap(lambda s: s.lower() if type(s) == str else s)
        len_temp_data = len(temp_df)

        curr_city_info = value_ALT_CITY_NAME

        high_score = 0
        high_score_index = 0
        high_score_address = ''
        high_score_alt_city = ''
        high_score_lat = ''
        high_score_long = ''

        for j in range(0, len_temp_data):
            all_city_info_main = str(temp_df['CITY_NAME'][j])
            all_city_info_alt = str(temp_df['ALIAS_CITY'][j])
            if (curr_city_info == all_city_info_main):
                return {"Message" : "DUPLICATE - CITY NAME MATCH", 
                        "ValidationStatus" : "FAILED",
                        "InputData" : city_input,
                        "MatchData" : {"DUP_MATCH_RKST" : temp_df['RKST'][j],
                                       "DUP_MATCH_UNLOC" : temp_df['UNLOC_CODE'][j].replace("nan", "null"),
                                       "DUP_MATCH_CITY" : str(all_city_info_main),
                                       "DUP_MATCH_ALT_CITY" : str(temp_df['ALIAS_CITY'][j]).replace("nan", "null"),
                                       "DUP_MATCH_SCORE" : str(100),
                                       "DUP_MATCH_LAT" : str(temp_df['LAT'][j]),
                                       "DUP_MATCH_LONG" : str(temp_df['LONG'][j])}}
            elif (curr_city_info == all_city_info_alt and temp_df['ALIAS_CITY'][j] != 'nan'):
                return {"Message" : "DUPLICATE - ALT CITY NAME MATCH", 
                        "ValidationStatus" : "FAILED",
                        "InputData" : city_input,
                        "MatchData" : {"DUP_MATCH_RKST" : temp_df['RKST'][j],
                                       "DUP_MATCH_UNLOC" : temp_df['UNLOC_CODE'][j].replace("nan", "null"),
                                       "DUP_MATCH_CITY" : str(all_city_info_main),
                                       "DUP_MATCH_ALT_CITY" : str(temp_df['ALIAS_CITY'][j]).replace("nan", "null"),
                                       "DUP_MATCH_SCORE" : str(100),
                                       "DUP_MATCH_LAT" : str(temp_df['LAT'][j]),
                                       "DUP_MATCH_LONG" : str(temp_df['LONG'][j])}}
            
        for j in range(0, len_temp_data):
            all_city_info_main = str(temp_df['CITY_NAME'][j])
            if (fuzz.token_set_ratio(curr_city_info, all_city_info_main) == 100 and 
                curr_city_info != all_city_info_main):
                curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info_main) - 3
            else:
                curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info_main)
            if curr_score >= 95 and curr_score > high_score:
                high_score = curr_score
                high_score_index = j
                high_score_address = str(temp_df['CITY_NAME'][j])
                high_score_alt_city = str(temp_df['ALIAS_CITY'][j])
                high_score_lat = str(temp_df['LAT'][j])
                high_score_long = str(temp_df['LONG'][j])
        if high_score >= 95:
            DUP_MATCH_RKST = str(temp_df['RKST'][high_score_index])
            DUP_MATCH_UNLOC = str(temp_df['UNLOC_CODE'][high_score_index])
            DUP_MATCH_CITY = high_score_address
            DUP_MATCH_ALT_CITY = high_score_alt_city
            DUP_MATCH_SCORE = str(high_score)
            DUP_MATCH_LAT = high_score_lat
            DUP_MATCH_LONG = high_score_long
        else:
            DUP_MATCH_RKST = 'null'
            DUP_MATCH_UNLOC = 'null'
            DUP_MATCH_CITY = 'null'
            DUP_MATCH_ALT_CITY = 'null'
            DUP_MATCH_SCORE = 'null'
            DUP_MATCH_LAT = 'null'
            DUP_MATCH_LONG = 'null'

        if DUP_MATCH_CITY != 'null':
            return {"Message" : "DUPLICATE - CITY NAME MATCH - MATCHED WITH RKST = " + str(DUP_MATCH_RKST).upper(),
                    "ValidationStatus" : "FAILED",
                    "InputData" : city_input,
                    "MatchData" : {"DUP_MATCH_RKST" : DUP_MATCH_RKST, 
                                   "DUP_MATCH_UNLOC" : DUP_MATCH_UNLOC.replace("nan", "null"),
                                   "DUP_MATCH_CITY" : DUP_MATCH_CITY, 
                                   "DUP_MATCH_ALT_CITY" : DUP_MATCH_ALT_CITY.replace("nan", "null"),
                                   "DUP_MATCH_SCORE" : DUP_MATCH_SCORE, 
                                   "DUP_MATCH_LAT" : DUP_MATCH_LAT,
                                   "DUP_MATCH_LONG" : DUP_MATCH_LONG}}
        else:
            for j in range(0, len_temp_data):
                all_city_info_alt = str(temp_df['ALIAS_CITY'][j])
                if (fuzz.token_set_ratio(curr_city_info, all_city_info_alt) == 100 and 
                    curr_city_info != all_city_info_alt):
                    curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info_alt) - 3
                else:
                    curr_score = fuzz.token_set_ratio(curr_city_info, all_city_info_alt)
                if curr_score >= 95 and curr_score > high_score:
                    high_score = curr_score
                    high_score_index = j
                    high_score_address = str(temp_df['CITY_NAME'][j])
                    high_score_alt_city = str(temp_df['ALIAS_CITY'][j])
                    high_score_lat = str(temp_df['LAT'][j])
                    high_score_long = str(temp_df['LONG'][j])
            if high_score >= 95:
                DUP_MATCH_RKST = str(temp_df['RKST'][high_score_index])
                DUP_MATCH_UNLOC = str(temp_df['UNLOC_CODE'][high_score_index])
                DUP_MATCH_CITY = high_score_address
                DUP_MATCH_ALT_CITY = high_score_alt_city
                DUP_MATCH_SCORE = str(high_score)
                DUP_MATCH_LAT = high_score_lat
                DUP_MATCH_LONG = high_score_long
            else:
                DUP_MATCH_RKST = 'null'
                DUP_MATCH_UNLOC = 'null'
                DUP_MATCH_CITY = 'null'
                DUP_MATCH_ALT_CITY = 'null'
                DUP_MATCH_SCORE = 'null'
                DUP_MATCH_LAT = 'null'
                DUP_MATCH_LONG = 'null'

            if DUP_MATCH_CITY != 'null':
                return {"Message" : "DUPLICATE - ALT CITY NAME MATCH - MATCHED WITH RKST = " + str(DUP_MATCH_RKST).upper(),
                        "ValidationStatus" : "FAILED",
                        "InputData" : city_input,
                        "MatchData" : {"DUP_MATCH_RKST" : DUP_MATCH_RKST, 
                                       "DUP_MATCH_UNLOC" : DUP_MATCH_UNLOC.replace("nan", "null"), 
                                       "DUP_MATCH_CITY" : DUP_MATCH_CITY,
                                       "DUP_MATCH_ALT_CITY" : DUP_MATCH_ALT_CITY.replace("nan", "null"),
                                       "DUP_MATCH_SCORE" : DUP_MATCH_SCORE, 
                                       "DUP_MATCH_LAT" : DUP_MATCH_LAT,
                                       "DUP_MATCH_LONG" : DUP_MATCH_LONG}}

    ## LAT LONG - LAT LONG MATCH ITERATION ############
    if ((city_input.LAT is not None and city_input.LAT != '') and 
        (city_input.LONG is not None and city_input.LONG != '') and 
        (float(city_input.LAT) >= -90.0 and float(city_input.LAT) <= 90.0) and 
        (float(city_input.LONG) >= -180.0 and float(city_input.LONG) <= 180.0)):
        if (value_COUNTRY_CODE in ['ar', 'au', 'at', 'be', 'br', 'ca', 'co', 'cn', 'fr', 'de', 'in', 'it', 
                                   'jp', 'my', 'mx', 'nl', 'za', 'ch', 'us', 'vn']):
            temp_df = df_smds_data.loc[(df_smds_data['RW_DESC'] == value_RW_DESC) & 
                                       (df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE)]
        else:
            temp_df = df_smds_data.loc[(df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE)]
        temp_df = temp_df.reset_index()
        temp_df = temp_df.astype(str)
        temp_df = temp_df.applymap(lambda s: s.lower() if type(s) == str else s)
        len_temp_data = len(temp_df)

        high_score = 9999999999
        high_score_index = 0
        high_score_address = ''
        high_score_alt_city = ''
        high_score_lat = ''
        high_score_long = ''

        lat_long_info_1 = (value_LONG, value_LAT)

        for j in range(0, len_temp_data):
            lat_long_info_2 = (temp_df['LONG'][j], temp_df['LAT'][j])
            try:
                curr_score = distance(lonlat(*lat_long_info_1), lonlat(*lat_long_info_2)).km
            except:
                curr_score = 999
        
            if (curr_score <= 1.50000 and curr_score < high_score):
                high_score = curr_score
                high_score_index = j
                high_score_address = str(temp_df['CITY_NAME'][j])
                high_score_alt_city = str(temp_df['ALIAS_CITY'][j])
                high_score_lat = str(temp_df['LAT'][j])
                high_score_long = str(temp_df['LONG'][j])

        if high_score <= 1.50000:
            DUP_MATCH_RKST = str(temp_df['RKST'][high_score_index])
            DUP_MATCH_UNLOC = str(temp_df['UNLOC_CODE'][high_score_index])
            DUP_MATCH_CITY = high_score_address
            DUP_MATCH_ALT_CITY = high_score_alt_city
            DUP_MATCH_SCORE = str(high_score)
            DUP_MATCH_LAT = high_score_lat
            DUP_MATCH_LONG = high_score_long
        else:
            DUP_MATCH_RKST = 'null'
            DUP_MATCH_UNLOC = 'null'
            DUP_MATCH_CITY = 'null'
            DUP_MATCH_ALT_CITY = 'null'
            DUP_MATCH_SCORE = 'null'
            DUP_MATCH_LAT = 'null'
            DUP_MATCH_LONG = 'null'

        if DUP_MATCH_CITY != 'null':
            return {"Message" : "DUPLICATE - LAT LONG MATCH - MATCHED WITH RKST = " + str(DUP_MATCH_RKST).upper(),
                    "ValidationStatus" : "FAILED",
                    "InputData" : city_input,
                    "MatchData" : {"DUP_MATCH_RKST" : DUP_MATCH_RKST, 
                                   "DUP_MATCH_UNLOC" : DUP_MATCH_UNLOC.replace("nan", "null"),
                                   "DUP_MATCH_CITY" : DUP_MATCH_CITY, 
                                   "DUP_MATCH_ALT_CITY" : DUP_MATCH_ALT_CITY.replace("nan", "null"),
                                   "DUP_MATCH_SCORE" : DUP_MATCH_SCORE, 
                                   "DUP_MATCH_LAT" : DUP_MATCH_LAT,
                                   "DUP_MATCH_LONG" : DUP_MATCH_LONG}}

    ## UNLOC - UNLOC MATCH ITERATION ##################
    if (city_input.CITY_NAME is not None and city_input.CITY_NAME != ''):
        if (value_COUNTRY_CODE in ['ar', 'au', 'at', 'be', 'br', 'ca', 'co', 'cn', 'fr', 'de', 'in', 'it', 
                                'jp', 'my', 'mx', 'nl', 'za', 'ch', 'us', 'vn']):
            temp_unloc_df = df_unloc_data.loc[(df_unloc_data['State Name'] == value_RW_DESC) & 
                                            (df_unloc_data['Country'] == value_COUNTRY_CODE)]
        else:
            temp_unloc_df = df_unloc_data.loc[(df_unloc_data['Country'] == value_COUNTRY_CODE)]
        temp_unloc_df = temp_unloc_df.reset_index()
        len_temp_unloc_data = len(temp_unloc_df)

        unloc_code_found = ''

        for i in range(0, len_temp_unloc_data):
            input_combined = str(value_CITY_NAME)
            unloc_combined = str(temp_unloc_df['Name'][i])
            if (input_combined == unloc_combined):
                unloc_code_found = temp_unloc_df['UNLOC'][i]
                break
        
        if unloc_code_found is not None and unloc_code_found != '':
            if (value_COUNTRY_CODE in ['ar', 'au', 'at', 'be', 'br', 'ca', 'co', 'cn', 'fr', 'de', 'in', 'it', 
                                    'jp', 'my', 'mx', 'nl', 'za', 'ch', 'us', 'vn']):
                temp_smds_df = df_smds_data.loc[(df_smds_data['RW_DESC'] == value_RW_DESC) & 
                                                (df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE)]
            else:
                temp_smds_df = df_smds_data.loc[(df_smds_data['COUNTRY_CODE'] == value_COUNTRY_CODE)]
            temp_smds_df = temp_smds_df.reset_index()
            len_temp_smds_df = len(temp_smds_df)

            for i in range(0, len_temp_smds_df):
                if (unloc_code_found == temp_smds_df['UNLOC_CODE'][i]):
                    return {"Message" : "DUPLICATE - UNLOC MATCH - MATCHED WITH RKST = " + str(temp_smds_df['RKST'][i]).upper(),
                            "ValidationStatus" : "FAILED",
                            "InputData" : city_input,
                            "MatchData" : {"DUP_MATCH_RKST" : temp_smds_df['RKST'][i],
                                        "DUP_MATCH_UNLOC" : temp_smds_df['UNLOC_CODE'][i].replace("nan", "null"),
                                        "DUP_MATCH_CITY" : temp_smds_df['CITY_NAME'][i], 
                                        "DUP_MATCH_ALT_CITY" : temp_smds_df['ALIAS_CITY'][i].replace("nan", "null"),
                                        "DUP_MATCH_SCORE" : 'null', 
                                        "DUP_MATCH_LAT" : temp_smds_df['LAT'][i],
                                        "DUP_MATCH_LONG" : temp_smds_df['LONG'][i]}}

    # ADDING DATA TO SMDS DB
    try: value_CITY_NAME = str(value_CITY_NAME) 
    except: value_CITY_NAME = None
    try: value_ALT_CITY_NAME = str(value_ALT_CITY_NAME) 
    except: value_ALT_CITY_NAME = None
    try: value_RW_DESC = str(value_RW_DESC) 
    except: value_RW_DESC = None
    try: value_LAT = str(value_LAT) 
    except: value_LAT = None
    try: value_LONG = str(value_LONG) 
    except: value_LONG = None

    ## NO DUPLICATE FOUND ##
    df_smds_data.loc[len(df_smds_data.index)] = ['GEO_ID TO ADD', 'RKST ADD', 'UNLOC CODE ADD',
                                                 value_CITY_NAME,
                                                 value_ALT_CITY_NAME,
                                                 value_RW_DESC,
                                                 value_LAT,
                                                 value_LONG,
                                                 value_COUNTRY_CODE]
    
    write_csv_data(df_smds_data)
    return {"Message" : "NOT DUPLICATE",
            "ValidationStatus" : "SUCCESS",
            "InputData" : city_input,
            "MatchData" : None}       
## NO DUPLICATE FOUND ##
#############################################################

# if __name__ == "__main__":
#     uvicorn.run(app, host="127.0.0.1", port=8000)
