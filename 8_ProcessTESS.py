# coding: utf-8

import requests
from datetime import datetime
from bis import tess
from bis2 import dd
from IPython.display import display

bisDB = dd.getDB("bis")
uniqueNamesCollection = bisDB["UniqueNames"]

count = 0
tessRegistration = 1
while tessRegistration is not None:
    tessRegistration = uniqueNamesCollection.find_one({"tess.processingMetadata":{"$exists":False}},{"_id":1,"tess.registration":1})
    
    if tessRegistration is not None:
        processingMetadata = {}
        processingMetadata["matchMethod"] = "Not Matched"
        processingMetadata["dateProcessed"] = datetime.utcnow().isoformat()
    
        _doName = True
        if "url_tsn" in tessRegistration["tess"]["registration"].keys():
            tessData = tess.tessQuery(tessRegistration["tess"]["registration"]["url_tsn"])
            if tessData["result"]:
                processingMetadata["matchMethod"] = "TSN Match"
                _doName = False
        else:
            _doName = True
            
        if _doName:
            tessData = tess.tessQuery(tessRegistration["tess"]["registration"]["url_name"])
            if tessData["result"]:
                processingMetadata["matchMethod"] = "SCINAME Match"

        uniqueNamesCollection.update_one({"_id":tessRegistration["_id"]},{"$set":{"tess.processingMetadata":processingMetadata,"tess.tessData":tessData}})
        count = count + 1
        print (count)