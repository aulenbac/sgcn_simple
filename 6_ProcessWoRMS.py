# coding: utf-8

import requests
from datetime import datetime
from bis import worms
from bis2 import dd
from IPython.display import display

bisDB = dd.getDB("bis")
uniqueNamesCollection = bisDB["UniqueNames"]

count = 0
wormsRegistration = 1
while wormsRegistration is not None:
    wormsRegistration = uniqueNamesCollection.find_one({"worms.processingMetadata":{"$exists":False}},{"_id":1,"worms.registration":1})
    
    if wormsRegistration is not None:

        processingMetadata = {}
        processingMetadata["matchMethod"] = "Not Matched"
        processingMetadata["dateProcessed"] = datetime.utcnow().isoformat()
        wormsData = None
    
        try:
            wormsSearch = requests.get(wormsRegistration["worms"]["registration"]["url_ExactMatch"])
            if wormsSearch.status_code == 204 or wormsSearch.json()[0]["valid_name"] is None:
                wormsSearch = requests.get(wormsRegistration["worms"]["registration"]["url_FuzzyMatch"])
                if wormsSearch.status_code != 204 and wormsSearch.json()[0]["valid_name"] is not None:
                    processingMetadata["matchMethod"] = "Fuzzy Match"
                    wormsData = wormsSearch.json()[0]
            else:
                processingMetadata["matchMethod"] = "Exact Match"
                wormsData = wormsSearch.json()[0]
        
            if wormsData is not None and type(wormsData) != int and wormsData["status"] != "accepted":
                wormsSearch = requests.get(worms.getWoRMSSearchURL("AphiaID",str(wormsData["valid_AphiaID"])))
                if wormsSearch.status_code != 204 and wormsSearch.json()["valid_name"] is not None:
                    wormsData = wormsSearch.json()
                    processingMetadata["matchMethod"] = "Followed Accepted AphiaID"
        except:
            processingMetadata["matchMethod"] = "Hard Fail on Query"
    
        if wormsData is not None:
            uniqueNamesCollection.update_one({"_id":wormsRegistration["_id"]},{"$set":{"worms.processingMetadata":processingMetadata,"worms.wormsData":wormsData}})
        else:
            uniqueNamesCollection.update_one({"_id":wormsRegistration["_id"]},{"$set":{"worms.processingMetadata":processingMetadata}})
        count = count + 1
        print (count)
