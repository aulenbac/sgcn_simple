# coding: utf-8

import requests
from datetime import datetime
from bis import itis
from bis2 import dd
from IPython.display import display

bisDB = dd.getDB("bis")
uniqueNamesCollection = bisDB["UniqueNames"]

count = 0
itisRegistration = 1
while itisRegistration is not None:
    itisRegistration = uniqueNamesCollection.find_one({"itis.processingMetadata":{"$exists":False}},{"_id":1,"itis.registration":1})
    
    if itisRegistration is not None:
    
        processingMetadata = {}
        processingMetadata["dateProcessed"] = datetime.utcnow().isoformat()
        
        try:
            newITISDocs = []
            itisResults = requests.get(itisRegistration["itis"]["registration"]["url_ExactMatch"]).json()
            if itisResults["response"]["numFound"] == 1:
                itisData = itis.packageITISJSON(itisResults["response"]["docs"][0])
                itisData["q"] = itisResults["responseHeader"]["params"]["q"]
                newITISDocs.append(itisData)
                processingMetadata["matchMethod"] = "Exact Match"
            elif itisResults["response"]["numFound"] == 0:
                itisResults = requests.get(itisRegistration["itis"]["registration"]["url_FuzzyMatch"]).json()
                if itisResults["response"]["numFound"] == 1:
                    itisData = itis.packageITISJSON(itisResults["response"]["docs"][0])
                    itisData["q"] = itisResults["responseHeader"]["params"]["q"]
                    newITISDocs.append(itisData)
                    processingMetadata["matchMethod"] = "Fuzzy Match"
        
            if len(newITISDocs) > 0 and newITISDocs[0]["usage"] in ["invalid","not accepted"]:
                itisResults = requests.get(itis.getITISSearchURL(newITISDocs[0]["acceptedTSN"][0],False,True)).json()
                if itisResults["response"]["numFound"] == 1:
                    itisData = itis.packageITISJSON(itisResults["response"]["docs"][0])
                    itisData["q"] = itisResults["responseHeader"]["params"]["q"]
                    newITISDocs.append(itisData)
                    processingMetadata["matchMethod"] = "Followed Accepted TSN"
        
            if len(newITISDocs) == 0:
                processingMetadata["matchMethod"] = "Not Matched"
                _itisData = None
            else:
                _itisData = newITISDocs
                
        except:
            _itisData = None
            processingMetadata["matchMethod"] = "Hard Fail Query"
            
        if _itisData is not None:
            uniqueNamesCollection.update_one({"_id":itisRegistration["_id"]},{"$set":{"itis.processingMetadata":processingMetadata,"itis.itisData":_itisData}})
        else:
            uniqueNamesCollection.update_one({"_id":itisRegistration["_id"]},{"$set":{"itis.processingMetadata":processingMetadata}})
        count = count + 1
        print (count)
    
    