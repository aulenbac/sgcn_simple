# coding: utf-8

from bis import natureserve
from bis2 import dd
from IPython.display import display
from datetime import datetime

bisDB = dd.getDB("bis")
sgcnTIRProcessCollection = bisDB["SGCN TIR Process"]

count = 0
natureServeRegistration = 1
while natureServeRegistration is not None:
    natureServeRegistration = sgcnTIRProcessCollection.find_one({"$and":[{"NatureServe.registration":{"$exists":True}},{"NatureServe.processingMetadata":{"$exists":False}}]},{"_id":1,"NatureServe.registration":1})
    
    if natureServeRegistration is not None:
        processingMetadata = {}
        processingMetadata["matchMethod"] = "Not Matched"
        processingMetadata["dateProcessed_search"] = datetime.utcnow().isoformat()
        
        speciesSearchResults = {}
        for index,uniqueName in enumerate(natureServeRegistration["NatureServe"]["registration"]["uniqueNames"]):
            speciesSearchResults["elementGlobalID"] = natureserve.queryNatureServeID(uniqueName["scientificname"])
            speciesSearchResults["matchSource"] = uniqueName["source"]
            speciesSearchResults["matchName"] = uniqueName["scientificname"]
            if speciesSearchResults["elementGlobalID"] is None and index > len(natureServeRegistration["NatureServe"]["registration"]["uniqueNames"]):
                continue
            else:
                break
            
        if speciesSearchResults["elementGlobalID"] is not None:
            processingMetadata["elementGlobalID"] = speciesSearchResults["elementGlobalID"]
            processingMetadata["matchMethod"] = "Name Match"
            processingMetadata["matchSource"] = speciesSearchResults["matchSource"]
            processingMetadata["matchName"] = speciesSearchResults["matchName"]
        display (processingMetadata)    
        sgcnTIRProcessCollection.update_one({"_id":natureServeRegistration["_id"]},{"$set":{"NatureServe.processingMetadata":processingMetadata}})
        count = count + 1
        print (count)


    