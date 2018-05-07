# coding: utf-8

from bis import natureserve
from bis2 import dd
from IPython.display import display
from datetime import datetime

bisDB = dd.getDB("bis")
sgcnTIRProcessCollection = bisDB["SGCN TIR Process"]

count = 0
lookupName = 1
while lookupName is not None:
#    if count > 5:
#        break

    lookupName = sgcnTIRProcessCollection.find_one({"$and":[{"NatureServe":{"$exists":False}},{"Scientific Name":{"$exists":True}}]},{"Scientific Name":1})

    if lookupName is not None:
        natureServePackage = {"processingMetadata":{}}
        natureServePackage["processingMetadata"]["searchName"] = lookupName["Scientific Name"]
        natureServePackage["processingMetadata"]["matchMethod"] = "Not Matched"
        natureServePackage["processingMetadata"]["dateProcessed_search"] = datetime.utcnow().isoformat()
        
        if  len(lookupName["Scientific Name"]) > 0:
            natureServeRecord = natureserve.query.query_natureserve(lookupName["Scientific Name"])
                
            if natureServeRecord is not None:
                natureServePackage["processingMetadata"]["matchMethod"] = "Name Match"
                natureServePackage["NatureServe Record"] = natureServeRecord

#        display (natureServePackage)
        sgcnTIRProcessCollection.update_many({"Scientific Name":lookupName["Scientific Name"]},{"$set":{"NatureServe":natureServePackage}})
        count = count + 1
        print (count)


    