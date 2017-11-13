# coding: utf-8

from bis import natureserve
from bis2 import natureserve as ns
from bis2 import dd
from IPython.display import display
from datetime import datetime

bisDB = dd.getDB("bis")
uniqueNamesCollection = bisDB["UniqueNames"]

count = 0
natureServeRegistration = 1
while natureServeRegistration is not None:
    natureServeRegistration = uniqueNamesCollection.find_one({"$and":[{"NatureServe.processingMetadata.elementGlobalID":{"$exists":True}},{"NatureServe.processingMetadata.dateProcessed_retrieved":{"$exists":False}}]},{"_id":1,"NatureServe.processingMetadata":1})
    
    if natureServeRegistration is not None:
        processingMetadata = natureServeRegistration["NatureServe"]["processingMetadata"]
        processingMetadata["dateProcessed_retrieved"] = datetime.utcnow().isoformat()
        natureServeData = natureserve.packageNatureServeJSON(ns.speciesAPI(),processingMetadata["elementGlobalID"])
        processingMetadata["result"] = natureServeData["result"]

        uniqueNamesCollection.update_one({"_id":natureServeRegistration["_id"]},{"$set":{"NatureServe.processingMetadata":processingMetadata,"NatureServe.natureServeData":natureServeData}})
        count = count + 1
        print (count)