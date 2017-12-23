#!/usr/bin/env python3

from bis import natureserve
from bis2 import natureserve as ns
from bis2 import dd
from IPython.display import display
from datetime import datetime

bisDB = dd.getDB("bis")
sgcnTIRProcessCollection = bisDB["SGCN TIR Process"]

count = 0
natureServeRegistration = 1
while natureServeRegistration is not None:
    natureServeRegistration = sgcnTIRProcessCollection.find_one({"$and":[{"NatureServe.processingMetadata.elementGlobalID":{"$exists":True}},{"NatureServe.processingMetadata.dateProcessed_retrieved":{"$exists":False}}]},{"_id":1,"NatureServe.processingMetadata":1})
    
    if natureServeRegistration is not None:
        processingMetadata = natureServeRegistration["NatureServe"]["processingMetadata"]
        processingMetadata["dateProcessed_retrieved"] = datetime.utcnow().isoformat()
        natureServeData = natureserve.packageNatureServeJSON(ns.speciesAPI(),processingMetadata["elementGlobalID"])
        processingMetadata["result"] = natureServeData["result"]

        if natureServeData["result"]:
            sgcnTIRProcessCollection.update_one({"_id":natureServeRegistration["_id"]},{"$set":{"NatureServe.processingMetadata":processingMetadata,"NatureServe.natureServeData":natureServeData}})
        else:
            sgcnTIRProcessCollection.update_one({"_id":natureServeRegistration["_id"]},{"$set":{"NatureServe.processingMetadata":processingMetadata}})
        count = count + 1
        print (count)