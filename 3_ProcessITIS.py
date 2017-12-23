# coding: utf-8

import requests
from datetime import datetime
from bis import itis
from bis2 import dd
from IPython.display import display

bisDB = dd.getDB("bis")
sgcnTIRProcessCollection = bisDB["SGCN TIR Process"]

count = 0
processRecord = 1
while processRecord is not None:
    processRecord = sgcnTIRProcessCollection.find_one({"itis":{"$exists":False}},{"ScientificName_clean":1})
    
    if processRecord is not None:
        itisResult = itis.checkITISSolr(processRecord["ScientificName_clean"])
        if itisResult is not None:
            sgcnTIRProcessCollection.update_one({"_id":processRecord["_id"]},{"$set":{"itis":itisResult}})
            count = count + 1
    print (count)
    
