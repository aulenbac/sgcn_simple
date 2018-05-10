# coding: utf-8

from bis import tess
from bis2 import dd
from IPython.display import display

bisDB = dd.getDB("bis")
sgcnTIRProcessCollection = bisDB["SGCN TIR Process"]

count = 0
uniqueName = {}
while uniqueName is not None:
    # This query gets a single record where TESS processing has not been set up and both ITIS and WoRMS processing have been done
    uniqueName = sgcnTIRProcessCollection.find_one({"$and":[{"tess":{"$exists":False}},{"itis.processingMetadata":{"$exists":True}}]},{"_id":1,"ScientificName_clean":1,"itis":1,"worms":1})
    
    if uniqueName is not None:
        tessDoc = {}
        tessDoc["registration"] = {}
        tessDoc["registration"]["url_name"] = tess.getTESSSearchURL("SCINAME",uniqueName["ScientificName_clean"])

        if "itisData" in uniqueName["itis"].keys():
            validITISDoc = next((d for d in uniqueName["itis"]["itisData"] if d["usage"] in ["valid","accepted"]), None)
        else:
            validITISDoc = None
        
        if any("status" in d for d in uniqueName["worms"]):
            validWoRMSDoc = next((d for d in uniqueName["worms"] if d["status"] == "accepted"), None)
        else:
            validWoRMSDoc = None
        
        if validITISDoc is not None:
            tessDoc["registration"]["url_tsn"] = tess.getTESSSearchURL("TSN",validITISDoc["tsn"])
            if uniqueName["ScientificName_clean"] != validITISDoc["nameWInd"]:
                tessDoc["registration"]["url_name"] = tess.getTESSSearchURL("SCINAME",validITISDoc["nameWInd"])
        elif validWoRMSDoc is not None:
            tessDoc["registration"]["url_name"] = tess.getTESSSearchURL("SCINAME",validWoRMSDoc["scientificname"])

        sgcnTIRProcessCollection.update_one({"_id":uniqueName["_id"]},{"$set":{"tess":tessDoc}})
        count = count + 1
        print (count)
