# coding: utf-8

from bis import tess
from bis2 import dd
from IPython.display import display

bisDB = dd.getDB("bis")
uniqueNamesCollection = bisDB["UniqueNames"]

count = 0
uniqueName = 1
while uniqueName is not None:
    # This query gets a single record where TESS processing has not been set up and both ITIS and WoRMS processing have been done
    uniqueName = uniqueNamesCollection.find_one({"$and":[{"tess":{"$exists":False}},{"itis.processingMetadata":{"$exists":True}}]},{"_id":1,"ScientificName_unique":1,"itis":1,"worms":1})
    
    if uniqueName is not None:
        tessDoc = {}
        tessDoc["registration"] = {}
        tessDoc["registration"]["url_name"] = tess.getTESSSearchURL("SCINAME",uniqueName["ScientificName_unique"])
    
        # In our TESS process we prefer to use ITIS TSN where it exists and will improve the chances of finding a name match by using either ITIS or WoRMS names if they are different from the one supplied in TIR registration
        if "itisData" in uniqueName["itis"].keys():
            validITISDoc = next((d for d in uniqueName["itis"]["itisData"] if d["usage"] in ["valid","accepted"]), None)
            tessDoc["registration"]["url_tsn"] = tess.getTESSSearchURL("TSN",validITISDoc["tsn"])
            if uniqueName["ScientificName_unique"] != validITISDoc["nameWInd"]:
                tessDoc["registration"]["url_name"] = tess.getTESSSearchURL("SCINAME",validITISDoc["nameWInd"])
        else:
            if "worms" in uniqueName.keys():
                if "wormsData" in uniqueName["worms"].keys():
                    if uniqueName["ScientificName_unique"] != uniqueName["worms"]["wormsData"]["scientificname"]:
                        tessDoc["registration"]["url_name"] = tess.getTESSSearchURL("SCINAME",uniqueName["worms"]["wormsData"]["scientificname"])
    
        uniqueNamesCollection.update_one({"_id":uniqueName["_id"]},{"$set":{"tess":tessDoc}})
        count = count + 1
        print (count)
