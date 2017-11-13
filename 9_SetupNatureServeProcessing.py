# coding: utf-8

from bis2 import dd
from IPython.display import display

bisDB = dd.getDB("bis")
uniqueNamesCollection = bisDB["UniqueNames"]

count = 0
uniqueName = 1
while uniqueName is not None:
    # This query gets a single record where NatureServe processing has not been set up and both ITIS and WoRMS processing have been done
    uniqueName = uniqueNamesCollection.find_one({"$and":[{"NatureServe":{"$exists":False}},{"itis.processingMetadata":{"$exists":True}},{"worms.processingMetadata":{"$exists":True}}]},{"_id":1,"ScientificName_unique":1,"itis":1,"worms":1})

    if uniqueName is not None:
        natureServeDoc = {}
        natureServeDoc["registration"] = {}
        uniqueNamesSoFar = []

        # In our NatureServe process we improve the chances of finding a name match by incuding ITIS and WoRMS names if they are different from the one supplied in TIR registration
        if "itisData" in uniqueName["itis"].keys():
            validITISDoc = next((d for d in uniqueName["itis"]["itisData"] if d["usage"] in ["valid","accepted"]), None)
            if validITISDoc["nameWInd"] not in [d["scientificname"] for d in uniqueNamesSoFar if "scientificname" in d]:
                uniqueNamesSoFar.append({"scientificname":validITISDoc["nameWInd"],"source":"ITIS"})
    
        if "wormsData" in uniqueName["worms"].keys():
            if uniqueName["worms"]["wormsData"]["scientificname"] not in [d["scientificname"] for d in uniqueNamesSoFar if "scientificname" in d]:
                uniqueNamesSoFar.append({"scientificname":uniqueName["worms"]["wormsData"]["scientificname"],"source":"WoRMS"})
    
        if uniqueName["ScientificName_unique"] not in [d["scientificname"] for d in uniqueNamesSoFar if "scientificname" in d]:
            uniqueNamesSoFar.append({"scientificname":uniqueName["ScientificName_unique"],"source":"SGCN"})
            
        natureServeDoc["registration"]["uniqueNames"] = uniqueNamesSoFar

        uniqueNamesCollection.update_one({"_id":uniqueName["_id"]},{"$set":{"NatureServe":natureServeDoc}})
        count = count + 1
        print (count)

