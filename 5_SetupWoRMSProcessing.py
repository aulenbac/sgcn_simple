# coding: utf-8

from bis import worms
from bis2 import dd
from IPython.display import display

bisDB = dd.getDB("bis")
uniqueNamesCollection = bisDB["UniqueNames"]

count = 0
uniqueName = 1
while uniqueName is not None:
    uniqueName = uniqueNamesCollection.find_one({"worms":{"$exists":False}},{"_id":1,"ScientificName_unique":1})
    
    if uniqueName is not None:
        wormsDoc = {}
        wormsDoc["registration"] = {}
        wormsDoc["registration"]["url_ExactMatch"] = worms.getWoRMSSearchURL("ExactName",uniqueName["ScientificName_unique"])
        wormsDoc["registration"]["url_FuzzyMatch"] = worms.getWoRMSSearchURL("FuzzyName",uniqueName["ScientificName_unique"])
        uniqueNamesCollection.update_one({"_id":uniqueName["_id"]},{"$set":{"worms":wormsDoc}})
        count = count + 1
        print (count)
