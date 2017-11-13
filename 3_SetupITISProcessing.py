# coding: utf-8

from bis import itis
from bis2 import dd
from IPython.display import display

bisDB = dd.getDB("bis")
uniqueNamesCollection = bisDB["UniqueNames"]

count = 0
uniqueName = 1
while uniqueName is not None:
    uniqueName = uniqueNamesCollection.find_one({"itis":{"$exists":False}},{"_id":1,"ScientificName_unique":1})
    if uniqueName is not None:
        itisDoc = {}
        itisDoc["registration"] = {}
        itisDoc["registration"]["url_ExactMatch"] = itis.getITISSearchURL(uniqueName["ScientificName_unique"],False,False)
        itisDoc["registration"]["url_FuzzyMatch"] = itis.getITISSearchURL(uniqueName["ScientificName_unique"],True,False)
        uniqueNamesCollection.update_one({"_id":uniqueName["_id"]},{"$set":{"itis":itisDoc}})
        count = count + 1
        print (count)

