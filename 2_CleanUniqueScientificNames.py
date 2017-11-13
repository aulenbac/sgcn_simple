
# coding: utf-8

from bis import bis
from bis2 import dd
from datetime import datetime

bisDB = dd.getDB("bis")
sgcnSpeciesCollection = bisDB["SGCN"]
sgcnUniqueNamesCollection = bisDB["UniqueNames"]

# Loop through all unique species from the SGCN source
for sgcnDistinctSpecies in sgcnSpeciesCollection.find({},{"Scientific Name":1}).distinct("Scientific Name"):
    # Set up the record to include process metadata referring to the algorithm used to produce a clean name
    thisRecord = {}
    thisRecord["processMetadata"] = {}
    thisRecord["processMetadata"]["originalSourceCollection"] = "SGCN"
    thisRecord["processMetadata"]["dateProcessed"] = datetime.utcnow().isoformat()
    thisRecord["processMetadata"]["processingAlgorithmName"] = "bis.cleanScientificName"
    thisRecord["processMetadata"]["processingAlgorithmURI"] = "https://github.com/usgs-bcb/bis/blob/master/bis/bis.py"
    thisRecord["ScientificName_original"] = sgcnDistinctSpecies
    thisRecord["ScientificName_unique"] = bis.cleanScientificName(sgcnDistinctSpecies)

    # Check to see if the unique name string already exists in the datanbase; if not, go ahead and insert the new record and update the SGCN source with the ID for all relevant original names
    if sgcnUniqueNamesCollection.find({"ScientificName_unique":thisRecord["ScientificName_unique"]}).count() == 0:
        sgcnSpeciesCollection.update_many({"Scientific Name":thisRecord["ScientificName_original"]},{"$set":{"references":{"collection":"UniqueNames","_id":sgcnUniqueNamesCollection.insert_one(thisRecord).inserted_id}}})
        print (thisRecord)
