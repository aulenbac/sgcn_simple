# coding: utf-8

from IPython.display import display
from datetime import datetime
from bis2 import dd
from bis import bis
from bis import itis
from bis import worms

bisDB = dd.getDB("bis")
sgcnSourceCollection = bisDB["SGCN Source Data"]
sgcnTIRProcessCollection = bisDB["SGCN TIR Process"]

pipeline = [
    {"$unwind":{"path":"$sourceData"}},
    {"$group":{"_id":None,"uniqueValues":{"$addToSet":"$sourceData.scientific name"}}}
]

uniqueNamesData = []
for record in sgcnSourceCollection.aggregate(pipeline):
    for uniqueName in record["uniqueValues"]:
        d_uniqueName = {}
        d_uniqueName["nameProcessingMetadata"] = {}
        d_uniqueName["nameProcessingMetadata"]["processingAlgorithmName"] = "bis.cleanScientificName"
        d_uniqueName["nameProcessingMetadata"]["processingAlgorithmURI"] = "https://github.com/usgs-bcb/bis/blob/master/bis/bis.py"
        d_uniqueName["nameProcessingMetadata"]["creationDate"] = datetime.utcnow().isoformat()
        d_uniqueName["nameProcessingMetadata"]["sourceCollection"] = "SGCN Source Data"
        d_uniqueName["nameProcessingMetadata"]["sourcePipeline"] = str(pipeline)
        d_uniqueName["ScientificName_original"] = uniqueName
        d_uniqueName["ScientificName_clean"] = bis.cleanScientificName(uniqueName)
        d_uniqueName["itis"] = {}
        d_uniqueName["itis"]["registration"] = {}
        d_uniqueName["itis"]["registration"]["url_ExactMatch"] = itis.getITISSearchURL(d_uniqueName["ScientificName_clean"],False,False)
        d_uniqueName["itis"]["registration"]["url_FuzzyMatch"] = itis.getITISSearchURL(d_uniqueName["ScientificName_clean"],True,False)
        d_uniqueName["worms"] = {}
        d_uniqueName["worms"]["registration"] = {}
        d_uniqueName["worms"]["registration"]["url_ExactMatch"] = worms.getWoRMSSearchURL("ExactName",d_uniqueName["ScientificName_clean"])
        d_uniqueName["worms"]["registration"]["url_FuzzyMatch"] = worms.getWoRMSSearchURL("FuzzyName",d_uniqueName["ScientificName_clean"])
        uniqueNamesData.append(d_uniqueName)

sgcnTIRProcessCollection.insert_many(uniqueNamesData)