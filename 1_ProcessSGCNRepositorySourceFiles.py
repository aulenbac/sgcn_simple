# coding: utf-8

from IPython.display import display
import requests,io,csv
from datetime import datetime
from bis2 import dd

bisDB = dd.getDB("bis")
sgcnCollection = bisDB["SGCN Source Data"]

swapCollection = "https://www.sciencebase.gov/catalog/items?parentId=56d720ece4b015c306f442d5&format=json&fields=files,tags,dates&max=1000"
sbR = requests.get(swapCollection).json()

for item in sbR["items"]:
    sourceItem = {}
    sourceItem["processingMetadata"] = {}
    sourceItem["processingMetadata"]["sourceID"] = item["link"]["url"]
    sourceItem["processingMetadata"]["sgcn_year"] = next((d for d in item["dates"] if d["type"] == "Collected"), None)["dateString"]
    sourceItem["processingMetadata"]["sgcn_state"] = next((t for t in item["tags"] if t["type"] == "Place"), None)["name"]
    processFile = next((f for f in item["files"] if f["title"] == "Process File"), None)
    sourceItem["processingMetadata"]["processFileURL"] = processFile["url"]
    sourceItem["processingMetadata"]["processFileName"] = processFile["name"]
    sourceItem["processingMetadata"]["processFileUploadDate"] = datetime.strptime(processFile["dateUploaded"].split("T")[0], "%Y-%m-%d")
    sourceItem["processingMetadata"]["dateProcessed"] = datetime.utcnow().isoformat()
    
    currentRecord = sgcnCollection.find_one({"$and":[{"processingMetadata.sgcn_state":sourceItem["processingMetadata"]["sgcn_state"]},{"processingMetadata.sgcn_year":sourceItem["processingMetadata"]["sgcn_year"]}]})
    
    if currentRecord is None or len(currentRecord["sourceData"]) == 0:
        sourceItem["sourceData"] = []
        sourceFileContents = requests.get(sourceItem["processingMetadata"]["processFileURL"]).text
        inputData = list(csv.DictReader(io.StringIO(sourceFileContents), delimiter='\t'))
        for record in inputData:
            sourceItem["sourceData"].append({k.lower(): v for k, v in record.items()})
        sourceItem["processingMetadata"]["sourceRecordCount"] = len(sourceItem["sourceData"])
        
        if len(sourceItem["sourceData"]) > 0 and currentRecord is None:
            sgcnCollection.insert_one(sourceItem)
        elif len(sourceItem["sourceData"]) > 0 and currentRecord is not None:
            sgcnCollection.update_one({"_id":currentRecord["_id"]},{"$set":{"sourceData":sourceItem["sourceData"]}})
        
