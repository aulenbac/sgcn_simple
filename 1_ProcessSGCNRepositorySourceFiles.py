# coding: utf-8

from IPython.display import display
import requests,io,csv
from datetime import datetime
from bis2 import dd
from bis import sgcn

bisDB = dd.getDB("bis")
sgcnCollection = bisDB["SGCN Source Data"]

swapCollection = "https://www.sciencebase.gov/catalog/items?parentId=56d720ece4b015c306f442d5&format=json&fields=files,tags,dates&max=1000"
sbR = requests.get(swapCollection).json()

for item in sbR["items"]:
    sourceItem = sgcn.sgcn_source_item_metadata(item)
    
    if sourceItem is None:
        continue

    currentRecord = sgcnCollection.find_one({"processingMetadata.processFileURL":sourceItem["processingMetadata"]["processFileURL"]})

    if currentRecord is None:
        sourceItemWithData = sgcn.process_sgcn_source_file(sourceItem)
        
        if len(sourceItemWithData["sourceData"]) > 0:
            sgcnCollection.insert_one(sourceItemWithData)
            print (sourceItemWithData["processingMetadata"]["sgcn_state"], sourceItemWithData["processingMetadata"]["sgcn_year"], sourceItemWithData["processingMetadata"]["sourceID"])

