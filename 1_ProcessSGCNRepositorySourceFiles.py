
# coding: utf-8

import requests,io,json,csv
from IPython.display import display
from datetime import datetime
from bis import bis
from bis2 import dd

bisDB = dd.getDB("bis")
sgcnCollection = bisDB["SGCN"]

thisRun = {}
thisRun["itemsToProcess"] = 0
thisRun["numberItemsProcessed"] = 0
thisRun["numberRecordsInserted"] = 0
thisRun["totalRecordsInFiles"] = 0
thisRun["commitItems"] = True
thisRun["processingLog"] = []
thisRun["items"] = []

# Build out local data structure from ScienceBase search results
# ScienceBase only allows returning 100 records at a time, so we need to loop the search results until finished
_urlToProcess = "https://www.sciencebase.gov/catalog/items?parentId=56d720ece4b015c306f442d5&format=json&fields=files,tags,dates&max=100"
while _urlToProcess is not None:
    sbR = requests.get(_urlToProcess).json()
    for item in sbR["items"]:
        thisRun["items"].append(item)
    if "nextlink" in sbR.keys():
        _urlToProcess = sbR["nextlink"]["url"]
    else:
        _urlToProcess = None

# Loop the items containing data for each state and year
for item in thisRun["items"]:
    if thisRun["itemsToProcess"] != 0 and thisRun["numberItemsProcessed"] == thisRun["itemsToProcess"]:
        break

    processMetadata = {}
    sourceMetadata = {}
    tempData = {}

    # Store processing metadata for reference
    processMetadata["sourceid"] = item["link"]["url"]
    processMetadata["dateProcessed"] = datetime.utcnow().isoformat()

    # Extract state tag and year date from item metadata
    # If either the date or year are not in the item correctly, we can't process this item
    tempData["sgcn_year"] = next((d for d in item["dates"] if d["type"] == "Collected"), None)
    if tempData["sgcn_year"] is None:
        thisRun["processingLog"].append({"result":"Failure","sourceid":processMetadata["sourceid"],"reason":"No Collected year in item"})
        continue

    tempData["sgcn_state"] = next((t for t in item["tags"] if t["type"] == "Place"), None)
    if tempData["sgcn_state"] is None:
        thisRun["processingLog"].append({"result":"Failure","sourceid":processMetadata["sourceid"],"reason":"No Place tag in item"})
        continue
    
    # If we have a valid year and state, the proceed with setting up the data
    sourceMetadata["sgcn_year"] = tempData["sgcn_year"]["dateString"]
    sourceMetadata["sgcn_state"] = tempData["sgcn_state"]["name"]

    # Check the collection to see if we have any records for this state and year
    processMetadata["startingRecordCount"] = sgcnCollection.find({"sourceData.sgcn_state":sourceMetadata["sgcn_state"],"sourceData.sgcn_year":sourceMetadata["sgcn_year"]}).count()
    
    # Get the process file information out of the item
    tempData["processFile"] = next((f for f in item["files"] if f["title"] == "Process File"), None)

    # If there is no process file on the item, we cannot proceed with this item
    if tempData["processFile"] is None:
        thisRun["processingLog"].append({"result":"Failure","sourceid":processMetadata["sourceid"],"reason":"No Process File in item"})
        continue

    # If we have a file to process, then proceed with this item
    processMetadata["sourceFileName"] = tempData["processFile"]["name"]
    processMetadata["sourceFileURL"] = tempData["processFile"]["url"]
    processMetadata["processFileUploaded"] = datetime.strptime(tempData["processFile"]["dateUploaded"].split("T")[0], "%Y-%m-%d")

    # Get the Process File and build it into a dictionary
    # If we fail here because the file is not compliant, we continue to the next item
    try:
        sourceFile = requests.get(processMetadata["sourceFileURL"]).text
    except:
        thisRun["processingLog"].append({"result":"Failure","sourceid":processMetadata["sourceid"],"reason":"Process File failed to read"})
        continue
    
    try:
        sourceData = list(csv.DictReader(io.StringIO(sourceFile), delimiter='\t'))
    except:
        thisRun["processingLog"].append({"result":"Failure","sourceid":processMetadata["sourceid"],"reason":"Process File failed to load into a dictionary"})
        continue

    # Set the total number of records in the source file
    processMetadata["sourceRecordCount"] = len(sourceData)

    # Continue to the next item if the database already appears to have the data
    if processMetadata["sourceRecordCount"] == processMetadata["startingRecordCount"]:
        thisRun["processingLog"].append({"result":"Pass","sourceid":processMetadata["sourceid"],"reason":"Database already has the same number of records as the source file"})
        continue
    else:
        # If something went wrong in the data and there are some but not all of the records, flush a state/year before proceeding
        if processMetadata["startingRecordCount"] > 0:
            print (sgcnCollection.remove({"$and":[{"sgcn_state":sourceMetadata["sgcn_state"]},{"sgcn_year":sourceMetadata["sgcn_year"]}]}))

    # Add metadata to records        
    for record in sourceData:
        record.update({"sgcn_state":sourceMetadata["sgcn_state"]})
        record.update({"sgcn_year":sourceMetadata["sgcn_year"]})
        record.update({"processMetadata":processMetadata})

    sgcnCollection.insert_many(sourceData)
    print (processMetadata)

    thisRun["numberItemsProcessed"] = thisRun["numberItemsProcessed"] + 1
    thisRun["totalRecordsInFiles"] = thisRun["totalRecordsInFiles"] + processMetadata["sourceRecordCount"]
    thisRun["numberRecordsInserted"] = thisRun["numberRecordsInserted"] + sgcnCollection.find({"sourceData.sgcn_state":sourceMetadata["sgcn_state"],"sourceData.sgcn_year":sourceMetadata["sgcn_year"]}).count()

thisRun.pop("items",None)
display (thisRun)
