from bis2 import dd
from IPython.display import display
from datetime import datetime
import requests,json,re
from bis import itis
from bis import sgcn

_doManualMatch = True

sbSWAPItem = requests.get("https://www.sciencebase.gov/catalog/item/56d720ece4b015c306f442d5?format=json&fields=files").json()
for file in sbSWAPItem["files"]:
    if file["title"] == "Historic 2005 SWAP National List":
        _historicSWAPFilePath = file["url"]
        swap2005List = []
        for line in requests.get(file["url"], stream=True).iter_lines():
            if line: swap2005List.append(line.decode("utf-8"))
    elif file["title"] == "Taxonomic Group Mappings":
        sgcnTaxonomicGroupMappings = json.loads(requests.get(file["url"]).text)
    elif file["title"] == "SGCN ITIS Overrides" and _doManualMatch:
        itisManualOverrides = json.loads(requests.get(file["url"]).text)

wormsMatchTypeMapping = {"exact":"Exact Match","like":"Fuzzy Match"}

bisDB = dd.getDB("bis")
sgcnTIRProcessCollection = bisDB["SGCN TIR Process"]
sgcnSource = bisDB["SGCN Source Data"]

if _doManualMatch:
    for manualMatch in itisManualOverrides:
        itisResult = {}
        itisResult["processingMetadata"] = {}
        itisResult["processingMetadata"]["Date Processed"] = datetime.utcnow().isoformat()
        itisResult["processingMetadata"]["Summary Result"] = "Manual Match"
        url_tsnSearch = manualMatch["taxonomicAuthorityID"]+"&wt=json"
        itisResult["processingMetadata"]["Detailed Results"] = [{"Manual Match":url_tsnSearch}]
        r_tsnSearch = requests.get(url_tsnSearch).json()
        itisResult["itisData"] = [itis.packageITISJSON(r_tsnSearch["response"]["docs"][0])]
        sgcnTIRProcessCollection.update_one({"ScientificName_original":re.compile(re.escape(manualMatch["ScientificName_original"]))},{"$set":{"itis":itisResult}})
        sgcnTIRProcessCollection.update_one({"ScientificName_original":re.compile(re.escape(manualMatch["ScientificName_original"]))},{"$unset":{"Scientific Name":1}})

count = 0
sgcnDecoration = {}
while sgcnDecoration is not None:
    sgcnDecoration = sgcnTIRProcessCollection.find_one({"Scientific Name":{"$exists":False}})

    if sgcnDecoration is not None:    
        sgcnDoc = {}
        if "itisData" in sgcnDecoration["itis"].keys():
            acceptedITISDoc = next((d for d in sgcnDecoration["itis"]["itisData"] if d["usage"] in ["accepted","valid"]), None)
            sgcnDoc["Scientific Name"] = acceptedITISDoc["nameWInd"]
    
            if "commonnames" in acceptedITISDoc.keys():
                cnItem = next((cn for cn in acceptedITISDoc["commonnames"] if cn["language"] == "English"), None)
                if cnItem is not None:
                    sgcnDoc["Common Name"] = cnItem["name"]
    
            sgcnDoc["Match Method"] = sgcnDecoration["itis"]["processingMetadata"]["Summary Result"]
            
            sgcnDoc["Taxonomic Authority ID"] = "https://services.itis.gov/?q=tsn:"+str(acceptedITISDoc["tsn"])
            sgcnDoc["Taxonomic Rank"] = acceptedITISDoc["rank"]
            sgcnDoc["Taxonomy"] = acceptedITISDoc["taxonomy"]
            
        else:
            if "worms" in sgcnDecoration.keys():
                acceptedWoRMS = next((doc for doc in sgcnDecoration["worms"] if doc["status"] == "accepted"),None)
                if acceptedWoRMS is not None:
                    sgcnDoc["Scientific Name"] = acceptedWoRMS["scientificname"]
                    sgcnDoc["Taxonomic Authority ID"] = "http://www.marinespecies.org/rest/AphiaRecordByAphiaID/"+str(acceptedWoRMS["AphiaID"])
                    sgcnDoc["Taxonomic Rank"] = acceptedWoRMS["rank"]
                    sgcnDoc["Taxonomy"] = acceptedWoRMS["taxonomy"]
                    sgcnDoc["Match Method"] = wormsMatchTypeMapping[acceptedWoRMS["match_type"]]

        if "Scientific Name" not in sgcnDoc.keys():
            sgcnDoc["Scientific Name"] = sgcnDecoration["ScientificName_original"]
            sgcnDoc["Match Method"] = "Not Matched"
            sgcnDoc["Taxonomic Authority ID"] = None
            sgcnDoc["Taxonomic Rank"] = "Undetermined"
            sgcnDoc["Taxonomy"] = None
            sgcnDoc["Taxonomic Group"] = "Other"

            if sgcnDecoration["ScientificName_clean"] in swap2005List or sgcnDecoration["ScientificName_original"] in swap2005List:
                sgcnDoc["Match Method"] = "Historic Match"
                sgcnDoc["Taxonomic Authority ID"] = _historicSWAPFilePath

        if sgcnDoc["Taxonomy"] is not None:
            sgcnDoc["Taxonomic Group"] = sgcn.getTaxGroup(sgcnDoc["Taxonomy"],sgcnTaxonomicGroupMappings)

        pipeline = [
            {"$unwind":{"path":"$sourceData"}},
            {"$match":{"sourceData.scientific name":sgcnDecoration["ScientificName_original"]}},
            {"$group":{"_id":"$processingMetadata.sgcn_year","states":{"$addToSet":"$processingMetadata.sgcn_state"},"Common Names":{"$addToSet":"$sourceData.common name"}}},
            {"$sort":{"_id":-1}}
        ]
    
        sgcnDoc["Source Data Summary"] = []
        for record in sgcnSource.aggregate(pipeline):
            summaryDoc = {}
            summaryDoc[record["_id"]] = {}
            summaryDoc[record["_id"]]["States"] = record["states"]
            summaryDoc[record["_id"]]["States"].sort()
            summaryDoc[record["_id"]]["Common Names"] = record["Common Names"]
            summaryDoc[record["_id"]]["Common Names"].sort()
            sgcnDoc["Source Data Summary"].append(summaryDoc)
    
        if "Common Name" not in sgcnDoc.keys():
            sgcnDoc["Common Name"] = next(iter(sgcnDoc["Source Data Summary"][0].values()))["Common Names"][0]
                
        sgcnTIRProcessCollection.update_one({"_id":sgcnDecoration["_id"]},{"$set":sgcnDoc})
        count = count + 1
        print (count)
