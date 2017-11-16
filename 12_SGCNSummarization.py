# coding: utf-8

from bis2 import dd
from IPython.display import display
from datetime import datetime
import requests,csv,io

taxGroupMappings = {}
for record in list(csv.DictReader(io.StringIO(requests.get("https://www.sciencebase.gov/catalog/file/get/56d720ece4b015c306f442d5?f=__disk__fd%2Feb%2F41%2Ffdeb4194e44ce4754adafecd795faca93d40d5c8").text))):
    taxGroupMappings[record["ProvidedName"]] = record["PreferredName"]

AuthorityFile_2005 = "https://www.sciencebase.gov/catalog/file/get/56d720ece4b015c306f442d5?f=__disk__38%2F22%2F26%2F38222632f48bf0c893ad1017f6ba557d0f672432"
AuthorityURL_2005 = "https://www.sciencebase.gov/catalog/item/56d720ece4b015c306f442d5"
speciesList_2005 = []
for record in list(csv.DictReader(io.StringIO(requests.get(AuthorityFile_2005).text), delimiter="\t")):
    if record["scientificname"] not in speciesList_2005:
        speciesList_2005.append(record["scientificname"])

bisDB = dd.getDB("bis")
uniqueNamesCollection = bisDB["UniqueNames"]
sgcnCollection = bisDB["SGCN"]

count = 0
sgcnSummary = 1
while sgcnSummary is not None:
    sgcnSummary = uniqueNamesCollection.find_one({"SGCN Summary":{"$exists":False}})
    
    if sgcnSummary is not None:
        sgcn = {}

        if "itisData" in sgcnSummary["itis"].keys():
            sgcn["Match Method"] = sgcnSummary["itis"]["processingMetadata"]["matchMethod"]
            validITISDoc = next((i for i in sgcnSummary["itis"]["itisData"] if i["usage"] in ["valid","accepted"]), None)
            sgcn["Scientific Name"] = validITISDoc["nameWInd"]
            sgcn["Taxonomic Rank"] = validITISDoc["rank"]
            sgcn["Taxonomic Authority ID"] = "http://services.itis.gov/?q=tsn:"+str(validITISDoc["tsn"])
            sgcn["Taxonomic Authority URL"] = "https://www.itis.gov/servlet/SingleRpt/SingleRpt?search_topic=TSN&search_value="+str(validITISDoc["tsn"])
            if "commonnames" in validITISDoc.keys():
                commonNameDict = next((c for c in validITISDoc["commonnames"] if c["language"] == "English"), None)
                if commonNameDict is not None:
                    sgcn["Common Name"] = commonNameDict["name"]
        else:
            if "wormsData" in sgcnSummary["worms"].keys():
                sgcn["Match Method"] = sgcnSummary["worms"]["processingMetadata"]["matchMethod"]
                sgcn["Scientific Name"] = sgcnSummary["worms"]["wormsData"]["valid_name"]
                sgcn["Taxonomic Rank"] = sgcnSummary["worms"]["wormsData"]["rank"]
                sgcn["Taxonomic Authority ID"] = sgcnSummary["worms"]["wormsData"]["lsid"]
                sgcn["Taxonomic Authority URL"] = sgcnSummary["worms"]["wormsData"]["url"]
            else:
                sgcn["Scientific Name"] = sgcnSummary["ScientificName_original"]
                sgcn["Taxonomic Rank"] = "Unknown Taxonomic Rank"
                if sgcnSummary["ScientificName_original"] in speciesList_2005 or sgcnSummary["ScientificName_unique"] in speciesList_2005:
                    sgcn["Match Method"] = "Legacy Match"
                    sgcn["Taxonomic Authority ID"] = AuthorityFile_2005
                    sgcn["Taxonomic Authority URL"] = AuthorityURL_2005
                else:
                    sgcn["Match Method"] = "Not Matched"
                    sgcn["Taxonomic Authority ID"] = "None"
                    sgcn["Taxonomic Authority URL"] ="None"
                
        sgcn["SGCN Species Records"] = []
        sgcn["State List 2005"] = []
        sgcn["State List 2015"] = []
        sgcn["Scientific Name List 2005"] = []
        sgcn["Common Name List 2005"] = []
        sgcn["Taxonomic Group List 2005"] = []
        sgcn["Scientific Name List 2015"] = []
        sgcn["Common Name List 2015"] = []
        sgcn["Taxonomic Group List 2015"] = []

        for sgcnDoc in sgcnCollection.find({"Scientific Name":sgcnSummary["ScientificName_original"]}):
            del sgcnDoc["processMetadata"]
            sgcn["SGCN Species Records"].append(sgcnDoc)

            if sgcnDoc["sgcn_year"] == "2005":
                if sgcnDoc["Common Name"].strip() not in sgcn["Common Name List 2005"]:
                    sgcn["Common Name List 2005"].append(sgcnDoc["Common Name"].strip())
                if sgcnDoc["Scientific Name"] not in sgcn["Scientific Name List 2005"]:
                    sgcn["Scientific Name List 2005"].append(sgcnDoc["Scientific Name"])
                if sgcnDoc["sgcn_state"] not in sgcn["State List 2005"]:
                    sgcn["State List 2005"].append(sgcnDoc["sgcn_state"])
                if sgcnDoc["Taxonomic Category"] not in sgcn["Taxonomic Group List 2005"]:
                    sgcn["Taxonomic Group List 2005"].append(sgcnDoc["Taxonomic Category"])
            elif sgcnDoc["sgcn_year"] == "2015": 
                if sgcnDoc["Common Name"].strip() not in sgcn["Common Name List 2015"]:
                    sgcn["Common Name List 2015"].append(sgcnDoc["Common Name"].strip())
                if sgcnDoc["Scientific Name"] not in sgcn["Scientific Name List 2015"]:
                    sgcn["Scientific Name List 2015"].append(sgcnDoc["Scientific Name"])
                if sgcnDoc["sgcn_state"] not in sgcn["State List 2015"]:
                    sgcn["State List 2015"].append(sgcnDoc["sgcn_state"])
                if "Taxonomy Group" in sgcnDoc.keys():
                    TaxGroup2015 = sgcnDoc["Taxonomy Group"]
                elif "Taxonomy Group (Use drop down box)" in sgcnDoc.keys():
                    TaxGroup2015 = sgcnDoc["Taxonomy Group (Use drop down box)"]
                if TaxGroup2015 not in sgcn["Taxonomic Group List 2015"]:
                    sgcn["Taxonomic Group List 2015"].append(TaxGroup2015)
                
        sgcn["State List 2005"] = sorted(sgcn["State List 2005"])
        sgcn["State List 2015"] = sorted(sgcn["State List 2015"])
        sgcn["Scientific Name List 2005"] = sorted(sgcn["Scientific Name List 2015"])
        sgcn["Common Name List 2005"] = sorted(sgcn["Common Name List 2015"])
        sgcn["Scientific Name List 2015"] = sorted(sgcn["Scientific Name List 2015"])
        sgcn["Common Name List 2015"] = sorted(sgcn["Common Name List 2015"])

        if "Common Name" not in sgcn.keys() and len(sgcn["Common Name List 2015"]) > 0:
            sgcn["Common Name"] = sgcn["Common Name List 2015"][0]
        elif "Common Name" not in sgcn.keys() and len(sgcn["Common Name List 2005"]) > 0:
            sgcn["Common Name"] = sgcn["Common Name List 2005"][0]

        sgcn["Taxonomic Group"] = None
        while sgcn["Taxonomic Group"] is None:
            for taxGroup in sgcn["Taxonomic Group List 2015"]+sgcn["Taxonomic Group List 2005"]:
                if taxGroup in taxGroupMappings.values():
                    sgcn["Taxonomic Group"] = taxGroup
                    break
                elif taxGroup in taxGroupMappings.keys():
                    sgcn["Taxonomic Group"] = taxGroupMappings[taxGroup]
                    break
            if sgcn["Taxonomic Group"] is None:
                sgcn["Taxonomic Group"] = "Other"

        uniqueNamesCollection.update_one({"_id":sgcnSummary["_id"]},{"$set":{"SGCN Summary":sgcn}})
        count = count + 1
        print (count)
        