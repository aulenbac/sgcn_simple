# coding: utf-8

from bis2 import dd
from IPython.display import display
from datetime import datetime
import requests,csv,io

bisDB = dd.getDB("bis")
uniqueNamesCollection = bisDB["UniqueNames"]

columns_common = ["cachedate","scientificname","taxonomicauthorityid","acceptedauthorityapi","acceptedauthorityurl","taxonomicrank","matchmethod","taxonomicgroup","commonname","fwslistingstatus","fwslistingdate"]
columns_national = columns_common+["statelist_2005","statelist_2015","scientificnames_submitted"]
columns_search = columns_national
columns_state = columns_common+["sgcn2005","sgcn2015","sgcn_state"]

uniqueNationalListSpecies = []
uniqueSearchListSpecies = []

cacheDate = datetime.utcnow().isoformat()

with open ("sgcn_nationallist.csv", "w") as national, open ("sgcn_search.csv", "w") as search, open("sgcn_statelists.csv", "w") as state:
    writer_national = csv.writer(national)
    writer_search = csv.writer(search)
    writer_state = csv.writer(state)
    
    writer_national.writerow(columns_national)
    writer_search.writerow(columns_search)
    writer_state.writerow(columns_state)
    
    for nameRecord in uniqueNamesCollection.find({}):
        row_common = [cacheDate,nameRecord["SGCN Summary"]["Scientific Name"],nameRecord["SGCN Summary"]["Taxonomic Authority ID"],nameRecord["SGCN Summary"]["Taxonomic Authority ID"],nameRecord["SGCN Summary"]["Taxonomic Authority URL"],nameRecord["SGCN Summary"]["Taxonomic Rank"],nameRecord["SGCN Summary"]["Match Method"],nameRecord["SGCN Summary"]["Taxonomic Group"],nameRecord["SGCN Summary"]["Common Name"],nameRecord["SGCN Summary"]["Listing Status"],nameRecord["SGCN Summary"]["Listing Date"]]
        row_national_search = row_common+[nameRecord["SGCN Summary"]["State List 2005"],nameRecord["SGCN Summary"]["State List 2015"],list(set(nameRecord["SGCN Summary"]["Scientific Name List 2005"]+nameRecord["SGCN Summary"]["Scientific Name List 2015"]))]

        if nameRecord["SGCN Summary"]["Scientific Name"] not in uniqueSearchListSpecies:
            uniqueSearchListSpecies.append(nameRecord["SGCN Summary"]["Scientific Name"])
            writer_search.writerow(row_national_search)

        if nameRecord["SGCN Summary"]["Match Method"] != "Not Matched" and nameRecord["SGCN Summary"]["Scientific Name"] not in uniqueNationalListSpecies:
            uniqueNationalListSpecies.append(nameRecord["SGCN Summary"]["Scientific Name"])
            writer_national.writerow(row_national_search)

        uniqueStateNames = []
        for stateRecord in nameRecord["SGCN Summary"]["SGCN Species Records"]:
            if not any(d['sgcn_state'] == stateRecord["sgcn_state"] for d in uniqueStateNames):
                if stateRecord["sgcn_year"] == "2005":
                    sgcn2005 = 1
                    sgcn2015 = 0
                elif stateRecord["sgcn_year"] == "2015":
                    sgcn2005 = 0
                    sgcn2015 = 1
                uniqueStateNames.append({"sgcn_state":stateRecord["sgcn_state"],"sgcn2005":sgcn2005,"sgcn2015":sgcn2015})
            else:
                for d in uniqueStateNames:
                    if d["sgcn_state"] == stateRecord["sgcn_state"] and stateRecord["sgcn_year"] == "2005":
                        d["sgcn2005"] = 1
                    elif d["sgcn_state"] == stateRecord["sgcn_state"] and stateRecord["sgcn_year"] == "2015":
                        d["sgcn2015"] = 1
        
        for uniqueState in uniqueStateNames:
            row_state = row_common+[uniqueState["sgcn2005"],uniqueState["sgcn2015"],uniqueState["sgcn_state"]]
            writer_state.writerow(row_state)