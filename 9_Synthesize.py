# coding: utf-8

from bis2 import dd
from IPython.display import display
from datetime import datetime
import re
from bis import sgcn

bisDB = dd.getDB("bis")
sgcnSynthesisCollection = bisDB["SGCN Synthesis"]
sgcnTIRProcessCollection = bisDB["SGCN TIR Process"]

_buildSynthesisCollection = False
_buildStateTESSSynthesis = False
_buildNatureServeSynthesis = True

if _buildSynthesisCollection:

    synthesisPipeline = [
        {"$match":{"ScientificName_clean":{"$ne":""}}},
        {"$group":{
            "_id":"$Scientific Name",
            "Common Name":{"$first":"$Common Name"},
            "Taxonomic Group":{"$first":"$Taxonomic Group"},
            "Match Method":{"$first":"$Match Method"},
            "Taxonomic Authority ID":{"$first":"$Taxonomic Authority ID"},
            "Taxonomy":{"$first":"$Taxonomy"},
            "Original Submitted Names":{"$addToSet":"$ScientificName_original"}
        }},
        {"$out":"SGCN Synthesis"}
    ]
    
    arg = {"allowDiskUse":True}
    
    print (sgcnSynthesisCollection.drop())
    print (sgcnTIRProcessCollection.aggregate(synthesisPipeline,arg))


if _buildStateTESSSynthesis:

    count = 0
    synthRecord = {}
    
    while synthRecord is not None:
        synthRecord = sgcnSynthesisCollection.find_one({"$and":[{"State Summary":{"$exists":False}},{"TESS Summary":{"$exists":False}}]})
    
        if synthRecord is not None:
            stateSummary = sgcn.sgcn_state_submissions(sgcnTIRProcessCollection,synthRecord["Original Submitted Names"])
            updateInfo = {"State Summary":stateSummary}
            tessSummary = sgcn.sgcn_tess_synthesis(sgcnTIRProcessCollection,synthRecord["Original Submitted Names"])
            if tessSummary is not None:
                updateInfo["TESS Summary"] = tessSummary
            
            sgcnSynthesisCollection.update_one({"_id":synthRecord["_id"]},{"$set":updateInfo})
        
        count = count + 1
        print ("State/TESS", count)
        
        
if _buildNatureServeSynthesis:
    
    count = 0
    synthRecord = {}
    while synthRecord is not None:
        synthRecord = sgcnSynthesisCollection.find_one({"NatureServe Summary":{"$exists":False}},{"_id":1})
    
        if synthRecord is not None:
            sgcnSynthesisCollection.update_one({"_id":synthRecord["_id"]},{"$set":sgcn.sgcn_natureserve_summary(sgcnTIRProcessCollection,synthRecord["_id"])})
        
        count = count + 1
        print ("NatureServe", count)
