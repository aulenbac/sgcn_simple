# coding: utf-8

from bis2 import dd
from IPython.display import display
from datetime import datetime
import re

bisDB = dd.getDB("bis")
sgcnTIRProcessCollection = bisDB["SGCN TIR Process"]

synthesisPipeline = [
    {"$match":{
        "$and":[
            {"Scientific Name":{"$ne":None}},
            {"Scientific Name":{"$ne":""}}
        ]
    }},
    {"$group":{
        "_id":"$Scientific Name",
        "Common Name":{"$first":"$Common Name"},
        "Taxonomic Group":{"$first":"$Taxonomic Group"},
        "Match Method":{"$first":"$Match Method"},
        "Taxonomic Authority ID":{"$first":"$Taxonomic Authority ID"},
        "Taxonomy":{"$first":"$Taxonomy"},
        "Source Data Summary":{"$addToSet":"$Source Data Summary"},
        "ITIS":{"$first":"$itis.itisData"},
        "WoRMS":{"$addToSet":"$worms"},
        "TESS":{"$first":"$tess.tessData"},
        "NatureServe":{"$first":"$NatureServe.natureServeData"}
    }},
    {"$project":{
        "Common Name":1,
        "Taxonomic Group":1,
        "Match Method":1,
        "Taxonomic Authority ID":1,
        "Taxonomy":1,
        "Source Data Summary":{"$arrayElemAt":["$Source Data Summary",0]},
        "ITIS":1,
        "WoRMS":{"$arrayElemAt":["$WoRMS",0]},
        "TESS":1,
        "NatureServe":1
    }},
    {"$out":"SGCN Synthesis"}
]

print (sgcnTIRProcessCollection.aggregate(synthesisPipeline,{"allowDiskUse":True}))