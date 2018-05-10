# coding: utf-8

from IPython.display import display
from datetime import datetime
from bis2 import dd
from bis import sgcn

bisDB = dd.getDB("bis")
sgcnSourceCollection = bisDB["SGCN Source Data"]
sgcnTIRProcessCollection = bisDB["SGCN TIR Process"]

# Get all unique names currently in the SGCN TIR processing collection
namesInDatabase = sgcnTIRProcessCollection.find({}).distinct("ScientificName_original")

# Use an aggregation grouping function to get all unique names currently in the SGCN Source Data collection set of processed files
pipeline = [
    {"$unwind":{"path":"$sourceData"}},
    {"$group":{"_id":None,"names":{"$addToSet":"$sourceData.scientific name"}}}
]
for record in sgcnSourceCollection.aggregate(pipeline):
    # Extract just the list of unique names from the aggregation cursor
    namesInFiles = record["names"]
    
# Process out any new names not currently in the SGCN TIR process collection to set them up for processing
for newName in list(set(namesInFiles) - set(namesInDatabase)):
    sgcnTIRProcessCollection.insert_one(sgcn.package_source_name(newName))