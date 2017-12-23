# coding: utf-8

from bis import worms
from bis2 import dd
from IPython.display import display
from datetime import datetime

bisDB = dd.getDB("bis")
sgcnTIRProcessCollection = bisDB["SGCN TIR Process"]

count = 0
processRecord = {}
while processRecord is not None:
    # Retrieve a record for processing - any record that does not yet have WoRMS data and has had a clean scientific name provided
    processRecord = sgcnTIRProcessCollection.find_one({"$and":[{"worms":{"$exists":False}},{"ScientificName_clean":{"$exists":True}}]},{"ScientificName_clean":1})

    if processRecord is not None:
        
        # We can only reasonably process something with a scientific name string of some length
        # For those records where we can't process anything, we set a simple metadata structure to the item so we can filter these out
        if processRecord["ScientificName_clean"] is None or len(processRecord["ScientificName_clean"]) == 0:
            _tirMetadata = {}
            _tirMetadata["dateProcessed"] = datetime.utcnow().isoformat()
            _tirMetadata["searchURL"] = None
            wormsData = [_tirMetadata]
        else:
            wormsData = worms.lookupWoRMS(processRecord["ScientificName_clean"])

        sgcnTIRProcessCollection.update_one({"_id":processRecord["_id"]},{"$set":{"worms":wormsData}})
        count = count + 1
        print (count)