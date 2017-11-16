# coding: utf-8

from bis2 import dd
from IPython.display import display
from datetime import datetime
import requests,csv,io

bisDB = dd.getDB("bis")
uniqueNamesCollection = bisDB["UniqueNames"]

for record in uniqueNamesCollection.aggregate([{"$group":{"_id":"$SGCN Summary.Match Method","count":{"$sum":1}}}]):
    display (record)

