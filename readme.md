# SGCN Processing System
This latest iteration of the processing system for State Species of Greatest Conservation Need uses MongoDB as its persistent data store and refines all of the underlying processes. I've rebuilt much of the system from scratch, keeping in place a few of the key functions but modifying as needed to fit the new model. As an experiment in building code in a collaborative, cloud-based environment (c9.io), I wrote everything in simple Python and kept dependencies to a minimum.

## Dependencies
The following are necessary in order for the system to run.

### Source file repository
The system relies on items in a [ScienceBase colection](https://www.sciencebase.gov/catalog/item/56d720ece4b015c306f442d5) with the following characteristics:

* A single "Place" type tag containing the full state name
* A single "Collection" type date containing the year for the list (2005 or 2015)
* A single tab-separated file with the title, "Process File," containing the data in the standard formats for 2005 and 2015 data (more on that later)
* Items need to be publicly available as the code does not authenticate

### Persistent data store
All database interactions in the system run using pymongo. Initially, I built the code against a free MLab sandbox account for testing, and then I moved it to a MongoDB instance we just got set up on the ESIP Testbed. Database connections are handled through the secure bis2 package. Data are orgainzed into three logical collections.

* SGCN Source Data - A dump of each submitted file transformed from text to JSON documents for processing
* SGCN TIR Process - This starts with setup of each unique name slightly cleaned up for further processing and then is built with subdocuments containing the results of various processes
* SGCN Synthesis - Final synthesis that is grouped on the settled unique scientific name for the species (or other taxonomic rank name) from authority matching and then built out for optimized use

### Custom Packages

The code in this repo makes use of two custom and in-development packages for the Biogeographic Information System. The [bis](https://github.com/usgs-bcb/bis) is a public collection of modules and functions that exercise common algorithms used across these and other codes. The bis2 package is internal only as it contains connection string information and other sensitive details used in our work (in the process of shifting this to a new method).

### Public/Open Source Packages

The major packages used in these codes include the following:

* csv
* xmltodict
* pymongo
* requests

## Processing Steps

Some aspects of the SGCN process need to occur in sequence while others can be initiated in parallel after certain dependencies in the data have been met. All codes were written to check for and act on available data based on a MongoDB query designed to look for something left to process, giving them a level of resilience in the face of data flow challenges (e.g., slow or flaky APIs). I numbered the Python scripts based on necessary or reasonable processing sequence.

### 1_ProcessSGCNRepositorySourceFiles.py
Processes the source repository in ScienceBase to integrate all compliant items/files into the SGCN collection in the target MongoDB instance within an SGCN Source Data collection

### 2_CleanUniqueScientificNames.py
Builds the initial unique name documents and runs a function from the BIS package called cleanScientificName to generate the actual name string used throughout the process. This function makes a number of assumptions about what to do with the name strings that may need to be revisited over time. It gets rid of some conventions in naming species that may be specific to a region or discipline or a specific agency that we simply cannot fully understand at this time.

### 3_ProcessITIS.py
The latest version of this has pretty much all of its logic contained in the bis.itis module. It takes a scientific name string and runs through a series of queries against the ITIS Solr service to find matches. It now packages and returns all documents found in the process, following the taxonomic information to return a valid ITIS record if available but also including invalid records at the point of discover. This sets up the data for later decisionmaking. The function does strip out some less meaningful (at this point) information from the ITIS documents and reformats taxonomy for easier use.

### 4_ProcessWoRMS.py
Similar to the ITIS process, this code now has all of the necessary logic built into the bis.worms module where we find one or more records via WoRMS REST services and bring back results for later processing.

### 5_SetupTESSProcessing.py
Sets up a sub-ducument called TESS in the UniqueNames collection and uses a function from bis.tess to set up how the TESS processor should run to find and retrieve Federal listing status information from the USFWS Threatened and Endangered Species System. Because this script relies on ITIS information (and WoRMS if available), it should be run (or started at least) after the ITIS process. The query that drives the while loop here will look for UniqueNames documents that have ITIS data on board and will use WoRMS data as well if available.

### 6_ProcessTESS.py
Processes the information from the tess.registration sub-document to lookup the species (based on either ITIS TSN or name), process the returned information using a function in the bis.tess module, record details in the tess.processingMetadata sub-document, and place data into the tess.tessData sub-document.

Note: TESS is a fairly old web service based on XML XQuery. We should keep an eye on this service over time for updates or changes.

### 7_SGCNDecisions.py
At this stage, we can make a number of decisions about how to handle the ITIS, WoRMS, and TESS information we've connected to and brought back for further processing. This code contains the logic that put species on the SGCN National List when they have been successfully matched to ITIS or WoRMS and synthesizes the FWS listing information. It sets up the Scientific Name variable to be used by subsequent steps from this point.

### 8_ProcessNatureServe.py
After working through difficulties with a number of previous steps, we found a new public service that can be used from NatureServe for discovering and returning basic information at a national scope for the US. This code is also now fully contained in the bis.natureserve module.

### 9_Synthesize.py
This final step uses an aggregation pipeline to group the TIR processed records on unique Scientific Name (determined in 7_SGCNDecisions) and then builds out an optimized structure of additional information based on what we are doing with the SGCN process.

## Data Distribution
From this point, we are planning to build out an sgcn end point on the nascent bis API as the point of access for these data. That work will be updated here once available.