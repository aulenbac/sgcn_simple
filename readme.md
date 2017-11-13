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
All database interactions in the system run using pymongo. Initially, I built the code against a free MLab sandbox account for testing, and then I moved it to a MongoDB instance we just got set up on the ESIP Testbed. Database connections are handled through the secure bis2 package. Data are orgainzed into two logical collections.

* SGCN - Initially a simple dump of the attributes and values from the source repository with process metadata; augmented with a script that cleans up the scientific name string for processing
* UniqueNames - Since the SGCN process all operates based on unique scientific names provided by the states, the first step is to pull out the unique names and give them a semipermanent identify for further processing. This collection creates an identifier for every unique name, essentially a document stub, and then everything else build data on that core. I initially wrote data from every discrete process into its own datastore, which has some architectural advantages, but then I experimented with building a single nested document for every unique name. The latter approach seemed to result in cleaner and simpler code to deal with, but we may refactor again in future.

###Custom Packages

The code in this repo makes use of two custom and in-development packages for the Biogeographic Information System. The [bis](https://github.com/usgs-bcb/bis) is a public collection of modules and functions that exercise common algorithms used across these and other codes. The bis2 package is internal only as it contains connection string information and other sensitive details used in our work.

###Public/Open Source Packages

The major packages used in these codes include the following:

* csv
* xmltodict
* pymongo
* requests

## Processing Steps

Some aspects of the SGCN process need to occur in sequence while others can be initiated in parallel after certain dependencies in the data have been met. All codes were written to check for and act on available data based on a MongoDB query designed to look for something left to process, giving them a level of resilience in the face of data flow challenges (e.g., slow or flaky APIs). I numbered the Python scripts based on necessary or reasonable processing sequence.

###1_ProcessSGCNRepositorySourceFiles.py
Processes the source repository in ScienceBase to integrate all compliant items/files into the SGCN collection in the target MongoDB instance

###2_CleanUniqueScientificNames.py
Builds the initial unique name documents and runs a function from the BIS package called cleanScientificName to generate the actual name string used throughout the process. This function makes a number of assumptions about what to do with the name strings that may need to be revisited over time. It gets rid of some conventions in naming species that may be specific to a region or discipline or a specific agency that we simply cannot fully understand at this time.

###3_SetupITISProcessing.py
Sets up a "sub-document" called "itis" in the UniqueNames collection. It uses a convention of a registration object containing information that essentially registers the entity for further processing. It uses a function from the bis.itis module to set up the search URLs for further processing.

###4_ProcessITIS.py
This script runs the ITIS process based on the previously established point of registration. It uses the configuration from that object and runs through a series of logical steps to determine what ITIS documents to return and place within an itisData sub-document and uses a convention of recording processing details in a processingMetadata sub-document. This script makes use of a key function in the bis.itis module that handles packaging the JSON returned from the ITIS Solr service.

Note: This process will need to be refined for other use cases where we are getting information from ITIS, but much of the packaging logic should translate. There would be some benefit in looking at how the R-Taxize package and the Python port of that package handle the lookup process with ITIS and other authorities, although what we put together here is designed to operate in a completely unassisted way, recording what the algorithm finds in ITIS for further use downstream.

###5_SetupWoRMSProcessing.py
Sets up a sub-document called WoRMS in the UniqueNames collection and uses a function from bis.worms to set up how the WoRMS processor should run to find taxon information from the World Register of Marine Species.

###6_ProcessWoRMS.py
Processes the information from the worms.registration object in the UniqueSpecies collection to lookup the species, record processing details in a worms.processMetadata sub-document, and drop a slightly processed WoRMS record into worms.wormsData sub-document.

###7_SetupTESSProcessing.py
Sets up a sub-ducument called TESS in the UniqueNames collection and uses a function from bis.tess to set up how the TESS processor should run to find and retrieve Federal listing status information from the USFWS Threatened and Endangered Species System. Because this script relies on ITIS information (and WoRMS if available), it should be run (or started at least) after the ITIS process. The query that drives the while loop here will look for UniqueNames documents that have ITIS data on board and will use WoRMS data as well if available.

###8_ProcessTESS.py
Processes the information from the tess.registration sub-document to lookup the species (based on either ITIS TSN or name), process the returned information using a function in the bis.tess module, record details in the tess.processingMetadata sub-document, and place data into the tess.tessData sub-document.

Note: TESS is a fairly old web service based on XML XQuery. We should keep an eye on this service over time for updates or changes.

###9_SetupNatureServeProcessing.py
Sets up a sub-document called NatureServe in the UniqueNames collection and consults any available names information (original "clean" name, ITIS, WoRMS) to set up a list of unique names for querying a NatureServe web service for an associated ID.

###10_LookupNatureServeSpecies.py
Processes the unique names teed up in the NatureServe.registration sub-document to look for the taxon and return a NatureServe Element Global ID for further processing. This process does not do anything fancy in terms of trying to match up with NatureServe, only running a crude lookup at this point that should return the best match available. We will need to look further into uses of the NatureServe information before we dig in and refine this process. The elementGlobalID is recorded in the NatureServe.processingMetadata sub-document generated as part of this process.

###11_ProcessNatureServe.py
Processes the elementGlobalID using functions in the bis.natureserve and bis2.natureserve modules to retrieve and process selected information from the NatureServe species web services for further use. The main processing step here is to work through global, US regional, and US state (subnational) conservation status codes and build a more usable data structure with this information. The NatureServe web services are XML based, and this code uses the xmltodict package to process them into a more digestible dictionary. The script records the available upper level keys for reference, processes its own version of US conservation status, and caches the classification and full conservationStatus sub-documents for further evaluation as we determine the best uses of this information.

##Data Distribution
Once these processing steps complete, everything needed to work with the SGCN data and our synthesis is contained in the SGCN and UniqueNames collections of whatever MongoDB instance the code is pointed at. I will be rounding out this new iteration of the process by initially distributing data into the GC2 instance of PostgreSQL piped to ElasticSearch so that the current web application for this information and other existing codes will continue to function with the same structures they are used to now. I will also be experimenting with building out ElasticSearch indexes directly from the MongoDB data as an alternative that will eliminate this particular use of the GC2 infrastructure that is still experimental but causing us fits in other areas.