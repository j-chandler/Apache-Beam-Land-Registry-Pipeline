# Apache Beam Land Registry Sample

A repository for practicing Apache Beam pipeline tasks.


## Task Description

The practice task was to read csv property data, links provided below, and convert it to JSON format.
The property data was made up of transactions depicting the sales of properties.
The final output was also to have transactions grouped by property for easier future use.

Data available [here](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads)
Description about the data [here](https://www.gov.uk/guidance/about-the-price-paid-data#download-options)


## Data Inspection

Before working with any data it is a good idea to understand it's format. I downloaded the monthly csv data
to act as a sample and immediately noticed that there was no unique identifier for the properties.

Some columns such as the Secondary Address Object Name could be blank would have to be accounted for.

Other categories, such as Old/New, have values that can be expected to change so should be attached to transactions not to a property identifier. Interestingly, the data description mentions that Postcodes can be reallocated and as such should also be treated separately and not as part of a unique identifier.

The most reliable option I came up with based on the data was to join the Primary Address Object Name, the Secondary Address Object Name, the Street, Locality, Town/City, District and County.

They were joined separated by a blank space and if any column values were missing they were ignored. This allowed for a unique identifier that was readable by humans and would make sense when being passed to other parts of the business.

Lastly whilst inspecting the data I noticed that some transactions that had different transaction IDs (i.e. had been processed as separate events) even though the sales data and even the date stamp were exactly the same. My thoughts on this were that it should be noted down, processed like the others and then later it can be analysed to decipher whether this was intentional and how best to resolve it.


## Pipeline Design

When thinking of designing the pipeline I came up with five minimum key steps that would need to be accomplished:

    - Read CSV data
    - Create a Unique Identifier for Properties
    - Join properties on transactions
    - Convert into desired JSON format
    - Write to JSON file

My initial attempt after creating the Unique Identifier, as described above in the Data Inspection section, was to split the pipeline into two. One PCollection would be grouping transactions, the other would be saving the property meta data, i.e. the data used to make the unique identifier saved in case other parts of the business just needed a certain column.

I planned to then use CoGroupByKey to combine the two PCollections into a JSON format and almost get two steps for one, however, CoGroupByKey had some difficulties with the transactions data being a list of lists. Therefore I elected to instead save the property data with the transactions, these were grouped as before and then later could be parsed out in a the JSONFormatter class I created.

When it came to writing the JSON files I wasn't sure exactly which format the files were desired to be in. Given the need for the Property Identifier key and grouping transactions together I intuited that software engineers needed to be able to input a property name and a JSON object would be returned.

I could think of three possibilities that suit this need whilst maintaining being a JSON file. One JSON file could be made for each property, though this would still have to be looped through in storage so not ideal. A single Newline JSON file could be made, this would still need to be looped through but would be easier to transfer to a databasing system or a Python Dictionary-esque JSON file could be created where the engineer could load the whole file and then enter the key as needed, easiest for the dev but massive redundancy in network data transfer if they only need one item.

I elected for the Newline JSON file, primarily because I believe the long-term solution for this problem would not be JSON files but rather a Database system that allows for nested data as I outline in the Next Steps section.


### Accuracy

On the topic of how I would attempt to guarantee accuracy I would like to point back to the old adage of "You Don't Know What You Don't Know" and so guaranteeing in my opinion is mostly in terms of how you gracefully handle errors that you haven't yet thought about.

To that end I have left some TODOs in my exception catches, it would also be a good idea to have a general try-except catch running over the whole pipeline if this was in production. These exceptions could then be logged and, if able, processed and stored somewhere so that the data could be reviewed, the pipeline code updated, and if possible, the data manually inserted.


## Cloud Deployment

Beam pipelines are often fairly easy to run on the cloud because there is no processing done until pipeline.run() is called. Therefore how the pipeline is run depends on the pipeline's runner. In my example it is set to DirectRunner meaning it will run on the local hardware. Cloud solutions should have their own runners depending on what provider you are using, for my preference I like Google Cloud Platform as it is the one I am most familiar with but also believe it is the one that provides the easiest solutions.

On GCP to run a Beam pipeline you have to change the runner type to "DataflowRunner", make sure your project, region etc. is properly implemented with Google Cloud and also check that your storage systems are to a Google Cloud Storage Bucket or is using a connector to another suitable storage system such as Datastore, BigQuery or Cloud Spanner, again depending on the use cases.

Running these jobs can then be done by executing it in the command shell or connecting via HTTPS to a Cloud Function or Pub/Sub for a triggered response.


## Next Steps

My next steps would be very dependent on what the use cases of the data is. 

What I would like to do is make sure that proper error logging is set up and rogue data is sent to another storage bucket for later analysis so that the current pipeline can be continuously improved.

I also believe that it would be a good idea to set up connectors to some of the services provided by Google Cloud, which could solve the JSON file issues I laid out above. BigQuery, Cloud Spanner and Cloud Datastore are all able to handle searching for a property by a key and having nested data inside it (i.e. transactions can be stored inside a property), which allows simple queries to be written to retrieve the data. This stops too much data being passed, removes the need for looping through a file/s and allows for the data engineering team to use a strong databasing system.

Also instead of passing around JSON files it may be a good idea to switch to AVRO files. These file types are able to contain the same nesting features that make JSON so useful but they also come with the schema attached making them very useful for uploading to database systems or parsing through.
