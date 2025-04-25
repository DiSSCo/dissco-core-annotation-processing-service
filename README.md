# annotation-processing-service
[![DOI](https://zenodo.org/badge/512689892.svg)](https://zenodo.org/badge/latestdoi/512689892)

The annotation processing service can receive an annotation from two different sources:
- Through the API as a request to register or archive an annotation
- Through a RabbitMQ queue to register an annotation

After the processing services received the annotation event it will take the following actions:
- Check if the same annotation is already in the system based on:
  - Annotation target
  - Annotation creator
  - Annotation Motivation
- If there is not an existing annotation in the system it will:
  - Create and register a new Handle
  - Insert the annotation in the database
  - Insert the annotation in Elasticsearch
  - Publish a CreateUpdateDeleteEvent 
  - Return the new annotation
- If there is an existing annotation but `body`, `creator` or `preferenceScore` differs from the new annotation the system will:
  - Check if we need to update the handle (only when motivation is changed)
  - Update the annotation in the database
  - Update the annotation in Elasticsearch
  - Publish a CreateUpdateDeleteEvent (with the jsonpatch)
  - Return the updated annotation
- If there is an existing annotation but the `body`, `creator` and `preferenceScore` is the same the system will:
  - Update the last checked timestamp of the annotation but not update anything else
  - Return `null`

The annotation processing does not work with batch functionality but processes a single annotation at a time.

If the insert into elasticSearch or the publishing of the CreateUpdateDelete event fails it will rollback the previous steps.

## Run locally
To run the system locally it can be run from an IDEA.
Clone the code and fill in the application properties (see below).
The application needs to store data in a database and an Elastic Search instance.
In RabbitMQ mode it needs a RabbitMQ cluster to connect to and receive the messages from.

## Run as Container 
The application can also be run as container.
It will require the environmental values described below.
The container can be built with the Dockerfile, in the root of the project.

## Profiles
There are two profiles with which the application can be run:
### Web
`spring.profiles.active=web`  
This listens to a API which had two endpoints:
- `POST /`
This endpoint can be used to post annotation events to the processing service. After this it will follow the above described process.
- `DELETE /{prefix}/{postfix}`
This endpoint can be used to archive a specific annotation.
Archiving will put the status of the Handle on `Archived`, fill the `deleted_on` field in the database and remove the annotation from Elasticsearch.

### RabbitMQ
`spring.profiles.active=rabbit-mq-mas`
This will make the application listen to a specified queue and process the annotation events from the queue.
After receiving it will follow the above describe process.
If exceptions are thrown, it will retry the message a X number of time after which it will push it to a Dead Letter Queue

## Environmental variables
The following backend specific properties can be configured:

```
# Database properties
spring.datasource.url=# The JDBC url to the PostgreSQL database to connect with
spring.datasource.username=# The login username to use for connecting with the database
spring.datasource.password=# The login password to use for connecting with the database

#Elasticsearch properties
elasticsearch.hostname=# The hostname of the Elasticsearch cluster
elasticsearch.port=# The port of the Elasticsearch cluster
elasticsearch.index-name=# The name of the index for Elasticsearch

# RabbitMQ properties (only necessary when the RabbitMQ profile is active). RabbitMQ also has a series of default properties
spring.rabbitmq.username=# Username to connect to RabbitMQ
spring.rabbitmq.password=# Password to connect to RabbitMQ
spring.rabbitmq.host=# Hostname of RabbitMQ
