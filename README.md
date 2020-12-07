# API Interface for Analytics on IMDB Data

## CSCI 5253: Datacenter Scale Computing, CU Boulder

<p></p>

<b><u> Project Contributors: </u></b>
 - Abhinivesh Palusa
 - Nikhil Prabhu

<b><u> Project Goals: </u></b>

IMDB data doesn’t provide an official documented API. The main goal of our project is to create an API for describing shows (titles), describing people working in those shows, and providing analytics on already available data (sourced from IMDB interfaces website [1]) and also to update these analytics if either new data is inserted or existing data is updated.

<b><u> Software and Hardware Components: </u></b>

 - AWS Services:
    - S3 buckets
    - API Gateway
    - Serverless Lambda functions
    - SQS (Simple Queue Service)
    - DynamoDB 
    - EC2 instances
    - Cloudwatch logs
 - Elasticsearch
 - Kibana
 - Nginx 

<b><u> Architectural Diagram: </u></b>

![Screen capture of 2 worker nodes output](https://github.com/AbhiniveshP/DCSC-Project-IMDB/blob/main/architecture_diagrams/DCSC%20Architecture%20Diagram.jpg)

<b><u> Pipelines separated: </u></b>

 - <i><u> Flow 1 ⇒ S3 → SQS → DynamoDB </u></i>
    - Batch of data falls into S3 pushed by developer and λ is triggered.
    - This λ pushes data record-wise into SQS and one more λ is triggered.
    - The second λ pushes each message/record into relevant table of DynamoDB.
 
 - <i><u> Flow 2 ⇒ API Gateway → DynamoDB → S3 </u></i>
    - User requests data via API Gateway triggering a λ to a DynamoDB database. 
    - This λ transforms data to a relevant format collecting data from required tables in the DB.
    - This data is stored in a HTML document in S3 whose public URL is sent as response.

 - <i><u> Flow 3 ⇒ API Gateway → DynamoDB </u></i>
    - A record to be inserted / updated is posted via an API Gateway triggering a λ.
    - This λ updates the relevant record or inserts a new one in the corresponding table of the DynamoDB database.

 - <i><u> Flow 4 ⇒ DynamoDB → ES → Kibana[NGINX] </u></i>
    - Whenever data is inserted or updated in DynamoDB, a λ is triggered.
    - This λ transforms data into a relevant JSON doc to be pushed into one of the available two indices on Elasticsearch (titles and people).
    - These two indices can create many visualizations on Kibana.
    - Kibana port 5601 is forwarded from port 80 by NGINX.

<b><u> Testing & Debugging </u></b>

 - Designed classes for each service (S3, SQS, DynamoDB, Elasticsearch) for better development and testing purposes.
 - Created unit-tests around above services and API Gateway using a mini-batch of data (5 records from each table with 7 tables).
 - Integration tests for each of the 4 flows using same type of mini-batch.
 - Integration tests for the entire architecture of the 4 flows.
 - Debugging data formats between components using Cloudwatch logs.
 - Using Kibana Dev tools to check whether the data is inserted in the expected format using various APIs like Insert API, Delete API, Count API, Bulk API, etc.


<b><u> Limitations & Future Work: </u></b>
 - Limited AWS credits (100) and limited time per session (3 hours).
 - Couldn’t store the entire 10GB of data and couldn’t do batch upload for all tables in any of the AWS services and Elasticsearch on EC2 instance. Only one table at max per session.
 - Performing advanced data transformations while moving data between components using corresponding λ functions.
 - More number of visualizations on the Kibana dashboard.
 - Currently limiting data to ‘en’ language and can be extended all available languages on IMDB.
 - Services involving container architecture (Docker & K8s).


<b><u> References: </u></b>

[1] https://datasets.imdbws.com/