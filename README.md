# PropertyTaxAI
Running LLM inference agent on publicly available property tax information

Note - initial scope of project will be limited to property tax data from the Harris County Appraisal District (HCAD) in Houston, Texas USA. 

# Diagram
<TBD\>

# Project Components
## Initial stage
[In progress] Leverage Python script to ingest HCAD zip files containing data in the form of csv files into a PostgreSQL Database  
[] Connect local Mistral LLM via Ollama to PostgreSQL DB and manually test simple uses cases (e.g. SQL fetch querries)

## Refining Stage
[] Automate downloading of CSV files from HCAD website  
[] Integrate metrics tracking for SQL Database queries for analytics
[] Create automated testing for evaluation model performance on database as well 
[] Investigate moving multi-threaded processing to serverless architecture (AWS Lambda) for paralle processing without additional costs with maintaining EC2 instances

## Deployment STage
[] Deployment of multi-node PostgreSQL DB with backup. 
[] Auotomate deployment of resources (local and cloud) using Docker and Kubernetes.

# Local Installation 
- Install `python3` and `virtualenv`
- Create python3 virtual environment: 
``` virtualenv -p python3 .venv ```
- Install requirements (Linux/Mac) 
``` 
source .venv/bin/activate 

pip install -r requirements.txt
```


# Challenges
- Cloud hosting - which provider to use as well as how to leverage cloud LLM for inference purposes? 
- Determining how to upscale Python application for handling ingest and processing of csv files (past years only need to be imported once, however data from the current year could be updated periodically). In addition, need to ensure that only changes in data are sent to database. (Operation mostly to be run on daily chron schedule.)

# Disclaimer 
Code is provided as-is. 

Generated Code Disclaimer
---------------

Some portions of this project's code are generated using Mistral LLM running locally with Ollama. 