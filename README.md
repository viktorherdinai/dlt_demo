# Delta Live Tables Demo project

The aim of the project was to conduct an experiment on one of the features called Delta Live Tables. The project also encompasses features such as AutoLoader and CDC tables. My objective was to gain a brief overview of these features and concepts.

---

# Architecture

![Architecture Diagram](assets/dlt_demo_architecture.jpg)

---

# How to run the project
## 1. Creating the AWS infrastructure
- Install terraform 1.9.0
- You need to set up your AWS credentials:
    - aws_access_key
    - aws_secret_access_key
- Navigate to the terraform directory and run the following commands:
    - `terraform init`
    - `terraform apply`
- To create the S3 bucket for both development and production environment, you need to create terraform the terraform workspaces
    - `terraform workspace new dev` (and the previous commands)
    - `terraform workspace new prod` (and the previous commands)

  
This will create the necessary infrastructure for the project.

## 2. Creating mock data to be processed

- In the root directory, install the required packages:
    - `pip install .`
- Navigate to the dlt_demo directory and run the following command:
    - In the script you need to specify the bucket name if you changed it in the terraform script
    - `python create_mock_data.py`


This will upload a new worker data to the bucket every second.

## 3. Creating the Databricks Asset Bundle file

- You need to set up the following environment variables (preferably in a `.env` file):
    - `DATABRICKS_HOST_NAME` - The hostname of your Databricks workspace
    - `S3_BUCKET_URI_DEV` - The URI of the created DEV bucket
    - `S3_BUCKET_URI_PRD` - The URI of the created PROD bucket

- Navigate to the `dlt_demo/utils` directory and run the following command:
    - `python create_dab_yaml.py`
  
This will create the necessary `databricks.yaml` file to easily deploy the pipeline via databricks cli tool. 


## 4. Deploying the Delta Live Tables pipeline to Databricks

- Navigate to the root directory and run the following command:
  - You can create the pipeline for DEV/TEST/PRD environments
  - `databricks bundle deploy --target {DEV|TEST|PRD}`

This will deploy the pipeline to Databricks with the predefined configurations.

## 5. Running the pipeline

- To run the pipeline you can use the following command:
  - `databricks bundle run --target {DEV|TEST|PRD}`
- Or alternatively you can run the pipeline via the Databricks UI.
