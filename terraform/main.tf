terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-central-1"
}

module "public_bucket"{
  source = "./modules/public_bucket_aws"
  bucket_name = "databricks-dlt-demo"
}

resource "aws_s3_object" "object" {
  bucket = module.public_bucket.bucket_id
  key    = "schemas/worker_schema_v1.json"
  source = "../schemas/worker_schema_v1.json"
  acl    = "public-read"
}