terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

locals {
  env = terraform.workspace
}

provider "aws" {
  region = "eu-central-1"
}

module "public_bucket" {
  source      = "./modules/public_bucket_aws"
  bucket_name = "databricks-dlt-demo"
  environment = local.env
}

resource "aws_s3_object" "object" {
  depends_on = [module.public_bucket]
  bucket = module.public_bucket.bucket_id
  key    = "schemas/worker_schema_v1.json"
  source = "../schemas/worker_schema_v1.json"
  acl    = "public-read"
}
