terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {}

module "public_bucket"{
  source = "./modules/public_bucket_aws"
  bucket_name = "databricks-dlt-demo"
}

resource "aws_s3_object" "object" {
  bucket = module.public_bucket.bucket_id
  key    = "from_tf.json"
  source = "../mock_data/workers2.json"
  acl    = "public-read"
}