variable "environment" {
  description = "The environment to deploy the bucket in."
  type = string
  default = "dev"
}

variable "bucket_name" {
  description = "The name of the bucket"
  type = string
}
