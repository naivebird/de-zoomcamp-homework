variable "project" {
  description = "Project ID"
  type        = string
  default     = "de-zoomcamp-448805"
}

variable "credentials" {
    description = "Path to the service account key file"
    type        = string
    default     = "./keys/credentials.json"
}

variable "region" {
  description = "Region for the resources"
  type        = string
  default     = "us-west1"
}

variable "location" {
  description = "Location for the resources"
  type        = string
  default     = "US"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
  type        = string
  default     = "demo_dataset"
}

variable "gcs_storage_class" {
  description = "Storage class for the bucket"
  type        = string
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "Name of the GCS bucket"
  type        = string
  default     = "de-zoomcamp-448805-demo-bucket"
}