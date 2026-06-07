# credentials = "${file("${var.gcp_credential_path}")}"
variable "gcp_credential_path" {
  type        = string
  description = "the gcp credential path"
  default     = ""
}

# project     = "sbx-4985-intpilotproc-dab2aa01"
variable "gcp_project_id" {
  type        = string
  description = "the internal project id of your project "
  default     = ""
}

# region      = "us-east1"
variable "gcp_project_region" {
  type        = string
  description = "the internal project id of your project"
  default     = ""
}

# zone        = "us-east1-d"
variable "gcp_project_zone" {
  type        = string
  description = "the internal project id of your project"
  default     = ""
}

# gcp_vpc_name = "processing-vpc-subnetwork"
variable "gcp_vpc_name" {
  type        = string
  description = "the name of the VPC to be used"
  default     = ""
}

variable "operation_sa_name" {
  description = "ID of the service account to operate composer"
  type        = string
}

variable "owner_sa_name" {
  description = "ID of the service account to operate composer"
  type        = string
}

variable "gcp_cs_bucket_name" {
  type        = string
  description = "The bucket to store dataproc files"
}

variable "gcp_cs_description" {
  type        = string
  description = "A short description about what the job role. Must not contain more than 500 characters."
  default     = "pubsub-scheduler"
}

variable "gcp_cs_schedule" {
  type        = string
  description = "Describes the schedule on which the job will be executed, in unix-cron format."
  default     = "0 */2 * * *"
}

variable "gcp_cs_timezone" {
  type        = string
  description = "Specifies the time zone to be used in interpreting schedule."
  default     = "America/Chicago"
}

variable "gcp_cs_payload" {
  type        = string
  description = "The message payload for PubsubMessage, which must contain non-empty data"
  default     = "message payload"
}

variable "cs_sm_name" {
  type        = string
  description = "Secret manager name"
}