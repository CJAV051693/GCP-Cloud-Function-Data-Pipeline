# -------------------------- PROVIDERs -------------------------------------

provider "google-beta" {
  credentials = file(var.gcp_credential_path)
  project     = var.gcp_project_id
  region      = var.gcp_project_region
  zone        = var.gcp_project_zone
  impersonate_service_account = var.owner_sa_name
  scopes = [
    "https://www.googleapis.com/auth/cloud-platform",
  ]
}

provider "google" {
  credentials  = file(var.gcp_credential_path)
  project      = var.gcp_project_id
  region       = var.gcp_project_region
  impersonate_service_account = var.owner_sa_name
  scopes = [
    "https://www.googleapis.com/auth/cloud-platform",
  ]
}

# -------------------------- Back-End Configuration --------------------------

terraform {
  backend "azurerm" {
    subscription_id      = "#{SUBSCRIPTION_ID}#"
    use_azuread_auth     = "true"
    resource_group_name  = "#{RG_NAME}#"
    storage_account_name = "#{SA_NAME}#"
    container_name       = "#{CONTAINER_NAME}#"
    key                  = "terraform/#{GCP_PROJECT_ID}#/#{TOOL_TF_PIPELINE_NAME}#/cloudfunction_cloudscheduler_sm/v_1_2_9"
  }
}

# -------------------------- Project Configuration ---------------------------
# Getting project resource

data "google_project" "project" {
  project_id = var.gcp_project_id
}

# ---------------------------- Locals ----------------------------------

locals {
  operation_sa_name = element(split("@", var.operation_sa_name),0)
}

# -------------------- Cloud Storage ----------------------------------
module "gcp_storage" {
    source              = "acnciotfregistry.accenture.com/accenture-cio/storage/google"
    version             = "#{TF_STORAGEMODULE_VERSION}#"
    project_id          = var.gcp_project_id
    storage_class       = "REGIONAL"
    location            = var.gcp_project_region
    storage_name        = var.gcp_cs_bucket_name
}

# -------------------- Secret Manager -----------------------------------

module "secret" {
  source        = "acnciotfregistry.accenture.com/accenture-cio/secretmanager/google"
  project_id    = var.gcp_project_id
  version       = "#{TF_SM_MODULE_VERSION}#"
  secret_name   = var.cs_sm_name
  locations     = ["us-centra1","us-east1"]
  secrets       = {
     AZURE_USER = "#{AZURE_USER}#"
     AZURE_PASS = "#{AZURE_PASS}#"
  }
}

# ------------------------- Cloud Funtion -----------------------------

module "google_function_gen2_https" {
    source                            = "#{TF_CF_MODULE_SOURCE}#"
    version                           = "#{TF_CF_MODULE_VERSION}#"
    location                          = var.gcp_project_region
    project_id                        = var.gcp_project_id
    google_function_name              = "#{TRIGGER_FUNCTION_NAME}#"
    google_function_entrypoint        = "#{TRIGGER_FUNCTION_ENTRYPOINT}#"
    function_type                     = "https" # https, pubsub, storage
    runtime                           = "python313"
    source_dir                        = "./cloudFunction"
    path_to_data_to_upload            = "./cloudFunction.zip"
    google_storage_bucket_name        = module.gcp_storage.storage_name
    google_storage_bucket_object_name = "python/cloudFunction"
    available_memory_mb               = "4096M"
    timeout                           = 3600
    min_instances                     = 0
    max_instances                     = 3
}

module "google_cloud_scheduler_job_post" {
  source = "#{TF_CS_MODULE_SOURCE}#"
  version = "#{TF_CS_MODULE_VERSION}#"
  project_id               = var.gcp_project_id 
  cloud_scheduler_httpjob_oidc = {
  #{TRIGGER_FUNCTION_NAME}#  = {
      region               = var.gcp_project_region
      description          = var.gcp_cs_description
      schedule             = var.gcp_cs_schedule
      time_zone            = var.gcp_cs_timezone
      attempt_deadline     = "600s"
      retry_count          = "0"
      max_retry_duration   = "60s"
      min_backoff_duration = "5s"
      max_backoff_duration = "30s"
      max_doublings        = "3"
      http_method          = "POST" 
      uri                  =  module.google_function_gen2_https.url[0]
      payload              = var.gcp_cs_payload
      body                 = "say=Hi&to=World"            # HTTP Request body. A request body is allowed only if the HTTP method is POST, PUT, or PATCH. Other methods still needs body to be defined as empty (body = "")
      headers              = {                            # This map contains the header field names and values.
        Host = "foo.com"
        Content-Type = "application/x-www-form-urlencoded"
        User-Agent = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)"
      }
    }
  }
}