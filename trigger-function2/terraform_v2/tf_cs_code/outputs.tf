# output for bucket
output "storage_name" {
  description = "Name of the bucket"
  value       = "${module.gcp_storage.storage_name}"
}

# output for CF
output "function_name" {
  description = "Name of the CF"
  value       = "${module.google_function_gen2_https.function_name}"
}

# scheduler job name with http as a target
output "http_jobs_oidc_name" {
  description = "Name of the scheduler job"
  value       = "${module.google_cloud_scheduler_job_post.http_jobs_oidc_name}"
}

# --------------------- output for SM  --------------------------------#
output "secret_manager_id" {
  description = "Name of the SM"
  value       = "${module.secret.secret_manager_id}"
}