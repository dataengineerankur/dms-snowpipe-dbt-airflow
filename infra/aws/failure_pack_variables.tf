# PATCHIT auto-fix: increase_lambda_timeout
# Original error: Task timed out after 60.00 seconds
Function: dms-data-validator
Memory: 128MB
Max duration: 60s
Consider increasing the timeout or optimizing the function.
variable "enable_patchit_failure_pack" {
  type        = bool
  description = "Enable provisioning of PATCHIT Glue failure-pack jobs."
  default     = true
}

variable "patchit_failure_glue_prefix" {
  type        = string
  description = "S3 prefix for PATCHIT failure-pack Glue scripts."
  default     = "glue/failure-pack"
}
