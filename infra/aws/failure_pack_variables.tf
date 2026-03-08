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
