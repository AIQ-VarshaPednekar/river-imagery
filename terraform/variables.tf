variable "project_id" {
  description = "GCP Project ID (e.g. plucky-sight-423703-k5)"
  type        = string
  default     = "plucky-sight-423703-k5"
}

variable "project_number" {
  description = <<-DESC
    GCP Project NUMBER (not the ID). Find it by running:
      gcloud projects describe plucky-sight-423703-k5 --format='value(projectNumber)'
    Example: 123456789012
  DESC
  type        = string
  # No default — you MUST set this in terraform.tfvars
}

variable "region" {
  description = "GCP region (us-central1 = Mumbai)"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "us-central1-a"
}

variable "bucket_name" {
  description = "GCS bucket where tiles live and merged output goes"
  type        = string
  default     = "aiq-river-imagery"
}

variable "input_prefix" {
  description = "GCS folder with raw tiles (e.g. Sentinel)"
  type        = string
  default     = "Sentinel"
}

variable "output_prefix" {
  description = "GCS folder for merged output TIFFs"
  type        = string
  default     = "Sentinel_Merged"
}

variable "vm_name" {
  description = "Compute instance name"
  type        = string
  default     = "river-merge-vm"
}

variable "machine_type" {
  description = <<-DESC
    VM machine type. Recommended options:
      n2-highmem-4  →  4 CPU, 32 GB RAM  (~$0.24/hr)  ← good default
      n2-highmem-8  →  8 CPU, 64 GB RAM  (~$0.48/hr)  ← if OOM on large rivers
      n2-standard-4 →  4 CPU, 16 GB RAM  (~$0.19/hr)  ← cheaper, tighter
  DESC
  type        = string
  default     = "n2-highmem-4"
}

variable "disk_size_gb" {
  description = "Boot disk size in GB. Needs to hold all tiles temporarily."
  type        = number
  default     = 200
}

variable "use_spot" {
  description = <<-DESC
    Use Spot (preemptible) VMs — ~60-70% cheaper, but can be killed mid-merge.
    Safe for short jobs (< 30 min). Set false for large multi-river batches.
  DESC
  type        = bool
  default     = false
}

variable "selected_rivers" {
  description = "Specific rivers to merge. Empty list = merge ALL rivers in bucket."
  type        = list(string)
  default     = []
}
