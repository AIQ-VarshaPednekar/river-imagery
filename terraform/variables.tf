# =============================================================================
# terraform/variables.tf — Input Variable Definitions
# =============================================================================
#
# PURPOSE:
#   Defines ALL input variables that terraform/main.tf uses.
#   Actual values come from terraform/terraform.tfvars (which is .gitignored
#   to protect sensitive project identifiers).
#
# HOW VARIABLES WORK IN TERRAFORM:
#   1. This file DECLARES variables (name, type, description, optional default)
#   2. terraform.tfvars SETS variable values  (project_id = "my-project")
#   3. main.tf READS variables using var.<name>
#
# RULE: Variables with `default` are optional in terraform.tfvars.
#       Variables WITHOUT `default` MUST be set in terraform.tfvars.
#       Only project_number has no default — it's unique per project.

# =============================================================================
# GCP PROJECT IDENTIFICATION
# =============================================================================

variable "project_id" {
  # WHY NEEDED:
  #   Terraform needs to know which GCP project to create resources in.
  #   All gcloud/gsutil commands also use this for --project flags.
  #   Format: lowercase alphanumeric with hyphens (e.g. "plucky-sight-423703-k5")
  #   Find it: GCP Console → top-left project dropdown → project ID column
  description = "GCP Project ID (e.g. plucky-sight-423703-k5)"
  type        = string
  default     = "plucky-sight-423703-k5"   # Pre-filled with this project's ID
}

variable "project_number" {
  # WHY NEEDED:
  #   The default Compute Engine service account email uses the PROJECT NUMBER (not ID):
  #     <project_number>-compute@developer.gserviceaccount.com
  #   Project ID and Project Number are DIFFERENT:
  #     ID:     "plucky-sight-423703-k5"  (human-readable string)
  #     Number: "123456789012"             (12-digit integer)
  #
  # HOW TO FIND IT:
  #   gcloud projects describe plucky-sight-423703-k5 --format='value(projectNumber)'
  #   OR: GCP Console → IAM & Admin → Settings → Project Number
  #
  # NO DEFAULT — must be set in terraform.tfvars every time (it's project-specific)
  description = <<-DESC
    GCP Project NUMBER (not the ID). Find it by running:
      gcloud projects describe plucky-sight-423703-k5 --format='value(projectNumber)'
    Example: 123456789012
  DESC
  type        = string
  # No default — you MUST set this in terraform.tfvars
}

variable "region" {
  # WHY NEEDED:
  #   Terraform's Google provider uses region as a default for regional resources.
  #   Our Compute Engine VM is zonal (more specific), but region is needed for context.
  #   us-central1 is the nearest major region with good connectivity to South Asia.
  description = "GCP region (e.g. us-central1)"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  # WHY NEEDED:
  #   GCP Compute Engine VMs are ZONAL resources — they exist within one specific zone.
  #   Zone must be within the chosen region (us-central1 → us-central1-a/b/c/f).
  #   us-central1-a has good VM availability and low latency for Iowa-based GCS operations.
  description = "GCP zone for the Compute Engine VM (e.g. us-central1-a)"
  type        = string
  default     = "us-central1-a"
}

# =============================================================================
# GCS BUCKET CONFIGURATION
# =============================================================================

variable "bucket_name" {
  # WHY NEEDED:
  #   The VM reads input tiles from this bucket and writes merged files back.
  #   Also used in IAM binding to grant the VM service account access.
  #   The bucket must already exist — Terraform does NOT create the bucket here.
  #   Create it manually: gsutil mb -l us-central1 gs://your-bucket-name
  description = "GCS bucket where Sentinel tiles are stored (must already exist)"
  type        = string
  default     = "aiq-river-imagery"
}

variable "input_prefix" {
  # WHY NEEDED:
  #   The path inside the bucket where raw GEE-exported Sentinel tiles are stored.
  #   Passed to vm_merge_gcs.py as --input-prefix.
  #   GEE exports to: gs://<bucket>/Sentinel/<river>_sentinel-000.tif
  # Example GCS paths:
  #   gs://aiq-river-imagery/Sentinel/Ganga_sentinel-000.tif
  #   gs://aiq-river-imagery/Sentinel/Yamuna_sentinel.tif
  description = "GCS folder (prefix) containing raw Sentinel tile .tif files"
  type        = string
  default     = "Sentinel"
}

variable "output_prefix" {
  # WHY NEEDED:
  #   Where the merged .tif files are uploaded after merge.
  #   Passed to vm_merge_gcs.py as --output-prefix.
  # Example output path:
  #   gs://aiq-river-imagery/Sentinel_Merged/Ganga_sentinel_merged.tif
  description = "GCS folder (prefix) where merged output .tif files are uploaded"
  type        = string
  default     = "Sentinel_Merged"
}

# =============================================================================
# VIRTUAL MACHINE CONFIGURATION
# =============================================================================

variable "vm_name" {
  # WHY NEEDED:
  #   The name given to the Compute Engine instance.
  #   Must be DNS-compatible (lowercase, letters/digits/hyphens, no underscores).
  #   Used in gcloud commands to monitor/kill the VM:
  #     gcloud compute instances describe river-merge-vm --zone=us-central1-a
  description = "Name of the Compute Engine instance to create"
  type        = string
  default     = "river-merge-vm"
}

variable "machine_type" {
  # WHY NEEDED:
  #   Determines CPU count and RAM for the VM.
  #   The tile merge is RAM-bound (large NumPy arrays) and somewhat CPU-bound.
  #
  # RECOMMENDATIONS:
  #   n2-highmem-4   →  4 CPU, 32 GB RAM  (~$0.24/hr)  ← RECOMMENDED default
  #     Best for: most rivers (Ganga, Yamuna, Krishna etc.)
  #     32 GB RAM easily handles 10 bands × 2048 rows chunk
  #
  #   n2-highmem-8   →  8 CPU, 64 GB RAM  (~$0.48/hr)
  #     Use for: very large rivers (Brahmaputra, Indus) with many tiles
  #     or if n2-highmem-4 runs out of memory (OOM kill)
  #
  #   n2-standard-4  →  4 CPU, 16 GB RAM  (~$0.19/hr)
  #     Use for: smaller rivers where RAM is not the bottleneck
  #     Risk: OOM kill for rivers with >1 row of tiles exceeding 16 GB canvas
  description = <<-DESC
    VM machine type. Options:
      n2-highmem-4  → 4 CPU, 32 GB RAM (~$0.24/hr) ← recommended
      n2-highmem-8  → 8 CPU, 64 GB RAM (~$0.48/hr) ← for large rivers
      n2-standard-4 → 4 CPU, 16 GB RAM (~$0.19/hr) ← cheaper, risk OOM
  DESC
  type        = string
  default     = "n2-highmem-4"
}

variable "disk_size_gb" {
  # WHY NEEDED:
  #   The VM needs local disk space to:
  #   1. Download raw tiles from GCS (can be 5–30 GB per river, multiple rivers batched)
  #   2. Write the merged output (can be 5–20 GB per river)
  #   3. Keep OS + Python packages (~3 GB)
  #
  # The script deletes tiles + merged file after each river is uploaded,
  # so only ONE river's data needs to fit at a time. 200 GB handles the
  # largest Indian rivers (Ganga, Brahmaputra) with headroom.
  #
  # pd-ssd type (set in main.tf) costs ~$0.17/GB/month = $34/month for 200 GB.
  # But you'll only have this for the merge duration (~30-60 minutes), so actual
  # disk cost is < $0.05 per merge session.
  description = "Boot disk size in GB. Must be large enough to hold tiles + merged output for one river at a time."
  type        = number
  default     = 200
}

variable "use_spot" {
  # WHY NEEDED:
  #   GCP Spot VMs are preemptible market-priced VMs that are:
  #   PROS: ~60-70% cheaper than standard VMs
  #   CONS: GCP can terminate them at any time with 30 seconds warning
  #         when it needs the capacity back. No warning = immediate termination.
  #
  # WHEN TO USE SPOT = true:
  #   - Short jobs (< 30 minutes): rarely preempted in practice
  #   - Single river merges: fast, low risk
  #   - Acceptable to re-run if preempted (merged files survive in GCS,
  #     so re-run skips already-merged rivers)
  #
  # WHEN TO USE SPOT = false (default):
  #   - Long multi-river batch runs (> 1 hour): preemption risk too high
  #   - Critical one-shot runs where you need guaranteed completion
  description = <<-DESC
    Use Spot (preemptible) VMs — ~60-70% cheaper, but GCP can kill them mid-merge.
    Set false (default) for safety. Set true for short single-river jobs to save cost.
  DESC
  type        = bool
  default     = false   # Safe default: pay full price for guaranteed completion
}

variable "selected_rivers" {
  # WHY NEEDED:
  #   Allows merging only specific rivers instead of everything in the bucket.
  #   Passed to vm_merge_gcs.py as: --rivers Ganga Yamuna Krishna
  #
  # HOW IT WORKS:
  #   Empty list [] (default) → merge ALL rivers found in gs://<bucket>/<input_prefix>/
  #   Non-empty list          → only merge the listed river names
  #
  # River names must match the TILE FILENAME PREFIXES in GCS.
  # E.g. if tiles are named "Ganga_sentinel-000.tif", the river name is "Ganga_sentinel".
  # The group key in vm_merge_gcs.py is derived from splitting on the first "-".
  description = "Specific river group names to merge. Leave empty to merge ALL rivers in the bucket."
  type        = list(string)
  default     = []   # Empty = merge everything
}
