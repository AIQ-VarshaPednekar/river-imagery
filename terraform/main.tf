# =============================================================================
# terraform/main.tf — River Sentinel Cloud Infrastructure
# =============================================================================
#
# PURPOSE:
#   This Terraform configuration provisions the Google Cloud Platform (GCP)
#   infrastructure needed to run the cloud VM tile merge job.
#
# WHAT IT CREATES:
#   1. IAM binding: Gives the VM's service account read/write access to the GCS bucket
#   2. IAM binding: Gives the VM's service account permission to delete itself
#   3. A GCP Compute Engine VM (Debian 12) that:
#      a) Installs rasterio and numpy
#      b) Downloads vm_merge_gcs.py from the GCS bucket
#      c) Merges Sentinel tiles from GCS → merged GeoTIFFs back to GCS
#      d) Self-deletes the VM when done (billing stops immediately)
#
# WHY TERRAFORM?
#   Terraform tracks what it has created in terraform.tfstate. This means:
#   • `terraform apply` is IDEMPOTENT: run it multiple times, it only creates
#     what doesn't already exist.
#   • `terraform destroy` removes EXACTLY what Terraform created: no hand-
#     crafted cleanup scripts needed.
#   • The startup script is version-controlled alongside the infrastructure.
#
# WORKFLOW:
#   1. Fill in terraform.tfvars with your project_id, project_number, etc.
#   2. Run: terraform init   (downloads Google provider plugin)
#   3. Run: terraform apply  (creates VM + IAM → VM boots + merges + self-deletes)
#   4. Optional: terraform destroy (cleans up IAM if needed — VM is already gone)
#
# TRIGGERED BY:
#   main.py /api/terraform/apply endpoint — runs terraform init + apply
#   main.py /api/terraform/destroy endpoint — runs terraform init + destroy
#
# AUTHENTICATION:
#   Terraform uses Google Application Default Credentials (ADC).
#   Run once on your machine: `gcloud auth application-default login`
#   No service account key file needed.

# =============================================================================
# TERRAFORM VERSION CONSTRAINT
# =============================================================================
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"  # Official Google Cloud provider from Terraform Registry
      version = "~> 5.0"            # Use any 5.x.x version (4.x deprecated many APIs)
    }
  }
}

# =============================================================================
# PROVIDER — tells Terraform which GCP project/region to use
# =============================================================================
# Auth: Application Default Credentials (ADC) — set once with:
#   gcloud auth application-default login
# No service account key needed — uses YOUR personal GCP account.
provider "google" {
  project = var.project_id   # Which GCP project to create resources in
  region  = var.region        # Default region (some resources need it)
  zone    = var.zone          # Default zone for Compute Engine resources
}

# =============================================================================
# IAM: Give the VM's service account read/write access to the GCS bucket
# =============================================================================
# BACKGROUND:
#   Every GCP project has a "default Compute Engine service account" with the email:
#     <project_number>-compute@developer.gserviceaccount.com
#   When we create the VM, it runs AS this service account.
#   Without this IAM binding, the VM cannot read tiles from or write to the GCS bucket.
#
# ROLE: roles/storage.objectAdmin
#   Grants: list objects, read objects, write objects, delete objects.
#   WHY objectAdmin instead of objectViewer?
#     The VM needs to UPLOAD merged files back to GCS (objectCreate/objectDelete).
#     objectViewer is read-only; we need write access.
#
# MEMBER: serviceAccount:<project_number>-compute@developer.gserviceaccount.com
#   This is the default Compute Engine service account email.
#   project_number (not project_id) is required for this email format.

resource "google_storage_bucket_iam_member" "compute_sa_bucket_access" {
  bucket = var.bucket_name   # The GCS bucket (e.g. "aiq-river-imagery")
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# =============================================================================
# IAM: Give the VM permission to delete itself when the merge completes
# =============================================================================
# WHY self-delete?
#   Running VMs cost money even when idle. The startup script runs:
#     gcloud compute instances delete "$INSTANCE" --zone="$ZONE" --quiet
#   as its final step. For this to work, the VM must have permission to delete
#   Compute Engine instances in the project.
#
# ROLE: roles/compute.instanceAdmin.v1
#   Grants full Compute Engine instance management: start, stop, delete.
#   This is broader than needed (ideally we'd grant only delete), but GCP
#   doesn't offer a finer-grained "delete-own-instance" role out of the box.
#
# SCOPE: Project-level (not just one VM) — the default Compute SA is project-scoped.

resource "google_project_iam_member" "compute_sa_self_delete" {
  project = var.project_id
  role    = "roles/compute.instanceAdmin.v1"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# =============================================================================
# LOCALS — computed values used in the VM resource below
# =============================================================================
locals {
  # Build the --rivers argument for vm_merge_gcs.py based on the selected_rivers variable.
  # If selected_rivers is empty (the default), don't pass --rivers → script merges ALL rivers.
  # If specific rivers are given, pass them as space-separated values.
  # Examples:
  #   rivers_arg = ""                     (merge all)
  #   rivers_arg = "--rivers Ganga Yamuna" (merge only Ganga and Yamuna)
  rivers_arg = length(var.selected_rivers) > 0 ? "--rivers ${join(" ", var.selected_rivers)}" : ""

  # ── Startup script that runs on the VM as root after first boot ─────────────
  # This is a heredoc (multi-line string). Terraform substitutes ${...} expressions.
  # The script is injected into the VM's metadata and executed by the OS agent
  # automatically on first boot. It does NOT need to be uploaded separately.
  #
  # set -e        → Exit immediately on any command failure
  # exec > >(tee) → Redirect all stdout+stderr to /var/log/river_merge.log AND terminal
  #   This enables reading logs via:
  #   gcloud compute instances get-serial-port-output river-merge-vm --zone=us-central1-a
  startup_script = <<-SCRIPT
    #!/bin/bash
    set -e
    exec > >(tee -a /var/log/river_merge.log) 2>&1

    echo "============================================"
    echo "  River Sentinel — Cloud VM Merge"
    echo "  $(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================"

    # ── Get own identity from GCE metadata server ──────────────────────────────
    # The GCE metadata service (169.254.169.254) provides VM identity info.
    # Used in the self-delete command at the end.
    # Metadata-Flavor: Google header is REQUIRED (rejects requests without it).
    INSTANCE=$(curl -sf -H "Metadata-Flavor: Google" \
      http://metadata.google.internal/computeMetadata/v1/instance/name)
    ZONE=$(curl -sf -H "Metadata-Flavor: Google" \
      http://metadata.google.internal/computeMetadata/v1/instance/zone \
      | awk -F/ '{print $NF}')
    # awk -F/ '{print $NF}' splits on "/" and prints the last field.
    # The zone is returned as "projects/123456/zones/us-central1-a" → extracts "us-central1-a"
    PROJECT=$(curl -sf -H "Metadata-Flavor: Google" \
      http://metadata.google.internal/computeMetadata/v1/project/project-id)

    echo "Instance : $INSTANCE"
    echo "Zone     : $ZONE"
    echo "Project  : $PROJECT"

    # ── Install Python dependencies ────────────────────────────────────────────
    # WHY apt-get for gdal/python3-dev?
    #   rasterio's pip wheel requires native GDAL libraries.
    #   On Debian 12, these are available as system packages.
    # WHY apt-get update first?
    #   GCE images are minimal; the package cache may be stale.
    # -qq = quiet mode (minimal output)
    # -y  = non-interactive yes to prompts
    echo ""
    echo "[1/4] Installing Python dependencies..."
    apt-get update -qq
    apt-get install -y -qq python3-pip python3-dev libgdal-dev gdal-bin
    python3 -m pip install --quiet --break-system-packages --upgrade pip
    # --break-system-packages: required on Debian 12 (PEP 668 prevents pip installs
    #   into system Python without this flag — it's safe here since it's a disposable VM)
    python3 -m pip install --quiet --break-system-packages rasterio numpy
    echo "✓ Dependencies ready"

    # ── Download vm_merge_gcs.py from the GCS bucket ──────────────────────────
    # WHY from GCS instead of bundling in the startup script?
    #   The startup script is limited to 256 KB. vm_merge_gcs.py is too large to inline.
    #   Storing it in GCS is the standard pattern for GCE startup scripts.
    # The user must upload vm_merge_gcs.py to gs://<bucket>/scripts/ manually (or via CI).
    echo ""
    echo "[2/4] Downloading merge script from GCS..."
    mkdir -p /tmp/river_merge
    gsutil cp gs://${var.bucket_name}/scripts/vm_merge_gcs.py /tmp/river_merge/vm_merge_gcs.py
    echo "✓ Script downloaded"

    # ── Run the merge ──────────────────────────────────────────────────────────
    # Passes the configured bucket, input/output prefixes, and optional river filter.
    # ${local.rivers_arg} is either "" (all rivers) or "--rivers Ganga Yamuna ..."
    echo ""
    echo "[3/4] Starting merge..."
    echo "  Bucket : ${var.bucket_name}"
    echo "  Input  : ${var.input_prefix}/"
    echo "  Output : ${var.output_prefix}/"
    echo ""

    python3 /tmp/river_merge/vm_merge_gcs.py \
      --bucket     "${var.bucket_name}"    \
      --input-prefix  "${var.input_prefix}"  \
      --output-prefix "${var.output_prefix}" \
      --work-dir   /tmp/river_merge        \
      ${local.rivers_arg}

    EXIT_CODE=$?   # Capture vm_merge_gcs.py's exit code (0=success, 1=failures)

    echo ""
    if [ $EXIT_CODE -eq 0 ]; then
      echo "✅ [3/4] All merges complete!"
    else
      echo "✗  [3/4] Merge finished with errors (exit code $EXIT_CODE)"
    fi

    # ── Self-delete the VM ─────────────────────────────────────────────────────
    # Whether the merge succeeded or failed, the VM self-deletes to stop billing.
    # --quiet: skip "Are you sure?" confirmation prompt (non-interactive mode)
    echo ""
    echo "[4/4] Self-deleting VM..."
    gcloud compute instances delete "$INSTANCE" \
      --zone="$ZONE"       \
      --project="$PROJECT" \
      --quiet

    echo "VM deleted. Goodbye!"
    SCRIPT
}

# =============================================================================
# THE MERGE VM — Compute Engine instance
# =============================================================================
resource "google_compute_instance" "river_merge_vm" {
  name         = var.vm_name       # e.g. "river-merge-vm" — DNS-safe name
  machine_type = var.machine_type  # n2-highmem-4: 4 CPU, 32 GB RAM
  zone         = var.zone          # us-central1-a (same zone as the GCS bucket for speed)

  # ── Boot disk ─────────────────────────────────────────────────────────────────
  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"  # Debian 12 (Bookworm): stable, small, apt-based
      size  = var.disk_size_gb          # 200 GB — needs to hold all tiles during merge
      type  = "pd-ssd"                  # Persistent SSD: faster read/write than pd-standard
                                        # Tile reads are I/O bound → SSD is worth the extra cost
    }
  }

  # ── Network: ephemeral public IP ──────────────────────────────────────────────
  network_interface {
    network = "default"   # Use the project's default VPC
    access_config {}      # Empty access_config → assigns an ephemeral (temporary) public IP
                          # Required for: pip install (PyPI), gsutil (GCS API), apt-get (Debian mirrors)
                          # Without this, the VM has no internet access and can't install packages.
  }

  # ── Startup script ────────────────────────────────────────────────────────────
  # Injected as VM instance metadata. GCE's built-in agent (google-startup-scripts)
  # reads the "startup-script" metadata key and executes it as root on first boot.
  metadata = {
    startup-script = local.startup_script   # The full bash script defined in locals above
  }

  # ── Service account + OAuth scopes ────────────────────────────────────────────
  # The VM runs as the project's default Compute Engine service account.
  # OAuth 2.0 scopes restrict what APIs the service account can access FROM THIS VM.
  # Even if the SA has broad IAM permissions, scopes further limit what the VM can do.
  service_account {
    email = "${var.project_number}-compute@developer.gserviceaccount.com"
    scopes = [
      "https://www.googleapis.com/auth/devstorage.full_control",  # Read/write GCS (download tiles, upload merged)
      "https://www.googleapis.com/auth/logging.write",             # Write to Cloud Logging (optional but useful)
      "https://www.googleapis.com/auth/compute",                   # Full Compute API (required for self-delete)
    ]
  }

  # ── VM scheduling (spot vs standard) ──────────────────────────────────────────
  # Spot VMs are ~60-70% cheaper but can be PREEMPTED (killed) by GCP when capacity is needed.
  # standard is more expensive but guaranteed to run until manually stopped.
  # var.use_spot controls this toggle.
  scheduling {
    preemptible         = var.use_spot          # True = Spot VM
    automatic_restart   = !var.use_spot         # Only standard VMs can auto-restart
    on_host_maintenance = var.use_spot ? "TERMINATE" : "MIGRATE"
    # TERMINATE: Spot VM is forcibly terminated when preempted (no migration)
    # MIGRATE: Standard VM is live-migrated during host maintenance (no downtime)
    provisioning_model  = var.use_spot ? "SPOT" : "STANDARD"
  }

  # ── Network tags ──────────────────────────────────────────────────────────────
  # Tags serve two purposes:
  #   1. Firewall rules can target VMs by tag (we don't need any extra firewall rules here)
  #   2. Can be used to identify and filter VMs in future automation
  tags = ["river-merge-vm"]

  # ── Lifecycle rule ─────────────────────────────────────────────────────────────
  # WHY ignore_changes = [metadata]?
  #   The startup script in metadata runs ONCE on first boot.
  #   After the VM self-deletes, terraform.tfstate still has the old metadata recorded.
  #   On the next `terraform apply`, Terraform would normally see the state mismatch
  #   and try to update the VM's metadata — but the VM no longer exists!
  #   ignore_changes = [metadata] tells Terraform to skip metadata drift detection.
  #   The next apply creates a FRESH VM with the current metadata (which is the intent).
  lifecycle {
    ignore_changes = [metadata]
  }
}

# =============================================================================
# OUTPUTS — values printed to the terminal after `terraform apply`
# =============================================================================
# Outputs make it easy for the user to grab the commands they need to monitor logs.

output "vm_name" {
  value       = google_compute_instance.river_merge_vm.name
  description = "VM name — use this in gcloud commands to monitor or kill the VM"
}

output "monitor_logs_command" {
  # Command to stream the VM's serial port output (same as /var/log/river_merge.log)
  # The serial port is available even before SSH is set up — useful for startup script logs
  value       = "gcloud compute instances get-serial-port-output ${var.vm_name} --zone=${var.zone} --project=${var.project_id}"
  description = "Run this command to stream VM logs in real-time"
}

output "watch_vm_command" {
  # Command to check if the VM is still running (returns: RUNNING, TERMINATED, or error if deleted)
  value       = "gcloud compute instances describe ${var.vm_name} --zone=${var.zone} --project=${var.project_id} --format='value(status)'"
  description = "Run this command to check if VM is still running"
}
