terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# ─── Provider ─────────────────────────────────────────────────────────────────
# Auth: run `gcloud auth application-default login` once on your machine.
# No service account key needed — uses your personal GCP account.
provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# ─── IAM: give default Compute SA access to the GCS bucket ───────────────────
# The VM uses the project's default Compute Engine service account.
# We grant it only what it needs: read/write to YOUR bucket.
resource "google_storage_bucket_iam_member" "compute_sa_bucket_access" {
  bucket = var.bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# Needed so the VM can delete itself when done
resource "google_project_iam_member" "compute_sa_self_delete" {
  project = var.project_id
  role    = "roles/compute.instanceAdmin.v1"
  member  = "serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com"
}

# ─── Startup script (runs on VM as root) ─────────────────────────────────────
locals {
  # Build --rivers arg only if specific rivers are given
  rivers_arg = length(var.selected_rivers) > 0 ? "--rivers ${join(" ", var.selected_rivers)}" : ""

  startup_script = <<-SCRIPT
    #!/bin/bash
    set -e
    exec > >(tee -a /var/log/river_merge.log) 2>&1

    echo "============================================"
    echo "  River Sentinel — Cloud VM Merge"
    echo "  $(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================"

    # ── Get own identity from metadata ──────────────────────────────────────
    INSTANCE=$(curl -sf -H "Metadata-Flavor: Google" \
      http://metadata.google.internal/computeMetadata/v1/instance/name)
    ZONE=$(curl -sf -H "Metadata-Flavor: Google" \
      http://metadata.google.internal/computeMetadata/v1/instance/zone \
      | awk -F/ '{print $NF}')
    PROJECT=$(curl -sf -H "Metadata-Flavor: Google" \
      http://metadata.google.internal/computeMetadata/v1/project/project-id)

    echo "Instance : $INSTANCE"
    echo "Zone     : $ZONE"
    echo "Project  : $PROJECT"

    # ── Install Python dependencies ──────────────────────────────────────────
    echo ""
    echo "[1/4] Installing Python dependencies..."
    apt-get update -qq
    apt-get install -y -qq python3-pip python3-dev libgdal-dev gdal-bin
    python3 -m pip install --quiet --break-system-packages --upgrade pip
    python3 -m pip install --quiet --break-system-packages rasterio numpy
    echo "✓ Dependencies ready"

    # ── Download merge script from bucket ───────────────────────────────────
    echo ""
    echo "[2/4] Downloading merge script from GCS..."
    mkdir -p /tmp/river_merge
    gsutil cp gs://${var.bucket_name}/scripts/vm_merge_gcs.py /tmp/river_merge/vm_merge_gcs.py
    echo "✓ Script downloaded"

    # ── Run the merge ────────────────────────────────────────────────────────
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

    EXIT_CODE=$?

    echo ""
    if [ $EXIT_CODE -eq 0 ]; then
      echo "✅ [3/4] All merges complete!"
    else
      echo "✗  [3/4] Merge finished with errors (exit code $EXIT_CODE)"
    fi

    # ── Self-delete ──────────────────────────────────────────────────────────
    echo ""
    echo "[4/4] Self-deleting VM..."
    gcloud compute instances delete "$INSTANCE" \
      --zone="$ZONE"       \
      --project="$PROJECT" \
      --quiet

    echo "VM deleted. Goodbye!"
    SCRIPT
}

# ─── The merge VM ─────────────────────────────────────────────────────────────
resource "google_compute_instance" "river_merge_vm" {
  name         = var.vm_name
  machine_type = var.machine_type
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"   # Debian 12 — stable, fast boot
      size  = var.disk_size_gb
      type  = "pd-ssd"                   # SSD for fast tile I/O
    }
  }

  network_interface {
    network = "default"
    access_config {}                     # Ephemeral public IP (for pip install / gsutil)
  }

  # Startup script injected here — runs automatically on first boot
  metadata = {
    startup-script = local.startup_script
  }

  # Default Compute Engine service account — already exists in every GCP project
  service_account {
    email = "${var.project_number}-compute@developer.gserviceaccount.com"
    scopes = [
      "https://www.googleapis.com/auth/devstorage.full_control",  # GCS
      "https://www.googleapis.com/auth/logging.write",             # Cloud Logging
      "https://www.googleapis.com/auth/compute",                   # Self-delete
    ]
  }

  scheduling {
    preemptible         = var.use_spot
    automatic_restart   = !var.use_spot
    on_host_maintenance = var.use_spot ? "TERMINATE" : "MIGRATE"
    provisioning_model  = var.use_spot ? "SPOT" : "STANDARD"
  }

  tags = ["river-merge-vm"]

  # Important: once VM self-deletes, Terraform state is stale.
  # Next apply will create a fresh VM — this is intentional.
  lifecycle {
    ignore_changes = [metadata]
  }
}

# ─── Outputs ─────────────────────────────────────────────────────────────────
output "vm_name" {
  value       = google_compute_instance.river_merge_vm.name
  description = "VM name — use to monitor logs"
}

output "monitor_logs_command" {
  value       = "gcloud compute instances get-serial-port-output ${var.vm_name} --zone=${var.zone} --project=${var.project_id}"
  description = "Run this to stream VM logs in real-time"
}

output "watch_vm_command" {
  value       = "gcloud compute instances describe ${var.vm_name} --zone=${var.zone} --project=${var.project_id} --format='value(status)'"
  description = "Run this to check if VM is still running"
}
