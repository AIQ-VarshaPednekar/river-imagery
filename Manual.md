# River Sentinel — Cloud VM Merge
## Complete Setup & Usage Manual

---

## What This Does

When you click **☁ Launch & Merge** in the dashboard:

```
[Your Browser]
     ↓  click button
[FastAPI Dashboard]  →  gcloud compute instances create
     ↓
[GCP VM starts in us-central1-a]
     ↓  downloads tiles from bucket
[gs://aiq-river-imagery/Sentinel/]
     ↓  merges tiles by river name
     ↓  uploads merged TIFFs back
[gs://aiq-river-imagery/Sentinel_Merged/]
     ↓  VM self-deletes
[Dashboard shows ✅ Done]
```

**No Terraform, no manual steps — one click does everything.**

---

## Where to See the VM Running in GCP Console

### Option 1 — GCP Console (visual)

1. Go to → https://console.cloud.google.com
2. Sign in with `varshapednekar136@gmail.com`
3. Select project → **plucky-sight-423703-k5**
4. Left menu → **Compute Engine** → **VM Instances**
5. You will see `river-merge-vm` with status **RUNNING** (green dot)
6. When merge finishes → VM disappears from the list (self-deleted ✅)

### Option 2 — Watch VM logs in real time (terminal)

```cmd
gcloud compute instances get-serial-port-output river-merge-vm --zone=us-central1-a --project=plucky-sight-423703-k5
```

Run this while VM is running to see exactly what it's doing:
```
[1/4] Installing dependencies...
[2/4] Downloading merge script from GCS...
[3/4] Running merge...
  Merging 4 tiles for: Tapi
  ✅ Merged → 1.24 GB
  ↑ Uploading to gs://aiq-river-imagery/Sentinel_Merged/Tapi_merged.tif
[4/4] Self-deleting VM...
```

### Option 3 — Check VM status in terminal

```cmd
gcloud compute instances describe river-merge-vm --zone=us-central1-a --format="value(status)"
```

Returns: `RUNNING` while active, error if deleted (= merge done).

### Option 4 — See merged output in bucket

```cmd
gsutil ls gs://aiq-river-imagery/Sentinel_Merged/
```

---

## Where Each File Lives

```
river project aiq/
├── main.py                      ← FastAPI backend (has /api/vm/launch route)
├── config.json                  ← Pipeline settings
├── templates/
│   └── index.html               ← Dashboard UI (has ☁ Launch & Merge button)
├── scripts/
│   └── vm_merge_gcs.py          ← Merge logic that runs ON the VM
├── runners/
│   ├── run_step1.py             ← GEE export runner
│   ├── run_step2.py             ← Drive download runner
│   └── run_step3.py             ← Local merge runner
└── terraform/                   ← HCL infrastructure code (manual use only)
    ├── main.tf
    ├── variables.tf
    └── terraform.tfvars

GCS Bucket (gs://aiq-river-imagery/)
├── Sentinel/                    ← Raw tiles from GEE export (INPUT)
│   ├── Tapi-00000000.tif
│   ├── Tapi-00000001.tif
│   └── ...
├── Sentinel_Merged/             ← Merged output (OUTPUT)
│   ├── Tapi_merged.tif
│   └── ...
└── scripts/
    └── vm_merge_gcs.py          ← VM downloads this at startup
```

---

## What the HCL (Terraform) Code Does

> **Note:** Terraform is NOT required for the button to work.
> The button uses `gcloud` directly via FastAPI.
> The HCL files are provided for:
> - Understanding the infrastructure as code
> - Manual use if you want to run the VM outside the dashboard
> - Reference for future deployments or multiple environments

### What is HCL?

HCL = **HashiCorp Configuration Language**. It's the language Terraform uses to describe cloud infrastructure. Instead of clicking around in the GCP Console, you write what you want and Terraform creates it.

```hcl
# This says "create a VM called river-merge-vm"
resource "google_compute_instance" "river_merge_vm" {
  name         = "river-merge-vm"
  machine_type = "n2-highmem-4"
  zone         = "us-central1-a"
  ...
}
```

### What our `main.tf` defines

| Block | What it does |
|---|---|
| `provider "google"` | Tells Terraform to use GCP, auth with your gcloud credentials |
| `google_storage_bucket_iam_member` | Grants the VM's identity read/write on your GCS bucket |
| `google_project_iam_member` | Grants the VM permission to delete itself |
| `google_compute_instance` | Defines the actual VM — machine type, disk, startup script, scopes |
| `locals { startup_script }` | The bash script that runs on the VM at first boot |
| `output` blocks | Prints useful commands after `terraform apply` |

### What our `variables.tf` defines

All configurable values extracted from `main.tf` so you don't hardcode them:

| Variable | Your value | Purpose |
|---|---|---|
| `project_id` | `plucky-sight-423703-k5` | GCP project |
| `project_number` | `196558636735` | Used to identify default Compute SA |
| `zone` | `us-central1-a` | Same region as your bucket |
| `machine_type` | `n2-highmem-4` | 4 CPU / 32 GB RAM for rasterio |
| `disk_size_gb` | `200` | Temp space for tile download |
| `use_spot` | `false` | Set `true` for ~60% cost savings |
| `selected_rivers` | `[]` | Empty = merge all, or `["Tapi"]` |

### How to use Terraform manually (optional)

If you ever want to launch the VM without the dashboard:

```cmd
cd "C:\Users\My Pc\Documents\river project aiq\terraform"

# First time only
terraform init

# Create the VM
terraform apply

# The VM self-deletes, so no destroy needed.
# But if it gets stuck, force delete:
gcloud compute instances delete river-merge-vm --zone=us-central1-a --quiet
```

---

## What the VM Merge Script Does (vm_merge_gcs.py)

This Python script runs **inside the VM**, not on your machine.

```
1. Scan gs://aiq-river-imagery/Sentinel/ for all .tif files
2. Group by river name:
      Tapi-00000000.tif  ┐
      Tapi-00000001.tif  ├─→ river: "Tapi"
      Tapi-00000002.tif  ┘
      Narmada-00000000.tif ─→ river: "Narmada"

3. For each river:
   a. Check if Sentinel_Merged/RiverName_merged.tif already exists
      → YES: skip (don't re-merge)
      → NO:  continue

   b. Download all tiles for this river to /tmp/river_merge/input/
   c. Validate each tile (skip corrupted ones)
   d. Merge using windowed chunked I/O:
      - Canvas of NODATA (-9999) filled row by row
      - 2048 rows per chunk (memory-safe, won't OOM)
      - LZW compression, tiled GeoTIFF, BigTIFF support
   e. Upload merged file to gs://aiq-river-imagery/Sentinel_Merged/
   f. Delete local temp files

4. Print summary: merged / skipped / failed
5. Exit (startup script then self-deletes the VM)
```

---

## GCP Setup Done (One-time, Already Completed)

| Step | Command | Status |
|---|---|---|
| Install gcloud CLI | Downloaded installer | ✅ Done |
| Authenticate user account | `gcloud auth login` | ✅ Done |
| Authenticate ADC | `gcloud auth application-default login` | ✅ Done |
| Set project | `gcloud config set project plucky-sight-423703-k5` | ✅ Done |
| Enable APIs | `gcloud services enable compute.googleapis.com storage.googleapis.com` | ✅ Done |
| Grant bucket access | `gsutil iam ch serviceAccount:196558636735-compute@...` | ✅ Done |
| Grant self-delete | `gcloud projects add-iam-policy-binding ... roles/compute.instanceAdmin.v1` | ✅ Done |
| Upload merge script | `gsutil cp vm_merge_gcs.py gs://aiq-river-imagery/scripts/` | ✅ Done |

---

## VM Specifications

| Property | Value |
|---|---|
| Name | `river-merge-vm` |
| Machine | `n2-highmem-4` |
| CPU | 4 vCPU |
| RAM | 32 GB |
| Disk | 200 GB SSD |
| OS | Debian 12 |
| Zone | `us-central1-a` |
| Auth | Default Compute SA (`196558636735-compute@developer.gserviceaccount.com`) |
| Cost | ~$0.24/hr (auto-deleted, usually runs 10–30 min) |

---

## Troubleshooting

### VM created but nothing in Sentinel_Merged/ after 30 min

View the VM's serial port logs:
```cmd
gcloud compute instances get-serial-port-output river-merge-vm --zone=us-central1-a
```

### "VM already running" error when clicking button

VM from a previous run got stuck. Force-delete it:
```cmd
gcloud compute instances delete river-merge-vm --zone=us-central1-a --quiet
```

### No tiles found in bucket

Check what's in the bucket:
```cmd
gsutil ls gs://aiq-river-imagery/Sentinel/
```

Tiles must be named like `RiverName-00000000.tif` (GEE export format).

### `gcloud` not recognized in terminal

Close terminal, reopen. If still failing, re-add to PATH:
`C:\Users\My Pc\AppData\Local\Google\Cloud SDK\bin`

### "403 Permission denied" on bucket

Re-run the IAM grant:
```cmd
gsutil iam ch serviceAccount:196558636735-compute@developer.gserviceaccount.com:objectAdmin gs://aiq-river-imagery
```

---

## Full Flow Summary

```
GEE Export (Step 1)
    ↓  tiles exported to Google Drive
Drive Download (Step 2)
    ↓  tiles downloaded locally to Imagery_Output/Sentinel/
    
        — OR if export_target = "gcs" —

GEE Export (Step 1)
    ↓  tiles exported directly to gs://aiq-river-imagery/Sentinel/
Cloud VM Merge (☁ button)
    ↓  VM merges tiles in bucket, uploads Sentinel_Merged/, self-deletes

Local Merge (Step 3) — alternative to Cloud VM Merge
    ↓  merges tiles on your local machine (slower, no VM needed)
```

---

*River Sentinel — AIQ Space Ventures | Project: plucky-sight-423703-k5*