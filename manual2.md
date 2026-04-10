# Manual 2: How The Cloud Merge Works

This document explains the cloud merge setup in simple words:

1. How the process works end to end
2. Why we create 3 Terraform resources
3. What each resource does
4. How the VM deletes itself

---

## Big Picture

The project uses a temporary Google Cloud VM to merge Sentinel GeoTIFF tiles stored in Google Cloud Storage.

The flow is:

```text
Dashboard / FastAPI
    ->
creates a temporary VM in GCP
    ->
VM starts and runs a startup script
    ->
VM installs Python packages
    ->
VM downloads the merge script from GCS
    ->
VM reads tiles from gs://aiq-river-imagery/Sentinel/
    ->
VM merges tiles river by river
    ->
VM uploads results to gs://aiq-river-imagery/Sentinel_Merged/
    ->
VM deletes itself
```

So the VM is not permanent. It exists only for the merge job, then it removes itself to save cost.

---

## Where The Logic Lives

There are 2 important places:

- `main.py`
  This is the FastAPI app. It launches the VM when the UI button is clicked.

- `terraform/main.tf`
  This is the Infrastructure as Code version of the same setup. It describes the VM and required permissions using Terraform.

Even if the dashboard uses `gcloud` directly, the Terraform file shows the exact cloud design clearly.

---

## How The Process Works Step By Step

### 1. User starts merge

When the user clicks the cloud merge button, `main.py` calls:

```text
gcloud compute instances create river-merge-vm
```

This creates a VM named `river-merge-vm`.

Before creating it, the app checks whether the VM already exists. If it is already running, it does not create another one.

---

### 1A. Exact VM configuration

The VM is created with these main settings from `main.py` and `terraform/main.tf`:

- `name`: `river-merge-vm`
- `project`: `plucky-sight-423703-k5`
- `zone`: `us-central1-a`
- `machine type`: `n2-highmem-4`
- `disk size`: `200 GB`
- `disk type`: `pd-ssd`
- `OS image`: `debian-cloud/debian-12`
- `bucket`: `aiq-river-imagery`
- `input prefix`: `Sentinel`
- `output prefix`: `Sentinel_Merged`

### Why these values are used

#### VM name

`river-merge-vm` is a fixed name so the app can:

- check whether it already exists
- show logs for the same known instance name
- delete it reliably later

Using a fixed name keeps monitoring and cleanup simple.

#### Project

The project is:

```text
plucky-sight-423703-k5
```

This is the GCP project where the VM is created and where IAM permissions are applied.

#### Zone

The zone is:

```text
us-central1-a
```

This tells GCP where to physically run the VM.

Why this matters:

- the VM must run in some compute zone
- keeping compute near the bucket region is usually better for speed and cost
- the delete command also needs the zone to identify the exact VM

#### Machine type

The machine type is:

```text
n2-highmem-4
```

This means:

- 4 virtual CPUs
- high memory machine family
- about 32 GB RAM

Why this matters:

- raster merging can use a lot of memory
- large TIFF files can cause out-of-memory issues on smaller machines
- `rasterio` merging is much safer on a high-memory VM

So this VM is sized for heavy geospatial processing, not just lightweight scripting.

#### Disk size and type

The boot disk is:

- `200 GB`
- `pd-ssd`

Why this matters:

- the VM downloads tiles locally before merging
- merged TIFFs can be large
- temporary files need enough space during processing
- SSD is faster than standard disk for raster read/write operations

So the disk is not only for the OS. It also acts as working space for temporary merge data.

#### OS image

The OS image is:

```text
debian-cloud/debian-12
```

Why Debian 12 is used:

- stable Linux base
- easy package installation with `apt-get`
- works well for Python, GDAL, and raster tools
- fast and predictable for startup scripts

This makes it a practical choice for an automated worker VM.

---

### 2. The startup script runs automatically

When the VM boots, GCP automatically runs the startup script attached in metadata.

That script does this:

1. Gets its own VM name, zone, and project from GCP metadata
2. Installs required Python packages like `rasterio`, `numpy`, and GCS tools
3. Downloads `vm_merge_gcs.py` from the bucket
4. Runs the merge script
5. Deletes the VM when finished

This is why the system is automatic. Nobody has to log into the VM manually.

---

## Why A Service Account Is Attached

The VM is created with a service account identity. In Terraform it is the default Compute Engine service account:

```text
${project_number}-compute@developer.gserviceaccount.com
```

### What a service account means

A service account is the identity the VM uses when talking to Google Cloud APIs.

It is needed because the VM itself must call cloud services such as:

- Google Cloud Storage
- Compute Engine
- logging-related APIs

Without a service account, the VM would have no trusted identity inside GCP.

### Why the VM needs it

The VM must do real cloud actions:

- download `vm_merge_gcs.py` from the bucket
- read input TIFF files from the bucket
- upload merged TIFF files back to the bucket
- call Compute Engine to delete itself

All of those actions require authentication. The service account provides that authentication.

### Why use the default Compute Engine service account

This project uses the default compute service account because:

- it already exists in the project
- it is easy to attach to a VM
- Terraform and GCP handle it cleanly
- permissions can be added to it with IAM resources

So the service account is the VM's cloud identity.

---

## Why API Scopes Are Added

In the VM definition, scopes are attached along with the service account.

Terraform uses:

- `https://www.googleapis.com/auth/devstorage.full_control`
- `https://www.googleapis.com/auth/logging.write`
- `https://www.googleapis.com/auth/compute`

The FastAPI `gcloud create` path uses:

- `storage-full`
- `logging-write`
- `compute-rw`

These are the same idea written in two different forms.

### What scopes do

Scopes are an access filter for the VM's credentials.

Think of it like this:

- IAM role says what the identity is allowed to do
- scope says which APIs the VM token can request access to

Both are important.

### Why each scope is needed

#### Storage scope

Needed for:

- downloading the merge script from GCS
- reading raw tiles from the bucket
- uploading merged outputs

#### Logging scope

Needed so the VM can write logs that can be monitored while it runs.

#### Compute scope

Needed so the VM can call Compute Engine and delete itself at the end.

Without the compute scope, the self-delete command may not be able to obtain the right access token even if IAM is granted.

---

## How The Merge Script Is Uploaded

The file `vm_merge_gcs.py` is not baked into the VM image.

Instead, it is stored in the bucket here:

```text
gs://aiq-river-imagery/scripts/vm_merge_gcs.py
```

The one-time upload command documented in the project is:

```text
gsutil cp vm_merge_gcs.py gs://aiq-river-imagery/scripts/
```

### Why upload the script to GCS

This design is useful because:

- the VM can stay generic and lightweight
- the latest merge script can be updated without building a custom VM image
- every new VM downloads the current script version at startup
- the same script can be reused by any temporary VM

So GCS acts like a shared delivery location for the merge logic.

### How the VM uses it

During startup, the VM runs:

```bash
gsutil cp gs://aiq-river-imagery/scripts/vm_merge_gcs.py /tmp/river_merge/vm_merge_gcs.py
```

Then it runs:

```bash
python3 /tmp/river_merge/vm_merge_gcs.py ...
```

That means:

1. the script lives centrally in GCS
2. the VM downloads a local copy into `/tmp/river_merge`
3. Python executes that local copy

This keeps the VM fully automated.

---

### 3. The merge script processes raster tiles

The merge script:

1. Reads input tiles from `gs://aiq-river-imagery/Sentinel/`
2. Groups files by river name
3. Skips rivers that already have a merged output
4. Downloads only the needed tiles
5. Merges them into one GeoTIFF per river
6. Uploads the merged file to `gs://aiq-river-imagery/Sentinel_Merged/`
7. Cleans up local temp files

So input is raw tiles, and output is one merged raster per river.

---

## Why We Create 3 Resources

In `terraform/main.tf`, there are 3 Terraform `resource` blocks because the VM cannot work by itself. It needs both:

- the machine itself
- permission to access the bucket
- permission to delete itself

That is why we create 3 separate resources:

1. `google_storage_bucket_iam_member.compute_sa_bucket_access`
2. `google_project_iam_member.compute_sa_self_delete`
3. `google_compute_instance.river_merge_vm`

Each resource has a different job. Terraform keeps them separate because they represent different parts of the cloud setup.

---

## Resource 1: Bucket Access Permission

```hcl
resource "google_storage_bucket_iam_member" "compute_sa_bucket_access"
```

### What it does

This grants the VM's service account permission on the GCS bucket.

The role used is:

```text
roles/storage.objectAdmin
```

It grants that role on the specific bucket named by `var.bucket_name` to this member:

```text
serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com
```

### Why it is needed

Without this permission, the VM cannot:

- read tiles from `Sentinel/`
- download `scripts/vm_merge_gcs.py`
- upload merged files to `Sentinel_Merged/`

So this resource is what allows the VM to work with bucket files.

### Why this resource is separate

This resource only manages bucket access.

It does not:

- create the bucket
- create the VM
- grant compute deletion permission

Its job is only to bind one IAM role on one bucket to one identity.

That separation is important because bucket access is a different permission problem from compute access.

### In simple words

This resource says:

```text
"Allow the VM identity to read and write objects in this bucket."
```

---

## Resource 2: Self-Delete Permission

```hcl
resource "google_project_iam_member" "compute_sa_self_delete"
```

### What it does

This gives the VM's service account permission to manage compute instances.

The role used is:

```text
roles/compute.instanceAdmin.v1
```

It grants that project-level role to the same VM service account identity:

```text
serviceAccount:${var.project_number}-compute@developer.gserviceaccount.com
```

### Why it is needed

At the end of the startup script, the VM runs:

```bash
gcloud compute instances delete "$INSTANCE" --zone="$ZONE" --project="$PROJECT" --quiet
```

Without this IAM permission, that delete command would fail.

### Why this is project-level

Compute instances belong to the GCP project, not to the storage bucket.

So this permission must be granted at the project level.

That is why Terraform uses:

```hcl
google_project_iam_member
```

and not a bucket IAM resource.

### What would happen without it

If this role is missing:

- the merge itself may still finish
- the outputs may still upload correctly
- but the final delete command would return a permission error
- the VM would stay alive and continue costing money until manually deleted

### In simple words

This resource says:

```text
"Allow this VM to delete itself after finishing the merge."
```

---

## Resource 3: The Actual VM

```hcl
resource "google_compute_instance" "river_merge_vm"
```

### What it does

This creates the actual compute machine that performs the merge job.

It defines:

- VM name
- machine type
- zone
- boot disk
- OS image
- network
- service account
- API scopes
- startup script

### What metadata startup script means

The VM resource injects the startup script through metadata.

That means when GCP boots the VM for the first time, it automatically runs the script without needing:

- SSH login
- manual commands
- custom image baking

This is the automation layer that turns a plain VM into a self-running merge worker.

### Why it is needed

This is the worker machine. Without it, there is nothing to run the merge process.

### In simple words

This resource says:

```text
"Create one temporary VM with enough CPU, RAM, disk, and startup instructions to merge the raster files."
```

---

## Why Permissions Are Separate From The VM

Terraform keeps permissions and machine creation as separate resources because they are different cloud objects:

- one resource changes bucket IAM
- one resource changes project IAM
- one resource creates the VM

This is cleaner and easier to manage.

It also makes debugging easier:

- if bucket access fails, check the bucket IAM resource
- if self-delete fails, check the project IAM resource
- if the VM does not start, check the compute instance resource

---

## How Deletion Works

Deletion happens in 2 possible ways.

### 1. Normal delete: self-delete after success or completion

At the end of the startup script, the VM runs:

```bash
gcloud compute instances delete "$INSTANCE" \
  --zone="$ZONE" \
  --project="$PROJECT" \
  --quiet
```

This means:

- the VM finishes the merge script
- then it calls GCP to delete itself
- after that, the VM disappears from Compute Engine

This is the main automatic cleanup behavior.

### 2. Manual delete: emergency force delete

If the VM gets stuck, the app has a force-delete API:

```text
DELETE /api/vm/kill
```

That route runs:

```text
gcloud compute instances delete river-merge-vm --zone=us-central1-a --project=plucky-sight-423703-k5 --quiet
```

So if auto-delete does not happen, the user can still remove it manually.

---

## Important Detail: What Gets Deleted

Only the VM instance is deleted.

The following are not deleted:

- files in `gs://aiq-river-imagery/Sentinel/`
- files in `gs://aiq-river-imagery/Sentinel_Merged/`
- the IAM permission resources
- the Terraform code

So deletion removes the compute machine, not your bucket data.

---

## Why This Design Is Good

This design is useful because:

- the VM exists only when needed
- costs stay low
- heavy raster merging happens in the cloud, not on the local machine
- output files remain safe in GCS after the VM is gone
- the whole process can be repeated with a fresh VM each time

---

## Short Summary

The system creates 3 Terraform resources because one merge job needs 3 things:

1. permission to access the GCS bucket
2. permission to delete the VM
3. the VM itself

The VM starts, installs dependencies, downloads the merge script, merges raster tiles, uploads results, and finally deletes itself using `gcloud compute instances delete`.

That is the full reason for the 3 resources and how deletion works.
