Step-by-step guide
PHASE 1 — One-time GCP setup (do this once)
1. Enable required APIs — run in your terminal:
    gcloud config set project plucky-sight-423703-k5
    gcloud services enable compute.googleapis.com storage.googleapis.com
2. Authenticate (this is the "use my account, not service account" part):
    gcloud auth login
    gcloud auth application-default login
3. Get your project number (needed for Terraform):
    gcloud projects describe plucky-sight-423703-k5 --format='value(projectNumber)'
Copy the number — you'll put it in terraform.tfvars.
4. Grant the default Compute SA permission to access your bucket — this is the VM's identity, and it's the default one GCP creates automatically (not a custom service account):
    PROJECT_NUMBER=$(gcloud projects describe plucky-sight-423703-k5 --format='value(projectNumber)')
    gsutil iam ch serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com:objectAdmin gs://aiq-river-imagery
PHASE 2 — Upload the merge script to your bucket
The VM downloads vm_merge_gcs.py from GCS at startup:
    gsutil cp vm_merge_gcs.py gs://aiq-river-imagery/scripts/vm_merge_gcs.py
PHASE 3 — Terraform setup (for the HCL approach)
    mkdir terraform && cd terraform
    # copy main.tf and variables.tf here
    cp terraform.tfvars.example terraform.tfvars
    # edit terraform.tfvars → fill in project_number
    terraform init
    terraform apply    # this creates the VM
The VM will run, merge, self-delete. Terraform state becomes stale after that — that's fine. Next time you click the button, it creates a fresh VM.

Install Google Cloud CLI on Windows
1. Download the installer:
Go to → https://cloud.google.com/sdk/docs/install-sdk#windows
    gcloud --version
5. Then run these in order:
    gcloud auth login
→ Browser opens → log in with your AIQ Google account
    gcloud auth application-default login
→ Browser opens again → same account → this is what FastAPI uses
    gcloud config set project plucky-sight-423703-k5
    gcloud projects describe plucky-sight-423703-k5 --format="value(projectNumber)"
→ Copy that number, you'll need it
Type 9 and press Enter — that picks us-central1-a, which matches your bucket region.
Please enter numeric choice: 9
Then after it finishes, run:
    gcloud auth application-default login
→ Browser opens → sign in with same account → come back after that's done.

Now continue:
Step 1 — Grant self-delete permission (so VM can delete itself when done):
    gcloud projects add-iam-policy-binding plucky-sight-423703-k5 --member="serviceAccount:196558636735-compute@developer.gserviceaccount.com" --role="roles/compute.instanceAdmin.v1"
Step 2 — Upload the merge script to your bucket:
    gsutil cp "C:\Users\My Pc\Documents\river project aiq\scripts\vm_merge_gcs.py" gs://aiq-river-imagery/scripts/vm_merge_gcs.py
(Put the downloaded vm_merge_gcs.py in your scripts/ folder first)
Step 3 — Fill in your terraform.tfvars (you already have the right zone, just fix the project number):
    project_id     = "plucky-sight-423703-k5"
    project_number = "196558636735"
    region         = "us-central1"
    zone           = "us-central1-a"
Step 4 — Update VM_CONFIG in main.py (change the zone):
    VM_CONFIG = {
    "project_id":    "plucky-sight-423703-k5",
    "zone":          "us-central1-a",       # ← changed from asia-south1-a
    ...
}
Step 5 — Install Terraform on Windows:
Go to → https://developer.hashicorp.com/terraform/install#windows
Download the zip, extract terraform.exe, and put it somewhere like C:\terraform\. Then add that folder to your PATH:

Search "Environment Variables" in Start → Edit System Environment Variables → Path → New → C:\terraform

Verify:
    terraform --version
Step 6 — Run Terraform:
    cd "C:\Users\My Pc\Documents\river project aiq\terraform"
    terraform init
    terraform apply



docker compose up --build -d
docker compose down
 Authenticate inside the container Your code runs inside the container, which means it doesn't automatically have access to your Windows Google Cloud credentials. You need to log in inside the container once so Terraform and GEE work:
    docker exec -it river-sentinel-dashboard gcloud auth application-default login