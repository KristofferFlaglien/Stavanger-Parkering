"""
deploy_to_databricks.py

Deploys Databricks notebooks, Lakeview dashboards, and Jobs in an idempotent way:
- Notebooks: imported to /Shared/{filename}_prod, overwriting existing ones.
- Dashboards: updates existing dashboards or creates new ones if they don't exist.
- Jobs: updates existing jobs or creates new ones if they don't exist.
"""

from pathlib import Path
import os
import glob
import json
import subprocess
import requests
import sys

# -------------------------------
# Read environment variables for authentication
# -------------------------------
# These environment variables must be set before running the script.
HOST = os.environ.get("DATABRICKS_HOST")  # Databricks workspace URL
TOKEN = os.environ.get("DATABRICKS_TOKEN")  # Personal access token for authentication

# Exit if credentials are missing
if not HOST or not TOKEN:
    print("❌ DATABRICKS_HOST or DATABRICKS_TOKEN not set in environment.")
    sys.exit(1)

# Headers used for API requests to Databricks
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# -------------------------------
# Function: Deploy Notebooks
# -------------------------------
def deploy_notebooks(notebooks_dir="notebooks"):
    """
    Deploy all notebooks from notebooks_dir to /Shared/{filename}_prod.
    Uses the Databricks CLI to import notebooks.
    """
    notebook_files = glob.glob(f"{notebooks_dir}/*.ipynb")

    if not notebook_files:
        print("⚠️ No notebooks found to deploy.")
        return

    for nb in notebook_files:
        filename = os.path.basename(nb).replace(".ipynb", "")
        workspace_path = f"/Shared/{filename}_prod"
        print(f"Deploying notebook: {nb} -> {workspace_path}")
        try:
            subprocess.run(
                ["databricks", "workspace", "import", nb, workspace_path, "-f", "SOURCE", "-l", "PYTHON", "--overwrite"],
                check=True
            )
            print(f"✅ Notebook deployed: {filename}")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to deploy notebook {filename}: {e}")

# -------------------------------
# Function: Deploy Dashboards
# -------------------------------
def deploy_dashboards(dashboards_dir="dashboards"):
    """
    Deploy all Lakeview dashboards from dashboards_dir to /Shared.
    Updates existing dashboards if found; otherwise, creates new dashboards.
    """
    dashboard_files = glob.glob(f"{dashboards_dir}/*.lvdash.json")

    if not dashboard_files:
        print("⚠️ No dashboards found to deploy.")
        return

    for dashboard_file in dashboard_files:
        print(f"Processing dashboard: {dashboard_file}")
        with open(dashboard_file, "r") as f:
            data = json.load(f)

        clean_name = os.path.splitext(os.path.splitext(os.path.basename(dashboard_file))[0])[0]
        data["display_name"] = clean_name
        data["parent_path"] = "/Shared"

        try:
            resp = requests.get(f"{HOST}/api/2.0/lakeview/dashboards", headers=HEADERS)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"⚠️ Failed to list dashboards: {e}")
            continue
        dashboards = resp.json().get("dashboards", [])
        existing = next((d for d in dashboards if d.get("display_name") == clean_name), None)

        if existing:
            dash_id = existing.get("dashboard_id")
            if not dash_id:
                print(f"⚠️ Found dashboard {clean_name} but no dashboard_id. Skipping.")
                continue

            update_url = f"{HOST}/api/2.0/lakeview/dashboards/{dash_id}"
            try:
                update_resp = requests.patch(update_url, headers=HEADERS, json=data)
                if update_resp.status_code == 200:
                    print(f"✅ Updated dashboard: {clean_name}")
                else:
                    print(f"❌ Failed to update {clean_name}: {update_resp.status_code} {update_resp.text}")
            except requests.RequestException as e:
                print(f"❌ Exception during update {clean_name}: {e}")

        else:
            create_url = f"{HOST}/api/2.0/lakeview/dashboards"
            try:
                create_resp = requests.post(create_url, headers=HEADERS, json=data)
                if create_resp.status_code in [200, 201]:
                    print(f"✅ Created dashboard: {clean_name}")
                else:
                    print(f"❌ Failed to create {clean_name}: {create_resp.status_code} {create_resp.text}")
            except requests.RequestException as e:
                print(f"❌ Exception during creation {clean_name}: {e}")


# -------------------------------
# Function: Deploy Jobs
# -------------------------------              
def deploy_jobs():
    """
    Deploys Databricks Jobs defined in JSON files (stored in ./jobs/ directory)
    into the Databricks workspace using the Jobs REST API.

    This function does three things:
      1. Reads each job definition from your repo (Git is the source of truth).
      2. Checks if a job with the same name already exists in Databricks.
      3. Either updates the existing job OR creates a new one if none exists.

    That way, your production Databricks jobs always match what’s defined in Git.
    """

    jobs_dir = Path("./jobs")
    if not jobs_dir.exists():
        print("ℹ️ No jobs directory found, skipping job deployment")
        return

    for job_file in jobs_dir.glob("*.json"):
        print(f"Processing job definition: {job_file}")

        with open(job_file, "r", encoding="utf-8") as f:
            job_def = json.load(f)

        job_name = job_def.get("name")
        if not job_name:
            print(f"⚠️ Skipping {job_file}, missing 'name' in job definition")
            continue

        # ------------------------------------------------------------
        # 1. Check if a job with the same name already exists in Databricks
        # ------------------------------------------------------------
        resp = requests.get(
            f"{HOST}/api/2.1/jobs/list",  # 👈 Use HOST instead of DATABRICKS_HOST
            headers=HEADERS,
        )
        resp.raise_for_status()
        jobs_list = resp.json().get("jobs", [])

        existing_job = next((j for j in jobs_list if j["settings"]["name"] == job_name), None)

        # ------------------------------------------------------------
        # 2. Update existing job if found
        # ------------------------------------------------------------
        if existing_job:
            job_id = existing_job["job_id"]
            print(f"🔄 Updating existing job: {job_name} (id={job_id})")

            update_payload = {
                "job_id": job_id,
                "new_settings": job_def
            }
            resp = requests.post(
                f"{HOST}/api/2.1/jobs/update",
                headers=HEADERS,
                data=json.dumps(update_payload),
            )
            resp.raise_for_status()
            print(f"✅ Job updated: {job_name}")

        # ------------------------------------------------------------
        # 3. Otherwise, create a new job
        # ------------------------------------------------------------
        else:
            print(f"➕ Creating new job: {job_name}")
            resp = requests.post(
                f"{HOST}/api/2.1/jobs/create",
                headers=HEADERS,
                data=json.dumps(job_def),
            )
            resp.raise_for_status()
            print(f"✅ Job created: {job_name}")


# -------------------------------
# Main execution
# -------------------------------
if __name__ == "__main__":
    deploy_notebooks()    # Deploy notebooks first
    deploy_dashboards()   # Then deploy dashboards
    deploy_jobs()         # Finally, deploy jobs

