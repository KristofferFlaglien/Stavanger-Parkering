"""
deploy_to_databricks.py

Deploys both Databricks notebooks and Lakeview dashboards.
- Notebooks are imported to /Shared/{filename}_prod
- Dashboards are updated if they exist, created if not.
"""

import os
import glob
import json
import subprocess
import requests
import sys

# Read Databricks credentials from environment
HOST = os.environ.get("DATABRICKS_HOST")
TOKEN = os.environ.get("DATABRICKS_TOKEN")

if not HOST or not TOKEN:
    print("❌ DATABRICKS_HOST or DATABRICKS_TOKEN not set in environment.")
    sys.exit(1)

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# --- Notebook deployment ---
def deploy_notebooks(notebooks_dir="notebooks"):
    notebook_files = glob.glob(f"{notebooks_dir}/*.ipynb")
    if not notebook_files:
        print("⚠️ No notebooks found to deploy.")
        return

    for nb in notebook_files:
        filename = os.path.basename(nb).replace(".ipynb", "")
        workspace_path = f"/Shared/{filename}_prod"
        print(f"Deploying notebook: {nb} -> {workspace_path}")
        try:
            # Use databricks CLI via subprocess
            subprocess.run(
                ["databricks", "workspace", "import", nb, workspace_path, "-f", "SOURCE", "-l", "PYTHON", "--overwrite"],
                check=True
            )
            print(f"✅ Notebook deployed: {filename}")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to deploy notebook {filename}: {e}")

# --- Dashboard deployment ---
def deploy_dashboards(dashboards_dir="dashboards"):
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

        # List dashboards
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

# --- Main ---
if __name__ == "__main__":
    deploy_notebooks()
    deploy_dashboards()
