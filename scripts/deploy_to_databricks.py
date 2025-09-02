# deploy_to_databricks.py
#
# This script automates the deployment of Databricks notebooks, Lakeview dashboards, and Jobs.
# It uses an "idempotent" approach, meaning it can be run multiple times without causing
# unintended side effects. It either creates new assets or updates existing ones.
#
# - Notebooks: Imported and overwrite existing notebooks.
# - Dashboards: Updated if a matching name is found, otherwise a new one is created.
# - Jobs: Updated if a matching name is found, otherwise a new one is created.
#
# -------------------------------
# Import necessary libraries
# -------------------------------
# Path allows for working with file system paths in an object-oriented way.
from pathlib import Path
# os provides functions for interacting with the operating system, like accessing environment variables.
import os
# glob finds all pathnames matching a specified pattern (e.g., all files with a .ipynb extension).
import glob
# json is used for parsing JSON data, which is the format for dashboards and job definitions.
import json
# subprocess allows for running external commands, which is used to call the Databricks CLI.
import subprocess
# requests is a powerful library for making HTTP requests to external APIs.
import requests
# sys provides access to system-specific parameters and functions, such as exiting the script.
import sys

# -------------------------------
# Read environment variables for authentication
# -------------------------------
# The script requires these two environment variables to be set for authentication.
# Databricks workspace URL (e.g., https://<your-workspace-name>.databricks.com)
HOST = os.environ.get("DATABRICKS_HOST")
# Databricks Personal Access Token for API authentication.
TOKEN = os.environ.get("DATABRICKS_TOKEN")

# Exit if credentials are not found. This is a crucial security and stability check.
if not HOST or not TOKEN:
    print("❌ DATABRICKS_HOST or DATABRICKS_TOKEN not set in environment.")
    sys.exit(1)

# Set up the standard headers for all API requests.
# The Authorization header uses a Bearer token for authentication.
# The Content-Type header specifies that the request body will be JSON.
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}


# -------------------------------
# Function: Deploy Dashboards
# -------------------------------
def deploy_dashboards(dashboards_dir="dashboards"):
    # This is the main function to deploy Lakeview dashboards.
    # It reads dashboard definitions from JSON files, checks if they exist in Databricks,
    # and either updates an existing one or creates a new one.

    # 1. Find all dashboard definition files in the specified directory.
    # The glob module searches for file paths matching a pattern.
    # Here, it finds all files ending with `.lvdash.json`.
    dashboard_files = glob.glob(f"{dashboards_dir}/*.lvdash.json")
    
    # 2. If no files are found, print a warning and exit the function.
    if not dashboard_files:
        print("⚠️ No dashboards found.")
        return

    # 3. Create a requests Session object.
    # A Session object reuses the same TCP connection for all requests,
    # which is more efficient for multiple API calls.
    session = requests.Session()
    # Apply the standard headers (Authorization, Content-Type) to the session.
    session.headers.update(HEADERS)

    # 4. Define a helper function for making API requests.
    # This centralizes error handling and makes the main code cleaner.
    def safe_request(method, url, **kwargs):
        try:
            # Use the session to send an HTTP request.
            resp = session.request(method, url, **kwargs)
            # Check if the request was successful (status code 2xx).
            # If not, it raises an HTTPError.
            resp.raise_for_status()
            # If successful, return the response object.
            return resp
        except requests.RequestException as e:
            # Catch any requests-related errors (e.g., network issues, bad status codes)
            # and print a descriptive error message.
            print(f"❌ {method.upper()} {url} failed: {e}")
            # Return None to indicate a failure.
            return None

    # 5. Fetch all existing dashboards from Databricks in a single API call.
    # This is a key optimization to avoid repeatedly calling the API inside the loop.
    resp = safe_request("get", f"{HOST}/api/2.0/lakeview/dashboards")
    # Parse the JSON response. If the request failed (resp is None), default to an empty list.
    dashboards = resp.json().get("dashboards", []) if resp else []

    # 6. Loop through each dashboard file found in the local directory.
    for dashboard_file in dashboard_files:
        # Extract a clean name from the filename by removing the `.lvdash.json` suffix.
        clean_name = os.path.basename(dashboard_file).replace(".lvdash.json", "")
        
        # Open and load the JSON content of the dashboard definition file.
        with open(dashboard_file, "r") as f:
            data = json.load(f)

        # Update the dashboard data with the correct display name and parent path.
        data.update({"display_name": clean_name, "parent_path": "/Shared"})

        # 7. Check if a dashboard with the same display name already exists.
        # The `next()` function efficiently finds the first matching item in the list.
        # The generator expression `(d for ...)` creates an iterator, so it stops searching once a match is found.
        existing = next((d for d in dashboards if d.get("display_name") == clean_name), None)

        # 8. Idempotent logic: Update if found, otherwise create.
        if existing and existing.get("dashboard_id"):
            # If an existing dashboard with a valid ID is found, prepare to update it.
            url = f"{HOST}/api/2.0/lakeview/dashboards/{existing['dashboard_id']}"
            # Send a PATCH request with the updated data. PATCH is used for partial updates.
            resp = safe_request("patch", url, json=data)
            # Print a status message based on whether the request was successful.
            print(f"✅ Updated {clean_name}" if resp else f"❌ Failed to update {clean_name}")
        else:
            # If no existing dashboard is found, prepare to create a new one.
            url = f"{HOST}/api/2.0/lakeview/dashboards"
            # Send a POST request to create the new dashboard.
            resp = safe_request("post", url, json=data)
            # Print a status message based on whether the request was successful.
            print(f"✅ Created {clean_name}" if resp else f"❌ Failed to create {clean_name}")


# -------------------------------
# Main execution
# -------------------------------
# This block ensures that the functions are called only when the script is executed directly.
if __name__ == "__main__":
     
    deploy_dashboards()   
  