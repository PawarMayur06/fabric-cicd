import os
import json
import requests
import base64
import time

# Configuration from environment variables
TARGET_WORKSPACE_ID = os.getenv("TARGET_WORKSPACE_ID")
AZURE_REPO_PATH = os.getenv("AZURE_REPO_PATH", "./")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")

HEADERS = {
    "Authorization": f"Bearer {AUTH_TOKEN}",
    "Content-Type": "application/json"
}

def fetch_items(workspace_id, item_type):
    """Fetch existing items (Notebooks) from Fabric workspace."""
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json()
    items = {item["displayName"]: item["id"] for item in data.get("value", []) if item.get("type") == item_type}
    print(f"Fetched {item_type}s: {items}")
    return items

def fetch_folders(workspace_id):
    """Fetch existing folders from Fabric workspace."""
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json()
    folders = {item["displayName"]: item["id"] for item in data.get("value", []) if item.get("type") == "Folder"}
    print(f"Fetched Folders: {folders}")
    return folders

def encode_file(file_path):
    """Read and encode file in Base64 format."""
    with open(file_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

def find_notebooks(repo_path, content_file):
    """Recursively search for folders containing specific content files and track their relative paths."""
    notebooks = []
    for root, _, files in os.walk(repo_path):
        if ".platform" in files and content_file in files:
            rel_path = os.path.relpath(root, repo_path)
            if rel_path == ".":
                rel_path = ""  # Root folder
            notebooks.append((root, rel_path))
    return notebooks

def create_folder(workspace_id, folder_name, existing_folders):
    """Create a folder in Fabric workspace if it doesn't exist and return its ID."""
    if folder_name in existing_folders:
        print(f"Folder '{folder_name}' already exists with ID: {existing_folders[folder_name]}")
        return existing_folders[folder_name]
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/folders"
    payload = {"displayName": folder_name}
    
    print(f"Creating folder: {folder_name}")
    response = requests.post(url, headers=HEADERS, json=payload)
    
    if response.status_code in [200, 201]:
        folder_id = response.json().get("id")
        existing_folders[folder_name] = folder_id
        print(f"Created folder '{folder_name}' with ID: {folder_id}")
        return folder_id
    else:
        print(f"Failed to create folder '{folder_name}': {response.status_code} - {response.text}")
        return None

def create_folder_structure(workspace_id, folder_path, existing_folders):
    """Create nested folder structure in Fabric workspace."""
    if not folder_path:
        return None  # Root folder
    
    parts = folder_path.split(os.sep)
    current_path = ""
    parent_id = None
    
    for i, part in enumerate(parts):
        if not part:  # Skip empty parts
            continue
            
        current_path = part if i == 0 else os.path.join(current_path, part)
        
        # Check if this folder already exists
        if current_path in existing_folders:
            parent_id = existing_folders[current_path]
        else:
            # Create the folder with the parent reference if needed
            url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/folders"
            payload = {"displayName": part}
            
            if parent_id:
                payload["parentFolderId"] = parent_id
                
            print(f"Creating folder: {part} in path {current_path}")
            response = requests.post(url, headers=HEADERS, json=payload)
            
            if response.status_code in [200, 201]:
                folder_id = response.json().get("id")
                existing_folders[current_path] = folder_id
                parent_id = folder_id
                print(f"Created folder '{part}' with ID: {folder_id}")
                # Add a small delay after creating a folder
                time.sleep(2)
            else:
                print(f"Failed to create folder '{part}': {response.status_code} - {response.text}")
                return None
    
    return parent_id

def process_notebook_migration(target_items, workspace_id, content_file, create_url, update_url):
    """Process and migrate Notebooks to Fabric with directory structure preserved."""
    notebooks = find_notebooks(AZURE_REPO_PATH, content_file)
    existing_folders = fetch_folders(workspace_id)
    
    for item_path, rel_path in notebooks:
        platform_file = os.path.join(item_path, ".platform")
        content_file_path = os.path.join(item_path, content_file)

        with open(platform_file, "r") as f:
            platform_data = json.load(f)
        display_name = platform_data.get("metadata", {}).get("displayName")

        if not display_name:
            print(f"Skipping {item_path}: No displayName found.")
            continue

        encoded_platform = encode_file(platform_file)
        encoded_content = encode_file(content_file_path)

        # Create folder structure if needed
        parent_folder_id = None
        if rel_path:
            print(f"Creating folder structure for: {rel_path}")
            parent_folder_id = create_folder_structure(workspace_id, rel_path, existing_folders)
            print(f"Parent folder ID for {rel_path}: {parent_folder_id}")

        payload = {
            "definition": {
                "parts": [
                    {"path": content_file, "payload": encoded_content, "payloadType": "InlineBase64"},
                    {"path": ".platform", "payload": encoded_platform, "payloadType": "InlineBase64"}
                ]
            }
        }

        # Add parent folder reference if needed
        if parent_folder_id:
            payload["parentFolderId"] = parent_folder_id

        full_path = os.path.join(rel_path, display_name) if rel_path else display_name
        
        if display_name in target_items:
            item_id = target_items[display_name]
            url = update_url.format(workspaceId=workspace_id, itemId=item_id)
            action = "Updated"
            response = requests.post(url, headers=HEADERS, json=payload)
        else:
            url = create_url.format(workspaceId=workspace_id)
            action = "Created"
            payload["name"] = display_name
            response = requests.post(url, headers=HEADERS, json=payload)

            # Add a delay of 10 seconds after creating a notebook
            if response.status_code in [200, 201, 202]:
                print("Waiting for 10 seconds before proceeding to the next notebook...")
                time.sleep(10)

        print(f"Making API request to {url}")
        print(f"Response: {response.status_code} - {response.text}")

        if response.status_code in [200, 201, 202]:
            print(f"{action} Notebook {full_path} successfully.")
        else:
            print(f"Failed to {action.lower()} Notebook {full_path}: {response.status_code} - {response.text}")

if __name__ == "__main__":
    if not TARGET_WORKSPACE_ID or not AUTH_TOKEN:
        print("Error: TARGET_WORKSPACE_ID and AUTH_TOKEN must be set as environment variables.")
        exit(1)

    # Process Notebook Migration Only
    target_notebooks = fetch_items(TARGET_WORKSPACE_ID, "Notebook")
    process_notebook_migration(
        target_notebooks,
        TARGET_WORKSPACE_ID,
        "notebook-content.py",
        "https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/notebooks",
        "https://api.fabric.microsoft.com/v1/workspaces/{workspaceId}/notebooks/{itemId}/updateDefinition"
    )