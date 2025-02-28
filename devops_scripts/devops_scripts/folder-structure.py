import os
import json
import requests
import time
import argparse
from urllib.parse import quote

# Configuration
class Config:
    def __init__(self):
        self.workspace_id = None
        self.auth_token = None
        self.api_base_url = "https://api.fabric.microsoft.com/v1"
        self.headers = None
    
    def setup(self, workspace_id, auth_token):
        self.workspace_id = workspace_id
        self.auth_token = auth_token
        self.headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }

config = Config()

def list_workspace_items():
    """List all items in the workspace"""
    url = f"{config.api_base_url}/workspaces/{config.workspace_id}/items"
    response = requests.get(url, headers=config.headers)
    
    if response.status_code != 200:
        print(f"Error fetching workspace items: {response.status_code} - {response.text}")
        return []
    
    items = response.json().get("value", [])
    print(f"Found {len(items)} items in workspace")
    return items

def list_workspace_folders():
    """List all folders in the workspace"""
    items = list_workspace_items()
    folders = [item for item in items if item.get("type") == "Folder"]
    print(f"Found {len(folders)} folders in workspace")
    
    # Create a dictionary with folder name as key and folder ID as value
    folder_dict = {folder["displayName"]: folder["id"] for folder in folders}
    return folder_dict

def create_folder(folder_name, parent_folder_id=None):
    """Create a folder in the workspace"""
    url = f"{config.api_base_url}/workspaces/{config.workspace_id}/folders"
    
    payload = {
        "displayName": folder_name
    }
    
    if parent_folder_id:
        payload["parentFolderId"] = parent_folder_id
    
    response = requests.post(url, headers=config.headers, json=payload)
    
    if response.status_code in [200, 201]:
        folder_data = response.json()
        print(f"Created folder '{folder_name}' with ID: {folder_data.get('id')}")
        return folder_data.get("id")
    else:
        print(f"Failed to create folder '{folder_name}': {response.status_code} - {response.text}")
        return None

def ensure_folder_path(folder_path, existing_folders):
    """Ensure a folder path exists, creating folders as needed"""
    if not folder_path:
        return None
    
    parts = folder_path.split('/')
    current_path = ""
    parent_id = None
    
    for i, part in enumerate(parts):
        if not part:  # Skip empty parts
            continue
            
        # Build the current path for tracking
        current_path = part if i == 0 else f"{current_path}/{part}"
        
        # Check if this folder already exists in our tracking dictionary
        if current_path in existing_folders:
            parent_id = existing_folders[current_path]
            print(f"Folder '{current_path}' already exists with ID: {parent_id}")
        else:
            # Create the folder with the parent reference if needed
            new_folder_id = create_folder(part, parent_id)
            
            if new_folder_id:
                existing_folders[current_path] = new_folder_id
                parent_id = new_folder_id
                # Add a small delay after creating a folder
                time.sleep(1)
            else:
                return None
    
    return parent_id

def move_item_to_folder(item_id, item_type, item_name, folder_id):
    """Move an item to a folder"""
    # URL encode item name for safety
    encoded_item_name = quote(item_name)
    
    # The endpoint depends on the item type
    if item_type.lower() == "notebook":
        url = f"{config.api_base_url}/workspaces/{config.workspace_id}/notebooks/{item_id}"
    elif item_type.lower() == "dataflow":
        url = f"{config.api_base_url}/workspaces/{config.workspace_id}/dataflows/{item_id}"
    elif item_type.lower() == "dataset":
        url = f"{config.api_base_url}/workspaces/{config.workspace_id}/datasets/{item_id}"
    elif item_type.lower() == "report":
        url = f"{config.api_base_url}/workspaces/{config.workspace_id}/reports/{item_id}"
    elif item_type.lower() == "dashboard":
        url = f"{config.api_base_url}/workspaces/{config.workspace_id}/dashboards/{item_id}"
    else:
        print(f"Unsupported item type: {item_type}")
        return False
    
    payload = {
        "parentFolderId": folder_id
    }
    
    response = requests.patch(url, headers=config.headers, json=payload)
    
    if response.status_code in [200, 201, 202, 204]:
        print(f"Successfully moved {item_type} '{encoded_item_name}' to folder")
        return True
    else:
        print(f"Failed to move {item_type} '{encoded_item_name}': {response.status_code} - {response.text}")
        return False

def organize_by_mapping_file(mapping_file):
    """Organize workspace items based on a mapping file"""
    # Load the mapping file
    try:
        with open(mapping_file, 'r') as f:
            mappings = json.load(f)
    except Exception as e:
        print(f"Error loading mapping file: {e}")
        return
    
    # Get all items in the workspace
    items = list_workspace_items()
    
    # Get existing folders
    existing_folders = list_workspace_folders()
    
    # Process each mapping
    for mapping in mappings:
        item_name = mapping.get("itemName")
        folder_path = mapping.get("folderPath", "")
        
        # Find the item in the workspace
        matching_items = [item for item in items if item.get("displayName") == item_name]
        
        if not matching_items:
            print(f"Item '{item_name}' not found in workspace")
            continue
        
        # Create or get the folder ID
        folder_id = ensure_folder_path(folder_path, existing_folders)
        
        if not folder_id and folder_path:
            print(f"Could not create folder path '{folder_path}'")
            continue
        
        # Move each matching item
        for item in matching_items:
            item_id = item.get("id")
            item_type = item.get("type")
            
            if folder_id:
                move_item_to_folder(item_id, item_type, item_name, folder_id)
            else:
                print(f"Skipping '{item_name}' as no folder path was specified or created")

def organize_by_source_structure(source_dir):
    """Organize workspace items based on source directory structure"""
    # Get all items in the workspace
    items = list_workspace_items()
    
    # Get existing folders
    existing_folders = list_workspace_folders()
    
    # Scan the source directory
    for root, dirs, files in os.walk(source_dir):
        # Skip hidden directories
        if os.path.basename(root).startswith('.'):
            continue
            
        # Look for notebook content files
        for file in files:
            if file == "notebook-content.py" and ".platform" in files:
                # Found a notebook directory
                platform_file = os.path.join(root, ".platform")
                
                try:
                    with open(platform_file, "r") as f:
                        platform_data = json.load(f)
                    
                    # Get the notebook name
                    notebook_name = platform_data.get("metadata", {}).get("displayName")
                    
                    if not notebook_name:
                        print(f"Skipping {root}: No displayName found.")
                        continue
                    
                    # Calculate relative path from source_dir
                    rel_path = os.path.relpath(root, source_dir)
                    if rel_path == ".":
                        folder_path = ""
                    else:
                        # Convert OS path separator to forward slash
                        folder_path = rel_path.replace(os.sep, "/")
                    
                    print(f"Found notebook '{notebook_name}' in path '{folder_path}'")
                    
                    # Find the notebook in the workspace
                    matching_items = [item for item in items if item.get("displayName") == notebook_name]
                    
                    if not matching_items:
                        print(f"Notebook '{notebook_name}' not found in workspace")
                        continue
                    
                    # If we have a folder path, ensure it exists
                    folder_id = None
                    if folder_path:
                        folder_id = ensure_folder_path(folder_path, existing_folders)
                        
                        if not folder_id:
                            print(f"Could not create folder path '{folder_path}'")
                            continue
                    
                    # Move each matching notebook
                    for item in matching_items:
                        item_id = item.get("id")
                        item_type = item.get("type")
                        
                        if folder_id:
                            move_item_to_folder(item_id, item_type, notebook_name, folder_id)
                        else:
                            print(f"Skipping '{notebook_name}' as no folder path was specified or created")
                
                except Exception as e:
                    print(f"Error processing {root}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Microsoft Fabric Workspace Resource Organizer")
    parser.add_argument("--workspace-id", required=True, help="Target Fabric workspace ID")
    parser.add_argument("--token", required=True, help="Authentication token")
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Mapping file command
    mapping_parser = subparsers.add_parser("mapping", help="Organize by mapping file")
    mapping_parser.add_argument("--file", required=True, help="Path to JSON mapping file")
    
    # Source structure command
    source_parser = subparsers.add_parser("source", help="Organize by source directory structure")
    source_parser.add_argument("--dir", required=True, help="Path to source directory")
    
    args = parser.parse_args()
    
    # Setup configuration
    config.setup(args.workspace_id, args.token)
    
    # Execute the appropriate command
    if args.command == "mapping":
        organize_by_mapping_file(args.file)
    elif args.command == "source":
        organize_by_source_structure(args.dir)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()