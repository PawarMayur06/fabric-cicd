import json
import requests
import base64
import os

def load_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            print(f"Loaded JSON from {file_path}: {json.dumps(data, indent=2)}")
            return data
    except Exception as e:
        print(f"Error loading JSON from {file_path}: {e}")
        return {}

def save_json(data, file_path):
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
            print(f"Saved JSON to {file_path}: {json.dumps(data, indent=2)}")
    except Exception as e:
        print(f"Error saving JSON to {file_path}: {e}")

def fetch_workspace_items(workspace_id, auth_token):
    try:
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
        headers = {"Authorization": f"Bearer {auth_token}", "Content-Type": "application/json"}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        print(f"Fetched workspace items for {workspace_id}:", json.dumps(data, indent=2))
        return data.get("value", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching workspace items: {e}")
        return []

def get_pipeline_definition(workspace_id, pipeline_id, auth_token):
    try:
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/getDefinition"
        headers = {"Authorization": f"Bearer {auth_token}", "Content-Type": "application/json"}
        response = requests.post(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching pipeline definition: {e}")
        return {}

def find_pipeline_by_name(items, pipeline_name):
    for item in items:
        if item.get('type') == 'DataPipeline' and item.get('displayName') == pipeline_name:
            return item.get('id')
    return None

def decode_base64(encoded_str):
    try:
        return base64.b64decode(encoded_str).decode('utf-8')
    except Exception as e:
        print(f"Error decoding base64: {e}")
        return None

def get_pipeline_folders(base_path):
    try:
        return [f.path for f in os.scandir(base_path) if f.is_dir() and f.name.endswith(".DataPipeline")]
    except Exception as e:
        print(f"Error fetching pipeline folders: {e}")
        return []

def get_pipeline_name_from_folder(folder_path):
    # Get the folder name from the path
    folder_name = os.path.basename(folder_path)
    # Remove the .DataPipeline suffix
    pipeline_name = folder_name.replace(".DataPipeline", "")
    return pipeline_name

def create_or_update_pipeline(workspace_id, pipeline_data, auth_token, pipeline_name):
    try:
        # First check if pipeline exists
        items = fetch_workspace_items(workspace_id, auth_token)
        existing_pipeline = next(
            (item for item in items if item.get('type') == 'DataPipeline' 
             and item.get('displayName') == pipeline_name),
            None
        )

        base_url = "https://api.fabric.microsoft.com/v1"
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }

        if existing_pipeline:
            # Update existing pipeline
            pipeline_id = existing_pipeline['id']
            url = f"{base_url}/workspaces/{workspace_id}/items/{pipeline_id}/updateDefinition"
            print(f"\nUpdating existing pipeline: {pipeline_name}")
            print(f"API URL (POST): {url}")
            
            response = requests.post(url, headers=headers, json=pipeline_data)
        else:
            # Create new pipeline
            url = f"{base_url}/workspaces/{workspace_id}/items"
            print(f"\nCreating new pipeline: {pipeline_name}")
            print(f"API URL (POST): {url}")
            
            # Add required fields for creation
            pipeline_data.update({
                "type": "DataPipeline",
                "displayName": pipeline_name,
                "description": f"Pipeline: {pipeline_name}"
            })
            
            response = requests.post(url, headers=headers, json=pipeline_data)

        print(f"Request payload: {json.dumps(pipeline_data, indent=2)}")
        print(f"Response status code: {response.status_code}")
        
        if response.status_code >= 400:
            print(f"Error response: {response.text}")
        
        response.raise_for_status()
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Error creating/updating pipeline: {e}")
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            print(f"Error details: {e.response.text}")
        return {}

def encode_to_base64(file_path):
    try:
        if not os.path.exists(file_path):
            print(f"Error: File {file_path} does not exist!")
            return ""
            
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            encoded = base64.b64encode(content.encode('utf-8')).decode('utf-8')
            print(f"Encoded content (first 100 chars): {encoded[:100]}...")
            return encoded
    except Exception as e:
        print(f"Error encoding {file_path} to Base64: {e}")
        return ""

def update_pipeline_files(base_path, source_workspace_id, target_workspace_id, auth_token):
    print("Fetching source workspace items...")
    source_items = fetch_workspace_items(source_workspace_id, auth_token)
    
    print("\nFetching target workspace notebooks...")
    target_items = fetch_workspace_items(target_workspace_id, auth_token)
    
    # Create mapping of notebook names to (id, workspaceId) from target workspace
    target_notebook_mapping = {
        item.get('displayName'): (item.get('id'), item.get('workspaceId'))
        for item in target_items 
        if isinstance(item, dict) and item.get('type') == 'Notebook'
    }
    print("\nTarget Notebook Mapping (Name to ID):", target_notebook_mapping)

    print("\nFetching pipeline folders...")
    pipeline_folders = get_pipeline_folders(base_path)
    
    for folder in pipeline_folders:
        print(f"\nProcessing folder: {folder}")
        
        # Get pipeline name from folder name
        pipeline_name = get_pipeline_name_from_folder(folder)
        print(f"Found pipeline name: {pipeline_name}")
        
        # Find pipeline ID in source workspace
        pipeline_id = find_pipeline_by_name(source_items, pipeline_name)
        if not pipeline_id:
            print(f"Couldn't find pipeline ID for {pipeline_name} in source workspace")
            continue
        
        print(f"Found pipeline ID: {pipeline_id}")
        
        # Get pipeline definition
        pipeline_def = get_pipeline_definition(source_workspace_id, pipeline_id, auth_token)
        if not pipeline_def:
            print(f"Couldn't get pipeline definition for {pipeline_name}")
            continue
        
        # Extract and decode pipeline content
        pipeline_content = None
        for part in pipeline_def.get('definition', {}).get('parts', []):
            if part.get('path') == 'pipeline-content.json':
                decoded_content = decode_base64(part.get('payload', ''))
                if decoded_content:
                    pipeline_content = json.loads(decoded_content)
                break
        
        if not pipeline_content:
            print(f"Couldn't extract pipeline content for {pipeline_name}")
            continue
        
        # Check if pipeline has TridentNotebook activities
        has_trident_notebook = False
        changes_made = False
        
        # Create notebook ID to name mapping from source workspace
        source_notebook_mapping = {
            item.get('id'): item.get('displayName')
            for item in source_items 
            if isinstance(item, dict) and item.get('type') == 'Notebook'
        }
        
        # Update pipeline content if it contains TridentNotebook activities
        for activity in pipeline_content.get("properties", {}).get("activities", []):
            if activity.get("type") == "TridentNotebook":
                has_trident_notebook = True
                source_notebook_id = activity.get('typeProperties', {}).get('notebookId')
                source_notebook_name = source_notebook_mapping.get(source_notebook_id)
                
                if source_notebook_name and source_notebook_name in target_notebook_mapping:
                    new_notebook_id, new_workspace_id = target_notebook_mapping[source_notebook_name]
                    activity["typeProperties"]["notebookId"] = new_notebook_id
                    activity["typeProperties"]["workspaceId"] = new_workspace_id
                    changes_made = True
                    print(f"\nUpdated notebook reference in activity {activity.get('name')}:")
                    print(f"Source Notebook Name: {source_notebook_name}")
                    print(f"New ID: {new_notebook_id}")
                    print(f"New Workspace: {new_workspace_id}")
                else:
                    print(f"\nNo mapping found for notebook ID {source_notebook_id}")
                    if not source_notebook_name:
                        print(f"Notebook ID not found in source workspace")
                    else:
                        print(f"Notebook '{source_notebook_name}' not found in target workspace")

        # Save the pipeline content to a temp file (whether changes were made or not)
        if has_trident_notebook and not changes_made:
            print("Pipeline has TridentNotebook activities but no changes were needed.")
        elif not has_trident_notebook:
            print("Pipeline has no TridentNotebook activities. Proceeding to deployment without changes.")
        else:
            print("Changes made to TridentNotebook references.")

        # Save pipeline content
        temp_file = os.path.join(folder, "temp.json")
        save_json(pipeline_content, temp_file)

        # Encode updated content
        encoded_pipeline = encode_to_base64(temp_file)
        if not encoded_pipeline:
            print("Error: Failed to encode pipeline content!")
            continue

        platform_file = os.path.join(folder, ".platform")
        encoded_platform = encode_to_base64(platform_file) if os.path.exists(platform_file) else ""

        # Create payload for update
        payload = {
            "definition": {
                "parts": [
                    {"path": "pipeline-content.json", "payload": encoded_pipeline, "payloadType": "InlineBase64"},
                    {"path": ".platform", "payload": encoded_platform, "payloadType": "InlineBase64"}
                ]
            }
        }

        # Update pipeline
        print(f"\nCreating or updating pipeline for {folder}")
        create_or_update_pipeline(target_workspace_id, payload, auth_token, pipeline_name)
        print(f"Pipeline {folder} processed successfully.")

        # Clean up temp file
        try:
            os.remove(temp_file)
            print(f"Cleaned up temp file {temp_file}")
        except Exception as e:
            print(f"Error removing temp file {temp_file}: {e}")

if __name__ == "__main__":
    # Configuration
    base_path = os.getenv("Azure_repo_path")
    source_workspace_id = os.getenv("source_workspace_id")  # Source workspace ID
    target_workspace_id = os.getenv("target_workspace_id")  # Target workspace ID
    
    # Get auth token from environment variable or direct assignment
    auth_token = os.getenv("auth_token")
    if not auth_token:
        raise ValueError("Auth token is not set")
    
    # Run the update
    update_pipeline_files(base_path, source_workspace_id, target_workspace_id, auth_token)