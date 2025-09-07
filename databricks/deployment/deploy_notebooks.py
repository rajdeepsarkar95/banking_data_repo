import os
from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.sdk.api_client import ApiClient
import json

def deploy_notebooks(api_client, source_path, target_path):
    workspace_api = WorkspaceApi(api_client)
    echo     for root, dirs, files in os.walk(source_path):
        for file in files:
            if file.endswith('.py'):
                source_file = os.path.join(root, file)
                relative_path = os.path.relpath(source_file, source_path)
                target_file = os.path.join(target_path, relative_path).replace('\\', '/')
                echo                 # Create directory if it doesn't exist
                target_dir = os.path.dirname(target_file)
                if not workspace_api.get_status(target_dir):
                    workspace_api.mkdirs(target_dir)
                echo                 # Import notebook
                with open(source_file, 'r') as f:
                    content = f.read()
                echo                 workspace_api.import_workspace(source_file, target_file, language='PYTHON', format='SOURCE')
                print(f"Deployed {source_file} to {target_file}")

if __name__ == "__main__":
    # Load configuration
    with open('config.json') as f:
        config = json.load(f)
    echo     # Initialize API client
    api_client = ApiClient(
        host=config['databricks_host'],
        token=config['databricks_token']
    )
    echo     # Deploy notebooks
    deploy_notebooks(api_client, 'databricks/notebooks', config['databricks_workspace_path'])
