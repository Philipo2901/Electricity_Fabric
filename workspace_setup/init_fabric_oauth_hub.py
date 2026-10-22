import fabric_functions as fabfunc
from azure.identity import InteractiveBrowserCredential
from datetime import datetime

start_time = datetime.now()

#region OAuth authentication using InteractiveBrowserCredential
# Using user authentication via browser instead of service principal
credential = fabfunc.create_credentials_from_user()
fabric_access_token = fabfunc.get_access_token_from_credentials(credential, 'https://api.fabric.microsoft.com/.default')

# For Azure Management operations (private endpoints)
management_access_token = fabfunc.get_access_token_from_credentials(credential, 'https://management.azure.com/.default')
#endregion

#region Fabric solution setup
fabric_solution_name = 'ws_Hub_{hub}_{stage}_[{environment}]'

fabric_hub = {
        "<fabric hub name>",   # e.g., "COMPANY Analytics"
    }

#region Fabric environments
fabric_environments = {
        "dev": {
            "capacity_id": "<dev capacity id>",
            "permissions": {
                "Admin": [
                    {"type": "User", "id": "some.one@COMPANY.group"},
                    {"type": "Group", "id": "<developer AAD group id>"},              # <developer AAD group id>
                    {"type": "App", "id": "<service principal id for deployment>"},   # <service principal id for deployment>
                ]
            }
        },
        "test": {
            "capacity_id": "<test capacity id>",
            "permissions": {
                "Admin": [
                    {"type": "User", "id": "some.one@COMPANY.group"},
                    {"type": "Group", "id": "<developer AAD group id>"},              # <developer AAD group id>
                    {"type": "App", "id": "<service principal id for deployment>"},   # <service principal id for deployment>
                ]
            }
        },
        "prod": {
            "capacity_id": "<prod capacity id>",
            "permissions": {
                "Admin": [
                    {"type": "User", "id": "some.one@COMPANY.group"},
                    {"type": "Group", "id": "<developer AAD group id>"},              # <developer AAD group id>
                    {"type": "App", "id": "<service principal id for deployment>"},   # <service principal id for deployment>
                ]
            }
        }
    }
#endregion

#region Fabric Workspaces stages
fabric_stages = {
        "Data": { },#"lakehouses": ["LH_Silver_Shortcut", "LH_Gold"], "schema": True },
        "Load": { },
        "Serve": { },
    }

#region Fabric Solution Git setup (the workspace subfolder must exist in the repo)
fabric_git_setup = {
        "dev": { 
            "Load": { 
                "DevOpsOrgName" : "COMPANYAnalytics",
                "DevOpsProjectName" : "fabriccicd",
                "DevOpsRepoName" : "COMPANY_Fabric_Hub",
                "DevOpsDefaultBranch" : "main",
                "DevOpsFabricSolutionFolder" : "solution"
                },
        }
    }
#endregion

#region Setup Fabric solution
for hub in fabric_hub:
    for environment, env_props in fabric_environments.items():
        print(f"############################## Setting up {hub} {environment} environment ##############################")
        
        for stage, stage_props in fabric_stages.items():
            workspace_name = fabric_solution_name.format(hub=hub, stage=stage, environment=environment)
            workspace = fabfunc.create_workspace(fabric_access_token, workspace_name, "Workspace automatically created from init_fabric.py script.")
            workspace = fabfunc.get_workspace_by_name(fabric_access_token, workspace_name)
            workspace_id = workspace.get('id')

            fabfunc.assign_workspace_to_capacity(fabric_access_token, workspace_id, env_props.get("capacity_id"))

            if not env_props.get("domain_id") is None:
                fabfunc.assign_workspace_to_domain(fabric_access_token, workspace_id, env_props.get("domain_id"))

            if not env_props.get("permissions") is None:
                for permission, permission_definitions in env_props.get("permissions").items():
                    for definition in permission_definitions:
                        fabfunc.add_workspace_user(fabric_access_token, workspace_id, permission, definition.get("type"), definition.get("id") )

            if not stage_props.get("lakehouses") is None:
                for lakehouse in stage_props.get("lakehouses"):
                    fabfunc.create_fabric_item(fabric_access_token, workspace_id, lakehouse, "Lakehouse", None, stage_props.get("schema"))

            if not stage_props.get("private_endpoints") is None:
                for private_endpoint in stage_props.get("private_endpoints"):
                    fabfunc.create_workspace_managed_private_endpoint(fabric_access_token, workspace_id, private_endpoint.get("name"), private_endpoint.get("id"))
                    if(private_endpoint.get("auto_approve")):
                        connection_name = f"{workspace_id}.{private_endpoint.get('name')}-conn"
                        fabfunc.approve_private_endpoint(management_access_token, private_endpoint.get("id"), connection_name)                 
        print ("")


if not fabric_git_setup is None:
    print(f"############################## Setting up git integrations ##############################")
    for hub in fabric_hub:
        for environment, stages in fabric_git_setup.items():
            for stage, git_props in stages.items():
                workspace_name = fabric_solution_name.format(hub=hub, stage=stage, environment=environment)
                fabric_git_name = 'ws_Prepare_{hub}_{stage}'
                workspace_git_name = fabric_git_name.format(hub=hub, stage=stage)
                workspace = fabfunc.get_workspace_by_name(fabric_access_token, workspace_name)
                print (f"Setting up Git integration for workspace {workspace_name}")

                connect_response = fabfunc.connect_workspace_to_git(
                    fabric_access_token, 
                    workspace.get('id'), 
                    git_props.get("DevOpsOrgName"),
                    git_props.get("DevOpsProjectName"), 
                    git_props.get("DevOpsRepoName"), 
                    git_props.get("DevOpsDefaultBranch"), 
                    git_props.get("DevOpsFabricSolutionFolder") + "/" + workspace_git_name)
                if not connect_response is None:
                    init_response = fabfunc.initialize_workspace_git_connection(fabric_access_token, workspace.get('id'))
                    if init_response and init_response.get("requiredAction") != "None" and init_response.get("remoteCommitHash"):
                        fabfunc.update_workspace_from_git(fabric_access_token, workspace.get('id'), init_response["remoteCommitHash"])
        
                print("")

duration = datetime.now() - start_time
print(f"Script duration: {duration}")
#endregion
