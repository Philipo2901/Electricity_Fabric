import fabric_functions as fabfunc
from datetime import datetime

start_time = datetime.now()

#region OAuth authentication using InteractiveBrowserCredential
# Using user authentication via browser instead of service principal
credential = fabfunc.create_credentials_from_user()
fabric_access_token = fabfunc.get_access_token_from_credentials(credential, 'https://api.fabric.microsoft.com/.default')
#endregion

#region Fabric solution setup
fabric_solution_name = 'BLOCKER  ws_Hub_{hub}_{stage}_[{environment}]'
fabric_hub = ["Telematics_Monitoring"]
fabric_environments = ['dev', 'test', 'prod']
fabric_stages = ['Data', 'Load']
#endregion

#region Clean up solution
for hub in fabric_hub:
    for environment in fabric_environments:
        print(f"############################## Clean up {hub} {environment} environment ##############################")
        for stage in fabric_stages:
            workspace_name = fabric_solution_name.format(hub=hub, stage=stage, environment=environment)
            workspace = fabfunc.get_workspace_by_name(fabric_access_token, workspace_name)
            if not workspace is None:
                fabfunc.delete_workspace(fabric_access_token, workspace.get('id'))
    print("")

duration = datetime.now() - start_time
print(f"Script duration: {duration}")
#endregion
