"""
Script to export all Microsoft Fabric workspace IDs and their item IDs to a README file.
Uses interactive browser authentication with user credentials.
"""

import os
import requests
from azure.identity import InteractiveBrowserCredential
from datetime import datetime

FABRIC_BASE_URL = "https://api.fabric.microsoft.com/v1"
POWERBI_BASE_URL = "https://api.powerbi.com/v1.0/myorg"


def get_access_token():
    """Authenticate via browser and return a Fabric API access token."""
    credential = InteractiveBrowserCredential()
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    return token


def get_all_workspaces(access_token):
    """
    Retrieve all workspaces the user has access to via the Power BI REST API.
    Uses $top and $skip for pagination.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    workspaces = []
    top = 1000
    skip = 0

    while True:
        url = f"{POWERBI_BASE_URL}/groups?$top={top}&$skip={skip}"
        response = requests.get(url, headers=headers)
        if not response.ok:
            print(f"Power BI API failed ({response.status_code}): {response.text}")
            print("Falling back to Fabric API...")
            return _get_all_workspaces_fabric(access_token)
        batch = response.json().get("value", [])
        if not batch:
            break
        workspaces.extend(batch)
        if len(batch) < top:
            break
        skip += top

    return workspaces


def _get_all_workspaces_fabric(access_token):
    """Fallback: list workspaces via the Fabric REST API."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    workspaces = []
    url = f"{FABRIC_BASE_URL}/workspaces"

    while url:
        response = requests.get(url, headers=headers)
        if not response.ok:
            print(f"Fabric API also failed ({response.status_code}): {response.text}")
            raise requests.exceptions.HTTPError(response=response)
        data = response.json()
        workspaces.extend(data.get("value", []))

        continuation_uri = data.get("continuationUri")
        url = continuation_uri if continuation_uri else None

    # Normalize keys to match Power BI format
    for ws in workspaces:
        if "name" not in ws and "displayName" in ws:
            ws["name"] = ws["displayName"]

    return workspaces


def get_workspace_items(access_token, workspace_id):
    """
    Retrieve all items within a given workspace.
    Handles pagination via continuationToken.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    items = []
    url = f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/items"

    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        items.extend(data.get("value", []))

        continuation_token = data.get("continuationToken")
        if continuation_token:
            url = f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/items?continuationToken={continuation_token}"
        else:
            url = None

    return items


# Item types that expose a SQL connection string
SQL_ITEM_TYPES = {
    "Warehouse": "warehouses",
    "Lakehouse": "lakehouses",
    "SQLEndpoint": "sqlEndpoints",
}


def get_sql_connection_string(access_token, workspace_id, item_id, item_type):
    """
    Retrieve the SQL connection string for a Warehouse, Lakehouse, or SQLEndpoint.
    Returns the connection string or None if unavailable.
    """
    api_segment = SQL_ITEM_TYPES.get(item_type)
    if not api_segment:
        return None

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    url = f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/{api_segment}/{item_id}"
    try:
        response = requests.get(url, headers=headers)
        if not response.ok:
            return None
        data = response.json()
        props = data.get("properties", {})
        # Warehouses / SQLEndpoints: connectionString is at top level of properties
        conn = props.get("connectionString")
        if conn:
            return conn
        # Lakehouses: nested under sqlEndpointProperties
        sql_props = props.get("sqlEndpointProperties", {})
        return sql_props.get("connectionString")
    except Exception:
        return None


def generate_readme(workspaces_data, output_path=None):
    """Write workspace and item inventory to a markdown README file."""
    if output_path is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, "fabric_inventory.md")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# Fabric Workspace & Item Inventory\n\n")
        f.write(f"_Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_\n\n")
        f.write(f"**Total workspaces:** {len(workspaces_data)}\n\n")
        f.write("---\n\n")

        for ws_name, ws_id, items, sql_connections in workspaces_data:
            f.write(f"## {ws_name}\n\n")
            f.write(f"**Workspace ID:** `{ws_id}`\n\n")

            if sql_connections:
                f.write("**SQL Connection Strings:**\n\n")
                for item_name, conn_str in sql_connections:
                    f.write(f"- **{item_name}:** `{conn_str}`\n")
                f.write("\n")

            if items:
                f.write(f"| # | Item Name | Item Type | Item ID |\n")
                f.write(f"|---|-----------|-----------|----------|\n")
                for idx, item in enumerate(items, 1):
                    name = item.get("displayName", "N/A")
                    item_type = item.get("type", "N/A")
                    item_id = item.get("id", "N/A")
                    f.write(f"| {idx} | {name} | {item_type} | `{item_id}` |\n")
            else:
                f.write("_No items found._\n")

            f.write("\n---\n\n")

    print(f"Inventory written to {output_path}")


def main():
    print("Authenticating via browser...")
    access_token = get_access_token()

    print("Fetching workspaces...")
    workspaces = get_all_workspaces(access_token)
    print(f"Found {len(workspaces)} workspaces.")

    workspaces_data = []
    for ws in workspaces:
        ws_id = ws.get("id")
        ws_name = ws.get("name", ws.get("displayName", "Unnamed"))
        print(f"  Fetching items for '{ws_name}' ({ws_id})...")
        try:
            items = get_workspace_items(access_token, ws_id)
        except requests.exceptions.HTTPError as e:
            print(f"    Warning: Could not fetch items for '{ws_name}': {e}")
            items = []

        # Fetch SQL connection strings for applicable items
        sql_connections = []
        for item in items:
            item_type = item.get("type", "")
            if item_type in SQL_ITEM_TYPES:
                item_id = item.get("id")
                item_name = item.get("displayName", "N/A")
                print(f"    Fetching SQL connection for '{item_name}' ({item_type})...")
                conn_str = get_sql_connection_string(access_token, ws_id, item_id, item_type)
                if conn_str:
                    sql_connections.append((item_name, conn_str))

        workspaces_data.append((ws_name, ws_id, items, sql_connections))

    generate_readme(workspaces_data)


if __name__ == "__main__":
    main()
