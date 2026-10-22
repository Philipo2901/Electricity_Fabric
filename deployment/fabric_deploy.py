"""
fabric_deploy.py
────────────────
Deployment script for Microsoft Fabric workspaces via fabric-cicd.
Called by azure-pipelines.yml for both TEST and PROD stages.

Usage (inline, handled by the pipeline):
    python fabric_deploy.py
        --workspace_id   <guid>
        --environment    TEST|PROD
        --repo_directory <path>
        --item_types     '["Notebook","DataPipeline","Environment"]'
        --tenant_id      <guid>
        --client_id      <guid>
        --client_secret  <secret>

Authentication: Uses SPN via ClientSecretCredential (azure-identity).
Swap for AzureCliCredential / AzurePowerShellCredential if you prefer
to rely on the AzureCLI@2 / AzurePowerShell@5 task login instead.
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Force unbuffered output so Azure DevOps shows logs in real time
sys.stdout.reconfigure(line_buffering=True, write_through=True)
sys.stderr.reconfigure(line_buffering=True, write_through=True)

from azure.identity import ClientSecretCredential
from fabric_cicd import (
    FabricWorkspace,
    change_log_level,
    publish_all_items,
    unpublish_all_orphan_items,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy items to a Fabric workspace.")
    parser.add_argument("--workspace_id",   required=True,  help="Target Fabric workspace GUID")
    parser.add_argument("--environment",    required=True,  help="Target environment name (TEST or PROD)")
    parser.add_argument("--repo_directory", required=True,  help="Path to the workspace folder in the repo")
    parser.add_argument("--item_types",     required=True,  help='JSON array, e.g. \'["Notebook","DataPipeline"]\'')
    parser.add_argument("--tenant_id",      required=True,  help="Azure AD tenant GUID")
    parser.add_argument("--client_id",      required=True,  help="SPN / App Registration client ID")
    parser.add_argument("--client_secret",  required=True,  help="SPN client secret")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Enable verbose fabric-cicd logging when the ADO pipeline debug flag is set
    if os.getenv("SYSTEM_DEBUG", "false").lower() == "true":
        change_log_level("DEBUG")

    # Parse item types from JSON string
    try:
        item_types: list[str] = json.loads(args.item_types)
    except json.JSONDecodeError as exc:
        print(f"ERROR: --item_types must be a valid JSON array. Got: {args.item_types!r}")
        raise SystemExit(1) from exc

    # Resolve the repository directory relative to this script's location
    script_root = Path(__file__).resolve().parent.parent   # one level above .deploy/
    repository_directory = str(script_root / args.repo_directory)

    print(f"[fabric-cicd] Environment     : {args.environment}")
    print(f"[fabric-cicd] Workspace ID    : {args.workspace_id}")
    print(f"[fabric-cicd] Repo directory  : {repository_directory}")
    print(f"[fabric-cicd] Items in scope  : {item_types}")

    # Build the token credential from the SPN values passed by the pipeline
    token_credential = ClientSecretCredential(
        tenant_id=args.tenant_id,
        client_id=args.client_id,
        client_secret=args.client_secret,
    )

    # Initialise the target workspace
    target_workspace = FabricWorkspace(
        workspace_id=args.workspace_id,
        environment=args.environment,
        repository_directory=repository_directory,
        item_type_in_scope=item_types,
        token_credential=token_credential,
    )

    # Deploy all items in scope
    print("[fabric-cicd] Publishing items ...")
    publish_all_items(target_workspace)

    # Remove items from the workspace that no longer exist in the repo
    print("[fabric-cicd] Removing orphan items ...")
    unpublish_all_orphan_items(target_workspace)

    print("[fabric-cicd] Deployment complete.")


if __name__ == "__main__":
    main()
