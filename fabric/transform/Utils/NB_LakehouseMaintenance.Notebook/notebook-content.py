# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f5df3ca3-6568-4c3b-80e3-6f4ceb6a2d6f",
# META       "default_lakehouse_name": "LH_Bronze",
# META       "default_lakehouse_workspace_id": "1cfeab7c-4df3-48cb-8407-c31a3bb8a7e5",
# META       "known_lakehouses": [
# META         {
# META           "id": "f5df3ca3-6568-4c3b-80e3-6f4ceb6a2d6f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Lakehouse Maintenance
# 
# Consolidated notebook for Delta table optimization, vacuum, and file cleanup operations.

# CELL ********************

from pyspark.sql.types import Row
from datetime import datetime, timedelta
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
import time

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vl = notebookutils.variableLibrary.getLibrary("Variables_Environment_Config")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_HelperFunctions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Notebook Purpose And Required Parameters
# 
# This notebook performs comprehensive lakehouse maintenance operations:
# 
# 1. **Delta table OPTIMIZE** - Compaction with configurable resource profiles
# 2. **Delta table VACUUM** - Remove old file versions
# 3. **File cleanup** - Delete old files from the Files section
# 
# All operations target a **single lakehouse** specified by `lakehouse_name`.
# 
# ### General Parameters
# 
# - `lakehouse_name`: Name of the lakehouse to maintain (e.g., "LH_Silver")
# 
# ### Resource Profile
# 
# - `resource_profile`: Spark optimization profile for OPTIMIZE operations
#   - `"writeHeavy"`: Optimized for ETL, ingestion (default for new workspaces)
#   - `"readHeavyForSpark"`: Optimized for analytical Spark queries
#   - `"readHeavyForPBI"`: Optimized for Power BI reports
#   - `"custom"`: User-defined (configure in Environment)
# 
# ### Delta Table Maintenance
# 
# - `delta_optimize_is_active`: Enables or disables OPTIMIZE (compaction) processing
# - `delta_vacuum_is_active`: Enables or disables VACUUM processing
# - `retention_days_vacuum`: Number of days of Delta history to retain
# 
# ### File Delete Parameters
# 
# - `file_delete_is_active`: Enables or disables file deletion processing
# - `retention_days_files`: Minimum file age in days before deletion
# - `lakehouse_file_subfolder`: Optional subfolder under `Files/` to scope cleanup. Leave empty to target the full `Files/` section


# PARAMETERS CELL ********************

# Lakehouse to maintain
lakehouse_name = "LH_Silver"

# Resource profile for optimize operations
# Options: "writeHeavy", "readHeavyForSpark", "readHeavyForPBI", "custom"
resource_profile = "readHeavyForSpark"

# Delta table maintenance
delta_optimize_is_active = True
delta_vacuum_is_active = True
retention_days_vacuum = 7

# File cleanup (Files section)
file_delete_is_active = False
retention_days_files = None
lakehouse_file_subfolder = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Calculate retention hours
if retention_days_vacuum:
    retention_hours_vacuum = retention_days_vacuum * 24

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_tables_in_lakehouse(lakehouse_name: str) -> list[Row]:
    """Collect all Delta tables from the specified lakehouse via filesystem.
    
    Uses filesystem discovery to work across any lakehouse, not just the default.
    Handles the schema hierarchy: Tables/schema/table/

    Args:
        lakehouse_name: Lakehouse name to scan for tables.

    Returns:
        A list of rows containing namespace and table name values.
    """
    workspace_id = vl.DataWorkspaceID
    lakehouse_id = notebookutils.lakehouse.get(lakehouse_name, workspace_id).id
    tables_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables"
    
    tables = []
    
    try:
        # List all schemas (directories under Tables/)
        schema_items = notebookutils.fs.ls(tables_path)
        
        for schema_item in schema_items:
            if schema_item.isDir:
                schema_name = schema_item.name.rstrip('/')
                
                # List all tables in this schema
                try:
                    table_items = notebookutils.fs.ls(schema_item.path)
                    for table_item in table_items:
                        if table_item.isDir:
                            table_name = table_item.name.rstrip('/')
                            
                            # Verify it's a Delta table by checking for _delta_log
                            try:
                                notebookutils.fs.ls(f"{table_item.path.rstrip('/')}/_delta_log")
                                tables.append(Row(namespace=schema_name, tableName=table_name))
                            except:
                                pass  # Not a Delta table, skip
                except Exception as e:
                    print(f"Warning: Could not access schema '{schema_name}': {str(e)}")
                    
    except Exception as e:
        print(f"Warning: Could not access Tables path '{tables_path}': {str(e)}")
    
    return tables

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def maintain_delta_tables(tables: list[Row], lakehouse_name: str, retention_hours: int = 168, 
                         resource_profile: str = "writeHeavy", run_optimize: bool = True, 
                         run_vacuum: bool = True, dry_run: bool = True) -> None:
    """Run OPTIMIZE and/or VACUUM on the provided lakehouse tables.

    Args:
        tables: Table references as rows containing schema/stage and table name.
        lakehouse_name: Name of the lakehouse being maintained.
        retention_hours: Retention period in hours for VACUUM.
        resource_profile: Spark resource profile for optimize operations.
        run_optimize: If True, execute OPTIMIZE compaction.
        run_vacuum: If True, execute VACUUM cleanup.
        dry_run: If True, only simulate operations without executing.
    """
    successful_optimizes = []
    failed_optimizes = []
    successful_vacuums = []
    failed_vacuums = []

    # Apply resource profile if optimize is enabled
    if run_optimize and not dry_run:
        try:
            spark.conf.set("spark.fabric.resourceProfile", resource_profile)
            print(f"Applied resource profile: {resource_profile}")
        except Exception as e:
            print(f"Warning: Could not set resource profile: {str(e)}")

    # Get workspace info for table paths (using DataWorkspace where lakehouses reside)
    workspace_id = vl.DataWorkspaceID
    lakehouse_id = notebookutils.lakehouse.get(lakehouse_name, workspace_id).id
    destination_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"

    # Iterate over tables and execute maintenance operations
    for table in tables:
        schema = table[0]
        name = table[1]
        table_full_name = f"{schema}.{name}"
        table_path = f"{destination_path}/Tables/{schema}/{name}"

        # OPTIMIZE operation
        if run_optimize:
            if not dry_run:
                try:
                    delta_table = DeltaTable.forPath(spark, table_path)
                    delta_table.optimize().executeCompaction()
                    successful_optimizes.append(table_full_name)
                except AnalysisException:
                    failed_optimizes.append({"table": table_full_name, "error": "Not a Delta table"})
                except Exception as e:
                    failed_optimizes.append({"table": table_full_name, "error": str(e)})
            else:
                successful_optimizes.append(table_full_name)

        # VACUUM operation
        if run_vacuum:
            if not dry_run:
                try:
                    delta_table = DeltaTable.forPath(spark, table_path)
                    delta_table.vacuum(retention_hours)
                    successful_vacuums.append(table_full_name)
                except AnalysisException:
                    failed_vacuums.append({"table": table_full_name, "error": "Not a Delta table"})
                except Exception as e:
                    failed_vacuums.append({"table": table_full_name, "error": str(e)})
            else:
                successful_vacuums.append(table_full_name)

    # Print summary of results
    if run_optimize:
        print(f"\nOPTIMIZE Summary:")
        print(f"Successfully optimized: {len(successful_optimizes)} tables")
        print(f"Failed: {len(failed_optimizes)} tables")
        if failed_optimizes:
            print("\nFailed tables:")
            for failure in failed_optimizes:
                print(f"  - {failure['table']}: {failure['error']}")

    if run_vacuum:
        print(f"\nVACUUM Summary:")
        print(f"Successfully vacuumed: {len(successful_vacuums)} tables")
        print(f"Failed: {len(failed_vacuums)} tables")
        if failed_vacuums:
            print("\nFailed tables:")
            for failure in failed_vacuums:
                print(f"  - {failure['table']}: {failure['error']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_all_files_recursive(path: str) -> list[str]:
    """Recursively list all files under a given directory path.

    Args:
        path: Root directory to traverse.

    Returns:
        A flat list of file info objects discovered under the path.
    """
    all_files = []
    
    try:
        # Get items in current directory
        items = notebookutils.fs.ls(path)
        
        for item in items:
            if item.isDir:
                # If it's a directory, recursively get files from it
                dir_path = item.path
                if not dir_path.endswith('/'):
                    dir_path += '/'
                
                # Recursively get files from subdirectory
                sub_files = get_all_files_recursive(dir_path)
                all_files.extend(sub_files)
            else:
                # If it's a file, add it to our list
                all_files.append(item)
                
    except Exception as e:
        print(f"Warning: Could not access path '{path}': {str(e)}")
    
    return all_files

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def delete_from_filesection(files: list, file_retention_days: int, max_retries=3, retry_delay_sec=5, dry_run=True) -> None:
    """Delete files older than the retention threshold with retry support.

    Args:
        files: File info objects to evaluate and optionally delete.
        file_retention_days: Minimum age in days required before deleting a file.
        max_retries: Number of retries for each delete operation on failure.
        retry_delay_sec: Delay in seconds between retry attempts.
        dry_run: If True, only simulate deletion and report candidates.
    """
    successful_deletes = []
    failed_deletes = []
    
    if file_retention_days < 1:
        raise ValueError("file_retention_days parameter must be > 0")
    
    today_datetime = datetime.now()
    for f in files:
        modified_datetime = datetime.fromtimestamp(f.modifyTime/1000.0)
        file_age = today_datetime - modified_datetime

        if file_age.days > file_retention_days:
            print(f"file should be deleted: {f.name}")
            try:
                if not dry_run:
                    for attempt in range(max_retries):
                        try:
                            notebookutils.fs.rm(f.path)
                            successful_deletes.append({f.name: f.path})
                            break
                        except Exception as e:
                            if attempt < max_retries - 1:
                                print(f"Attempt {attempt + 1} failed for {f.name} Retrying in {retry_delay_sec} seconds...")
                                time.sleep(retry_delay_sec)
                            else:
                                raise
                else:
                    successful_deletes.append({f.name: f.path})
                    
            except Exception as e:
                failed_deletes.append({"file": f.path, "error": str(e)})

    # print summary of delete results
    print(f"DELETE Summary:")
    print(f"Successfully deleted: {len(successful_deletes)} files")
    print(f"Failed: {len(failed_deletes)} files")

    if failed_deletes:
        print("\nFailed files:")
        for failure in failed_deletes:
            print(f"  - {failure['file']}: {failure['error']}")            

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute Delta table maintenance (OPTIMIZE and/or VACUUM)
if delta_optimize_is_active or delta_vacuum_is_active:
    print(f"Starting Delta table maintenance for lakehouse: {lakehouse_name}")
    maintenance_tables = get_tables_in_lakehouse(lakehouse_name)
    
    if maintenance_tables:
        print(f"Found {len(maintenance_tables)} tables to maintain")
        maintain_delta_tables(
            tables=maintenance_tables,
            lakehouse_name=lakehouse_name,
            retention_hours=retention_hours_vacuum,
            resource_profile=resource_profile,
            run_optimize=delta_optimize_is_active,
            run_vacuum=delta_vacuum_is_active,
            dry_run=False
        )
    else:
        print(f"No tables found in lakehouse: {lakehouse_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute File Delete
if file_delete_is_active:
    print(f"\nStarting file cleanup for lakehouse: {lakehouse_name}")
    # Build path using DataWorkspace where lakehouses reside
    workspace_id = vl.DataWorkspaceID
    lakehouse_id = notebookutils.lakehouse.get(lakehouse_name, workspace_id).id
    lh_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"
    file_delete_path = f"{lh_path.rstrip('/')}/Files/{lakehouse_file_subfolder.strip('/')}" if lakehouse_file_subfolder else f"{lh_path.rstrip('/')}/Files/"
    
    if not file_delete_path.endswith('/'):
        file_delete_path += '/'
    
    print(f"Scanning path: {file_delete_path}")
    files = get_all_files_recursive(file_delete_path)
    
    if files:
        print(f"Found {len(files)} files to evaluate")
        delete_from_filesection(files, retention_days_files, dry_run=False)
    else:
        print("No files found in the specified path")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
