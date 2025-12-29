#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
This is an example DAG demonstrating usage of the DatabricksSdkHook.

The DatabricksSdkHook provides direct access to the Databricks SDK's WorkspaceClient,
allowing users to call any SDK method for full Databricks functionality without requiring
provider updates for new features.

Examples include:
- Listing and managing jobs
- Checking cluster status
- Listing workspace objects
- Getting current user information

For full SDK documentation, see: https://databricks-sdk-py.readthedocs.io/
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.databricks.hooks.databricks_sdk import DatabricksSdkHook

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_databricks_sdk_hook"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    tags=["example", "databricks", "sdk"],
    catchup=False,
) as dag:

    @task
    def list_jobs():
        """
        List Databricks jobs using the SDK.

        This example demonstrates calling the Jobs API directly through the WorkspaceClient.
        """
        hook = DatabricksSdkHook(databricks_conn_id="databricks_default")

        # Access Jobs API through workspace_client
        jobs = list(hook.workspace_client.jobs.list(limit=10))

        print(f"Found {len(jobs)} jobs:")
        for job in jobs:
            print(f"  - Job ID: {job.job_id}, Name: {job.settings.name}")

        return [{"job_id": job.job_id, "name": job.settings.name} for job in jobs]

    @task
    def get_cluster_info(cluster_id: str = None):
        """
        Get information about a Databricks cluster.

        This example demonstrates calling the Clusters API.

        :param cluster_id: Optional cluster ID to check (if not provided, lists all clusters)
        """
        hook = DatabricksSdkHook(databricks_conn_id="databricks_default")

        if cluster_id:
            # Get specific cluster
            cluster = hook.workspace_client.clusters.get(cluster_id=cluster_id)
            print(f"Cluster {cluster_id}:")
            print(f"  Name: {cluster.cluster_name}")
            print(f"  State: {cluster.state}")
            print(f"  Spark Version: {cluster.spark_version}")
            return {"cluster_id": cluster_id, "state": str(cluster.state)}
        # List all clusters
        clusters = list(hook.workspace_client.clusters.list())
        print(f"Found {len(clusters)} clusters:")
        for cluster in clusters:
            print(f"  - ID: {cluster.cluster_id}, Name: {cluster.cluster_name}, State: {cluster.state}")
        return [{"cluster_id": c.cluster_id, "name": c.cluster_name, "state": str(c.state)} for c in clusters]

    @task
    def list_workspace_objects(path: str = "/"):
        """
        List objects in Databricks workspace.

        This example demonstrates calling the Workspace API to list notebooks,
        directories, and other workspace objects.

        :param path: Workspace path to list (default: root)
        """
        hook = DatabricksSdkHook(databricks_conn_id="databricks_default")

        # List workspace objects
        objects = list(hook.workspace_client.workspace.list(path))

        print(f"Objects in {path}:")
        for obj in objects:
            obj_type = "DIR" if obj.object_type.value == "DIRECTORY" else obj.object_type.value
            print(f"  - [{obj_type}] {obj.path}")

        return [{"path": obj.path, "type": obj.object_type.value} for obj in objects]

    @task
    def get_current_user():
        """
        Get current authenticated user information.

        This example demonstrates a simple API call to verify authentication
        and get user details.
        """
        hook = DatabricksSdkHook(databricks_conn_id="databricks_default")

        # Get current user
        user = hook.workspace_client.current_user.me()

        print("Authenticated as:")
        print(f"  Username: {user.user_name}")
        print(f"  Display Name: {user.display_name}")
        print(f"  Active: {user.active}")

        return {"username": user.user_name, "display_name": user.display_name}

    @task
    def test_connection():
        """Test Databricks connection using the SDK hook."""
        hook = DatabricksSdkHook(databricks_conn_id="databricks_default")

        status, message = hook.test_connection()

        print(f"Connection test: {'SUCCESS' if status else 'FAILED'}")
        print(f"Message: {message}")

        return {"status": status, "message": message}

    # Define task dependencies
    test_result = test_connection()
    user_info = get_current_user()
    jobs_list = list_jobs()
    clusters_info = get_cluster_info()
    workspace_objects = list_workspace_objects("/Shared")

    # Run tests sequentially
    test_result >> user_info >> jobs_list >> clusters_info >> workspace_objects


# Example of using DatabricksSdkHook with async operations
# (Requires Python 3.9+)
@task
async def list_jobs_async():
    """
    List Databricks jobs asynchronously using the SDK.

    This example demonstrates async support for the SDK hook using thread pools.
    Requires Python 3.9+ for asyncio.to_thread() support.
    """
    hook = DatabricksSdkHook(databricks_conn_id="databricks_default")

    # Call SDK method asynchronously (runs in thread pool)
    jobs = await hook.run_async(lambda: list(hook.workspace_client.jobs.list(limit=10)))

    print(f"Found {len(jobs)} jobs (async):")
    for job in jobs:
        print(f"  - Job ID: {job.job_id}, Name: {job.settings.name}")

    return [{"job_id": job.job_id, "name": job.settings.name} for job in jobs]


# Example of checking job run status asynchronously
@task
async def check_run_status_async(run_id: int):
    """
    Check Databricks job run status asynchronously.

    :param run_id: The run ID to check
    """
    hook = DatabricksSdkHook(databricks_conn_id="databricks_default")

    # Get run state asynchronously
    state = await hook.get_run_state_async(run_id=run_id)

    print(f"Run {run_id} state: {state}")

    return {"run_id": run_id, "state": state}


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
