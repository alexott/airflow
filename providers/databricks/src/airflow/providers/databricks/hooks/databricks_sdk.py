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

"""Hook for interacting with Databricks using the official SDK."""

from __future__ import annotations

import sys
from functools import cached_property
from typing import Any

from databricks.sdk import WorkspaceClient

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook
from airflow.providers.databricks.hooks.databricks_credentials import create_sdk_config_from_connection


class DatabricksSdkHook(BaseDatabricksHook):
    """
    Hook to interact with Databricks using the official Databricks SDK.

    Provides direct access to the WorkspaceClient from databricks-sdk package.
    Users call SDK methods directly for full SDK functionality without additional abstraction.

    This hook uses the Databricks SDK's native authentication and automatically supports
    all current and future Databricks features without requiring provider updates.

    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
    :param caller: Identifier for the calling operator (for user agent tracking).

    Example usage::

        from airflow.providers.databricks.hooks.databricks_sdk import DatabricksSdkHook

        hook = DatabricksSdkHook(databricks_conn_id="databricks_default")

        # Access Jobs API
        run = hook.workspace_client.jobs.run_now(job_id=123)
        print(f"Started run: {run.run_id}")

        # Access Clusters API
        cluster = hook.workspace_client.clusters.get(cluster_id="abc-123")
        print(f"Cluster state: {cluster.state}")

        # Access Workspace API
        objects = hook.workspace_client.workspace.list("/Users")
        for obj in objects:
            print(f"- {obj.path}")

        # Access any other SDK feature
        # See: https://databricks-sdk-py.readthedocs.io/
    """

    hook_name = "Databricks SDK"

    def __init__(
        self,
        databricks_conn_id: str = BaseDatabricksHook.default_conn_name,
        caller: str = "DatabricksSdkHook",
    ) -> None:
        super().__init__(databricks_conn_id, caller=caller)
        self._workspace_client: WorkspaceClient | None = None

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        """
        Get Databricks WorkspaceClient instance.

        The client is cached and reused for the lifetime of the hook instance.
        Authentication is handled by the SDK using credential providers configured
        from the Airflow connection.

        :return: Authenticated WorkspaceClient from databricks-sdk
        """
        if self._workspace_client is None:
            # Create SDK Config from Airflow connection
            config = create_sdk_config_from_connection(self.databricks_conn, base_hook=self)

            # Set user agent for tracking
            config.user_agent = self.user_agent_value

            # Initialize WorkspaceClient
            self._workspace_client = WorkspaceClient(config=config)

        return self._workspace_client

    def get_conn(self) -> WorkspaceClient:
        """
        Return WorkspaceClient for compatibility with base hook pattern.

        :return: Authenticated WorkspaceClient
        """
        return self.workspace_client

    def test_connection(self) -> tuple[bool, str]:
        """
        Test the Databricks SDK connection from UI.

        :return: Tuple of (success, message)
        """
        try:
            # Try to get current user as a simple connectivity test
            user = self.workspace_client.current_user.me()
            status = True
            message = f"Connection successfully tested. Authenticated as: {user.user_name}"
        except Exception as e:
            status = False
            message = str(e)

        return status, message

    # Async support methods

    async def run_async(self, func, *args, **kwargs):
        """
        Execute a synchronous SDK method asynchronously using thread pool.

        This method allows async operators to use the synchronous Databricks SDK
        without blocking the event loop. The SDK call is executed in a thread pool.

        :param func: Synchronous SDK method to call (e.g., self.workspace_client.jobs.run_now)
        :param args: Positional arguments to pass to the method
        :param kwargs: Keyword arguments to pass to the method
        :return: Result from the SDK method call

        Example usage in async operator::

            async def execute_async(self, context):
                hook = DatabricksSdkHook(databricks_conn_id=self.databricks_conn_id)

                # Run SDK method asynchronously
                run = await hook.run_async(hook.workspace_client.jobs.run_now, job_id=123)

                # Wait for completion asynchronously
                result = await hook.run_async(
                    hook.workspace_client.jobs.wait_get_run_job_terminated_or_skipped, run_id=run.run_id
                )

                return result.state.result_state
        """
        if sys.version_info < (3, 9):
            raise AirflowException(
                "Async SDK methods require Python 3.9+. "
                f"Current version: {sys.version_info.major}.{sys.version_info.minor}. "
                "Use synchronous methods instead or upgrade Python."
            )

        import asyncio

        # Use asyncio.to_thread to run sync function in default thread pool
        # This requires Python 3.9+, which is fine as Airflow 2.x requires Python 3.8+
        # and we're targeting newer versions
        return await asyncio.to_thread(func, *args, **kwargs)

    async def get_run_state_async(self, run_id: int) -> str:
        """
        Get job run state asynchronously (convenience method).

        :param run_id: The run ID
        :return: Run state string
        """
        run = await self.run_async(self.workspace_client.jobs.get_run, run_id=run_id)
        return run.state.life_cycle_state

    async def wait_for_run_async(self, run_id: int, timeout_seconds: int = 3600) -> dict[str, Any]:
        """
        Wait for job run to complete asynchronously.

        :param run_id: The run ID to wait for
        :param timeout_seconds: Maximum time to wait in seconds
        :return: Final run state information
        """
        result = await self.run_async(
            self.workspace_client.jobs.wait_get_run_job_terminated_or_skipped,
            run_id=run_id,
            timeout=timeout_seconds,
        )
        return {
            "run_id": result.run_id,
            "state": result.state.life_cycle_state,
            "result_state": result.state.result_state,
        }
