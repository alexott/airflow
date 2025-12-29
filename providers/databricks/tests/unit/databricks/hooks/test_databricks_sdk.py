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
from __future__ import annotations

import sys
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.databricks.hooks.databricks_sdk import DatabricksSdkHook

DEFAULT_CONN_ID = "databricks_default"


class TestDatabricksSdkHook:
    """Test DatabricksSdkHook."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock Databricks connection."""
        return Connection(
            conn_id=DEFAULT_CONN_ID,
            conn_type="databricks",
            host="xx.cloud.databricks.com",
            password="dapi1234567890",
        )

    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.create_sdk_config_from_connection")
    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.WorkspaceClient")
    @mock.patch.object(DatabricksSdkHook, "get_connection")
    def test_workspace_client_initialization(
        self, mock_get_connection, mock_workspace_client, mock_create_config, mock_connection
    ):
        """Test WorkspaceClient is initialized correctly."""
        mock_get_connection.return_value = mock_connection
        mock_config = mock.MagicMock()
        mock_create_config.return_value = mock_config

        hook = DatabricksSdkHook(databricks_conn_id=DEFAULT_CONN_ID)
        client = hook.workspace_client

        # Verify config was created
        mock_create_config.assert_called_once()
        call_args = mock_create_config.call_args
        assert call_args[0][0] == mock_connection  # First positional arg is connection
        assert call_args[1]["base_hook"] == hook  # base_hook kwarg is the hook itself

        # Verify WorkspaceClient was initialized with config
        mock_workspace_client.assert_called_once_with(config=mock_config)

        # Verify user agent was set
        assert mock_config.user_agent == hook.user_agent_value

    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.create_sdk_config_from_connection")
    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.WorkspaceClient")
    @mock.patch.object(DatabricksSdkHook, "get_connection")
    def test_workspace_client_caching(
        self, mock_get_connection, mock_workspace_client, mock_create_config, mock_connection
    ):
        """Test that WorkspaceClient is cached and reused."""
        mock_get_connection.return_value = mock_connection
        mock_create_config.return_value = mock.MagicMock()

        hook = DatabricksSdkHook(databricks_conn_id=DEFAULT_CONN_ID)

        # Access workspace_client multiple times
        client1 = hook.workspace_client
        client2 = hook.workspace_client
        client3 = hook.workspace_client

        # WorkspaceClient should be created only once
        assert mock_workspace_client.call_count == 1
        assert client1 == client2 == client3

    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.create_sdk_config_from_connection")
    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.WorkspaceClient")
    @mock.patch.object(DatabricksSdkHook, "get_connection")
    def test_get_conn_returns_workspace_client(
        self, mock_get_connection, mock_workspace_client, mock_create_config, mock_connection
    ):
        """Test that get_conn returns WorkspaceClient."""
        mock_get_connection.return_value = mock_connection
        mock_create_config.return_value = mock.MagicMock()

        hook = DatabricksSdkHook(databricks_conn_id=DEFAULT_CONN_ID)
        conn = hook.get_conn()

        assert conn == hook.workspace_client

    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.create_sdk_config_from_connection")
    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.WorkspaceClient")
    @mock.patch.object(DatabricksSdkHook, "get_connection")
    def test_test_connection_success(
        self, mock_get_connection, mock_workspace_client, mock_create_config, mock_connection
    ):
        """Test successful connection test."""
        mock_get_connection.return_value = mock_connection
        mock_create_config.return_value = mock.MagicMock()

        # Mock current_user.me() to return user info
        mock_client_instance = mock_workspace_client.return_value
        mock_user = mock.MagicMock()
        mock_user.user_name = "test.user@example.com"
        mock_client_instance.current_user.me.return_value = mock_user

        hook = DatabricksSdkHook(databricks_conn_id=DEFAULT_CONN_ID)
        status, message = hook.test_connection()

        assert status is True
        assert "test.user@example.com" in message
        assert "successfully tested" in message.lower()

    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.create_sdk_config_from_connection")
    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.WorkspaceClient")
    @mock.patch.object(DatabricksSdkHook, "get_connection")
    def test_test_connection_failure(
        self, mock_get_connection, mock_workspace_client, mock_create_config, mock_connection
    ):
        """Test failed connection test."""
        mock_get_connection.return_value = mock_connection
        mock_create_config.return_value = mock.MagicMock()

        # Mock current_user.me() to raise exception
        mock_client_instance = mock_workspace_client.return_value
        mock_client_instance.current_user.me.side_effect = Exception("Authentication failed")

        hook = DatabricksSdkHook(databricks_conn_id=DEFAULT_CONN_ID)
        status, message = hook.test_connection()

        assert status is False
        assert "Authentication failed" in message

    def test_init_with_custom_params(self):
        """Test hook initialization with custom parameters."""
        custom_conn_id = "custom_databricks"
        custom_caller = "CustomOperator"

        hook = DatabricksSdkHook(databricks_conn_id=custom_conn_id, caller=custom_caller)

        assert hook.databricks_conn_id == custom_conn_id
        assert hook.caller == custom_caller

    def test_init_with_default_params(self):
        """Test hook initialization with default parameters."""
        hook = DatabricksSdkHook()

        assert hook.databricks_conn_id == "databricks_default"
        assert hook.caller == "DatabricksSdkHook"

    def test_hook_name(self):
        """Test hook name is set correctly."""
        assert DatabricksSdkHook.hook_name == "Databricks SDK"


class TestDatabricksSdkHookAsync:
    """Test async methods of DatabricksSdkHook."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock Databricks connection."""
        return Connection(
            conn_id=DEFAULT_CONN_ID,
            conn_type="databricks",
            host="xx.cloud.databricks.com",
            password="dapi1234567890",
        )

    @pytest.mark.skipif(sys.version_info < (3, 9), reason="Async methods require Python 3.9+")
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.create_sdk_config_from_connection")
    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.WorkspaceClient")
    @mock.patch.object(DatabricksSdkHook, "get_connection")
    async def test_run_async(
        self, mock_get_connection, mock_workspace_client, mock_create_config, mock_connection
    ):
        """Test run_async executes SDK method in thread pool."""
        mock_get_connection.return_value = mock_connection
        mock_create_config.return_value = mock.MagicMock()

        hook = DatabricksSdkHook(databricks_conn_id=DEFAULT_CONN_ID)

        # Create a simple sync function to test
        def sync_func(x, y):
            return x + y

        result = await hook.run_async(sync_func, 1, 2)
        assert result == 3

    @pytest.mark.skipif(sys.version_info < (3, 9), reason="Async methods require Python 3.9+")
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.create_sdk_config_from_connection")
    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.WorkspaceClient")
    @mock.patch.object(DatabricksSdkHook, "get_connection")
    async def test_get_run_state_async(
        self, mock_get_connection, mock_workspace_client, mock_create_config, mock_connection
    ):
        """Test get_run_state_async returns run state."""
        mock_get_connection.return_value = mock_connection
        mock_create_config.return_value = mock.MagicMock()

        # Mock jobs.get_run to return run with state
        mock_client_instance = mock_workspace_client.return_value
        mock_run = mock.MagicMock()
        mock_run.state.life_cycle_state = "RUNNING"
        mock_client_instance.jobs.get_run.return_value = mock_run

        hook = DatabricksSdkHook(databricks_conn_id=DEFAULT_CONN_ID)
        state = await hook.get_run_state_async(run_id=123)

        assert state == "RUNNING"

    @pytest.mark.skipif(sys.version_info < (3, 9), reason="Async methods require Python 3.9+")
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.create_sdk_config_from_connection")
    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.WorkspaceClient")
    @mock.patch.object(DatabricksSdkHook, "get_connection")
    async def test_wait_for_run_async(
        self, mock_get_connection, mock_workspace_client, mock_create_config, mock_connection
    ):
        """Test wait_for_run_async waits for completion."""
        mock_get_connection.return_value = mock_connection
        mock_create_config.return_value = mock.MagicMock()

        # Mock wait_get_run_job_terminated_or_skipped to return completed run
        mock_client_instance = mock_workspace_client.return_value
        mock_result = mock.MagicMock()
        mock_result.run_id = 123
        mock_result.state.life_cycle_state = "TERMINATED"
        mock_result.state.result_state = "SUCCESS"
        mock_client_instance.jobs.wait_get_run_job_terminated_or_skipped.return_value = mock_result

        hook = DatabricksSdkHook(databricks_conn_id=DEFAULT_CONN_ID)
        result = await hook.wait_for_run_async(run_id=123, timeout_seconds=600)

        assert result["run_id"] == 123
        assert result["state"] == "TERMINATED"
        assert result["result_state"] == "SUCCESS"

    @pytest.mark.skipif(sys.version_info >= (3, 9), reason="Test for Python < 3.9")
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.create_sdk_config_from_connection")
    @mock.patch("airflow.providers.databricks.hooks.databricks_sdk.WorkspaceClient")
    @mock.patch.object(DatabricksSdkHook, "get_connection")
    async def test_run_async_python_38_raises_error(
        self, mock_get_connection, mock_workspace_client, mock_create_config, mock_connection
    ):
        """Test that async methods raise error on Python 3.8."""
        mock_get_connection.return_value = mock_connection
        mock_create_config.return_value = mock.MagicMock()

        hook = DatabricksSdkHook(databricks_conn_id=DEFAULT_CONN_ID)

        with pytest.raises(AirflowException, match="Async SDK methods require Python 3.9+"):
            await hook.run_async(lambda: None)
