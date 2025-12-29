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

from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.databricks.hooks.databricks_credentials import (
    AirflowDatabricksCredentialAdapter,
    _get_server_hostname,
    _get_workspace_url,
    create_sdk_config_from_connection,
)


class TestWorkspaceUrlParsing:
    """Test workspace URL parsing for SDK."""

    @pytest.mark.parametrize(
        ("input_host", "expected_url"),
        [
            ("xx.cloud.databricks.com", "https://xx.cloud.databricks.com"),
            ("https://xx.cloud.databricks.com", "https://xx.cloud.databricks.com"),
            ("http://xx.cloud.databricks.com", "https://xx.cloud.databricks.com"),  # Upgrade to https
            ("https://xx.cloud.databricks.com/", "https://xx.cloud.databricks.com"),  # Strip trailing slash
            ("xx.cloud.databricks.com:443", "https://xx.cloud.databricks.com:443"),
        ],
    )
    def test_get_workspace_url(self, input_host, expected_url):
        """Test workspace URL construction from various input formats."""
        conn = Connection(conn_id="test", conn_type="databricks", host=input_host)
        assert _get_workspace_url(conn) == expected_url

    def test_get_workspace_url_from_extra(self):
        """Test workspace URL from extra field."""
        conn = Connection(
            conn_id="test",
            conn_type="databricks",
            extra='{"host": "xx.cloud.databricks.com"}',
        )
        assert _get_workspace_url(conn) == "https://xx.cloud.databricks.com"

    def test_get_workspace_url_missing_host(self):
        """Test error when host is missing."""
        conn = Connection(conn_id="test", conn_type="databricks")
        with pytest.raises(AirflowException, match="Host must be provided"):
            _get_workspace_url(conn)


class TestServerHostnameParsing:
    """Test server hostname parsing for SQL connector."""

    @pytest.mark.parametrize(
        ("input_host", "expected_hostname"),
        [
            ("xx.cloud.databricks.com", "xx.cloud.databricks.com"),
            ("https://xx.cloud.databricks.com", "xx.cloud.databricks.com"),
            ("http://xx.cloud.databricks.com", "xx.cloud.databricks.com"),
            ("https://xx.cloud.databricks.com/", "xx.cloud.databricks.com"),
        ],
    )
    def test_get_server_hostname(self, input_host, expected_hostname):
        """Test server hostname extraction (no scheme)."""
        conn = Connection(conn_id="test", conn_type="databricks", host=input_host)
        assert _get_server_hostname(conn) == expected_hostname

    def test_get_server_hostname_missing_host(self):
        """Test error when host is missing."""
        conn = Connection(conn_id="test", conn_type="databricks")
        with pytest.raises(AirflowException, match="Host must be provided"):
            _get_server_hostname(conn)


class TestAirflowDatabricksCredentialAdapter:
    """Test credential adapter wrapping BaseDatabricksHook auth."""

    @pytest.fixture
    def mock_base_hook(self):
        """Create a mock BaseDatabricksHook for testing."""
        hook = mock.MagicMock()
        hook.databricks_conn = Connection(
            conn_id="test",
            conn_type="databricks",
            host="xx.cloud.databricks.com",
            password="test_token",
        )
        hook._get_token.return_value = "test_token_value"
        hook._get_aad_headers.return_value = {}
        return hook

    def test_auth_type_pat_token_from_password(self, mock_base_hook):
        """Test auth type detection for PAT token in password field."""
        mock_base_hook.databricks_conn.login = None
        mock_base_hook.databricks_conn.password = "dapi1234567890"

        adapter = AirflowDatabricksCredentialAdapter(mock_base_hook)
        assert adapter.auth_type() == "pat-token"

    def test_auth_type_pat_token_from_extra(self, mock_base_hook):
        """Test auth type detection for PAT token in extra field."""
        mock_base_hook.databricks_conn.extra_dejson = {"token": "dapi1234567890"}

        adapter = AirflowDatabricksCredentialAdapter(mock_base_hook)
        assert adapter.auth_type() == "pat-token"

    def test_auth_type_azure_service_principal(self, mock_base_hook):
        """Test auth type detection for Azure Service Principal."""
        mock_base_hook.databricks_conn.login = "client_id"
        mock_base_hook.databricks_conn.password = "client_secret"
        mock_base_hook.databricks_conn.extra_dejson = {"azure_tenant_id": "tenant_id"}

        adapter = AirflowDatabricksCredentialAdapter(mock_base_hook)
        assert adapter.auth_type() == "azure-service-principal"

    def test_auth_type_azure_managed_identity(self, mock_base_hook):
        """Test auth type detection for Azure Managed Identity."""
        mock_base_hook.databricks_conn.extra_dejson = {"use_azure_managed_identity": True}

        adapter = AirflowDatabricksCredentialAdapter(mock_base_hook)
        assert adapter.auth_type() == "azure-managed-identity"

    def test_auth_type_azure_default_credential(self, mock_base_hook):
        """Test auth type detection for Azure Default Credential."""
        mock_base_hook.databricks_conn.extra_dejson = {"use_default_azure_credential": True}

        adapter = AirflowDatabricksCredentialAdapter(mock_base_hook)
        assert adapter.auth_type() == "azure-default-credential"

    def test_auth_type_service_principal_oauth(self, mock_base_hook):
        """Test auth type detection for Service Principal OAuth."""
        mock_base_hook.databricks_conn.login = "client_id"
        mock_base_hook.databricks_conn.password = "client_secret"
        mock_base_hook.databricks_conn.extra_dejson = {"service_principal_oauth": True}

        adapter = AirflowDatabricksCredentialAdapter(mock_base_hook)
        assert adapter.auth_type() == "oauth-m2m"

    def test_auth_type_basic_auth(self, mock_base_hook):
        """Test auth type detection for basic auth."""
        mock_base_hook.databricks_conn.login = "username"
        mock_base_hook.databricks_conn.password = "password"
        mock_base_hook.databricks_conn.extra_dejson = {}

        adapter = AirflowDatabricksCredentialAdapter(mock_base_hook)
        assert adapter.auth_type() == "basic-auth"

    def test_auth_type_unknown(self, mock_base_hook):
        """Test auth type detection for unknown configuration."""
        mock_base_hook.databricks_conn.login = None
        mock_base_hook.databricks_conn.password = None
        mock_base_hook.databricks_conn.extra_dejson = {}

        adapter = AirflowDatabricksCredentialAdapter(mock_base_hook)
        assert adapter.auth_type() == "unknown"

    def test_auth_type_caching(self, mock_base_hook):
        """Test that auth type is determined only once and cached."""
        adapter = AirflowDatabricksCredentialAdapter(mock_base_hook)

        # First call determines auth type
        auth_type1 = adapter.auth_type()

        # Second call should return cached value
        auth_type2 = adapter.auth_type()

        assert auth_type1 == auth_type2
        assert adapter._auth_type == auth_type1

    def test_call_returns_headers_with_token(self, mock_base_hook):
        """Test that calling adapter returns authorization headers."""
        mock_base_hook._get_token.return_value = "test_token_value"
        mock_base_hook._get_aad_headers.return_value = {}

        adapter = AirflowDatabricksCredentialAdapter(mock_base_hook)
        headers = adapter()

        assert headers == {"Authorization": "Bearer test_token_value"}
        mock_base_hook._get_token.assert_called_once_with(raise_error=True)
        mock_base_hook._get_aad_headers.assert_called_once()

    def test_call_includes_aad_headers(self, mock_base_hook):
        """Test that AAD headers are included when present."""
        mock_base_hook._get_token.return_value = "test_token_value"
        mock_base_hook._get_aad_headers.return_value = {
            "X-Databricks-Azure-Workspace-Resource-Id": "/subscriptions/...",
            "X-Databricks-Azure-SP-Management-Token": "mgmt_token",
        }

        adapter = AirflowDatabricksCredentialAdapter(mock_base_hook)
        headers = adapter()

        assert headers["Authorization"] == "Bearer test_token_value"
        assert headers["X-Databricks-Azure-Workspace-Resource-Id"] == "/subscriptions/..."
        assert headers["X-Databricks-Azure-SP-Management-Token"] == "mgmt_token"

    def test_call_raises_on_token_error(self, mock_base_hook):
        """Test that adapter raises error when token acquisition fails."""
        mock_base_hook._get_token.side_effect = AirflowException("Token error")

        adapter = AirflowDatabricksCredentialAdapter(mock_base_hook)

        with pytest.raises(AirflowException, match="Token error"):
            adapter()


class TestCreateSdkConfigFromConnection:
    """Test SDK Config creation from Airflow connection."""

    @mock.patch("airflow.providers.databricks.hooks.databricks_credentials.BaseDatabricksHook")
    def test_create_sdk_config_with_pat_token(self, mock_hook_class):
        """Test SDK Config creation with PAT token."""
        conn = Connection(
            conn_id="test",
            conn_type="databricks",
            host="xx.cloud.databricks.com",
            password="dapi1234567890",
        )

        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance._get_token.return_value = "dapi1234567890"
        mock_hook_instance._get_aad_headers.return_value = {}

        config = create_sdk_config_from_connection(conn)

        assert config.host == "https://xx.cloud.databricks.com"
        assert config.credentials_provider is not None

        # Test that credentials provider returns token
        headers = config.credentials_provider()
        assert "Authorization" in headers
        assert headers["Authorization"] == "Bearer dapi1234567890"

    @mock.patch("airflow.providers.databricks.hooks.databricks_credentials.BaseDatabricksHook")
    def test_create_sdk_config_with_provided_base_hook(self, mock_hook_class):
        """Test SDK Config creation with provided base hook."""
        conn = Connection(
            conn_id="test",
            conn_type="databricks",
            host="xx.cloud.databricks.com",
            password="dapi1234567890",
        )

        # Create a mock base hook to pass in
        provided_hook = mock.MagicMock()
        provided_hook.databricks_conn = conn
        provided_hook._get_token.return_value = "provided_token"
        provided_hook._get_aad_headers.return_value = {}

        config = create_sdk_config_from_connection(conn, base_hook=provided_hook)

        assert config.host == "https://xx.cloud.databricks.com"

        # Should use provided hook, not create new one
        headers = config.credentials_provider()
        assert headers["Authorization"] == "Bearer provided_token"

    @mock.patch("airflow.providers.databricks.hooks.databricks_credentials.BaseDatabricksHook")
    def test_create_sdk_config_with_azure_resource_id(self, mock_hook_class):
        """Test SDK Config with Azure cross-workspace headers."""
        conn = Connection(
            conn_id="test",
            conn_type="databricks",
            host="https://adb-xxx.azuredatabricks.net",
            login="client_id",
            password="client_secret",
            extra='{"azure_tenant_id": "tenant_id", "azure_resource_id": "/subscriptions/..."}',
        )

        mock_hook_instance = mock_hook_class.return_value
        mock_hook_instance._get_token.return_value = "workspace_token"
        mock_hook_instance._get_aad_headers.return_value = {
            "X-Databricks-Azure-Workspace-Resource-Id": "/subscriptions/...",
            "X-Databricks-Azure-SP-Management-Token": "mgmt_token",
        }

        config = create_sdk_config_from_connection(conn)

        # Test credentials include cross-workspace headers
        headers = config.credentials_provider()
        assert headers["Authorization"] == "Bearer workspace_token"
        assert "X-Databricks-Azure-Workspace-Resource-Id" in headers
        assert "X-Databricks-Azure-SP-Management-Token" in headers
