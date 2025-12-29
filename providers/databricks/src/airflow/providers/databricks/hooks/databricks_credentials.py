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

"""SDK credential adapter wrapping existing BaseDatabricksHook authentication."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from databricks.sdk.core import CredentialsStrategy

from airflow.providers.common.compat.sdk import AirflowException

if TYPE_CHECKING:
    from databricks.sdk import Config

    from airflow.models.connection import Connection
    from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook


class AirflowDatabricksCredentialAdapter(CredentialsStrategy):
    """
    Adapter that wraps BaseDatabricksHook authentication for SDK usage.

    This adapter reuses all existing authentication logic including:
    - Token refresh and caching
    - Azure AAD token acquisition (SPN, MI, Default Credential)
    - Service Principal OAuth
    - Cross-workspace headers for Azure
    - Authentication precedence

    Instead of reimplementing this logic, we wrap the existing methods and
    expose them in SDK CredentialsStrategy format.

    :param base_hook: BaseDatabricksHook instance for auth operations
    """

    def __init__(self, base_hook: BaseDatabricksHook) -> None:
        self.base_hook = base_hook
        self._auth_type: str | None = None

    def auth_type(self) -> str:
        """Return authentication type identifier based on connection config."""
        if self._auth_type:
            return self._auth_type

        conn = self.base_hook.databricks_conn

        # Determine auth type from connection (preserves precedence)
        if "token" in conn.extra_dejson or (not conn.login and conn.password):
            self._auth_type = "pat-token"
        elif "azure_tenant_id" in conn.extra_dejson:
            self._auth_type = "azure-service-principal"
        elif conn.extra_dejson.get("use_azure_managed_identity", False):
            self._auth_type = "azure-managed-identity"
        elif conn.extra_dejson.get("use_default_azure_credential", False):
            self._auth_type = "azure-default-credential"
        elif conn.extra_dejson.get("service_principal_oauth", False):
            self._auth_type = "oauth-m2m"
        elif conn.login and conn.password:
            self._auth_type = "basic-auth"
        else:
            self._auth_type = "unknown"

        return self._auth_type

    def __call__(self, *args, **kwargs) -> dict[str, str]:
        """
        Return authorization headers with token.

        Uses existing BaseDatabricksHook._get_token() which handles:
        - Token refresh and caching
        - All auth types
        - Azure cross-workspace scenarios

        Also includes Azure cross-workspace headers via existing _get_aad_headers().
        """
        # Get token using existing logic (handles caching, refresh, all auth types)
        token = self.base_hook._get_token(raise_error=True)

        headers = {"Authorization": f"Bearer {token}"}

        # Add Azure cross-workspace headers if configured
        # This reuses existing _get_aad_headers() which handles:
        # - azure_resource_id presence check
        # - Management token acquisition for SPN/MI
        # - Proper header names
        aad_headers = self.base_hook._get_aad_headers()
        if aad_headers:
            headers.update(aad_headers)

        return headers


def _get_workspace_url(conn: Connection) -> str:
    """
    Get full workspace URL for SDK (includes https:// scheme).

    Handles various user input formats:
    - hostname only: prepend https://
    - https://hostname: use as-is
    - https://hostname/: strip trailing slash
    """
    host = conn.host or conn.extra_dejson.get("host")
    if not host:
        raise AirflowException("Host must be provided in Databricks connection")

    # Parse the host
    parsed = urlsplit(host if "://" in host else f"https://{host}")

    # Reconstruct with https scheme, no trailing slash
    if not parsed.scheme:
        scheme = "https"
    elif parsed.scheme == "http":
        # Upgrade http to https for Databricks
        scheme = "https"
    else:
        scheme = parsed.scheme

    hostname = parsed.hostname or host
    port = f":{parsed.port}" if parsed.port else ""

    return f"{scheme}://{hostname}{port}"


def _get_server_hostname(conn: Connection) -> str:
    """
    Get server hostname only (no scheme) for SQL connector.

    This preserves existing BaseDatabricksHook.host behavior.
    """
    host = conn.host or conn.extra_dejson.get("host")
    if not host:
        raise AirflowException("Host must be provided in Databricks connection")

    # Reuse existing parse logic
    urlparse_host = urlsplit(host).hostname
    if urlparse_host:
        return urlparse_host
    return host


def create_sdk_config_from_connection(
    conn: Connection, base_hook: BaseDatabricksHook | None = None
) -> Config:
    """
    Convert Airflow Databricks connection to SDK Config object.

    Uses adapter pattern to wrap existing BaseDatabricksHook authentication logic.
    This preserves all existing auth behavior including:
    - Token refresh and caching
    - Authentication precedence
    - Azure cross-workspace headers
    - Service Principal OAuth token acquisition

    :param conn: Airflow connection object
    :param base_hook: Optional BaseDatabricksHook for auth adapter (created if not provided)
    :return: Databricks SDK Config object
    """
    from databricks.sdk import Config

    from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook

    # Get full workspace URL for SDK (not just hostname)
    workspace_url = _get_workspace_url(conn)

    # Create or use provided base hook for authentication
    if base_hook is None:
        # Create minimal base hook just for auth operations
        # Note: conn_id doesn't matter here as we're passing the connection directly
        base_hook = BaseDatabricksHook(databricks_conn_id=conn.conn_id)
        # Inject the connection to avoid lookup
        base_hook._databricks_conn = conn

    # Create credential adapter wrapping existing auth logic
    credentials_provider = AirflowDatabricksCredentialAdapter(base_hook)

    # Create SDK Config with our credential provider
    # The adapter will handle all auth types through existing logic
    return Config(host=workspace_url, credentials_provider=credentials_provider)
