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
from databricks.sdk.service.workspace import ObjectInfo, ObjectType

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.databricks.operators.databricks_repos import (
    DatabricksReposCreateOperator,
    DatabricksReposDeleteOperator,
    DatabricksReposUpdateOperator,
)

TASK_ID = "databricks-operator"
DEFAULT_CONN_ID = "databricks_default"


class TestDatabricksReposUpdateOperator:
    @mock.patch("airflow.providers.databricks.operators.databricks_repos.DatabricksSdkHook")
    def test_update_with_id(self, db_mock_class):
        """
        Test the execute function using Repo ID.
        """
        op = DatabricksReposUpdateOperator(task_id=TASK_ID, branch="releases", repo_id="123")
        db_mock = db_mock_class.return_value
        mock_repo = mock.MagicMock()
        mock_repo.head_commit_id = "123456"
        db_mock.workspace_client.repos.update.return_value = mock_repo

        op.execute(None)

        db_mock_class.assert_called_once_with(
            databricks_conn_id=DEFAULT_CONN_ID,
            caller="DatabricksReposUpdateOperator",
        )

        db_mock.workspace_client.repos.update.assert_called_once_with(repo_id=123, branch="releases")

    @mock.patch("airflow.providers.databricks.operators.databricks_repos.DatabricksSdkHook")
    def test_update_with_path(self, db_mock_class):
        """
        Test the execute function using Repo path.
        """
        op = DatabricksReposUpdateOperator(
            task_id=TASK_ID, tag="v1.0.0", repo_path="/Repos/user@domain.com/test-repo"
        )
        db_mock = db_mock_class.return_value
        # Mock workspace.get_status to return a repo object
        mock_status = ObjectInfo(object_id=123, object_type=ObjectType.REPO, path="/Repos/user@domain.com/test-repo")
        db_mock.workspace_client.workspace.get_status.return_value = mock_status

        mock_repo = mock.MagicMock()
        mock_repo.head_commit_id = "123456"
        db_mock.workspace_client.repos.update.return_value = mock_repo

        op.execute(None)

        db_mock_class.assert_called_once_with(
            databricks_conn_id=DEFAULT_CONN_ID,
            caller="DatabricksReposUpdateOperator",
        )

        db_mock.workspace_client.repos.update.assert_called_once_with(repo_id=123, tag="v1.0.0")

    def test_init_exception(self):
        """
        Tests handling of incorrect parameters passed to ``__init__``
        """
        with pytest.raises(
            AirflowException, match="Only one of repo_id or repo_path should be provided, but not both"
        ):
            DatabricksReposUpdateOperator(task_id=TASK_ID, repo_id="abc", repo_path="path", branch="abc")

        with pytest.raises(AirflowException, match="One of repo_id or repo_path should be provided"):
            DatabricksReposUpdateOperator(task_id=TASK_ID, branch="abc")

        with pytest.raises(
            AirflowException, match="Only one of branch or tag should be provided, but not both"
        ):
            DatabricksReposUpdateOperator(task_id=TASK_ID, repo_id="123", branch="123", tag="123")

        with pytest.raises(AirflowException, match="One of branch or tag should be provided"):
            DatabricksReposUpdateOperator(task_id=TASK_ID, repo_id="123")


class TestDatabricksReposDeleteOperator:
    @mock.patch("airflow.providers.databricks.operators.databricks_repos.DatabricksSdkHook")
    def test_delete_with_id(self, db_mock_class):
        """
        Test the execute function using Repo ID.
        """
        op = DatabricksReposDeleteOperator(task_id=TASK_ID, repo_id="123")
        db_mock = db_mock_class.return_value
        db_mock.workspace_client.repos.delete.return_value = None

        op.execute(None)

        db_mock_class.assert_called_once_with(
            databricks_conn_id=DEFAULT_CONN_ID,
            caller="DatabricksReposDeleteOperator",
        )

        db_mock.workspace_client.repos.delete.assert_called_once_with(repo_id=123)

    @mock.patch("airflow.providers.databricks.operators.databricks_repos.DatabricksSdkHook")
    def test_delete_with_path(self, db_mock_class):
        """
        Test the execute function using Repo path.
        """
        op = DatabricksReposDeleteOperator(task_id=TASK_ID, repo_path="/Repos/user@domain.com/test-repo")
        db_mock = db_mock_class.return_value
        # Mock workspace.get_status to return a repo object
        mock_status = ObjectInfo(object_id=123, object_type=ObjectType.REPO, path="/Repos/user@domain.com/test-repo")
        db_mock.workspace_client.workspace.get_status.return_value = mock_status
        db_mock.workspace_client.repos.delete.return_value = None

        op.execute(None)

        db_mock_class.assert_called_once_with(
            databricks_conn_id=DEFAULT_CONN_ID,
            caller="DatabricksReposDeleteOperator",
        )

        db_mock.workspace_client.repos.delete.assert_called_once_with(repo_id=123)

    def test_init_exception(self):
        """
        Tests handling of incorrect parameters passed to ``__init__``
        """
        with pytest.raises(
            AirflowException, match="Only one of repo_id or repo_path should be provided, but not both"
        ):
            DatabricksReposDeleteOperator(task_id=TASK_ID, repo_id="abc", repo_path="path")

        with pytest.raises(AirflowException, match="One of repo_id repo_path tag should be provided"):
            DatabricksReposDeleteOperator(task_id=TASK_ID)


class TestDatabricksReposCreateOperator:
    @mock.patch("airflow.providers.databricks.operators.databricks_repos.DatabricksSdkHook")
    def test_create_plus_checkout(self, db_mock_class):
        """
        Test the execute function creating new Repo.
        """
        git_url = "https://github.com/test/test"
        repo_path = "/Repos/Project1/test-repo"
        op = DatabricksReposCreateOperator(
            task_id=TASK_ID, git_url=git_url, repo_path=repo_path, branch="releases"
        )
        db_mock = db_mock_class.return_value
        # Mock workspace.get_status to return None (repo doesn't exist)
        db_mock.workspace_client.workspace.get_status.side_effect = Exception("Not found")

        # Mock repos.create
        mock_created_repo = mock.MagicMock()
        mock_created_repo.id = 123
        db_mock.workspace_client.repos.create.return_value = mock_created_repo

        # Mock repos.update for branch checkout
        db_mock.workspace_client.repos.update.return_value = mock.MagicMock()

        op.execute(None)

        db_mock_class.assert_called_once_with(
            databricks_conn_id=DEFAULT_CONN_ID,
            caller="DatabricksReposCreateOperator",
        )

        db_mock.workspace_client.repos.create.assert_called_once_with(
            url=git_url, provider="gitHub", path=repo_path
        )
        db_mock.workspace_client.repos.update.assert_called_once_with(repo_id=123, branch="releases")

    @mock.patch("airflow.providers.databricks.operators.databricks_repos.DatabricksSdkHook")
    def test_create_ignore_existing_plus_checkout(self, db_mock_class):
        """
        Test the execute function with existing repo and ignore_existing_repo=True.
        """
        git_url = "https://github.com/test/test"
        repo_path = "/Repos/Project1/test-repo"
        op = DatabricksReposCreateOperator(
            task_id=TASK_ID,
            git_url=git_url,
            repo_path=repo_path,
            branch="releases",
            ignore_existing_repo=True,
        )
        db_mock = db_mock_class.return_value
        # Mock workspace.get_status to return existing repo
        mock_status = ObjectInfo(object_id=123, object_type=ObjectType.REPO, path=repo_path)
        db_mock.workspace_client.workspace.get_status.return_value = mock_status

        # Mock repos.update for branch checkout
        db_mock.workspace_client.repos.update.return_value = mock.MagicMock()

        op.execute(None)

        db_mock_class.assert_called_once_with(
            databricks_conn_id=DEFAULT_CONN_ID,
            caller="DatabricksReposCreateOperator",
        )

        db_mock.workspace_client.workspace.get_status.assert_called_once_with(repo_path)
        db_mock.workspace_client.repos.update.assert_called_once_with(repo_id=123, branch="releases")

    def test_init_exception(self):
        """
        Tests handling of incorrect parameters passed to ``__init__``
        """
        git_url = "https://github.com/test/test"
        repo_path = "/Repos/test-repo"
        exception_message = (
            f"repo_path should have form of /Repos/{{folder}}/{{repo-name}}, got '{repo_path}'"
        )

        op = DatabricksReposCreateOperator(task_id=TASK_ID, git_url=git_url, repo_path=repo_path)
        with pytest.raises(AirflowException, match=exception_message):
            op.execute(None)

        with pytest.raises(
            AirflowException, match="Only one of branch or tag should be provided, but not both"
        ):
            DatabricksReposCreateOperator(task_id=TASK_ID, git_url=git_url, branch="123", tag="123")
