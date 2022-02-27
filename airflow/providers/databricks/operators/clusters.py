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
#
"""This module contains Databricks operators."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union
from dataclasses import dataclass

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


@dataclass
class Autoscale:
    """class to represent Autoscale data structure in the Databricks cluster specification"""
    min: int
    max: int


class BaseStorageInfo(object):
    def __init__(self, storage_type: str):
        self.storage_type = storage_type

    def __call__(self):
        return {self.storage_type: self.__dict__ }


class DBFSStorageInfo(BaseStorageInfo):
    def __init__(self, destination: str):
        super().__init__("dbfs")
        self.destination = destination


class FileStorageInfo(BaseStorageInfo):
    def __init__(self, destination: str):
        super().__init__("file")
        self.destination = destination


class S3StorageInfo(BaseStorageInfo):
    def __init__(self, destination: str, region):
        super().__init__("s3")
        self.destination = destination


class DatabricksCluster(object):
    def __init__(self,
                 spark_version: str,
                 node_type_id: str,
                 num_workers: Union[int, Autoscale],
                 name: Optional[str] = None,
                 spark_conf: Optional[Dict[str, str]] = None,
                 driver_node_type_id: Optional[str] = None,
                 ssh_public_keys: Optional[List[str]] = None,
                 custom_tags: Optional[Dict[str, str]] = None,
                 spark_env_vars: Optional[Dict[str, str]] = None,
                 aws_attributes: Optional[Dict[str, str]] = None,
                 azure_attributes: Optional[Dict[str, str]] = None,
                 gcp_attributes: Optional[Dict[str, str]] = None,
                 policy_id: Optional[str] = None,
                 autotermination_minutes: int = 120,
                 init_scripts: Optional[List[BaseStorageInfo], Dict[str, Any]] = None,
                 enable_elastic_disk: Optional[bool] = None,
                 driver_instance_pool_id: Optional[str] = None,
                 instance_pool_id: Optional[str] = None,
                 idempotency_token: Optional[str] = None,
                 apply_policy_default_values: Optional[bool] = None,
                 enable_local_disk_encryption: Optional[bool] = None,
                 cluster_log_conf: Optional[Union[DBFSStorageInfo, S3StorageInfo, Dict[str, Any]]] = None,
                 ):
        self.spark_version = spark_version
        self.node_type_id = node_type_id
        if isinstance(num_workers, int):
            self.num_workers = num_workers
        elif isinstance(num_workers, Autoscale):
            self.autoscale = {"min_workers": num_workers.min, "max_workers": num_workers.max}
        else:
            raise AirflowException(f"Incorrect type for num_workers parameter: {type(num_workers)}")
        if aws_attributes is not None and azure_attributes is None and gcp_attributes is None:
            self.aws_attributes = aws_attributes
        elif azure_attributes is not None and aws_attributes is None and gcp_attributes is None:
            self.azure_attributes = azure_attributes
        elif gcp_attributes is not None and azure_attributes is None and aws_attributes is None:
            self.gcp_attributes = gcp_attributes
        else:
            raise AirflowException("Only one of aws_attributes, azure_attributes or gcp_attributes could be specified")
        if name is not None:
            self.cluster_name = name
        if spark_conf is not None:
            self.spark_conf = spark_conf
        if driver_node_type_id is not None:
            self.driver_node_type_id = driver_node_type_id
        if ssh_public_keys is not None:
            self.ssh_public_keys = ssh_public_keys
        if custom_tags is not None:
            self.custom_tags = custom_tags
        if spark_env_vars is not None:
            self.spark_env_vars = spark_env_vars
        if policy_id is not None:
            self.policy_id = policy_id
        self.autotermination_minutes = autotermination_minutes
        if init_scripts is not None:
            self.init_scripts = [x() if isinstance(x, BaseStorageInfo) else x for x in init_scripts]
        if enable_elastic_disk is not None:
            self.enable_elastic_disk = enable_elastic_disk
        if driver_instance_pool_id is not None:
            self.driver_instance_pool_id = driver_instance_pool_id
        if instance_pool_id is not None:
            self.instance_pool_id = instance_pool_id
        if idempotency_token is not None:
            self.idempotency_token = idempotency_token
        if apply_policy_default_values is not None:
            self.apply_policy_default_values = apply_policy_default_values
        if enable_local_disk_encryption is not None:
            self.enable_local_disk_encryption = enable_local_disk_encryption
        if cluster_log_conf:
            if isinstance(cluster_log_conf, BaseStorageInfo):
                self.cluster_log_conf = cluster_log_conf()
            else:
                self.cluster_log_conf = cluster_log_conf

    def __call__(self):
        return self.__dict__


class DatabricksCreateClusterOperator(BaseOperator):
    """
    Creates a Databricks cluster using the
    `api/2.0/clusters/create
    <https://docs.databricks.com/dev-tools/api/latest/clusters.html#create>`_
    API endpoint.

    There are two ways to instantiate this operator.

    In the first way, you can take the JSON payload that you typically use
    to call the ``api/2.0/clusters/create`` endpoint and pass it directly
    to our ``DatabricksCreateClusterOperator`` through the ``json`` parameter.
    For example ::

        json = {
            'spark_version': '2.1.0-db3-scala2.11',
            'num_workers': 2
        }
        notebook_run = DatabricksSubmitRunOperator(task_id='notebook_run', json=json)

    Another way to accomplish the same thing is to use the named parameters
    of the ``DatabricksSubmitRunOperator`` directly. Note that there is exactly
    one named parameter for each top level parameter in the ``runs/submit``
    endpoint. In this method, your code would look like this: ::

        new_cluster = {
          'spark_version': '10.1.x-scala2.12',
          'num_workers': 2
        }
        notebook_task = {
          'notebook_path': '/Users/airflow@example.com/PrepareData',
        }
        notebook_run = DatabricksSubmitRunOperator(
            task_id='notebook_run',
            new_cluster=new_cluster,
            notebook_task=notebook_task)

    In the case where both the json parameter **AND** the named parameters
    are provided, they will be merged together. If there are conflicts during the merge,
    the named parameters will take precedence and override the top level ``json`` keys.

    Currently the named parameters that ``DatabricksSubmitRunOperator`` supports are
        - ``spark_jar_task``
        - ``notebook_task``
        - ``spark_python_task``
        - ``spark_jar_task``
        - ``spark_submit_task``
        - ``pipeline_task``
        - ``new_cluster``
        - ``existing_cluster_id``
        - ``libraries``
        - ``run_name``
        - ``timeout_seconds``

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DatabricksSubmitRunOperator`

    :param json: A JSON object containing API parameters which will be passed
        directly to the ``api/2.1/jobs/runs/submit`` endpoint. The other named parameters
        (i.e. ``spark_jar_task``, ``notebook_task``..) to this operator will
        be merged with this json dictionary if they are provided.
        If there are conflicts during the merge, the named parameters will
        take precedence and override the top level json keys. (templated)

        .. seealso::
            For more information about templating see :ref:`concepts:jinja-templating`.
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit

    :param timeout_seconds: The timeout for this run. By default a value of 0 is used
        which means to have no timeout.
        This field will be templated.
    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty.
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :param databricks_retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    :param do_xcom_push: Whether we should push run_id and run_page_url to xcom.
    """

    # Used in airflow.models.BaseOperator
    template_fields: Sequence[str] = ('json',)
    template_ext: Sequence[str] = ('.json',)
    # Databricks brand color (blue) under white text
    ui_color = '#1CB1C2'
    ui_fgcolor = '#fff'

    def __init__(
        self,
        *,
        cluster_spec: Union[DatabricksCluster, Dict[str, Any]] = None,
        databricks_conn_id: str = 'databricks_default',
        polling_period_seconds: int = 30,
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 1,
        do_xcom_push: bool = False,
        **kwargs,
    ) -> None:
        """Creates a new ``DatabricksCreateClusterOperator``."""
        super().__init__(**kwargs)
        if isinstance(cluster_spec, DatabricksCluster):
            self.json = cluster_spec()
        else:
            self.json = cluster_spec
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        # This variable will be used in case our task gets killed.
        self.cluster_id: Optional[int] = None
        self.do_xcom_push = do_xcom_push

    def _get_hook(self) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
        )

    def execute(self, context: 'Context'):
        hook = self._get_hook()
        # self.run_id = hook.submit_run(self.json)
        #_handle_databricks_operator_execution(self, hook, self.log, context)



