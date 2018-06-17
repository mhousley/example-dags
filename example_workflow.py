# -*- coding: utf-8 -*-
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

import airflow
from airflow.models import DAG
from airflow.operators import BaseOperator
from datetime import timedelta
from airflow.contrib.hooks.aws_hook import AwsHook


class StartCrawler(BaseOperator):
    def __init__(
            self,
            *args,
            **kwargs):
        super(StartCrawler, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info('Triggering crawler.')
        glue_hook = AwsHook()
        client = glue_hook.get_client_type(client_type='glue')
        client.start_crawler(Name='s3 crawler')


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='example_bash_operator', default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60))

run_this = StartCrawler(
    task_id='start_crawler', dag=dag)

if __name__ == "__main__":
    dag.cli()
