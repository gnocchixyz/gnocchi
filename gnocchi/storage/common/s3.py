# -*- encoding: utf-8 -*-
#
# Copyright © 2016 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import daiquiri

import tenacity
try:
    import boto3
    import botocore.exceptions
except ImportError:
    boto3 = None
    botocore = None

from gnocchi import utils

LOG = daiquiri.getLogger(__name__)


def retry_if_operationaborted(exception):
    return (isinstance(exception, botocore.exceptions.ClientError)
            and exception.response['Error'].get('Code') == "OperationAborted")


def get_connection(conf):
    if boto3 is None:
        raise RuntimeError("boto3 unavailable")
    conn = boto3.client(
        's3',
        endpoint_url=conf.s3_endpoint_url,
        region_name=conf.s3_region_name,
        aws_access_key_id=conf.s3_access_key_id,
        aws_secret_access_key=conf.s3_secret_access_key)
    return conn, conf.s3_region_name, conf.s3_bucket_prefix


# NOTE(jd) OperationAborted might be raised if we try to create the bucket
# for the first time at the same time
@tenacity.retry(
    stop=tenacity.stop_after_attempt(10),
    wait=tenacity.wait_fixed(0.5),
    retry=tenacity.retry_if_exception(retry_if_operationaborted)
)
def create_bucket(conn, name, region_name):
    if region_name:
        kwargs = dict(CreateBucketConfiguration={
            "LocationConstraint": region_name,
        })
    else:
        kwargs = {}
    return conn.create_bucket(Bucket=name, **kwargs)


def bulk_delete(conn, bucket, objects):
    # NOTE(jd) The maximum object to delete at once is 1000
    # TODO(jd) Parallelize?
    deleted = 0
    for obj_slice in utils.grouper(objects, 1000):
        d = {
            'Objects': [{'Key': o} for o in obj_slice],
            # FIXME(jd) Use Quiet mode, but s3rver does not seem to
            # support it
            # 'Quiet': True,
        }
        response = conn.delete_objects(
            Bucket=bucket,
            Delete=d)
        deleted += len(response['Deleted'])
    LOG.debug('%s objects deleted, %s objects skipped',
              deleted, len(objects) - deleted)
