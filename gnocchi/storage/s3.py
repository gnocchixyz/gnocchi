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
from collections import defaultdict
import contextlib
import datetime
import logging
import os
import uuid

from oslo_config import cfg
import retrying
import six
try:
    import boto3
    import botocore.exceptions
except ImportError:
    boto3 = None

from gnocchi import storage
from gnocchi.storage import _carbonara
from gnocchi import utils

LOG = logging.getLogger(__name__)

OPTS = [
    cfg.StrOpt('s3_endpoint_url',
               help='S3 endpoint URL'),
    cfg.StrOpt('s3_region_name',
               default=os.getenv("AWS_DEFAULT_REGION"),
               help='S3 region name'),
    cfg.StrOpt('s3_access_key_id',
               default=os.getenv("AWS_ACCESS_KEY_ID"),
               help='S3 access key id'),
    cfg.StrOpt('s3_secret_access_key',
               default=os.getenv("AWS_SECRET_ACCESS_KEY"),
               help='S3 secret access key'),
    cfg.StrOpt('s3_bucket_prefix',
               default='gnocchi',
               help='Prefix to namespace metric bucket.'),
]


def retry_if_operationaborted(exception):
    return (isinstance(exception, botocore.exceptions.ClientError)
            and exception.response['Error'].get('Code') == "OperationAborted")


class S3Storage(_carbonara.CarbonaraBasedStorage):

    WRITE_FULL = True

    def __init__(self, conf):
        super(S3Storage, self).__init__(conf)
        if boto3 is None:
            raise RuntimeError("boto3 unavailable")
        self.s3 = boto3.client(
            's3',
            endpoint_url=conf.s3_endpoint_url,
            region_name=conf.s3_region_name,
            aws_access_key_id=conf.s3_access_key_id,
            aws_secret_access_key=conf.s3_secret_access_key)
        self._region_name = conf.s3_region_name
        self._bucket_prefix = conf.s3_bucket_prefix
        self._bucket_name_measures = (
            self._bucket_prefix + "-" + self.MEASURE_PREFIX
        )
        try:
            self._create_bucket(self._bucket_name_measures)
        except botocore.exceptions.ClientError as e:
            if e.response['Error'].get('Code') not in (
                    "BucketAlreadyExists", "BucketAlreadyOwnedByYou"
            ):
                raise

    # NOTE(jd) OperationAborted might be raised if we try to create the bucket
    # for the first time at the same time
    @retrying.retry(stop_max_attempt_number=10,
                    wait_fixed=500,
                    retry_on_exception=retry_if_operationaborted)
    def _create_bucket(self, name):
        if self._region_name:
            kwargs = dict(CreateBucketConfiguration={
                "LocationConstraint": self._region_name,
            })
        else:
            kwargs = {}
        return self.s3.create_bucket(Bucket=name, **kwargs)

    def _bucket_name(self, metric):
        return '%s-%s' % (self._bucket_prefix, str(metric.id))

    @staticmethod
    def _object_name(split_key, aggregation, granularity, version=3):
        name = '%s_%s_%s' % (split_key, aggregation, granularity)
        return name + '_v%s' % version if version else name

    def _create_metric(self, metric):
        try:
            self._create_bucket(self._bucket_name(metric))
        except botocore.exceptions.ClientError as e:
            if e.response['Error'].get('Code') != "BucketAlreadyExists":
                raise
        # raise storage.MetricAlreadyExists(metric)

    def _store_new_measures(self, metric, data):
        now = datetime.datetime.utcnow().strftime("_%Y%m%d_%H:%M:%S")
        self.s3.put_object(
            Bucket=self._bucket_name_measures,
            Key=(six.text_type(metric.id)
                 + "/"
                 + six.text_type(uuid.uuid4())
                 + now),
            Body=data)

    def _build_report(self, details):
        metric_details = defaultdict(int)
        response = {}
        while response.get('IsTruncated', True):
            if 'NextContinuationToken' in response:
                kwargs = {
                    'ContinuationToken': response['NextContinuationToken']
                }
            else:
                kwargs = {}
            response = self.s3.list_objects_v2(
                Bucket=self._bucket_name_measures,
                **kwargs)
            for c in response.get('Contents', ()):
                metric, metric_file = c['Key'].split("/", 1)
                metric_details[metric] += 1
        return (len(metric_details), sum(metric_details.values()),
                metric_details if details else None)

    def list_metric_with_measures_to_process(self, size, part, full=False):
        if full:
            limit = 1000        # 1000 is the default anyway
        else:
            limit = size * (part + 1)

        metrics = set()
        response = {}
        # Handle pagination
        while response.get('IsTruncated', True):
            if 'NextContinuationToken' in response:
                kwargs = {
                    'ContinuationToken': response['NextContinuationToken']
                }
            else:
                kwargs = {}
            response = self.s3.list_objects_v2(
                Bucket=self._bucket_name_measures,
                Delimiter="/",
                MaxKeys=limit,
                **kwargs)
            for p in response.get('CommonPrefixes', ()):
                metrics.add(p['Prefix'].rstrip('/'))

        if full:
            return metrics

        return metrics[size * part:]

    def _list_measure_files_for_metric_id(self, metric_id):
        files = set()
        response = {}
        while response.get('IsTruncated', True):
            if 'NextContinuationToken' in response:
                kwargs = {
                    'ContinuationToken': response['NextContinuationToken']
                }
            else:
                kwargs = {}
            response = self.s3.list_objects_v2(
                Bucket=self._bucket_name_measures,
                Prefix=six.text_type(metric_id) + "/",
                **kwargs)

            for c in response.get('Contents', ()):
                files.add(c['Key'])

        return files

    def _pending_measures_to_process_count(self, metric_id):
        return len(self._list_measure_files_for_metric_id(metric_id))

    def _bulk_delete(self, bucket, objects):
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
            response = self.s3.delete_objects(
                Bucket=bucket,
                Delete=d)
            deleted += len(response['Deleted'])
        LOG.debug('%s objects deleted, %s objects skipped',
                  deleted,
                  len(objects) - deleted)

    def _delete_unprocessed_measures_for_metric_id(self, metric_id):
        files = self._list_measure_files_for_metric_id(metric_id)
        self._bulk_delete(self._bucket_name_measures, files)

    @contextlib.contextmanager
    def _process_measure_for_metric(self, metric):
        files = self._list_measure_files_for_metric_id(metric.id)

        measures = []
        for f in files:
            response = self.s3.get_object(
                Bucket=self._bucket_name_measures,
                Key=f)
            measures.extend(
                self._unserialize_measures(response['Body'].read()))

        yield measures

        # Now clean objects
        self._bulk_delete(self._bucket_name_measures, files)

    def _store_metric_measures(self, metric, timestamp_key, aggregation,
                               granularity, data, offset=0, version=3):
        self.s3.put_object(
            Bucket=self._bucket_name(metric),
            Key=self._object_name(
                timestamp_key, aggregation, granularity, version),
            Body=data)

    def _delete_metric_measures(self, metric, timestamp_key, aggregation,
                                granularity, version=3):
        self.s3.delete_object(
            Bucket=self._bucket_name(metric),
            Key=self._object_name(
                timestamp_key, aggregation, granularity, version))

    def _delete_metric(self, metric):
        self._delete_unaggregated_timeserie(metric)
        bucket = self._bucket_name(metric)
        response = {}
        while response.get('IsTruncated', True):
            if 'NextContinuationToken' in response:
                kwargs = {
                    'ContinuationToken': response['NextContinuationToken']
                }
            else:
                kwargs = {}
            try:
                response = self.s3.list_objects_v2(
                    Bucket=bucket, **kwargs)
            except botocore.exceptions.ClientError as e:
                if e.response['Error'].get('Code') == "NoSuchBucket":
                    # Maybe it never has been created (no measure)
                    return
                raise
            self._bulk_delete(bucket, [c['Key']
                                       for c in response.get('Contents', ())])
        try:
            self.s3.delete_bucket(Bucket=bucket)
        except botocore.exceptions.ClientError as e:
            if e.response['Error'].get('Code') != "NoSuchBucket":
                raise

    def _get_measures(self, metric, timestamp_key, aggregation, granularity,
                      version=3):
        try:
            response = self.s3.get_object(
                Bucket=self._bucket_name(metric),
                Key=self._object_name(
                    timestamp_key, aggregation, granularity, version))
        except botocore.exceptions.ClientError as e:
            code = e.response['Error'].get('Code')
            if code == "NoSuchBucket":
                raise storage.MetricDoesNotExist(metric)
            elif code == "NoSuchKey":
                raise storage.AggregationDoesNotExist(metric, aggregation)
            raise
        return response['Body'].read()

    def _list_split_keys_for_metric(self, metric, aggregation, granularity,
                                    version=None):
        bucket = self._bucket_name(metric)
        keys = set()
        response = {}
        while response.get('IsTruncated', True):
            if 'NextContinuationToken' in response:
                kwargs = {
                    'ContinuationToken': response['NextContinuationToken']
                }
            else:
                kwargs = {}
            try:
                response = self.s3.list_objects_v2(
                    Bucket=bucket,
                    **kwargs)
            except botocore.exceptions.ClientError as e:
                if e.response['Error'].get('Code') == "NoSuchBucket":
                    raise storage.MetricDoesNotExist(metric)
                raise
            for f in response.get('Contents', ()):
                try:
                    meta = f['Key'].split('_')
                    if (aggregation == meta[1]
                       and granularity == float(meta[2])
                       and self._version_check(f['Key'], version)):
                        keys.add(meta[0])
                except (ValueError, IndexError):
                    # Might be "none", or any other file. Be resilient.
                    continue
        return keys

    @staticmethod
    def _build_unaggregated_timeserie_path(version):
        return 'none' + ("_v%s" % version if version else "")

    def _get_unaggregated_timeserie(self, metric, version=3):
        try:
            response = self.s3.get_object(
                Bucket=self._bucket_name(metric),
                Key=self._build_unaggregated_timeserie_path(version))
        except botocore.exceptions.ClientError as e:
            if e.response['Error'].get('Code') in ("NoSuchBucket",
                                                   "NoSuchKey"):
                raise storage.MetricDoesNotExist(metric)
            raise
        return response['Body'].read()

    def _store_unaggregated_timeserie(self, metric, data, version=3):
        self.s3.put_object(
            Bucket=self._bucket_name(metric),
            Key=self._build_unaggregated_timeserie_path(version),
            Body=data)

    def _delete_unaggregated_timeserie(self, metric, version=3):
        try:
            self.s3.delete_object(
                Bucket=self._bucket_name(metric),
                Key=self._build_unaggregated_timeserie_path(version))
        except botocore.exceptions.ClientError as e:
            code = e.response['Error'].get('Code')
            if code not in ("NoSuchKey", "NoSuchBucket"):
                raise
