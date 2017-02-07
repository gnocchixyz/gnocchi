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
import logging
import os

from oslo_config import cfg

from gnocchi import storage
from gnocchi.storage import _carbonara
from gnocchi.storage.common import s3

boto3 = s3.boto3
botocore = s3.botocore

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
               # Max bucket length is 63 and we use "-" as separator
               # 63 - 1 - len(uuid) = 26
               max_length=26,
               default='gnocchi',
               help='Prefix to namespace metric bucket.'),
]


def retry_if_operationaborted(exception):
    return (isinstance(exception, botocore.exceptions.ClientError)
            and exception.response['Error'].get('Code') == "OperationAborted")


class S3Storage(_carbonara.CarbonaraBasedStorage):

    WRITE_FULL = True

    def __init__(self, conf, incoming):
        super(S3Storage, self).__init__(conf, incoming)
        self.s3, self._region_name, self._bucket_prefix = (
            s3.get_connection(conf)
        )

    def _bucket_name(self, metric):
        return '%s-%s' % (self._bucket_prefix, str(metric.id))

    @staticmethod
    def _object_name(split_key, aggregation, granularity, version=3):
        name = '%s_%s_%s' % (split_key, aggregation, granularity)
        return name + '_v%s' % version if version else name

    def _create_metric(self, metric):
        try:
            s3.create_bucket(self.s3, self._bucket_name(metric),
                             self._region_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error'].get('Code') != "BucketAlreadyExists":
                raise
        # raise storage.MetricAlreadyExists(metric)

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
            s3.bulk_delete(self.s3, bucket,
                           [c['Key'] for c in response.get('Contents', ())])
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
