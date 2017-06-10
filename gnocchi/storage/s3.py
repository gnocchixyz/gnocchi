# -*- encoding: utf-8 -*-
#
# Copyright © 2016-2017 Red Hat, Inc.
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
import os

from oslo_config import cfg
import tenacity

from gnocchi import storage
from gnocchi.storage import _carbonara
from gnocchi.storage.common import s3

boto3 = s3.boto3
botocore = s3.botocore

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
    cfg.FloatOpt('s3_check_consistency_timeout',
                 min=0,
                 default=60,
                 help="Maximum time to wait checking data consistency when "
                 "writing to S3. Set to 0 to disable data consistency "
                 "validation."),
]


def retry_if_operationaborted(exception):
    return (isinstance(exception, botocore.exceptions.ClientError)
            and exception.response['Error'].get('Code') == "OperationAborted")


class S3Storage(_carbonara.CarbonaraBasedStorage):

    WRITE_FULL = True

    _consistency_wait = tenacity.wait_exponential(multiplier=0.1)

    def __init__(self, conf, incoming, coord=None):
        super(S3Storage, self).__init__(conf, incoming, coord)
        self.s3, self._region_name, self._bucket_prefix = (
            s3.get_connection(conf)
        )
        self._bucket_name = '%s-aggregates' % self._bucket_prefix
        if conf.s3_check_consistency_timeout > 0:
            self._consistency_stop = tenacity.stop_after_delay(
                conf.s3_check_consistency_timeout)
        else:
            self._consistency_stop = None

    def upgrade(self, num_sacks):
        super(S3Storage, self).upgrade(num_sacks)
        try:
            s3.create_bucket(self.s3, self._bucket_name, self._region_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error'].get('Code') != "BucketAlreadyExists":
                raise

    @staticmethod
    def _object_name(split_key, aggregation, granularity, version=3):
        name = '%s_%s_%s' % (aggregation, granularity, split_key)
        return name + '_v%s' % version if version else name

    @staticmethod
    def _prefix(metric):
        return str(metric.id) + '/'

    def _create_metric(self, metric):
        pass

    def _put_object_safe(self, Bucket, Key, Body):
        put = self.s3.put_object(Bucket=Bucket, Key=Key, Body=Body)

        if self._consistency_stop:

            def _head():
                return self.s3.head_object(Bucket=Bucket,
                                           Key=Key, IfMatch=put['ETag'])

            tenacity.Retrying(
                retry=tenacity.retry_if_result(
                    lambda r: r['ETag'] != put['ETag']),
                wait=self._consistency_wait,
                stop=self._consistency_stop)(_head)

    def _store_metric_measures(self, metric, timestamp_key, aggregation,
                               granularity, data, offset=0, version=3):
        self._put_object_safe(
            Bucket=self._bucket_name,
            Key=self._prefix(metric) + self._object_name(
                timestamp_key, aggregation, granularity, version),
            Body=data)

    def _delete_metric_measures(self, metric, timestamp_key, aggregation,
                                granularity, version=3):
        self.s3.delete_object(
            Bucket=self._bucket_name,
            Key=self._prefix(metric) + self._object_name(
                timestamp_key, aggregation, granularity, version))

    def _delete_metric(self, metric):
        bucket = self._bucket_name
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
                    Bucket=bucket, Prefix=self._prefix(metric), **kwargs)
            except botocore.exceptions.ClientError as e:
                if e.response['Error'].get('Code') == "NoSuchKey":
                    # Maybe it never has been created (no measure)
                    return
                raise
            s3.bulk_delete(self.s3, bucket,
                           [c['Key'] for c in response.get('Contents', ())])

    def _get_measures(self, metric, timestamp_key, aggregation, granularity,
                      version=3):
        try:
            response = self.s3.get_object(
                Bucket=self._bucket_name,
                Key=self._prefix(metric) + self._object_name(
                    timestamp_key, aggregation, granularity, version))
        except botocore.exceptions.ClientError as e:
            if e.response['Error'].get('Code') == 'NoSuchKey':
                try:
                    response = self.s3.list_objects_v2(
                        Bucket=self._bucket_name, Prefix=self._prefix(metric))
                except botocore.exceptions.ClientError as e:
                    if e.response['Error'].get('Code') == 'NoSuchKey':
                        raise storage.MetricDoesNotExist(metric)
                    raise
                raise storage.AggregationDoesNotExist(metric, aggregation)
            raise
        return response['Body'].read()

    def _list_split_keys_for_metric(self, metric, aggregation, granularity,
                                    version=3):
        bucket = self._bucket_name
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
                    Prefix=self._prefix(metric) + '%s_%s' % (aggregation,
                                                             granularity),
                    **kwargs)
            except botocore.exceptions.ClientError as e:
                if e.response['Error'].get('Code') == "NoSuchKey":
                    raise storage.MetricDoesNotExist(metric)
                raise
            for f in response.get('Contents', ()):
                try:
                    meta = f['Key'].split('_')
                    if (self._version_check(f['Key'], version)):
                        keys.add(meta[2])
                except (ValueError, IndexError):
                    # Might be "none", or any other file. Be resilient.
                    continue
        return keys

    @staticmethod
    def _build_unaggregated_timeserie_path(metric, version):
        return S3Storage._prefix(metric) + 'none' + ("_v%s" % version
                                                     if version else "")

    def _get_unaggregated_timeserie(self, metric, version=3):
        try:
            response = self.s3.get_object(
                Bucket=self._bucket_name,
                Key=self._build_unaggregated_timeserie_path(metric, version))
        except botocore.exceptions.ClientError as e:
            if e.response['Error'].get('Code') == "NoSuchKey":
                raise storage.MetricDoesNotExist(metric)
            raise
        return response['Body'].read()

    def _store_unaggregated_timeserie(self, metric, data, version=3):
        self._put_object_safe(
            Bucket=self._bucket_name,
            Key=self._build_unaggregated_timeserie_path(metric, version),
            Body=data)
