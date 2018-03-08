# -*- encoding: utf-8 -*-
#
# Copyright © 2016-2018 Red Hat, Inc.
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

from gnocchi import carbonara
from gnocchi.common import s3
from gnocchi import storage
from gnocchi import utils

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
    cfg.IntOpt('s3_max_pool_connections',
               min=1,
               default=50,
               help="The maximum number of connections to keep in a "
               "connection pool."),
]


def retry_if_operationaborted(exception):
    return (isinstance(exception, botocore.exceptions.ClientError)
            and exception.response['Error'].get('Code') == "OperationAborted")


class S3Storage(storage.StorageDriver):

    WRITE_FULL = True

    _consistency_wait = tenacity.wait_exponential(multiplier=0.1)

    def __init__(self, conf):
        super(S3Storage, self).__init__(conf)
        self.s3, self._region_name, self._bucket_prefix = (
            s3.get_connection(conf)
        )
        self._bucket_name = '%s-aggregates' % self._bucket_prefix
        if conf.s3_check_consistency_timeout > 0:
            self._consistency_stop = tenacity.stop_after_delay(
                conf.s3_check_consistency_timeout)
        else:
            self._consistency_stop = None

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, self._bucket_name)

    def upgrade(self):
        super(S3Storage, self).upgrade()
        try:
            s3.create_bucket(self.s3, self._bucket_name, self._region_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error'].get('Code') != "BucketAlreadyExists":
                raise

    @staticmethod
    def _object_name(split_key, aggregation, version=3):
        name = '%s_%s_%s' % (
            aggregation,
            utils.timespan_total_seconds(split_key.sampling),
            split_key,
        )
        return name + '_v%s' % version if version else name

    @staticmethod
    def _prefix(metric):
        return str(metric.id) + '/'

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

    def _store_metric_splits_unbatched(self, metric, key, aggregation, data,
                                       offset, version):
        self._put_object_safe(
            Bucket=self._bucket_name,
            Key=self._prefix(metric) + self._object_name(
                key, aggregation.method, version),
            Body=data)

    def _delete_metric_splits_unbatched(self, metric, key, aggregation,
                                        version=3):
        self.s3.delete_object(
            Bucket=self._bucket_name,
            Key=self._prefix(metric) + self._object_name(
                key, aggregation.method, version))

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

    def _get_splits_unbatched(self, metric, key, aggregation, version=3):
        try:
            response = self.s3.get_object(
                Bucket=self._bucket_name,
                Key=self._prefix(metric) + self._object_name(
                    key, aggregation.method, version))
        except botocore.exceptions.ClientError as e:
            if e.response['Error'].get('Code') == 'NoSuchKey':
                return
            raise
        return response['Body'].read()

    def _metric_exists_p(self, metric, version):
        unaggkey = self._build_unaggregated_timeserie_path(metric, version)
        try:
            self.s3.head_object(Bucket=self._bucket_name, Key=unaggkey)
        except botocore.exceptions.ClientError as e:
            if e.response['Error'].get('Code') == "404":
                return False
            raise
        return True

    def _list_split_keys_unbatched(self, metric, aggregations, version=3):
        bucket = self._bucket_name
        keys = {}
        for aggregation in aggregations:
            keys[aggregation] = set()
            response = {}
            while response.get('IsTruncated', True):
                if 'NextContinuationToken' in response:
                    kwargs = {
                        'ContinuationToken': response['NextContinuationToken']
                    }
                else:
                    kwargs = {}
                response = self.s3.list_objects_v2(
                    Bucket=bucket,
                    Prefix=self._prefix(metric) + '%s_%s' % (
                        aggregation.method,
                        utils.timespan_total_seconds(
                            aggregation.granularity),
                    ),
                    **kwargs)
                # If response is empty then check that the metric exists
                contents = response.get('Contents', ())
                if not contents and not self._metric_exists_p(metric, version):
                    raise storage.MetricDoesNotExist(metric)
                for f in contents:
                    try:
                        if (self._version_check(f['Key'], version)):
                            meta = f['Key'].split('_')
                            keys[aggregation].add(carbonara.SplitKey(
                                utils.to_timestamp(meta[2]),
                                sampling=aggregation.granularity))
                    except (ValueError, IndexError):
                        # Might be "none", or any other file. Be resilient.
                        continue
        return keys

    @staticmethod
    def _build_unaggregated_timeserie_path(metric, version):
        return S3Storage._prefix(metric) + 'none' + ("_v%s" % version
                                                     if version else "")

    def _get_or_create_unaggregated_timeseries_unbatched(
            self, metric, version=3):
        key = self._build_unaggregated_timeserie_path(metric, version)
        try:
            response = self.s3.get_object(
                Bucket=self._bucket_name, Key=key)
        except botocore.exceptions.ClientError as e:
            if e.response['Error'].get('Code') == "NoSuchKey":
                # Create the metric with empty data
                self._put_object_safe(
                    Bucket=self._bucket_name, Key=key, Body="")
            else:
                raise
        else:
            return response['Body'].read() or None

    def _store_unaggregated_timeseries_unbatched(
            self, metric, data, version=3):
        self._put_object_safe(
            Bucket=self._bucket_name,
            Key=self._build_unaggregated_timeserie_path(metric, version),
            Body=data)
