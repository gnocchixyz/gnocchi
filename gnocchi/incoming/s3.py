# -*- encoding: utf-8 -*-
#
<<<<<<< HEAD
<<<<<<< HEAD
# Copyright © 2016 Red Hat, Inc.
=======
# Copyright © 2016-2018 Red Hat, Inc.
>>>>>>> 11a2520... api: avoid some indexer queries
=======
# Copyright © 2016-2018 Red Hat, Inc.
>>>>>>> f21ea84... Add automatic backport labels
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
<<<<<<< HEAD
<<<<<<< HEAD
=======
import daiquiri
>>>>>>> 11a2520... api: avoid some indexer queries
=======
import daiquiri
>>>>>>> f21ea84... Add automatic backport labels
import datetime
import json
import uuid

import numpy
<<<<<<< HEAD
<<<<<<< HEAD
import six
=======
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels

from gnocchi.common import s3
from gnocchi import incoming

boto3 = s3.boto3
botocore = s3.botocore

<<<<<<< HEAD
<<<<<<< HEAD

class S3Storage(incoming.IncomingDriver):

=======
=======
>>>>>>> f21ea84... Add automatic backport labels
LOG = daiquiri.getLogger(__name__)


class S3Storage(incoming.IncomingDriver):

    # NOTE(gordc): override to follow s3 partitioning logic
    SACK_NAME_FORMAT = "{number}-{total}"

<<<<<<< HEAD
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels
    def __init__(self, conf, greedy=True):
        super(S3Storage, self).__init__(conf)
        self.s3, self._region_name, self._bucket_prefix = (
            s3.get_connection(conf)
        )

        self._bucket_name_measures = (
            self._bucket_prefix + "-" + self.MEASURE_PREFIX
        )

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, self._bucket_name_measures)

    def _get_storage_sacks(self):
        response = self.s3.get_object(Bucket=self._bucket_name_measures,
                                      Key=self.CFG_PREFIX)
        return json.loads(response['Body'].read().decode())[self.CFG_SACKS]

    def set_storage_settings(self, num_sacks):
        data = {self.CFG_SACKS: num_sacks}
        self.s3.put_object(Bucket=self._bucket_name_measures,
                           Key=self.CFG_PREFIX,
                           Body=json.dumps(data).encode())

<<<<<<< HEAD
<<<<<<< HEAD
    def get_sack_prefix(self, num_sacks=None):
        # NOTE(gordc): override to follow s3 partitioning logic
        return '%s-' + ('%s/' % (num_sacks if num_sacks else self.NUM_SACKS))

    @staticmethod
    def remove_sack_group(num_sacks):
=======
    @staticmethod
    def remove_sacks(num_sacks):
>>>>>>> 11a2520... api: avoid some indexer queries
=======
    @staticmethod
    def remove_sacks(num_sacks):
>>>>>>> f21ea84... Add automatic backport labels
        # nothing to cleanup since sacks are part of path
        pass

    def upgrade(self, num_sacks):
        try:
            s3.create_bucket(self.s3, self._bucket_name_measures,
                             self._region_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error'].get('Code') not in (
                    "BucketAlreadyExists", "BucketAlreadyOwnedByYou"
            ):
                raise
        # need to create bucket first to store storage settings object
        super(S3Storage, self).upgrade(num_sacks)

    def _store_new_measures(self, metric_id, data):
        now = datetime.datetime.utcnow().strftime("_%Y%m%d_%H:%M:%S")
        self.s3.put_object(
            Bucket=self._bucket_name_measures,
<<<<<<< HEAD
<<<<<<< HEAD
            Key=(self.get_sack_name(self.sack_for_metric(metric_id))
                 + six.text_type(metric_id) + "/"
                 + six.text_type(uuid.uuid4()) + now),
=======
            Key="/".join((str(self.sack_for_metric(metric_id)),
                          str(metric_id),
                          str(uuid.uuid4()) + now)),
>>>>>>> 11a2520... api: avoid some indexer queries
=======
            Key="/".join((str(self.sack_for_metric(metric_id)),
                          str(metric_id),
                          str(uuid.uuid4()) + now)),
>>>>>>> f21ea84... Add automatic backport labels
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
                if c['Key'] != self.CFG_PREFIX:
                    __, metric, metric_file = c['Key'].split("/", 2)
                    metric_details[metric] += 1
        return (len(metric_details), sum(metric_details.values()),
                metric_details if details else None)

<<<<<<< HEAD
<<<<<<< HEAD
    def list_metric_with_measures_to_process(self, sack):
        limit = 1000        # 1000 is the default anyway
        metrics = set()
=======
    def _list_files(self, path_items, **kwargs):
>>>>>>> 11a2520... api: avoid some indexer queries
=======
    def _list_files(self, path_items, **kwargs):
>>>>>>> f21ea84... Add automatic backport labels
        response = {}
        # Handle pagination
        while response.get('IsTruncated', True):
            if 'NextContinuationToken' in response:
<<<<<<< HEAD
<<<<<<< HEAD
                kwargs = {
                    'ContinuationToken': response['NextContinuationToken']
                }
            else:
                kwargs = {}
            response = self.s3.list_objects_v2(
                Bucket=self._bucket_name_measures,
                Prefix=self.get_sack_name(sack),
                Delimiter="/",
                MaxKeys=limit,
                **kwargs)
            for p in response.get('CommonPrefixes', ()):
                metrics.add(p['Prefix'].split('/', 2)[1])
        return metrics

    def _list_measure_files_for_metric(self, sack, metric_id):
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
                Prefix=(self.get_sack_name(sack)
                        + six.text_type(metric_id) + "/"),
                **kwargs)

            for c in response.get('Contents', ()):
                files.add(c['Key'])

        return files

=======
=======
>>>>>>> f21ea84... Add automatic backport labels
                kwargs['ContinuationToken'] = response['NextContinuationToken']
            else:
                try:
                    del kwargs['ContinuationToken']
                except KeyError:
                    pass
            response = self.s3.list_objects_v2(
                Bucket=self._bucket_name_measures,
                Prefix="/".join(path_items) + "/",
                **kwargs)
            yield response

    def _list_measure_files(self, path_items):
        files = set()
        for response in self._list_files(path_items):
            for c in response.get('Contents', ()):
                files.add(c['Key'])
        return files

    def _list_measure_files_for_metric(self, sack, metric_id):
        return self._list_measure_files((str(sack), str(metric_id)))

<<<<<<< HEAD
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels
    def delete_unprocessed_measures_for_metric(self, metric_id):
        sack = self.sack_for_metric(metric_id)
        files = self._list_measure_files_for_metric(sack, metric_id)
        s3.bulk_delete(self.s3, self._bucket_name_measures, files)

    def has_unprocessed(self, metric_id):
        sack = self.sack_for_metric(metric_id)
        return bool(self._list_measure_files_for_metric(sack, metric_id))

    @contextlib.contextmanager
<<<<<<< HEAD
<<<<<<< HEAD
    def process_measure_for_metric(self, metric_id):
        sack = self.sack_for_metric(metric_id)
        files = self._list_measure_files_for_metric(sack, metric_id)

        measures = self._make_measures_array()
        for f in files:
            response = self.s3.get_object(
                Bucket=self._bucket_name_measures,
                Key=f)
            measures = numpy.concatenate((
                measures,
=======
=======
>>>>>>> f21ea84... Add automatic backport labels
    def process_measure_for_metrics(self, metric_ids):
        measures = defaultdict(self._make_measures_array)
        all_files = []
        for metric_id in metric_ids:
            sack = self.sack_for_metric(metric_id)
            files = self._list_measure_files_for_metric(sack, metric_id)
            all_files.extend(files)
            for f in files:
                response = self.s3.get_object(
                    Bucket=self._bucket_name_measures,
                    Key=f)
                measures[metric_id] = numpy.concatenate((
                    measures[metric_id],
                    self._unserialize_measures(f, response['Body'].read())
                ))

        yield measures

        # Now clean objects
        s3.bulk_delete(self.s3, self._bucket_name_measures, all_files)

    @contextlib.contextmanager
    def process_measures_for_sack(self, sack):
        measures = defaultdict(self._make_measures_array)
        files = self._list_measure_files((str(sack),))
        for f in files:
            try:
                sack, metric_id, measure_id = f.split("/")
                metric_id = uuid.UUID(metric_id)
            except ValueError:
                LOG.warning("Unable to parse measure file name %s", f)
                continue

            response = self.s3.get_object(
                Bucket=self._bucket_name_measures,
                Key=f)
            measures[metric_id] = numpy.concatenate((
                measures[metric_id],
<<<<<<< HEAD
>>>>>>> 11a2520... api: avoid some indexer queries
=======
>>>>>>> f21ea84... Add automatic backport labels
                self._unserialize_measures(f, response['Body'].read())
            ))

        yield measures

        # Now clean objects
        s3.bulk_delete(self.s3, self._bucket_name_measures, files)
