# -*- encoding: utf-8 -*-
#
# Copyright © 2018 Red Hat
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
from __future__ import absolute_import

import collections
import os
import socket
import struct

import cotyledon
import daiquiri
import itertools
from oslo_config import cfg
import six

try:
    import rocksdb
except ImportError:
    rocksdb = None

from gnocchi import carbonara
from gnocchi import storage
from gnocchi import utils


OPTS = [
    cfg.StrOpt('rocksdb_path',
               default='/var/lib/gnocchi/storage.db',
               help='Path used to store gnocchi data files.'),
    cfg.StrOpt('rocksdb_writer_socket',
               default='/var/lib/gnocchi/writer.sock',
               help='Path used to exchange data between reader and '
               'writer daemon'),
    cfg.BoolOpt('rocksdb_readonly',
                default=True,
                help='Make the rocksdb database readonly'),
    cfg.IntOpt('rocksdb_flush_every_operation',
               min=1, default=500,
               help='Flush every N operations'),

]

LOG = daiquiri.getLogger(__name__)

VERSION_MAX_LEN = 3

if rocksdb is not None:
    class MetricPrefix(rocksdb.interfaces.SliceTransform):
        # NOTE(sileht): Only one digit for the version, this assume
        # we never reach 1000 :p, or more seriously this must be changed
        # during upgrade when we reach version 1000
        #
        # <metric_id>_v<version>_
        METRIC_LEN = 36 + 2 + VERSION_MAX_LEN + 1

        def name(self):
            return b'<metric-id>_v<version>_'

        def transform(self, src):
            return (0, self.METRIC_LEN)

        def in_domain(self, src):
            return len(src) >= self.METRIC_LEN

        def in_range(self, dst):
            return len(dst) == self.METRIC_LEN


def open_db(path, read_only=True):
    # NOTE(sileht): Depending of the used disk, the database configuration
    # should differ.
    # https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#difference-of-spinning-disk
    # https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#prefix-database-on-flash-storage
    # I pick recommanded options for flash storage
    options = rocksdb.Options()
    options.create_if_missing = True
    options.compression = "snappy_compression"
    options.max_open_files = -1

    options.compaction_style = 'level'
    options.write_buffer_size = 64 * 1024 * 1024
    options.max_write_buffer_number = 3
    options.target_file_size_base = 64 * 1024 * 1024
    options.level0_file_num_compaction_trigger = 10
    options.level0_slowdown_writes_trigger = 20
    options.level0_stop_writes_trigger = 40
    options.max_bytes_for_level_base = 512 * 1024 * 1024
    options.max_background_compactions = 1
    options.max_background_flushes = 1
    # memtable_prefix_bloom_bits=1024 * 1024 * 8,

    # Creates an index with metric ids
    options.prefix_extractor = MetricPrefix()
    options.table_factory = rocksdb.BlockBasedTableFactory(
        index_type="hash_search",
        filter_policy=rocksdb.BloomFilterPolicy(10),
        block_size=4 * 1024,
        block_cache=rocksdb.LRUCache(512 * 1024 * 1024, 10),
    )

    try:
        return rocksdb.DB(path, options, read_only=read_only)
    except rocksdb.errors.Corruption:
        if read_only:
            LOG.exception("RocksDB database corrupted.")
        else:
            LOG.warning("RocksDB database corrupted, trying to repair.")
            rocksdb.repair_db(path, options)
            return rocksdb.DB(path, options, read_only=read_only)


WRITER_OP_MAP = ['put', 'delete']


class WriterService(cotyledon.Service):
    BUFSIZE = 4096
    BATCH_LEN_SIZE = struct.calcsize("Q")

    def __init__(self, conf):
        return
        self._db = open_db(conf.storage.rocksdb_path, read_only=False)
        if os.path.exists(conf.storage.rocksdb_writer_socket):
            os.remove(conf.storage.rocksdb_writer_socket)

        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.bind(conf.storage.rocksdb_writer_socket)
        self._server.listen(100)  # soconnmax default is 128

        self._client_to_acks = []
        self._current_buffer = b""
        self._current_buffer_len = 0

        self._flush_every_operation = (
            conf.storage.rocksdb_flush_every_operation)
        self._flush_every_time = 30

    def _get_or_fetch(self, connection, fmt):
        needed_len = struct.calcsize(fmt)
        while self._current_buffer_len < needed_len:
            try:
                buf = connection.recv(self.BUFSIZE)
            except socket.error:
                LOG.exception("SOCKET ERROR")
                return None
            self._current_buffer_len += len(buf)
            self._current_buffer += buf

        buf = self._current_buffer[:needed_len]
        self._current_buffer = self._current_buffer[needed_len:]
        self._current_buffer_len -= needed_len
        return buf

    def run(self):
        # NOTE(sileht): tocksdb batch is more efficient if keys are ordered
        ops = []
        sw = utils.StopWatch().start()
        count = 0

        while True:
            connection, client_address = self._server.accept()
            # TODO(sileht): This make priority between workers not very fair
            # but for now I don't care.
            connection.set_blocking(False)

            while True:
                number_of_operations = self._get_or_fetch(connection, "<Q")
                for i in range(number_of_operations):
                    count += 1
                    op, lkey, ldata = self._get_or_fetch(connection, "<BIQ")
                    op = WRITER_OP_MAP[op]
                    key, data = self._get_or_fetch(connection, "<%ds%ds")
                    # NOTE(sileht): Count ensure that within a key operation
                    # are ordered in the expected order.
                    ops.append((key, count, op, data))
                self._client_to_acks.append(connection)

            if (count >= self._flush_every_operation or
                    sw.elapsed() >= self._flush_every_time):
                batch = rocksdb.WriteBatch()
                for key, _, op, data in sorted(ops):
                    if op == "put":
                        batch.put(key, data)
                    else:
                        batch.delete(key)
                self._db.write(batch)
                ops = []
                count = 0
                sw.reset()
                for conn in self._client_to_acks:
                    conn.write(struct.pack("<?", True))

    def terminate(self):
        self._server.close()


class WriterProxy(object):
    # The protocol:
    # payload:
    #   <number_of_operation_to_batch><operation><operation><operation>....
    #
    # operation:
    #   <put/delete><len_of_key><len_of_data><key><data>

    def __init__(self, conf):
        self._client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        self._client.connect(conf.storage.rocksdb_writer_socket)

    @staticmethod
    def _serialize(op, key, data):
        lkey = len(key)
        ldata = len(data)
        return struct.pack("<BIQ%ds%ds" % (lkey, ldata),
                           WRITER_OP_MAP[op],
                           lkey, ldata, key, data)

    def _send(self, msgs):
        # recv is non blocking, and code currently assume the whole msg
        # have been sent when nodata can be read, so we need only one write
        # here (or later change the server side)
        data = struct.pack("<Q", len(msgs)) + b"".join(msgs)
        self._client.send(data)

    def _wait(self):
        # Handle error here ?
        self._client.recv(struct.calcsize("<?"))

    def delete(self, keys):
        self._send([self._serialize("delete", key, "") for key in keys])
        self._wait()

    def mput(self, values):
        self._send([self._serialize("put", key, data)
                    for key, data in six.iteritems(values)])
        self._wait()

    def close(self):
        self._client.close()


class RocksDBStorage(storage.StorageDriver):
    """RocksDBStorage

    Index format optimised for browsing list split keys:

        <metric.id>_<aggregation.method>_<aggregation.granularity>_...

    """
    WRITE_FULL = True
    FIELD_SEP = '_'

    def __init__(self, conf):
        super(RocksDBStorage, self).__init__(conf)
        self._conf = conf
        if rocksdb is None:
            raise RuntimeError("python-rocksdb module is unavailable")

        self._db = None

    def stop(self):
        # TODO(sileht): Make everything call this. for cotyledon in terminate
        # for API in atexit
        if self._db is not None:
            self._db.close()

    @property
    def db(self):
        # TODO(sileht): proxy this
        if self._db is None:
            self._db = open_db(self._conf.rocksdb_path,
                               read_only=self._conf.rocksdb_readonly)
        return self._db

    def __str__(self):
        return "%s: %s" % (self.__class__.__name__, str(
            self._conf.rocksdb_path))

    @classmethod
    def _unaggregated_key(cls, metric, version=3):
        return cls.FIELD_SEP.join([
            str(metric.id),
            "v%s" % str(version).rjust(VERSION_MAX_LEN, "0"),
            'none',
        ]).encode()

    @classmethod
    def _aggregated_key_for_split(cls, metric, version=3, aggregation=None,
                                  key=None):
        items = [str(metric.id),
                 'v%s' % str(version).rjust(VERSION_MAX_LEN, "0")]
        if aggregation is None:
            items.append("")
        else:
            items.append(aggregation.method)
            if key is None:
                items.append(str(utils.timespan_total_seconds(
                    aggregation.granularity)))
                items.append("")
            else:
                items.append(str(utils.timespan_total_seconds(key.sampling)))
                items.append(str(key))

        return cls.FIELD_SEP.join(items).encode()

    def _store_unaggregated_timeseries(self, metrics_and_data, version=3):
        batch = rocksdb.WriteBatch()
        for metric, data in metrics_and_data:
            batch.put(self._unaggregated_key(metric, version), data)
        self.db.write(batch)

    # READ ONLY OPERATIONS

    def _get_splits(self, metrics_keys_aggregations, version=3):
        # Use a list of metric with a constant sorting
        metrics = list(metrics_keys_aggregations.keys())
        results = {}
        keys = []
        results_metrics = []
        for metric in metrics:
            keys_and_aggregations = metrics_keys_aggregations[metric]
            for key, aggregation in keys_and_aggregations:
                k = self._aggregated_key_for_split(
                    metric, version, aggregation, key)
                keys.append(k)
                results_metrics.append((metric, k))

        intermediate_results = self.db.multi_get(keys)
        results = collections.defaultdict(list)
        for metric, k in results_metrics:
            results[metric].append(intermediate_results[k])
        return results

    def _get_or_create_unaggregated_timeseries(self, metrics, version=3):
        keys = [self._unaggregated_key(metric, version) for metric in metrics]

        # FIXME(sileht): Do we really need to create them ?
        batch = rocksdb.WriteBatch()
        for key in keys:
            if not self.db.key_may_exist(key, fetch=False)[0]:
                batch.put(key, "")
        self.db.write(batch)
        return dict(six.moves.zip(
            metrics, (data or None for data in
                      self.db.multi_get(keys).values())))

    def _list_split_keys(self, metrics_and_aggregations, version=3):
        it = self.db.iterkeys()

        split_keys = collections.defaultdict(dict)
        metrics = sorted(metrics_and_aggregations.keys())
        while metrics:
            metric = metrics.pop(0)
            prefix = self._aggregated_key_for_split(metric, version)
            it.seek(prefix)

            # This returns aggregated and unaggregated keys
            metric_keys = itertools.takewhile(lambda i: i.startswith(prefix),
                                              it)
            metric_keys = list(metric_keys)

            if not metric_keys:
                # Existing metric should at least have the unaggregated ts
                raise storage.MetricDoesNotExist(metric)

            for aggregation in metrics_and_aggregations[metric]:
                split_keys[metric][aggregation] = set()
                prefix_with_agg = self._aggregated_key_for_split(
                    metric, version, aggregation)
                k = six.moves.filter(lambda i: i.startswith(prefix_with_agg),
                                     metric_keys)
                k = six.moves.map(
                    lambda i: tuple(i.split(self.FIELD_SEP)[3:5]), k)
                granularities_and_timestamps = list(zip(*k))
                if not granularities_and_timestamps:
                    continue

                granularities, timestamps = granularities_and_timestamps
                timestamps = utils.to_timestamps(timestamps)
                granularities = map(utils.to_timespan, granularities)

                split_keys[metric][aggregation] = {
                    carbonara.SplitKey(timestamp,
                                       sampling=granularity)
                    for timestamp, granularity
                    in six.moves.zip(timestamps, granularities)
                }
        return split_keys

    # WRITE ONLY OPERATIONS

    def _delete_metric_splits(self, metrics_keys_aggregations, version=3):
        batch = rocksdb.WriteBatch()
        for metric, keys_and_aggregations in six.iteritems(
                metrics_keys_aggregations):
            for key, aggregation in keys_and_aggregations:
                batch.delete(self._aggregated_key_for_split(
                    metric, version, aggregation, key))
        self.db.write(batch)

    def _store_metric_splits(self, metrics_keys_aggregations_data_offset,
                             version=3):
        batch = rocksdb.WriteBatch()
        for metric, keys_aggs_data_offset in six.iteritems(
                metrics_keys_aggregations_data_offset):
            for key, aggregation, data, offset in keys_aggs_data_offset:
                k = self._aggregated_key_for_split(metric, version,
                                                   aggregation, key)
                batch.put(k, data)
        self.db.write(batch)

    def _delete_metric(self, metric):
        batch = rocksdb.WriteBatch()
        prefix = str(metric.id)
        it = self.db.iterkeys()
        it.seek(prefix)
        for key in itertools.takewhile(lambda i: i.startswith(prefix), it):
            batch.delete(key)
        self.db.write(batch)
