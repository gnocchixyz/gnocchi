# -*- encoding: utf-8 -*-
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
import datetime
import json
import types
import uuid

import numpy
from unittest import mock

from gnocchi.carbonara import TIMESERIES_ARRAY_DTYPE
from gnocchi.common import ceph as common_ceph
from gnocchi import incoming
from gnocchi.incoming import ceph as ceph_incoming
from gnocchi.tests import base as tests_base


class ObjectNotFound(Exception):
    pass


class FakeOmapIterator(object):
    def __init__(self, ioctx, op):
        self._ioctx = ioctx
        self._op = op

    def __iter__(self):
        obj = self._ioctx.objects.get(self._op._object)
        if obj is None:
            return iter([])
        pairs = [(k, v) for k, v in sorted(obj["omap"].items())
                 if k.startswith(self._op._prefix)
                 and k > self._op._marker]
        if self._op._limit is not None and self._op._limit >= 0:
            pairs = pairs[:self._op._limit]
        return iter(pairs)


class FakeReadOpCtx(object):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeWriteOpCtx(object):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeAioCompletion(object):
    def wait_for_complete(self):
        pass


class FakeIoctx(object):
    def __init__(self):
        self.objects = {}

    def read(self, name):
        try:
            return self.objects[name]["data"]
        except KeyError:
            raise ObjectNotFound

    def write_full(self, name, data):
        self.objects[name] = {"data": data, "omap": {}}

    def remove_object(self, name):
        try:
            del self.objects[name]
        except KeyError:
            raise ObjectNotFound

    def get_omap_vals(self, op, marker, prefix, limit):
        op._marker = marker
        op._prefix = prefix
        op._limit = limit
        return FakeOmapIterator(self, op), 0

    def operate_read_op(self, op, name, flag=0):
        op._object = name
        if name not in self.objects:
            raise ObjectNotFound

    def set_omap(self, op, keys, values):
        op._writes = list(zip(keys, values))

    def remove_omap_keys(self, op, keys):
        op._removes = tuple(keys)

    def operate_write_op(self, op, name, flags=0):
        obj = self.objects.setdefault(name, {"data": b"", "omap": {}})
        for key, value in getattr(op, "_writes", []):
            obj["omap"][key] = value
        for key in getattr(op, "_removes", ()):
            obj["omap"].pop(key, None)

    def operate_aio_write_op(self, op, name, flags=0):
        self.operate_write_op(op, name, flags=flags)
        return FakeAioCompletion()


FAKE_RADOS = types.SimpleNamespace(
    ObjectNotFound=ObjectNotFound,
    Error=Exception,
    LIBRADOS_OPERATION_BALANCE_READS=1,
    LIBRADOS_OPERATION_SKIPRWLOCKS=2,
    ReadOpCtx=FakeReadOpCtx,
    WriteOpCtx=FakeWriteOpCtx,
)


def config_of(ioctx):
    return json.loads(ioctx.objects["gnocchi-config"]["data"].decode())


def make_bundle(metric_id, value, tag):
    return ("_".join(("measure", str(metric_id), tag,
                      "20260101_00:00:00")),
            numpy.fromiter(
                [(datetime.datetime(2026, 1, 1), value)],
                dtype=TIMESERIES_ARRAY_DTYPE).tobytes())


class TestIncomingCephDriver(tests_base.TestCase):
    def setUp(self):
        super(TestIncomingCephDriver, self).setUp()
        self.ioctx = FakeIoctx()
        self._patch_rados = mock.patch.object(
            ceph_incoming, "rados", FAKE_RADOS)
        self._patch_connection = mock.patch.object(
            common_ceph, "create_rados_connection",
            return_value=(mock.Mock(), self.ioctx))
        self._patch_rados.start()
        self._patch_connection.start()

    def tearDown(self):
        self._patch_connection.stop()
        self._patch_rados.stop()
        super(TestIncomingCephDriver, self).tearDown()

    def make_driver(self, config=None):
        if config is not None:
            self.ioctx.objects["gnocchi-config"] = {
                "data": json.dumps(config).encode(), "omap": {}}
        return ceph_incoming.CephStorage(mock.Mock(), greedy=False)

    def test_upgrade_initializes_sacks(self):
        driver = self.make_driver()
        self.assertRaises(incoming.SackDetectionError,
                          lambda: driver.NUM_SACKS)
        driver.upgrade(4)
        self.assertEqual(config_of(self.ioctx), {"sacks": 4})

    def test_no_legacy_sacks_without_legacy_key(self):
        driver = self.make_driver({"sacks": 2})
        self.assertEqual(driver.LEGACY_SACKS, ())
        self.assertEqual(
            [str(s) for s in driver.iter_sacks()],
            ["incoming2-0", "incoming2-1"])

    def test_iter_sacks_includes_legacy_layouts(self):
        driver = self.make_driver({"sacks": 4, "legacy_sacks": [2]})
        self.assertEqual(
            [str(s) for s in driver.iter_sacks()],
            ["incoming4-0", "incoming4-1", "incoming4-2", "incoming4-3",
             "incoming2-0", "incoming2-1"])
        # NOTE: new measures only go to the current layout
        for metric_id in (uuid.uuid4(), uuid.uuid4(), uuid.uuid4()):
            self.assertEqual(driver.sack_for_metric(metric_id).total, 4)

    def test_migrate_sacks(self):
        driver = self.make_driver({"sacks": 4})
        driver.migrate_sacks(8)
        self.assertEqual(
            config_of(self.ioctx), {"sacks": 8, "legacy_sacks": [4]})
        # A second migration keeps the older layout marked as legacy
        driver = self.make_driver({"sacks": 8, "legacy_sacks": [4]})
        driver.migrate_sacks(16)
        self.assertEqual(
            config_of(self.ioctx), {"sacks": 16, "legacy_sacks": [4, 8]})
        # Migrating back to a previously used total
        driver = self.make_driver({"sacks": 16, "legacy_sacks": [4, 8]})
        driver.migrate_sacks(4)
        self.assertEqual(
            config_of(self.ioctx), {"sacks": 4, "legacy_sacks": [8, 16]})

    def test_measures_report_includes_legacy_sacks(self):
        driver = self.make_driver({"sacks": 2, "legacy_sacks": [1]})
        m1, m2 = uuid.uuid4(), uuid.uuid4()
        self.ioctx.objects["incoming2-0"] = {
            "data": b"",
            "omap": dict([
                make_bundle(m1, 1.0, "a"),
                make_bundle(m1, 2.0, "b"),
            ])}
        self.ioctx.objects["incoming1-0"] = {
            "data": b"",
            "omap": dict([
                make_bundle(m2, 3.0, "c"),
                make_bundle(m2, 4.0, "d"),
                make_bundle(m2, 5.0, "e"),
            ])}
        report = driver.measures_report(details=False)
        self.assertEqual(report["summary"],
                         {"metrics": 2, "measures": 5})

    def test_process_measures_for_sack_removes_drained_legacy_sack(self):
        driver = self.make_driver({"sacks": 2, "legacy_sacks": [1]})
        m = uuid.uuid4()
        key, value = make_bundle(m, 2.0, "a")
        self.ioctx.objects["incoming1-0"] = {
            "data": b"", "omap": {key: value}}
        legacy = driver._make_sack(0, 1)
        with driver.process_measures_for_sack(legacy) as measures:
            self.assertEqual(len(measures[m]), 1)
        # NOTE: keys are removed but the object survives this pass since
        # it contained measures
        self.assertEqual(self.ioctx.objects["incoming1-0"]["omap"], {})
        # NOTE: on the next pass the sack is drained and removed
        with driver.process_measures_for_sack(legacy):
            pass
        self.assertNotIn("incoming1-0", self.ioctx.objects)
        # NOTE: the last sack of the legacy layout is gone, so the layout
        # is pruned from the stored config
        self.assertEqual(config_of(self.ioctx), {"sacks": 2})
        # NOTE: current sacks are never removed, even when empty
        self.ioctx.objects["incoming2-0"] = {"data": b"", "omap": {}}
        with driver.process_measures_for_sack(driver._make_sack(0)):
            pass
        self.assertIn("incoming2-0", self.ioctx.objects)
        # NOTE: a legacy sack object that is already gone is fine
        with driver.process_measures_for_sack(driver._make_sack(1, 1)):
            pass

    def test_drain_keeps_legacy_total_while_siblings_remain(self):
        driver = self.make_driver({"sacks": 2, "legacy_sacks": [3]})
        m = uuid.uuid4()
        key, value = make_bundle(m, 2.0, "a")
        # NOTE: legacy layout 3 has three sacks; only incoming3-0 carries
        # measures, the others exist but are empty
        self.ioctx.objects["incoming3-0"] = {
            "data": b"", "omap": {key: value}}
        self.ioctx.objects["incoming3-1"] = {"data": b"", "omap": {}}
        self.ioctx.objects["incoming3-2"] = {"data": b"", "omap": {}}
        legacy = driver._make_sack(0, 3)
        # NOTE: first pass drains the measures (object survives, now empty)
        with driver.process_measures_for_sack(legacy):
            pass
        # NOTE: second pass removes the emptied sack, but siblings still
        # exist so the legacy layout is kept
        with driver.process_measures_for_sack(legacy):
            pass
        self.assertNotIn("incoming3-0", self.ioctx.objects)
        self.assertEqual(config_of(self.ioctx),
                         {"sacks": 2, "legacy_sacks": [3]})

    def test_process_measure_for_metrics_same_sack(self):
        driver = self.make_driver({"sacks": 3})
        # NOTE: two metrics hashing into the same sack
        m1 = uuid.UUID(int=3)
        m2 = uuid.UUID(int=6)
        self.assertEqual(driver.sack_for_metric(m1),
                         driver.sack_for_metric(m2))
        k1, v1 = make_bundle(m1, 1.0, "a")
        k2, v2 = make_bundle(m2, 2.0, "b")
        self.ioctx.objects["incoming3-0"] = {
            "data": b"", "omap": {k1: v1, k2: v2}}
        with driver.process_measure_for_metrics([m1, m2]) as measures:
            self.assertEqual(set(measures.keys()), {m1, m2})
            self.assertEqual(len(measures[m1]), 1)
            self.assertEqual(len(measures[m2]), 1)
        self.assertEqual(self.ioctx.objects["incoming3-0"]["omap"], {})
