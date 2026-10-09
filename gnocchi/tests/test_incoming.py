# -*- encoding: utf-8 -*-
#
# Copyright © 2017 Red Hat, Inc.
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
import sys
import threading
from unittest import mock
import uuid

import numpy

from gnocchi.cli import manage
from gnocchi import incoming
from gnocchi import indexer
from gnocchi.tests import base as tests_base
from gnocchi.tests.test_utils import get_measures_list


def datetime64(*args):
    return numpy.datetime64(datetime.datetime(*args))


class TestIncomingDriver(tests_base.TestCase):
    def setUp(self):
        super(TestIncomingDriver, self).setUp()
        # A lot of tests wants a metric, create one
        self.metric = indexer.Metric(
            uuid.uuid4(),
            self.archive_policies["low"])

    def test_iter_on_sacks_to_process(self):
        if (self.incoming.iter_on_sacks_to_process ==
           incoming.IncomingDriver.iter_on_sacks_to_process):
            self.skipTest("Incoming driver does not implement "
                          "iter_on_sacks_to_process")

        found = threading.Event()

        sack_to_find = self.incoming.sack_for_metric(self.metric.id)

        def _iter_on_sacks_to_process():
            for sack in self.incoming.iter_on_sacks_to_process():
                self.assertIsInstance(sack, incoming.Sack)
                if sack == sack_to_find:
                    found.set()
                    break

        finder = threading.Thread(target=_iter_on_sacks_to_process)
        finder.daemon = True
        finder.start()

        # Try for 30s to get a notification about this sack
        for _ in range(30):
            if found.wait(timeout=1):
                break
            # NOTE(jd) Retry to send measures. It cannot be done only once as
            # there might be a race condition between the threads
            self.incoming.finish_sack_processing(sack_to_find)
            self.incoming.add_measures(self.metric.id, [
                incoming.Measure(numpy.datetime64("2014-01-01 12:00:01"), 69),
            ])
        else:
            self.fail("Notification for metric not received")

    def test_change_sack_size_updates_stored_sack_count(self):
        old = self.incoming.NUM_SACKS
        new = old + 2
        old_argv = sys.argv
        sys.argv = ['gnocchi-change-sack-size', '--sacks-number', str(new)]
        try:
            with mock.patch.object(manage.incoming, 'get_driver',
                                   return_value=self.incoming):
                manage.change_sack_size()
        finally:
            sys.argv = old_argv
        self.assertEqual(int(self.incoming._get_storage_sacks()), new)
        self.incoming.reset_num_sacks()
        self.assertEqual(self.incoming.NUM_SACKS, new)

    def _run_change_sack_size(self, *extra_argv):
        new = self.incoming.NUM_SACKS + 2
        old_argv = sys.argv
        sys.argv = (['gnocchi-change-sack-size',
                     '--sacks-number', str(new)]
                    + list(extra_argv))
        try:
            with mock.patch.object(manage.incoming, 'get_driver',
                                   return_value=self.incoming), \
                 mock.patch.object(type(self.incoming),
                                   'SUPPORTS_SACK_MIGRATION', True), \
                 mock.patch.object(self.incoming, 'migrate_sacks') as migrate:
                manage.change_sack_size()
        finally:
            sys.argv = old_argv
        return new, migrate

    def test_change_sack_size_default_online_when_supported(self):
        new, migrate = self._run_change_sack_size()
        migrate.assert_called_once_with(new)

    def test_change_sack_size_offline_forces_offline(self):
        new, migrate = self._run_change_sack_size('--offline')
        migrate.assert_not_called()
        # NOTE: the offline path was applied, the sack count is updated
        # without a migration
        self.assertEqual(int(self.incoming._get_storage_sacks()), new)
        self.incoming.reset_num_sacks()
        self.assertEqual(self.incoming.NUM_SACKS, new)


class TestIncomingDriverSackMigration(tests_base.TestCase):
    """Test sack migration in incoming drivers.

    Measures written to the old sack layout must survive an online
    migration and be aggregated alongside the new layout.
    """

    def setUp(self):
        super(TestIncomingDriverSackMigration, self).setUp()
        self.metric, _ = self._create_metric()

    def _pending_measures(self):
        return self.incoming.measures_report(False)["summary"]["measures"]

    def test_measures_survive_online_migration(self):
        if not self.incoming.SUPPORTS_SACK_MIGRATION:
            self.skipTest("Driver does not support online sack migration")

        old = self.incoming.NUM_SACKS
        new = old + 3

        # NOTE: written while the metric is sharded over `old` sacks, so it
        # lands in the layout that is about to become legacy.
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
        ])
        # NOTE: it is still pending, so the migration happens online, with
        # unprocessed measures in the old layout.
        self.assertEqual(self._pending_measures(), 1)

        self.incoming.migrate_sacks(new)
        self.assertEqual(self.incoming.NUM_SACKS, new)
        self.assertEqual(self.incoming.LEGACY_SACKS, (old,))

        # NOTE: written after the migration, so it lands in a sack of the new
        # current layout.
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 13, 0, 1), 42),
        ])
        # NOTE: one pending measure in the legacy layout and one in the new
        # one.
        self.assertEqual(self._pending_measures(), 2)

        # NOTE: drain every sack, exactly like metricd does: both the new
        # current layout and the legacy one.
        for sack in self.incoming.iter_sacks():
            self.chef.process_new_measures_for_sack(sack, blocking=True,
                                                    sync=True)

        self.assertEqual(self._pending_measures(), 0)

        # NOTE: both measures must have reached storage: the one written to
        # the old layout (12:00) and the one written to the new layout
        # (13:00).
        aggregations = (
            self.metric.archive_policy.get_aggregations_for_method("mean"))
        m = get_measures_list(
            self.storage.get_aggregated_measures(
                {self.metric: aggregations})[self.metric])["mean"]
        self.assertIn((datetime64(2014, 1, 1, 12), numpy.timedelta64(1, "h"),
                       69.0), m)
        self.assertIn((datetime64(2014, 1, 1, 13), numpy.timedelta64(1, "h"),
                       42.0), m)

        # NOTE: once fully drained, the legacy layout is pruned from the
        # config so it stops being iterated.
        self.assertEqual(self.incoming.LEGACY_SACKS, ())
