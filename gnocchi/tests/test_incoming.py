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
import sys
import threading
from unittest import mock
import uuid

import numpy

from gnocchi.cli import manage
from gnocchi import incoming
from gnocchi import indexer
from gnocchi.tests import base as tests_base


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
