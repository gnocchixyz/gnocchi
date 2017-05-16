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
import os
import uuid

import iso8601
import mock

from gnocchi.tests import base as tests_base
from gnocchi import utils


class TestUtils(tests_base.TestCase):
    def _do_test_datetime_to_unix_timezone_change(self, expected, dt):
        self.assertEqual(expected, utils.datetime_to_unix(dt))
        with mock.patch.dict(os.environ, {'TZ': 'UTC'}):
            self.assertEqual(expected, utils.datetime_to_unix(dt))
        with mock.patch.dict(os.environ, {'TZ': 'Europe/Paris'}):
            self.assertEqual(expected, utils.datetime_to_unix(dt))
        with mock.patch.dict(os.environ, {'TZ': 'US/Eastern'}):
            self.assertEqual(expected, utils.datetime_to_unix(dt))

    def test_datetime_to_unix_timezone_change_utc(self):
        dt = datetime.datetime(2015, 1, 1, 10, 0, tzinfo=iso8601.iso8601.UTC)
        self._do_test_datetime_to_unix_timezone_change(1420106400.0, dt)

    def test_datetime_to_unix_timezone_change_offset(self):
        dt = datetime.datetime(2015, 1, 1, 15, 0,
                               tzinfo=iso8601.iso8601.FixedOffset(5, 0, '+5h'))
        self._do_test_datetime_to_unix_timezone_change(1420106400.0, dt)

    def test_to_timestamps_epoch(self):
        self.assertEqual(
            utils.to_datetime("1425652440"),
            datetime.datetime(2015, 3, 6, 14, 34,
                              tzinfo=iso8601.iso8601.UTC))
        self.assertEqual(
            utils.to_datetime("1425652440.4"),
            datetime.datetime(2015, 3, 6, 14, 34, 0, 400000,
                              tzinfo=iso8601.iso8601.UTC))
        self.assertEqual(
            utils.to_datetime(1425652440),
            datetime.datetime(2015, 3, 6, 14, 34,
                              tzinfo=iso8601.iso8601.UTC))
        self.assertEqual(
            utils.to_datetime(utils.to_timestamp(1425652440.4)),
            datetime.datetime(2015, 3, 6, 14, 34, 0, 400000,
                              tzinfo=iso8601.iso8601.UTC))


class TestResourceUUID(tests_base.TestCase):
    def test_conversion(self):
        self.assertEqual(
            uuid.UUID('ba571521-1de6-5aff-b183-1535fd6eb5d0'),
            utils.ResourceUUID(
                uuid.UUID('ba571521-1de6-5aff-b183-1535fd6eb5d0'),
                "bar"))
        self.assertEqual(
            uuid.UUID('ba571521-1de6-5aff-b183-1535fd6eb5d0'),
            utils.ResourceUUID("foo", "bar"))
        self.assertEqual(
            uuid.UUID('4efb21f6-3d19-5fe3-910b-be8f0f727846'),
            utils.ResourceUUID("foo", None))
        self.assertEqual(
            uuid.UUID('853e5c64-f45e-58b2-999c-96df856fbe3d'),
            utils.ResourceUUID("foo", ""))


class StopWatchTest(tests_base.TestCase):
    def test_no_states(self):
        watch = utils.StopWatch()
        self.assertRaises(RuntimeError, watch.stop)

    def test_start_stop(self):
        watch = utils.StopWatch()
        watch.start()
        watch.stop()

    def test_no_elapsed(self):
        watch = utils.StopWatch()
        self.assertRaises(RuntimeError, watch.elapsed)

    def test_elapsed(self):
        watch = utils.StopWatch()
        watch.start()
        watch.stop()
        elapsed = watch.elapsed()
        self.assertAlmostEqual(elapsed, watch.elapsed())

    def test_context_manager(self):
        with utils.StopWatch() as watch:
            pass
        self.assertGreater(watch.elapsed(), 0)
