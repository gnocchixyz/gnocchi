# -*- encoding: utf-8 -*-
#
# Copyright © 2018 Red Hat
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

import numpy

from gnocchi import carbonara
from gnocchi import chef
from gnocchi import incoming
from gnocchi import indexer
from gnocchi.tests import base
from gnocchi import utils

from unittest import mock


def datetime64(*args):
    return numpy.datetime64(datetime.datetime(*args))


class TestChef(base.TestCase):
    def setUp(self):
        super(TestChef, self).setUp()
        self.metric, __ = self._create_metric()
        self.chef = chef.Chef(self.coord, self.incoming,
                              self.index, self.storage)

    def test_delete_nonempty_metric_unprocessed(self):
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
        ])
        self.index.delete_metric(self.metric.id)
        self.trigger_processing()
        __, __, details = self.incoming._build_report(True)
        self.assertNotIn(str(self.metric.id), details)
        self.chef.expunge_metrics(10000, sync=True)

    def test_delete_expunge_metric(self):
        self.incoming.add_measures(self.metric.id, [
            incoming.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
        ])
        self.trigger_processing()
        self.index.delete_metric(self.metric.id)
        self.chef.expunge_metrics(10000, sync=True)
        self.assertRaises(indexer.NoSuchMetric, self.index.delete_metric,
                          self.metric.id)

    def test_auto_clean_expired_resources_lock_not_acquired(self):
        auto_clean_lock_mock = mock.Mock()
        auto_clean_lock_mock.acquire.return_value = False

        with mock.patch.object(self.chef,
                               'get_sack_lock',
                               return_value=auto_clean_lock_mock) as get_sack_lock_mock:

            with mock.patch.object(self.index,
                                   'list_resources',
                                   return_value=[]) as list_resources_mock:
                self.chef.auto_clean_expired_resources(resource_ended_at_normalization=300)

                get_sack_lock_mock.assert_called()
                auto_clean_lock_mock.acquire.assert_called()
                self.assertEquals(0, list_resources_mock.call_count)
                auto_clean_lock_mock.release.assert_called()

    def test_auto_clean_expired_resources_no_resources(self):
        auto_clean_lock_mock = mock.Mock()
        auto_clean_lock_mock.acquire.return_value = True

        with mock.patch.object(self.chef,
                               'get_sack_lock',
                               return_value=auto_clean_lock_mock) as get_sack_lock_mock:

            with mock.patch.object(self.index,
                                   'list_resources',
                                   return_value=[]) as list_resources_mock:
                self.chef.auto_clean_expired_resources(resource_ended_at_normalization=300)

                get_sack_lock_mock.assert_called()
                auto_clean_lock_mock.acquire.assert_called()
                self.assertEquals(1, list_resources_mock.call_count)
                auto_clean_lock_mock.release.assert_called()

    @mock.patch.object(utils, 'utcnow')
    def test_auto_clean_expired_resources(self, utcnow_mock):
        auto_clean_lock_mock = mock.Mock()
        auto_clean_lock_mock.acquire.return_value = True

        resource_mock_1 = mock.Mock(id="resource-1")
        resource_mock_2 = mock.Mock(id="resource-2")

        utc_return = utils.datetime_utc(2025, 7, 17, 10, 00, 00)
        utcnow_mock.return_value = utc_return

        resource_ended_at_normalization_used = 300
        moment_expected = utc_return - datetime.timedelta(seconds=resource_ended_at_normalization_used)
        attribute_filter_expected = {"<": {"ended_at": moment_expected}}

        with mock.patch.object(self.chef, 'get_sack_lock',
                               return_value=auto_clean_lock_mock) as get_sack_lock_mock:
            with mock.patch.object(self.index, 'list_resources',
                                   return_value=[resource_mock_1, resource_mock_2]) as list_resources_mock:
                with mock.patch.object(self.index, 'delete_resource') as delete_resource_mock:
                    self.chef.auto_clean_expired_resources(
                        resource_ended_at_normalization=resource_ended_at_normalization_used)

                    get_sack_lock_mock.assert_called()
                    auto_clean_lock_mock.acquire.assert_called()
                    auto_clean_lock_mock.release.assert_called()

                    self.assertEquals(1, list_resources_mock.call_count)
                    self.assertEquals(2, delete_resource_mock.call_count)

                    list_resources_mock.assert_has_calls([mock.call(attribute_filter=attribute_filter_expected)])
                    delete_resource_mock.assert_has_calls([mock.call("resource-1"), mock.call("resource-2")])

    @mock.patch.object(utils, 'utcnow')
    def test_resource_ended_at_normalization_not_all_metrics_expired(self, utcnow_mock):
        metric_inactive_after_used = 3600

        utc_return = utils.datetime_utc(2025, 7, 17, 12, 00, 00)
        utcnow_mock.return_value = utc_return

        moment_expected = utc_return - datetime.timedelta(seconds=metric_inactive_after_used)
        attribute_filter_expected = {"<": {"last_measure_timestamp": moment_expected}}

        inactive_metric_mock_1_resource_1 = mock.Mock(id="metric-1", resource_id="resource-1")
        inactive_metric_mock_2_resource_1 = mock.Mock(id="metric-2", resource_id="resource-1")
        inactive_metric_mock_3_resource_1 = mock.Mock(id="metric-3", resource_id="resource-1")
        inactive_metric_mock_4_resource_2 = mock.Mock(id="metric-4", resource_id="resource-2")
        inactive_metric_mock_5_resource_2 = mock.Mock(id="metric-5", resource_id="resource-2")

        all_resource_1_inactive_metrics = [inactive_metric_mock_1_resource_1,
                                           inactive_metric_mock_2_resource_1,
                                           inactive_metric_mock_3_resource_1]
        resource_1_mock = mock.Mock(id="resource-1", metrics=all_resource_1_inactive_metrics + [
            mock.Mock(id="metric-10", resource_id="resource-1")])

        all_resource_2_inactive_metrics = [inactive_metric_mock_4_resource_2,
                                           inactive_metric_mock_5_resource_2]
        resource_2_mock = mock.Mock(id="resource-2", metrics=all_resource_2_inactive_metrics + [
            mock.Mock(id="metric-12", resource_id="resource-2")])

        all_metrics_mock = all_resource_1_inactive_metrics + all_resource_2_inactive_metrics

        def get_resource_side_effect(resource_type, resource_id, with_metrics):
            if resource_id == "resource-1":
                return resource_1_mock
            if resource_id == "resource-2":
                return resource_2_mock
            return None

        with mock.patch.object(self.index, 'list_metrics',
                               return_value=all_metrics_mock) as list_metrics_mock:

            with mock.patch.object(self.index, 'get_resource',
                                   side_effect=get_resource_side_effect) as get_resource_mock:
                with mock.patch.object(self.index, 'update_resource') as update_resource_mock:
                    self.chef.resource_ended_at_normalization(metric_inactive_after=metric_inactive_after_used)

                    get_resource_mock.assert_has_calls([mock.call("generic", "resource-1", with_metrics=True),
                                                        mock.call("generic", "resource-2", with_metrics=True)])

                    list_metrics_mock.assert_has_calls([mock.call(attribute_filter=attribute_filter_expected,
                                                                  resource_policy_filter={"==": {"ended_at": None}})])

                    self.assertEquals(0, update_resource_mock.call_count)

    @mock.patch.object(utils, 'utcnow')
    def test_resource_ended_at_normalization_all_metrics_expired(self, utcnow_mock):
        metric_inactive_after_used = 3600

        utc_return = utils.datetime_utc(2025, 7, 17, 12, 00, 00)
        utcnow_mock.return_value = utc_return

        moment_expected = utc_return - datetime.timedelta(seconds=metric_inactive_after_used)
        attribute_filter_expected = {"<": {"last_measure_timestamp": moment_expected}}

        inactive_metric_mock_1_resource_1 = mock.Mock(id="metric-1", resource_id="resource-1")
        inactive_metric_mock_2_resource_1 = mock.Mock(id="metric-2", resource_id="resource-1")
        inactive_metric_mock_3_resource_1 = mock.Mock(id="metric-3", resource_id="resource-1")
        inactive_metric_mock_4_resource_2 = mock.Mock(id="metric-4", resource_id="resource-2")
        inactive_metric_mock_5_resource_2 = mock.Mock(id="metric-5", resource_id="resource-2")

        all_resource_1_inactive_metrics = [inactive_metric_mock_1_resource_1,
                                           inactive_metric_mock_2_resource_1,
                                           inactive_metric_mock_3_resource_1]
        resource_1_mock = mock.Mock(id="resource-1", metrics=all_resource_1_inactive_metrics, ended_at=None)

        all_resource_2_inactive_metrics = [inactive_metric_mock_4_resource_2,
                                           inactive_metric_mock_5_resource_2]
        resource_2_mock = mock.Mock(id="resource-2", metrics=all_resource_2_inactive_metrics, ended_at=None)

        all_metrics_mock = all_resource_1_inactive_metrics + all_resource_2_inactive_metrics

        def get_resource_side_effect(resource_type, resource_id, with_metrics):
            if resource_id == "resource-1":
                return resource_1_mock
            if resource_id == "resource-2":
                return resource_2_mock
            return None

        with mock.patch.object(self.index, 'list_metrics',
                               return_value=all_metrics_mock) as list_metrics_mock:

            with mock.patch.object(self.index, 'get_resource',
                                   side_effect=get_resource_side_effect) as get_resource_mock:
                with mock.patch.object(self.index, 'update_resource') as update_resource_mock:
                    self.chef.resource_ended_at_normalization(metric_inactive_after=metric_inactive_after_used)
                    get_resource_mock.assert_has_calls([mock.call("generic", "resource-1", with_metrics=True),
                                                        mock.call("generic", "resource-2", with_metrics=True)])

                    list_metrics_mock.assert_has_calls([mock.call(attribute_filter=attribute_filter_expected,
                                                                  resource_policy_filter={"==": {"ended_at": None}})])

                    self.assertEquals(2, update_resource_mock.call_count)

    @mock.patch.object(utils, 'utcnow')
    def test_resource_ended_at_normalization_all_metrics_expired_one_resource_finished(self, utcnow_mock):
        metric_inactive_after_used = 3600

        utc_return = utils.datetime_utc(2025, 7, 17, 12, 00, 00)
        utcnow_mock.return_value = utc_return

        moment_expected = utc_return - datetime.timedelta(seconds=metric_inactive_after_used)
        attribute_filter_expected = {"<": {"last_measure_timestamp": moment_expected}}

        inactive_metric_mock_1_resource_1 = mock.Mock(id="metric-1", resource_id="resource-1")
        inactive_metric_mock_2_resource_1 = mock.Mock(id="metric-2", resource_id="resource-1")
        inactive_metric_mock_3_resource_1 = mock.Mock(id="metric-3", resource_id="resource-1")
        inactive_metric_mock_4_resource_2 = mock.Mock(id="metric-4", resource_id="resource-2")
        inactive_metric_mock_5_resource_2 = mock.Mock(id="metric-5", resource_id="resource-2")

        all_resource_1_inactive_metrics = [inactive_metric_mock_1_resource_1,
                                           inactive_metric_mock_2_resource_1,
                                           inactive_metric_mock_3_resource_1]
        resource_1_mock = mock.Mock(id="resource-1", metrics=all_resource_1_inactive_metrics, ended_at=None)

        all_resource_2_inactive_metrics = [inactive_metric_mock_4_resource_2,
                                           inactive_metric_mock_5_resource_2]
        resource_2_mock = mock.Mock(id="resource-2", metrics=all_resource_2_inactive_metrics,
                                    ended_at=utils.datetime_utc(2025, 5, 17, 12, 00, 00))

        all_metrics_mock = all_resource_1_inactive_metrics + all_resource_2_inactive_metrics

        def get_resource_side_effect(resource_type, resource_id, with_metrics):
            if resource_id == "resource-1":
                return resource_1_mock
            if resource_id == "resource-2":
                return resource_2_mock
            return None

        with mock.patch.object(self.index, 'list_metrics',
                               return_value=all_metrics_mock) as list_metrics_mock:

            with mock.patch.object(self.index, 'get_resource',
                                   side_effect=get_resource_side_effect) as get_resource_mock:
                with mock.patch.object(self.index, 'update_resource') as update_resource_mock:
                    self.chef.resource_ended_at_normalization(metric_inactive_after=metric_inactive_after_used)
                    get_resource_mock.assert_has_calls([mock.call("generic", "resource-1", with_metrics=True),
                                                        mock.call("generic", "resource-2", with_metrics=True)])

                    list_metrics_mock.assert_has_calls([mock.call(attribute_filter=attribute_filter_expected,
                                                                  resource_policy_filter={"==": {"ended_at": None}})])

                    self.assertEquals(1, update_resource_mock.call_count)

    @mock.patch.object(utils, 'utcnow')
    def test_resource_ended_at_normalization_only_one_resource_expired(self, utcnow_mock):
        metric_inactive_after_used = 3600

        utc_return = utils.datetime_utc(2025, 7, 17, 12, 00, 00)
        utcnow_mock.return_value = utc_return

        moment_expected = utc_return - datetime.timedelta(seconds=metric_inactive_after_used)
        attribute_filter_expected = {"<": {"last_measure_timestamp": moment_expected}}

        inactive_metric_mock_1_resource_1 = mock.Mock(id="metric-1", resource_id="resource-1")
        inactive_metric_mock_2_resource_1 = mock.Mock(id="metric-2", resource_id="resource-1")
        inactive_metric_mock_3_resource_1 = mock.Mock(id="metric-3", resource_id="resource-1")
        inactive_metric_mock_4_resource_2 = mock.Mock(id="metric-4", resource_id="resource-2")
        inactive_metric_mock_5_resource_2 = mock.Mock(id="metric-5", resource_id="resource-2")

        all_resource_1_inactive_metrics = [inactive_metric_mock_1_resource_1,
                                           inactive_metric_mock_2_resource_1,
                                           inactive_metric_mock_3_resource_1]
        resource_1_mock = mock.Mock(id="resource-1", metrics=all_resource_1_inactive_metrics + [
            mock.Mock(id="metric-10", resource_id="resource-1")])

        all_resource_2_inactive_metrics = [inactive_metric_mock_4_resource_2,
                                           inactive_metric_mock_5_resource_2]
        resource_2_mock = mock.Mock(id="resource-2", metrics=all_resource_2_inactive_metrics, ended_at=None)

        all_metrics_mock = all_resource_1_inactive_metrics + all_resource_2_inactive_metrics

        def get_resource_side_effect(resource_type, resource_id, with_metrics):
            if resource_id == "resource-1":
                return resource_1_mock
            if resource_id == "resource-2":
                return resource_2_mock
            return None

        with mock.patch.object(self.index, 'list_metrics',
                               return_value=all_metrics_mock) as list_metrics_mock:

            with mock.patch.object(self.index, 'get_resource',
                                   side_effect=get_resource_side_effect) as get_resource_mock:
                with mock.patch.object(self.index, 'update_resource') as update_resource_mock:
                    self.chef.resource_ended_at_normalization(metric_inactive_after=metric_inactive_after_used)
                    get_resource_mock.assert_has_calls([mock.call("generic", "resource-1", with_metrics=True),
                                                        mock.call("generic", "resource-2", with_metrics=True)])

                    list_metrics_mock.assert_has_calls([mock.call(attribute_filter=attribute_filter_expected,
                                                                  resource_policy_filter={"==": {"ended_at": None}})])

                    self.assertEquals(1, update_resource_mock.call_count)

    def test_clean_raw_data_inactive_metrics(self):
        metric_mock_1_resource_1 = mock.Mock()
        metric_mock_1_resource_1.id = mock.Mock(int=1)
        metric_mock_2_resource_2 = mock.Mock()
        metric_mock_2_resource_2.id = mock.Mock(int=2)
        metric_mock_3_resource_1 = mock.Mock()
        metric_mock_3_resource_1.id = mock.Mock(int=3)

        mock_metrics_to_clean = [metric_mock_1_resource_1, metric_mock_2_resource_2, metric_mock_3_resource_1]

        sack_mock = mock.Mock()
        with mock.patch.object(self.index, 'list_metrics',
                               return_value=mock_metrics_to_clean) as list_metrics_mock:
            with mock.patch.object(self.chef, 'execute_raw_data_cleanup') as execute_raw_data_cleanup_mock:
                with mock.patch.object(self.chef, 'get_sack_lock', return_value=sack_mock) as get_sack_lock_mock:
                    self.chef.clean_raw_data_inactive_metrics()

                    list_metrics_mock.assert_has_calls([mock.call(attribute_filter={"==": {
                        "needs_raw_data_truncation": True}})])
                    self.assertEquals(3, execute_raw_data_cleanup_mock.call_count)
                    self.assertEquals(3, get_sack_lock_mock.call_count)
                    self.assertEquals(3, sack_mock.release.call_count)

    def test_clean_raw_data_inactive_metrics_no_metrics_to_clean(self):
        with mock.patch.object(self.index, 'list_metrics',
                               return_value=[]) as list_metrics_mock:
            with mock.patch.object(self.chef, 'execute_raw_data_cleanup') as execute_raw_data_cleanup_mock:
                self.chef.clean_raw_data_inactive_metrics()

                list_metrics_mock.assert_has_calls([mock.call(attribute_filter={"==": {
                    "needs_raw_data_truncation": True}})])
                self.assertEquals(0, execute_raw_data_cleanup_mock.call_count)

    def test_execute_raw_data_cleanup(self):
        metric_mock = mock.Mock()
        archive_policy_mock = mock.Mock(
            aggregation_methods=["mean"], max_block_size=10000,
            back_window=50000)
        metric_mock.archive_policy = archive_policy_mock

        raw_measure_mock = mock.Mock()
        ts_mock = mock.Mock()

        with mock.patch.object(self.storage, '_get_or_create_unaggregated_timeseries_unbatched',
                               return_value=raw_measure_mock) as _get_or_create_unaggregated_timeseries_unbatched_mock:
            with mock.patch.object(self.index, 'update_needs_raw_data_truncation') as update_needs_raw_data_truncation_mock:
                with mock.patch.object(carbonara.BoundTimeSerie, 'unserialize', return_value=ts_mock) as unserialize_mock:
                    with mock.patch.object(self.storage, '_store_unaggregated_timeseries_unbatched') as _store_unaggregated_timeseries_unbatched_mock:
                        self.chef.execute_raw_data_cleanup(metric_mock)

                        self.assertEquals(1, _get_or_create_unaggregated_timeseries_unbatched_mock.call_count)
                        self.assertEquals(1, _store_unaggregated_timeseries_unbatched_mock.call_count)
                        self.assertEquals(1, update_needs_raw_data_truncation_mock.call_count)
                        self.assertEquals(1, unserialize_mock.call_count)
                        self.assertEquals(1, ts_mock._truncate.call_count)

                        unserialize_mock.assert_has_calls([mock.call(raw_measure_mock, 10000, 50000)])

    def test_execute_raw_data_cleanup_rate_aggregate_method(self):
        metric_mock = mock.Mock()
        archive_policy_mock = mock.Mock(
            aggregation_methods=["mean", "rate:mean"], max_block_size=10000,
            back_window=50000)

        metric_mock.archive_policy = archive_policy_mock

        raw_measure_mock = mock.Mock()
        ts_mock = mock.Mock()

        with mock.patch.object(self.storage, '_get_or_create_unaggregated_timeseries_unbatched',
                               return_value=raw_measure_mock) as _get_or_create_unaggregated_timeseries_unbatched_mock:
            with mock.patch.object(self.index, 'update_needs_raw_data_truncation') as update_needs_raw_data_truncation_mock:
                with mock.patch.object(carbonara.BoundTimeSerie, 'unserialize', return_value=ts_mock) as unserialize_mock:
                    with mock.patch.object(
                            self.storage, '_store_unaggregated_timeseries_unbatched'
                    ) as _store_unaggregated_timeseries_unbatched_mock:
                        self.chef.execute_raw_data_cleanup(metric_mock)

                        self.assertEquals(1, _get_or_create_unaggregated_timeseries_unbatched_mock.call_count)
                        self.assertEquals(1, _store_unaggregated_timeseries_unbatched_mock.call_count)
                        self.assertEquals(1, update_needs_raw_data_truncation_mock.call_count)
                        self.assertEquals(1, unserialize_mock.call_count)
                        self.assertEquals(1, ts_mock._truncate.call_count)

                        unserialize_mock.assert_has_calls([mock.call(raw_measure_mock, 10000, 50001)])

    def test_execute_raw_data_cleanup_no_raw_measures(self):
        metric_mock = mock.Mock()
        archive_policy_mock = mock.Mock(
            aggregation_methods=["mean"], max_block_size=10000,
            back_window=50000)
        metric_mock.archive_policy = archive_policy_mock

        ts_mock = mock.Mock()

        with mock.patch.object(self.storage, '_get_or_create_unaggregated_timeseries_unbatched',
                               return_value=None) as _get_or_create_unaggregated_timeseries_unbatched_mock:
            with mock.patch.object(self.index, 'update_needs_raw_data_truncation') as update_needs_raw_data_truncation_mock:
                with mock.patch.object(carbonara.BoundTimeSerie, 'unserialize', return_value=ts_mock) as unserialize_mock:
                    with mock.patch.object(self.storage, '_store_unaggregated_timeseries_unbatched') as _store_unaggregated_timeseries_unbatched_mock:
                        self.chef.execute_raw_data_cleanup(metric_mock)

                        self.assertEquals(1, _get_or_create_unaggregated_timeseries_unbatched_mock.call_count)
                        self.assertEquals(0, _store_unaggregated_timeseries_unbatched_mock.call_count)
                        self.assertEquals(1, update_needs_raw_data_truncation_mock.call_count)
                        self.assertEquals(0, unserialize_mock.call_count)
                        self.assertEquals(0, ts_mock._truncate.call_count)
