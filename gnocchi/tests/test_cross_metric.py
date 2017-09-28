# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2016 eNovance
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
import functools
import uuid

import numpy

from gnocchi import carbonara
from gnocchi.rest import cross_metric
from gnocchi import storage
from gnocchi.tests import base


def datetime64(*args):
    return numpy.datetime64(datetime.datetime(*args))


class TestAggregatedTimeseries(base.BaseTestCase):
    @staticmethod
    def _resample_and_merge(ts, agg_dict):
        """Helper method that mimics _add_measures workflow."""
        grouped = ts.group_serie(agg_dict['sampling'])
        existing = agg_dict.get('return')
        agg_dict['return'] = carbonara.AggregatedTimeSerie.from_grouped_serie(
            grouped, agg_dict['sampling'], agg_dict['agg'],
            max_size=agg_dict.get('size'), truncate=True)
        if existing:
            existing.merge(agg_dict['return'])
            agg_dict['return'] = existing

    def test_aggregated_different_archive_no_overlap(self):
        tsc1 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 50, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 50, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        tsb1.set_values(numpy.array([(datetime64(2014, 1, 1, 11, 46, 4), 4)],
                                    dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
                        before_truncate_callback=functools.partial(
                            self._resample_and_merge, agg_dict=tsc1))
        tsb2.set_values(numpy.array([(datetime64(2014, 1, 1, 9, 1, 4), 4)],
                                    dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
                        before_truncate_callback=functools.partial(
                            self._resample_and_merge, agg_dict=tsc2))

        dtfrom = datetime64(2014, 1, 1, 11, 0, 0)
        self.assertRaises(cross_metric.UnAggregableTimeseries,
                          cross_metric.aggregated,
                          [tsc1['return'], tsc2['return']],
                          from_timestamp=dtfrom, aggregation='mean')

    def test_aggregated_different_archive_no_overlap2(self):
        tsc1 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 50, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = carbonara.AggregatedTimeSerie(
            sampling=numpy.timedelta64(60, 's'),
            max_size=50,
            aggregation_method='mean')

        tsb1.set_values(numpy.array([(datetime64(2014, 1, 1, 12, 3, 0), 4)],
                                    dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
                        before_truncate_callback=functools.partial(
                            self._resample_and_merge, agg_dict=tsc1))
        self.assertRaises(cross_metric.UnAggregableTimeseries,
                          cross_metric.aggregated,
                          [tsc1['return'], tsc2], aggregation='mean')

    def test_aggregated_different_archive_overlap(self):
        tsc1 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 10, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 10, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        # NOTE(sileht): minute 8 is missing in both and
        # minute 7 in tsc2 too, but it looks like we have
        # enough point to do the aggregation
        tsb1.set_values(numpy.array([
            (datetime64(2014, 1, 1, 11, 0, 0), 4),
            (datetime64(2014, 1, 1, 12, 1, 0), 3),
            (datetime64(2014, 1, 1, 12, 2, 0), 2),
            (datetime64(2014, 1, 1, 12, 3, 0), 4),
            (datetime64(2014, 1, 1, 12, 4, 0), 2),
            (datetime64(2014, 1, 1, 12, 5, 0), 3),
            (datetime64(2014, 1, 1, 12, 6, 0), 4),
            (datetime64(2014, 1, 1, 12, 7, 0), 10),
            (datetime64(2014, 1, 1, 12, 9, 0), 2)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=tsc1))

        tsb2.set_values(numpy.array([
            (datetime64(2014, 1, 1, 12, 1, 0), 3),
            (datetime64(2014, 1, 1, 12, 2, 0), 4),
            (datetime64(2014, 1, 1, 12, 3, 0), 4),
            (datetime64(2014, 1, 1, 12, 4, 0), 6),
            (datetime64(2014, 1, 1, 12, 5, 0), 3),
            (datetime64(2014, 1, 1, 12, 6, 0), 6),
            (datetime64(2014, 1, 1, 12, 9, 0), 2),
            (datetime64(2014, 1, 1, 12, 11, 0), 2),
            (datetime64(2014, 1, 1, 12, 12, 0), 2)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=tsc2))

        dtfrom = datetime64(2014, 1, 1, 12, 0, 0)
        dtto = datetime64(2014, 1, 1, 12, 10, 0)

        # By default we require 100% of point that overlap
        # so that fail
        self.assertRaises(cross_metric.UnAggregableTimeseries,
                          cross_metric.aggregated,
                          [tsc1['return'], tsc2['return']],
                          from_timestamp=dtfrom,
                          to_timestamp=dtto, aggregation='mean')

        # Retry with 80% and it works
        output = cross_metric.aggregated([
            tsc1['return'], tsc2['return']],
            from_timestamp=dtfrom, to_timestamp=dtto,
            aggregation='mean', needed_percent_of_overlap=80.0)

        self.assertEqual([
            (datetime64(2014, 1, 1, 12, 1, 0),
             numpy.timedelta64(60, 's'), 3.0),
            (datetime64(2014, 1, 1, 12, 2, 0),
             numpy.timedelta64(60, 's'), 3.0),
            (datetime64(2014, 1, 1, 12, 3, 0),
             numpy.timedelta64(60, 's'), 4.0),
            (datetime64(2014, 1, 1, 12, 4, 0),
             numpy.timedelta64(60, 's'), 4.0),
            (datetime64(2014, 1, 1, 12, 5, 0),
             numpy.timedelta64(60, 's'), 3.0),
            (datetime64(2014, 1, 1, 12, 6, 0),
             numpy.timedelta64(60, 's'), 5.0),
            (datetime64(2014, 1, 1, 12, 7, 0),
             numpy.timedelta64(60, 's'), 10.0),
            (datetime64(2014, 1, 1, 12, 9, 0),
             numpy.timedelta64(60, 's'), 2.0),
        ], list(output))

    def test_aggregated_different_archive_overlap_edge_missing1(self):
        tsc1 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 10, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 10, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        tsb1.set_values(numpy.array([
            (datetime64(2014, 1, 1, 12, 3, 0), 9),
            (datetime64(2014, 1, 1, 12, 4, 0), 1),
            (datetime64(2014, 1, 1, 12, 5, 0), 2),
            (datetime64(2014, 1, 1, 12, 6, 0), 7),
            (datetime64(2014, 1, 1, 12, 7, 0), 5),
            (datetime64(2014, 1, 1, 12, 8, 0), 3)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=tsc1))

        tsb2.set_values(numpy.array([
            (datetime64(2014, 1, 1, 11, 0, 0), 6),
            (datetime64(2014, 1, 1, 12, 1, 0), 2),
            (datetime64(2014, 1, 1, 12, 2, 0), 13),
            (datetime64(2014, 1, 1, 12, 3, 0), 24),
            (datetime64(2014, 1, 1, 12, 4, 0), 4),
            (datetime64(2014, 1, 1, 12, 5, 0), 16),
            (datetime64(2014, 1, 1, 12, 6, 0), 12)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=tsc2))

        # By default we require 100% of point that overlap
        # but we allow that the last datapoint is missing
        # of the precisest granularity
        output = cross_metric.aggregated([
            tsc1['return'], tsc2['return']], aggregation='sum')

        self.assertEqual([
            (datetime64(2014, 1, 1, 12, 3, 0),
             numpy.timedelta64(60, 's'), 33.0),
            (datetime64(2014, 1, 1, 12, 4, 0),
             numpy.timedelta64(60, 's'), 5.0),
            (datetime64(2014, 1, 1, 12, 5, 0),
             numpy.timedelta64(60, 's'), 18.0),
            (datetime64(2014, 1, 1, 12, 6, 0),
             numpy.timedelta64(60, 's'), 19.0),
        ], list(output))

    def test_aggregated_different_archive_overlap_edge_missing2(self):
        tsc1 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 10, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 10, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        tsb1.set_values(numpy.array([(datetime64(2014, 1, 1, 12, 3, 0), 4)],
                                    dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
                        before_truncate_callback=functools.partial(
                            self._resample_and_merge, agg_dict=tsc1))

        tsb2.set_values(numpy.array([(datetime64(2014, 1, 1, 11, 0, 0), 4),
                                     (datetime64(2014, 1, 1, 12, 3, 0), 4)],
                                    dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
                        before_truncate_callback=functools.partial(
                            self._resample_and_merge, agg_dict=tsc2))

        output = cross_metric.aggregated(
            [tsc1['return'], tsc2['return']], aggregation='mean')
        self.assertEqual([
            (datetime64(
                2014, 1, 1, 12, 3, 0
            ), numpy.timedelta64(60000000000, 'ns'), 4.0),
        ], list(output))

    def test_aggregated_some_overlap_with_fill_zero(self):
        tsc1 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 10, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 10, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        tsb1.set_values(numpy.array([
            (datetime64(2014, 1, 1, 12, 3, 0), 9),
            (datetime64(2014, 1, 1, 12, 4, 0), 1),
            (datetime64(2014, 1, 1, 12, 5, 0), 2),
            (datetime64(2014, 1, 1, 12, 6, 0), 7),
            (datetime64(2014, 1, 1, 12, 7, 0), 5),
            (datetime64(2014, 1, 1, 12, 8, 0), 3)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=tsc1))

        tsb2.set_values(numpy.array([
            (datetime64(2014, 1, 1, 12, 0, 0), 6),
            (datetime64(2014, 1, 1, 12, 1, 0), 2),
            (datetime64(2014, 1, 1, 12, 2, 0), 13),
            (datetime64(2014, 1, 1, 12, 3, 0), 24),
            (datetime64(2014, 1, 1, 12, 4, 0), 4),
            (datetime64(2014, 1, 1, 12, 5, 0), 16),
            (datetime64(2014, 1, 1, 12, 6, 0), 12)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=tsc2))

        output = cross_metric.aggregated([
            tsc1['return'], tsc2['return']], aggregation='mean', fill=0)

        self.assertEqual([
            (datetime64(2014, 1, 1, 12, 0, 0),
             numpy.timedelta64(60000000000, 'ns'), 3.0),
            (datetime64(2014, 1, 1, 12, 1, 0),
             numpy.timedelta64(60000000000, 'ns'), 1.0),
            (datetime64(2014, 1, 1, 12, 2, 0),
             numpy.timedelta64(60000000000, 'ns'), 6.5),
            (datetime64(2014, 1, 1, 12, 3, 0),
             numpy.timedelta64(60000000000, 'ns'), 16.5),
            (datetime64(2014, 1, 1, 12, 4, 0),
             numpy.timedelta64(60000000000, 'ns'), 2.5),
            (datetime64(2014, 1, 1, 12, 5, 0),
             numpy.timedelta64(60000000000, 'ns'), 9.0),
            (datetime64(2014, 1, 1, 12, 6, 0),
             numpy.timedelta64(60000000000, 'ns'), 9.5),
            (datetime64(2014, 1, 1, 12, 7, 0),
             numpy.timedelta64(60000000000, 'ns'), 2.5),
            (datetime64(2014, 1, 1, 12, 8, 0),
             numpy.timedelta64(60000000000, 'ns'), 1.5),
        ], list(output))

    def test_aggregated_some_overlap_with_fill_null(self):
        tsc1 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 10, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 10, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        tsb1.set_values(numpy.array([
            (datetime64(2014, 1, 1, 12, 3, 0), 9),
            (datetime64(2014, 1, 1, 12, 4, 0), 1),
            (datetime64(2014, 1, 1, 12, 5, 0), 2),
            (datetime64(2014, 1, 1, 12, 6, 0), 7),
            (datetime64(2014, 1, 1, 12, 7, 0), 5),
            (datetime64(2014, 1, 1, 12, 8, 0), 3)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=tsc1))

        tsb2.set_values(numpy.array([
            (datetime64(2014, 1, 1, 12, 0, 0), 6),
            (datetime64(2014, 1, 1, 12, 1, 0), 2),
            (datetime64(2014, 1, 1, 12, 2, 0), 13),
            (datetime64(2014, 1, 1, 12, 3, 0), 24),
            (datetime64(2014, 1, 1, 12, 4, 0), 4),
            (datetime64(2014, 1, 1, 12, 5, 0), 16),
            (datetime64(2014, 1, 1, 12, 6, 0), 12)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=tsc2))

        output = cross_metric.aggregated([
            tsc1['return'], tsc2['return']], aggregation='mean', fill='null')

        self.assertEqual([
            (datetime64(2014, 1, 1, 12, 0, 0),
             numpy.timedelta64(60000000000, 'ns'), 6.0),
            (datetime64(2014, 1, 1, 12, 1, 0),
             numpy.timedelta64(60000000000, 'ns'), 2.0),
            (datetime64(2014, 1, 1, 12, 2, 0),
             numpy.timedelta64(60000000000, 'ns'), 13.0),
            (datetime64(2014, 1, 1, 12, 3, 0),
             numpy.timedelta64(60000000000, 'ns'), 16.5),
            (datetime64(2014, 1, 1, 12, 4, 0),
             numpy.timedelta64(60000000000, 'ns'), 2.5),
            (datetime64(2014, 1, 1, 12, 5, 0),
             numpy.timedelta64(60000000000, 'ns'), 9.0),
            (datetime64(2014, 1, 1, 12, 6, 0),
             numpy.timedelta64(60000000000, 'ns'), 9.5),
            (datetime64(2014, 1, 1, 12, 7, 0),
             numpy.timedelta64(60000000000, 'ns'), 5.0),
            (datetime64(2014, 1, 1, 12, 8, 0),
             numpy.timedelta64(60000000000, 'ns'), 3.0),
        ], list(output))

    def test_aggregate_no_points_with_fill_zero(self):
        tsc1 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 10, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 10, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        tsb1.set_values(numpy.array([
            (datetime64(2014, 1, 1, 12, 3, 0), 9),
            (datetime64(2014, 1, 1, 12, 4, 0), 1),
            (datetime64(2014, 1, 1, 12, 7, 0), 5),
            (datetime64(2014, 1, 1, 12, 8, 0), 3)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=tsc1))

        tsb2.set_values(numpy.array([
            (datetime64(2014, 1, 1, 12, 0, 0), 6),
            (datetime64(2014, 1, 1, 12, 1, 0), 2),
            (datetime64(2014, 1, 1, 12, 2, 0), 13),
            (datetime64(2014, 1, 1, 12, 3, 0), 24),
            (datetime64(2014, 1, 1, 12, 4, 0), 4)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=tsc2))

        output = cross_metric.aggregated([
            tsc1['return'], tsc2['return']], aggregation='mean', fill=0)

        self.assertEqual([
            (datetime64(2014, 1, 1, 12, 0, 0),
             numpy.timedelta64(60000000000, 'ns'), 3.0),
            (datetime64(2014, 1, 1, 12, 1, 0),
             numpy.timedelta64(60000000000, 'ns'), 1.0),
            (datetime64(2014, 1, 1, 12, 2, 0),
             numpy.timedelta64(60000000000, 'ns'), 6.5),
            (datetime64(2014, 1, 1, 12, 3, 0),
             numpy.timedelta64(60000000000, 'ns'), 16.5),
            (datetime64(2014, 1, 1, 12, 4, 0),
             numpy.timedelta64(60000000000, 'ns'), 2.5),
            (datetime64(2014, 1, 1, 12, 7, 0),
             numpy.timedelta64(60000000000, 'ns'), 2.5),
            (datetime64(2014, 1, 1, 12, 8, 0),
             numpy.timedelta64(60000000000, 'ns'), 1.5),
        ], list(output))

    def test_aggregated_nominal(self):
        tsc1 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 10, 'agg': 'mean'}
        tsc12 = {'sampling': numpy.timedelta64(300, 's'),
                 'size': 6, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc12['sampling'])
        tsc2 = {'sampling': numpy.timedelta64(60, 's'),
                'size': 10, 'agg': 'mean'}
        tsc22 = {'sampling': numpy.timedelta64(300, 's'),
                 'size': 6, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc22['sampling'])

        def ts1_update(ts):
            grouped = ts.group_serie(tsc1['sampling'])
            existing = tsc1.get('return')
            tsc1['return'] = carbonara.AggregatedTimeSerie.from_grouped_serie(
                grouped, tsc1['sampling'], tsc1['agg'],
                max_size=tsc1['size'], truncate=True)
            if existing:
                existing.merge(tsc1['return'])
            grouped = ts.group_serie(tsc12['sampling'])
            existing = tsc12.get('return')
            tsc12['return'] = carbonara.AggregatedTimeSerie.from_grouped_serie(
                grouped, tsc12['sampling'], tsc12['agg'],
                max_size=tsc12['size'], truncate=True)
            if existing:
                existing.merge(tsc12['return'])

        def ts2_update(ts):
            grouped = ts.group_serie(tsc2['sampling'])
            existing = tsc2.get('return')
            tsc2['return'] = carbonara.AggregatedTimeSerie.from_grouped_serie(
                grouped, tsc2['sampling'], tsc2['agg'],
                max_size=tsc2['size'], truncate=True)
            if existing:
                existing.merge(tsc2['return'])
            grouped = ts.group_serie(tsc22['sampling'])
            existing = tsc22.get('return')
            tsc22['return'] = carbonara.AggregatedTimeSerie.from_grouped_serie(
                grouped, tsc22['sampling'], tsc22['agg'],
                max_size=tsc22['size'], truncate=True)
            if existing:
                existing.merge(tsc22['return'])
        tsb1.set_values(numpy.array([
            (datetime64(2014, 1, 1, 11, 46, 4), 4),
            (datetime64(2014, 1, 1, 11, 47, 34), 8),
            (datetime64(2014, 1, 1, 11, 50, 54), 50),
            (datetime64(2014, 1, 1, 11, 54, 45), 4),
            (datetime64(2014, 1, 1, 11, 56, 49), 4),
            (datetime64(2014, 1, 1, 11, 57, 22), 6),
            (datetime64(2014, 1, 1, 11, 58, 22), 5),
            (datetime64(2014, 1, 1, 12, 1, 4), 4),
            (datetime64(2014, 1, 1, 12, 1, 9), 7),
            (datetime64(2014, 1, 1, 12, 2, 1), 15),
            (datetime64(2014, 1, 1, 12, 2, 12), 1),
            (datetime64(2014, 1, 1, 12, 3, 0), 3),
            (datetime64(2014, 1, 1, 12, 4, 9), 7),
            (datetime64(2014, 1, 1, 12, 5, 1), 15),
            (datetime64(2014, 1, 1, 12, 5, 12), 1),
            (datetime64(2014, 1, 1, 12, 6, 0), 3)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=ts1_update)

        tsb2.set_values(numpy.array([
            (datetime64(2014, 1, 1, 11, 46, 4), 6),
            (datetime64(2014, 1, 1, 11, 47, 34), 5),
            (datetime64(2014, 1, 1, 11, 50, 54), 51),
            (datetime64(2014, 1, 1, 11, 54, 45), 5),
            (datetime64(2014, 1, 1, 11, 56, 49), 5),
            (datetime64(2014, 1, 1, 11, 57, 22), 7),
            (datetime64(2014, 1, 1, 11, 58, 22), 5),
            (datetime64(2014, 1, 1, 12, 1, 4), 5),
            (datetime64(2014, 1, 1, 12, 1, 9), 8),
            (datetime64(2014, 1, 1, 12, 2, 1), 10),
            (datetime64(2014, 1, 1, 12, 2, 12), 2),
            (datetime64(2014, 1, 1, 12, 3, 0), 6),
            (datetime64(2014, 1, 1, 12, 4, 9), 4),
            (datetime64(2014, 1, 1, 12, 5, 1), 10),
            (datetime64(2014, 1, 1, 12, 5, 12), 1),
            (datetime64(2014, 1, 1, 12, 6, 0), 1)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=ts2_update)
        output = cross_metric.aggregated(
            [tsc1['return'], tsc12['return'], tsc2['return'], tsc22['return']],
            'mean')
        self.assertEqual([
            (datetime64(2014, 1, 1, 11, 45),
             numpy.timedelta64(300, 's'), 5.75),
            (datetime64(2014, 1, 1, 11, 50),
             numpy.timedelta64(300, 's'), 27.5),
            (datetime64(2014, 1, 1, 11, 55),
             numpy.timedelta64(300, 's'), 5.3333333333333339),
            (datetime64(2014, 1, 1, 12, 0),
             numpy.timedelta64(300, 's'), 6.0),
            (datetime64(2014, 1, 1, 12, 5),
             numpy.timedelta64(300, 's'), 5.1666666666666661),
            (datetime64(2014, 1, 1, 11, 54),
             numpy.timedelta64(60, 's'), 4.5),
            (datetime64(2014, 1, 1, 11, 56),
             numpy.timedelta64(60, 's'), 4.5),
            (datetime64(2014, 1, 1, 11, 57),
             numpy.timedelta64(60, 's'), 6.5),
            (datetime64(2014, 1, 1, 11, 58),
             numpy.timedelta64(60, 's'), 5.0),
            (datetime64(2014, 1, 1, 12, 1),
             numpy.timedelta64(60, 's'), 6.0),
            (datetime64(2014, 1, 1, 12, 2),
             numpy.timedelta64(60, 's'), 7.0),
            (datetime64(2014, 1, 1, 12, 3),
             numpy.timedelta64(60, 's'), 4.5),
            (datetime64(2014, 1, 1, 12, 4),
             numpy.timedelta64(60, 's'), 5.5),
            (datetime64(2014, 1, 1, 12, 5),
             numpy.timedelta64(60, 's'), 6.75),
            (datetime64(2014, 1, 1, 12, 6),
             numpy.timedelta64(60, 's'), 2.0),
        ], list(output))

    def test_aggregated_partial_overlap(self):
        tsc1 = {'sampling': numpy.timedelta64(1, 's'),
                'size': 86400, 'agg': 'mean'}
        tsb1 = carbonara.BoundTimeSerie(block_size=tsc1['sampling'])
        tsc2 = {'sampling': numpy.timedelta64(1, 's'),
                'size': 60, 'agg': 'mean'}
        tsb2 = carbonara.BoundTimeSerie(block_size=tsc2['sampling'])

        tsb1.set_values(numpy.array([
            (datetime64(2015, 12, 3, 13, 19, 15), 1),
            (datetime64(2015, 12, 3, 13, 20, 15), 1),
            (datetime64(2015, 12, 3, 13, 21, 15), 1),
            (datetime64(2015, 12, 3, 13, 22, 15), 1)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=tsc1))

        tsb2.set_values(numpy.array([
            (datetime64(2015, 12, 3, 13, 21, 15), 10),
            (datetime64(2015, 12, 3, 13, 22, 15), 10),
            (datetime64(2015, 12, 3, 13, 23, 15), 10),
            (datetime64(2015, 12, 3, 13, 24, 15), 10)],
            dtype=carbonara.TIMESERIES_ARRAY_DTYPE),
            before_truncate_callback=functools.partial(
                self._resample_and_merge, agg_dict=tsc2))

        output = cross_metric.aggregated(
            [tsc1['return'], tsc2['return']], aggregation="sum")

        self.assertEqual([
            (datetime64(
                2015, 12, 3, 13, 21, 15
            ), numpy.timedelta64(1, 's'), 11.0),
            (datetime64(
                2015, 12, 3, 13, 22, 15
            ), numpy.timedelta64(1, 's'), 11.0),
        ], list(output))

        dtfrom = datetime64(2015, 12, 3, 13, 17, 0)
        dtto = datetime64(2015, 12, 3, 13, 25, 0)

        output = cross_metric.aggregated(
            [tsc1['return'], tsc2['return']],
            from_timestamp=dtfrom, to_timestamp=dtto,
            aggregation="sum", needed_percent_of_overlap=0)
        self.assertEqual([
            (datetime64(
                2015, 12, 3, 13, 19, 15
            ), numpy.timedelta64(1, 's'), 1.0),
            (datetime64(
                2015, 12, 3, 13, 20, 15
            ), numpy.timedelta64(1, 's'), 1.0),
            (datetime64(
                2015, 12, 3, 13, 21, 15
            ), numpy.timedelta64(1, 's'), 11.0),
            (datetime64(
                2015, 12, 3, 13, 22, 15
            ), numpy.timedelta64(1, 's'), 11.0),
            (datetime64(
                2015, 12, 3, 13, 23, 15
            ), numpy.timedelta64(1, 's'), 10.0),
            (datetime64(
                2015, 12, 3, 13, 24, 15
            ), numpy.timedelta64(1, 's'), 10.0),
        ], list(output))

        # By default we require 100% of point that overlap
        # so that fail if from or to is set
        self.assertRaises(cross_metric.UnAggregableTimeseries,
                          cross_metric.aggregated,
                          [tsc1['return'], tsc2['return']],
                          to_timestamp=dtto, aggregation='mean')
        self.assertRaises(cross_metric.UnAggregableTimeseries,
                          cross_metric.aggregated,
                          [tsc1['return'], tsc2['return']],
                          from_timestamp=dtfrom, aggregation='mean')
        # Retry with 50% and it works
        output = cross_metric.aggregated(
            [tsc1['return'], tsc2['return']], from_timestamp=dtfrom,
            aggregation="sum",
            needed_percent_of_overlap=50.0)
        self.assertEqual([
            (datetime64(
                2015, 12, 3, 13, 19, 15
            ), numpy.timedelta64(1, 's'), 1.0),
            (datetime64(
                2015, 12, 3, 13, 20, 15
            ), numpy.timedelta64(1, 's'), 1.0),
            (datetime64(
                2015, 12, 3, 13, 21, 15
            ), numpy.timedelta64(1, 's'), 11.0),
            (datetime64(
                2015, 12, 3, 13, 22, 15
            ), numpy.timedelta64(1, 's'), 11.0),
        ], list(output))

        output = cross_metric.aggregated(
            [tsc1['return'], tsc2['return']], to_timestamp=dtto,
            aggregation="sum",
            needed_percent_of_overlap=50.0)
        self.assertEqual([
            (datetime64(
                2015, 12, 3, 13, 21, 15
            ), numpy.timedelta64(1, 's'), 11.0),
            (datetime64(
                2015, 12, 3, 13, 22, 15
            ), numpy.timedelta64(1, 's'), 11.0),
            (datetime64(
                2015, 12, 3, 13, 23, 15
            ), numpy.timedelta64(1, 's'), 10.0),
            (datetime64(
                2015, 12, 3, 13, 24, 15
            ), numpy.timedelta64(1, 's'), 10.0),
        ], list(output))


class CrossMetricAggregated(base.TestCase):
    def setUp(self):
        super(CrossMetricAggregated, self).setUp()
        # A lot of tests wants a metric, create one
        self.metric, __ = self._create_metric()

    def test_get_cross_metric_measures_unknown_metric(self):
        self.assertEqual([],
                         cross_metric.get_cross_metric_measures(
                             self.storage,
                             [storage.Metric(uuid.uuid4(),
                                             self.archive_policies['low']),
                              storage.Metric(uuid.uuid4(),
                                             self.archive_policies['low'])]))

    def test_get_cross_metric_measures_unknown_aggregation(self):
        metric2 = storage.Metric(uuid.uuid4(),
                                 self.archive_policies['low'])
        self.incoming.add_measures(self.metric, [
            storage.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime64(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime64(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime64(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.incoming.add_measures(metric2, [
            storage.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime64(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime64(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime64(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.assertRaises(storage.AggregationDoesNotExist,
                          cross_metric.get_cross_metric_measures,
                          self.storage,
                          [self.metric, metric2],
                          aggregation='last')

    def test_get_cross_metric_measures_unknown_granularity(self):
        metric2 = storage.Metric(uuid.uuid4(),
                                 self.archive_policies['low'])
        self.incoming.add_measures(self.metric, [
            storage.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime64(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime64(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime64(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.incoming.add_measures(metric2, [
            storage.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime64(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime64(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime64(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.assertRaises(storage.GranularityDoesNotExist,
                          cross_metric.get_cross_metric_measures,
                          self.storage,
                          [self.metric, metric2],
                          granularity=numpy.timedelta64(12345456, 'ms'))

    def test_add_and_get_cross_metric_measures_different_archives(self):
        metric2 = storage.Metric(uuid.uuid4(),
                                 self.archive_policies['no_granularity_match'])
        self.incoming.add_measures(self.metric, [
            storage.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime64(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime64(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime64(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.incoming.add_measures(metric2, [
            storage.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime64(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime64(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime64(2014, 1, 1, 12, 12, 45), 44),
        ])

        self.assertRaises(cross_metric.MetricUnaggregatable,
                          cross_metric.get_cross_metric_measures,
                          self.storage,
                          [self.metric, metric2])

    def test_add_and_get_cross_metric_measures(self):
        metric2, __ = self._create_metric()
        self.incoming.add_measures(self.metric, [
            storage.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime64(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime64(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime64(2014, 1, 1, 12, 12, 45), 44),
        ])
        self.incoming.add_measures(metric2, [
            storage.Measure(datetime64(2014, 1, 1, 12, 0, 5), 9),
            storage.Measure(datetime64(2014, 1, 1, 12, 7, 41), 2),
            storage.Measure(datetime64(2014, 1, 1, 12, 10, 31), 4),
            storage.Measure(datetime64(2014, 1, 1, 12, 13, 10), 4),
        ])
        self.trigger_processing([str(self.metric.id), str(metric2.id)])

        values = cross_metric.get_cross_metric_measures(
            self.storage, [self.metric, metric2])
        self.assertEqual([
            (datetime64(2014, 1, 1, 0, 0, 0),
             numpy.timedelta64(1, 'D'), 22.25),
            (datetime64(2014, 1, 1, 12, 0, 0),
             numpy.timedelta64(1, 'h'), 22.25),
            (datetime64(2014, 1, 1, 12, 0, 0),
             numpy.timedelta64(5, 'm'), 39.0),
            (datetime64(2014, 1, 1, 12, 5, 0),
             numpy.timedelta64(5, 'm'), 12.5),
            (datetime64(2014, 1, 1, 12, 10, 0),
             numpy.timedelta64(5, 'm'), 24.0)
        ], values)

        values = cross_metric.get_cross_metric_measures(
            self.storage, [self.metric, metric2], reaggregation='max')
        self.assertEqual([
            (datetime64(2014, 1, 1, 0, 0, 0),
             numpy.timedelta64(1, 'D'), 39.75),
            (datetime64(2014, 1, 1, 12, 0, 0),
             numpy.timedelta64(1, 'h'), 39.75),
            (datetime64(2014, 1, 1, 12, 0, 0),
             numpy.timedelta64(5, 'm'), 69),
            (datetime64(2014, 1, 1, 12, 5, 0),
             numpy.timedelta64(5, 'm'), 23),
            (datetime64(2014, 1, 1, 12, 10, 0),
             numpy.timedelta64(5, 'm'), 44)
        ], values)

        values = cross_metric.get_cross_metric_measures(
            self.storage, [self.metric, metric2],
            from_timestamp=datetime64(2014, 1, 1, 12, 10, 0))
        self.assertEqual([
            (datetime64(2014, 1, 1),
             numpy.timedelta64(1, 'D'), 22.25),
            (datetime64(2014, 1, 1, 12),
             numpy.timedelta64(1, 'h'), 22.25),
            (datetime64(2014, 1, 1, 12, 10, 0),
             numpy.timedelta64(5, 'm'), 24.0),
        ], values)

        values = cross_metric.get_cross_metric_measures(
            self.storage, [self.metric, metric2],
            to_timestamp=datetime64(2014, 1, 1, 12, 5, 0))

        self.assertEqual([
            (datetime64(2014, 1, 1, 0, 0, 0),
             numpy.timedelta64(1, 'D'), 22.25),
            (datetime64(2014, 1, 1, 12, 0, 0),
             numpy.timedelta64(1, 'h'), 22.25),
            (datetime64(2014, 1, 1, 12, 0, 0),
             numpy.timedelta64(5, 'm'), 39.0),
        ], values)

        values = cross_metric.get_cross_metric_measures(
            self.storage, [self.metric, metric2],
            from_timestamp=datetime64(2014, 1, 1, 12, 10, 10),
            to_timestamp=datetime64(2014, 1, 1, 12, 10, 10))
        self.assertEqual([
            (datetime64(2014, 1, 1),
             numpy.timedelta64(1, 'D'), 22.25),
            (datetime64(2014, 1, 1, 12),
             numpy.timedelta64(1, 'h'), 22.25),
            (datetime64(2014, 1, 1, 12, 10),
             numpy.timedelta64(5, 'm'), 24.0),
        ], values)

        values = cross_metric.get_cross_metric_measures(
            self.storage, [self.metric, metric2],
            from_timestamp=datetime64(2014, 1, 1, 12, 0, 0),
            to_timestamp=datetime64(2014, 1, 1, 12, 0, 1))

        self.assertEqual([
            (datetime64(2014, 1, 1),
             numpy.timedelta64(1, 'D'), 22.25),
            (datetime64(2014, 1, 1, 12, 0, 0),
             numpy.timedelta64(1, 'h'), 22.25),
            (datetime64(2014, 1, 1, 12, 0, 0),
             numpy.timedelta64(5, 'm'), 39.0),
        ], values)

        values = cross_metric.get_cross_metric_measures(
            self.storage, [self.metric, metric2],
            from_timestamp=datetime64(2014, 1, 1, 12, 0, 0),
            to_timestamp=datetime64(2014, 1, 1, 12, 0, 1),
            granularity=numpy.timedelta64(5, 'm'))

        self.assertEqual([
            (datetime64(2014, 1, 1, 12, 0, 0),
             numpy.timedelta64(5, 'm'), 39.0),
        ], values)

    def test_add_and_get_cross_metric_measures_with_holes(self):
        metric2, __ = self._create_metric()
        self.incoming.add_measures(self.metric, [
            storage.Measure(datetime64(2014, 1, 1, 12, 0, 1), 69),
            storage.Measure(datetime64(2014, 1, 1, 12, 7, 31), 42),
            storage.Measure(datetime64(2014, 1, 1, 12, 5, 31), 8),
            storage.Measure(datetime64(2014, 1, 1, 12, 9, 31), 4),
            storage.Measure(datetime64(2014, 1, 1, 12, 12, 45), 42),
        ])
        self.incoming.add_measures(metric2, [
            storage.Measure(datetime64(2014, 1, 1, 12, 0, 5), 9),
            storage.Measure(datetime64(2014, 1, 1, 12, 7, 31), 2),
            storage.Measure(datetime64(2014, 1, 1, 12, 9, 31), 6),
            storage.Measure(datetime64(2014, 1, 1, 12, 13, 10), 2),
        ])
        self.trigger_processing([str(self.metric.id), str(metric2.id)])

        values = cross_metric.get_cross_metric_measures(
            self.storage, [self.metric, metric2])
        self.assertEqual([
            (datetime64(2014, 1, 1, 0, 0, 0),
             numpy.timedelta64(1, 'D'), 18.875),
            (datetime64(2014, 1, 1, 12, 0, 0),
             numpy.timedelta64(1, 'h'), 18.875),
            (datetime64(2014, 1, 1, 12, 0, 0),
             numpy.timedelta64(5, 'm'), 39.0),
            (datetime64(2014, 1, 1, 12, 5, 0),
             numpy.timedelta64(5, 'm'), 11.0),
            (datetime64(2014, 1, 1, 12, 10, 0),
             numpy.timedelta64(5, 'm'), 22.0)
        ], values)
