# -*- encoding: utf-8 -*-
#
# Copyright (c) 2018 Red Hat
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import hashlib

import daiquiri
import datetime
import random

from gnocchi import carbonara
from gnocchi import indexer
from gnocchi import utils

LOG = daiquiri.getLogger(__name__)


class SackAlreadyLocked(Exception):
    def __init__(self, sack):
        self.sack = sack
        super(SackAlreadyLocked, self).__init__(
            "Sack %s already locked" % sack)


class Chef(object):
    """A master of cooking gnocchi.

    Give it a few tools and it'll make you happy!

    The Chef is responsible for executing actions that requires several drivers
    at the same time, such as the coordinator, the incoming and storage
    drivers, or the indexer.

    """

    def __init__(self, coord, incoming, index, storage):
        self.coord = coord
        self.incoming = incoming
        # This variable is an instance of the indexer,
        # which means, database connector.
        self.index = index
        self.storage = storage

    def resource_ended_at_normalization(self, metric_inactive_after):
        """Marks resources as ended at if needed.

        This method will check all metrics that have not received new
        datapoints after a given period. The period is defined by
        'metric_inactive_after' parameter. If all metrics of resource are in
        inactive state, we mark the ended_at field with a timestmap. Therefore,
        we consider that the resource has ceased existing.

        In this process we will handle only metrics that are considered as
        inactive, according to `metric_inactive_after` parameter. Therefore,
        we do not need to lock these metrics while processing, as they are
        inactive, and chances are that they will not receive measures anymore.
        Moreover, we are only touching metadata, and not the actual data.
        """

        momment_now = utils.utcnow()
        momment = momment_now - datetime.timedelta(
            seconds=metric_inactive_after)

        inactive_metrics = self.index.list_metrics(
            attribute_filter={"<": {
                "last_measure_timestamp": momment}},
            resource_policy_filter={"==": {"ended_at": None}}
        )

        LOG.debug("Inactive metrics found for processing: [%s].",
                  inactive_metrics)

        inactive_metrics_by_resource_id = {}
        for metric in inactive_metrics:
            resource_id = metric.resource_id
            if inactive_metrics_by_resource_id.get(resource_id) is None:
                inactive_metrics_by_resource_id[resource_id] = []

            inactive_metrics_by_resource_id[resource_id].append(metric)

        for resource_id in inactive_metrics_by_resource_id.keys():
            if resource_id is None:
                LOG.debug("We do not need to process inactive metrics that do "
                          "not have resource. Therefore, these metrics [%s] "
                          "will be considered inactive, but there is nothing "
                          "else we can do in this process.",
                          inactive_metrics_by_resource_id[resource_id])
                continue
            resource = self.index.get_resource(
                "generic", resource_id, with_metrics=True)
            resource_metrics = resource.metrics
            resource_inactive_metrics = inactive_metrics_by_resource_id.get(resource_id)

            all_metrics_are_inactive = True
            for m in resource_metrics:
                if m not in resource_inactive_metrics:
                    all_metrics_are_inactive = False
                    LOG.debug("Not all metrics of resource [%s] are inactive. "
                              "Metric [%s] is not inactive. The inactive "
                              "metrics are [%s].",
                              resource, m, resource_inactive_metrics)
                    break

            if all_metrics_are_inactive:
                LOG.info("All metrics [%s] of resource [%s] are inactive."
                         "Therefore, we will mark it as finished with an"
                         "ended at timestmap.", resource_metrics, resource)
                if resource.ended_at is not None:
                    LOG.debug(
                        "Resource [%s] already has an ended at value.", resource)
                else:
                    LOG.info("Marking ended at timestamp for resource "
                             "[%s] because all of its metrics are inactive.",
                             resource)
                    self.index.update_resource(
                        "generic", resource_id, ended_at=momment_now)

    def clean_raw_data_inactive_metrics(self):
        """Cleans metrics raw data if they are inactive.

        The truncating of the raw metrics data is only done when new
        measures are pushed. Therefore, if no new measures are pushed, and the
        archive policy was updated to reduce the backwindow, the raw
        datapoints for metrics that are not receiving new datapoints are never
        truncated.

        The goal of this method is to identify metrics that are in
        "inactive state", meaning, not receiving new datapoints, and execute
        their raw data points truncation. We check the column
        "needs_raw_data_truncation", to determine if the archive policy was
        updated, and no measure push was executed for the metric.

        If the metric is not receiving new datapoints, the processing workflow
        will not mark the column "needs_raw_data_truncation" to False;
        therefore, that is how we identify such metrics.
        """

        metrics_to_clean = self.index.list_metrics(
            attribute_filter={"==": {
                "needs_raw_data_truncation": True}}
        )

        LOG.debug("Metrics [%s] found to execute the raw data cleanup.",
                  metrics_to_clean)

        sack_by_metric = self.group_metrics_by_sack(metrics_to_clean)

        # We randomize the list to reduce the chances of lock collision.
        all_sacks = list(sack_by_metric.keys())
        random.shuffle(all_sacks)
        for sack in all_sacks:
            LOG.debug("Executing the raw data cleanup for sack [%s].",
                      sack)
            try:
                sack_lock = self.get_sack_lock(sack)

                if not sack_lock.acquire():
                    LOG.debug(
                        "Sack [%s] is locked, cannot clean its metric "
                        "now. Probably some other agent is processing its "
                        "metrics.", sack)
                    continue

                sack_metrics = sack_by_metric[sack]

                for metric in sack_metrics:
                    self.execute_raw_data_cleanup(metric)
            except Exception:
                LOG.error("Unable to lock sack [%s] for cleanup.",
                          sack, exc_info=True)
                continue
            finally:
                if sack_lock:
                    sack_lock.release()
                    LOG.debug("Releasing lock [%s] for sack [%s].",
                              sack_lock, sack)
                else:
                    LOG.debug(
                        "There is no lock for sack [%s] to be released.",
                        sack)

        if metrics_to_clean:
            LOG.debug("Cleaned up metrics [%s].", metrics_to_clean)

    def execute_raw_data_cleanup(self, metric):
        LOG.debug("Executing the raw data cleanup for metric [%s].",
                  metric)

        agg_methods = list(metric.archive_policy.aggregation_methods)
        block_size = metric.archive_policy.max_block_size
        back_window = metric.archive_policy.back_window

        if any(filter(lambda x: x.startswith("rate:"), agg_methods)):
            back_window += 1

        raw_measure = self.storage. \
            _get_or_create_unaggregated_timeseries_unbatched(metric)

        if raw_measure:
            LOG.debug("Truncating metric [%s] for backwindow [%s].",
                      metric.id, back_window)

            ts = carbonara.BoundTimeSerie.unserialize(raw_measure,
                                                      block_size,
                                                      back_window)
            # Trigger the truncation process to remove the excess of
            # raw data according to the updated back_window.
            ts._truncate()

            self.storage._store_unaggregated_timeseries_unbatched(
                metric, ts.serialize())
        else:
            LOG.debug("No raw measures found for metric [%s] for "
                      "cleanup.", metric.id)

        self.index.update_needs_raw_data_truncation(metric.id)

    def group_metrics_by_sack(self, metrics_to_clean):
        sack_by_metric = {}
        for metric in metrics_to_clean:
            sack_for_metric = self.incoming.sack_for_metric(metric.id)
            if sack_for_metric not in sack_by_metric.keys():
                sack_by_metric[sack_for_metric] = []

            sack_by_metric[sack_for_metric].append(metric)
        LOG.debug("Metrics grouped by sacks: [%s].", sack_by_metric)
        return sack_by_metric

    def expunge_metrics(self, cleanup_batch_size, sync=False):
        """Remove deleted metrics.

        :param cleanup_batch_size: The amount of metrics to delete in one
                                   run.
        :param sync: If True, then delete everything synchronously and raise
                     on error
        :type sync: bool
        """
        metrics_to_expunge = self.index.list_metrics(status='delete',
                                                     limit=cleanup_batch_size)
        metrics_by_id = {m.id: m for m in metrics_to_expunge}
        for sack, metric_ids in self.incoming.group_metrics_by_sack(
                metrics_by_id.keys()):
            try:
                lock = self.get_sack_lock(sack)
                if not lock.acquire(blocking=sync):
                    # Retry later
                    LOG.debug(
                        "Sack %s is locked, cannot expunge metrics", sack)
                    continue
                # NOTE(gordc): no need to hold lock because the metric has been
                # already marked as "deleted" in the indexer so no measure
                # worker is going to process it anymore.
                lock.release()
            except Exception:
                if sync:
                    raise
                LOG.error("Unable to lock sack %s for expunging metrics",
                          sack, exc_info=True)
            else:
                for metric_id in metric_ids:
                    metric = metrics_by_id[metric_id]
                    LOG.debug("Deleting metric %s", metric)
                    try:
                        self.incoming.delete_unprocessed_measures_for_metric(
                            metric.id)
                        self.storage._delete_metric(metric)
                        try:
                            self.index.expunge_metric(metric.id)
                        except indexer.NoSuchMetric:
                            # It's possible another process deleted or is
                            # deleting the metric, not a big deal
                            pass
                    except Exception:
                        if sync:
                            raise
                        LOG.error("Unable to expunge metric %s from storage",
                                  metric, exc_info=True)

    def refresh_metrics(self, metrics, timeout=None, sync=False):
        """Process added measures in background for some metrics only.

        :param metrics: The list of `indexer.Metric` to refresh.
        :param timeout: Time to wait for the process to happen.
        :param sync: If an error occurs, raise, otherwise just log it.
        """
        # process only active metrics. deleted metrics with unprocessed
        # measures will be skipped until cleaned by janitor.
        metrics_by_id = {m.id: m for m in metrics}
        for sack, metric_ids in self.incoming.group_metrics_by_sack(
                metrics_by_id.keys()):
            lock = self.get_sack_lock(sack)
            # FIXME(jd) timeout should be global for all sack locking
            if not lock.acquire(blocking=timeout):
                raise SackAlreadyLocked(sack)
            try:
                LOG.debug("Processing measures for %d metrics",
                          len(metric_ids))
                with self.incoming.process_measure_for_metrics(
                        metric_ids) as metrics_and_measures:
                    if metrics_and_measures:
                        self.storage.add_measures_to_metrics({
                            metrics_by_id[metric_id]: measures
                            for metric_id, measures
                            in metrics_and_measures.items()
                        }, self.index)
                        LOG.debug("Measures for %d metrics processed",
                                  len(metric_ids))
            except Exception:
                if sync:
                    raise
                LOG.error("Error processing new measures", exc_info=True)
            finally:
                lock.release()

    def process_new_measures_for_sack(self, sack, blocking=False, sync=False):
        """Process added measures in background.

        Lock a sack and try to process measures from it. If the sack cannot be
        locked, the method will raise `SackAlreadyLocked`.

        :param sack: The sack to process new measures for.
        :param blocking: Block to be sure the sack is processed or raise
                         `SackAlreadyLocked` otherwise.
        :param sync: If True, raise any issue immediately otherwise just log it
        :return: The number of metrics processed.

        """
        lock = self.get_sack_lock(sack)
        if not lock.acquire(blocking=blocking):
            raise SackAlreadyLocked(sack)
        LOG.debug("Processing measures for sack %s", sack)
        try:
            with self.incoming.process_measures_for_sack(sack) as measures:
                # process only active metrics. deleted metrics with unprocessed
                # measures will be skipped until cleaned by janitor.
                if not measures:
                    return 0

                metrics = self.index.list_metrics(
                    attribute_filter={
                        "in": {"id": measures.keys()}
                    })
                self.storage.add_measures_to_metrics({
                    metric: measures[metric.id]
                    for metric in metrics
                }, self.index)
                LOG.debug("Measures for %d metrics processed",
                          len(metrics))
                return len(measures)
        except Exception:
            if sync:
                raise
            LOG.error("Error processing new measures", exc_info=True)
            return 0
        finally:
            lock.release()

    def get_sack_lock(self, sack):
        # FIXME(jd) Some tooz drivers have a limitation on lock name length
        # (e.g. MySQL). This should be handled by tooz, but it's not yet.
        lock_name = ('gnocchi-sack-%s-lock' % str(sack)).encode()
        lock_name = hashlib.new('sha1', lock_name).hexdigest().encode()
        return self.coord.get_lock(lock_name)
