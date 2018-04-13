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
import six

from gnocchi import indexer


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
        self.index = index
        self.storage = storage

    def expunge_metrics(self, sync=False):
        """Remove deleted metrics.

        :param sync: If True, then delete everything synchronously and raise
                     on error
        :type sync: bool
        """
        metrics_to_expunge = self.index.list_metrics(status='delete')
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
                            in six.iteritems(metrics_and_measures)
                        })
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
                })
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
        lock_name = hashlib.new(
            'sha1',
            ('gnocchi-sack-%s-lock' % str(sack)).encode()).hexdigest().encode()
        return self.coord.get_lock(lock_name)
