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
import itertools
import operator

import daiquiri

from gnocchi import indexer


ITEMGETTER_1 = operator.itemgetter(1)

LOG = daiquiri.getLogger(__name__)


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
        # FIXME(jd) The indexer could return them sorted/grouped by directly
        metrics_to_expunge = sorted(
            ((m, self.incoming.sack_for_metric(m.id))
             for m in self.index.list_metrics(status='delete')),
            key=ITEMGETTER_1)
        for sack, metrics in itertools.groupby(
                metrics_to_expunge, key=ITEMGETTER_1):
            try:
                lock = self.incoming.get_sack_lock(self.coord, sack)
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
                for metric, sack in metrics:
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
