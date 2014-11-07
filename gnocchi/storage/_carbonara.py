# -*- encoding: utf-8 -*-
#
# Copyright © 2014 eNovance
#
# Authors: Julien Danjou <julien@danjou.info>
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
import pandas

from gnocchi import carbonara
from gnocchi import storage


class CarbonaraBasedStorage(storage.StorageDriver, storage.CoordinatorMixin):
    def __init__(self, conf):
        super(CarbonaraBasedStorage, self).__init__(conf)
        self._init_coordinator(conf.coordination_url)

    @staticmethod
    def _create_entity_container(entity):
        pass

    def create_entity(self, entity, archive_policy):
        self._create_entity_container(entity)
        for aggregation in self.aggregation_types:
            # TODO(jd) Having the TimeSerieArchive.timeserie duplicated in
            # each archive isn't the most efficient way of doing things. We
            # may want to store it as its own object.
            # TODO(jd) We should not use Pandas here
            # – abstraction layer violation!
            archive = carbonara.TimeSerieArchive.from_definitions(
                [(pandas.tseries.offsets.Second(v['granularity']), v['points'])
                 for v in archive_policy],
                aggregation_method=aggregation)
            self._store_entity_measures(entity, aggregation,
                                        archive.serialize())

    @staticmethod
    def _get_measures(entity, aggregation):
        raise NotImplementedError

    @staticmethod
    def _store_entity_measures(entity, aggregation, data):
        raise NotImplementedError

    def get_measures(self, entity, from_timestamp=None, to_timestamp=None,
                     aggregation='mean'):
        contents = self._get_measures(entity, aggregation)
        archive = carbonara.TimeSerieArchive.unserialize(contents)
        return archive.fetch(from_timestamp, to_timestamp)

    def add_measures(self, entity, measures):
        # We are going to iterate multiple time over measures, so if it's a
        # generator we need to build a list out of it right now.
        measures = list(measures)
        for aggregation in self.aggregation_types:
            lock_name = (b"gnocchi-" + entity.encode('ascii')
                         + b"-" + aggregation.encode('ascii'))
            lock = self.coord.get_lock(lock_name)
            with lock:
                contents = self._get_measures(entity, aggregation)
                archive = carbonara.TimeSerieArchive.unserialize(contents)
                archive.set_values([(m.timestamp, m.value) for m in measures])
                self._store_entity_measures(entity, aggregation,
                                            archive.serialize())
