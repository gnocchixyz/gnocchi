# -*- encoding: utf-8 -*-
#
# Copyright © 2014 Objectif Libre
#
# Authors: Stéphane Albert
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
import errno
import os
import shutil

from oslo.config import cfg
import pandas

from gnocchi import carbonara
from gnocchi import storage


OPTS = [
    cfg.StrOpt('file_basepath',
               default='/var/lib/gnocchi',
               help='Path used to store gnocchi data files.'),
]

cfg.CONF.register_opts(OPTS, group="storage")


class FileStorage(storage.StorageDriver, storage.CoordinatorMixin):
    def __init__(self, conf):
        super(FileStorage, self).__init__(conf)
        self.basepath = conf.file_basepath
        self._init_coordinator(conf.coordination_url)

    def create_entity(self, entity, archive_policy):
        path = os.path.join(self.basepath, entity)
        try:
            os.mkdir(path, 0o750)
        except OSError as e:
            if e.errno == errno.EEXIST:
                raise storage.EntityAlreadyExists(entity)
            raise
        for aggregation in self.aggregation_types:
            # TODO(jd) Having the TimeSerieArchive.timeserie duplicated in
            # each archive isn't the most efficient way of doing things. We
            # may want to store it as its own object.
            tsc = carbonara.TimeSerieArchive.from_definitions(
                [(pandas.tseries.offsets.Second(v['granularity']), v['points'])
                 for v in archive_policy],
                aggregation_method=aggregation)
            aggregation_path = os.path.join(path, aggregation)
            with open(aggregation_path, 'wb') as aggregation_file:
                aggregation_file.write(tsc.serialize())

    def delete_entity(self, entity):
        path = os.path.join(self.basepath, entity)
        try:
            shutil.rmtree(path)
        except OSError as e:
            if e.errno == errno.ENOENT:
                raise storage.EntityDoesNotExist(entity)
            raise

    def add_measures(self, entity, measures):
        # We are going to iterate multiple time over measures, so if it's a
        # generator we need to build a list out of it right now.
        measures = list(measures)
        entity_path = os.path.join(self.basepath, entity)
        for aggregation in self.aggregation_types:
            lock_name = (b"gnocchi-" + entity.encode('ascii')
                         + b"-" + aggregation.encode('ascii'))
            lock = self.coord.get_lock(lock_name)
            with lock:
                try:
                    aggregation_path = os.path.join(entity_path, aggregation)
                    with open(aggregation_path, 'rb') as aggregation_file:
                        contents = aggregation_file.read()
                except IOError as e:
                    if e.errno == errno.ENOENT:
                        raise storage.EntityDoesNotExist(entity)
                    raise
                else:
                    tsc = carbonara.TimeSerieArchive.unserialize(contents)
                    tsc.set_values([(m.timestamp, m.value) for m in measures])
                    with open(aggregation_path, 'wb') as aggregation_file:
                        aggregation_file.write(tsc.serialize())

    def get_measures(self, entity, from_timestamp=None, to_timestamp=None,
                     aggregation='mean'):
        path = os.path.join(self.basepath, entity, aggregation)

        try:
            with open(path, 'rb') as aggregation_file:
                contents = aggregation_file.read()
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise storage.EntityDoesNotExist(entity)
            raise
        tsc = carbonara.TimeSerieArchive.unserialize(contents)
        return dict(tsc.fetch(from_timestamp, to_timestamp))
