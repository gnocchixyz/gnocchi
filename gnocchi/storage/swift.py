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
import gzip
import random
import uuid

from oslo.config import cfg
import pandas
import six
from swiftclient import client as swclient
from tooz import coordination

from gnocchi import carbonara
from gnocchi import storage


OPTIONS = [
    cfg.StrOpt('swift_auth_version',
               default='1',
               help='Swift authentication version to user.'),
    cfg.StrOpt('swift_preauthurl',
               default=None,
               help='Swift pre-auth URL.'),
    cfg.StrOpt('swift_authurl',
               default="http://localhost:8080/auth/v1.0",
               help='Swift auth URL.'),
    cfg.StrOpt('swift_preauthtoken',
               default=None,
               help='Swift token to user to authenticate.'),
    cfg.StrOpt('swift_user',
               default="admin:admin",
               help='Swift user.'),
    cfg.StrOpt('swift_key',
               default="admin",
               help='Swift key/password.'),
    cfg.StrOpt('swift_tenant_name',
               help='Swift tenant name, only used in v2 auth.'),
    cfg.StrOpt('swift_coordination_driver',
               help='Coordination driver',
               default='memcached'),
]

cfg.CONF.register_opts(OPTIONS, group="storage")


class SwiftStorage(storage.StorageDriver):
    def __init__(self, conf):
        self.swift = swclient.Connection(
            auth_version=conf.swift_auth_version,
            authurl=conf.swift_authurl,
            preauthtoken=conf.swift_preauthtoken,
            user=conf.swift_user,
            key=conf.swift_key,
            tenant_name=conf.swift_tenant_name)
        self.compresslevel = conf.compression_level
        self.coord = coordination.get_coordinator(
            conf.swift_coordination_driver,
            str(uuid.uuid4()).encode('ascii'))
        self.coord.start()
        # NOTE(jd) So this is a (smart?) optimization: since we're going to
        # lock for each of this aggregation type, if we are using running
        # Gnocchi with multiple processses, let's randomize what we iter
        # over so there are less chances we fight for the same lock!
        self.aggregation_types = list(storage.AGGREGATION_TYPES)
        random.shuffle(self.aggregation_types)

    def create_entity(self, entity, archive):
        try:
            self.swift.head_container(entity)
        except swclient.ClientException as e:
            if e.http_status != 404:
                raise
        else:
            raise storage.EntityAlreadyExists(entity)
        # TODO(jd) A container per user in their account?
        # TODO(jd) put_container does not return anything, but if it
        # returned the status code, we could guess that 201 is created
        # (entity did not exist) whereas 204 means it already exist, so we
        # would raise EntityAlreadyExists without doing a head_container()
        # before – needs https://review.openstack.org/#/c/87575/
        self.swift.put_container(entity)
        for aggregation in self.aggregation_types:
            tsc = carbonara.TimeSerieCollection([
                carbonara.TimeSerie([], [],
                                    aggregation_method=aggregation,
                                    max_size=size,
                                    sampling=pandas.tseries.offsets.Second(
                                        second))
                for second, size in archive
            ])
            compressed = six.BytesIO()
            z = gzip.GzipFile(
                fileobj=compressed, mode="wb",
                compresslevel=self.compresslevel)
            z.write(tsc.serialize())
            z.close()
            self.swift.put_object(entity, aggregation,
                                  compressed.getvalue())

    def delete_entity(self, entity):
        try:
            self.swift.delete_container(entity)
        except swclient.ClientException as e:
            if e.http_status == 404:
                raise storage.EntityDoesNotExist(entity)
            raise

    def add_measures(self, entity, measures):
        # We are going to iterate multiple time over measures, so if it's a
        # generator we need to build a list out of it right now.
        measures = list(measures)
        for aggregation in self.aggregation_types:
            # NOTE(jd) Use a lock to not update the same entity+aggregation
            # carbonara at the same time. If we don't do that, someone might
            # other work might run add_measures() at the same time and we
            # might overwrite its measures when re-puting the file in Swift.
            # This should be replaceable by using a mechanism where we store
            # the ETag when getting the object from Switch, then put with
            # If-Match, and then restart the whole get/update/put if the put
            # returned 412 (If-Match failed). But for now Swift does not
            # support If-Match with ETag. :(
            with self.coord.get_lock(b"gnocchi-" + entity.encode('ascii')
                                     + b"-" + aggregation.encode('ascii')):
                try:
                    headers, contents = self.swift.get_object(
                        entity, aggregation)
                except swclient.ClientException as e:
                    if e.http_status == 404:
                        raise storage.EntityDoesNotExist(entity)
                    raise
                tsc = carbonara.TimeSerieCollection.unserialize(
                    gzip.GzipFile(fileobj=six.BytesIO(contents)).read())
                for measure in measures:
                    tsc[measure.timestamp] = measure.value
                compressed = six.BytesIO()
                z = gzip.GzipFile(fileobj=compressed, mode="wb",
                                  compresslevel=self.compresslevel)
                z.write(tsc.serialize())
                z.close()
                self.swift.put_object(entity, aggregation,
                                      compressed.getvalue())

    def get_measures(self, entity, from_timestamp=None, to_timestamp=None,
                     aggregation='mean'):
        try:
            headers, contents = self.swift.get_object(entity, aggregation)
        except swclient.ClientException as e:
            if e.http_status == 404:
                raise storage.EntityDoesNotExist(entity)
            raise
        tsc = carbonara.TimeSerieCollection.unserialize(
            gzip.GzipFile(fileobj=six.BytesIO(contents)).read())
        return dict(tsc.fetch(from_timestamp, to_timestamp))
