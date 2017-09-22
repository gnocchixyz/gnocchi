# Copyright 2015 eNovance
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
import abc

import fixtures
import mock
import oslo_db.exception
from oslo_db.sqlalchemy import test_migrations
import six
import sqlalchemy.schema
import sqlalchemy_utils

from gnocchi import indexer
from gnocchi.indexer import sqlalchemy
from gnocchi.indexer import sqlalchemy_base
from gnocchi.tests import base


class ABCSkip(base.SkipNotImplementedMeta, abc.ABCMeta):
    pass


class ModelsMigrationsSync(
        six.with_metaclass(ABCSkip,
                           base.TestCase,
                           test_migrations.ModelsMigrationsSync)):

    def setUp(self):
        super(ModelsMigrationsSync, self).setUp()
        self.useFixture(fixtures.Timeout(120, gentle=True))
        self.db = mock.Mock()
        self.conf.set_override(
            'url',
            sqlalchemy.SQLAlchemyIndexer._create_new_database(
                self.conf.indexer.url),
            'indexer')
        self.index = indexer.get_driver(self.conf)
        self.index.upgrade(nocreate=True)
        self.addCleanup(self._drop_database)

        # NOTE(sileht): remove tables dynamically created by other tests
        valid_resource_type_tables = []
        for rt in self.index.list_resource_types():
            valid_resource_type_tables.append(rt.tablename)
            valid_resource_type_tables.append("%s_history" % rt.tablename)
            # NOTE(sileht): load it in sqlalchemy metadata
            self.index._RESOURCE_TYPE_MANAGER.get_classes(rt)

        for table in sqlalchemy_base.Base.metadata.sorted_tables:
            if (table.name.startswith("rt_") and
                    table.name not in valid_resource_type_tables):
                sqlalchemy_base.Base.metadata.remove(table)
                self.index._RESOURCE_TYPE_MANAGER._cache.pop(
                    table.name.replace('_history', ''), None)

    def _drop_database(self):
        try:
            sqlalchemy_utils.drop_database(self.conf.indexer.url)
        except oslo_db.exception.DBNonExistentDatabase:
            # NOTE(sileht): oslo db >= 4.15.0 cleanup this for us
            pass

    @staticmethod
    def get_metadata():
        return sqlalchemy_base.Base.metadata

    def get_engine(self):
        return self.index.get_engine()
