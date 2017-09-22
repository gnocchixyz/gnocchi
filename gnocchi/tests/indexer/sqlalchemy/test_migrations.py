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
import operator

import fixtures
import mock
import oslo_db.exception
from oslo_db.sqlalchemy import test_migrations
import six
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

    def db_sync(self, engine):
        # NOTE(sileht): We ensure all resource type sqlalchemy model are loaded
        # in this process
        for rt in self.index.list_resource_types():
            if rt.state == "active":
                self.index._RESOURCE_TYPE_MANAGER.get_classes(rt)

    def filter_metadata_diff(self, diff):
        new_diff = []
        for item in diff:
            if item[0] == "add_table":
                cols = tuple(sorted(map(operator.attrgetter("name"),
                                        item[1].c)))
                # NOTES(sileht): prometheus resource type tables
                if cols in [("id", "instance", "job"),
                            ("instance", "job", "revision")]:
                    continue

            new_diff.append(item)
        return new_diff
