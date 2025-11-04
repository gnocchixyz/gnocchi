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
import oslo_db.exception
from oslo_db.sqlalchemy import test_migrations

import sqlalchemy as sa
import sqlalchemy_utils

from unittest import mock

from gnocchi import indexer
from gnocchi.indexer import sqlalchemy as gnocchi_sqlalchemy
from gnocchi.indexer import sqlalchemy_base as gnocchi_sqlalchemy_base
from gnocchi.tests import base


class ABCSkip(base.SkipNotImplementedMeta, abc.ABCMeta):
    pass


class ModelsMigrationsSync(base.TestCase,
                           test_migrations.ModelsMigrationsSync,
                           metaclass=ABCSkip):

    def setUp(self):
        super(ModelsMigrationsSync, self).setUp()
        self.useFixture(fixtures.Timeout(120, gentle=True))
        self.db = mock.Mock()
        self.conf.set_override(
            'url',
            gnocchi_sqlalchemy.SQLAlchemyIndexer._create_new_database(
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

        for table in gnocchi_sqlalchemy_base.Base.metadata.sorted_tables:
            if (table.name.startswith("rt_") and
                    table.name not in valid_resource_type_tables):
                gnocchi_sqlalchemy_base.Base.metadata.remove(table)
                self.index._RESOURCE_TYPE_MANAGER._cache.pop(
                    table.name.replace('_history', ''), None)

    def _drop_database(self):
        try:
            # We need to close any other connection before calling the database drop method.
            self.index.get_engine().dispose()

            sqlalchemy_utils.drop_database(self.conf.indexer.url)
        except oslo_db.exception.DBNonExistentDatabase:
            # NOTE(sileht): oslo db >= 4.15.0 cleanup this for us
            pass

    @staticmethod
    def get_metadata():
        return gnocchi_sqlalchemy_base.Base.metadata

    def get_engine(self):
        return self.index.get_engine()

    def compare_server_default(self, ctxt, ins_col, meta_col, insp_def, meta_def, rendered_meta_def):
        """Compare default values between model and db table.

        Return True if the defaults are different, False if not, or None to
        allow the default implementation to compare these defaults.

        :param ctxt: alembic MigrationContext instance
        :param ins_col: reflected column
        :param insp_def: reflected column default value
        :param meta_col: column from model
        :param meta_def: column default value from model
        :param rendered_meta_def: rendered column default value (from model)

        When the column has server_default=sqlalchemy.sql.func.now(), the diff includes the followings diff
         [ [ ( 'modify_default',
               None,
               'metric',
               'last_measure_timestamp',
               { 'existing_comment': None,
                 'existing_nullable': False,
                 'existing_type': DATETIME()},
               DefaultClause(<sqlalchemy.sql.elements.TextClause object at 0x7f0100b24b50>, for_update=False),
               DefaultClause(<sqlalchemy.sql.functions.now at 0x7f01010b08d0; now>, for_update=False))]]

        """

        method_return = super(ModelsMigrationsSync, self).compare_server_default(ctxt, ins_col, meta_col, insp_def,
                                                                                 meta_def, rendered_meta_def)

        is_meta_column_default_timestamp = meta_def is not None and isinstance(
            meta_def.arg, sa.sql.functions.current_timestamp)
        is_reflected_column_default_text_type = ins_col is not None and ins_col.server_default is not None and \
            isinstance(ins_col.server_default.arg, sa.sql.elements.TextClause)

        is_server_default_current_timestamp = is_meta_column_default_timestamp and is_reflected_column_default_text_type

        if not is_server_default_current_timestamp:
            return method_return

        # If it is different from "CURRENT_TIMESTAMP", then we must return True, so the test flow continues.
        return rendered_meta_def != "CURRENT_TIMESTAMP"
