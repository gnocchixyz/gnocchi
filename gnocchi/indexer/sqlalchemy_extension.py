# -*- encoding: utf-8 -*-

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

from __future__ import absolute_import

import sqlalchemy
import sqlalchemy_utils

from gnocchi.indexer import sqlalchemy_types
from gnocchi import resource_type


class SchemaMixin(object):
    def for_filling(self, dialect):
        # NOTE(sileht): This must be used only for patching resource type
        # to fill all row with a default value and then switch back the
        # server_default to None
        if self.fill is None:
            return None

        # NOTE(sileht): server_default must be converted in sql element
        return sqlalchemy.literal(self.fill)


class StringSchema(resource_type.StringSchema, SchemaMixin):
    @property
    def satype(self):
        return sqlalchemy.String(self.max_length)


class UUIDSchema(resource_type.UUIDSchema, SchemaMixin):
    satype = sqlalchemy_utils.UUIDType()

    def for_filling(self, dialect):
        if self.fill is None:
            return None
        return sqlalchemy.literal(
            self.satype.process_bind_param(self.fill, dialect))


class NumberSchema(resource_type.NumberSchema, SchemaMixin):
    satype = sqlalchemy.Float(53)


class BoolSchema(resource_type.BoolSchema, SchemaMixin):
    satype = sqlalchemy.Boolean


class DatetimeSchema(resource_type.DatetimeSchema, SchemaMixin):
    satype = sqlalchemy_types.TimestampUTC()

    def for_filling(self, dialect):
        if self.fill is None:
            return None
        return self.satype.process_bind_param(self.fill, dialect).isoformat()
