# -*- encoding: utf-8 -*-
#
# Copyright © 2016 Red Hat, Inc.
# Copyright © 2014-2015 eNovance
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
from __future__ import absolute_import

import calendar
import datetime
import decimal

import iso8601
import sqlalchemy
from sqlalchemy.dialects import mysql
from sqlalchemy import types

from gnocchi import utils


class PreciseTimestamp(types.TypeDecorator):
    """Represents a timestamp precise to the microsecond.

    Deprecated in favor of TimestampUTC.
    Still used in alembic migrations.
    """

    impl = sqlalchemy.DateTime

    @staticmethod
    def _decimal_to_dt(dec):
        """Return a datetime from Decimal unixtime format."""
        if dec is None:
            return None

        integer = int(dec)
        micro = (dec - decimal.Decimal(integer)) * decimal.Decimal(1000000)
        daittyme = datetime.datetime.utcfromtimestamp(integer)
        return daittyme.replace(microsecond=int(round(micro)))

    @staticmethod
    def _dt_to_decimal(utc):
        """Datetime to Decimal.

        Some databases don't store microseconds in datetime
        so we always store as Decimal unixtime.
        """
        if utc is None:
            return None

        decimal.getcontext().prec = 30
        return (decimal.Decimal(str(calendar.timegm(utc.utctimetuple()))) +
                (decimal.Decimal(str(utc.microsecond)) /
                 decimal.Decimal("1000000.0")))

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mysql':
            return dialect.type_descriptor(
                types.DECIMAL(precision=20,
                              scale=6,
                              asdecimal=True))
        return dialect.type_descriptor(self.impl)

    def compare_against_backend(self, dialect, conn_type):
        if dialect.name == 'mysql':
            return issubclass(type(conn_type), types.DECIMAL)
        return issubclass(type(conn_type), type(self.impl))

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = utils.normalize_time(value)
        if dialect.name == 'mysql':
            return self._dt_to_decimal(value)
        return value

    def process_result_value(self, value, dialect):
        if dialect.name == 'mysql':
            value = self._decimal_to_dt(value)
        if value is not None:
            return utils.normalize_time(value).replace(
                tzinfo=iso8601.iso8601.UTC)


class TimestampUTC(types.TypeDecorator):
    """Represents a timestamp precise to the microsecond."""

    impl = sqlalchemy.DateTime

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mysql':
            return dialect.type_descriptor(mysql.DATETIME(fsp=6))
        return self.impl

    def process_bind_param(self, value, dialect):
        if value is not None:
            return utils.normalize_time(value)

    def process_result_value(self, value, dialect):
        if value is not None:
            return value.replace(tzinfo=iso8601.iso8601.UTC)
