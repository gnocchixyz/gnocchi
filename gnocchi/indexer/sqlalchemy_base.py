# -*- encoding: utf-8 -*-
#
# Copyright © 2014-2015 eNovance
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
from __future__ import absolute_import
import calendar
import datetime
import decimal

from oslo.db.sqlalchemy import models
from oslo.utils import units
import sqlalchemy
from sqlalchemy.ext import declarative
from sqlalchemy import types
import sqlalchemy_utils

Base = declarative.declarative_base()

COMMON_TABLES_ARGS = {'mysql_charset': "utf8",
                      'mysql_engine': "InnoDB"}


class PreciseTimestamp(types.TypeDecorator):
    """Represents a timestamp precise to the microsecond."""

    impl = sqlalchemy.DateTime

    @staticmethod
    def _decimal_to_dt(dec):
        """Return a datetime from Decimal unixtime format."""
        if dec is None:
            return None

        integer = int(dec)
        micro = (dec - decimal.Decimal(integer)) * decimal.Decimal(units.M)
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
        return self.impl

    def process_bind_param(self, value, dialect):
        if dialect.name == 'mysql':
            return self._dt_to_decimal(value)
        return value

    def process_result_value(self, value, dialect):
        if dialect.name == 'mysql':
            return self._decimal_to_dt(value)
        return value


class GnocchiBase(models.ModelBase):
    pass


class ArchivePolicy(Base, GnocchiBase):
    __tablename__ = 'archive_policy'
    __table_args__ = (
        sqlalchemy.Index('ix_archive_policy_name', 'name'),
        COMMON_TABLES_ARGS,
    )

    name = sqlalchemy.Column(sqlalchemy.String(255), primary_key=True)
    back_window = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    definition = sqlalchemy.Column(sqlalchemy_utils.JSONType, nullable=False)
    # TODO(jd) Use an array of string instead, PostgreSQL can do that
    aggregation_methods = sqlalchemy.Column(sqlalchemy_utils.JSONType,
                                            nullable=False)


class Metric(Base, GnocchiBase):
    __tablename__ = 'metric'
    __table_args__ = (
        sqlalchemy.Index('ix_metric_id', 'id'),
        sqlalchemy.UniqueConstraint("resource_id", "name",
                                    name="uniq_metric0resource_id0name"),
        COMMON_TABLES_ARGS,
    )

    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                           primary_key=True)
    archive_policy_name = sqlalchemy.Column(
        sqlalchemy.String(255),
        sqlalchemy.ForeignKey('archive_policy.name',
                              ondelete="RESTRICT"),
        nullable=False)
    archive_policy = sqlalchemy.orm.relationship(ArchivePolicy)
    created_by_user_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(binary=False))
    created_by_project_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(binary=False))
    resource_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                                    sqlalchemy.ForeignKey('resource.id',
                                                          ondelete="CASCADE"))
    name = sqlalchemy.Column(sqlalchemy.String(255))


class Resource(Base, GnocchiBase):
    __tablename__ = 'resource'
    __table_args__ = (
        sqlalchemy.Index('ix_resource_id', 'id'),
        COMMON_TABLES_ARGS,
    )

    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                           primary_key=True)
    type = sqlalchemy.Column(sqlalchemy.Enum('metric', 'generic', 'instance',
                                             'swift_account', 'volume',
                                             'ceph_account', 'network',
                                             'identity', 'ipmi', 'stack',
                                             'image',
                                             name="resource_type_enum"),
                             nullable=False, default='generic')
    created_by_user_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(binary=False))
    created_by_project_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(binary=False))
    metrics = sqlalchemy.orm.relationship(Metric)
    started_at = sqlalchemy.Column(PreciseTimestamp, nullable=False,
                                   # NOTE(jd): We would like to use
                                   # sqlalchemy.func.now, but we can't
                                   # because the type of PreciseTimestamp in
                                   # MySQL is not a Timestamp, so it would
                                   # not store a timestamp but a date as an
                                   # integer.
                                   default=datetime.datetime.utcnow)
    ended_at = sqlalchemy.Column(PreciseTimestamp)
    user_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False))
    project_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False))
