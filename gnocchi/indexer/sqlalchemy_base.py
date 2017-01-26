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
from oslo_db.sqlalchemy import models
from oslo_utils import timeutils
from oslo_utils import units
import six
import sqlalchemy
from sqlalchemy.dialects import mysql
from sqlalchemy.ext import declarative
from sqlalchemy import types
import sqlalchemy_utils

from gnocchi import archive_policy
from gnocchi import indexer
from gnocchi.indexer import sqlalchemy_legacy_resources as legacy
from gnocchi import resource_type
from gnocchi import storage
from gnocchi import utils

Base = declarative.declarative_base()

COMMON_TABLES_ARGS = {'mysql_charset': "utf8",
                      'mysql_engine': "InnoDB"}


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
        return dialect.type_descriptor(self.impl)

    def compare_against_backend(self, dialect, conn_type):
        if dialect.name == 'mysql':
            return issubclass(type(conn_type), types.DECIMAL)
        return issubclass(type(conn_type), type(self.impl))

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = timeutils.normalize_time(value)
        if dialect.name == 'mysql':
            return self._dt_to_decimal(value)
        return value

    def process_result_value(self, value, dialect):
        if dialect.name == 'mysql':
            value = self._decimal_to_dt(value)
        if value is not None:
            return timeutils.normalize_time(value).replace(
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
            return timeutils.normalize_time(value)

    def process_result_value(self, value, dialect):
        if value is not None:
            return value.replace(tzinfo=iso8601.iso8601.UTC)


class GnocchiBase(models.ModelBase):
    __table_args__ = (
        COMMON_TABLES_ARGS,
    )


class ArchivePolicyDefinitionType(sqlalchemy_utils.JSONType):
    def process_result_value(self, value, dialect):
        values = super(ArchivePolicyDefinitionType,
                       self).process_result_value(value, dialect)
        return [archive_policy.ArchivePolicyItem(**v) for v in values]


class SetType(sqlalchemy_utils.JSONType):
    def process_result_value(self, value, dialect):
        return set(super(SetType,
                         self).process_result_value(value, dialect))


class ArchivePolicy(Base, GnocchiBase, archive_policy.ArchivePolicy):
    __tablename__ = 'archive_policy'

    name = sqlalchemy.Column(sqlalchemy.String(255), primary_key=True)
    back_window = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    definition = sqlalchemy.Column(ArchivePolicyDefinitionType, nullable=False)
    # TODO(jd) Use an array of string instead, PostgreSQL can do that
    aggregation_methods = sqlalchemy.Column(SetType,
                                            nullable=False)


class Metric(Base, GnocchiBase, storage.Metric):
    __tablename__ = 'metric'
    __table_args__ = (
        sqlalchemy.Index('ix_metric_status', 'status'),
        sqlalchemy.UniqueConstraint("resource_id", "name",
                                    name="uniq_metric0resource_id0name"),
        COMMON_TABLES_ARGS,
    )

    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(),
                           primary_key=True)
    archive_policy_name = sqlalchemy.Column(
        sqlalchemy.String(255),
        sqlalchemy.ForeignKey(
            'archive_policy.name',
            ondelete="RESTRICT",
            name="fk_metric_ap_name_ap_name"),
        nullable=False)
    archive_policy = sqlalchemy.orm.relationship(ArchivePolicy, lazy="joined")
    creator = sqlalchemy.Column(sqlalchemy.String(255))
    resource_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(),
        sqlalchemy.ForeignKey('resource.id',
                              ondelete="SET NULL",
                              name="fk_metric_resource_id_resource_id"))
    name = sqlalchemy.Column(sqlalchemy.String(255))
    unit = sqlalchemy.Column(sqlalchemy.String(31))
    status = sqlalchemy.Column(sqlalchemy.Enum('active', 'delete',
                                               name="metric_status_enum"),
                               nullable=False,
                               server_default='active')

    def jsonify(self):
        d = {
            "id": self.id,
            "creator": self.creator,
            "name": self.name,
            "unit": self.unit,
        }
        unloaded = sqlalchemy.inspect(self).unloaded
        if 'resource' in unloaded:
            d['resource_id'] = self.resource_id
        else:
            d['resource'] = self.resource
        if 'archive_policy' in unloaded:
            d['archive_policy_name'] = self.archive_policy_name
        else:
            d['archive_policy'] = self.archive_policy

        if self.creator is None:
            d['created_by_user_id'] = d['created_by_project_id'] = None
        else:
            d['created_by_user_id'], _, d['created_by_project_id'] = (
                self.creator.partition(":")
            )

        return d

    def __eq__(self, other):
        # NOTE(jd) If `other` is a SQL Metric, we only compare
        # archive_policy_name, and we don't compare archive_policy that might
        # not be loaded. Otherwise we fallback to the original comparison for
        # storage.Metric.
        return ((isinstance(other, Metric)
                 and self.id == other.id
                 and self.archive_policy_name == other.archive_policy_name
                 and self.creator == other.creator
                 and self.name == other.name
                 and self.unit == other.unit
                 and self.resource_id == other.resource_id)
                or (storage.Metric.__eq__(self, other)))

    __hash__ = storage.Metric.__hash__


RESOURCE_TYPE_SCHEMA_MANAGER = resource_type.ResourceTypeSchemaManager(
    "gnocchi.indexer.sqlalchemy.resource_type_attribute")


def get_legacy_resource_types():
    resource_types = []
    for name, attributes in legacy.ceilometer_resources.items():
        tablename = legacy.ceilometer_tablenames.get(name, name)
        attrs = RESOURCE_TYPE_SCHEMA_MANAGER.attributes_from_dict(
            attributes)
        resource_types.append(ResourceType(name=name,
                                           tablename=tablename,
                                           state="creating",
                                           attributes=attrs))
    return resource_types


class ResourceTypeAttributes(sqlalchemy_utils.JSONType):
    def process_bind_param(self, attributes, dialect):
        return super(ResourceTypeAttributes, self).process_bind_param(
            attributes.jsonify(), dialect)

    def process_result_value(self, value, dialect):
        attributes = super(ResourceTypeAttributes, self).process_result_value(
            value, dialect)
        return RESOURCE_TYPE_SCHEMA_MANAGER.attributes_from_dict(attributes)


class ResourceType(Base, GnocchiBase, resource_type.ResourceType):
    __tablename__ = 'resource_type'
    __table_args__ = (
        sqlalchemy.UniqueConstraint("tablename",
                                    name="uniq_resource_type0tablename"),
        COMMON_TABLES_ARGS,
    )

    name = sqlalchemy.Column(sqlalchemy.String(255), primary_key=True,
                             nullable=False)
    tablename = sqlalchemy.Column(sqlalchemy.String(35), nullable=False)
    attributes = sqlalchemy.Column(ResourceTypeAttributes)
    state = sqlalchemy.Column(sqlalchemy.Enum("active", "creating",
                                              "creation_error", "deleting",
                                              "deletion_error", "updating",
                                              "updating_error",
                                              name="resource_type_state_enum"),
                              nullable=False,
                              server_default="creating")
    updated_at = sqlalchemy.Column(TimestampUTC, nullable=False,
                                   # NOTE(jd): We would like to use
                                   # sqlalchemy.func.now, but we can't
                                   # because the type of PreciseTimestamp in
                                   # MySQL is not a Timestamp, so it would
                                   # not store a timestamp but a date as an
                                   # integer.
                                   default=lambda: utils.utcnow())

    def to_baseclass(self):
        cols = {}
        for attr in self.attributes:
            cols[attr.name] = sqlalchemy.Column(attr.satype,
                                                nullable=not attr.required)
        return type(str("%s_base" % self.tablename), (object, ), cols)


class ResourceJsonifier(indexer.Resource):
    def jsonify(self):
        d = dict(self)
        del d['revision']
        if 'metrics' not in sqlalchemy.inspect(self).unloaded:
            d['metrics'] = dict((m.name, six.text_type(m.id))
                                for m in self.metrics)

        if self.creator is None:
            d['created_by_user_id'] = d['created_by_project_id'] = None
        else:
            d['created_by_user_id'], _, d['created_by_project_id'] = (
                self.creator.partition(":")
            )

        return d


class ResourceMixin(ResourceJsonifier):
    @declarative.declared_attr
    def __table_args__(cls):
        return (sqlalchemy.CheckConstraint('started_at <= ended_at',
                                           name="ck_started_before_ended"),
                COMMON_TABLES_ARGS)

    @declarative.declared_attr
    def type(cls):
        return sqlalchemy.Column(
            sqlalchemy.String(255),
            sqlalchemy.ForeignKey('resource_type.name',
                                  ondelete="RESTRICT",
                                  name="fk_%s_resource_type_name" %
                                  cls.__tablename__),
            nullable=False)

    creator = sqlalchemy.Column(sqlalchemy.String(255))
    started_at = sqlalchemy.Column(TimestampUTC, nullable=False,
                                   default=lambda: utils.utcnow())
    revision_start = sqlalchemy.Column(TimestampUTC, nullable=False,
                                       default=lambda: utils.utcnow())
    ended_at = sqlalchemy.Column(TimestampUTC)
    user_id = sqlalchemy.Column(sqlalchemy.String(255))
    project_id = sqlalchemy.Column(sqlalchemy.String(255))
    original_resource_id = sqlalchemy.Column(sqlalchemy.String(255),
                                             nullable=False)


class Resource(ResourceMixin, Base, GnocchiBase):
    __tablename__ = 'resource'
    _extra_keys = ['revision', 'revision_end']
    revision = -1
    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(),
                           primary_key=True)
    revision_end = None
    metrics = sqlalchemy.orm.relationship(
        Metric, backref="resource",
        primaryjoin="and_(Resource.id == Metric.resource_id, "
        "Metric.status == 'active')")

    def get_metric(self, metric_name):
        m = super(Resource, self).get_metric(metric_name)
        if m:
            if sqlalchemy.orm.session.object_session(self):
                # NOTE(jd) The resource is already loaded so that should not
                # trigger a SELECT
                m.resource
            return m


class ResourceHistory(ResourceMixin, Base, GnocchiBase):
    __tablename__ = 'resource_history'

    revision = sqlalchemy.Column(sqlalchemy.Integer, autoincrement=True,
                                 primary_key=True)
    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(),
                           sqlalchemy.ForeignKey(
                               'resource.id',
                               ondelete="CASCADE",
                               name="fk_rh_id_resource_id"),
                           nullable=False)
    revision_end = sqlalchemy.Column(TimestampUTC, nullable=False,
                                     default=lambda: utils.utcnow())
    metrics = sqlalchemy.orm.relationship(
        Metric, primaryjoin="Metric.resource_id == ResourceHistory.id",
        foreign_keys='Metric.resource_id')


class ResourceExt(object):
    """Default extension class for plugin

    Used for plugin that doesn't need additional columns
    """


class ResourceExtMixin(object):
    @declarative.declared_attr
    def __table_args__(cls):
        return (COMMON_TABLES_ARGS, )

    @declarative.declared_attr
    def id(cls):
        tablename_compact = cls.__tablename__
        if tablename_compact.endswith("_history"):
            tablename_compact = tablename_compact[:-6]
        return sqlalchemy.Column(
            sqlalchemy_utils.UUIDType(),
            sqlalchemy.ForeignKey(
                'resource.id',
                ondelete="CASCADE",
                name="fk_%s_id_resource_id" % tablename_compact,
                # NOTE(sileht): We use to ensure that postgresql
                # does not use AccessExclusiveLock on destination table
                use_alter=True),
            primary_key=True
        )


class ResourceHistoryExtMixin(object):
    @declarative.declared_attr
    def __table_args__(cls):
        return (COMMON_TABLES_ARGS, )

    @declarative.declared_attr
    def revision(cls):
        tablename_compact = cls.__tablename__
        if tablename_compact.endswith("_history"):
            tablename_compact = tablename_compact[:-6]
        return sqlalchemy.Column(
            sqlalchemy.Integer,
            sqlalchemy.ForeignKey(
                'resource_history.revision',
                ondelete="CASCADE",
                name="fk_%s_revision_rh_revision"
                % tablename_compact,
                # NOTE(sileht): We use to ensure that postgresql
                # does not use AccessExclusiveLock on destination table
                use_alter=True),
            primary_key=True
        )


class HistoryModelIterator(models.ModelIterator):
    def __next__(self):
        # NOTE(sileht): Our custom resource attribute columns don't
        # have the same name in database than in sqlalchemy model
        # so remove the additional "f_" for the model name
        n = six.advance_iterator(self.i)
        model_attr = n[2:] if n[:2] == "f_" else n
        return model_attr, getattr(self.model, n)


class ArchivePolicyRule(Base, GnocchiBase):
    __tablename__ = 'archive_policy_rule'

    name = sqlalchemy.Column(sqlalchemy.String(255), primary_key=True)
    archive_policy_name = sqlalchemy.Column(
        sqlalchemy.String(255),
        sqlalchemy.ForeignKey(
            'archive_policy.name',
            ondelete="RESTRICT",
            name="fk_apr_ap_name_ap_name"),
        nullable=False)
    metric_pattern = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
