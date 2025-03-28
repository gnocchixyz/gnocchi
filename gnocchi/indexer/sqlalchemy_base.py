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

import datetime
from oslo_db.sqlalchemy import models
import sqlalchemy
from sqlalchemy.ext import declarative
from sqlalchemy.orm import declarative_base

import sqlalchemy_utils

from gnocchi import archive_policy
from gnocchi import indexer
from gnocchi.indexer import sqlalchemy_types as types
from gnocchi import resource_type
from gnocchi import utils

Base = declarative_base()

COMMON_TABLES_ARGS = {'mysql_charset': "utf8",
                      'mysql_engine': "InnoDB"}


class GnocchiBase(models.ModelBase):
    __table_args__ = (
        COMMON_TABLES_ARGS,
    )


class ArchivePolicyDefinitionType(sqlalchemy_utils.JSONType):
    def process_bind_param(self, value, dialect):
        if value is not None:
            return super(
                ArchivePolicyDefinitionType, self).process_bind_param(
                    [v.serialize() for v in value],
                    dialect)

    def process_result_value(self, value, dialect):
        values = super(ArchivePolicyDefinitionType,
                       self).process_result_value(value, dialect)
        if values is None:
            return []
        return [archive_policy.ArchivePolicyItem(**v) for v in values]


class SetType(sqlalchemy_utils.JSONType):
    def process_result_value(self, value, dialect):
        values = super(SetType, self).process_result_value(value, dialect)
        if values is None:
            return set()
        return set(values)


class ArchivePolicy(Base, GnocchiBase, archive_policy.ArchivePolicy):
    __tablename__ = 'archive_policy'

    name = sqlalchemy.Column(sqlalchemy.String(255), primary_key=True)
    back_window = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    definition = sqlalchemy.Column(ArchivePolicyDefinitionType, nullable=False)
    # TODO(jd) Use an array of string instead, PostgreSQL can do that
    aggregation_methods = sqlalchemy.Column(SetType,
                                            nullable=False)


class Metric(Base, GnocchiBase, indexer.Metric):
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
    resource = sqlalchemy.orm.relationship("Resource",
                                           back_populates="metrics")
    name = sqlalchemy.Column(sqlalchemy.String(255))
    unit = sqlalchemy.Column(sqlalchemy.String(31))
    status = sqlalchemy.Column(sqlalchemy.Enum('active', 'delete',
                                               name="metric_status_enum"),
                               nullable=False,
                               server_default='active')

    needs_raw_data_truncation = sqlalchemy.Column(
        "needs_raw_data_truncation", sqlalchemy.Boolean,
        nullable=False, default=True,
        server_default=sqlalchemy.sql.true())

    # Timestamp that represents when the last measure push was received for the
    # given metric. This allows us to identify when a metric ceased receiving
    # measurements; thus, if all metric for a resource are in this situation,
    # chances are that the resource ceased existing in the backend.
    last_measure_timestamp = sqlalchemy.Column(
        "last_measure_timestamp", sqlalchemy.DateTime, default=datetime.datetime.utcnow(), nullable=False,
        server_default=sqlalchemy.sql.func.current_timestamp())

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
        # indexer.Metric.
        return ((isinstance(other, Metric)
                 and self.id == other.id
                 and self.archive_policy_name == other.archive_policy_name
                 and self.creator == other.creator
                 and self.name == other.name
                 and self.unit == other.unit
                 and self.resource_id == other.resource_id)
                or (indexer.Metric.__eq__(self, other)))

    __hash__ = indexer.Metric.__hash__


RESOURCE_TYPE_SCHEMA_MANAGER = resource_type.ResourceTypeSchemaManager(
    "gnocchi.indexer.sqlalchemy.resource_type_attribute")


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
    updated_at = sqlalchemy.Column(types.TimestampUTC, nullable=False,
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
    def __str__(self):
        return str(self.jsonify())

    def jsonify(self, attrs=None):
        d = dict(self)
        del d['revision']
        if 'metrics' not in sqlalchemy.inspect(self).unloaded:
            d['metrics'] = dict((m.name, str(m.id))
                                for m in self.metrics)

        if self.creator is None:
            d['created_by_user_id'] = d['created_by_project_id'] = None
        else:
            d['created_by_user_id'], _, d['created_by_project_id'] = (
                self.creator.partition(":")
            )

        if attrs:
            return {key: val for key, val in d.items() if key in attrs}
        else:
            return d


class ResourceMixin(ResourceJsonifier):
    @declarative.declared_attr
    def __table_args__(cls):
        return (sqlalchemy.CheckConstraint(
            'started_at <= ended_at',
            name="ck_{}_started_before_ended".format(
                cls.__tablename__
            )
        ),
            COMMON_TABLES_ARGS
        )

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
    started_at = sqlalchemy.Column(types.TimestampUTC, nullable=False,
                                   default=lambda: utils.utcnow())
    revision_start = sqlalchemy.Column(types.TimestampUTC,
                                       nullable=False,
                                       default=lambda: utils.utcnow())
    ended_at = sqlalchemy.Column(types.TimestampUTC)
    user_id = sqlalchemy.Column(sqlalchemy.String(255))
    project_id = sqlalchemy.Column(sqlalchemy.String(255))
    original_resource_id = sqlalchemy.Column(sqlalchemy.String(289),
                                             nullable=False)


class Resource(ResourceMixin, Base, GnocchiBase):
    __tablename__ = 'resource'
    _extra_keys = ['revision', 'revision_end']
    revision = -1
    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(),
                           primary_key=True)
    revision_end = None
    metrics = sqlalchemy.orm.relationship(
        Metric, back_populates="resource",
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
    revision_end = sqlalchemy.Column(types.TimestampUTC,
                                     nullable=False,
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
        n = next(self.i)
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
