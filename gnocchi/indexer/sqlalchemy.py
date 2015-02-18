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
import itertools
import operator

from oslo.db import exception
from oslo.db import options
from oslo.db.sqlalchemy import models
from oslo.db.sqlalchemy import session
from oslo.utils import timeutils
from oslo.utils import units
import six
import sqlalchemy
from sqlalchemy.ext import declarative
from sqlalchemy import types
import sqlalchemy_utils

from gnocchi import indexer


Base = declarative.declarative_base()

COMMON_TABLES_ARGS = {'mysql_charset': "utf8",
                      'mysql_engine': "InnoDB"}

_marker = indexer._marker


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
        sqlalchemy_utils.UUIDType(binary=False),
        nullable=False)
    created_by_project_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(binary=False),
        nullable=False)
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
                                             name="resource_type_enum"),
                             nullable=False, default='generic')
    created_by_user_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(binary=False),
        nullable=False)
    created_by_project_id = sqlalchemy.Column(
        sqlalchemy_utils.UUIDType(binary=False),
        nullable=False)
    metrics = sqlalchemy.orm.relationship(Metric)
    started_at = sqlalchemy.Column(PreciseTimestamp, nullable=False,
                                   # NOTE(jd): We would like to use
                                   # sqlalchemy.func.now, but we can't
                                   # because the type of PreciseTimestamp in
                                   # MySQL is not a Timestamp, so it would
                                   # not store a timestamp but a date as an
                                   # integer…
                                   default=datetime.datetime.utcnow)
    ended_at = sqlalchemy.Column(PreciseTimestamp)
    user_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False))
    project_id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False))


class Instance(Resource):
    __tablename__ = 'instance'
    __table_args__ = (
        sqlalchemy.Index('ix_instance_id', 'id'),
        COMMON_TABLES_ARGS,
    )

    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                           sqlalchemy.ForeignKey('resource.id',
                                                 ondelete="CASCADE"),
                           primary_key=True)

    flavor_id = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    image_ref = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    host = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    display_name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)
    server_group = sqlalchemy.Column(sqlalchemy.String(255))


class SwiftAccount(Resource):
    __tablename__ = 'swift_account'
    __table_args__ = (
        sqlalchemy.Index('ix_swift_account_id', 'id'),
        COMMON_TABLES_ARGS,
    )

    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                           sqlalchemy.ForeignKey('resource.id',
                                                 ondelete="CASCADE"),
                           primary_key=True)


class Volume(Resource):
    __tablename__ = 'volume'
    __table_args__ = (
        sqlalchemy.Index('ix_volume_id', 'id'),
        COMMON_TABLES_ARGS,
    )

    id = sqlalchemy.Column(sqlalchemy_utils.UUIDType(binary=False),
                           sqlalchemy.ForeignKey('resource.id',
                                                 ondelete="CASCADE"),
                           primary_key=True)

    display_name = sqlalchemy.Column(sqlalchemy.String(255), nullable=False)


class SQLAlchemyIndexer(indexer.IndexerDriver):
    # TODO(jd) Use stevedore instead to allow extending?
    _RESOURCE_CLASS_MAPPER = {
        'generic': Resource,
        'instance': Instance,
        'swift_account': SwiftAccount,
        'volume': Volume,
    }

    def __init__(self, conf):
        options.set_defaults(conf)
        self.conf = conf

    def connect(self):
        self.engine_facade = session.EngineFacade.from_config(self.conf)

    def disconnect(self):
        self.engine_facade.get_engine().dispose()

    def upgrade(self):
        engine = self.engine_facade.get_engine()
        Base.metadata.create_all(engine, checkfirst=True)

    def _resource_type_to_class(self, resource_type):
        if resource_type not in self._RESOURCE_CLASS_MAPPER:
            raise indexer.UnknownResourceType(resource_type)
        return self._RESOURCE_CLASS_MAPPER[resource_type]

    def list_archive_policies(self):
        session = self.engine_facade.get_session()
        return [dict(ap) for ap in session.query(ArchivePolicy).all()]

    def get_archive_policy(self, name):
        session = self.engine_facade.get_session()
        ap = session.query(ArchivePolicy).get(name)
        if ap:
            return dict(ap)

    def delete_archive_policy(self, name):
        session = self.engine_facade.get_session()
        try:
            if session.query(ArchivePolicy).filter(
                    ArchivePolicy.name == name).delete() == 0:
                raise indexer.NoSuchArchivePolicy(name)
        except exception.DBError as e:
            # TODO(jd) Add an exception in oslo.db to match foreign key
            # violations
            if isinstance(e.inner_exception, sqlalchemy.exc.IntegrityError):
                raise indexer.ArchivePolicyInUse(name)

    def get_metrics(self, uuids, details=False):
        session = self.engine_facade.get_session()
        query = session.query(Metric).filter(Metric.id.in_(uuids))
        if details:
            query = query.options(sqlalchemy.orm.joinedload(
                Metric.archive_policy))
            metrics = []
            for m in query:
                metric = self._resource_to_dict(m)
                metric['archive_policy'] = self._resource_to_dict(
                    m.archive_policy)
                del metric['archive_policy_name']
                metrics.append(metric)
            return metrics

        return list(map(self._resource_to_dict, query.all()))

    def create_archive_policy(self, archive_policy):
        ap = ArchivePolicy(
            name=archive_policy.name,
            back_window=archive_policy.back_window,
            definition=[d.to_dict()
                        for d in archive_policy.definition],
            aggregation_methods=list(archive_policy.aggregation_methods),
        )
        session = self.engine_facade.get_session()
        session.add(ap)
        try:
            session.flush()
        except exception.DBDuplicateEntry:
            raise indexer.ArchivePolicyAlreadyExists(archive_policy.name)
        return dict(ap)

    def create_metric(self, id, created_by_user_id, created_by_project_id,
                      archive_policy_name,
                      name=None, resource_id=None):
        m = Metric(id=id,
                   created_by_user_id=created_by_user_id,
                   created_by_project_id=created_by_project_id,
                   archive_policy_name=archive_policy_name,
                   name=name,
                   resource_id=resource_id)
        session = self.engine_facade.get_session()
        session.add(m)
        session.flush()
        return self._resource_to_dict(m)

    def list_metrics(self, user_id=None, project_id=None):
        session = self.engine_facade.get_session()
        q = session.query(Metric)
        if user_id is not None:
            q = q.filter(Metric.created_by_user_id == user_id)
        if project_id is not None:
            q = q.filter(Metric.created_by_project_id == project_id)
        return [self._resource_to_dict(m) for m in q.all()]

    def create_resource(self, resource_type, id,
                        created_by_user_id, created_by_project_id,
                        user_id=None, project_id=None,
                        started_at=None, ended_at=None, metrics=None,
                        **kwargs):
        resource_cls = self._resource_type_to_class(resource_type)
        if (started_at is not None
           and ended_at is not None
           and started_at > ended_at):
            raise ValueError("Start timestamp cannot be after end timestamp")
        r = resource_cls(
            id=id,
            type=resource_type,
            created_by_user_id=created_by_user_id,
            created_by_project_id=created_by_project_id,
            user_id=user_id,
            project_id=project_id,
            started_at=started_at,
            ended_at=ended_at,
            **kwargs)
        session = self.engine_facade.get_session()
        with session.begin():
            session.add(r)
            try:
                session.flush()
            except exception.DBDuplicateEntry:
                raise indexer.ResourceAlreadyExists(id)
            except exception.DBReferenceError as ex:
                raise indexer.ResourceValueError(r.type,
                                                 ex.key,
                                                 getattr(r, ex.key))
            if metrics is not None:
                self._set_metrics_for_resource(session, id,
                                               created_by_user_id,
                                               created_by_project_id,
                                               metrics)

        return self._resource_to_dict(r, with_metrics=True)

    @staticmethod
    def _resource_to_dict(resource, with_metrics=False):
        r = dict(resource)
        if with_metrics and isinstance(resource, Resource):
            r['metrics'] = dict((m['name'], six.text_type(m['id']))
                                for m in resource.metrics)
        return r

    def update_resource(self, resource_type,
                        resource_id, ended_at=_marker, metrics=_marker,
                        append_metrics=False,
                        **kwargs):
        resource_cls = self._resource_type_to_class(resource_type)
        session = self.engine_facade.get_session()
        with session.begin():
            q = session.query(
                resource_cls).filter(
                    resource_cls.id == resource_id)
            r = q.first()
            if r is None:
                raise indexer.NoSuchResource(resource_id)

            if ended_at is not _marker:
                # NOTE(jd) Could be better to have check in the db for that so
                # we can just run the UPDATE
                if r.started_at is not None and ended_at is not None:
                    # Convert to UTC because we store in UTC :(
                    ended_at = timeutils.normalize_time(ended_at)
                    if r.started_at > ended_at:
                        raise ValueError(
                            "Start timestamp cannot be after end timestamp")
                r.ended_at = ended_at

            if kwargs:
                for attribute, value in six.iteritems(kwargs):
                    if hasattr(r, attribute):
                        setattr(r, attribute, value)
                    else:
                        raise indexer.ResourceAttributeError(
                            r.type, attribute)

            if metrics is not _marker:
                if not append_metrics:
                    session.query(Metric).filter(
                        Metric.resource_id == resource_id).update(
                            {"resource_id": None})
                self._set_metrics_for_resource(session, resource_id,
                                               r.created_by_user_id,
                                               r.created_by_project_id,
                                               metrics)

        return self._resource_to_dict(r, with_metrics=True)

    @staticmethod
    def _set_metrics_for_resource(session, resource_id,
                                  user_id, project_id, metrics):
        for name, metric_id in six.iteritems(metrics):
            try:
                update = session.query(Metric).filter(
                    Metric.id == metric_id,
                    Metric.created_by_user_id == user_id,
                    Metric.created_by_project_id == project_id).update(
                        {"resource_id": resource_id, "name": name})
            except exception.DBDuplicateEntry:
                raise indexer.NamedMetricAlreadyExists(name)
            if update == 0:
                raise indexer.NoSuchMetric(metric_id)

    def delete_resource(self, resource_id, delete_metrics=None):
        session = self.engine_facade.get_session()
        with session.begin():
            q = session.query(Resource).filter(
                Resource.id == resource_id).options(
                    sqlalchemy.orm.joinedload(Resource.metrics))
            r = q.first()
            if r is None:
                raise indexer.NoSuchResource(resource_id)
            if delete_metrics is not None:
                delete_metrics(self.get_metrics([m.id for m in r.metrics],
                                                details=True))
            q.delete()

    def get_resource(self, resource_type, resource_id, with_metrics=False):
        resource_cls = self._resource_type_to_class(resource_type)
        session = self.engine_facade.get_session()
        q = session.query(
            resource_cls).filter(
                resource_cls.id == resource_id)
        if with_metrics:
            q = q.options(sqlalchemy.orm.joinedload(resource_cls.metrics))
        r = q.first()
        if r:
            return self._resource_to_dict(r, with_metrics)

    def list_resources(self, resource_type='generic',
                       started_after=None,
                       ended_before=None,
                       attributes_filter=None,
                       details=False):
        resource_cls = self._resource_type_to_class(resource_type)
        session = self.engine_facade.get_session()
        q = session.query(
            resource_cls)
        # Always include metrics
        q = q.options(sqlalchemy.orm.joinedload(resource_cls.metrics))
        if started_after is not None:
            q = q.filter(resource_cls.started_at >= started_after)
        if ended_before is not None:
            q = q.filter(resource_cls.ended_at < ended_before)
        if attributes_filter is not None:
            for attribute, value in six.iteritems(attributes_filter):
                try:
                    q = q.filter(getattr(resource_cls, attribute) == value)
                except AttributeError:
                    raise indexer.ResourceAttributeError(
                        resource_type, attribute)
        if details:
            grouped_by_type = itertools.groupby(q.all(),
                                                operator.attrgetter('type'))
            all_resources = []
            for type, resources in grouped_by_type:
                if type == 'generic':
                    # No need for a second query
                    all_resources.extend(resources)
                else:
                    resources_ids = [r.id for r in resources]
                    all_resources.extend(
                        session.query(
                            self._RESOURCE_CLASS_MAPPER[type]).filter(
                                self._RESOURCE_CLASS_MAPPER[type].id.in_(
                                    resources_ids)).all())
        else:
            all_resources = q.all()

        return [self._resource_to_dict(r, with_metrics=True)
                for r in all_resources]

    def delete_metric(self, id):
        session = self.engine_facade.get_session()
        session.query(Metric).filter(Metric.id == id).delete()
        session.flush()
