# -*- encoding: utf-8 -*-
#
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
import itertools
import operator

from oslo_db import exception
from oslo_db.sqlalchemy import models
from oslo_db.sqlalchemy import session
from oslo_utils import timeutils
import six
import sqlalchemy
from stevedore import extension

from gnocchi import exceptions
from gnocchi import indexer
from gnocchi.indexer import sqlalchemy_base as base
from gnocchi import utils

Base = base.Base
Metric = base.Metric
ArchivePolicy = base.ArchivePolicy
ArchivePolicyRule = base.ArchivePolicyRule
Resource = base.Resource
ResourceHistory = base.ResourceHistory

_marker = indexer._marker


def get_resource_mappers(ext):
    if ext.name == "generic":
        resource_ext = ext.plugin
        resource_history_ext = ResourceHistory
    else:
        resource_ext = type(str(ext.name),
                            (ext.plugin, base.ResourceExtMixin, Resource),
                            {"__tablename__": ext.name})
        resource_history_ext = type(str("%s_history" % ext.name),
                                    (ext.plugin, base.ResourceHistoryExtMixin,
                                     ResourceHistory),
                                    {"__tablename__": (
                                        "%s_history" % ext.name)})

    return {'resource': resource_ext,
            'history': resource_history_ext}


class SQLAlchemyIndexer(indexer.IndexerDriver):
    resources = extension.ExtensionManager('gnocchi.indexer.resources')

    _RESOURCE_CLASS_MAPPER = {ext.name: get_resource_mappers(ext)
                              for ext in resources.extensions}

    def __init__(self, conf):
        conf.set_override("connection", conf.indexer.url, "database")
        self.conf = conf

    def connect(self):
        self.engine_facade = session.EngineFacade.from_config(self.conf)

    def disconnect(self):
        self.engine_facade.get_engine().dispose()

    def upgrade(self):
        engine = self.engine_facade.get_engine()
        Base.metadata.create_all(engine, checkfirst=True)

    def _resource_type_to_class(self, resource_type, purpose="resource"):
        if resource_type not in self._RESOURCE_CLASS_MAPPER:
            raise indexer.UnknownResourceType(resource_type)
        return self._RESOURCE_CLASS_MAPPER[resource_type][purpose]

    def list_archive_policies(self):
        session = self.engine_facade.get_session()
        return session.query(ArchivePolicy).all()

    def get_archive_policy(self, name):
        session = self.engine_facade.get_session()
        return session.query(ArchivePolicy).get(name)

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

    def get_metrics(self, uuids):
        if not uuids:
            return []
        session = self.engine_facade.get_session()
        query = session.query(Metric).filter(Metric.id.in_(uuids)).options(
            sqlalchemy.orm.joinedload(
                Metric.archive_policy)).options(
                    sqlalchemy.orm.joinedload(Metric.resource))

        return list(query.all())

    def create_archive_policy(self, archive_policy):
        ap = ArchivePolicy(
            name=archive_policy.name,
            back_window=archive_policy.back_window,
            definition=archive_policy.definition,
            aggregation_methods=list(archive_policy.aggregation_methods),
        )
        session = self.engine_facade.get_session()
        session.add(ap)
        try:
            session.flush()
        except exception.DBDuplicateEntry:
            raise indexer.ArchivePolicyAlreadyExists(archive_policy.name)
        return ap

    def list_archive_policy_rules(self):
        session = self.engine_facade.get_session()
        return session.query(ArchivePolicyRule).all()

    def get_archive_policy_rule(self, name):
        session = self.engine_facade.get_session()
        return session.query(ArchivePolicyRule).get(name)

    def delete_archive_policy_rule(self, name):
        session = self.engine_facade.get_session()
        try:
            if session.query(ArchivePolicyRule).filter(
                    ArchivePolicyRule.name == name).delete() == 0:
                raise indexer.NoSuchArchivePolicyRule(name)
        except exception.DBError as e:
            # TODO(prad): fix foreign key violations when oslo.db supports it
            if isinstance(e.inner_exception, sqlalchemy.exc.IntegrityError):
                raise indexer.ArchivePolicyRuleInUse(name)

    def create_archive_policy_rule(self, name, metric_pattern,
                                   archive_policy_name):
        apr = ArchivePolicyRule(
            name=name,
            archive_policy_name=archive_policy_name,
            metric_pattern=metric_pattern
        )
        session = self.engine_facade.get_session()
        session.add(apr)
        try:
            session.flush()
        except exception.DBDuplicateEntry:
            raise indexer.ArchivePolicyRuleAlreadyExists(name)
        return apr

    def create_metric(self, id, created_by_user_id, created_by_project_id,
                      archive_policy_name,
                      name=None, resource_id=None,
                      details=False):
        m = Metric(id=id,
                   created_by_user_id=created_by_user_id,
                   created_by_project_id=created_by_project_id,
                   archive_policy_name=archive_policy_name,
                   name=name,
                   resource_id=resource_id)
        session = self.engine_facade.get_session()
        session.add(m)
        session.flush()
        if details:
            # Fetch archive policy
            m.archive_policy
        return m

    def list_metrics(self, user_id=None, project_id=None, details=False,
                     **kwargs):
        session = self.engine_facade.get_session()
        q = session.query(Metric)
        if user_id is not None:
            q = q.filter(Metric.created_by_user_id == user_id)
        if project_id is not None:
            q = q.filter(Metric.created_by_project_id == project_id)
        for attr in kwargs:
            q = q.filter(getattr(Metric, attr) == kwargs[attr])
        if details:
            q = q.options(sqlalchemy.orm.joinedload(Metric.archive_policy)
                          ).options(sqlalchemy.orm.joinedload(Metric.resource))

        return q.all()

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
                self._set_metrics_for_resource(session, r, metrics)

        # NOTE(jd) Force load of metrics :)
        r.metrics

        return r

    def update_resource(self, resource_type,
                        resource_id, ended_at=_marker, metrics=_marker,
                        append_metrics=False,
                        **kwargs):

        now = timeutils.utcnow()

        resource_cls = self._resource_type_to_class(resource_type)
        resource_history_cls = self._resource_type_to_class(resource_type,
                                                            "history")
        session = self.engine_facade.get_session()
        with session.begin():
            # NOTE(sileht): We use FOR UPDATE that is not galera friendly,
            # but they are no other way to cleanly patch a resource and store
            # the history that safe when two concurrent calls are done.
            q = session.query(resource_cls).filter(
                resource_cls.id == resource_id).with_for_update()

            r = q.first()
            if r is None:
                raise indexer.NoSuchResource(resource_id)

            # Build history
            rh = resource_history_cls()
            for col in sqlalchemy.inspect(resource_cls).columns:
                setattr(rh, col.name, getattr(r, col.name))
            rh.revision_end = now
            session.add(rh)

            # Update the resource
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

            r.revision_start = now

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
                self._set_metrics_for_resource(session, r, metrics)

        # NOTE(jd) Force load of metrics – do it outside the session!
        r.metrics

        return r

    @staticmethod
    def _set_metrics_for_resource(session, r, metrics):
        for name, metric_id in six.iteritems(metrics):
            try:
                update = session.query(Metric).filter(
                    Metric.id == metric_id,
                    Metric.created_by_user_id == r.created_by_user_id,
                    Metric.created_by_project_id == r.created_by_project_id,
                ).update({"resource_id": r.id, "name": name})
            except exception.DBDuplicateEntry:
                raise indexer.NamedMetricAlreadyExists(name)
            if update == 0:
                raise indexer.NoSuchMetric(metric_id)
        session.expire(r, ['metrics'])

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
                delete_metrics(self.get_metrics([m.id for m in r.metrics]))
            q.delete()

    def get_resource(self, resource_type, resource_id, with_metrics=False):
        resource_cls = self._resource_type_to_class(resource_type)
        session = self.engine_facade.get_session()
        q = session.query(
            resource_cls).filter(
                resource_cls.id == resource_id)
        if with_metrics:
            q = q.options(sqlalchemy.orm.joinedload(resource_cls.metrics))
        return q.first()

    def _get_history_result_mapper(self, resource_type):
        resource_cls = self._resource_type_to_class(resource_type)
        history_cls = self._resource_type_to_class(resource_type, 'history')

        resource_cols = {}
        history_cols = {}
        for col in sqlalchemy.inspect(history_cls).columns:
            history_cols[col.name] = col
            if col.name in ["revision", "revision_end"]:
                value = None if col.name == "revision_end" else -1
                resource_cols[col.name] = sqlalchemy.bindparam(
                    col.name, value, col.type).label(col.name)
            else:
                resource_cols[col.name] = getattr(resource_cls, col.name)
        s1 = sqlalchemy.select(history_cols.values())
        s2 = sqlalchemy.select(resource_cols.values())
        if resource_type != "generic":
            s1 = s1.where(history_cls.revision == ResourceHistory.revision)
            s2 = s2.where(resource_cls.id == Resource.id)
        union_stmt = sqlalchemy.union(s1, s2)
        stmt = union_stmt.alias("result")

        class Result(base.ResourceJsonifier, base.GnocchiBase):
            def __iter__(self):
                return models.ModelIterator(self, iter(stmt.c.keys()))

        sqlalchemy.orm.mapper(
            Result, stmt, primary_key=[stmt.c.id, stmt.c.revision],
            properties={
                'metrics': sqlalchemy.orm.relationship(
                    Metric,
                    primaryjoin=Metric.resource_id == stmt.c.id,
                    foreign_keys=Metric.resource_id)
            })

        return Result

    def list_resources(self, resource_type='generic',
                       attribute_filter=None,
                       details=False,
                       history=False):

        session = self.engine_facade.get_session()

        if history:
            target_cls = self._get_history_result_mapper(resource_type)
        else:
            target_cls = self._resource_type_to_class(resource_type)

        q = session.query(target_cls)

        if attribute_filter:
            engine = self.engine_facade.get_engine()
            try:
                f = QueryTransformer.build_filter(engine.dialect.name,
                                                  target_cls,
                                                  attribute_filter)
            except indexer.QueryAttributeError as e:
                # NOTE(jd) The QueryAttributeError does not know about
                # resource_type, so convert it
                raise indexer.ResourceAttributeError(resource_type,
                                                     e.attribute)

            q = q.filter(f)

        # Always include metrics
        q = q.options(sqlalchemy.orm.joinedload("metrics"))
        q = q.order_by(target_cls.revision_start)
        all_resources = q.all()

        if details:
            grouped_by_type = itertools.groupby(
                all_resources, lambda r: (r.revision != -1, r.type))
            all_resources = []
            for (is_history, type), resources in grouped_by_type:
                if type == 'generic':
                    # No need for a second query
                    all_resources.extend(resources)
                else:
                    if is_history:
                        target_cls = self._resource_type_to_class(type,
                                                                  "history")
                        f = target_cls.revision.in_(
                            [r.revision for r in resources])
                    else:
                        target_cls = self._resource_type_to_class(type)
                        f = target_cls.id.in_([r.id for r in resources])

                    q = session.query(target_cls).filter(f)
                    # Always include metrics
                    q = q.options(
                        sqlalchemy.orm.joinedload(target_cls.metrics))
                    all_resources.extend(q.all())
        return all_resources

    def delete_metric(self, id):
        session = self.engine_facade.get_session()
        session.query(Metric).filter(Metric.id == id).delete()
        session.flush()


class QueryTransformer(object):
    unary_operators = {
        u"not": sqlalchemy.not_,
    }

    binary_operators = {
        u"=": operator.eq,
        u"==": operator.eq,
        u"eq": operator.eq,

        u"<": operator.lt,
        u"lt": operator.lt,

        u">": operator.gt,
        u"gt": operator.gt,

        u"<=": operator.le,
        u"≤": operator.le,
        u"le": operator.le,

        u">=": operator.ge,
        u"≥": operator.ge,
        u"ge": operator.ge,

        u"!=": operator.ne,
        u"≠": operator.ne,
        u"ne": operator.ne,

        u"in": lambda field_name, values: field_name.in_(values),

        u"like": lambda field, value: field.like(value),
    }

    multiple_operators = {
        u"or": sqlalchemy.or_,
        u"∨": sqlalchemy.or_,

        u"and": sqlalchemy.and_,
        u"∧": sqlalchemy.and_,
    }

    @classmethod
    def _handle_multiple_op(cls, engine, table, op, nodes):
        return op(*[
            cls.build_filter(engine, table, node)
            for node in nodes
        ])

    @classmethod
    def _handle_unary_op(cls, engine, table, op, node):
        return op(cls.build_filter(engine, table, node))

    @staticmethod
    def _handle_binary_op(engine, table, op, nodes):
        try:
            field_name, value = list(nodes.items())[0]
        except Exception:
            raise indexer.QueryError()

        if field_name == "lifespan":
            attr = getattr(table, "ended_at") - getattr(table, "started_at")
            value = utils.to_timespan(value)
            if engine == "mysql":
                # NOTE(jd) So subtracting 2 timestamps in MySQL result in some
                # weird results based on string comparison. It's useless and it
                # does not work at all with seconds or anything. Just skip it.
                raise exceptions.NotImplementedError
        else:
            try:
                attr = getattr(table, field_name)
            except AttributeError:
                raise indexer.QueryAttributeError(table, field_name)

            if not hasattr(attr, "type"):
                # This is not a column
                raise indexer.QueryAttributeError(table, field_name)

            # Convert value to the right type
            if value is not None and isinstance(attr.type,
                                                base.PreciseTimestamp):
                value = utils.to_timestamp(value)

        return op(attr, value)

    @classmethod
    def build_filter(cls, engine, table, tree):
        try:
            operator, nodes = list(tree.items())[0]
        except Exception:
            raise indexer.QueryError()

        try:
            op = cls.multiple_operators[operator]
        except KeyError:
            try:
                op = cls.binary_operators[operator]
            except KeyError:
                try:
                    op = cls.unary_operators[operator]
                except KeyError:
                    raise indexer.QueryInvalidOperator(operator)
                return cls._handle_unary_op(engine, op, nodes)
            return cls._handle_binary_op(engine, table, op, nodes)
        return cls._handle_multiple_op(engine, table, op, nodes)
