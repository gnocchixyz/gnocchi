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
import uuid

from oslo.db import exception
from oslo.db.sqlalchemy import session
from oslo.utils import timeutils
import six
import sqlalchemy
from stevedore import extension

from gnocchi import indexer
from gnocchi.indexer import sqlalchemy_base as base
from gnocchi import utils

Base = base.Base
Metric = base.Metric
ArchivePolicy = base.ArchivePolicy
Resource = base.Resource

_marker = indexer._marker


class SQLAlchemyIndexer(indexer.IndexerDriver):
    resources = extension.ExtensionManager('gnocchi.indexer.resources')

    _RESOURCE_CLASS_MAPPER = {ext.name: ext.plugin
                              for ext in resources.extensions}

    def __init__(self, conf):
        conf.set_override("connection", conf.indexer.url, "database")
        self.conf = conf
        self.qt = QueryTransformer()

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

    @staticmethod
    def _fixup_created_by_uuid(obj):
        # FIXME(sileht): so weird, sqlachemy_utils.UUIDTYPE try to convert any
        # input to a UUID to write it in db but don't update the orm object
        # if the object doesn't come from the database
        if (obj.created_by_user_id
           and not isinstance(obj.created_by_user_id, uuid.UUID)):
            obj.created_by_user_id = uuid.UUID(obj.created_by_user_id)
        if (obj.created_by_project_id
           and not isinstance(obj.created_by_project_id, uuid.UUID)):
            obj.created_by_project_id = uuid.UUID(obj.created_by_project_id)

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

        self._fixup_created_by_uuid(r)
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

        self._fixup_created_by_uuid(r)
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
                       attribute_filter=None,
                       details=False):

        resource_cls = self._resource_type_to_class(resource_type)
        session = self.engine_facade.get_session()

        q = session.query(resource_cls)

        if attribute_filter:
            try:
                f = self.qt.build_filter(resource_cls, attribute_filter)
            except QueryAttributeError as e:
                raise indexer.ResourceAttributeError(
                    resource_type, e.attribute)
            q = q.filter(f)

        # Always include metrics
        q = q.options(sqlalchemy.orm.joinedload(resource_cls.metrics))

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


class QueryAttributeError(AttributeError):
    def __init__(self, table, attribute):
        self.table = table
        self.attribute = attribute


class QueryTransformer(object):
    operators = {
        "=": operator.eq,
        "<": operator.lt,
        ">": operator.gt,
        "<=": operator.le,
        ">=": operator.ge,
        "!=": operator.ne,
        "in": lambda field_name, values: field_name.in_(values),
        "like": lambda field, value: field.like(value),
    }

    complex_operators = {"or": sqlalchemy.or_,
                         "and": sqlalchemy.and_,
                         "not": sqlalchemy.not_}

    def _handle_complex_op(self, table, complex_op, nodes):
        op = self.complex_operators[complex_op]
        if op == sqlalchemy.not_:
            nodes = [nodes]
        element_list = []
        for node in nodes:
            element = self.build_filter(table, node)
            element_list.append(element)
        return op(*element_list)

    def _handle_simple_op(self, table, simple_op, nodes):
        op = self.operators[simple_op]
        field_name = list(nodes.keys())[0]
        value = list(nodes.values())[0]
        try:
            attr = getattr(table, field_name)
        except AttributeError:
            raise QueryAttributeError(table, field_name)

        # Convert value to the right type
        if isinstance(attr.type, base.PreciseTimestamp):
            value = utils.to_timestamp(value)

        return op(attr, value)

    def build_filter(self, table, sub_tree):
        if sub_tree == {}:
            return True
        operator = list(sub_tree.keys())[0]
        nodes = list(sub_tree.values())[0]
        if operator in self.complex_operators:
            return self._handle_complex_op(table, operator, nodes)
        return self._handle_simple_op(table, operator, nodes)
