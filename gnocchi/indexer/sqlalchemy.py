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
import os.path
import threading
import uuid

import oslo_db.api
from oslo_db import exception
from oslo_db.sqlalchemy import enginefacade
from oslo_db.sqlalchemy import utils as oslo_db_utils
from oslo_log import log
try:
    import psycopg2
except ImportError:
    psycopg2 = None
try:
    import pymysql.constants.ER
    import pymysql.err
except ImportError:
    pymysql = None
import six
import sqlalchemy
import sqlalchemy.exc
from sqlalchemy import types
import sqlalchemy_utils

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
ResourceType = base.ResourceType

_marker = indexer._marker

LOG = log.getLogger(__name__)


def retry_on_deadlock(f):
    # FIXME(jd) The default values in oslo.db are useless, we need to fix that.
    # Once it's done, let's remove that wrapper of wrapper.
    return oslo_db.api.wrap_db_retry(retry_on_deadlock=True,
                                     retry_on_request=True,
                                     max_retries=10,
                                     retry_interval=0.1,
                                     inc_retry_interval=True,
                                     max_retry_interval=2)(f)


class PerInstanceFacade(object):
    def __init__(self, conf):
        self.trans = enginefacade.transaction_context()
        self.trans.configure(
            **dict(conf.database.items())
        )
        self._context = threading.local()

    def independent_writer(self):
        return self.trans.independent.writer.using(self._context)

    def independent_reader(self):
        return self.trans.independent.reader.using(self._context)

    def writer_connection(self):
        return self.trans.connection.writer.using(self._context)

    def reader_connection(self):
        return self.trans.connection.reader.using(self._context)

    def writer(self):
        return self.trans.writer.using(self._context)

    def reader(self):
        return self.trans.reader.using(self._context)

    def get_engine(self):
        # TODO(mbayer): add get_engine() to enginefacade
        if not self.trans._factory._started:
            self.trans._factory._start()
        return self.trans._factory._writer_engine

    def dispose(self):
        # TODO(mbayer): add dispose() to enginefacade
        if self.trans._factory._started:
            self.trans._factory._writer_engine.dispose()


class ResourceClassMapper(object):
    def __init__(self):
        self._cache = {'generic': {'resource': base.Resource,
                                   'history': base.ResourceHistory}}
        self._lock = threading.RLock()

    @staticmethod
    def _build_class_mappers(resource_type, baseclass=None):
        tablename = resource_type.tablename
        # TODO(sileht): Add columns
        if not baseclass:
            baseclass = resource_type.to_baseclass()
        resource_ext = type(
            str("%s_resource" % tablename),
            (baseclass, base.ResourceExtMixin, base.Resource),
            {"__tablename__": tablename})
        resource_history_ext = type(
            str("%s_history" % tablename),
            (baseclass, base.ResourceHistoryExtMixin, base.ResourceHistory),
            {"__tablename__": ("%s_history" % tablename)})
        return {'resource': resource_ext,
                'history': resource_history_ext}

    def get_classes(self, resource_type):
        # NOTE(sileht): Most of the times we can bypass the lock so do it
        try:
            return self._cache[resource_type.tablename]
        except KeyError:
            pass
        # TODO(sileht): if the table doesn't exis
        with self._lock:
            try:
                return self._cache[resource_type.tablename]
            except KeyError:
                mapper = self._build_class_mappers(resource_type)
                self._cache[resource_type.tablename] = mapper
                return mapper

    @retry_on_deadlock
    def map_and_create_tables(self, resource_type, facade):
        with self._lock:
            # NOTE(sileht): map this resource_type to have
            # Base.metadata filled with sa.Table objects
            mappers = self.get_classes(resource_type)
            tables = [Base.metadata.tables[klass.__tablename__]
                      for klass in mappers.values()]
            try:
                with facade.writer_connection() as connection:
                    Base.metadata.create_all(connection, tables=tables)
            except exception.DBError as e:
                # HACK(jd) Sometimes, PostgreSQL raises an error such as
                # "current transaction is aborted, commands ignored until end
                # of transaction block" on its own catalog, so we need to
                # retry, but this is not caught by oslo.db as a deadlock. This
                # is likely because when we use Base.metadata.create_all(),
                # sqlalchemy itself gets an error it does not catch or
                # something. So this is paperover I guess.
                inn_e = e.inner_exception
                if (psycopg2
                   and isinstance(inn_e, sqlalchemy.exc.InternalError)
                   and isinstance(inn_e.orig, psycopg2.InternalError)
                    # current transaction is aborted
                   and inn_e.orig.pgcode == '25P02'):
                    raise exception.RetryRequest(e)
                raise

    def unmap_and_delete_tables(self, resource_type, connection):
        with self._lock:
            # NOTE(sileht): map this resource_type to have
            # Base.metadata filled with sa.Table objects
            mappers = self.get_classes(resource_type)
            tables = [Base.metadata.tables[klass.__tablename__]
                      for klass in mappers.values()]

            if connection is not None:
                # NOTE(sileht): Base.metadata.drop_all doesn't
                # issue CASCADE stuffs correctly at least on postgresql
                # We drop foreign keys manually to not lock the destination
                # table for too long during drop table.
                # It's safe to not use a transaction since
                # the resource_type table is already cleaned and commited
                # so this code cannot be triggerred anymore for this
                # resource_type
                for table in tables:
                    for fk in table.foreign_key_constraints:
                        self._safe_execute(
                            connection,
                            sqlalchemy.schema.DropConstraint(fk))
                for table in tables:
                    self._safe_execute(connection,
                                       sqlalchemy.schema.DropTable(table))

            # TODO(sileht): Remove this resource on other workers
            # by using expiration on cache ?
            for table in tables:
                Base.metadata.remove(table)
            del self._cache[resource_type.tablename]

    @retry_on_deadlock
    def _safe_execute(self, connection, works):
        # NOTE(sileht): we create a transaction to ensure mysql
        # create locks on other transaction...
        trans = connection.begin()
        connection.execute(works)
        trans.commit()


class SQLAlchemyIndexer(indexer.IndexerDriver):
    _RESOURCE_TYPE_MANAGER = ResourceClassMapper()

    def __init__(self, conf):
        conf.set_override("connection", conf.indexer.url, "database")
        self.conf = conf
        self.facade = PerInstanceFacade(conf)

    def disconnect(self):
        self.facade.dispose()

    def _get_alembic_config(self):
        from alembic import config

        cfg = config.Config(
            "%s/alembic/alembic.ini" % os.path.dirname(__file__))
        cfg.set_main_option('sqlalchemy.url',
                            self.conf.database.connection)
        return cfg

    def get_engine(self):
        return self.facade.get_engine()

    def upgrade(self, nocreate=False, create_legacy_resource_types=False):
        from alembic import command
        from alembic import migration

        cfg = self._get_alembic_config()
        cfg.conf = self.conf
        if nocreate:
            command.upgrade(cfg, "head")
        else:
            with self.facade.writer_connection() as connection:
                ctxt = migration.MigrationContext.configure(connection)
                current_version = ctxt.get_current_revision()
                if current_version is None:
                    Base.metadata.create_all(connection)
                    command.stamp(cfg, "head")
                else:
                    command.upgrade(cfg, "head")

        # TODO(sileht): generic shouldn't be a particular case
        # we must create a rt_generic and rt_generic_history table
        # like other type
        for rt in base.get_legacy_resource_types():
            if not (rt.name == "generic" or create_legacy_resource_types):
                continue
            try:
                with self.facade.writer() as session:
                    session.add(rt)
            except exception.DBDuplicateEntry:
                pass
            self._RESOURCE_TYPE_MANAGER.map_and_create_tables(rt, self.facade)

    # NOTE(jd) We can have deadlock errors either here or later in
    # map_and_create_tables(). We can't decorate create_resource_type()
    # directly or each part might retry later on its own and cause a
    # duplicate. And it seems there's no way to use the same session for
    # both adding the resource_type in our table and calling
    # map_and_create_tables() :-(
    @retry_on_deadlock
    def _add_resource_type(self, resource_type):
        try:
            with self.facade.writer() as session:
                session.add(resource_type)
        except exception.DBDuplicateEntry:
            raise indexer.ResourceTypeAlreadyExists(resource_type.name)

    def create_resource_type(self, resource_type):
        # NOTE(sileht): mysql have a stupid and small length limitation on the
        # foreign key and index name, so we can't use the resource type name as
        # tablename, the limit is 64. The longest name we have is
        # fk_<tablename>_history_revision_resource_history_revision,
        # so 64 - 46 = 18
        tablename = "rt_%s" % uuid.uuid4().hex[:15]
        resource_type = ResourceType(name=resource_type.name,
                                     tablename=tablename,
                                     attributes=resource_type.attributes)

        # NOTE(sileht): ensure the driver is able to store the request
        # resource_type
        resource_type.to_baseclass()

        self._add_resource_type(resource_type)

        self._RESOURCE_TYPE_MANAGER.map_and_create_tables(resource_type,
                                                          self.facade)

        return resource_type

    def get_resource_type(self, name):
        with self.facade.independent_reader() as session:
            return self._get_resource_type(session, name)

    def _get_resource_type(self, session, name):
        resource_type = session.query(ResourceType).get(name)
        if not resource_type:
            raise indexer.NoSuchResourceType(name)
        return resource_type

    @staticmethod
    def get_resource_type_schema():
        return base.RESOURCE_TYPE_SCHEMA_MANAGER

    @staticmethod
    def get_resource_attributes_schemas():
        return [ext.plugin.schema() for ext in ResourceType.RESOURCE_SCHEMAS]

    def list_resource_types(self):
        with self.facade.independent_reader() as session:
            return list(session.query(ResourceType).order_by(
                ResourceType.name.asc()).all())

    # NOTE(jd) We can have deadlock errors either here or later in
    # map_and_create_tables(). We can't decorate delete_resource_type()
    # directly or each part might retry later on its own and cause a
    # duplicate. And it seems there's no way to use the same session for
    # both adding the resource_type in our table and calling
    # map_and_create_tables() :-(
    @retry_on_deadlock
    def _delete_resource_type(self, name):
        try:
            with self.facade.writer() as session:
                resource_type = self._get_resource_type(session, name)
                session.delete(resource_type)
        except exception.DBReferenceError as e:
            if (e.constraint in [
                    'fk_resource_resource_type_name',
                    'fk_resource_history_resource_type_name',
                    'fk_rh_resource_type_name']):
                raise indexer.ResourceTypeInUse(name)
            raise
        return resource_type

    def delete_resource_type(self, name):
        if name == "generic":
            raise indexer.ResourceTypeInUse(name)

        resource_type = self._delete_resource_type(name)

        with self.facade.writer_connection() as connection:
            self._RESOURCE_TYPE_MANAGER.unmap_and_delete_tables(resource_type,
                                                                connection)

    def _resource_type_to_classes(self, session, name):
        resource_type = self._get_resource_type(session, name)
        return self._RESOURCE_TYPE_MANAGER.get_classes(resource_type)

    def list_archive_policies(self):
        with self.facade.independent_reader() as session:
            return list(session.query(ArchivePolicy).all())

    def get_archive_policy(self, name):
        with self.facade.independent_reader() as session:
            return session.query(ArchivePolicy).get(name)

    def delete_archive_policy(self, name):
        with self.facade.writer() as session:
            try:
                if session.query(ArchivePolicy).filter(
                        ArchivePolicy.name == name).delete() == 0:
                    raise indexer.NoSuchArchivePolicy(name)
            except exception.DBReferenceError as e:
                if (e.constraint ==
                   'fk_metric_archive_policy_name_archive_policy_name'):
                    raise indexer.ArchivePolicyInUse(name)
                raise

    def create_archive_policy(self, archive_policy):
        ap = ArchivePolicy(
            name=archive_policy.name,
            back_window=archive_policy.back_window,
            definition=archive_policy.definition,
            aggregation_methods=list(archive_policy.aggregation_methods),
        )
        try:
            with self.facade.writer() as session:
                session.add(ap)
        except exception.DBDuplicateEntry:
            raise indexer.ArchivePolicyAlreadyExists(archive_policy.name)
        return ap

    def list_archive_policy_rules(self):
        with self.facade.independent_reader() as session:
            return session.query(ArchivePolicyRule).order_by(
                ArchivePolicyRule.metric_pattern.desc()).all()

    def get_archive_policy_rule(self, name):
        with self.facade.independent_reader() as session:
            return session.query(ArchivePolicyRule).get(name)

    def delete_archive_policy_rule(self, name):
        with self.facade.writer() as session:
            if session.query(ArchivePolicyRule).filter(
                    ArchivePolicyRule.name == name).delete() == 0:
                raise indexer.NoSuchArchivePolicyRule(name)

    def create_archive_policy_rule(self, name, metric_pattern,
                                   archive_policy_name):
        apr = ArchivePolicyRule(
            name=name,
            archive_policy_name=archive_policy_name,
            metric_pattern=metric_pattern
        )
        try:
            with self.facade.writer() as session:
                session.add(apr)
        except exception.DBDuplicateEntry:
            raise indexer.ArchivePolicyRuleAlreadyExists(name)
        return apr

    @retry_on_deadlock
    def create_metric(self, id, created_by_user_id, created_by_project_id,
                      archive_policy_name,
                      name=None, resource_id=None):
        m = Metric(id=id,
                   created_by_user_id=created_by_user_id,
                   created_by_project_id=created_by_project_id,
                   archive_policy_name=archive_policy_name,
                   name=name,
                   resource_id=resource_id)
        try:
            with self.facade.writer() as session:
                session.add(m)
        except exception.DBReferenceError as e:
            if (e.constraint ==
               'fk_metric_archive_policy_name_archive_policy_name'):
                raise indexer.NoSuchArchivePolicy(archive_policy_name)
            raise
        return m

    @retry_on_deadlock
    def list_metrics(self, names=None, ids=None, details=False,
                     status='active', **kwargs):
        if ids is not None and not ids:
            return []
        with self.facade.independent_reader() as session:
            q = session.query(Metric).filter(
                Metric.status == status).order_by(Metric.id)
            if names is not None:
                q = q.filter(Metric.name.in_(names))
            if ids is not None:
                q = q.filter(Metric.id.in_(ids))
            for attr in kwargs:
                q = q.filter(getattr(Metric, attr) == kwargs[attr])
            if details:
                q = q.options(sqlalchemy.orm.joinedload('resource'))

            return list(q.all())

    @retry_on_deadlock
    def create_resource(self, resource_type, id,
                        created_by_user_id, created_by_project_id,
                        user_id=None, project_id=None,
                        started_at=None, ended_at=None, metrics=None,
                        **kwargs):
        if (started_at is not None
           and ended_at is not None
           and started_at > ended_at):
            raise ValueError(
                "Start timestamp cannot be after end timestamp")
        with self.facade.writer() as session:
            resource_cls = self._resource_type_to_classes(
                session, resource_type)['resource']
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

    @retry_on_deadlock
    def update_resource(self, resource_type,
                        resource_id, ended_at=_marker, metrics=_marker,
                        append_metrics=False,
                        create_revision=True,
                        **kwargs):
        with self.facade.writer() as session:
            classes = self._resource_type_to_classes(session, resource_type)
            resource_cls = classes["resource"]
            resource_history_cls = classes["history"]

            try:
                # NOTE(sileht): We use FOR UPDATE that is not galera friendly,
                # but they are no other way to cleanly patch a resource and
                # store the history that safe when two concurrent calls are
                # done.
                q = session.query(resource_cls).filter(
                    resource_cls.id == resource_id).with_for_update()

                r = q.first()
                if r is None:
                    raise indexer.NoSuchResource(resource_id)

                if create_revision:
                    # Build history
                    rh = resource_history_cls()
                    for col in sqlalchemy.inspect(resource_cls).columns:
                        setattr(rh, col.name, getattr(r, col.name))
                    now = utils.utcnow()
                    rh.revision_end = now
                    session.add(rh)
                    r.revision_start = now

                # Update the resource
                if ended_at is not _marker:
                    # NOTE(jd) MySQL does not honor checks. I hate it.
                    engine = session.connection()
                    if engine.dialect.name == "mysql":
                        if r.started_at is not None and ended_at is not None:
                            if r.started_at > ended_at:
                                raise indexer.ResourceValueError(
                                    resource_type, "ended_at", ended_at)
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
                            Metric.resource_id == resource_id,
                            Metric.status == 'active').update(
                                {"resource_id": None})
                    self._set_metrics_for_resource(session, r, metrics)

                session.flush()
            except exception.DBConstraintError as e:
                if e.check_name == "ck_started_before_ended":
                    raise indexer.ResourceValueError(
                        resource_type, "ended_at", ended_at)
                raise

            # NOTE(jd) Force load of metrics – do it outside the session!
            r.metrics

            return r

    @staticmethod
    def _set_metrics_for_resource(session, r, metrics):
        for name, value in six.iteritems(metrics):
            if isinstance(value, uuid.UUID):
                try:
                    update = session.query(Metric).filter(
                        Metric.id == value,
                        Metric.status == 'active',
                        (Metric.created_by_user_id
                         == r.created_by_user_id),
                        (Metric.created_by_project_id
                         == r.created_by_project_id),
                    ).update({"resource_id": r.id, "name": name})
                except exception.DBDuplicateEntry:
                    raise indexer.NamedMetricAlreadyExists(name)
                if update == 0:
                    raise indexer.NoSuchMetric(value)
            else:
                ap_name = value['archive_policy_name']
                m = Metric(id=uuid.uuid4(),
                           created_by_user_id=r.created_by_user_id,
                           created_by_project_id=r.created_by_project_id,
                           archive_policy_name=ap_name,
                           name=name,
                           resource_id=r.id)
                session.add(m)
                try:
                    session.flush()
                except exception.DBDuplicateEntry:
                    raise indexer.NamedMetricAlreadyExists(name)
                except exception.DBReferenceError as e:
                    if (e.constraint ==
                       'fk_metric_archive_policy_name_archive_policy_name'):
                        raise indexer.NoSuchArchivePolicy(ap_name)
                    raise

        session.expire(r, ['metrics'])

    @retry_on_deadlock
    def delete_resource(self, resource_id):
        with self.facade.writer() as session:
            # We are going to delete the resource; the on delete will set the
            # resource_id of the attached metrics to NULL, we just have to mark
            # their status as 'delete'
            session.query(Metric).filter(
                Metric.resource_id == resource_id).update(
                    {"status": "delete"})
            if session.query(Resource).filter(
                    Resource.id == resource_id).delete() == 0:
                raise indexer.NoSuchResource(resource_id)

    @retry_on_deadlock
    def get_resource(self, resource_type, resource_id, with_metrics=False):
        with self.facade.independent_reader() as session:
            resource_cls = self._resource_type_to_classes(
                session, resource_type)['resource']
            q = session.query(
                resource_cls).filter(
                    resource_cls.id == resource_id)
            if with_metrics:
                q = q.options(sqlalchemy.orm.joinedload('metrics'))
            return q.first()

    def _get_history_result_mapper(self, session, resource_type):
        classes = self._resource_type_to_classes(session, resource_type)
        resource_cls = classes['resource']
        history_cls = classes['history']

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
                return iter((key, getattr(self, key)) for key in stmt.c.keys())

        sqlalchemy.orm.mapper(
            Result, stmt, primary_key=[stmt.c.id, stmt.c.revision],
            properties={
                'metrics': sqlalchemy.orm.relationship(
                    Metric,
                    primaryjoin=sqlalchemy.and_(
                        Metric.resource_id == stmt.c.id,
                        Metric.status == 'active'),
                    foreign_keys=Metric.resource_id)
            })

        return Result

    @retry_on_deadlock
    def list_resources(self, resource_type='generic',
                       attribute_filter=None,
                       details=False,
                       history=False,
                       limit=None,
                       marker=None,
                       sorts=None):
        sorts = sorts or []

        with self.facade.independent_reader() as session:
            if history:
                target_cls = self._get_history_result_mapper(
                    session, resource_type)
            else:
                target_cls = self._resource_type_to_classes(
                    session, resource_type)["resource"]

            q = session.query(target_cls)

            if attribute_filter:
                engine = session.connection()
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

            # transform the api-wg representation to the oslo.db one
            sort_keys = []
            sort_dirs = []
            for sort in sorts:
                sort_key, __, sort_dir = sort.partition(":")
                sort_keys.append(sort_key.strip())
                sort_dirs.append(sort_dir or 'asc')

            # paginate_query require at list one uniq column
            if 'id' not in sort_keys:
                sort_keys.append('id')
                sort_dirs.append('asc')

            if marker:
                resource_marker = self.get_resource(resource_type, marker)
                if resource_marker is None:
                    raise indexer.InvalidPagination(
                        "Invalid marker: `%s'" % marker)
            else:
                resource_marker = None

            try:
                q = oslo_db_utils.paginate_query(q, target_cls, limit=limit,
                                                 sort_keys=sort_keys,
                                                 marker=resource_marker,
                                                 sort_dirs=sort_dirs)
            except ValueError as e:
                raise indexer.InvalidPagination(e)
            except exception.InvalidSortKey as e:
                # FIXME(jd) Wait for https://review.openstack.org/274868 to be
                # released so we can return which key
                raise indexer.InvalidPagination("Invalid sort keys")

            # Always include metrics
            q = q.options(sqlalchemy.orm.joinedload("metrics"))
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
                            target_cls = self._resource_type_to_classes(
                                session, type)['history']
                            f = target_cls.revision.in_(
                                [r.revision for r in resources])
                        else:
                            target_cls = self._resource_type_to_classes(
                                session, type)["resource"]
                            f = target_cls.id.in_([r.id for r in resources])

                        q = session.query(target_cls).filter(f)
                        # Always include metrics
                        q = q.options(sqlalchemy.orm.joinedload('metrics'))
                        try:
                            all_resources.extend(q.all())
                        except sqlalchemy.exc.ProgrammingError as e:
                            # NOTE(jd) This exception can happen when the
                            # resources and their resource type have been
                            # deleted in the meantime:
                            #  sqlalchemy.exc.ProgrammingError:
                            #    (pymysql.err.ProgrammingError)
                            #    (1146, "Table \'test.rt_f00\' doesn\'t exist")
                            # In that case, just ignore those resources.
                            inn_e = e.inner_exception
                            if (not pymysql
                               or not isinstance(
                                   inn_e, sqlalchemy.exc.ProgrammingError)
                               or not isinstance(
                                   inn_e.orig, pymysql.err.ProgrammingError)
                               or (inn_e.orig.args[0]
                                   != pymysql.constants.ER.NO_SUCH_TABLE)):
                                raise

            return all_resources

    def expunge_metric(self, id):
        with self.facade.writer() as session:
            if session.query(Metric).filter(Metric.id == id).delete() == 0:
                raise indexer.NoSuchMetric(id)

    def delete_metric(self, id):
        with self.facade.writer() as session:
            if session.query(Metric).filter(
                Metric.id == id, Metric.status == 'active').update(
                    {"status": "delete"}) == 0:
                raise indexer.NoSuchMetric(id)


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
            if value is not None:
                converter = None

                if isinstance(attr.type, base.PreciseTimestamp):
                    converter = utils.to_timestamp
                elif (isinstance(attr.type, sqlalchemy_utils.UUIDType)
                      and not isinstance(value, uuid.UUID)):
                    converter = utils.ResourceUUID
                elif isinstance(attr.type, types.String):
                    converter = six.text_type
                elif isinstance(attr.type, types.Integer):
                    converter = int
                elif isinstance(attr.type, types.Numeric):
                    converter = float

                if converter:
                    try:
                        value = converter(value)
                    except Exception:
                        raise indexer.QueryValueError(value, field_name)

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
