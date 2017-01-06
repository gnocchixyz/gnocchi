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

from alembic import migration
from alembic import operations
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
from sqlalchemy.engine import url as sqlalchemy_url
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
    return oslo_db.api.wrap_db_retry(retry_on_deadlock=True,
                                     max_retries=20,
                                     retry_interval=0.1,
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
        # FIXME(sileht): 3 attributes, perhaps we need a better structure.
        self._cache = {'generic': {'resource': base.Resource,
                                   'history': base.ResourceHistory,
                                   'updated_at': utils.utcnow()}}

    @staticmethod
    def _build_class_mappers(resource_type, baseclass=None):
        tablename = resource_type.tablename
        tables_args = {"extend_existing": True}
        tables_args.update(base.COMMON_TABLES_ARGS)
        # TODO(sileht): Add columns
        if not baseclass:
            baseclass = resource_type.to_baseclass()
        resource_ext = type(
            str("%s_resource" % tablename),
            (baseclass, base.ResourceExtMixin, base.Resource),
            {"__tablename__": tablename, "__table_args__": tables_args})
        resource_history_ext = type(
            str("%s_history" % tablename),
            (baseclass, base.ResourceHistoryExtMixin, base.ResourceHistory),
            {"__tablename__": ("%s_history" % tablename),
             "__table_args__": tables_args})
        return {'resource': resource_ext,
                'history': resource_history_ext,
                'updated_at': resource_type.updated_at}

    def get_classes(self, resource_type):
        # NOTE(sileht): We don't care about concurrency here because we allow
        # sqlalchemy to override its global object with extend_existing=True
        # this is safe because classname and tablename are uuid.
        try:
            mappers = self._cache[resource_type.tablename]
            # Cache is outdated
            if (resource_type.name != "generic"
                    and resource_type.updated_at > mappers['updated_at']):
                for table_purpose in ['resource', 'history']:
                    Base.metadata.remove(Base.metadata.tables[
                        mappers[table_purpose].__tablename__])
                del self._cache[resource_type.tablename]
                raise KeyError
            return mappers
        except KeyError:
            mapper = self._build_class_mappers(resource_type)
            self._cache[resource_type.tablename] = mapper
            return mapper

    @retry_on_deadlock
    def map_and_create_tables(self, resource_type, facade):
        if resource_type.state != "creating":
            raise RuntimeError("map_and_create_tables must be called in state "
                               "creating")

        mappers = self.get_classes(resource_type)
        tables = [Base.metadata.tables[mappers["resource"].__tablename__],
                  Base.metadata.tables[mappers["history"].__tablename__]]

        try:
            with facade.writer_connection() as connection:
                Base.metadata.create_all(connection, tables=tables)
        except exception.DBError as e:
            if self._is_current_transaction_aborted(e):
                raise exception.RetryRequest(e)
            raise

        # NOTE(sileht): no need to protect the _cache with a lock
        # get_classes cannot be called in state creating
        self._cache[resource_type.tablename] = mappers

    @staticmethod
    def _is_current_transaction_aborted(exception):
        # HACK(jd) Sometimes, PostgreSQL raises an error such as "current
        # transaction is aborted, commands ignored until end of transaction
        # block" on its own catalog, so we need to retry, but this is not
        # caught by oslo.db as a deadlock. This is likely because when we use
        # Base.metadata.create_all(), sqlalchemy itself gets an error it does
        # not catch or something. So this is why this function exists. To
        # paperover I guess.
        inn_e = exception.inner_exception
        return (psycopg2
                and isinstance(inn_e, sqlalchemy.exc.InternalError)
                and isinstance(inn_e.orig, psycopg2.InternalError)
                # current transaction is aborted
                and inn_e.orig.pgcode == '25P02')

    @retry_on_deadlock
    def unmap_and_delete_tables(self, resource_type, facade):
        if resource_type.state != "deleting":
            raise RuntimeError("unmap_and_delete_tables must be called in "
                               "state deleting")

        mappers = self.get_classes(resource_type)
        del self._cache[resource_type.tablename]

        tables = [Base.metadata.tables[mappers['resource'].__tablename__],
                  Base.metadata.tables[mappers['history'].__tablename__]]

        # NOTE(sileht): Base.metadata.drop_all doesn't
        # issue CASCADE stuffs correctly at least on postgresql
        # We drop foreign keys manually to not lock the destination
        # table for too long during drop table.
        # It's safe to not use a transaction since
        # the resource_type table is already cleaned and committed
        # so this code cannot be triggerred anymore for this
        # resource_type
        with facade.writer_connection() as connection:
            try:
                for table in tables:
                    for fk in table.foreign_key_constraints:
                        try:
                            self._safe_execute(
                                connection,
                                sqlalchemy.schema.DropConstraint(fk))
                        except exception.DBNonExistentConstraint:
                            pass
                for table in tables:
                    try:
                        self._safe_execute(connection,
                                           sqlalchemy.schema.DropTable(table))
                    except exception.DBNonExistentTable:
                        pass
            except exception.DBError as e:
                if self._is_current_transaction_aborted(e):
                    raise exception.RetryRequest(e)
                raise

            # NOTE(sileht): If something goes wrong here, we are currently
            # fucked, that why we expose the state to the superuser.
            # But we allow him to delete a resource type in error state
            # in case of he cleanup the mess manually and want gnocchi to
            # control and finish the cleanup.

        # TODO(sileht): Remove this resource on other workers
        # by using expiration on cache ?
        for table in tables:
            Base.metadata.remove(table)

    @retry_on_deadlock
    def _safe_execute(self, connection, works):
        # NOTE(sileht): we create a transaction to ensure mysql
        # create locks on other transaction...
        trans = connection.begin()
        connection.execute(works)
        trans.commit()


class SQLAlchemyIndexer(indexer.IndexerDriver):
    _RESOURCE_TYPE_MANAGER = ResourceClassMapper()

    @classmethod
    def _create_new_database(cls, url):
        """Used by testing to create a new database."""
        purl = sqlalchemy_url.make_url(
            cls.dress_url(
                url))
        purl.database = purl.database + str(uuid.uuid4()).replace('-', '')
        new_url = str(purl)
        sqlalchemy_utils.create_database(new_url)
        return new_url

    @staticmethod
    def dress_url(url):
        # If no explicit driver has been set, we default to pymysql
        if url.startswith("mysql://"):
            url = sqlalchemy_url.make_url(url)
            url.drivername = "mysql+pymysql"
            return str(url)
        return url

    def __init__(self, conf):
        conf.set_override("connection",
                          self.dress_url(conf.indexer.url),
                          "database")
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
                continue

            if rt.name != "generic":
                try:
                    self._RESOURCE_TYPE_MANAGER.map_and_create_tables(
                        rt, self.facade)
                except Exception:
                    self._set_resource_type_state(rt.name, "creation_error")
                    LOG.exception('Fail to create tables for '
                                  'resource_type "%s"', rt.name)
                    continue

            self._set_resource_type_state(rt.name, "active")

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
        # fk_<tablename>_h_revision_rh_revision,
        # so 64 - 26 = 38 and 3 chars for rt_, 35 chars, uuid is 32, it's cool.
        tablename = "rt_%s" % uuid.uuid4().hex
        resource_type = ResourceType(name=resource_type.name,
                                     tablename=tablename,
                                     attributes=resource_type.attributes,
                                     state="creating")

        # NOTE(sileht): ensure the driver is able to store the request
        # resource_type
        resource_type.to_baseclass()

        self._add_resource_type(resource_type)

        try:
            self._RESOURCE_TYPE_MANAGER.map_and_create_tables(resource_type,
                                                              self.facade)
        except Exception:
            # NOTE(sileht): We fail the DDL, we have no way to automatically
            # recover, just set a particular state
            self._set_resource_type_state(resource_type.name, "creation_error")
            raise

        self._set_resource_type_state(resource_type.name, "active")
        resource_type.state = "active"
        return resource_type

    def update_resource_type(self, name, add_attributes=None,
                             del_attributes=None):
        if not add_attributes and not del_attributes:
            return
        add_attributes = add_attributes or []
        del_attributes = del_attributes or []

        self._set_resource_type_state(name, "updating", "active")

        try:
            with self.facade.independent_writer() as session:
                rt = self._get_resource_type(session, name)

                with self.facade.writer_connection() as connection:
                    ctx = migration.MigrationContext.configure(connection)
                    op = operations.Operations(ctx)
                    for table in [rt.tablename, '%s_history' % rt.tablename]:
                        with op.batch_alter_table(table) as batch_op:
                            for attr in del_attributes:
                                batch_op.drop_column(attr)
                            for attr in add_attributes:
                                # TODO(sileht): When attr.required is True, we
                                # have to pass a default. rest layer current
                                # protect us, requied = True is not yet allowed
                                batch_op.add_column(sqlalchemy.Column(
                                    attr.name, attr.satype,
                                    nullable=not attr.required))

                rt.state = "active"
                rt.updated_at = utils.utcnow()
                rt.attributes.extend(add_attributes)
                for attr in list(rt.attributes):
                    if attr.name in del_attributes:
                        rt.attributes.remove(attr)
                # FIXME(sileht): yeah that's wierd but attributes is a custom
                # json column and 'extend' doesn't trigger sql update, this
                # enforce the update. I wonder if sqlalchemy provides something
                # on column description side.
                sqlalchemy.orm.attributes.flag_modified(rt, 'attributes')

        except Exception:
            # NOTE(sileht): We fail the DDL, we have no way to automatically
            # recover, just set a particular state
            # TODO(sileht): Create a repair REST endpoint that delete
            # columns not existing in the database but in the resource type
            # description. This will allow to pass wrong update_error to active
            # state, that currently not possible.
            self._set_resource_type_state(name, "updating_error")
            raise

        return rt

    def get_resource_type(self, name):
        with self.facade.independent_reader() as session:
            return self._get_resource_type(session, name)

    def _get_resource_type(self, session, name):
        resource_type = session.query(ResourceType).get(name)
        if not resource_type:
            raise indexer.NoSuchResourceType(name)
        return resource_type

    @retry_on_deadlock
    def _set_resource_type_state(self, name, state,
                                 expected_previous_state=None):
        with self.facade.writer() as session:
            q = session.query(ResourceType)
            q = q.filter(ResourceType.name == name)
            if expected_previous_state is not None:
                q = q.filter(ResourceType.state == expected_previous_state)
            update = q.update({'state': state})
            if update == 0:
                if expected_previous_state is not None:
                    rt = session.query(ResourceType).get(name)
                    if rt:
                        raise indexer.UnexpectedResourceTypeState(
                            name, expected_previous_state, rt.state)
                raise indexer.IndexerException(
                    "Fail to set resource type state of %s to %s" %
                    (name, state))

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
    def _mark_as_deleting_resource_type(self, name):
        try:
            with self.facade.writer() as session:
                rt = self._get_resource_type(session, name)
                if rt.state not in ["active", "deletion_error",
                                    "creation_error", "updating_error"]:
                    raise indexer.UnexpectedResourceTypeState(
                        name,
                        "active/deletion_error/creation_error/updating_error",
                        rt.state)
                session.delete(rt)

                # FIXME(sileht): Why do I need to flush here !!!
                # I want remove/add in the same transaction !!!
                session.flush()

                # NOTE(sileht): delete and recreate to:
                # * raise duplicate constraints
                # * ensure we do not create a new resource type
                #   with the same name while we destroy the tables next
                rt = ResourceType(name=rt.name,
                                  tablename=rt.tablename,
                                  state="deleting",
                                  attributes=rt.attributes)
                session.add(rt)
        except exception.DBReferenceError as e:
            if (e.constraint in [
                    'fk_resource_resource_type_name',
                    'fk_resource_history_resource_type_name',
                    'fk_rh_resource_type_name']):
                raise indexer.ResourceTypeInUse(name)
            raise
        return rt

    @retry_on_deadlock
    def _delete_resource_type(self, name):
        # Really delete the resource type, no resource can be linked to it
        # Because we cannot add a resource to a resource_type not in 'active'
        # state
        with self.facade.writer() as session:
            resource_type = self._get_resource_type(session, name)
            session.delete(resource_type)

    def delete_resource_type(self, name):
        if name == "generic":
            raise indexer.ResourceTypeInUse(name)

        rt = self._mark_as_deleting_resource_type(name)

        try:
            self._RESOURCE_TYPE_MANAGER.unmap_and_delete_tables(
                rt, self.facade)
        except Exception:
            # NOTE(sileht): We fail the DDL, we have no way to automatically
            # recover, just set a particular state
            self._set_resource_type_state(rt.name, "deletion_error")
            raise

        self._delete_resource_type(name)

    def _resource_type_to_mappers(self, session, name):
        resource_type = self._get_resource_type(session, name)
        if resource_type.state != "active":
            raise indexer.UnexpectedResourceTypeState(
                name, "active", resource_type.state)
        return self._RESOURCE_TYPE_MANAGER.get_classes(resource_type)

    def list_archive_policies(self):
        with self.facade.independent_reader() as session:
            return list(session.query(ArchivePolicy).all())

    def get_archive_policy(self, name):
        with self.facade.independent_reader() as session:
            return session.query(ArchivePolicy).get(name)

    def update_archive_policy(self, name, ap_items):
        with self.facade.independent_writer() as session:
            ap = session.query(ArchivePolicy).get(name)
            if not ap:
                raise indexer.NoSuchArchivePolicy(name)
            current = sorted(ap.definition,
                             key=operator.attrgetter('granularity'))
            new = sorted(ap_items, key=operator.attrgetter('granularity'))
            if len(current) != len(new):
                raise indexer.UnsupportedArchivePolicyChange(
                    name, 'Cannot add or drop granularities')
            for c, n in zip(current, new):
                if c.granularity != n.granularity:
                    raise indexer.UnsupportedArchivePolicyChange(
                        name, '%s granularity interval was changed'
                        % c.granularity)
            # NOTE(gordc): ORM doesn't update JSON column unless new
            ap.definition = ap_items
            return ap

    def delete_archive_policy(self, name):
        constraints = [
            "fk_metric_ap_name_ap_name",
            "fk_apr_ap_name_ap_name"]
        with self.facade.writer() as session:
            try:
                if session.query(ArchivePolicy).filter(
                        ArchivePolicy.name == name).delete() == 0:
                    raise indexer.NoSuchArchivePolicy(name)
            except exception.DBReferenceError as e:
                if e.constraint in constraints:
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
                      name=None, unit=None, resource_id=None):
        m = Metric(id=id,
                   created_by_user_id=created_by_user_id,
                   created_by_project_id=created_by_project_id,
                   archive_policy_name=archive_policy_name,
                   name=name,
                   unit=unit,
                   resource_id=resource_id)
        try:
            with self.facade.writer() as session:
                session.add(m)
        except exception.DBReferenceError as e:
            if (e.constraint ==
               'fk_metric_ap_name_ap_name'):
                raise indexer.NoSuchArchivePolicy(archive_policy_name)
            raise
        return m

    @retry_on_deadlock
    def list_metrics(self, names=None, ids=None, details=False,
                     status='active', limit=None, marker=None, sorts=None,
                     **kwargs):
        sorts = sorts or []
        if ids is not None and not ids:
            return []
        with self.facade.independent_reader() as session:
            q = session.query(Metric).filter(
                Metric.status == status)
            if names is not None:
                q = q.filter(Metric.name.in_(names))
            if ids is not None:
                q = q.filter(Metric.id.in_(ids))
            for attr in kwargs:
                q = q.filter(getattr(Metric, attr) == kwargs[attr])
            if details:
                q = q.options(sqlalchemy.orm.joinedload('resource'))

            sort_keys, sort_dirs = self._build_sort_keys(sorts)

            if marker:
                metric_marker = self.list_metrics(ids=[marker])
                if metric_marker:
                    metric_marker = metric_marker[0]
                else:
                    raise indexer.InvalidPagination(
                        "Invalid marker: `%s'" % marker)
            else:
                metric_marker = None

            try:
                q = oslo_db_utils.paginate_query(q, Metric, limit=limit,
                                                 sort_keys=sort_keys,
                                                 marker=metric_marker,
                                                 sort_dirs=sort_dirs)
            except ValueError as e:
                raise indexer.InvalidPagination(e)
            except exception.InvalidSortKey as e:
                raise indexer.InvalidPagination(e)

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
            resource_cls = self._resource_type_to_mappers(
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
            mappers = self._resource_type_to_mappers(session, resource_type)
            resource_cls = mappers["resource"]
            resource_history_cls = mappers["history"]

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
                unit = value.get('unit')
                ap_name = value['archive_policy_name']
                m = Metric(id=uuid.uuid4(),
                           created_by_user_id=r.created_by_user_id,
                           created_by_project_id=r.created_by_project_id,
                           archive_policy_name=ap_name,
                           name=name,
                           unit=unit,
                           resource_id=r.id)
                session.add(m)
                try:
                    session.flush()
                except exception.DBDuplicateEntry:
                    raise indexer.NamedMetricAlreadyExists(name)
                except exception.DBReferenceError as e:
                    if (e.constraint ==
                       'fk_metric_ap_name_ap_name'):
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
    def delete_resources(self, resource_type='generic',
                         attribute_filter=None):
        if not attribute_filter:
            raise ValueError("attribute_filter must be set")

        with self.facade.writer() as session:
            target_cls = self._resource_type_to_mappers(
                session, resource_type)["resource"]

            q = session.query(target_cls.id)

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

            session.query(Metric).filter(
                Metric.resource_id.in_(q)
            ).update({"status": "delete"},
                     synchronize_session=False)
            return q.delete(synchronize_session=False)

    @retry_on_deadlock
    def get_resource(self, resource_type, resource_id, with_metrics=False):
        with self.facade.independent_reader() as session:
            resource_cls = self._resource_type_to_mappers(
                session, resource_type)['resource']
            q = session.query(
                resource_cls).filter(
                    resource_cls.id == resource_id)
            if with_metrics:
                q = q.options(sqlalchemy.orm.joinedload('metrics'))
            return q.first()

    def _get_history_result_mapper(self, session, resource_type):
        mappers = self._resource_type_to_mappers(session, resource_type)
        resource_cls = mappers['resource']
        history_cls = mappers['history']

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
                target_cls = self._resource_type_to_mappers(
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

            sort_keys, sort_dirs = self._build_sort_keys(sorts)

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
                raise indexer.InvalidPagination(e)

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
                        try:
                            target_cls = self._resource_type_to_mappers(
                                session, type)['history' if is_history else
                                               'resource']
                        except (indexer.UnexpectedResourceTypeState,
                                indexer.NoSuchResourceType):
                            # NOTE(sileht): This resource_type have been
                            # removed in the meantime.
                            continue
                        if is_history:
                            f = target_cls.revision.in_([r.revision
                                                         for r in resources])
                        else:
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
                            if (not pymysql
                               or not isinstance(
                                   e, sqlalchemy.exc.ProgrammingError)
                               or not isinstance(
                                   e.orig, pymysql.err.ProgrammingError)
                               or (e.orig.args[0]
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

    @staticmethod
    def _build_sort_keys(sorts):
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

        return sort_keys, sort_dirs


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
                        if isinstance(value, list):
                            # we got a list for in_ operator
                            value = [converter(v) for v in value]
                        else:
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
