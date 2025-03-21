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

import copy
import datetime
import itertools
import operator
import os.path
import threading
import uuid

from alembic import migration
from alembic import operations
import daiquiri
import oslo_db.api
from oslo_db import exception
from oslo_db.sqlalchemy import enginefacade
from oslo_db.sqlalchemy import utils as oslo_db_utils
try:
    import psycopg2
except ImportError:
    psycopg2 = None
try:
    import pymysql.constants.ER
    import pymysql.err
except ImportError:
    pymysql = None
import sqlalchemy
from sqlalchemy.engine import url as sqlalchemy_url
import sqlalchemy.exc
from sqlalchemy import (
    delete,
    select,
    types as sa_types,
    update,
)
import sqlalchemy_utils
from urllib import parse as urlparse

from gnocchi import exceptions
from gnocchi import indexer
from gnocchi.indexer import sqlalchemy_base as base
from gnocchi.indexer import sqlalchemy_types as types
from gnocchi import resource_type
from gnocchi import utils


mapper_reg = sqlalchemy.orm.registry()

Base = base.Base
Metric = base.Metric
ArchivePolicy = base.ArchivePolicy
ArchivePolicyRule = base.ArchivePolicyRule
Resource = base.Resource
ResourceHistory = base.ResourceHistory
ResourceType = base.ResourceType

_marker = indexer._marker

LOG = daiquiri.getLogger(__name__)


def _retry_on_exceptions(exc):
    if not isinstance(exc, exception.DBError):
        return False
    inn_e = exc.inner_exception
    if not isinstance(inn_e, sqlalchemy.exc.InternalError):
        return False
    return ((
        pymysql and
        isinstance(inn_e.orig, pymysql.err.InternalError) and
        (inn_e.orig.args[0] == pymysql.constants.ER.TABLE_DEF_CHANGED)
    ) or (
        # HACK(jd) Sometimes, PostgreSQL raises an error such as "current
        # transaction is aborted, commands ignored until end of transaction
        # block" on its own catalog, so we need to retry, but this is not
        # caught by oslo.db as a deadlock. This is likely because when we use
        # Base.metadata.create_all(), sqlalchemy itself gets an error it does
        # not catch or something. So this is why this function exists. To
        # paperover I guess.
        psycopg2
        and isinstance(inn_e.orig, psycopg2.InternalError)
        # current transaction is aborted
        and inn_e.orig.pgcode == '25P02'
    ))


def retry_on_deadlock(f):
    return oslo_db.api.wrap_db_retry(retry_on_deadlock=True,
                                     max_retries=20,
                                     retry_interval=0.1,
                                     max_retry_interval=2,
                                     exception_checker=_retry_on_exceptions)(f)


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
        return self.trans.writer.get_engine()

    def dispose_pool(self):
        self.trans.dispose_pool()


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

        with facade.writer_connection() as connection:
            Base.metadata.create_all(connection, tables=tables)

        # NOTE(sileht): no need to protect the _cache with a lock
        # get_classes cannot be called in state creating
        self._cache[resource_type.tablename] = mappers

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
        for table in tables:
            for fk in table.foreign_key_constraints:
                with facade.writer() as session:
                    try:
                        stmt = sqlalchemy.schema.DropConstraint(fk)
                        session.execute(stmt)
                        session.commit()
                    except exception.DBNonExistentConstraint:
                        pass

        for table in tables:
            with facade.writer() as session:
                try:
                    stmt = sqlalchemy.schema.DropTable(table)
                    session.execute(stmt)
                    session.commit()
                except exception.DBNonExistentTable:
                    pass

            # NOTE(sileht): If something goes wrong here, we are currently
            # fucked, that why we expose the state to the superuser.
            # But we allow him to delete a resource type in error state
            # in case of he cleanup the mess manually and want gnocchi to
            # control and finish the cleanup.

        # TODO(sileht): Remove this resource on other workers
        # by using expiration on cache ?
        for table in tables:
            Base.metadata.remove(table)


class SQLAlchemyIndexer(indexer.IndexerDriver):
    _RESOURCE_TYPE_MANAGER = ResourceClassMapper()

    @classmethod
    def _create_new_database(cls, url):
        """Used by testing to create a new database."""
        purl = sqlalchemy_url.make_url(
            cls.dress_url(
                url))
        new_database = purl.database + str(uuid.uuid4()).replace('-', '')
        purl = purl.set(database=new_database)
        new_url = purl.render_as_string(hide_password=False)
        sqlalchemy_utils.create_database(new_url)
        return new_url

    @classmethod
    def dress_url(cls, url):
        # If no explicit driver has been set, we default to pymysql
        if url.startswith("mysql://"):
            url = sqlalchemy_url.make_url(url)
            url = url.set(drivername="mysql+pymysql")
            return url.render_as_string(hide_password=False)
        if url.startswith("postgresql://"):
            url = sqlalchemy_url.make_url(url)
            url = url.set(drivername="postgresql+psycopg2")
            return url.render_as_string(hide_password=False)
        return url

    def __init__(self, conf):
        conf.set_override("connection",
                          self.dress_url(conf.indexer.url),
                          "database")
        self.conf = conf
        self.facade = PerInstanceFacade(conf)

    def __str__(self):
        parsed = urlparse.urlparse(self.conf.indexer.url)
        url = urlparse.urlunparse((
            parsed.scheme,
            "***:***@%s%s" % (parsed.hostname,
                              ":%s" % parsed.port if parsed.port else ""),
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment))
        return "%s: %s" % (self.__class__.__name__, url)

    def disconnect(self):
        self.facade.dispose_pool()

    def _get_alembic_config(self):
        from alembic import config

        cfg = config.Config(
            "%s/alembic/alembic.ini" % os.path.dirname(__file__))
        cfg.set_main_option('sqlalchemy.url',
                            self.conf.database.connection.replace('%', '%%'))
        return cfg

    def get_engine(self):
        return self.facade.get_engine()

    def upgrade(self, nocreate=False):
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

        try:
            with self.facade.writer() as session:
                session.add(
                    ResourceType(
                        name="generic",
                        tablename="generic",
                        state="active",
                        attributes=resource_type.ResourceTypeAttributes()))
        except exception.DBDuplicateEntry:
            pass

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
                             del_attributes=None, update_attributes=None):
        if not add_attributes and not del_attributes and not update_attributes:
            return
        add_attributes = add_attributes or []
        del_attributes = del_attributes or []
        update_attributes = update_attributes or []

        with self.facade.independent_writer() as session:
            engine = session.connection()
            rt = self._get_resource_type(session, name)

            with self.facade.writer_connection() as connection:
                ctx = migration.MigrationContext.configure(connection)
                op = operations.Operations(ctx)
                self.fill_null_attribute_values(engine, name, rt, session,
                                                update_attributes)

                self._set_resource_type_state(name, "updating", "active")
                for table in [rt.tablename, '%s_history' % rt.tablename]:
                    with op.batch_alter_table(table) as batch_op:
                        for attr in del_attributes:
                            LOG.debug("Dropping column [%s] from resource [%s]"
                                      " and database table [%s]",
                                      attr, name, table)
                            batch_op.drop_column(attr)
                        for attr in add_attributes:
                            LOG.debug("Adding new column [%s] with type [%s], "
                                      "nullable [%s] and default value [%s] "
                                      "in resource [%s] and database "
                                      "table [%s]", attr.name, attr.satype,
                                      not attr.required,
                                      getattr(attr, 'fill', None), name, table)

                            server_default = attr.for_filling(
                                engine.dialect)
                            batch_op.add_column(sqlalchemy.Column(
                                attr.name, attr.satype,
                                nullable=not attr.required,
                                server_default=server_default))

                            # We have all rows filled now, we can remove
                            # the server_default
                            if server_default is not None:
                                LOG.debug("Removing default value [%s] from "
                                          "column [%s] of resource [%s] and "
                                          "database table [%s]",
                                          getattr(attr, 'fill', None),
                                          attr.name, name, table)
                                batch_op.alter_column(
                                    column_name=attr.name,
                                    existing_type=attr.satype,
                                    existing_server_default=server_default,
                                    existing_nullable=not attr.required,
                                    server_default=None)

                        for attr in update_attributes:
                            LOG.debug("Updating column [%s] from old values "
                                      "type [%s], nullable [%s], to new values"
                                      " type [%s], nullable [%s] of resource "
                                      "[%s] and database_table [%s]",
                                      attr[1].name, attr[1].satype,
                                      not attr[1].required, attr[0].satype,
                                      not attr[0].required, name, table)
                            batch_op.alter_column(
                                column_name=attr[1].name,
                                existing_type=attr[1].satype,
                                existing_nullable=not attr[1].required,
                                type_=attr[0].satype,
                                nullable=not attr[0].required)

            rt.state = "active"
            rt.updated_at = utils.utcnow()
            rt.attributes.extend(add_attributes)
            update_attributes = list(map(lambda a: a[0],
                                         update_attributes))
            update_attributes_names = list(map(lambda a: a.name,
                                               update_attributes))
            for attr in list(rt.attributes):
                if (attr.name in del_attributes or
                        attr.name in update_attributes_names):
                    rt.attributes.remove(attr)

            rt.attributes.extend(update_attributes)
            # FIXME(sileht): yeah that's wierd but attributes is a custom
            # json column and 'extend' doesn't trigger sql update, this
            # enforce the update. I wonder if sqlalchemy provides something
            # on column description side.
            LOG.debug("Updating resource [%s] setting attributes as [%s]",
                      name, list(rt.attributes))
            sqlalchemy.orm.attributes.flag_modified(rt, 'attributes')

        return rt

    def fill_null_attribute_values(self, engine, name, rt, session,
                                   update_attributes):
        for table in [rt.tablename, '%s_history' % rt.tablename]:
            for attr in update_attributes:
                if (hasattr(attr[0], 'fill') and
                        attr[0].fill is not None):
                    mappers = self._resource_type_to_mappers(
                        session, name)
                    if table == rt.tablename:
                        resource_cls = mappers["resource"]
                    else:
                        resource_cls = mappers["history"]
                    cls_attr = attr[0].name
                    f = QueryTransformer.build_filter(
                        engine.dialect.name, resource_cls,
                        {'=': {cls_attr: None}})
                    q = select(resource_cls).filter(
                        f).with_for_update()
                    resources = session.scalars(q).all()
                    if resources:
                        LOG.debug("Null resources [%s] to be filled with [%s] "
                                  "for resource-type [%s]", resources,
                                  attr[0].fill, name)
                        for resource in resources:
                            if hasattr(resource, attr[0].name):
                                setattr(resource, attr[0].name,
                                        attr[0].fill)

    def get_resource_type(self, name):
        with self.facade.independent_reader() as session:
            return self._get_resource_type(session, name)

    def _get_resource_type(self, session, name):
        resource_type = session.get(ResourceType, name)
        if not resource_type:
            raise indexer.NoSuchResourceType(name)
        return resource_type

    @retry_on_deadlock
    def _set_resource_type_state(self, name, state,
                                 expected_previous_state=None):
        with self.facade.writer() as session:
            q = update(ResourceType).filter(
                ResourceType.name == name
            ).values(state=state)
            if expected_previous_state is not None:
                q = q.filter(ResourceType.state == expected_previous_state)
            if session.execute(q).rowcount == 0:
                if expected_previous_state is not None:
                    rt = session.get(ResourceType, name)
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
            stmt = select(ResourceType).order_by(
                ResourceType.name.asc())
            return list(session.scalars(stmt).all())

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
            stmt = select(ArchivePolicy)
            return list(session.scalars(stmt).all())

    def get_archive_policy(self, name):
        with self.facade.independent_reader() as session:
            return session.get(ArchivePolicy, name)

    def update_archive_policy(self, name, ap_items, **kwargs):
        with self.facade.independent_writer() as session:
            ap = session.get(ArchivePolicy, name)
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
                        % utils.timespan_total_seconds(c.granularity))
            # NOTE(gordc): ORM doesn't update JSON column unless new
            ap.definition = ap_items
            if kwargs.get("back_window") is not None:
                ap.back_window = kwargs.get("back_window")
            return ap

    def delete_archive_policy(self, name):
        constraints = [
            "fk_metric_ap_name_ap_name",
            "fk_apr_ap_name_ap_name"]
        with self.facade.writer() as session:
            try:
                stmt = delete(ArchivePolicy).where(
                    ArchivePolicy.name == name)
                if session.execute(stmt).rowcount == 0:
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
            stmt = select(ArchivePolicyRule).order_by(
                ArchivePolicyRule.metric_pattern.desc(),
                ArchivePolicyRule.name.asc()
            )
            return session.scalars(stmt).all()

    def get_archive_policy_rule(self, name):
        with self.facade.independent_reader() as session:
            return session.get(ArchivePolicyRule, name)

    def delete_archive_policy_rule(self, name):
        with self.facade.writer() as session:
            stmt = delete(ArchivePolicyRule).where(
                ArchivePolicyRule.name == name)
            if session.execute(stmt).rowcount == 0:
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
        except exception.DBReferenceError as e:
            if e.constraint == 'fk_apr_ap_name_ap_name':
                raise indexer.NoSuchArchivePolicy(archive_policy_name)
            raise
        except exception.DBDuplicateEntry:
            raise indexer.ArchivePolicyRuleAlreadyExists(name)
        return apr

    def update_archive_policy_rule(self, name, new_name):
        apr = self.get_archive_policy_rule(name)
        if not apr:
            raise indexer.NoSuchArchivePolicyRule(name)
        apr.name = new_name
        try:
            with self.facade.writer() as session:
                session.add(apr)
        except exception.DBDuplicateEntry:
            raise indexer.UnsupportedArchivePolicyRuleChange(
                name,
                'Archive policy rule %s already exists.'
                % new_name)
        return apr

    @retry_on_deadlock
    def create_metric(self, id, creator, archive_policy_name,
                      name=None, unit=None, resource_id=None):
        m = Metric(id=id,
                   creator=creator,
                   archive_policy_name=archive_policy_name,
                   name=name,
                   unit=unit,
                   resource_id=resource_id)
        try:
            with self.facade.writer() as session:
                session.add(m)
        except exception.DBDuplicateEntry:
            raise indexer.NamedMetricAlreadyExists(name)
        except exception.DBReferenceError as e:
            if (e.constraint ==
               'fk_metric_ap_name_ap_name'):
                raise indexer.NoSuchArchivePolicy(archive_policy_name)
            if e.constraint == 'fk_metric_resource_id_resource_id':
                raise indexer.NoSuchResource(resource_id)
            raise
        return m

    @retry_on_deadlock
    def list_metrics(self, details=False, status='active',
                     limit=None, marker=None, sorts=None,
                     policy_filter=None, resource_policy_filter=None,
                     attribute_filter=None):
        sorts = sorts or []
        with self.facade.independent_reader() as session:
            q = select(Metric).filter(Metric.status == status)
            if details:
                q = q.options(sqlalchemy.orm.joinedload(Metric.resource))
            if policy_filter or resource_policy_filter or attribute_filter:
                engine = session.connection()
                if attribute_filter:
                    # We don't catch the indexer.QueryAttributeError error here
                    # since we expect any user input on this function. If the
                    # caller screws it, it's its problem: no need to convert
                    # the exception to another type.
                    attribute_f = QueryTransformer.build_filter(
                        engine.dialect.name,
                        Metric, attribute_filter)
                    q = q.filter(attribute_f)
                if policy_filter:
                    # We don't catch the indexer.QueryAttributeError error here
                    # since we expect any user input on this function. If the
                    # caller screws it, it's its problem: no need to convert
                    # the exception to another type.
                    policy_f = QueryTransformer.build_filter(
                        engine.dialect.name,
                        Metric, policy_filter)
                else:
                    policy_f = None
                if resource_policy_filter:
                    q = q.join(Metric.resource)
                    try:
                        resource_policy_f = QueryTransformer.build_filter(
                            engine.dialect.name,
                            Resource,
                            resource_policy_filter)
                    except indexer.QueryAttributeError as e:
                        # NOTE(jd) The QueryAttributeError does not know about
                        # resource_type, so convert it
                        raise indexer.ResourceAttributeError("generic",
                                                             e.attribute)
                else:
                    resource_policy_f = None

                if policy_filter or resource_policy_filter:
                    q = q.filter(sqlalchemy.or_(policy_f, resource_policy_f))

            sort_keys, sort_dirs = self._build_sort_keys(sorts, ['id'])

            if marker:
                metric_marker = self.list_metrics(
                    attribute_filter={"in": {"id": [marker]}})
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

            return list(session.scalars(q).all())

    @retry_on_deadlock
    def create_resource(self, resource_type, id,
                        creator, user_id=None, project_id=None,
                        started_at=None, ended_at=None, metrics=None,
                        original_resource_id=None,
                        **kwargs):
        if (started_at is not None
           and ended_at is not None
           and started_at > ended_at):
            raise ValueError(
                "Start timestamp cannot be after end timestamp")
        if original_resource_id is None:
            original_resource_id = str(id)
        with self.facade.writer() as session:
            resource_cls = self._resource_type_to_mappers(
                session, resource_type)['resource']
            r = resource_cls(
                id=id,
                original_resource_id=original_resource_id,
                type=resource_type,
                creator=creator,
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

            session.commit()

            # NOTE(jd) Force load of metrics :)
            r.metrics

            return r

    @retry_on_deadlock
    def update_resource(self, resource_type, resource_id,
                        ended_at=_marker, metrics=_marker,
                        append_metrics=False, create_revision=True,
                        **kwargs):
        with self.facade.writer() as session:
            data_to_update = kwargs.copy()

            data_to_update['ended_at'] = ended_at
            data_to_update['metrics'] = metrics
            if create_revision:
                resource = self.get_resource(
                    resource_type, resource_id, with_metrics=True)
                if not utils.is_resource_revision_needed(
                        resource, data_to_update):
                    LOG.info("We thought that a revision for resource "
                             "[%s] was needed. However, after locking the "
                             "table and checking it again, we found that it "
                             "is not needed anymore. This is due to a "
                             "concurrency issue that might happen. Therefore, "
                             "no revision is going to be generated at this "
                             "time.", data_to_update)
                    create_revision = False

            mappers = self._resource_type_to_mappers(session, resource_type)
            resource_cls = mappers["resource"]
            resource_history_cls = mappers["history"]

            try:
                # NOTE(sileht): We use FOR UPDATE that is not galera friendly,
                # but they are no other way to cleanly patch a resource and
                # store the history that safe when two concurrent calls are
                # done.
                q = select(resource_cls).filter(
                    resource_cls.id == resource_id).with_for_update()

                r = session.scalars(q).first()
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
                    for attribute, value in kwargs.items():
                        if hasattr(r, attribute):
                            setattr(r, attribute, value)
                        else:
                            raise indexer.ResourceAttributeError(
                                r.type, attribute)

                if metrics is not _marker:
                    if not append_metrics:
                        stmt = update(Metric).filter(
                            Metric.resource_id == resource_id,
                            Metric.status == 'active').values(
                                resource_id=None)
                        session.execute(stmt)
                    self._set_metrics_for_resource(session, r, metrics)

                session.commit()
            except exception.DBConstraintError as e:
                if e.check_name in (
                        "ck_resource_started_before_ended",
                        "ck_resource_history_started_before_ended"):
                    raise indexer.ResourceValueError(
                        resource_type, "ended_at", ended_at)
                raise

            # NOTE(jd) Force load of metrics – do it outside the session!
            r.metrics

            return r

    @staticmethod
    def _set_metrics_for_resource(session, r, metrics):
        for name, value in metrics.items():
            if isinstance(value, uuid.UUID):
                try:
                    stmt = update(Metric).filter(
                        Metric.id == value,
                        Metric.status == 'active',
                        Metric.creator == r.creator,
                    ).values(resource_id=r.id, name=name)
                    result = session.execute(stmt)
                except exception.DBDuplicateEntry:
                    raise indexer.NamedMetricAlreadyExists(name)
                if result.rowcount == 0:
                    raise indexer.NoSuchMetric(value)
            else:
                unit = value.get('unit')
                ap_name = value['archive_policy_name']
                m = Metric(id=uuid.uuid4(),
                           creator=r.creator,
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
            stmt = update(Metric).filter(
                Metric.resource_id == resource_id).values(status="delete")
            session.execute(stmt)

            stmt = delete(Resource).where(Resource.id == resource_id)
            if session.execute(stmt).rowcount == 0:
                raise indexer.NoSuchResource(resource_id)

    @retry_on_deadlock
    def delete_resources(self, resource_type='generic',
                         attribute_filter=None):
        if not attribute_filter:
            raise ValueError("attribute_filter must be set")

        with self.facade.writer() as session:
            target_cls = self._resource_type_to_mappers(
                session, resource_type)["resource"]

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

            resourceFilter = select(target_cls.id).filter(f)

            stmt = update(Metric).filter(
                Metric.resource_id.in_(resourceFilter)
            ).values(status="delete").execution_options(
                synchronize_session=False)
            session.execute(stmt)

            stmt = delete(target_cls).filter(f).execution_options(
                synchronize_session=False)
            return session.execute(stmt).rowcount

    @retry_on_deadlock
    def get_resource(self, resource_type, resource_id, with_metrics=False):
        with self.facade.independent_reader() as session:
            resource_cls = self._resource_type_to_mappers(
                session, resource_type)['resource']
            q = select(resource_cls).filter(resource_cls.id == resource_id)
            if with_metrics:
                q = q.options(sqlalchemy.orm.joinedload(Resource.metrics))
            return session.scalars(q).first()

    def extracts_filters_for_table(self, attribute_filter,
                                   allowed_keys_for_table=[
                                       'creator', 'started_at', 'ended_at',
                                       'user_id', 'project_id',
                                       'original_resource_id', 'id', 'type']):
        """Extracts the filters for resource history table.

        Extracts the filters that can be used in the resource history table to
        apply in the aggregates query that we execute in the database.
        """

        attribute_filters_to_use = copy.deepcopy(attribute_filter)

        LOG.debug("Executing the processing of attributes filters [%s] for "
                  "resource history table.", attribute_filters_to_use)

        is_value_list = isinstance(attribute_filters_to_use, list)
        is_value_dict = isinstance(attribute_filters_to_use, dict)
        is_value_dict_or_list = (is_value_dict or is_value_list)

        if not is_value_dict_or_list:
            LOG.debug("Attribute filter [%s] is not of expected types [list "
                      "or dict]. Therefore, we do not do anything with it.",
                      attribute_filters_to_use)
            return attribute_filters_to_use

        if is_value_list:
            for attribute in attribute_filter:
                LOG.debug("Sending attribute filter [%s] to be processed, "
                          "as it is part of a list of attribute filters.",
                          attribute)

                value_sanitized = self.extracts_filters_for_table(
                    attribute, allowed_keys_for_table=allowed_keys_for_table)

                if not value_sanitized:
                    LOG.debug("Value [%s] was totally cleaned after being "
                              "sanitized. Therefore, we remove it from our "
                              "attribute filter list.", attribute)
                    attribute_filters_to_use.remove(attribute)
                else:
                    LOG.debug("Replacing value [%s] in list with the sanitized"
                              "value [%s] in its current position.",
                              attribute, value_sanitized)
                    value_index = attribute_filters_to_use.index(attribute)
                    attribute_filters_to_use[value_index] = value_sanitized

        elif is_value_dict:
            all_keys = list(attribute_filter.keys())
            for key in all_keys:
                value = attribute_filter.get(key)

                # The value is a leaf when it is not of type dict of list.
                is_value_leaf = not (isinstance(
                    value, dict) or isinstance(value, list))

                if key not in allowed_keys_for_table and is_value_leaf:
                    attribute_being_remove = attribute_filters_to_use.pop(key)
                    LOG.debug('Removing attribute [%s] with value [%s] from '
                              'attributes [%s] as it is not an expected key '
                              'value [%s].', key, attribute_being_remove,
                              attribute_filter,
                              allowed_keys_for_table)
                else:
                    LOG.debug("Sending attribute [key=%s, value=%s] from "
                              "dictionary to be processed.", key, value)
                    value_sanitized = self.extracts_filters_for_table(
                        value, allowed_keys_for_table=allowed_keys_for_table)

                    is_value_changed = value != value_sanitized
                    if not is_value_changed:
                        LOG.debug("Value [%s] for key [%s] did not changed. "
                                  "Therefore, we go for the next iteration.",
                                  value, key)
                        continue
                    if not value_sanitized:
                        LOG.debug("Value from dict [%s] was totally cleaned after"
                                  " being sanitized. Therefore, we remove it from "
                                  "our attribute filter dictionary.", value)
                        attribute_filters_to_use.pop(key)
                    else:
                        LOG.debug("Replacing attribute [%s] in dict, with "
                                  "sanitized data [%s]. Old value was [%s].",
                                  key, value_sanitized, value)
                        attribute_filters_to_use[key] = value_sanitized
        else:
            LOG.debug("This condition should never happen. Attribute filter [%s] "
                      "is not of expected types [list or dict].",
                      attribute_filters_to_use)
        return attribute_filters_to_use

    def _get_history_result_mapper(self, session, resource_type,
                                   attribute_filter=None):

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
        s1 = select(*history_cols.values())
        s2 = select(*resource_cols.values())
        if resource_type != "generic":
            engine = session.connection()

            # Get the available columns for ResourceHistory table
            resource_history_filters_names = list(map(
                lambda column: column.name, sqlalchemy.inspect(
                    ResourceHistory).columns))

            history_filters = self.extracts_filters_for_table(
                attribute_filter,
                allowed_keys_for_table=resource_history_filters_names)

            LOG.debug("Filters to be used [%s] in query according to resource "
                      "history columns [%s] extracted from [%s].",
                      history_filters, resource_history_filters_names,
                      attribute_filter)

            # Get the available columns for Resource table
            resource_filter_names = list(map(
                lambda column: column.name, sqlalchemy.inspect(
                    Resource).columns))

            resource_filters = self.extracts_filters_for_table(
                attribute_filter, allowed_keys_for_table=resource_filter_names)
            LOG.debug("Filters to be used [%s] in query according to resource "
                      "columns [%s] extracted from [%s].", resource_filters,
                      resource_filter_names, attribute_filter)

            s1 = s1.where(history_cls.revision == ResourceHistory.revision)
            if history_filters:
                f1 = QueryTransformer.build_filter(
                    engine.dialect.name, ResourceHistory, history_filters)
                s1 = s1.filter(f1)
            else:
                LOG.debug("No filters supplied to be applied for the resource "
                          "history table. Attribute filters: [%s].",
                          attribute_filter)

            s2 = s2.where(resource_cls.id == Resource.id)

            if resource_filters:
                f2 = QueryTransformer.build_filter(
                    engine.dialect.name, Resource, resource_filters)
                s2 = s2.filter(f2)
            else:
                LOG.debug("No filters supplied to be applied for the resource "
                          "table. Attribute filters: [%s].", attribute_filter)

        union_stmt = sqlalchemy.union(s1, s2)
        stmt = union_stmt.alias("result")

        class Result(base.ResourceJsonifier, base.GnocchiBase):
            def __iter__(self):
                return iter((key, getattr(self, key)) for key in stmt.c.keys())

        mapper_reg.map_imperatively(
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
                    session, resource_type, attribute_filter)
                unique_keys = ["id", "revision"]
            else:
                target_cls = self._resource_type_to_mappers(
                    session, resource_type)["resource"]
                unique_keys = ["id"]

            q = select(target_cls)

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

            sort_keys, sort_dirs = self._build_sort_keys(sorts, unique_keys)

            if marker:
                marker_q = select(target_cls)
                if history:
                    try:
                        rid, rrev = marker.split("@")
                        rrev = int(rrev)
                    except ValueError:
                        resource_marker = None
                    else:
                        mfilter = marker_q.filter(
                            target_cls.id == rid,
                            target_cls.revision == rrev)

                        resource_marker = session.scalars(mfilter).first()
                else:
                    mfilter = marker_q.filter(target_cls.id == marker)
                    resource_marker = session.scalars(mfilter).first()

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
            q = q.options(sqlalchemy.orm.joinedload(target_cls.metrics))

            LOG.debug("Executing query [%s] to search for resources.", q)
            all_resources = session.scalars(q).unique().all()

            LOG.debug("Resources [quantity=%s] [%s] found with query: [%s].",
                      len(all_resources), all_resources, q)

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

                        q = select(target_cls).filter(f)
                        # Always include metrics
                        q = q.options(sqlalchemy.orm.joinedload(
                            target_cls.metrics))
                        try:
                            all_resources.extend(
                                session.scalars(q).unique().all())
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
            stmt = delete(Metric).where(Metric.id == id)
            if session.execute(stmt).rowcount == 0:
                raise indexer.NoSuchMetric(id)

    def delete_metric(self, id):
        with self.facade.writer() as session:
            stmt = update(Metric).filter(
                Metric.id == id, Metric.status == "active").values(
                    status="delete", resource_id=None)
            if session.execute(stmt).rowcount == 0:
                raise indexer.NoSuchMetric(id)

    def update_needs_raw_data_truncation(self, metrid_id, value=False):
        with self.facade.writer() as session:
            stmt = update(Metric).filter(Metric.id == metrid_id).values(
                needs_raw_data_truncation=value)
            if session.execute(stmt).rowcount == 0:
                raise indexer.NoSuchMetric(metrid_id)

    def update_last_measure_timestamp(self, metrid_id):
        with self.facade.writer() as session:
            stmt = update(Metric).filter(Metric.id == metrid_id).values(
                last_measure_timestamp=datetime.datetime.utcnow())
            if session.execute(stmt).rowcount == 0:
                raise indexer.NoSuchMetric(metrid_id)

    def update_backwindow_changed_for_metrics_archive_policy(
            self, archive_policy_name):
        with self.facade.writer() as session:
            stmt = update(Metric).filter(
                Metric.archive_policy_name == archive_policy_name).values(
                needs_raw_data_truncation=True)
            if session.execute(stmt).rowcount == 0:
                LOG.info("No metric was updated for archive_policy [%s]. "
                         "This might indicate that the archive policy is not "
                         "used by any metric.", archive_policy_name)

    @staticmethod
    def _build_sort_keys(sorts, unique_keys):
        # transform the api-wg representation to the oslo.db one
        sort_keys = []
        sort_dirs = []
        for sort in sorts:
            sort_key, __, sort_dir = sort.partition(":")
            sort_keys.append(sort_key.strip())
            sort_dirs.append(sort_dir or 'asc')

        # paginate_query require at list one uniq column
        for key in unique_keys:
            if key not in sort_keys:
                sort_keys.append(key)
                sort_dirs.append('asc')

        return sort_keys, sort_dirs


def _operator_in(field_name, value):
    # Do not generate empty IN comparison
    # https://github.com/gnocchixyz/gnocchi/issues/530
    if len(value):
        return field_name.in_(value)


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

        u"in": _operator_in,

        u"like": lambda field, value: field.like(value),
    }

    multiple_operators = {
        u"or": sqlalchemy.or_,
        u"∨": sqlalchemy.or_,

        u"and": sqlalchemy.and_,
        u"∧": sqlalchemy.and_,
    }

    converters = (
        (types.TimestampUTC, utils.to_datetime),
        (sa_types.String, str),
        (sa_types.Integer, int),
        (sa_types.Numeric, float),
    )

    @classmethod
    def _handle_multiple_op(cls, engine, table, op, nodes):
        args = [
            cls.build_filter(engine, table, node)
            for node in nodes
        ]

        if len(args) == 0:
            return op(sqlalchemy.true(), *args)

        return op(*args)

    @classmethod
    def _handle_unary_op(cls, engine, table, op, node):
        return op(cls.build_filter(engine, table, node))

    @classmethod
    def _handle_binary_op(cls, engine, table, op, nodes):
        try:
            field_name, value = list(nodes.items())[0]
        except Exception:
            raise indexer.QueryError()

        if field_name == "lifespan":
            attr = getattr(table, "ended_at") - getattr(table, "started_at")
            value = datetime.timedelta(
                seconds=utils.timespan_total_seconds(
                    utils.to_timespan(value)))
            if engine == "mysql":
                # NOTE(jd) So subtracting 2 timestamps in MySQL result in some
                # weird results based on string comparison. It's useless and it
                # does not work at all with seconds or anything. Just skip it.
                raise exceptions.NotImplementedError
        elif field_name == "created_by_user_id":
            creator = getattr(table, "creator")
            if op == operator.eq:
                return creator.like("%s:%%" % value)
            elif op == operator.ne:
                return sqlalchemy.not_(creator.like("%s:%%" % value))
            elif op == cls.binary_operators[u"like"]:
                return creator.like("%s:%%" % value)
            raise indexer.QueryValueError(value, field_name)
        elif field_name == "created_by_project_id":
            creator = getattr(table, "creator")
            if op == operator.eq:
                return creator.like("%%:%s" % value)
            elif op == operator.ne:
                return sqlalchemy.not_(creator.like("%%:%s" % value))
            elif op == cls.binary_operators[u"like"]:
                return creator.like("%%:%s" % value)
            raise indexer.QueryValueError(value, field_name)
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
                for klass, converter in cls.converters:
                    if isinstance(attr.type, klass):
                        try:
                            if isinstance(value, list):
                                # we got a list for in_ operator
                                value = [converter(v) for v in value]
                            else:
                                value = converter(value)
                        except Exception:
                            raise indexer.QueryValueError(value, field_name)
                        break

        if op == operator.ne and value is not None:
            return operator.or_(operator.eq(attr, None),
                                op(attr, value))
        else:
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
                return cls._handle_unary_op(engine, table, op, nodes)
            return cls._handle_binary_op(engine, table, op, nodes)
        return cls._handle_multiple_op(engine, table, op, nodes)
