# -*- encoding: utf-8 -*-
#
# Copyright © 2014 eNovance
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
import uuid

from oslo.config import cfg
import sqlalchemy
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext import declarative
from sqlalchemy import types

from gnocchi import indexer
from gnocchi.openstack.common.db import exception
from gnocchi.openstack.common.db.sqlalchemy import models
from gnocchi.openstack.common.db.sqlalchemy import session
from gnocchi.openstack.common import timeutils


cfg.CONF.import_opt('connection', 'gnocchi.openstack.common.db.options',
                    group='database')


Base = declarative.declarative_base()


_marker = indexer._marker


class GUID(types.TypeDecorator):
    """Platform-independent GUID type.

    Uses Postgresql's UUID type, otherwise uses
    CHAR(32), storing as stringified hex values.

    """
    impl = types.CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(postgresql.UUID())
        return dialect.type_descriptor(types.CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        if not isinstance(value, uuid.UUID):
            return "%.32x" % uuid.UUID(value)
        # hexstring
        return "%.32x" % value

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        return uuid.UUID(value)


class ResourceEntity(Base, models.ModelBase):
    __tablename__ = 'resource_entity'

    resource_id = sqlalchemy.Column(GUID,
                                    sqlalchemy.ForeignKey('resource.id',
                                                          ondelete="CASCADE"),
                                    primary_key=True)
    entity_id = sqlalchemy.Column(GUID,
                                  sqlalchemy.ForeignKey('entity.id',
                                                        ondelete="CASCADE"),
                                  primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    resources = sqlalchemy.orm.relationship(
        'Resource')


class Entity(Base, models.ModelBase):
    __tablename__ = 'entity'

    id = sqlalchemy.Column(GUID, primary_key=True)


class Resource(Base, models.ModelBase):
    __tablename__ = 'resource'

    id = sqlalchemy.Column(GUID, primary_key=True)
    user_id = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    project_id = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    started_at = sqlalchemy.Column(sqlalchemy.DateTime, nullable=False,
                                   default=sqlalchemy.func.now())
    ended_at = sqlalchemy.Column(sqlalchemy.DateTime)
    entities = sqlalchemy.orm.relationship(
        ResourceEntity)


class Instance(Resource):
    __tablename__ = 'instance'

    id = sqlalchemy.Column(GUID, sqlalchemy.ForeignKey('resource.id'),
                           primary_key=True)

    flavor_id = sqlalchemy.Column(sqlalchemy.Integer, nullable=False)
    image_ref = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    host = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    display_name = sqlalchemy.Column(sqlalchemy.String, nullable=False)
    architecture = sqlalchemy.Column(sqlalchemy.String, nullable=False)


class SQLAlchemyIndexer(indexer.IndexerDriver):
    # TODO(jd) Use stevedore instead to allow extending?
    _RESOURCE_CLASS_MAPPER = {
        'generic': Resource,
        'instance': Instance,
    }

    def __init__(self, conf):
        self.engine_facade = session.EngineFacade.from_config(
            conf.database.connection, conf)

    def upgrade(self):
        engine = self.engine_facade.get_engine()
        Base.metadata.create_all(engine)

    def create_resource(self, resource_type, uuid, user_id, project_id,
                        started_at=None, ended_at=None, entities=None,
                        **kwargs):
        if resource_type not in self._RESOURCE_CLASS_MAPPER:
            raise indexer.UnknownResourceType(resource_type)
        # Convert to UTC because we store in UTC :(
        if started_at is not None:
            started_at = timeutils.normalize_time(started_at)
        if ended_at is not None:
            ended_at = timeutils.normalize_time(ended_at)
        if started_at is not None \
           and ended_at is not None \
           and started_at > ended_at:
            raise ValueError("Start timestamp cannot be after end timestamp")
        r = self._RESOURCE_CLASS_MAPPER[resource_type](
            id=uuid,
            user_id=user_id,
            project_id=project_id,
            started_at=started_at,
            ended_at=ended_at,
            **kwargs)
        session = self.engine_facade.get_session()
        with session.begin():
            session.add(r)
            if entities is None:
                entities = {}
            for name, e in entities.iteritems():
                session.add(ResourceEntity(resource_id=r.id,
                                           entity_id=e,
                                           name=name))
            try:
                session.flush()
            except exception.DBError as e:
                # TODO(jd) Add an exception in oslo.db to match foreign key
                # issues
                if isinstance(e.inner_exception,
                              sqlalchemy.exc.IntegrityError):
                    raise indexer.NoSuchEntity("???")

        return self._resource_to_dict(r)

    @staticmethod
    def _resource_to_dict(resource):
        r = dict(resource)
        r['id'] = str(resource.id)
        r['entities'] = dict((k.name, str(k.entity_id))
                             for k in resource.entities)
        return r

    def update_resource(self, uuid, ended_at=_marker, entities=_marker):
        session = self.engine_facade.get_session()
        with session.begin():
            q = session.query(
                Resource).filter(
                    Resource.id == uuid).with_for_update()
            r = q.first()
            if r is None:
                raise indexer.NoSuchResource(uuid)

            if ended_at is not _marker:
                # NOTE(jd) Could be better to have check in the db for that so
                # we can just run the UPDATE
                if r.started_at is not None \
                   and ended_at is not None:
                    # Convert to UTC because we store in UTC :(
                    ended_at = timeutils.normalize_time(ended_at)
                    if r.started_at > ended_at:
                        raise ValueError(
                            "Start timestamp cannot be after end timestamp")
                r.ended_at = ended_at
                session.flush()
            if entities is not _marker:
                session.query(ResourceEntity).filter(
                    ResourceEntity.resource_id == uuid).delete()
                for name, e in entities.iteritems():
                    session.add(ResourceEntity(resource_id=uuid,
                                               entity_id=e,
                                               name=name))
                    try:
                        session.flush()
                    except exception.DBError as e:
                        # TODO(jd) Add an exception in oslo.db to match
                        # foreign key issues
                        if isinstance(e.inner_exception,
                                      sqlalchemy.exc.IntegrityError):
                            # FIXME(jd) This could also be a non existent
                            # resource!
                            raise indexer.NoSuchEntity("???")

        return self._resource_to_dict(r)

    def delete_resource(self, id):
        session = self.engine_facade.get_session()
        if session.query(Resource).filter(Resource.id == id).delete() == 0:
            raise indexer.NoSuchResource(id)

    def get_resource(self, uuid):
        session = self.engine_facade.get_session()
        q = session.query(
            Resource).filter(
                Resource.id == uuid)
        r = q.first()
        if r:
            return self._resource_to_dict(r)

    def create_entity(self, id):
        session = self.engine_facade.get_session()
        session.add(Entity(id=id))
        try:
            session.flush()
        except exception.DBDuplicateEntry:
            raise indexer.EntityAlreadyExists(id)

    def delete_entity(self, id):
        session = self.engine_facade.get_session()
        session.query(Entity).filter(Entity.id == id).delete()
        session.flush()
