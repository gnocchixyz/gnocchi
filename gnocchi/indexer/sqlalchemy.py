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


cfg.CONF.import_opt('connection', 'gnocchi.openstack.common.db.options',
                    group='database')


Base = declarative.declarative_base()


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
    entities = sqlalchemy.orm.relationship(
        ResourceEntity)


class SQLAlchemyIndexer(indexer.IndexerDriver):
    def __init__(self, conf):
        self.engine_facade = session.EngineFacade.from_config(
            conf.database.connection, conf)

    def upgrade(self):
        engine = self.engine_facade.get_engine()
        Base.metadata.create_all(engine)

    def create_resource(self, uuid, entities=None):
        session = self.engine_facade.get_session()
        r = Resource(id=uuid)
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
                raise indexer.NoSuchEntity(None)

        return {"id": r.id,
                'entities': entities}

    def update_resource(self, uuid, entities=None):
        session = self.engine_facade.get_session()
        try:
            with session.begin():
                session.query(ResourceEntity).filter(
                    ResourceEntity.resource_id == uuid).delete()
                if entities is None:
                    entities = {}
                for name, e in entities.iteritems():
                    session.add(ResourceEntity(resource_id=uuid,
                                               entity_id=e,
                                               name=name))
        except exception.DBError as e:
            # TODO(jd) Add an exception in oslo.db to match foreign key
            # issues
            if isinstance(e.inner_exception,
                          sqlalchemy.exc.IntegrityError):
                # FIXME(jd) This could also be a non existent resource!
                raise indexer.NoSuchEntity(None)
        return {"id": str(uuid),
                'entities': dict((k, str(v))
                                 for k, v in entities.iteritems())}

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
            return {"id": str(r.id),
                    'entities': dict((e.name, str(e.entity_id))
                                     for e in r.entities)}

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
