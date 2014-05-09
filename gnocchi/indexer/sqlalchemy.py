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
import operator
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


ResourceEntity = sqlalchemy.Table(
    'resource_entity',
    Base.metadata,
    sqlalchemy.Column('resource', GUID,
                      sqlalchemy.ForeignKey('resource.id',
                                            ondelete="CASCADE")),
    sqlalchemy.Column('entity', GUID,
                      sqlalchemy.ForeignKey('entity.name',
                                            ondelete="CASCADE"))
)


class Entity(Base, models.ModelBase):
    __tablename__ = 'entity'

    name = sqlalchemy.Column(GUID, primary_key=True)


class Resource(Base, models.ModelBase):
    __tablename__ = 'resource'

    id = sqlalchemy.Column(GUID, primary_key=True)
    entities = sqlalchemy.orm.relationship(
        'Entity',
        backref='resources',
        secondary=ResourceEntity)


class SQLAlchemyIndexer(indexer.IndexerDriver):
    def __init__(self, conf):
        self.engine_facade = session.EngineFacade.from_config(
            conf.database.connection, conf)

    def upgrade(self):
        engine = self.engine_facade.get_engine()
        Base.metadata.create_all(engine)

    def create_resource(self, uuid, entities=[]):
        session = self.engine_facade.get_session()
        with session.begin():
            # FIXME(jd) Seriously, THERE IS NOT NEED TO DO THAT. But someone
            # sucks, either me or the ORM. Please fix that so there's no
            # need to select before inserting FFS. What needs to be done is
            # an INSERT in resources and then an INSERT into ResourceEntity;
            # that last one should fails if the entity does not exist, so we
            # just have to raise back to the caller! I offer a pack of beer
            # to whoever fix that.
            loaded_entities = []
            for e in entities:
                entity = session.query(Entity).filter(Entity.name == e).first()
                if not entity:
                    raise indexer.NoSuchEntity(e)
                loaded_entities.append(entity)
            r = Resource(id=uuid, entities=loaded_entities)
            session.add(r)
        return {"id": r['id'],
                'entities': map(operator.attrgetter('name'),
                                r['entities'])}

    def get_resource(self, uuid):
        session = self.engine_facade.get_session()
        with session.begin():
            q = session.query(Resource).filter(Resource.id == uuid)
            r = q.first()
            return {"id": r['id'],
                    'entities': map(operator.attrgetter('name'),
                                    r['entities'])}

    def create_entity(self, name):
        session = self.engine_facade.get_session()
        try:
            with session.begin():
                session.add(Entity(name=name))
        except exception.DBDuplicateEntry:
            raise indexer.EntityAlreadyExists(name)

    def delete_entity(self, name):
        session = self.engine_facade.get_session()
        with session.begin():
            session.query(Entity).filter(Entity.name == name).delete()
