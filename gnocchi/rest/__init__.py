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
import functools
import uuid

import iso8601
import pecan
from pecan import rest
import six
import voluptuous

from gnocchi import indexer
from gnocchi.openstack.common import jsonutils
from gnocchi.openstack.common import timeutils
from gnocchi import storage


def vexpose(schema, *vargs, **vkwargs):
    def expose(f):
        f = pecan.expose(*vargs, **vkwargs)(f)

        @functools.wraps(f)
        def callfunction(*args, **kwargs):
            params = jsonutils.loads(pecan.request.body)
            try:
                params = schema(params)
            except voluptuous.Error as e:
                pecan.abort(400, "Invalid input: %s" % e)
            return f(*args, body=params, **kwargs)
        return callfunction
    return expose


def Timestamp(v):
    # TODO(jd) Support Unix timestamp?
    return iso8601.parse_date(v)


class EntityController(rest.RestController):
    _custom_actions = {
        'measures': ['POST', 'GET']
    }

    def __init__(self, entity_id):
        self.entity_id = entity_id

    Measures = voluptuous.Schema([{
        voluptuous.Required("timestamp"):
        Timestamp,
        voluptuous.Required("value"): voluptuous.Any(float, int),
    }])

    @vexpose(Measures)
    def post_measures(self, body):
        try:
            pecan.request.storage.add_measures(
                self.entity_id,
                (storage.Measure(
                    m['timestamp'],
                    m['value']) for m in body))
        except storage.EntityDoesNotExist as e:
            pecan.abort(400, str(e))
        # NOTE(jd) Until https://bugs.launchpad.net/pecan/+bug/1311629 is fixed
        pecan.response.status = 204

    @pecan.expose('json')
    def get_measures(self, start=None, stop=None, aggregation='mean'):
        if aggregation not in storage.AGGREGATION_TYPES:
            pecan.abort(400, "Invalid aggregation value %s, must be one of %s"
                        % (aggregation, str(storage.AGGREGATION_TYPES)))

        try:
            # Replace timestamp keys by their string versions
            return dict((timeutils.strtime(k), v)
                        for k, v in pecan.request.storage.get_measures(
                            self.entity_id,
                            start, stop, aggregation).iteritems())
        except storage.EntityDoesNotExist as e:
            pecan.abort(400, str(e))

    @pecan.expose()
    def delete(self):
        try:
            pecan.request.storage.delete_entity(self.entity_id)
        except storage.EntityDoesNotExist as e:
            pecan.abort(400, str(e))
        pecan.request.indexer.delete_entity(self.entity_id)
        # NOTE(jd) Until https://bugs.launchpad.net/pecan/+bug/1311629 is fixed
        pecan.response.status = 204


class EntitiesController(rest.RestController):
    @staticmethod
    @pecan.expose()
    def _lookup(id, *remainder):
        return EntityController(id), remainder

    Entity = voluptuous.Schema({
        voluptuous.Required('archives'):
        voluptuous.All([voluptuous.All([int],
                                       voluptuous.Length(min=2, max=2))],
                       voluptuous.Length(min=1))
    })

    @staticmethod
    def create_entity(archives):
        id = uuid.uuid4()
        pecan.request.storage.create_entity(str(id), archives)
        pecan.request.indexer.create_entity(id)
        return id

    @vexpose(Entity, 'json')
    def post(self, body):
        # TODO(jd) Use policy to limit what values the user can use as
        # 'archive'?
        # TODO(jd) Use a better format than (seconds,number of metric)
        id = self.create_entity(body['archives'])
        pecan.response.headers['Location'] = "/v1/entity/" + str(id)
        pecan.response.status = 201
        return {"id": str(id),
                "archives": body['archives']}


def UUID(value):
    try:
        return uuid.UUID(value)
    except Exception as e:
        raise ValueError(e)


class ResourceController(rest.RestController):
    Entities = voluptuous.Schema({
        six.text_type: voluptuous.Any(UUID,
                                      EntitiesController.Entity),
    })

    Resource = voluptuous.Schema({
        voluptuous.Required("id"): UUID,
        'started_at': Timestamp,
        'ended_at': Timestamp,
        voluptuous.Required('user_id'): six.text_type,
        voluptuous.Required('project_id'): six.text_type,
        'entities': Entities,
    })

    def __init__(self, id):
        self.id = id

    @staticmethod
    def convert_entity_list(entities):
        # Replace None as value for an entity by a brand a new entity
        new_entities = {}
        for k, v in entities.iteritems():
            if isinstance(v, uuid.UUID):
                new_entities[k] = v
            else:
                new_entities[k] = str(EntitiesController.create_entity(
                    v['archives']))
        return new_entities

    @pecan.expose('json')
    def get(self):
        resource = pecan.request.indexer.get_resource(self.id)
        if resource:
            return resource
        pecan.abort(404)

    ResourcePatch = voluptuous.Schema({
        'entities': Entities,
        'ended_at': Timestamp,
    })

    @vexpose(ResourcePatch)
    def patch(self, body):
        # NOTE(jd) Until https://bugs.launchpad.net/pecan/+bug/1311629 is fixed
        pecan.response.status = 204
        if len(body) == 0:
            # Empty update, just check if the resource exists
            if pecan.request.indexer.get_resource(self.id):
                return
            pecan.abort(404)

        try:
            kwargs = {}
            if 'ended_at' in body:
                kwargs['ended_at'] = body['ended_at']
            if 'entities' in body:
                kwargs['entities'] = self.convert_entity_list(
                    body['entities'])
            pecan.request.indexer.update_resource(
                self.id, **kwargs)
        except (indexer.NoSuchEntity, indexer.NoSuchResource, ValueError) as e:
            pecan.abort(400, e)

    @pecan.expose()
    def delete(self):
        try:
            pecan.request.indexer.delete_resource(self.id)
        except indexer.NoSuchResource as e:
            pecan.abort(400, str(e))
        # NOTE(jd) Until https://bugs.launchpad.net/pecan/+bug/1311629 is fixed
        pecan.response.status = 204


class ResourcesController(rest.RestController):
    @staticmethod
    @pecan.expose()
    def _lookup(id, *remainder):
        return ResourceController(id), remainder

    @staticmethod
    @vexpose(ResourceController.Resource, 'json')
    def post(body):
        _id = body['id']
        entities = ResourceController.convert_entity_list(
            body.get('entities', {}))
        try:
            resource = pecan.request.indexer.create_resource(
                _id,
                body['user_id'], body['project_id'],
                body.get('started_at'), body.get('ended_at'),
                entities=entities)
        except ValueError as e:
            pecan.abort(400, str(e))
        pecan.response.headers['Location'] = "/v1/resource/" + str(_id)
        pecan.response.status = 201
        return resource


class V1Controller(object):
    entity = EntitiesController()
    resource = ResourcesController()


class RootController(object):
    v1 = V1Controller()

    @staticmethod
    @pecan.expose(content_type="text/plain")
    def index():
        return "Nom nom nom."
