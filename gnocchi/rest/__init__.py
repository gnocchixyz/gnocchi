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
import datetime
import functools
import json
import uuid

import iso8601
from oslo.utils import strutils
from oslo.utils import timeutils
import pecan
from pecan import rest
import six
import voluptuous
import werkzeug.http

from gnocchi import indexer
from gnocchi import storage


def deserialize(schema):
    try:
        type, options = werkzeug.http.parse_options_header(
            pecan.request.headers.get('Content-Type'))
        params = json.loads(pecan.request.body.decode(
            options.get('charset', 'ascii')))
    except Exception:
        pecan.abort(400, "Unable to decode body")
    try:
        return schema(params)
    except voluptuous.Error as e:
        pecan.abort(400, "Invalid input: %s" % e)


def vexpose(schema, *vargs, **vkwargs):
    def expose(f):
        f = pecan.expose(*vargs, **vkwargs)(f)

        @functools.wraps(f)
        def callfunction(*args, **kwargs):
            return f(*args, body=deserialize(schema), **kwargs)
        return callfunction
    return expose


def Timestamp(v):
    if v is None:
        return v
    try:
        v = float(v)
    except (ValueError, TypeError):
        return timeutils.normalize_time(iso8601.parse_date(v))
    return datetime.datetime.utcfromtimestamp(v)


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
            pecan.abort(404, str(e))
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
                        for k, v
                        in six.iteritems(pecan.request.storage.get_measures(
                            self.entity_id, start, stop, aggregation)))
        except storage.EntityDoesNotExist as e:
            pecan.abort(404, str(e))

    @pecan.expose()
    def delete(self):
        try:
            pecan.request.storage.delete_entity(self.entity_id)
        except storage.EntityDoesNotExist as e:
            pecan.abort(404, str(e))
        pecan.request.indexer.delete_entity(self.entity_id)
        # NOTE(jd) Until https://bugs.launchpad.net/pecan/+bug/1311629 is fixed
        pecan.response.status = 204


EntitySchemaDefinition = {
    voluptuous.Required('archive_policy'):
    voluptuous.Any(*storage.ARCHIVE_POLICIES.keys()),
}


class EntitiesController(rest.RestController):
    @staticmethod
    @pecan.expose()
    def _lookup(id, *remainder):
        return EntityController(id), remainder

    Entity = voluptuous.Schema(EntitySchemaDefinition)

    @staticmethod
    def create_entity(archive_policy, user_id, project_id):
        id = uuid.uuid4()
        pecan.request.storage.create_entity(str(id), archive_policy)
        pecan.request.indexer.create_resource('entity', id,
                                              user_id, project_id,
                                              archive_policy=archive_policy)
        return id

    @vexpose(Entity, 'json')
    def post(self, body):
        # TODO(jd) Use policy to limit what values the user can use as
        # 'archive'?
        # FIXME(jd) Use the real user_id/project_id
        id = self.create_entity(body['archive_policy'], "admin", "admin")
        pecan.response.headers['Location'] = "/v1/entity/" + str(id)
        pecan.response.status = 201
        return {"id": str(id),
                "archive_policy": body['archive_policy']}


class NamedEntityController(rest.RestController):
    def __init__(self, resource_id):
        self.resource_id = resource_id

    @pecan.expose()
    def _lookup(self, name, *remainder):
        # TODO(jd) There might be an slight optimization to do by using a
        # dedicated driver method rather than get_resource, which might be
        # heavier.
        resource = pecan.request.indexer.get_resource(
            'generic', self.resource_id)
        if name in resource['entities']:
            return EntityController(resource['entities'][name]), remainder
        pecan.abort(404)


def UUID(value):
    try:
        return uuid.UUID(value)
    except Exception as e:
        raise ValueError(e)


Entities = voluptuous.Schema({
    six.text_type: voluptuous.Any(UUID,
                                  EntitiesController.Entity),
})


def ResourceSchema(schema):
    base_schema = {
        voluptuous.Required("id"): UUID,
        'started_at': Timestamp,
        'ended_at': Timestamp,
        voluptuous.Required('user_id'): six.text_type,
        voluptuous.Required('project_id'): six.text_type,
        'entities': Entities,
    }
    base_schema.update(schema)
    return voluptuous.Schema(base_schema)


def ResourcePatchSchema(schema):
    base_schema = {
        'entities': Entities,
        'ended_at': Timestamp,
    }
    base_schema.update(schema)
    return voluptuous.Schema(base_schema)


class GenericResourceController(rest.RestController):
    _resource_type = 'generic'

    ResourcePatch = ResourcePatchSchema({})

    def __init__(self, id):
        self.id = id
        self.entity = NamedEntityController(id)

    @staticmethod
    def convert_entity_list(entities, user_id, project_id):
        # Replace None as value for an entity by a brand a new entity
        new_entities = {}
        for k, v in six.iteritems(entities):
            if isinstance(v, uuid.UUID):
                new_entities[k] = v
            else:
                new_entities[k] = str(EntitiesController.create_entity(
                    v['archive_policy'], user_id, project_id))
        return new_entities

    @pecan.expose('json')
    def get(self):
        resource = pecan.request.indexer.get_resource(
            self._resource_type, self.id)
        if resource:
            return resource
        pecan.abort(404)

    @pecan.expose()
    def patch(self):
        if getattr(self, "read_only", False):
            pecan.abort(403, "Unable to patch resource")
        # NOTE(jd) Can't use vexpose because it does not take into account
        # inheritance
        body = deserialize(self.ResourcePatch)
        # NOTE(jd) Until https://bugs.launchpad.net/pecan/+bug/1311629 is fixed
        pecan.response.status = 204
        if len(body) == 0:
            # Empty update, just check if the resource exists
            if pecan.request.indexer.get_resource(
                    self._resource_type, self.id):
                return
            pecan.abort(404)

        try:
            if 'entities' in body:
                # FIXME(jd) Use the real user_id/project_id
                body['entities'] = self.convert_entity_list(
                    body['entities'], "admin", "admin")
            pecan.request.indexer.update_resource(
                self._resource_type,
                self.id, **body)
        except (indexer.NoSuchEntity, ValueError) as e:
            pecan.abort(400, e)
        except indexer.NoSuchResource as e:
            pecan.abort(404, e)

    @pecan.expose()
    def delete(self):
        try:
            pecan.request.indexer.delete_resource(self.id)
        except indexer.NoSuchResource as e:
            pecan.abort(400, str(e))
        # NOTE(jd) Until https://bugs.launchpad.net/pecan/+bug/1311629 is fixed
        pecan.response.status = 204


class InstanceResourceController(GenericResourceController):
    _resource_type = 'instance'

    ResourcePatch = ResourcePatchSchema({
        "flavor_id": int,
        "image_ref": six.text_type,
        "host": six.text_type,
        "display_name": six.text_type,
    })


class EntityResourceController(GenericResourceController):
    _resource_type = 'entity'

    read_only = True


class GenericResourcesController(rest.RestController):
    _resource_type = 'generic'
    _resource_rest_class = GenericResourceController

    Resource = ResourceSchema({})

    @pecan.expose()
    def _lookup(self, id, *remainder):
        return self._resource_rest_class(id), remainder

    @pecan.expose('json')
    def post(self):
        if getattr(self, "read_only", False):
            pecan.abort(403, "Unable to create resource")
        # NOTE(jd) Can't use vexpose because it does not take into account
        # inheritance
        body = deserialize(self.Resource)
        body['entities'] = GenericResourceController.convert_entity_list(
            body.get('entities', {}), body['user_id'], body['project_id'])
        try:
            resource = pecan.request.indexer.create_resource(
                self._resource_type,
                **body)
        except ValueError as e:
            pecan.abort(400, e)
        except indexer.ResourceAlreadyExists as e:
            pecan.abort(409, e)
        pecan.response.headers['Location'] = "/v1/resource/" \
                                             + self._resource_type + "/" \
                                             + resource['id']
        pecan.response.status = 201
        return resource

    @pecan.expose('json')
    def get_all(self, **kwargs):
        started_after = kwargs.pop('started_after', None)
        ended_before = kwargs.pop('ended_before', None)

        type, options = werkzeug.http.parse_options_header(
            pecan.request.headers.get('Accept'))
        try:
            details = strutils.bool_from_string(
                options.get('details', kwargs.pop('details', 'false')),
                strict=True)
        except ValueError as e:
            method = 'Accept' if 'details' in options else 'query'
            pecan.abort(
                400,
                "Unable to parse details value in %s: %s" % (method, str(e)))

        if started_after is not None:
            try:
                started_after = Timestamp(started_after)
            except Exception:
                pecan.abort(400, "Unable to parse started_after timestamp")
        if ended_before is not None:
            try:
                ended_before = Timestamp(ended_before)
            except Exception:
                pecan.abort(400, "Unable to parse ended_before timestamp")
        try:
            return pecan.request.indexer.list_resources(
                self._resource_type,
                started_after=started_after,
                ended_before=ended_before,
                attributes_filter=kwargs,
                details=details)
        except indexer.ResourceAttributeError as e:
            pecan.abort(400, e)


class InstancesResourcesController(GenericResourcesController):
    _resource_type = 'instance'
    _resource_rest_class = InstanceResourceController

    Resource = ResourceSchema({
        voluptuous.Required("flavor_id"): int,
        voluptuous.Required("image_ref"): six.text_type,
        voluptuous.Required("host"): six.text_type,
        voluptuous.Required("display_name"): six.text_type,
    })


class EntitiesResourcesController(GenericResourcesController):
    _resource_type = 'entity'
    _resource_rest_class = EntityResourceController

    read_only = True

    Resource = ResourceSchema(EntitySchemaDefinition)


class ResourcesController(rest.RestController):
    generic = GenericResourcesController()
    entity = EntitiesResourcesController()
    instance = InstancesResourcesController()


class V1Controller(object):
    entity = EntitiesController()
    resource = ResourcesController()


class RootController(object):
    v1 = V1Controller()

    @staticmethod
    @pecan.expose(content_type="text/plain")
    def index():
        return "Nom nom nom."
