# -*- encoding: utf-8 -*-
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

import numbers
import re
import six
import stevedore
import voluptuous

from gnocchi import utils


INVALID_NAMES = [
    "id", "type", "metrics",
    "revision", "revision_start", "revision_end",
    "started_at", "ended_at",
    "user_id", "project_id",
    "created_by_user_id", "created_by_project_id", "get_metric",
    "creator",
]

VALID_CHARS = re.compile("[a-zA-Z0-9][a-zA-Z0-9_]*")


class InvalidResourceAttribute(ValueError):
    pass


class InvalidResourceAttributeName(InvalidResourceAttribute):
    """Error raised when the resource attribute name is invalid."""
    def __init__(self, name):
        super(InvalidResourceAttributeName, self).__init__(
            "Resource attribute name %s is invalid" % str(name))
        self.name = name


class InvalidResourceAttributeValue(InvalidResourceAttribute):
    """Error raised when the resource attribute min is greater than max"""
    def __init__(self, min, max):
        super(InvalidResourceAttributeValue, self).__init__(
            "Resource attribute value min (or min_length) %s must be less  "
            "than or equal to max (or max_length) %s!" % (str(min), str(max)))
        self.min = min
        self.max = max


class InvalidResourceAttributeOption(InvalidResourceAttribute):
    """Error raised when the resource attribute name is invalid."""
    def __init__(self, name, option, reason):
        super(InvalidResourceAttributeOption, self).__init__(
            "Option '%s' of resource attribute %s is invalid: %s" %
            (option, str(name), str(reason)))
        self.name = name
        self.option = option
        self.reason = reason


# NOTE(sileht): This is to store the behavior of some operations:
#  * fill, to set a default value to all existing resource type
#
# in the future for example, we can allow to change the length of
# a string attribute, if the new one is shorter, we can add a option
# to define the behavior like:
#  * resize = trunc or reject
OperationOptions = {
    voluptuous.Optional('fill'): object
}


class CommonAttributeSchema(object):
    meta_schema_ext = {}
    schema_ext = None

    def __init__(self, type, name, required, options=None):
        if (len(name) > 63 or name in INVALID_NAMES
                or not VALID_CHARS.match(name)):
            raise InvalidResourceAttributeName(name)

        self.name = name
        self.required = required
        self.fill = None

        # options is set only when we update a resource type
        if options is not None:
            fill = options.get("fill")
            if fill is None and required:
                raise InvalidResourceAttributeOption(
                    name, "fill", "must not be empty if required=True")
            elif fill is not None:
                # Ensure fill have the correct attribute type
                try:
                    self.fill = voluptuous.Schema(self.schema_ext)(fill)
                except voluptuous.Error as e:
                    raise InvalidResourceAttributeOption(name, "fill", e)

    @classmethod
    def meta_schema(cls, for_update=False):
        d = {
            voluptuous.Required('type'): cls.typename,
            voluptuous.Required('required', default=True): bool
        }
        if for_update:
            d[voluptuous.Required('options', default={})] = OperationOptions
        if callable(cls.meta_schema_ext):
            d.update(cls.meta_schema_ext())
        else:
            d.update(cls.meta_schema_ext)
        return d

    def schema(self):
        if self.required:
            return {self.name: self.schema_ext}
        else:
            return {voluptuous.Optional(self.name): self.schema_ext}

    def jsonify(self):
        return {"type": self.typename,
                "required": self.required}


class StringSchema(CommonAttributeSchema):
    typename = "string"

    def __init__(self, min_length, max_length, *args, **kwargs):
        if min_length > max_length:
            raise InvalidResourceAttributeValue(min_length, max_length)

        self.min_length = min_length
        self.max_length = max_length
        super(StringSchema, self).__init__(*args, **kwargs)

    meta_schema_ext = {
        voluptuous.Required('min_length', default=0):
        voluptuous.All(int, voluptuous.Range(min=0, max=255)),
        voluptuous.Required('max_length', default=255):
        voluptuous.All(int, voluptuous.Range(min=1, max=255))
    }

    @property
    def schema_ext(self):
        return voluptuous.All(six.text_type,
                              voluptuous.Length(
                                  min=self.min_length,
                                  max=self.max_length))

    def jsonify(self):
        d = super(StringSchema, self).jsonify()
        d.update({"max_length": self.max_length,
                  "min_length": self.min_length})
        return d


class UUIDSchema(CommonAttributeSchema):
    typename = "uuid"

    @staticmethod
    def schema_ext(value):
        try:
            return utils.UUID(value)
        except ValueError as e:
            raise voluptuous.Invalid(e)


class DatetimeSchema(CommonAttributeSchema):
    typename = "datetime"

    @staticmethod
    def schema_ext(value):
        try:
            return utils.to_datetime(value)
        except ValueError as e:
            raise voluptuous.Invalid(e)


class NumberSchema(CommonAttributeSchema):
    typename = "number"

    def __init__(self, min, max, *args, **kwargs):
        if max is not None and min is not None and min > max:
            raise InvalidResourceAttributeValue(min, max)
        self.min = min
        self.max = max
        super(NumberSchema, self).__init__(*args, **kwargs)

    meta_schema_ext = {
        voluptuous.Required('min', default=None): voluptuous.Any(
            None, numbers.Real),
        voluptuous.Required('max', default=None): voluptuous.Any(
            None, numbers.Real)
    }

    @property
    def schema_ext(self):
        return voluptuous.All(numbers.Real,
                              voluptuous.Range(min=self.min,
                                               max=self.max))

    def jsonify(self):
        d = super(NumberSchema, self).jsonify()
        d.update({"min": self.min, "max": self.max})
        return d


class BoolSchema(CommonAttributeSchema):
    typename = "bool"
    schema_ext = bool


class ResourceTypeAttributes(list):
    def jsonify(self):
        d = {}
        for attr in self:
            d[attr.name] = attr.jsonify()
        return d


class ResourceTypeSchemaManager(stevedore.ExtensionManager):
    def __init__(self, *args, **kwargs):
        super(ResourceTypeSchemaManager, self).__init__(*args, **kwargs)
        type_schemas = tuple([ext.plugin.meta_schema()
                              for ext in self.extensions])
        self._schema = voluptuous.Schema({
            "name": six.text_type,
            voluptuous.Required("attributes", default={}): {
                six.text_type: voluptuous.Any(*tuple(type_schemas))
            }
        })

        type_schemas = tuple([ext.plugin.meta_schema(for_update=True)
                              for ext in self.extensions])
        self._schema_for_update = voluptuous.Schema({
            "name": six.text_type,
            voluptuous.Required("attributes", default={}): {
                six.text_type: voluptuous.Any(*tuple(type_schemas))
            }
        })

    def __call__(self, definition):
        return self._schema(definition)

    def for_update(self, definition):
        return self._schema_for_update(definition)

    def attributes_from_dict(self, attributes):
        return ResourceTypeAttributes(
            self[attr["type"]].plugin(name=name, **attr)
            for name, attr in attributes.items())

    def resource_type_from_dict(self, name, attributes, state):
        return ResourceType(name, self.attributes_from_dict(attributes), state)


class ResourceType(object):
    def __init__(self, name, attributes, state):
        self.name = name
        self.attributes = attributes
        self.state = state

    @property
    def schema(self):
        schema = {}
        for attr in self.attributes:
            schema.update(attr.schema())
        return schema

    def __eq__(self, other):
        return self.name == other.name

    def jsonify(self):
        return {"name": self.name,
                "attributes": self.attributes.jsonify(),
                "state": self.state}
