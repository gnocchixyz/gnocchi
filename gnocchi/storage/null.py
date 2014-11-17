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
from gnocchi import storage


class NullStorage(storage.StorageDriver):

    def __init__(self, conf):
        self.entities = {}

    def create_entity(self, entity, back_window, archive_policy):
        if entity in self.entities:
            raise storage.EntityAlreadyExists(entity)
        self.entities[entity] = True

    def delete_entity(self, entity):
        try:
            del self.entities[entity]
        except KeyError:
            raise storage.EntityDoesNotExist(entity)
