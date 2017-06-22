# -*- encoding: utf-8 -*-
#
# Copyright © 2017 Red Hat
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
import struct

from gnocchi.common import redis
from gnocchi import notifier


class RedisNotifier(notifier.Notifier):

    _SERIALIZATION_FORMAT_LEN = struct.calcsize("<H")
    DEFAULT_CHANNEL = b"gnocchi-measures"

    def __init__(self, conf):
        super(RedisNotifier, self).__init__(conf)
        self._client, options = redis.get_client(conf.url)
        self.channel = options.get('channel',
                                   [self.DEFAULT_CHANNEL])[-1].decode()

    def __str__(self):
        return "%s: %s channel %s" % (
            self.__class__.__name__, self._client, self.channel
        )

    def iter_on_sacks_to_process(self):
        p = self._client.pubsub()
        p.subscribe(self.channel)
        for message in p.listen():
            if (message['type'] == 'message' and
               message['channel'] == self.channel):
                data = message['data']
                nb_sack = data / self._SERIALIZATION_FORMAT_LEN
                for sack in struct.unpack("<" + "H" * nb_sack):
                    yield sack

    def notify_new_measures_for_sacks(self, sacks):
        self._client.publish(self.channel,
                             struct.pack("<" + "H" * len(sacks), *sacks))
