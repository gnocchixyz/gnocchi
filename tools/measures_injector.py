#!/usr/bin/env python
# Copyright (c) 2016 Red Hat
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import random
import uuid

from concurrent import futures
from oslo_config import cfg
import six

from gnocchi import indexer
from gnocchi import service
from gnocchi import storage
from gnocchi import utils


def injector():
    conf = cfg.ConfigOpts()
    conf.register_cli_opts([
        cfg.IntOpt("metrics", default=1, min=1),
        cfg.StrOpt("archive-policy-name", default="low"),
        cfg.StrOpt("creator", default="admin"),
        cfg.IntOpt("batch-of-measures", default=1000),
        cfg.IntOpt("measures-per-batch", default=10),
    ])
    conf = service.prepare_service(conf=conf)
    index = indexer.get_driver(conf)
    index.connect()
    s = storage.get_driver(conf)

    def todo():
        metric = index.create_metric(
            uuid.uuid4(),
            creator=conf.creator,
            archive_policy_name=conf.archive_policy_name)

        for _ in six.moves.range(conf.batch_of_measures):
            measures = [
                storage.Measure(
                    utils.dt_in_unix_ns(utils.utcnow()), random.random())
                for __ in six.moves.range(conf.measures_per_batch)]
            s.incoming.add_measures(metric, measures)

    with futures.ThreadPoolExecutor(max_workers=conf.metrics) as executor:
        for m in six.moves.range(conf.metrics):
            executor.submit(todo)


if __name__ == '__main__':
    injector()
