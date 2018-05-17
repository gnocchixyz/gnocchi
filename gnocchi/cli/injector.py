# -*- encoding: utf-8 -*-
#
# Copyright (c) 2018 Red Hat
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
import time
import uuid

import daiquiri
import numpy
from oslo_config import cfg

from gnocchi import chef
from gnocchi import incoming
from gnocchi import service
from gnocchi import utils

LOG = daiquiri.getLogger(__name__)


def injector():
    conf = cfg.ConfigOpts()
    conf.register_cli_opts([
        cfg.IntOpt("--measures",
                   help="Measures per metric."),
        cfg.IntOpt("--metrics",
                   help="Number of metrics to create."),
        cfg.IntOpt("--archive-policy-name",
                   help="Name of archive policy to use.",
                   default="low"),
        cfg.IntOpt("--interval",
                   help="Interval to sleep between metrics sending."),
        cfg.BoolOpt("--process", default=False,
                    help="Process the ingested measures."),
    ])
    return _inject(service.prepare_service(conf=conf, log_to_std=True),
                   metrics=conf.metrics,
                   measures=conf.measures,
                   archive_policy_name=conf.archive_policy_name,
                   process=conf.process,
                   interval=conf.interval)


def _inject(inc, coord, store, idx,
            metrics, measures, archive_policy_name="low", process=False,
            interval=None):
    LOG.info("Creating %d metrics", metrics)
    with utils.StopWatch() as sw:
        metric_ids = [
            idx.create_metric(uuid.uuid4(), "admin",
                              archive_policy_name).id
            for _ in range(metrics)
        ]
    LOG.info("Created %d metrics in %.2fs", metrics, sw.elapsed())

    LOG.info("Generating %d measures per metric for %d metrics… ",
             measures, metrics)
    now = numpy.datetime64(utils.utcnow())
    with utils.StopWatch() as sw:
        measures = {
            m_id: [incoming.Measure(
                now + numpy.timedelta64(seconds=s),
                random.randint(-999999, 999999)) for s in range(measures)]
            for m_id in metric_ids
        }
    LOG.info("… done in %.2fs", sw.elapsed())

    interval_timer = utils.StopWatch().start()

    while True:
        interval_timer.reset()
        with utils.StopWatch() as sw:
            inc.add_measures_batch(measures)
        total_measures = sum(map(len, measures.values()))
        LOG.info("Pushed %d measures in %.2fs",
                 total_measures,
                 sw.elapsed())

        if process:
            c = chef.Chef(coord, inc, idx, store)

            with utils.StopWatch() as sw:
                for s in inc.iter_sacks():
                    c.process_new_measures_for_sack(s, blocking=True)
            LOG.info("Processed %d sacks in %.2fs",
                     inc.NUM_SACKS, sw.elapsed())
            LOG.info("Speed: %.2f measures/s",
                     float(total_measures) / sw.elapsed())

        if interval is None:
            break
        time.sleep(max(0, interval - interval_timer.elapsed()))

    return total_measures
