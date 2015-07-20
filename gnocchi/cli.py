# Copyright (c) 2013 Mirantis Inc.
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
try:
    import asyncio
except ImportError:
    import trollius as asyncio
import logging
import multiprocessing
import signal
import sys
import time

from gnocchi import indexer
from gnocchi.indexer import sqlalchemy as sql_db
from gnocchi.rest import app
from gnocchi import service
from gnocchi import statsd as statsd_service
from gnocchi import storage


LOG = logging.getLogger(__name__)


def storage_dbsync():
    conf = service.prepare_service()
    indexer = sql_db.SQLAlchemyIndexer(conf)
    indexer.connect()
    indexer.upgrade()


def api():
    app.build_server()


def statsd():
    statsd_service.start()


def _metricd(conf, cpu_number):
    # Sleep a bit just not to start and poll everything at the same time.
    time.sleep(cpu_number)
    s = storage.get_driver(conf)
    i = indexer.get_driver(conf)
    i.connect()
    loop = asyncio.get_event_loop()

    def process():
        loop.call_later(conf.storage.metric_processing_delay, process)
        LOG.debug("Processing new measures")
        s.process_measures(i)

    process()
    loop.run_forever()


def _wrap_metricd(cpu_number):
    """Small wrapper for _metricd() that ensure it ALWAYS return.

    Otherwise multiprocessing.Pool is stuck for ever.
    """
    try:
        return _metricd(service.prepare_service(), cpu_number)
    finally:
        return


def metricd():
    conf = service.prepare_service()
    p = multiprocessing.Pool(conf.metricd.workers)

    signal.signal(signal.SIGTERM, lambda signum, frame: sys.exit(0))

    p.map_async(_wrap_metricd, range(conf.metricd.workers))
    signal.pause()
