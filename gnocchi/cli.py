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


def metricd():
    conf = service.prepare_service()
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
