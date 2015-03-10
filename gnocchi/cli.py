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
from gnocchi.indexer import sqlalchemy as sql_db
from gnocchi.rest import app
from gnocchi import service
from gnocchi import statsd as statsd_service


def storage_dbsync():
    conf = service.prepare_service()
    indexer = sql_db.SQLAlchemyIndexer(conf)
    indexer.connect()
    indexer.upgrade()


def api():
    app.build_server()


def statsd():
    statsd_service.start()
