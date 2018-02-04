============================
 InfluxDB ingestion support
============================

Gnocchi implements some part of the InfluxDB REST API. That allows tool that
are used to write to InfluxDB to write directly to Gnocchi instead, such as
`Telegraf`_.

The endpoint is available at `/v1/influxdb`. It supports:

* `GET /v1/influxdb/ping`
* `POST /v1/influxdb/query` where the only query that is handled is `CREATE
  DATABASE <db>`. That will create a new resource type named after the database
  handle.
* `POST /v1/influxdb/write?db=<db>`. The `db` parameter should be an existing
  resource type that does not require any attributes to be set. The body should
  follow the `InfluxDB line protocol`_.

In order to map InfluxDB data to Gnocchi data model, the following
transformation happen when writing metrics:

* For each measure sent, one of the tag value is used as the original resource
  id. By default the `host` tag is used. This can be overriden by passing the
  `X-Gnocchi-InfluxDB-Tag-Resource-ID` HTTP header.

* The metric names associated to the resource have the format:
  `<measurement>.<field_key>[@<tag_key>=<tag_value>,…]`. The tag are sorted
  by keys.


Telegraf configuration
======================

In order to use `Telegraf`_ with Gnocchi, you can use the following
configuration example::

  [[outputs.influxdb]]
    urls = ["http://admin:localhost:8041/v1/influxdb"]
    http_headers = {"X-Gnocchi-InfluxDB-Tag-Resource-ID" = "host"}


Gnocchi configuration
=====================

The default Gnocchi API server does not support the chunked encoding required
by the InfluxDB compatible endpoint. To enable chunked encoding, you must put a
real HTTP Server (Apache/NGINX/...) on front of Gnocchi API, and set
`[api]/uwsgi_mode = http-socket`.


.. _`Telegraf`: https://github.com/influxdata/telegraf
.. _`InfluxDB line protocol`: https://docs.influxdata.com/influxdb/v1.3/write_protocols/line_protocol_reference/
