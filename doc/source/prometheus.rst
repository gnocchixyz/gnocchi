====================
 Prometheus support
====================

`Prometheus`_ can use Gnocchi to store its data through `Remote Write
Adapter`_. Gnocchi needs to be installed with the `prometheus` flavor.

Example of Prometheus configuration::

  remote_write:
  - url: "http://localhost:8041/v1/prometheus/write"
    basic_auth:
      username: "admin"
      password: "whatever"


The `/v1/prometheus/write` endpoint handles the `WriteRequest` protobuf
message.

Gnocchi maps Prometheus metrics to its data model.

For each metric sent by Prometheus, Gnocchi maintains a corresponding resource
based on each `job` and `instance` pair. This resource is created with the
`prometheus` resource type and contains two attributes, `job` and `instance`.
The metrics sent by Prometheus with this pair are attached to that resource and
filled with the provided measures.

.. _`Prometheus`: https://prometheus.io/
.. _`Remote Write Adapter`: https://prometheus.io/docs/operating/configuration/#<remote_write>
