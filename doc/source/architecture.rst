======================
 Project Architecture
======================

Gnocchi is built around 2 main components: a storage driver and an indexer
driver. The REST API exposed to the user manipulates both these drivers to
provide all the features that are needed to provide correct infrastructure
measurement.

The *storage* is responsible for storing measures of created metrics. It
receives timestamps and values and computes aggregations according to the
defined archive policies.

The *indexer* is responsible for storing the index of all resources, along with
their types and their properties. Gnocchi only knows resource types from the
OpenStack project, but also provides a *generic* type so you can create basic
resources and handle the resource properties yourself. The indexer is also
responsible for linking resources with metrics.
