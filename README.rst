===============================
 Gnocchi - Metric as a Service
===============================

.. image:: https://badge.fury.io/py/gnocchi.svg
    :target: https://badge.fury.io/py/gnocchi

.. image:: doc/source/_static/gnocchi-logo.png

Gnocchi is an open-source time series database.

The problem that Gnocchi solves is the storage and indexing of time series
data and resources at a large scale. This is useful in modern cloud platforms
which are not only huge but also are dynamic and potentially multi-tenant.
Gnocchi takes all of that into account.

Gnocchi has been designed to handle large amounts of aggregates being stored
while being performant, scalable and fault-tolerant. While doing this, the goal
was to be sure to not build any hard dependency on any complex storage system.

Gnocchi takes a unique approach to time series storage: rather than storing
raw data points, it aggregates them before storing them. This built-in feature
is different from most other time series databases, which usually support
this mechanism as an option and compute aggregation (average, minimum, etc.) at
query time.

Because Gnocchi computes all the aggregations at ingestion, getting the data
back is extremely fast, as it just needs to read back the pre-computed results.

You can read the full documentation online at http://gnocchi.osci.io.
