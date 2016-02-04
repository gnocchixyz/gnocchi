======================
 Project Architecture
======================

Gnocchi consists of several services: a HTTP REST API (see :doc:`rest`), an
optional statsd-compatible daemon (see :doc:`statsd`), and an asynchronous
processing daemon. Data is received via the HTTP REST API and statsd daemon.
The asynchronous processing daemon, called `gnocchi-metricd`, performs
operations (statistics computing, metric cleanup, etc...) on the received data
in the background.

Both the HTTP REST API and the asynchronous processing daemon are stateless and
are scalable. Additional workers can be added depending on load.


Back-ends
---------

Gnocchi uses two different back-end for storing data: one for storing the time
series (the storage driver) and one for indexing the data (the index driver).

The *storage* is responsible for storing measures of created metrics. It
receives timestamps and values, and pre-computes aggregations according to
the defined archive policies.

The *indexer* is responsible for storing the index of all resources, along with
their types and properties. Gnocchi only knows about resource types from the
OpenStack project, but also provides a *generic* type so you can create basic
resources and handle the resource properties yourself. The indexer is also
responsible for linking resources with metrics.

How to choose back-ends
~~~~~~~~~~~~~~~~~~~~~~~

Gnocchi currently offers 4 storage drivers:

* File
* Swift
* Ceph (preferred)
* InfluxDB (experimental)

The first three drivers are based on an intermediate library, named
*Carbonara*, which handles the time series manipulation, since none of these
storage technologies handle time series natively. `InfluxDB`_ does not need
this layer since it is itself a time series database. However, The InfluxDB
driver is still experimental and suffers from bugs in InfluxDB itself that are
yet to be fixed as of this writing.

The three *Carbonara* based drivers are working well and are as scalable as
their back-end technology permits. Ceph and Swift are inherently more scalable
than the file driver.

Depending on the size of your architecture, using the file driver and storing
your data on a disk might be enough. If you need to scale the number of server
with the file driver, you can export and share the data via NFS among all
Gnocchi processes. In any case, it is obvious that Ceph and Swift drivers are
largely more scalable. Ceph also offers better consistency, and hence is the
recommended driver.

.. _InfluxDB: http://influxdb.com

How to plan for Gnocchi’s storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Gnocchi uses a custom file format based on its library *Carbonara*. In Gnocchi,
a time serie is a collection of points, where a point is a given measure, or
sample, in the lifespan of a time serie. The storage format is compressed using
various techniques, therefore the computing of a time serie's size can be
estimated based on its worst case scenario with the following formula::

    number of points × 9 bytes = size in bytes

The number of points you want to keep is usually determined by the following
formula::

    number of points = timespan ÷ granularity

For example, if you want to keep a year of data with a one minute resolution::

    number of points = (365 days × 24 hours × 60 minutes) ÷ 1 minute
    number of points = 525 600

Then::

    size in bytes = 525 600 × 9 = 4 730 400 bytes = 4 620 KiB

This is just for a single aggregated time serie. If your archive policy uses
the 8 default aggregation methods (mean, min, max, sum, std, median, count,
95pct) with the same "one year, one minute aggregations" resolution, the space
used will go up to a maximum of 8 × 4.5 MiB = 36 MiB.

How to set the archive policy and granularity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In Gnocchi, the archive policy is expressed in number of points. If your
archive policy defines a policy of 10 points with a granularity of 1 second,
the time serie archive will keep up to 10 points, each representing an
aggregation over 1 second. This means the time serie will at maximum retain 10
seconds of data, **but** that does not mean it will be 10 consecutive seconds:
there might be a gap if data is fed irregularly.

Consequently, there is no expiry of data relative to the current timestamp, and
you cannot delete old data points (at least for now).

Therefore, both the archive policy and the granularity entirely depends on your
use case. Depending on the usage of your data, you can define several archiving
policies. A typical low grained use case could be::

    3600 points with a granularity of 1 second = 1 hour
    1440 points with a granularity of 1 minute = 24 hours
    1800 points with a granularity of 1 hour = 30 days
    365 points with a granularity of 1 day = 1 year

This would represent 7205 points × 17.92 = 126 KiB per aggregation method. If
you use the 8 standard aggregation method, your metric will take up to 8 × 126
KiB = 0.98 MiB of disk space.
