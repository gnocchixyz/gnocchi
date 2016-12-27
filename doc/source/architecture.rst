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

Gnocchi currently offers different storage drivers:

* File
* Swift
* Ceph (preferred)

The drivers are based on an intermediate library, named *Carbonara*, which
handles the time series manipulation, since none of these storage technologies
handle time series natively.

The three *Carbonara* based drivers are working well and are as scalable as
their back-end technology permits. Ceph and Swift are inherently more scalable
than the file driver.

Depending on the size of your architecture, using the file driver and storing
your data on a disk might be enough. If you need to scale the number of server
with the file driver, you can export and share the data via NFS among all
Gnocchi processes. In any case, it is obvious that Ceph and Swift drivers are
largely more scalable. Ceph also offers better consistency, and hence is the
recommended driver.

How to plan for Gnocchi’s storage
---------------------------------

Gnocchi uses a custom file format based on its library *Carbonara*. In Gnocchi,
a time series is a collection of points, where a point is a given measure, or
sample, in the lifespan of a time series. The storage format is compressed
using various techniques, therefore the computing of a time series' size can be
estimated based on its **worst** case scenario with the following formula::

    number of points × 8 bytes = size in bytes

The number of points you want to keep is usually determined by the following
formula::

    number of points = timespan ÷ granularity

For example, if you want to keep a year of data with a one minute resolution::

    number of points = (365 days × 24 hours × 60 minutes) ÷ 1 minute
    number of points = 525 600

Then::

    size in bytes = 525 600 × 8 = 4 204 800 bytes = 4 106 KiB

This is just for a single aggregated time series. If your archive policy uses
the 8 default aggregation methods (mean, min, max, sum, std, median, count,
95pct) with the same "one year, one minute aggregations" resolution, the space
used will go up to a maximum of 8 × 4.1 MiB = 32.8 MiB.


How to define archive policies
------------------------------

In Gnocchi, the archive policy definitions are expressed in number of points.
If your archive policy defines a policy of 10 points with a granularity of 1
second, the time series archive will keep up to 10 seconds, each representing
an aggregation over 1 second. This means the time series will at maximum retain
10 seconds of data (sometimes a bit more) between the more recent point and the
oldest point. That does not mean it will be 10 consecutive seconds: there might
be a gap if data is fed irregularly.

There is no expiry of data relative to the current timestamp. Also, you cannot
delete old data points (at least for now).

Therefore, both the archive policy and the granularity entirely depends on your
use case. Depending on the usage of your data, you can define several archiving
policies. A typical low grained use case could be::

    3600 points with a granularity of 1 second = 1 hour
    1440 points with a granularity of 1 minute = 24 hours
    720 points with a granularity of 1 hour = 30 days
    365 points with a granularity of 1 day = 1 year

This would represent 6125 points × 9 = 54 KiB per aggregation method. If
you use the 8 standard aggregation method, your metric will take up to 8 × 54
KiB = 432 KiB of disk space.

Be aware that the more definitions you set in an archive policy, the more CPU
it will consume. Therefore, creating an archive policy with 2 definitons (e.g.
1 second granularity for 1 day and 1 minute granularity for 1 month) will
consume twice CPU than just one definition (e.g. just 1 second granularity for
1 day).

Default archive policies
------------------------

By default, 3 archive policies are created using the default archive policy
list (listed in `default_aggregation_methods`, i.e. mean, min, max, sum, std,
median, count, 95pct):

- low (maximum estimated size per metric: 406 MiB)

  * 5 minutes granularity over 30 days

- medium (maximum estimated size per metric: 887 KiB)

  * 1 minute granularity over 7 days
  * 1 hour granularity over 365 days

- high (maximum estimated size per metric: 1 057 KiB)

  * 1 second granularity over 1 hour
  * 1 minute granularity over 1 week
  * 1 hour granularity over 1 year
