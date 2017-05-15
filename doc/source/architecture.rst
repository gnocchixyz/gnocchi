======================
 Project Architecture
======================

Gnocchi consists of several services: a HTTP REST API (see :doc:`rest`), an
optional statsd-compatible daemon (see :doc:`statsd`), and an asynchronous
processing daemon (named `gnocchi-metricd`). Data is received via the HTTP REST
API or statsd daemon. `gnocchi-metricd` performs operations (statistics
computing, metric cleanup, etc...) on the received data in the background.

Both the HTTP REST API and the asynchronous processing daemon are stateless and
are scalable. Additional workers can be added depending on load.

.. image:: architecture.png
  :align: center
  :width: 80%
  :alt: Gnocchi architecture


Back-ends
---------

Gnocchi uses three different back-ends for storing data: one for storing new
incoming measures (the incoming driver), one for storing the time series (the
storage driver) and one for indexing the data (the index driver).

The *incoming* storage is responsible for storing new measures sent to metrics.
It is by default – and usually – the same driver as the *storage* one.

The *storage* is responsible for storing measures of created metrics. It
receives timestamps and values, and pre-computes aggregations according to the
defined archive policies.

The *indexer* is responsible for storing the index of all resources, archive
policies and metrics, along with their definitions, types and properties. The
indexer is also responsible for linking resources with metrics.

Available storage back-ends
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Gnocchi currently offers different storage drivers:

* File (default)
* `Ceph`_ (preferred)
* `OpenStack Swift`_
* `S3`_
* `Redis`_

The drivers are based on an intermediate library, named *Carbonara*, which
handles the time series manipulation, since none of these storage technologies
handle time series natively.

The four *Carbonara* based drivers are working well and are as scalable as
their back-end technology permits. Ceph and Swift are inherently more scalable
than the file driver.

Depending on the size of your architecture, using the file driver and storing
your data on a disk might be enough. If you need to scale the number of server
with the file driver, you can export and share the data via NFS among all
Gnocchi processes. In any case, it is obvious that S3, Ceph and Swift drivers
are largely more scalable. Ceph also offers better consistency, and hence is
the recommended driver.

.. _OpenStack Swift: http://docs.openstack.org/developer/swift/
.. _Ceph: https://ceph.com
.. _`S3`: https://aws.amazon.com/s3/
.. _`Redis`: https://redis.io

Available index back-ends
~~~~~~~~~~~~~~~~~~~~~~~~~

Gnocchi currently offers different index drivers:

* `PostgreSQL`_ (preferred)
* `MySQL`_ (at least version 5.6.4)

Those drivers offer almost the same performance and features, though PostgreSQL
tends to be more performant and has some additional features (e.g. resource
duration computing).

.. _PostgreSQL: http://postgresql.org
.. _MySQL: http://mysql.org
