==================================
Gnocchi – Metric as a Service
==================================

.. include:: ../../README.rst
   :start-line: 13

Key Features
------------

Gnocchi's main features are:

- HTTP REST interface
- Horizontal scalability
- Metric aggregation
- Measures batching support
- Archiving policy
- Metric value search
- Structured resources
- Resource history
- Queryable resource indexer
- Multi-tenant
- Grafana support
- Nagios/Icinga support
- Statsd protocol support
- Collectd plugin support

Community
---------
You can join Gnocchi's community via the following channels:

- Source code: https://github.com/gnocchixyz/gnocchi
- Bug tracker: https://github.com/gnocchixyz/gnocchi/issues
- IRC: #gnocchi on `Freenode <https://freenode.net>`_

Why Gnocchi?
------------

Gnocchi has been created to fulfill the need of a |time series| database usable
in the context of cloud computing: providing the ability to store large
quantities of |metrics|. It has been designed to handle large amount of
|aggregates| being stored, while being performant, scalable and fault-tolerant.
While doing this, the goal was to be sure to not build any hard dependency on
any complex storage system.

The Gnocchi project was started in 2014 as a spin-off of the `OpenStack
Ceilometer`_ project to address the performance issues that Ceilometer
encountered while using standard databases as a storage backends for |metrics|.
More information are available on `Julien's blog post on Gnocchi
<https://julien.danjou.info/blog/2014/openstack-ceilometer-the-gnocchi-experiment>`_.

.. _`OpenStack Ceilometer`: https://docs.openstack.org/developer/ceilometer/



Comparisons To Alternatives
---------------------------

The following table summarises feature comparison between different existing
open source time series database. More details are written below, if needed.

.. include:: comparison-table.rst

Gnocchi vs Prometheus
~~~~~~~~~~~~~~~~~~~~~
`Prometheus <https://prometheus.io/>`_ is a full-featured solution that
includes everything from polling the metrics to storing and archiving them. It
offers advanced features such as alerting.

In comparison, Gnocchi does not offer polling as it prefers to leverage
existing solutions (e.g. `collectd <http://collectd.org>`_). However, it
provides high-availability and horizontal scalablity as well as multi-tenancy.


Gnocchi vs InfluxDB
~~~~~~~~~~~~~~~~~~~

`InfluxDB <http://influxdb.org>`_ is a time series database storing metrics
into local files. It offers a variety of input protocol support and created its
own query language, InfluxQL, inspired from SQL. The HTTP API it offers is just
a way to pass InfluxQL over the wire. Horizontal scalability is only provided
in the commercial version. The data model is based on time series with labels
associated to it.

In comparison, Gnocchi offers scalability and multi-tenancy. Its data model
differs as it does not provide labels, but |resources| to attach to |metrics|.

Gnocchi vs OpenTSDB
~~~~~~~~~~~~~~~~~~~

`OpenTSDB <http://opentsdb.net/>`_ is a distributed time series database that
uses `Hadoop <http://hadoop.apache.org/>`_ and `HBase
<http://hbase.apache.org/>`_ to store its data. That makes it easy to scale
horizontally. However, its querying feature are rather simple.

In comparison, Gnocchi offers a proper query language with more features. The
usage of Hadoop might be a show-stopper for many as it's quite heavy to deploy
and operate.

Gnocchi vs Graphite
~~~~~~~~~~~~~~~~~~~

`Graphite <http://graphite.readthedocs.org/en/latest/>`_ is essentially a data
metric storage composed of flat files (Whisper), and focuses on rendering those
time series. Each time series stored is composed of points that are stored
regularly and are related to the current date and time.

In comparison, Gnocchi offers much more scalability, a better file format and
no relativity to the current time and date.

Documentation
-------------

.. toctree::
   :maxdepth: 1

   architecture
   install
   operating
   client
   rest
   statsd
   grafana
   nagios
   collectd
   glossary
   releasenotes/index.rst
   contributing

.. include:: include/term-substitution.rst
