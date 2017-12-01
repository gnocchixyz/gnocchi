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

.. include:: include/term-substitution.rst
