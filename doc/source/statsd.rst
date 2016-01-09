===================
Statsd Daemon Usage
===================

What Is It?
===========
`Statsd`_ is a network daemon that listens for statistics sent over the network
using TCP or UDP, and then sends aggregates to another backend.

Gnocchi provides a daemon that is compatible with the statsd protocol and can
listen to metrics sent over the network, named `gnocchi-statsd`.

.. _`Statsd`: https://github.com/etsy/statsd/

How It Works?
=============
In order to enable statsd support in Gnocchi, you need to configure the
`[statsd]` option group in the configuration file. You need to provide a
resource ID that will be used as the main generic resource where all the
metrics will be attached, a user and project id that will be associated with
the resource and metrics, and an archive policy name that will be used to
create the metrics.

All the metrics will be created dynamically as the metrics are sent to
`gnocchi-statsd`, and attached with the provided name to the resource ID you
configured.

The `gnocchi-statsd` may be scaled, but trade-offs have to been made due to the
nature of the statsd protocol. That means that if you use metrics of type
`counter`_ or sampling (`c` in the protocol), you should always send those
metrics to the same daemon – or not use them at all. The other supported
types (`timing`_ and `gauges`_) does not suffer this limitation, but be aware
that you might have more measures that expected if you send the same metric to
different `gnocchi-statsd` server, as their cache nor their flush delay are
synchronized.

.. _`counter`: https://github.com/etsy/statsd/blob/master/docs/metric_types.md#counting
.. _`timing`: https://github.com/etsy/statsd/blob/master/docs/metric_types.md#timing
.. _`gauges`: https://github.com/etsy/statsd/blob/master/docs/metric_types.md#gauges

.. note ::
   The statsd protocol support is incomplete: relative gauge values with +/-
   and sets are not supported yet.
