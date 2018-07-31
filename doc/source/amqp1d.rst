=====================
AMQP 1.0 Daemon Usage
=====================

Gnocchi provides a daemon `gnocchi-amqp1d` that is compatible with the `AMQP
1.0`_ (Advanced Messaging Queuing Protocol 1.0 (ISO/IEC 19464)) protocol and
can listen to |metrics| sent over the network via the amqp1 `collectd`_  plugin
named `amqp1`_.

.. _`amqp1`: https://github.com/collectd/collectd/blob/master/src/amqp1.c
.. _`collectd`: https://github.com/collectd/collectd
.. _`AMQP 1.0`: https://www.amqp.org/resources/specifications

`amqp1` collectd write plugin enables collectd output to be sent to an Advanced
Messaging Queuing Protocol 1.0 intermediary such as the Apache Qpid Dispatch
Router or Apache Artemis Broker.

How It Works?
=============
In order to enable amqp1d support in Gnocchi, you need to configure the
`[amqp1d]` option group in the configuration file. You need to provide a
host with port and topic name that amqp1 collectd plugin is publishing metric
to and a |resource| name that will be used as the main  |resource| where all
the |metrics| will be attached with host name as an attribute, a user and
project id that will be associated with the |resource| and |metrics|,
and an |archive policy| name that will be used to create the |metrics|.

All the |metrics| will be created dynamically as the |metrics| are sent to
`gnocchi-amqp1d`, and attached with the source host name to the |resource|
name you configured.

To use it, Gnocchi must be installed with the `amqp1` flavor::

  pip install -e .[postgresql,file,amqp1]


.. include:: include/term-substitution.rst
