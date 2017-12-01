===================
Amqp1d Daemon Usage
===================

What Is It?
===========
`AMQP 1.0`_ Advanced Messaging Queuing Protocol 1.0 is ISO/IEC 19464.

Gnocchi provides a daemon `gnocchi-amqp1d` that is compatible with the
amqpd 1.0 protocol and can listen to |metrics| sent over the network
via amqp1 `collectd`_  plugin , named `amqp1`_.

.. _`amqp1`: https://github.com/ajssmith/collectd/tree/amqp1_dev_branch
.. _`collectd`: https://github.com/collectd/collectd
.. _`AMQP 1.0`: https://www.amqp.org/resources/specifications

`amqp1` collectd write plugin enables collectd output to be sent to an
Advanced Messaging Queuing Protocol 1.0 intermediary such as the
Apache Qpid Dispatch Router or Apache Artemis Broker. The AMQP 1.0 protocol is
ISO/IEC 19464. The plugin supports collectd command, JSON and graphite
formatted output.


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

.. include:: include/term-substitution.rst
