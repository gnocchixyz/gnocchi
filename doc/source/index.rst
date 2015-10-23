================================
 Gnocchi -- Metric as a Service
================================

.. image:: gnocchi-logo.jpg

Gnocchi is a multi-tenant timeseries, metrics and resources database. It
provides an `HTTP REST`_ interface to create and manipulate those data. It is
designed to store metrics at a very large scale while providing access to
metrics and resources information to operators and users.

Gnocchi is part of the `OpenStack` project. While Gnocchi has support for
OpenStack, it is fully able to work stand-alone.

A Brief History of Gnocchi
--------------------------

The Gnocchi project was started in 2014 as a spin-off of the `OpenStack
Ceilometer`_ project to address the performance issues that Ceilometer
encountered while using standard databases as a storage backends for metrics.
More information are available on `Julien's blog post on Gnocchi
<https://julien.danjou.info/blog/2014/openstack-ceilometer-the-gnocchi-experiment>`_.

.. _`OpenStack Ceilometer`: http://launchpad.net/ceilometer

Key Features
============

- HTTP REST interface
- Horizontal scalability
- Metric aggregation
- Archiving policy
- Metric value search
- Structured resources
- Queryable resource indexer
- Multi-tenant
- Grafana support
- Statsd protocol support


Documentation
=============

.. toctree::
   :maxdepth: 1

   install
   rest
   statsd
   resource_types

.. _`HTTP REST`: https://en.wikipedia.org/wiki/Representational_state_transfer
.. _`OpenStack`: http://openstack.org
