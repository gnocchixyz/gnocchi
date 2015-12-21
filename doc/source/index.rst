==================================
Gnocchi – Metric as a Service
==================================

.. image:: gnocchi-logo.jpg
  :align: right
  :width: 20%
  :alt: Gnocchi logo

.. include:: ../../README.rst
   :start-line: 6

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

   architecture
   install
   configuration
   rest
   statsd
   resource_types

.. _`OpenStack`: http://openstack.org
