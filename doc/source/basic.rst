==================
 What is Gnocchi?
==================

.. image:: gnocchi-logo.jpg

Gnocchi is a service for managing a set of resources and storing metrics about
them. It allows its users to create resources (servers, images, volumes…)
with properties (name, URL, flavors…) and to associate those resources with
metrics (CPU usage, bandwidth…) that are going to metered.

The point of Gnocchi is to provide this service and its features in a scalable
and resilient way. Its functionalities are exposed over an HTTP REST API.

============================
 A Brief History of Gnocchi
============================

The Gnocchi project was started in 2014 as a spin-off of the `OpenStack
Ceilometer`_ project to address the performance issues that Ceilometer
encountered while using standard databases as a storage backends for metrics.
More information are available on `Julien's blog post on Gnocchi
<https://julien.danjou.info/blog/2014/openstack-ceilometer-the-gnocchi-experiment>`_.

.. _`OpenStack Ceilometer`: http://launchpad.net/ceilometer

======================
 Project Architecture
======================

Gnocchi is built around 2 main components: a storage driver and an indexer
driver. The REST API exposed to the user manipulates both these drivers to
provide all the features that are needed to provide correct infrastructure
measurement.

The *storage* is responsible for storing measures of created metrics. It
receives timestamps and values and computes aggregations according to the
defined archive policies.

The *indexer* is responsible for storing the index of all resources, along with
their types and their properties. Gnocchi only knows resource types from the
OpenStack project, but also provides a *generic* type so you can create basic
resources and handle the resource properties yourself. The indexer is also
responsible for linking resources with metrics.
