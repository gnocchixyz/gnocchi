==============
 Installation
==============

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

Installation Using Devstack
===========================

To enable Gnocchi in devstack, add the following to local.conf:

::

    enable_plugin gnocchi https://github.com/openstack/gnocchi master
    enable_service gnocchi-api,gnocchi-metricd

To enable Grafana support in devstack, you can also enable `gnocchi-grafana`::

    enable_service gnocchi-grafana

Then, you can start devstack:

::

    ./stack.sh


Installation Using Sources
==========================

To install Gnocchi, run the standard Python installation procedure:

::

    pip install -e .


Configuration
=============

Configure Gnocchi by editing `/etc/gnocchi/gnocchi.conf`.

No config file is provided with the source code, but one can be easily
created by running:

::

    tox -e genconfig

This command will create an `etc/gnocchi/gnocchi.conf` file which can be used
as a base for the default configuration file at `/etc/gnocchi/gnocchi.conf`. If
you're using _devstack_, this file is already generated and put in place.

The configuration file should be pretty explicit, but here are some of the base
options you want to change and configure:


+---------------------+---------------------------------------------------+
| Option name         | Help                                              |
+=====================+===================================================+
| storage.driver      | The storage driver for metrics.                   |
+---------------------+---------------------------------------------------+
| indexer.url         | URL to your indexer.                              |
+---------------------+---------------------------------------------------+
| storage.file_*      | Configuration options to store files              |
|                     | if you use the file storage driver.               |
+---------------------+---------------------------------------------------+
| storage.swift_*     | Configuration options to access Swift             |
|                     | if you use the Swift storage driver.              |
+---------------------+---------------------------------------------------+
| storage.ceph_*      | Configuration options to access Ceph              |
|                     | if you use the Ceph storage driver.               |
+---------------------+---------------------------------------------------+


Gnocchi provides these storage drivers:

- File (default)
- `Swift`_
- `Ceph`_
- `InfluxDB`_ (experimental)

Gnocchi provides these indexer drivers:

- `PostgreSQL`_ (recommended)
- `MySQL`_

.. _`Swift`: https://launchpad.net/swift
.. _`Ceph`: http://ceph.com/
.. _`PostgreSQL`: http://postgresql.org
.. _`MySQL`: http://mysql.com
.. _`InfluxDB`: http://influxdb.com

Indexer Initialization
======================

Once you have configured Gnocchi properly, you need to initialize the indexer:

::

    gnocchi-dbsync


Running Gnocchi
===============

To run Gnocchi, simply run the HTTP server and metric daemon:

::

    gnocchi-api
    gnocchi-metricd


Running As A WSGI Application
=============================

It's possible – and strongly advised – to run Gnocchi through a WSGI
service such as `mod_wsgi`_ or any other WSGI application. The file
`gnocchi/rest/app.wsgi` provided with Gnocchi allows you to enable Gnocchi as
a WSGI application.
For other WSGI setup you can refer to the `pecan deployement`_ documentation.

.. _`mod_wsgi`: https://modwsgi.readthedocs.org/en/master/
.. _`pecan deployement`: http://pecan.readthedocs.org/en/latest/deployment.html#deployment


Drivers notes
=============

Carbonara based drivers (file, swift, ceph)
-------------------------------------------

To ensure consistency accross all gnocchi-api and gnocchi-metricd workers,
these drivers need a distributed locking mechanism. This is provided by the
'coordinator' of the `tooz`_ library.

By default, the configured backend for `tooz`_ is 'file', this allows locking
across workers on the same node.

In a multi-nodes deployement, the coordinator needs to be changed via
the storage/coordination_url configuration options to one of the other
`tooz backends`_.

For example::

    coordination_url = redis://<sentinel host>?sentinel=<master name>
    coordination_url = zookeeper:///hosts=<zookeeper_host1>&hosts=<zookeeper_host2>

.. _`tooz`: http://docs.openstack.org/developer/tooz/
.. _`tooz backends`: http://docs.openstack.org/developer/tooz/drivers.html


Ceph driver implementation details
----------------------------------

Each batch of measurements to process is stored into one rados object.
These objects are named `measures_<metric_id>_<random_uuid>_<timestamp>`

Also a special empty object called `measures` has the list of measures to
process stored in its xattr attributes.

Because of the asynchronous nature of how we store measurements in Gnocchi,
`gnocchi-metricd` need to known the list of objects that wait to be processed:

- Listing rados objects for this is not a solution since it takes too much
  time.
- Using a custom format into a rados object, would force us to use a lock
  each time we would change it.

Instead, the xattrs of one empty rados object are used. No lock is needed to
add/remove a xattr.

But depending of the filesystem used by ceph OSDs, this xattrs can have
limitation in term of numbers and size if Ceph if not correctly configured.
See `Ceph extended attributes documentation`_ for more details.

Then, each Carbonara generated file is stored in *one* rados object.
So each metric has one rados object per aggregation in the archive policy.

Because of this, the OSDs filling can look less balanced comparing of the RBD.
Some other objects will be big and some others small depending on how archive
policies are set up.

We can imagine an unrealisting case like 1 point per second during one year,
the rados object size will be ~384MB.

And a more realistic scenario, a 4MB rados object (like rbd uses) could
come from:

- 20 days with 1 point every seconds
- 100 days with 1 point every 5 seconds

So, in realistic scenarios, the direct relation between the archive policy and
the size of the rados objects created by Gnocchi is not a problem.

.. _`Ceph extended attributes documentation`: http://docs.ceph.com/docs/master/rados/configuration/filestore-config-ref/#extended-attributes
