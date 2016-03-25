==============
 Installation
==============

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


.. _installation:

Installation
============

To install Gnocchi using `pip`, just type::

  pip install gnocchi

Depending on the drivers and features you want to use, you need to install
extra variants using, for example::

  pip install gnocchi[postgresql,ceph,keystone]

This would install PostgreSQL support for the indexer, Ceph support for
storage, and Keystone support for authentication and authorization.

The list of variants available is:

* keystone – provides Keystone authentication support
* mysql - provides MySQL indexer support
* postgresql – provides PostgreSQL indexer support
* swift – provides OpenStack Swift storage support
* ceph – provides common part of Ceph storage support
* ceph-pre-jewel – provides Ceph (<10.1.0) storage support
* ceph-jewel-and-later – provides Ceph (>=10.1.0) storage support
* file – provides file driver support
* doc – documentation building support
* test – unit and functional tests support

To install Gnocchi from source, run the standard Python installation
procedure::

  pip install -e .

Again, depending on the drivers and features you want to use, you need to
install extra variants using, for example::

  pip install -e .[postgresql,ceph,ceph-pre-jewel]


Ceph requirements
-----------------

Gnocchi leverages omap API of librados, but this is available in python binding
only since python-rados >= 9.1.0. To handle this, Gnocchi uses 'cradox' python
library which has exactly the same API but works with Ceph >= 0.80.0.

If Ceph and python-rados are >= 9.1.0, cradox python library becomes optional
but is still recommended until 10.1.0.


Initialization
==============

Once you have configured Gnocchi properly (see :doc:`configuration`), you need
to initialize the indexer and storage:

::

    gnocchi-upgrade


Upgrading
=========
In order to upgrade from a previous version of Gnocchi, you need to make sure
that your indexer and storage are properly upgraded. Run the following:

1. Stop the old version of Gnocchi API server and metric daemon

2. Install the new version of Gnocchi

2. Run `gnocchi-upgrade`
   This can take several hours depending on the size of your index and
   storage.

3. Start the new Gnocchi API server and metric daemon

Minimal interruption upgrade
============================
Gnocchi supports online upgrade of its storage system, which avoids
interrupting Gnocchi for a long time. In order to upgrade from previous
versions, you need to follow the following steps:

1. Stop the old Gnocchi API server and metric daemon

2. Run `gnocchi-upgrade --skip-storage` with the new version of Gnocchi.
   This can take several minutes depending on the size of your index.

3. Start the new Gnocchi API server.

4. Run `gnocchi-upgrade` with the new version of Gnocchi
   This can take several hours depending on the size of your storage.

5. Start the new Gnocchi metric daemon.

This will upgrade the indexer and storage in two passes. While a new version of
Gnocchi API cannot run with an old version of the indexer, it can run with an
old version of its storage back-end. For performance reasons, _metricd_ needs
to run an upgraded storage back-end, otherwise it would spend too much time
checking for upgrade pattern on each run.
