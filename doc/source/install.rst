==============
 Installation
==============

Installation Using Devstack
===========================

To enable Gnocchi in devstack, add the following to local.conf:

::

    enable_plugin gnocchi https://github.com/openstack/gnocchi master

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
* ceph_recommended_lib – provides Ceph (>=0.80) storage support
* ceph_alternative_lib – provides Ceph (>=10.1.0) storage support
* file – provides file driver support
* doc – documentation building support
* test – unit and functional tests support

To install Gnocchi from source, run the standard Python installation
procedure::

  pip install -e .

Again, depending on the drivers and features you want to use, you need to
install extra variants using, for example::

  pip install -e .[postgresql,ceph,ceph_recommended_lib]


Ceph requirements
-----------------

Gnocchi leverages omap API of librados, but this is available in python binding
only since python-rados >= 9.1.0. To handle this, Gnocchi uses 'cradox' python
library which has exactly the same API but works with Ceph >= 0.80.0.

If Ceph and python-rados are >= 9.1.0, cradox python library becomes optional
but is still recommended.


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

1. Stop the old version of Gnocchi API server and `gnocchi-statsd` daemon

2. Stop the old version of `gnocchi-metricd` daemon

3. Install the new version of Gnocchi

4. Run `gnocchi-upgrade`
   This can take several hours depending on the size of your index and
   storage.

5. Start the new Gnocchi API server, `gnocchi-metricd`
   and `gnocchi-statsd` daemons
